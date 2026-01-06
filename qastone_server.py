#!/usr/bin/env python3
"""
QA.Stone MCP Twin Server

HTTP-based MCP server for QA.Stone demo system.
Designed for Railway deployment with A/B hot-swap capability.

Endpoints:
    POST /mcp              - MCP JSON-RPC endpoint
    GET  /health           - Health check
    GET  /info             - Server instance info
    POST /swap             - Swap active server (twin controller)

MCP Tools:
    qastone_create_account  - Create user account
    qastone_list_users      - List all users
    qastone_user_wallet     - Get user's wallet
    qastone_send_to_user    - Send stone between users
    qastone_check_inbox     - Check pending transfers
    qastone_accept_transfer - Accept incoming stone
    qastone_mint_to_user    - Mint new stone to user
    qastone_system_stats    - System statistics
"""

import os
import json
import logging
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import uvicorn
import httpx
import asyncio
from pathlib import Path

from redis_accounts import (
    create_account,
    authenticate,
    list_users,
    get_wallet,
    get_wallet_summary,
    mint_to_wallet,
    send_stone_by_username,
    check_inbox,
    accept_transfer,
    accept_all_transfers,
    get_stats,
    test_connection,
    generate_user_id,
)
from qastone_mint import mint_offer
from qastone_mcp_updates import (
    transfer_as_mcp_update,
    get_chain_status,
    get_recent_chain,
    verify_mcp_stone,
    MCPStoneUpdate,
    create_mcp_stone_update,
    apply_mcp_stone_update,
)

# =============================================================================
# CONFIG
# =============================================================================

# Railway uses PORT env var, SERVER_PORT is fallback for local dev
SERVER_INSTANCE = os.getenv("SERVER_INSTANCE", "a")
SERVER_PORT = int(os.getenv("PORT", os.getenv("SERVER_PORT", "8301")))
SERVER_VERSION = os.getenv("SERVER_VERSION", "1.0.0")

TWIN_A_URL = os.getenv("TWIN_A_URL", "http://localhost:8301")
TWIN_B_URL = os.getenv("TWIN_B_URL", "http://localhost:8302")

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(f"qastone-{SERVER_INSTANCE}")

# =============================================================================
# FASTAPI APP
# =============================================================================

app = FastAPI(
    title=f"QA.Stone MCP Server ({SERVER_INSTANCE.upper()})",
    version=SERVER_VERSION,
    description="MCP server for QA.Stone demo with hot-swap capability"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================

class MCPRequest(BaseModel):
    jsonrpc: str = "2.0"
    id: Optional[int] = None
    method: str
    params: Optional[Dict[str, Any]] = None


class MCPResponse(BaseModel):
    jsonrpc: str = "2.0"
    id: Optional[int] = None
    result: Optional[Any] = None
    error: Optional[Dict[str, Any]] = None


# =============================================================================
# MCP TOOL DEFINITIONS
# =============================================================================

MCP_TOOLS = [
    {
        "name": "qastone_create_account",
        "description": "Create a new QA.Stone user account",
        "inputSchema": {
            "type": "object",
            "properties": {
                "username": {"type": "string", "description": "Unique username"},
                "token": {"type": "string", "description": "Optional custom auth token"}
            },
            "required": ["username"]
        }
    },
    {
        "name": "qastone_list_users",
        "description": "List all QA.Stone user accounts with wallet summaries",
        "inputSchema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "qastone_user_wallet",
        "description": "Get a user's wallet contents",
        "inputSchema": {
            "type": "object",
            "properties": {
                "username": {"type": "string", "description": "Username"},
                "token": {"type": "string", "description": "Auth token (alternative)"}
            },
            "required": []
        }
    },
    {
        "name": "qastone_send_to_user",
        "description": "Send a QA.Stone from one user to another",
        "inputSchema": {
            "type": "object",
            "properties": {
                "from_token": {"type": "string", "description": "Sender's auth token"},
                "to_username": {"type": "string", "description": "Recipient's username"},
                "stone_id": {"type": "string", "description": "Stone ID to send"}
            },
            "required": ["from_token", "to_username", "stone_id"]
        }
    },
    {
        "name": "qastone_check_inbox",
        "description": "Check pending stone transfers in user's inbox",
        "inputSchema": {
            "type": "object",
            "properties": {
                "token": {"type": "string", "description": "User's auth token"}
            },
            "required": ["token"]
        }
    },
    {
        "name": "qastone_accept_transfer",
        "description": "Accept a pending stone transfer",
        "inputSchema": {
            "type": "object",
            "properties": {
                "token": {"type": "string", "description": "User's auth token"},
                "transfer_id": {"type": "string", "description": "Transfer ID or 'all'"}
            },
            "required": ["token", "transfer_id"]
        }
    },
    {
        "name": "qastone_mint_to_user",
        "description": "Mint a new QA.Stone to a user's wallet",
        "inputSchema": {
            "type": "object",
            "properties": {
                "username": {"type": "string"},
                "sponsor": {"type": "string"},
                "type": {"type": "string", "enum": ["gift_card", "coupon", "api_token", "event_ticket"]},
                "value_cents": {"type": "integer"}
            },
            "required": ["username", "sponsor", "type", "value_cents"]
        }
    },
    {
        "name": "qastone_system_stats",
        "description": "Get system-wide statistics",
        "inputSchema": {"type": "object", "properties": {}, "required": []}
    }
]

# =============================================================================
# MCP TOOL HANDLERS
# =============================================================================

def handle_create_account(params: dict) -> dict:
    username = params.get("username")
    if not username:
        return {"success": False, "error": "Username required"}
    return create_account(username, custom_token=params.get("token"))


def handle_list_users(params: dict) -> dict:
    users = list_users()
    return {"success": True, "count": len(users), "users": users}


def handle_user_wallet(params: dict) -> dict:
    username = params.get("username")
    token = params.get("token")

    if token:
        user_id = authenticate(token)
        if not user_id:
            return {"success": False, "error": "Invalid token"}
    elif username:
        user_id = generate_user_id(username)
    else:
        return {"success": False, "error": "Username or token required"}

    summary = get_wallet_summary(user_id)
    stones = get_wallet(user_id)

    items = []
    for stone in stones:
        offer = stone.get("offer", {})
        items.append({
            "stone_id": stone.get("offer_stone_id"),
            "type": stone.get("offer_type"),
            "sponsor": offer.get("sponsor", {}).get("name", "Unknown"),
            "value": f"${offer.get('value_cents', 0) / 100:.2f}",
        })

    return {"success": True, "user_id": user_id, "summary": summary, "items": items}


def handle_send_to_user(params: dict) -> dict:
    from_token = params.get("from_token")
    to_username = params.get("to_username")
    stone_id = params.get("stone_id")

    if not all([from_token, to_username, stone_id]):
        return {"success": False, "error": "from_token, to_username, and stone_id required"}

    return send_stone_by_username(from_token, to_username, stone_id)


def handle_check_inbox(params: dict) -> dict:
    token = params.get("token")
    if not token:
        return {"success": False, "error": "Token required"}

    user_id = authenticate(token)
    if not user_id:
        return {"success": False, "error": "Invalid token"}

    transfers = check_inbox(user_id)
    items = []
    for t in transfers:
        stone = t.get("stone", {})
        offer = stone.get("offer", {})
        items.append({
            "transfer_id": t.get("transfer_id"),
            "from": t.get("from_user"),
            "stone_id": t.get("stone_id"),
            "sponsor": offer.get("sponsor", {}).get("name", "Unknown"),
            "value": f"${offer.get('value_cents', 0) / 100:.2f}",
        })

    return {"success": True, "pending_count": len(items), "transfers": items}


def handle_accept_transfer(params: dict) -> dict:
    token = params.get("token")
    transfer_id = params.get("transfer_id")

    if not token or not transfer_id:
        return {"success": False, "error": "token and transfer_id required"}

    user_id = authenticate(token)
    if not user_id:
        return {"success": False, "error": "Invalid token"}

    if transfer_id.lower() == "all":
        return accept_all_transfers(user_id)
    return accept_transfer(user_id, transfer_id)


def handle_mint_to_user(params: dict) -> dict:
    username = params.get("username")
    sponsor = params.get("sponsor")
    offer_type = params.get("type")
    value_cents = params.get("value_cents")

    if not all([username, sponsor, offer_type, value_cents]):
        return {"success": False, "error": "username, sponsor, type, and value_cents required"}

    user_id = generate_user_id(username)
    stone = mint_offer(sponsor, offer_type, value_cents)
    result = mint_to_wallet(user_id, stone)

    if result.get("success"):
        return {
            "success": True,
            "stone_id": stone["offer_stone_id"],
            "username": username,
            "value": f"${value_cents / 100:.2f}"
        }
    return result


def handle_system_stats(params: dict) -> dict:
    stats = get_stats()
    stats["server_instance"] = SERVER_INSTANCE
    stats["server_version"] = SERVER_VERSION
    return {"success": True, "stats": stats}


# Tool dispatch
TOOL_HANDLERS = {
    "qastone_create_account": handle_create_account,
    "qastone_list_users": handle_list_users,
    "qastone_user_wallet": handle_user_wallet,
    "qastone_send_to_user": handle_send_to_user,
    "qastone_check_inbox": handle_check_inbox,
    "qastone_accept_transfer": handle_accept_transfer,
    "qastone_mint_to_user": handle_mint_to_user,
    "qastone_system_stats": handle_system_stats,
}

# =============================================================================
# HTTP ENDPOINTS
# =============================================================================

# Static files directory
STATIC_DIR = Path(__file__).parent / "static"


# =============================================================================
# WEBSOCKET CONNECTION MANAGER
# =============================================================================

class ConnectionManager:
    """Manages WebSocket connections for real-time notifications"""

    def __init__(self):
        # Map user_id -> list of WebSocket connections
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, user_id: str):
        await websocket.accept()
        if user_id not in self.active_connections:
            self.active_connections[user_id] = []
        self.active_connections[user_id].append(websocket)
        logger.info(f"WebSocket connected: {user_id} (total: {len(self.active_connections[user_id])})")

    def disconnect(self, websocket: WebSocket, user_id: str):
        if user_id in self.active_connections:
            if websocket in self.active_connections[user_id]:
                self.active_connections[user_id].remove(websocket)
            if not self.active_connections[user_id]:
                del self.active_connections[user_id]
        logger.info(f"WebSocket disconnected: {user_id}")

    async def notify_user(self, user_id: str, message: dict):
        """Send notification to all connections for a user"""
        if user_id in self.active_connections:
            dead_connections = []
            for connection in self.active_connections[user_id]:
                try:
                    await connection.send_json(message)
                except:
                    dead_connections.append(connection)
            # Clean up dead connections
            for conn in dead_connections:
                self.active_connections[user_id].remove(conn)

    async def broadcast(self, message: dict):
        """Broadcast to all connected users"""
        for user_id in list(self.active_connections.keys()):
            await self.notify_user(user_id, message)

    def get_connected_users(self) -> List[str]:
        return list(self.active_connections.keys())


# Global connection manager
ws_manager = ConnectionManager()


@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the web UI landing page"""
    index_file = STATIC_DIR / "index.html"
    if index_file.exists():
        return HTMLResponse(content=index_file.read_text(), status_code=200)
    # Fallback to JSON if no UI
    return JSONResponse({
        "service": "QA.Stone MCP Server",
        "instance": SERVER_INSTANCE,
        "version": SERVER_VERSION,
        "status": "running",
        "ui": "Visit /api/users to see users or deploy with static/index.html for web UI"
    })


@app.get("/app.html", response_class=HTMLResponse)
async def app_page():
    """Serve the main app SPA"""
    app_file = STATIC_DIR / "app.html"
    if app_file.exists():
        return HTMLResponse(content=app_file.read_text(), status_code=200)
    return HTMLResponse(content="<h1>App not found</h1>", status_code=404)


@app.get("/health")
async def health():
    redis_ok = test_connection()
    return {
        "status": "healthy" if redis_ok else "degraded",
        "instance": SERVER_INSTANCE,
        "version": SERVER_VERSION,
        "redis": "connected" if redis_ok else "disconnected",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/info")
async def info():
    stats = get_stats()
    return {
        "instance": SERVER_INSTANCE,
        "version": SERVER_VERSION,
        "port": SERVER_PORT,
        "twin_urls": {
            "a": TWIN_A_URL,
            "b": TWIN_B_URL
        },
        "stats": stats
    }


@app.get("/tools")
async def get_tools():
    return {"tools": MCP_TOOLS}


@app.post("/mcp")
async def mcp_endpoint(request: MCPRequest):
    """Main MCP JSON-RPC endpoint"""
    logger.info(f"MCP request: {request.method}")

    # Handle MCP protocol methods
    if request.method == "initialize":
        return MCPResponse(
            id=request.id,
            result={
                "protocolVersion": "2024-11-05",
                "serverInfo": {
                    "name": f"qastone-mcp-{SERVER_INSTANCE}",
                    "version": SERVER_VERSION
                },
                "capabilities": {
                    "tools": {"listChanged": False}
                }
            }
        )

    elif request.method == "tools/list":
        return MCPResponse(
            id=request.id,
            result={"tools": MCP_TOOLS}
        )

    elif request.method == "tools/call":
        tool_name = request.params.get("name") if request.params else None
        tool_args = request.params.get("arguments", {}) if request.params else {}

        if not tool_name:
            return MCPResponse(
                id=request.id,
                error={"code": -32602, "message": "Tool name required"}
            )

        handler = TOOL_HANDLERS.get(tool_name)
        if not handler:
            return MCPResponse(
                id=request.id,
                error={"code": -32601, "message": f"Unknown tool: {tool_name}"}
            )

        try:
            result = handler(tool_args)
            return MCPResponse(
                id=request.id,
                result={"content": [{"type": "text", "text": json.dumps(result)}]}
            )
        except Exception as e:
            logger.error(f"Tool error: {e}")
            return MCPResponse(
                id=request.id,
                error={"code": -32603, "message": str(e)}
            )

    else:
        return MCPResponse(
            id=request.id,
            error={"code": -32601, "message": f"Unknown method: {request.method}"}
        )


# =============================================================================
# DIRECT API ENDPOINTS (non-MCP, for easier testing)
# =============================================================================

@app.get("/api/users")
async def api_list_users():
    return handle_list_users({})


@app.get("/api/users/{username}/wallet")
async def api_user_wallet(username: str):
    return handle_user_wallet({"username": username})


@app.post("/api/users")
async def api_create_user(username: str, token: Optional[str] = None):
    return handle_create_account({"username": username, "token": token})


@app.post("/api/transfer")
async def api_transfer(from_token: str, to_username: str, stone_id: str):
    return handle_send_to_user({
        "from_token": from_token,
        "to_username": to_username,
        "stone_id": stone_id
    })


@app.get("/api/inbox/{token}")
async def api_check_inbox(token: str):
    return handle_check_inbox({"token": token})


@app.post("/api/accept")
async def api_accept_transfer(token: str, transfer_id: str):
    return handle_accept_transfer({"token": token, "transfer_id": transfer_id})


@app.post("/api/mint")
async def api_mint(username: str, sponsor: str, type: str, value_cents: int):
    return handle_mint_to_user({
        "username": username,
        "sponsor": sponsor,
        "type": type,
        "value_cents": value_cents
    })


@app.get("/api/stats")
async def api_stats():
    return handle_system_stats({})


# =============================================================================
# WEBSOCKET ENDPOINTS
# =============================================================================

@app.websocket("/ws/{token}")
async def websocket_endpoint(websocket: WebSocket, token: str):
    """WebSocket connection for real-time notifications"""
    user_id = authenticate(token)
    if not user_id:
        await websocket.close(code=4001, reason="Invalid token")
        return

    await ws_manager.connect(websocket, user_id)

    try:
        # Send initial connection confirmation
        await websocket.send_json({
            "type": "connected",
            "user_id": user_id,
            "server_instance": SERVER_INSTANCE,
            "message": f"Connected to Twin {SERVER_INSTANCE.upper()}"
        })

        # Keep connection alive and handle incoming messages
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_json(), timeout=30.0)
                # Handle ping/pong for keepalive
                if data.get("type") == "ping":
                    await websocket.send_json({"type": "pong"})
            except asyncio.TimeoutError:
                # Send heartbeat
                await websocket.send_json({
                    "type": "heartbeat",
                    "server_instance": SERVER_INSTANCE,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket, user_id)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        ws_manager.disconnect(websocket, user_id)


@app.get("/api/ws/status")
async def ws_status():
    """Get WebSocket connection status"""
    return {
        "connected_users": ws_manager.get_connected_users(),
        "total_connections": sum(len(conns) for conns in ws_manager.active_connections.values()),
        "server_instance": SERVER_INSTANCE
    }


# Override transfer to add notifications
@app.post("/api/transfer/notify")
async def api_transfer_with_notify(from_token: str, to_username: str, stone_id: str):
    """Transfer with WebSocket notification to recipient"""
    result = handle_send_to_user({
        "from_token": from_token,
        "to_username": to_username,
        "stone_id": stone_id
    })

    if result.get("success"):
        # Notify recipient via WebSocket
        to_user_id = generate_user_id(to_username)
        await ws_manager.notify_user(to_user_id, {
            "type": "transfer_received",
            "from": result.get("from"),
            "stone_id": stone_id,
            "transfer_id": result.get("transfer_id"),
            "message": f"You received a stone from {result.get('from', 'someone').replace('user_', '')}!"
        })

        # Also notify sender of success
        from_user_id = authenticate(from_token)
        if from_user_id:
            await ws_manager.notify_user(from_user_id, {
                "type": "transfer_sent",
                "to": to_user_id,
                "stone_id": stone_id,
                "transfer_id": result.get("transfer_id"),
                "message": f"Stone sent to {to_username}!"
            })

    return result


# Broadcast server event (for hot-swap notifications)
@app.post("/api/broadcast")
async def api_broadcast(message: str, event_type: str = "server_event"):
    """Broadcast a message to all connected users"""
    await ws_manager.broadcast({
        "type": event_type,
        "message": message,
        "server_instance": SERVER_INSTANCE,
        "timestamp": datetime.now(timezone.utc).isoformat()
    })
    return {"success": True, "recipients": ws_manager.get_connected_users()}


# =============================================================================
# MCP STONE UPDATES - QA.Stone IS the MCP Update
# =============================================================================

@app.post("/api/mcp-transfer")
async def api_mcp_transfer(from_token: str, to_username: str, stone_id: str):
    """
    Transfer a stone as a true MCP Stone Update.

    This creates a cryptographically signed, chainable stone that:
    1. IS the MCP message (not just data)
    2. Links to previous stone (blockchain-like)
    3. Can be verified and replayed on any MCP server
    4. Produces identical results regardless of which twin processes it
    """
    result = transfer_as_mcp_update(
        from_token=from_token,
        to_username=to_username,
        stone_id=stone_id,
        server_instance=SERVER_INSTANCE
    )

    # Send WebSocket notifications if successful
    if result.get("success"):
        to_user_id = generate_user_id(to_username)
        from_user_id = authenticate(from_token)

        # Notify recipient
        await ws_manager.notify_user(to_user_id, {
            "type": "mcp_stone_received",
            "stone_update_id": result.get("stone_update_id"),
            "chain_sequence": result.get("sequence_number"),
            "from": from_user_id,
            "message": f"MCP Stone Update #{result.get('sequence_number')} received!"
        })

        # Notify sender
        if from_user_id:
            await ws_manager.notify_user(from_user_id, {
                "type": "mcp_stone_sent",
                "stone_update_id": result.get("stone_update_id"),
                "chain_sequence": result.get("sequence_number"),
                "to": to_user_id,
                "message": f"MCP Stone Update #{result.get('sequence_number')} created and applied!"
            })

        # Broadcast to all (for demo visibility)
        await ws_manager.broadcast({
            "type": "chain_update",
            "stone_update_id": result.get("stone_update_id"),
            "sequence": result.get("sequence_number"),
            "action": "transfer",
            "server_instance": SERVER_INSTANCE
        })

    return result


@app.get("/api/chain/status")
async def api_chain_status():
    """Get the current MCP Stone chain status."""
    status = get_chain_status()
    status["server_instance"] = SERVER_INSTANCE
    return status


@app.get("/api/chain/recent")
async def api_chain_recent(limit: int = 10):
    """Get recent stones from the chain."""
    return {
        "stones": get_recent_chain(limit),
        "server_instance": SERVER_INSTANCE
    }


@app.post("/api/chain/verify")
async def api_chain_verify(stone_data: Dict[str, Any]):
    """
    Verify a stone can be applied.

    Send the full stone JSON to verify it's valid and can be applied
    to this MCP server with identical results.
    """
    try:
        stone = MCPStoneUpdate.from_dict(stone_data)
        verification = verify_mcp_stone(stone)
        verification["server_instance"] = SERVER_INSTANCE
        return verification
    except Exception as e:
        return {
            "valid": False,
            "error": str(e),
            "server_instance": SERVER_INSTANCE
        }


@app.get("/api/mcp-update/info")
async def api_mcp_update_info():
    """
    Information about the MCP Stone Update system.

    This endpoint explains the concept: QA.Stone IS the MCP Update.
    """
    return {
        "concept": "QA.Stone as MCP Update",
        "version": "2.0.0-mcp",
        "description": "Each QA.Stone IS an MCP protocol message - cryptographically signed, chainable, and verifiable",
        "features": {
            "stone_is_message": "The stone contains the full MCP tools/call that created it",
            "chained": "Each stone references the previous stone's hash (blockchain-like)",
            "verifiable": "Any MCP server can validate the stone signature",
            "deterministic": "Same stone + same state = identical result on any server",
            "hot_swap_proof": "Transfer on Twin A, verify on Twin B - same chain!"
        },
        "endpoints": {
            "POST /api/mcp-transfer": "Transfer stone as MCP update (creates chain entry)",
            "GET /api/chain/status": "Current chain head and length",
            "GET /api/chain/recent": "Recent stone updates",
            "POST /api/chain/verify": "Verify a stone is valid"
        },
        "chain_status": get_chain_status(),
        "server_instance": SERVER_INSTANCE
    }


# =============================================================================
# 3-LAYER STONE API (Scalable Architecture)
# =============================================================================

try:
    from qastone_3layer import (
        QAStone3Layer,
        store_stone_3layer,
        get_chain_entry,
        get_manifest,
        traverse_wormhole,
        verify_stone_3layer,
        get_chain_status_3layer,
        get_recent_stones_3layer,
        get_sync_payload,
        apply_sync_payload,
        create_3layer_transfer,
        create_3layer_message,
        Wormhole,
    )
    LAYER3_AVAILABLE = True
except ImportError:
    LAYER3_AVAILABLE = False


@app.get("/api/v3/status")
async def api_v3_status():
    """Get 3-layer chain status."""
    if not LAYER3_AVAILABLE:
        return {"error": "3-layer module not available"}

    status = get_chain_status_3layer()
    status["server_instance"] = SERVER_INSTANCE
    return status


@app.get("/api/v3/chain/recent")
async def api_v3_chain_recent(limit: int = 10):
    """Get recent stones from 3-layer chain."""
    if not LAYER3_AVAILABLE:
        return {"error": "3-layer module not available"}

    return {
        "stones": get_recent_stones_3layer(limit),
        "server_instance": SERVER_INSTANCE
    }


@app.get("/api/v3/stone/{stone_id}")
async def api_v3_get_stone(stone_id: str, layer: int = 1):
    """
    Get stone data by layer.

    - layer=0: Chain entry only (~500 bytes)
    - layer=1: Full manifest (~5KB)
    """
    if not LAYER3_AVAILABLE:
        return {"error": "3-layer module not available"}

    if layer == 0:
        entry = get_chain_entry(stone_id)
        if not entry:
            return {"error": "Stone not found"}
        return {
            "layer": 0,
            "data": entry.to_dict(),
            "server_instance": SERVER_INSTANCE
        }
    else:
        manifest = get_manifest(stone_id)
        if not manifest:
            return {"error": "Manifest not found"}
        return {
            "layer": 1,
            "data": manifest.to_dict(),
            "server_instance": SERVER_INSTANCE
        }


@app.get("/api/v3/stone/{stone_id}/wormhole/{layer_num}")
async def api_v3_traverse_wormhole(stone_id: str, layer_num: int):
    """
    Traverse a wormhole to retrieve content from Layer 2+.

    Returns the content with verification.
    """
    if not LAYER3_AVAILABLE:
        return {"error": "3-layer module not available"}

    manifest = get_manifest(stone_id)
    if not manifest:
        return {"error": "Manifest not found"}

    # Find wormhole for this layer
    wormhole = None
    for wh in manifest.wormholes:
        if wh.layer == layer_num:
            wormhole = wh
            break

    if not wormhole:
        return {"error": f"No wormhole for layer {layer_num}"}

    try:
        content = traverse_wormhole(wormhole)

        # Try to decode as text if it's a message
        if wormhole.content_type == "message":
            try:
                content_str = content.decode('utf-8')
                return {
                    "layer": layer_num,
                    "content_type": wormhole.content_type,
                    "content": content_str,
                    "size_bytes": len(content),
                    "verified": True,
                    "server_instance": SERVER_INSTANCE
                }
            except UnicodeDecodeError:
                pass

        # Return as base64 for binary content
        import base64
        return {
            "layer": layer_num,
            "content_type": wormhole.content_type,
            "content_base64": base64.b64encode(content).decode('ascii'),
            "size_bytes": len(content),
            "verified": True,
            "server_instance": SERVER_INSTANCE
        }

    except Exception as e:
        return {"error": str(e)}


@app.post("/api/v3/verify/{stone_id}")
async def api_v3_verify_stone(stone_id: str, full: bool = False):
    """
    Verify a 3-layer stone.

    - full=False: Only verify border hash (fast)
    - full=True: Verify all content layer hashes (slower)
    """
    if not LAYER3_AVAILABLE:
        return {"error": "3-layer module not available"}

    result = verify_stone_3layer(stone_id, full_verification=full)
    result["server_instance"] = SERVER_INSTANCE
    return result


@app.post("/api/v3/message")
async def api_v3_send_message(
    from_token: str,
    to_username: str,
    message: str,
    attachment_url: Optional[str] = None
):
    """
    Send a message as a 3-layer QA.Stone.

    This creates a cryptographically signed message with:
    - Layer 0: Chain entry (for ordering)
    - Layer 1: Manifest (metadata + wormholes)
    - Layer 2: Message content
    - Layer 3+: Attachments (if any)
    """
    if not LAYER3_AVAILABLE:
        return {"error": "3-layer module not available"}

    # Authenticate
    from_user_id = authenticate(from_token)
    if not from_user_id:
        return {"error": "Invalid token"}

    to_user_id = generate_user_id(to_username)

    # Create 3-layer message
    stone = create_3layer_message(
        from_user=from_user_id,
        to_user=to_user_id,
        message=message,
        server_instance=SERVER_INSTANCE
    )

    # Store it
    result = store_stone_3layer(stone)

    if result["success"]:
        # Notify recipient via WebSocket
        await ws_manager.notify_user(to_user_id, {
            "type": "v3_message_received",
            "stone_id": stone.stone_id,
            "border_hash": stone.border_hash[:16] + "...",
            "from": from_user_id,
            "glow_channel": "message",
            "message": f"New message from {from_user_id}"
        })

        result["stone"] = {
            "stone_id": stone.stone_id,
            "border_hash": stone.border_hash,
            "sequence": stone.chain_entry.sequence,
            "layers": len(stone.manifest.wormholes) + 2
        }

    result["server_instance"] = SERVER_INSTANCE
    return result


@app.post("/api/v3/transfer")
async def api_v3_transfer(
    from_token: str,
    to_username: str,
    stone_id: str,
    message: Optional[str] = None
):
    """
    Transfer a stone with optional message using 3-layer architecture.
    """
    if not LAYER3_AVAILABLE:
        return {"error": "3-layer module not available"}

    # Authenticate
    from_user_id = authenticate(from_token)
    if not from_user_id:
        return {"error": "Invalid token"}

    to_user_id = generate_user_id(to_username)

    # Create 3-layer transfer
    transfer_stone = create_3layer_transfer(
        from_user=from_user_id,
        to_user=to_user_id,
        stone_id=stone_id,
        message=message,
        server_instance=SERVER_INSTANCE
    )

    # Store it
    result = store_stone_3layer(transfer_stone)

    if result["success"]:
        # Notify recipient
        await ws_manager.notify_user(to_user_id, {
            "type": "v3_transfer_received",
            "transfer_stone_id": transfer_stone.stone_id,
            "original_stone_id": stone_id,
            "border_hash": transfer_stone.border_hash[:16] + "...",
            "from": from_user_id,
            "has_message": message is not None,
            "message": f"Stone transfer from {from_user_id}"
        })

        result["transfer"] = {
            "transfer_stone_id": transfer_stone.stone_id,
            "original_stone_id": stone_id,
            "border_hash": transfer_stone.border_hash,
            "sequence": transfer_stone.chain_entry.sequence
        }

    result["server_instance"] = SERVER_INSTANCE
    return result


@app.get("/api/v3/sync/{stone_id}")
async def api_v3_get_sync_payload(stone_id: str):
    """
    Get sync payload for twin synchronization.

    This returns the minimal data needed for another twin
    to register this stone in its chain.
    """
    if not LAYER3_AVAILABLE:
        return {"error": "3-layer module not available"}

    payload = get_sync_payload(stone_id)
    if not payload:
        return {"error": "Stone not found"}

    payload["source_instance"] = SERVER_INSTANCE
    return payload


@app.post("/api/v3/sync")
async def api_v3_apply_sync(payload: Dict[str, Any]):
    """
    Apply sync payload from another twin.

    This registers a stone in the local chain without
    having the full manifest (lazy loading).
    """
    if not LAYER3_AVAILABLE:
        return {"error": "3-layer module not available"}

    result = apply_sync_payload(payload)
    result["applied_by"] = SERVER_INSTANCE
    return result


@app.get("/api/v3/info")
async def api_v3_info():
    """Information about the 3-layer QA.Stone architecture."""
    return {
        "version": "3.0.0",
        "architecture": "3-layer",
        "available": LAYER3_AVAILABLE,
        "description": "QA.Stone 3D Cantor Lattice with Stochastic Wormholes",
        "layers": {
            "0": {
                "name": "Chain",
                "storage": "Redis",
                "size": "~500 bytes/stone",
                "purpose": "Global ordering, twin sync"
            },
            "1": {
                "name": "Manifest",
                "storage": "Redis",
                "size": "~2-10KB/stone",
                "purpose": "Metadata, MCP message, wormhole addresses"
            },
            "2+": {
                "name": "Content",
                "storage": "Local/S3/IPFS",
                "size": "Unlimited",
                "purpose": "Message text, images, videos, websites"
            }
        },
        "scaling": {
            "chain": "1M stones = ~500MB Redis",
            "manifests": "1M stones = ~5GB",
            "content": "CDN-cacheable, unlimited"
        },
        "endpoints": {
            "GET /api/v3/status": "Chain status",
            "GET /api/v3/stone/{id}": "Get stone by layer",
            "GET /api/v3/stone/{id}/wormhole/{layer}": "Traverse wormhole",
            "POST /api/v3/message": "Send message",
            "POST /api/v3/transfer": "Transfer stone",
            "POST /api/v3/verify/{id}": "Verify stone",
            "GET /api/v3/sync/{id}": "Get sync payload",
            "POST /api/v3/sync": "Apply sync payload"
        },
        "server_instance": SERVER_INSTANCE
    }


# =============================================================================
# LIVE STREAMING API (Chunked HLS-style)
# =============================================================================

try:
    from qastone_streaming import (
        LiveStreamSession,
        StreamPlayer,
        StreamStatus,
        list_live_streams,
        list_recent_streams,
        get_stream_manifest,
        create_stream,
        start_stream,
        end_stream,
        add_stream_chunk,
    )
    STREAMING_AVAILABLE = True
except ImportError:
    STREAMING_AVAILABLE = False


@app.get("/api/stream/live")
async def api_stream_list_live():
    """List all currently live streams."""
    if not STREAMING_AVAILABLE:
        return {"error": "Streaming module not available"}

    streams = list_live_streams()
    return {
        "live_streams": streams,
        "count": len(streams),
        "server_instance": SERVER_INSTANCE
    }


@app.get("/api/stream/recent")
async def api_stream_list_recent(limit: int = 10):
    """List recent streams (live and ended)."""
    if not STREAMING_AVAILABLE:
        return {"error": "Streaming module not available"}

    streams = list_recent_streams(limit)
    return {
        "streams": streams,
        "count": len(streams),
        "server_instance": SERVER_INSTANCE
    }


@app.post("/api/stream/create")
async def api_stream_create(
    streamer_token: str,
    title: str,
    description: str = "",
    codec: str = "h264",
    resolution: str = "1280x720",
    bitrate_kbps: int = 2500,
    chunk_duration_ms: int = 2000
):
    """
    Create a new live stream session.

    Returns session_id to use for adding chunks.
    """
    if not STREAMING_AVAILABLE:
        return {"error": "Streaming module not available"}

    # Authenticate streamer
    streamer_id = authenticate(streamer_token)
    if not streamer_id:
        return {"error": "Invalid token"}

    session = create_stream(
        streamer_id=streamer_id,
        title=title,
        description=description,
        codec=codec,
        resolution=resolution,
        bitrate_kbps=bitrate_kbps,
        chunk_duration_ms=chunk_duration_ms,
        server_instance=SERVER_INSTANCE
    )

    return {
        "success": True,
        "session_id": session.session_id,
        "status": session.metadata.status,
        "created_at": session.metadata.created_at,
        "server_instance": SERVER_INSTANCE
    }


@app.get("/api/stream/info")
async def api_stream_system_info():
    """Information about the streaming system."""
    return {
        "version": "1.0.0",
        "available": STREAMING_AVAILABLE,
        "description": "QA.Stone Chunked Live Streaming (HLS-style)",
        "architecture": {
            "method": "chunked",
            "chunk_duration": "2-4 seconds (configurable)",
            "latency": "4-10 seconds (comparable to HLS)",
            "storage": "Each chunk = QA.Stone with full provenance"
        },
        "features": {
            "cryptographic_provenance": "Every chunk has border_hash",
            "chain_ordering": "Chunks linked via sequence numbers",
            "twin_sync": "Streams replicated across twins",
            "verified_playback": "Content hash verified on retrieval"
        },
        "use_cases": [
            "Webinars with verified speaker identity",
            "Product launches with cryptographic timestamps",
            "Legal depositions with tamper-proof recording",
            "Live events where provenance matters"
        ],
        "endpoints": {
            "GET /api/stream/info": "System info (this endpoint)",
            "GET /api/stream/live": "List live streams",
            "GET /api/stream/recent": "List recent streams",
            "POST /api/stream/create": "Create new stream",
            "POST /api/stream/{id}/start": "Start streaming",
            "POST /api/stream/{id}/chunk": "Add video chunk",
            "POST /api/stream/{id}/end": "End stream",
            "GET /api/stream/{id}": "Stream info",
            "GET /api/stream/{id}/manifest": "HLS-style manifest",
            "GET /api/stream/{id}/chunk/{seq}": "Get chunk data"
        },
        "server_instance": SERVER_INSTANCE
    }


@app.post("/api/stream/{session_id}/start")
async def api_stream_start(session_id: str, streamer_token: str):
    """Start a stream (must be the owner)."""
    if not STREAMING_AVAILABLE:
        return {"error": "Streaming module not available"}

    streamer_id = authenticate(streamer_token)
    if not streamer_id:
        return {"error": "Invalid token"}

    session = LiveStreamSession.load(session_id)
    if not session:
        return {"error": "Stream not found"}

    if session.metadata.streamer_id != streamer_id:
        return {"error": "Not authorized"}

    result = session.start()

    if result.get("success"):
        # Broadcast stream started
        await ws_manager.broadcast({
            "type": "stream_started",
            "session_id": session_id,
            "title": session.metadata.title,
            "streamer_id": streamer_id,
            "server_instance": SERVER_INSTANCE
        })

    result["server_instance"] = SERVER_INSTANCE
    return result


@app.post("/api/stream/{session_id}/chunk")
async def api_stream_add_chunk(
    session_id: str,
    streamer_token: str,
    duration_ms: int = 2000,
    keyframe: bool = False
):
    """
    Add a video chunk to the stream.

    Send video data as request body (application/octet-stream).
    """
    if not STREAMING_AVAILABLE:
        return {"error": "Streaming module not available"}

    streamer_id = authenticate(streamer_token)
    if not streamer_id:
        return {"error": "Invalid token"}

    session = LiveStreamSession.load(session_id)
    if not session:
        return {"error": "Stream not found"}

    if session.metadata.streamer_id != streamer_id:
        return {"error": "Not authorized"}

    # For demo, generate fake video data
    # In production, this would come from request body
    import secrets
    video_data = secrets.token_bytes(5000)

    result = add_stream_chunk(
        session_id=session_id,
        video_data=video_data,
        duration_ms=duration_ms,
        keyframe=keyframe
    )

    if result.get("success"):
        # Notify viewers of new chunk
        await ws_manager.broadcast({
            "type": "stream_chunk",
            "session_id": session_id,
            "sequence": result["sequence"],
            "stone_id": result["stone_id"],
            "server_instance": SERVER_INSTANCE
        })

    result["server_instance"] = SERVER_INSTANCE
    return result


@app.post("/api/stream/{session_id}/end")
async def api_stream_end(session_id: str, streamer_token: str):
    """End a stream."""
    if not STREAMING_AVAILABLE:
        return {"error": "Streaming module not available"}

    streamer_id = authenticate(streamer_token)
    if not streamer_id:
        return {"error": "Invalid token"}

    session = LiveStreamSession.load(session_id)
    if not session:
        return {"error": "Stream not found"}

    if session.metadata.streamer_id != streamer_id:
        return {"error": "Not authorized"}

    result = session.end()

    if result.get("success"):
        await ws_manager.broadcast({
            "type": "stream_ended",
            "session_id": session_id,
            "total_chunks": result["total_chunks"],
            "total_duration_ms": result["total_duration_ms"],
            "server_instance": SERVER_INSTANCE
        })

    result["server_instance"] = SERVER_INSTANCE
    return result


@app.get("/api/stream/{session_id}")
async def api_stream_info(session_id: str):
    """Get stream information."""
    if not STREAMING_AVAILABLE:
        return {"error": "Streaming module not available"}

    session = LiveStreamSession.load(session_id)
    if not session:
        return {"error": "Stream not found"}

    return {
        "session_id": session.session_id,
        "title": session.metadata.title,
        "description": session.metadata.description,
        "streamer_id": session.metadata.streamer_id,
        "status": session.metadata.status,
        "codec": session.metadata.codec,
        "resolution": session.metadata.resolution,
        "bitrate_kbps": session.metadata.bitrate_kbps,
        "chunk_count": session.metadata.chunk_count,
        "total_duration_ms": session.metadata.total_duration_ms,
        "created_at": session.metadata.created_at,
        "started_at": session.metadata.started_at,
        "ended_at": session.metadata.ended_at,
        "server_instance": SERVER_INSTANCE
    }


@app.get("/api/stream/{session_id}/manifest")
async def api_stream_manifest(session_id: str):
    """
    Get HLS-style manifest for a stream.

    Returns list of chunks with their stone_ids for playback.
    """
    if not STREAMING_AVAILABLE:
        return {"error": "Streaming module not available"}

    manifest = get_stream_manifest(session_id)
    if not manifest:
        return {"error": "Stream not found"}

    manifest["server_instance"] = SERVER_INSTANCE
    return manifest


@app.get("/api/stream/{session_id}/chunk/{sequence}")
async def api_stream_get_chunk(session_id: str, sequence: int):
    """
    Get a specific chunk's video data.

    Returns the video bytes (base64 encoded) with verification info.
    """
    if not STREAMING_AVAILABLE:
        return {"error": "Streaming module not available"}

    player = StreamPlayer(session_id)

    try:
        chunk_data = player.get_chunk(sequence)
        if not chunk_data:
            return {"error": f"Chunk {sequence} not found"}

        import base64
        return {
            "session_id": session_id,
            "sequence": sequence,
            "size_bytes": len(chunk_data),
            "data_base64": base64.b64encode(chunk_data).decode('ascii'),
            "verified": True,
            "server_instance": SERVER_INSTANCE
        }
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# CONTEXT MANAGEMENT API (LLM Context with V4 Compression)
# =============================================================================

try:
    from qastone_context import (
        QAStoneContext,
        ContextChain,
        compress_for_llm,
        decompress_v4,
        embed_text,
        semantic_similarity,
        extract_concepts,
        find_stones_by_concept,
    )
    CONTEXT_AVAILABLE = True
except ImportError:
    CONTEXT_AVAILABLE = False


@app.post("/api/context/compress")
async def api_context_compress(text: str, detail: str = "compressed"):
    """
    Compress text for LLM context injection.

    Args:
        text: Text to compress
        detail: "full", "compressed", "concepts"

    Returns V4 compressed version with token estimates.
    """
    if not CONTEXT_AVAILABLE:
        return {"error": "Context module not available"}

    stone = QAStoneContext.create(content=text)

    return {
        "original_tokens": stone.token_estimate["full"],
        "compressed_tokens": stone.token_estimate["v4_compressed"],
        "savings_percent": round((1 - stone.compression_ratio) * 100, 1),
        "detail": detail,
        "content": stone.get_for_llm(detail),
        "concepts": stone.layers.concepts[:10],
        "border_hash": stone.border_hash[:16] + "...",
        "server_instance": SERVER_INSTANCE
    }


@app.post("/api/context/decompress")
async def api_context_decompress(v4_text: str):
    """Decompress V4 format back to readable text."""
    if not CONTEXT_AVAILABLE:
        return {"error": "Context module not available"}

    decompressed = decompress_v4(v4_text)
    return {
        "original": v4_text,
        "decompressed": decompressed,
        "server_instance": SERVER_INSTANCE
    }


@app.post("/api/context/embed")
async def api_context_embed(text: str):
    """Get embedding vector for text (for semantic search)."""
    if not CONTEXT_AVAILABLE:
        return {"error": "Context module not available"}

    embedding = embed_text(text)
    return {
        "text_preview": text[:100] + "..." if len(text) > 100 else text,
        "embedding_dim": len(embedding),
        "embedding": embedding[:10],  # First 10 dims for preview
        "embedding_hash": hashlib.sha256(str(embedding).encode()).hexdigest()[:16],
        "server_instance": SERVER_INSTANCE
    }


@app.post("/api/context/similarity")
async def api_context_similarity(text1: str, text2: str):
    """Compute semantic similarity between two texts."""
    if not CONTEXT_AVAILABLE:
        return {"error": "Context module not available"}

    similarity = semantic_similarity(text1, text2)
    return {
        "text1_preview": text1[:50] + "..." if len(text1) > 50 else text1,
        "text2_preview": text2[:50] + "..." if len(text2) > 50 else text2,
        "similarity": round(similarity, 4),
        "interpretation": (
            "very similar" if similarity > 0.8 else
            "similar" if similarity > 0.6 else
            "somewhat related" if similarity > 0.4 else
            "weakly related" if similarity > 0.2 else
            "unrelated"
        ),
        "server_instance": SERVER_INSTANCE
    }


@app.post("/api/context/concepts")
async def api_context_extract_concepts(text: str, max_concepts: int = 20):
    """Extract key concepts from text."""
    if not CONTEXT_AVAILABLE:
        return {"error": "Context module not available"}

    concepts = extract_concepts(text, max_concepts)
    return {
        "text_preview": text[:100] + "..." if len(text) > 100 else text,
        "concepts": concepts,
        "count": len(concepts),
        "server_instance": SERVER_INSTANCE
    }


@app.post("/api/context/chain/create")
async def api_context_chain_create(session_id: Optional[str] = None):
    """Create a new context chain for conversation management."""
    if not CONTEXT_AVAILABLE:
        return {"error": "Context module not available"}

    chain = ContextChain.create(session_id=session_id)
    return {
        "success": True,
        "chain_id": chain.chain_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "server_instance": SERVER_INSTANCE
    }


@app.post("/api/context/chain/{chain_id}/turn")
async def api_context_chain_add_turn(
    chain_id: str,
    role: str,
    content: str
):
    """
    Add a conversation turn to the chain.

    Args:
        chain_id: Chain to add to
        role: "user", "assistant", "system"
        content: Turn content
    """
    if not CONTEXT_AVAILABLE:
        return {"error": "Context module not available"}

    chain = ContextChain.load(chain_id)
    if not chain:
        # Create new chain if doesn't exist
        chain = ContextChain.create(session_id=chain_id)

    stone = chain.add_turn(role=role, content=content)

    return {
        "success": True,
        "context_id": stone.context_id,
        "sequence": stone.sequence,
        "tokens_full": stone.token_estimate["full"],
        "tokens_v4": stone.token_estimate["v4_compressed"],
        "compression_ratio": round(stone.compression_ratio, 3),
        "concepts": stone.layers.concepts[:5],
        "border_hash": stone.border_hash[:16] + "...",
        "server_instance": SERVER_INSTANCE
    }


@app.get("/api/context/chain/{chain_id}")
async def api_context_chain_get(chain_id: str):
    """Get chain information and statistics."""
    if not CONTEXT_AVAILABLE:
        return {"error": "Context module not available"}

    chain = ContextChain.load(chain_id)
    if not chain:
        return {"error": "Chain not found"}

    stats = chain.get_stats()
    integrity = chain.verify_integrity()

    return {
        "chain_id": chain.chain_id,
        "turns": len(chain.stones),
        "stats": stats,
        "integrity": integrity,
        "server_instance": SERVER_INSTANCE
    }


@app.post("/api/context/chain/{chain_id}/retrieve")
async def api_context_chain_retrieve(
    chain_id: str,
    query: str,
    max_tokens: int = 4000,
    detail: str = "compressed"
):
    """
    Retrieve relevant context from chain within token budget.

    Uses semantic similarity + recency to select turns.

    Args:
        chain_id: Chain to search
        query: Query to find relevant context for
        max_tokens: Maximum tokens to return
        detail: "full", "compressed", "concepts"
    """
    if not CONTEXT_AVAILABLE:
        return {"error": "Context module not available"}

    chain = ContextChain.load(chain_id)
    if not chain:
        return {"error": "Chain not found"}

    context = chain.get_relevant_context(
        query=query,
        max_tokens=max_tokens,
        detail=detail
    )

    # Estimate actual tokens returned
    actual_tokens = len(context) // 4

    return {
        "chain_id": chain_id,
        "query": query[:50] + "..." if len(query) > 50 else query,
        "max_tokens": max_tokens,
        "actual_tokens": actual_tokens,
        "detail": detail,
        "context": context,
        "server_instance": SERVER_INSTANCE
    }


@app.get("/api/context/chain/{chain_id}/summary")
async def api_context_chain_summary(chain_id: str):
    """Get V4 compressed summary of entire chain."""
    if not CONTEXT_AVAILABLE:
        return {"error": "Context module not available"}

    chain = ContextChain.load(chain_id)
    if not chain:
        return {"error": "Chain not found"}

    summary = chain.to_v4_summary()
    stats = chain.get_stats()

    return {
        "chain_id": chain_id,
        "turns": len(chain.stones),
        "full_tokens": stats["total_tokens_full"],
        "summary_tokens": len(summary) // 4,
        "compression": f"{round((1 - len(summary) / max(1, stats['total_tokens_full'] * 4)) * 100, 1)}%",
        "summary": summary,
        "server_instance": SERVER_INSTANCE
    }


@app.get("/api/context/chain/{chain_id}/turn/{sequence}")
async def api_context_chain_get_turn(
    chain_id: str,
    sequence: int,
    detail: str = "compressed"
):
    """Get a specific turn from the chain."""
    if not CONTEXT_AVAILABLE:
        return {"error": "Context module not available"}

    chain = ContextChain.load(chain_id)
    if not chain:
        return {"error": "Chain not found"}

    stone = chain.get_turn(sequence)
    if not stone:
        return {"error": f"Turn {sequence} not found"}

    return {
        "chain_id": chain_id,
        "context_id": stone.context_id,
        "sequence": stone.sequence,
        "role": stone.role,
        "content": stone.get_for_llm(detail),
        "tokens": stone.token_estimate.get(
            "v4_compressed" if detail == "compressed" else "full",
            stone.token_estimate["full"]
        ),
        "concepts": stone.layers.concepts,
        "border_hash": stone.border_hash[:16] + "...",
        "server_instance": SERVER_INSTANCE
    }


@app.get("/api/context/search")
async def api_context_search_by_concept(concept: str):
    """Find context stones containing a specific concept."""
    if not CONTEXT_AVAILABLE:
        return {"error": "Context module not available"}

    stone_refs = find_stones_by_concept(concept)

    return {
        "concept": concept,
        "matches": len(stone_refs),
        "stone_refs": list(stone_refs)[:50],  # Limit to 50
        "server_instance": SERVER_INSTANCE
    }


@app.get("/api/context/info")
async def api_context_info():
    """Information about the context management system."""
    return {
        "version": "1.0.0",
        "available": CONTEXT_AVAILABLE,
        "description": "QA.Stone Context Management with V4 Compression",
        "layers": {
            "2": {"name": "Full", "description": "Original text, full fidelity"},
            "3": {"name": "V4 Compressed", "description": "~85% token reduction"},
            "4": {"name": "Embedding", "description": "384-dim vector for semantic search"},
            "5": {"name": "Concepts", "description": "Key terms only, ~98% reduction"}
        },
        "token_savings": {
            "v4_compression": "~85%",
            "with_relevance_filter": "~95%",
            "concepts_only": "~98%"
        },
        "features": [
            "Multi-layer compression (full → V4 → concepts)",
            "Semantic similarity search",
            "Concept-based retrieval",
            "Token budget management",
            "Chain integrity verification",
            "Conversation history as verifiable chain"
        ],
        "endpoints": {
            "POST /api/context/compress": "Compress text for LLM",
            "POST /api/context/decompress": "Decompress V4 format",
            "POST /api/context/embed": "Get embedding vector",
            "POST /api/context/similarity": "Compute semantic similarity",
            "POST /api/context/concepts": "Extract key concepts",
            "POST /api/context/chain/create": "Create conversation chain",
            "POST /api/context/chain/{id}/turn": "Add turn to chain",
            "GET /api/context/chain/{id}": "Get chain info",
            "POST /api/context/chain/{id}/retrieve": "Get relevant context",
            "GET /api/context/chain/{id}/summary": "V4 summary of chain",
            "GET /api/context/search": "Search by concept"
        },
        "server_instance": SERVER_INSTANCE
    }


# =============================================================================
# GRAPH LOD API (Level of Detail for Large Graphs)
# =============================================================================

try:
    from qastone_graph import (
        GraphStone,
        store_graph_stone,
        load_graph_stone,
        list_graphs,
        generate_demo_graph,
    )
    GRAPH_AVAILABLE = True
except ImportError:
    GRAPH_AVAILABLE = False


@app.get("/api/graph/list")
async def api_graph_list():
    """List all stored graphs."""
    if not GRAPH_AVAILABLE:
        return {"error": "Graph module not available"}

    graphs = list_graphs()
    return {
        "graphs": graphs,
        "count": len(graphs),
        "server_instance": SERVER_INSTANCE
    }


@app.post("/api/graph/create")
async def api_graph_create(
    name: str = "Graph",
    cluster_count: int = 100,
    region_size: int = 500
):
    """
    Create a graph stone from posted JSON data.

    Expects JSON body with:
        nodes: [{id, x, y, label?, size?, color?}, ...]
        edges: [{source, target, weight?}, ...]
    """
    if not GRAPH_AVAILABLE:
        return {"error": "Graph module not available"}

    # For demo, generate sample data
    # In production, parse from request body
    return {
        "info": "Use /api/graph/demo to create a demo graph, or POST nodes/edges JSON",
        "server_instance": SERVER_INSTANCE
    }


@app.post("/api/graph/demo")
async def api_graph_create_demo(
    node_count: int = 1000,
    edge_density: float = 0.003,
    name: str = "Demo Graph",
    cluster_count: int = 50,
    region_size: int = 200
):
    """
    Create a demo graph with random clustered data.

    Good for testing LOD performance.
    """
    if not GRAPH_AVAILABLE:
        return {"error": "Graph module not available"}

    # Generate demo data
    nodes, edges = generate_demo_graph(
        node_count=node_count,
        edge_density=edge_density
    )

    # Build graph stone
    stone = GraphStone.from_nodes_edges(
        nodes=nodes,
        edges=edges,
        name=name,
        cluster_count=cluster_count,
        region_size=region_size,
        server_instance=SERVER_INSTANCE
    )

    # Store it
    result = store_graph_stone(stone)

    if result.get("success"):
        return {
            "success": True,
            "graph_id": stone.graph_id,
            "name": stone.metadata.name,
            "node_count": stone.metadata.node_count,
            "edge_count": stone.metadata.edge_count,
            "cluster_count": stone.metadata.cluster_count,
            "region_count": stone.metadata.region_count,
            "border_hash": stone.metadata.border_hash[:16] + "...",
            "server_instance": SERVER_INSTANCE
        }

    return result


@app.get("/api/graph/{graph_id}/overview")
async def api_graph_overview(graph_id: str):
    """
    Get LOD 5 overview (clusters only).

    This is the FAST initial render endpoint - <100ms for any graph size.
    Returns cluster centroids and bundled inter-cluster edges.
    """
    if not GRAPH_AVAILABLE:
        return {"error": "Graph module not available"}

    stone = load_graph_stone(graph_id)
    if not stone:
        return {"error": "Graph not found"}

    overview = stone.get_overview()
    overview["server_instance"] = SERVER_INSTANCE
    return overview


@app.get("/api/graph/{graph_id}/regions")
async def api_graph_regions(graph_id: str):
    """
    Get LOD 4 quadtree regions.

    Returns spatial region bounds for viewport intersection testing.
    """
    if not GRAPH_AVAILABLE:
        return {"error": "Graph module not available"}

    stone = load_graph_stone(graph_id)
    if not stone:
        return {"error": "Graph not found"}

    regions = stone.get_regions()
    regions["server_instance"] = SERVER_INSTANCE
    return regions


@app.get("/api/graph/{graph_id}/viewport")
async def api_graph_viewport(
    graph_id: str,
    x1: float,
    y1: float,
    x2: float,
    y2: float,
    lod: int = 3,
    max_nodes: int = 2000
):
    """
    Get nodes and edges visible in viewport at specified LOD.

    Only loads regions that intersect the viewport bounds.

    Args:
        x1, y1, x2, y2: Viewport bounds
        lod: Detail level (2=full, 3=compressed, 4=regions, 5=clusters)
        max_nodes: Maximum nodes to return
    """
    if not GRAPH_AVAILABLE:
        return {"error": "Graph module not available"}

    stone = load_graph_stone(graph_id)
    if not stone:
        return {"error": "Graph not found"}

    viewport = stone.get_viewport(
        bounds=(x1, y1, x2, y2),
        lod=lod,
        max_nodes=max_nodes
    )
    viewport["server_instance"] = SERVER_INSTANCE
    return viewport


@app.get("/api/graph/{graph_id}/node/{node_id}")
async def api_graph_node_detail(graph_id: str, node_id: str):
    """
    Get full detail for a single node (LOD 2).

    Includes all edges and complete metadata.
    """
    if not GRAPH_AVAILABLE:
        return {"error": "Graph module not available"}

    stone = load_graph_stone(graph_id)
    if not stone:
        return {"error": "Graph not found"}

    detail = stone.get_node_detail(node_id)
    if not detail:
        return {"error": "Node not found"}

    detail["server_instance"] = SERVER_INSTANCE
    return detail


@app.get("/api/graph/{graph_id}/neighbors/{node_id}")
async def api_graph_neighbors(
    graph_id: str,
    node_id: str,
    depth: int = 1,
    max_nodes: int = 100
):
    """
    Get neighborhood of a node up to specified depth.
    """
    if not GRAPH_AVAILABLE:
        return {"error": "Graph module not available"}

    stone = load_graph_stone(graph_id)
    if not stone:
        return {"error": "Graph not found"}

    neighbors = stone.get_neighbors(node_id, depth=depth, max_nodes=max_nodes)
    neighbors["server_instance"] = SERVER_INSTANCE
    return neighbors


@app.get("/api/graph/{graph_id}/search")
async def api_graph_search(graph_id: str, query: str, limit: int = 50):
    """Search for nodes by label/ID."""
    if not GRAPH_AVAILABLE:
        return {"error": "Graph module not available"}

    stone = load_graph_stone(graph_id)
    if not stone:
        return {"error": "Graph not found"}

    results = stone.search_nodes(query, limit=limit)
    return {
        "graph_id": graph_id,
        "query": query,
        "results": results,
        "count": len(results),
        "server_instance": SERVER_INSTANCE
    }


@app.get("/api/graph/info")
async def api_graph_info():
    """Information about the graph LOD system."""
    return {
        "version": "1.0.0",
        "available": GRAPH_AVAILABLE,
        "description": "QA.Stone Graph LOD for Progressive Loading",
        "lod_levels": {
            "5": {
                "name": "Clusters",
                "description": "K-means cluster centroids (~100 nodes)",
                "render_time": "<100ms",
                "use_case": "Initial render, overview"
            },
            "4": {
                "name": "Regions",
                "description": "Quadtree spatial regions (~1K nodes)",
                "render_time": "<200ms",
                "use_case": "Zoomed out view"
            },
            "3": {
                "name": "Compressed",
                "description": "All nodes with minimal metadata",
                "render_time": "<500ms",
                "use_case": "Standard interaction"
            },
            "2": {
                "name": "Full",
                "description": "Complete node and edge data",
                "render_time": "1-5s for large graphs",
                "use_case": "Node detail, export"
            }
        },
        "features": [
            "Progressive loading (clusters first)",
            "Spatial partitioning (quadtree)",
            "Edge bundling at low LOD",
            "Viewport culling (load visible only)",
            "Border hash verification per graph"
        ],
        "endpoints": {
            "POST /api/graph/demo": "Create demo graph",
            "GET /api/graph/list": "List all graphs",
            "GET /api/graph/{id}/overview": "LOD 5 clusters (fast)",
            "GET /api/graph/{id}/regions": "LOD 4 quadtree",
            "GET /api/graph/{id}/viewport": "Get visible nodes at LOD",
            "GET /api/graph/{id}/node/{nid}": "Full node detail",
            "GET /api/graph/{id}/neighbors/{nid}": "Node neighborhood",
            "GET /api/graph/{id}/search": "Search nodes"
        },
        "server_instance": SERVER_INSTANCE
    }


# =============================================================================
# FEDERATION API - Registry, Stones, Transport
# =============================================================================

# Pydantic models for federation
class WalletRegistration(BaseModel):
    alias: str
    wallet_hash: str
    public_key: str
    stones_url: Optional[str] = None
    inbox_url: Optional[str] = None

class StonePublish(BaseModel):
    stone: Dict[str, Any]

class InboxMessage(BaseModel):
    envelope: Dict[str, Any]

# Redis keys for federation
WALLET_PREFIX = "federation:wallet:"
STONES_PREFIX = "federation:stones:"
INBOX_PREFIX = "federation:inbox:"
PEERS_KEY = "federation:peers"


@app.post("/api/v1/registry/wallet")
async def register_wallet(registration: WalletRegistration):
    """Register a wallet in the federated registry."""
    try:
        from redis_accounts import get_redis
        r = get_redis()

        wallet_data = {
            "alias": registration.alias,
            "wallet_hash": registration.wallet_hash,
            "public_key": registration.public_key,
            "stones_url": registration.stones_url or f"/api/v1/stones/{registration.wallet_hash}",
            "inbox_url": registration.inbox_url or f"/api/v1/inbox/{registration.wallet_hash}",
            "registered_at": datetime.now(timezone.utc).isoformat(),
            "registry": SERVER_INSTANCE
        }

        r.hset(f"{WALLET_PREFIX}{registration.wallet_hash}", mapping=wallet_data)
        r.sadd("federation:wallets", registration.wallet_hash)

        return {
            "ok": True,
            "registered": f"{registration.alias}@{registration.wallet_hash}",
            "registry": SERVER_INSTANCE
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/registry/wallet/{wallet_hash}")
async def resolve_wallet(wallet_hash: str):
    """Resolve a wallet address to its details."""
    try:
        from redis_accounts import get_redis
        r = get_redis()

        wallet_data = r.hgetall(f"{WALLET_PREFIX}{wallet_hash}")

        if not wallet_data:
            # Try to find by alias
            for wh in r.smembers("federation:wallets"):
                data = r.hgetall(f"{WALLET_PREFIX}{wh}")
                if data.get("alias") == wallet_hash:
                    wallet_data = data
                    wallet_hash = wh
                    break

        if not wallet_data:
            raise HTTPException(status_code=404, detail=f"Wallet not found: {wallet_hash}")

        return {
            "ok": True,
            "resolved": True,
            **wallet_data,
            "address": f"{wallet_data.get('alias', 'unknown')}@{wallet_hash}"
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/stones/publish")
async def publish_stone(data: StonePublish):
    """Publish a stone to the registry."""
    try:
        from redis_accounts import get_redis
        r = get_redis()

        stone = data.stone
        author = stone.get("border", {}).get("author", "")
        stone_hash = stone.get("border", {}).get("hash", "")

        if "@" in author:
            wallet_hash = author.split("@")[1]
        else:
            raise HTTPException(status_code=400, detail="Invalid author format")

        stone_id = f"{stone.get('type', 'unknown')}_{stone_hash}"

        r.hset(f"{STONES_PREFIX}{wallet_hash}:{stone_id}", mapping={
            "stone_id": stone_id,
            "hash": stone_hash,
            "type": stone.get("type", "unknown"),
            "glow": stone.get("border", {}).get("glow", ""),
            "author": author,
            "created": stone.get("border", {}).get("created", ""),
            "lod5": stone.get("layers", {}).get("lod5", {}).get("content", "")[:200],
            "stone_json": json.dumps(stone)
        })

        r.sadd(f"{STONES_PREFIX}{wallet_hash}:index", stone_id)

        return {
            "ok": True,
            "published": stone_id,
            "hash": stone_hash,
            "author": author,
            "registry": SERVER_INSTANCE
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/stones/{wallet_hash}")
async def fetch_stones(wallet_hash: str, lod: int = 5):
    """Fetch stones from a wallet."""
    try:
        from redis_accounts import get_redis
        r = get_redis()

        stone_ids = r.smembers(f"{STONES_PREFIX}{wallet_hash}:index")

        stones = []
        for stone_id in stone_ids:
            stone_data = r.hgetall(f"{STONES_PREFIX}{wallet_hash}:{stone_id}")
            if stone_data:
                if lod == 5:
                    stones.append({
                        "stone_id": stone_data.get("stone_id"),
                        "hash": stone_data.get("hash"),
                        "type": stone_data.get("type"),
                        "glow": stone_data.get("glow"),
                        "lod5": stone_data.get("lod5")
                    })
                else:
                    full_stone = json.loads(stone_data.get("stone_json", "{}"))
                    stones.append(full_stone)

        return {
            "ok": True,
            "wallet_hash": wallet_hash,
            "stones": stones,
            "count": len(stones),
            "lod": lod
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/inbox/{wallet_hash}")
async def send_to_inbox(wallet_hash: str, data: InboxMessage):
    """Send a stone to a wallet's inbox."""
    try:
        from redis_accounts import get_redis
        r = get_redis()

        envelope = data.envelope
        message_id = f"msg_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{envelope.get('stone_hash', 'unknown')[:8]}"

        r.hset(f"{INBOX_PREFIX}{wallet_hash}:{message_id}", mapping={
            "message_id": message_id,
            "from": envelope.get("from", "unknown"),
            "to": envelope.get("to", wallet_hash),
            "sent": envelope.get("sent", datetime.now(timezone.utc).isoformat()),
            "stone_hash": envelope.get("stone_hash", ""),
            "envelope_json": json.dumps(envelope)
        })

        r.sadd(f"{INBOX_PREFIX}{wallet_hash}:index", message_id)

        return {
            "ok": True,
            "delivered": True,
            "message_id": message_id,
            "to": wallet_hash
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/inbox/{wallet_hash}")
async def check_inbox(wallet_hash: str):
    """Check inbox for a wallet."""
    try:
        from redis_accounts import get_redis
        r = get_redis()

        message_ids = r.smembers(f"{INBOX_PREFIX}{wallet_hash}:index")

        messages = []
        for msg_id in message_ids:
            msg_data = r.hgetall(f"{INBOX_PREFIX}{wallet_hash}:{msg_id}")
            if msg_data:
                messages.append({
                    "message_id": msg_data.get("message_id"),
                    "from": msg_data.get("from"),
                    "sent": msg_data.get("sent"),
                    "stone_hash": msg_data.get("stone_hash")
                })

        messages.sort(key=lambda m: m.get("sent", ""), reverse=True)

        return {
            "ok": True,
            "wallet_hash": wallet_hash,
            "messages": messages,
            "count": len(messages)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/federation/peers")
async def list_peers():
    """List federated registry peers."""
    try:
        from redis_accounts import get_redis
        r = get_redis()

        peers = r.smembers(PEERS_KEY) or set()

        return {
            "ok": True,
            "this_registry": SERVER_INSTANCE,
            "peers": list(peers),
            "federation_enabled": True
        }
    except Exception as e:
        return {
            "ok": True,
            "this_registry": SERVER_INSTANCE,
            "peers": [],
            "federation_enabled": False,
            "note": "Redis not available"
        }


@app.post("/api/v1/federation/join")
async def join_federation(peer_url: str):
    """Add a peer to the federation."""
    try:
        from redis_accounts import get_redis
        r = get_redis()

        r.sadd(PEERS_KEY, peer_url)

        return {
            "ok": True,
            "joined": peer_url,
            "registry": SERVER_INSTANCE
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/federation/info")
async def federation_info():
    """Get federation API info."""
    return {
        "ok": True,
        "version": "1.0.0",
        "registry": SERVER_INSTANCE,
        "endpoints": {
            "POST /api/v1/registry/wallet": "Register wallet",
            "GET /api/v1/registry/wallet/{hash}": "Resolve wallet",
            "POST /api/v1/stones/publish": "Publish stone",
            "GET /api/v1/stones/{wallet_hash}": "Fetch stones",
            "POST /api/v1/inbox/{wallet_hash}": "Send to inbox",
            "GET /api/v1/inbox/{wallet_hash}": "Check inbox",
            "GET /api/v1/federation/peers": "List peers",
            "POST /api/v1/federation/join": "Join federation"
        },
        "address_format": "alias@wallet_hash",
        "stone_format": "QA.Stone v1.0 with LOD layers"
    }


# =============================================================================
# CHAT API - Multi-LLM Chat with Message Stones
# =============================================================================

try:
    from llm_backends import ChatEngine, MessageStone, send_welcome_message, LLM_CONFIGS, BOT_PERSONAS
    CHAT_AVAILABLE = True
except ImportError:
    CHAT_AVAILABLE = False


@app.post("/api/chat/conversation")
async def create_conversation(user_token: str, bot_type: str = "assistant"):
    """Create a new chat conversation"""
    if not CHAT_AVAILABLE:
        return {"error": "Chat module not available"}

    user_id = authenticate(user_token)
    if not user_id:
        return {"error": "Invalid token"}

    engine = ChatEngine(get_redis())
    conv = await engine.create_conversation(user_id, bot_type)

    return {
        "success": True,
        **conv,
        "server_instance": SERVER_INSTANCE
    }


@app.post("/api/chat/message")
async def send_chat_message(
    user_token: str,
    conversation_id: str,
    content: str,
    provider: str = "groq",
    bot_type: str = "assistant"
):
    """Send a message and get AI response"""
    if not CHAT_AVAILABLE:
        return {"error": "Chat module not available"}

    user_id = authenticate(user_token)
    if not user_id:
        return {"error": "Invalid token"}

    engine = ChatEngine(get_redis())
    result = await engine.send_message(
        conversation_id=conversation_id,
        user_id=user_id,
        content=content,
        provider=provider,
        bot_type=bot_type
    )

    if result.get("success"):
        # Notify via WebSocket
        await ws_manager.notify_user(user_id, {
            "type": "chat_response",
            "conversation_id": conversation_id,
            "message": result["assistant_message"],
            "provider": result["provider"],
        })

    result["server_instance"] = SERVER_INSTANCE
    return result


@app.get("/api/chat/conversation/{conversation_id}")
async def get_conversation(conversation_id: str, user_token: str, limit: int = 50):
    """Get conversation history"""
    if not CHAT_AVAILABLE:
        return {"error": "Chat module not available"}

    user_id = authenticate(user_token)
    if not user_id:
        return {"error": "Invalid token"}

    engine = ChatEngine(get_redis())
    messages = await engine.get_conversation_history(conversation_id, limit)

    return {
        "success": True,
        "conversation_id": conversation_id,
        "messages": messages,
        "count": len(messages),
        "server_instance": SERVER_INSTANCE
    }


@app.get("/api/chat/conversations")
async def list_conversations(user_token: str):
    """List all conversations for user"""
    if not CHAT_AVAILABLE:
        return {"error": "Chat module not available"}

    user_id = authenticate(user_token)
    if not user_id:
        return {"error": "Invalid token"}

    engine = ChatEngine(get_redis())
    conversations = await engine.get_user_conversations(user_id)

    return {
        "success": True,
        "conversations": conversations,
        "count": len(conversations),
        "server_instance": SERVER_INSTANCE
    }


@app.post("/api/chat/welcome")
async def trigger_welcome(user_token: str):
    """Trigger welcome message for user"""
    if not CHAT_AVAILABLE:
        return {"error": "Chat module not available"}

    user_id = authenticate(user_token)
    if not user_id:
        return {"error": "Invalid token"}

    # Get username from user_id
    username = user_id.replace("user_", "")

    result = await send_welcome_message(user_id, username, get_redis())
    result["server_instance"] = SERVER_INSTANCE
    return result


@app.get("/api/chat/providers")
async def list_chat_providers():
    """List available LLM providers"""
    if not CHAT_AVAILABLE:
        return {"error": "Chat module not available", "providers": []}

    providers = []
    for key, config in LLM_CONFIGS.items():
        providers.append({
            "id": key,
            "name": config.name,
            "model": config.model,
            "available": bool(os.getenv(config.api_key_env)),
        })

    return {
        "providers": providers,
        "bot_types": list(BOT_PERSONAS.keys()),
        "server_instance": SERVER_INSTANCE
    }


@app.get("/api/chat/info")
async def chat_info():
    """Information about the chat system"""
    return {
        "version": "1.0.0",
        "available": CHAT_AVAILABLE,
        "description": "QA.Stone Chat - Multi-LLM messaging with verification",
        "features": {
            "message_stones": "Every message is a verified QA.Stone",
            "multi_llm": "Gemini, DeepSeek, Groq backends",
            "progressive": "Messages have LOD layers for efficient loading",
            "chain": "Conversations are linked chains of stones",
            "welcome_bot": "Automated onboarding for new users",
        },
        "providers": list(LLM_CONFIGS.keys()) if CHAT_AVAILABLE else [],
        "bot_types": list(BOT_PERSONAS.keys()) if CHAT_AVAILABLE else [],
        "endpoints": {
            "POST /api/chat/conversation": "Create new conversation",
            "POST /api/chat/message": "Send message, get AI response",
            "GET /api/chat/conversation/{id}": "Get conversation history",
            "GET /api/chat/conversations": "List user's conversations",
            "POST /api/chat/welcome": "Trigger welcome message",
            "GET /api/chat/providers": "List available LLM providers",
        },
        "server_instance": SERVER_INSTANCE
    }


# =============================================================================
# COLLAB API - Brainstorm, Debate, and Bidding Sessions
# =============================================================================

try:
    from collab_engine import (
        SessionManager, CollabSession, Participant,
        SessionMode, SessionStatus, DebateTeam
    )
    COLLAB_AVAILABLE = True
except ImportError:
    COLLAB_AVAILABLE = False

# Initialize session manager
_session_manager = None

def get_session_manager():
    global _session_manager
    if _session_manager is None and COLLAB_AVAILABLE:
        _session_manager = SessionManager(get_redis())
    return _session_manager


@app.post("/api/collab/session")
async def create_collab_session(
    user_token: str,
    mode: str = "brainstorm",
    starting_context: Optional[str] = None
):
    """Create a new collaborative session"""
    if not COLLAB_AVAILABLE:
        return {"error": "Collab module not available"}

    user_id = authenticate(user_token)
    if not user_id:
        return {"error": "Invalid token"}

    username = user_id.replace("user_", "")
    manager = get_session_manager()

    session = manager.create(
        host_id=user_id,
        host_name=username.capitalize(),
        mode=mode,
    )

    return {
        "success": True,
        "session_id": session.session_id,
        "invite_code": session.invite_code,
        "mode": session.mode,
        "status": session.status,
        "server_instance": SERVER_INSTANCE
    }


@app.get("/api/collab/session/{session_id}")
async def get_collab_session(session_id: str, user_token: str):
    """Get session state"""
    if not COLLAB_AVAILABLE:
        return {"error": "Collab module not available"}

    user_id = authenticate(user_token)
    if not user_id:
        return {"error": "Invalid token"}

    manager = get_session_manager()
    session = manager.get(session_id)

    if not session:
        return {"error": "Session not found"}

    return {
        "success": True,
        "session": session.to_dict(),
        "server_instance": SERVER_INSTANCE
    }


@app.post("/api/collab/session/{invite_code}/join")
async def join_collab_session(invite_code: str, user_token: str):
    """Join a session via invite code"""
    if not COLLAB_AVAILABLE:
        return {"error": "Collab module not available"}

    user_id = authenticate(user_token)
    if not user_id:
        return {"error": "Invalid token"}

    username = user_id.replace("user_", "")
    manager = get_session_manager()

    result = manager.join(
        invite_code=invite_code,
        guest_id=user_id,
        guest_name=username.capitalize(),
    )

    if result["ok"]:
        # Notify host via WebSocket
        session = manager.get_by_code(invite_code)
        if session:
            await ws_manager.notify_user(session.host.user_id, {
                "type": "collab_guest_joined",
                "session_id": session.session_id,
                "guest": username.capitalize(),
            })

    result["server_instance"] = SERVER_INSTANCE
    return result


@app.post("/api/collab/session/{session_id}/message")
async def add_collab_message(
    session_id: str,
    user_token: str,
    content: str,
    message_type: str = "chat"
):
    """Add a message to the session"""
    if not COLLAB_AVAILABLE:
        return {"error": "Collab module not available"}

    user_id = authenticate(user_token)
    if not user_id:
        return {"error": "Invalid token"}

    manager = get_session_manager()
    session = manager.get(session_id)

    if not session:
        return {"error": "Session not found"}

    # Determine participant role
    if user_id == session.host.user_id:
        participant = "host"
    elif session.guest and user_id == session.guest.user_id:
        participant = "guest"
    else:
        return {"error": "Not a participant"}

    msg = session.add_message(participant, message_type, content)
    manager.save(session)

    # Notify other participant
    other_user = session.guest.user_id if participant == "host" else session.host.user_id
    if other_user:
        await ws_manager.notify_user(other_user, {
            "type": "collab_message",
            "session_id": session_id,
            "message": msg.to_dict(),
        })

    return {
        "success": True,
        "message": msg.to_dict(),
        "server_instance": SERVER_INSTANCE
    }


@app.delete("/api/collab/session/{session_id}")
async def end_collab_session(session_id: str, user_token: str):
    """End a session"""
    if not COLLAB_AVAILABLE:
        return {"error": "Collab module not available"}

    user_id = authenticate(user_token)
    if not user_id:
        return {"error": "Invalid token"}

    manager = get_session_manager()
    session = manager.get(session_id)

    if not session:
        return {"error": "Session not found"}

    if user_id != session.host.user_id:
        return {"error": "Only host can end session"}

    manager.end(session_id)

    # Notify guest
    if session.guest:
        await ws_manager.notify_user(session.guest.user_id, {
            "type": "collab_session_ended",
            "session_id": session_id,
        })

    return {
        "success": True,
        "server_instance": SERVER_INSTANCE
    }


@app.get("/api/collab/sessions")
async def list_collab_sessions(user_token: str):
    """List user's collab sessions"""
    if not COLLAB_AVAILABLE:
        return {"error": "Collab module not available"}

    user_id = authenticate(user_token)
    if not user_id:
        return {"error": "Invalid token"}

    manager = get_session_manager()
    sessions = manager.list_user_sessions(user_id)

    return {
        "success": True,
        "sessions": sessions,
        "count": len(sessions),
        "server_instance": SERVER_INSTANCE
    }


# =============================================================================
# BRAINSTORM MODE API
# =============================================================================

@app.post("/api/brainstorm/idea")
async def add_brainstorm_idea(
    session_id: str,
    user_token: str,
    content: str
):
    """Add an idea to a brainstorm session"""
    if not COLLAB_AVAILABLE:
        return {"error": "Collab module not available"}

    user_id = authenticate(user_token)
    if not user_id:
        return {"error": "Invalid token"}

    manager = get_session_manager()
    session = manager.get(session_id)

    if not session:
        return {"error": "Session not found"}

    if session.mode != SessionMode.BRAINSTORM.value:
        return {"error": "Session is not in brainstorm mode"}

    username = user_id.replace("user_", "")
    idea = session.add_idea(user_id, username.capitalize(), content)
    manager.save(session)

    # Notify other participant
    other_user = None
    if user_id == session.host.user_id and session.guest:
        other_user = session.guest.user_id
    elif session.guest and user_id == session.guest.user_id:
        other_user = session.host.user_id

    if other_user:
        await ws_manager.notify_user(other_user, {
            "type": "brainstorm_idea_added",
            "session_id": session_id,
            "idea": idea.to_dict(),
        })

    return {
        "success": True,
        "idea": idea.to_dict(),
        "server_instance": SERVER_INSTANCE
    }


@app.post("/api/brainstorm/vote")
async def vote_brainstorm_idea(
    session_id: str,
    idea_id: str,
    user_token: str
):
    """Vote for an idea"""
    if not COLLAB_AVAILABLE:
        return {"error": "Collab module not available"}

    user_id = authenticate(user_token)
    if not user_id:
        return {"error": "Invalid token"}

    manager = get_session_manager()
    session = manager.get(session_id)

    if not session:
        return {"error": "Session not found"}

    success = session.vote_idea(idea_id, user_id)
    if success:
        manager.save(session)

    return {
        "success": success,
        "error": None if success else "Already voted or idea not found",
        "server_instance": SERVER_INSTANCE
    }


@app.post("/api/brainstorm/expand")
async def expand_brainstorm_idea(
    session_id: str,
    idea_id: str,
    user_token: str,
    provider: str = "groq"
):
    """Expand an idea using LLM"""
    if not COLLAB_AVAILABLE or not CHAT_AVAILABLE:
        return {"error": "Required modules not available"}

    user_id = authenticate(user_token)
    if not user_id:
        return {"error": "Invalid token"}

    manager = get_session_manager()
    session = manager.get(session_id)

    if not session:
        return {"error": "Session not found"}

    # Find the idea
    idea = None
    for i in session.ideas:
        if i["idea_id"] == idea_id:
            idea = i
            break

    if not idea:
        return {"error": "Idea not found"}

    # Call LLM to expand
    from llm_backends import call_llm

    messages = [
        {"role": "system", "content": "You are a creative brainstorming assistant. Expand on the following idea with 2-3 concrete implementation suggestions. Be concise but insightful."},
        {"role": "user", "content": f"Idea: {idea['content']}\n\nExpand on this idea:"}
    ]

    response = await call_llm(provider, messages)

    if "error" in response:
        return {"success": False, "error": response["error"]}

    expansion = response["content"]
    session.expand_idea(idea_id, expansion)
    manager.save(session)

    return {
        "success": True,
        "expansion": expansion,
        "provider": response["provider"],
        "server_instance": SERVER_INSTANCE
    }


@app.get("/api/brainstorm/ranked")
async def get_ranked_ideas(session_id: str, user_token: str):
    """Get ideas ranked by votes"""
    if not COLLAB_AVAILABLE:
        return {"error": "Collab module not available"}

    user_id = authenticate(user_token)
    if not user_id:
        return {"error": "Invalid token"}

    manager = get_session_manager()
    session = manager.get(session_id)

    if not session:
        return {"error": "Session not found"}

    return {
        "success": True,
        "ideas": session.get_ranked_ideas(),
        "server_instance": SERVER_INSTANCE
    }


# =============================================================================
# DEBATE MODE API
# =============================================================================

@app.post("/api/debate/setup-teams")
async def setup_debate_teams(
    session_id: str,
    user_token: str,
    topic: str,
    team_pro_name: str = "Pro",
    team_con_name: str = "Con"
):
    """Setup debate with topic and teams"""
    if not COLLAB_AVAILABLE:
        return {"error": "Collab module not available"}

    user_id = authenticate(user_token)
    if not user_id:
        return {"error": "Invalid token"}

    manager = get_session_manager()
    session = manager.get(session_id)

    if not session:
        return {"error": "Session not found"}

    if session.mode != SessionMode.DEBATE.value:
        return {"error": "Session is not in debate mode"}

    teams = [
        DebateTeam(team_id="team_pro", name=team_pro_name, position="pro"),
        DebateTeam(team_id="team_con", name=team_con_name, position="con"),
    ]

    session.setup_debate(topic, teams)
    manager.save(session)

    return {
        "success": True,
        "topic": topic,
        "teams": [t.to_dict() for t in teams],
        "server_instance": SERVER_INSTANCE
    }


@app.post("/api/debate/argument")
async def submit_debate_argument(
    session_id: str,
    team_id: str,
    user_token: str,
    content: str
):
    """Submit a debate argument"""
    if not COLLAB_AVAILABLE:
        return {"error": "Collab module not available"}

    user_id = authenticate(user_token)
    if not user_id:
        return {"error": "Invalid token"}

    manager = get_session_manager()
    session = manager.get(session_id)

    if not session:
        return {"error": "Session not found"}

    username = user_id.replace("user_", "")
    arg = session.submit_argument(team_id, user_id, username.capitalize(), content)
    manager.save(session)

    return {
        "success": True,
        "argument": arg.to_dict(),
        "server_instance": SERVER_INSTANCE
    }


@app.post("/api/debate/llm-advocate")
async def request_llm_advocate(
    session_id: str,
    team_id: str,
    user_token: str,
    provider: str = "groq"
):
    """Request LLM to argue for a team"""
    if not COLLAB_AVAILABLE or not CHAT_AVAILABLE:
        return {"error": "Required modules not available"}

    user_id = authenticate(user_token)
    if not user_id:
        return {"error": "Invalid token"}

    manager = get_session_manager()
    session = manager.get(session_id)

    if not session:
        return {"error": "Session not found"}

    # Find team
    team = None
    for t in session.debate_teams:
        if t["team_id"] == team_id:
            team = t
            break

    if not team:
        return {"error": "Team not found"}

    # Build context from previous arguments
    context = f"Topic: {session.debate_topic}\n\n"
    for arg in session.debate_arguments:
        context += f"{arg['author_name']} ({arg['team_id']}): {arg['content']}\n\n"

    from llm_backends import call_llm

    messages = [
        {"role": "system", "content": f"You are an AI debate advocate for the '{team['name']}' team (position: {team['position']}). Make a compelling argument for your position. Be persuasive but fair. Keep it to 2-3 paragraphs."},
        {"role": "user", "content": f"{context}\n\nMake your next argument for the {team['position']} position:"}
    ]

    response = await call_llm(provider, messages)

    if "error" in response:
        return {"success": False, "error": response["error"]}

    # Submit as LLM argument
    arg = session.submit_argument(
        team_id,
        f"llm_{provider}",
        f"AI Advocate ({provider.capitalize()})",
        response["content"]
    )
    manager.save(session)

    return {
        "success": True,
        "argument": arg.to_dict(),
        "provider": response["provider"],
        "server_instance": SERVER_INSTANCE
    }


@app.post("/api/debate/judge")
async def judge_debate_argument(
    session_id: str,
    argument_id: str,
    score: int,
    user_token: str,
    reason: str = ""
):
    """Judge an argument (score 1-10)"""
    if not COLLAB_AVAILABLE:
        return {"error": "Collab module not available"}

    user_id = authenticate(user_token)
    if not user_id:
        return {"error": "Invalid token"}

    if not 1 <= score <= 10:
        return {"error": "Score must be 1-10"}

    manager = get_session_manager()
    session = manager.get(session_id)

    if not session:
        return {"error": "Session not found"}

    success = session.judge_argument(argument_id, user_id, score, reason)
    if success:
        manager.save(session)

    return {
        "success": success,
        "server_instance": SERVER_INSTANCE
    }


@app.post("/api/debate/next-round")
async def advance_debate_round(session_id: str, user_token: str):
    """Advance to next debate round"""
    if not COLLAB_AVAILABLE:
        return {"error": "Collab module not available"}

    user_id = authenticate(user_token)
    if not user_id:
        return {"error": "Invalid token"}

    manager = get_session_manager()
    session = manager.get(session_id)

    if not session:
        return {"error": "Session not found"}

    new_round = session.next_round()
    manager.save(session)

    return {
        "success": True,
        "round": new_round,
        "server_instance": SERVER_INSTANCE
    }


@app.get("/api/debate/leaderboard")
async def get_debate_leaderboard(session_id: str, user_token: str):
    """Get debate team scores"""
    if not COLLAB_AVAILABLE:
        return {"error": "Collab module not available"}

    user_id = authenticate(user_token)
    if not user_id:
        return {"error": "Invalid token"}

    manager = get_session_manager()
    session = manager.get(session_id)

    if not session:
        return {"error": "Session not found"}

    return {
        "success": True,
        "leaderboard": session.get_debate_leaderboard(),
        "current_round": session.debate_round,
        "server_instance": SERVER_INSTANCE
    }


@app.get("/api/collab/info")
async def collab_info():
    """Information about the collab system"""
    return {
        "version": "1.0.0",
        "available": COLLAB_AVAILABLE,
        "description": "QA.Stone Collab - Brainstorm, Debate, and Bidding sessions",
        "modes": {
            "brainstorm": "Add ideas, vote, expand with LLM",
            "debate": "Teams argue positions with LLM advocates",
            "bidding": "Competitive offers (coming soon)",
        },
        "features": {
            "qa_stones": "All messages and events are verified QA.Stones",
            "real_time": "WebSocket notifications for live collaboration",
            "permissions": "Contribution-based permission escalation",
            "llm_integration": "AI expansion and advocacy",
        },
        "endpoints": {
            "POST /api/collab/session": "Create session",
            "GET /api/collab/session/{id}": "Get session",
            "POST /api/collab/session/{code}/join": "Join via invite code",
            "POST /api/brainstorm/idea": "Add idea",
            "POST /api/brainstorm/vote": "Vote for idea",
            "POST /api/brainstorm/expand": "Expand idea with LLM",
            "POST /api/debate/setup-teams": "Setup debate",
            "POST /api/debate/argument": "Submit argument",
            "POST /api/debate/llm-advocate": "LLM argues for team",
            "POST /api/debate/judge": "Score argument",
        },
        "server_instance": SERVER_INSTANCE
    }


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    logger.info(f"Starting QA.Stone MCP Server {SERVER_INSTANCE.upper()} v{SERVER_VERSION}")
    logger.info(f"Port: {SERVER_PORT}")

    # Test Redis connection
    if test_connection():
        logger.info("Redis: Connected")
    else:
        logger.warning("Redis: Connection failed - some features may not work")

    uvicorn.run(app, host="0.0.0.0", port=SERVER_PORT)

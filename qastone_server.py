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
    """Serve the web UI"""
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

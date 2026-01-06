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

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn
import httpx

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

# =============================================================================
# CONFIG
# =============================================================================

SERVER_INSTANCE = os.getenv("SERVER_INSTANCE", "a")
SERVER_PORT = int(os.getenv("SERVER_PORT", "8301"))
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

@app.get("/")
async def root():
    return {
        "service": "QA.Stone MCP Server",
        "instance": SERVER_INSTANCE,
        "version": SERVER_VERSION,
        "status": "running",
        "endpoints": {
            "mcp": "/mcp",
            "health": "/health",
            "info": "/info",
            "tools": "/tools"
        }
    }


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

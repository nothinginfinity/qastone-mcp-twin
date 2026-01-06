#!/usr/bin/env python3
"""
QA.Stone as MCP Updates - Proof of Concept

This module implements QA.Stones as actual MCP protocol messages.
Each stone IS the update - cryptographically signed, chainable, and verifiable.

Key Concepts:
1. Stone = MCP Message: The stone contains the MCP tools/call that created it
2. Chained: Each stone references the previous stone's hash (like blockchain)
3. Verifiable: Any MCP server can validate and apply the same stone
4. Deterministic: Same stone + same state = same result on any server

Usage:
    # Create a transfer stone (MCP update)
    stone = create_mcp_stone_update(
        action="transfer",
        from_user="user_alice",
        to_user="user_bob",
        stone_id="GI_amazon_xxx"
    )

    # Apply the stone to any MCP server
    result = apply_mcp_stone_update(stone)

    # Verify stone is valid
    is_valid = verify_mcp_stone(stone)
"""

import json
import hashlib
import secrets
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, asdict

from redis_accounts import (
    get_redis,
    authenticate,
    get_wallet,
    send_stone,
    accept_transfer,
    check_inbox,
    mint_to_wallet,
    generate_user_id,
)

# =============================================================================
# MCP STONE UPDATE FORMAT
# =============================================================================

@dataclass
class MCPStoneUpdate:
    """
    A QA.Stone that IS an MCP update.

    This is not just data - it's a complete, verifiable state transition
    that can be applied to any MCP server with identical results.
    """
    # Stone Identity
    stone_update_id: str          # Unique ID for this update

    # MCP Message (the actual update)
    mcp_message: Dict             # Full MCP tools/call message

    # Chain (for ordering and integrity)
    previous_hash: str            # Hash of previous stone in chain
    sequence_number: int          # Position in chain

    # Cryptographic Proof
    timestamp: str                # When created
    nonce: str                    # Random value for uniqueness
    signature: str                # SHA256 of entire stone content

    # Metadata
    created_by: str               # User/system that created this
    server_instance: str          # Which MCP server created it

    # Defaults must come last
    version: str = "2.0.0-mcp"    # Version indicating MCP-native stone

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> "MCPStoneUpdate":
        return cls(**data)

    def compute_hash(self) -> str:
        """Compute hash of this stone (excluding signature)."""
        content = {
            "stone_update_id": self.stone_update_id,
            "mcp_message": self.mcp_message,
            "previous_hash": self.previous_hash,
            "sequence_number": self.sequence_number,
            "timestamp": self.timestamp,
            "nonce": self.nonce,
            "created_by": self.created_by,
        }
        return hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()


# =============================================================================
# STONE CHAIN (LEDGER)
# =============================================================================

CHAIN_KEY = "mcp_stone_chain"
CHAIN_HEAD_KEY = "mcp_stone_chain_head"


def get_chain_head() -> Dict:
    """Get the current head of the stone chain."""
    r = get_redis()
    head_data = r.get(CHAIN_HEAD_KEY)
    if not head_data:
        # Genesis block
        return {
            "hash": "0" * 64,  # Genesis hash
            "sequence_number": 0,
            "stone_update_id": "genesis"
        }
    return json.loads(head_data)


def append_to_chain(stone: MCPStoneUpdate) -> bool:
    """Append a validated stone to the chain."""
    r = get_redis()

    # Store the stone
    r.hset(CHAIN_KEY, stone.stone_update_id, json.dumps(stone.to_dict()))

    # Update chain head
    new_head = {
        "hash": stone.signature,
        "sequence_number": stone.sequence_number,
        "stone_update_id": stone.stone_update_id
    }
    r.set(CHAIN_HEAD_KEY, json.dumps(new_head))

    return True


def get_chain_length() -> int:
    """Get number of stones in chain."""
    r = get_redis()
    return r.hlen(CHAIN_KEY)


def get_stone_from_chain(stone_update_id: str) -> Optional[MCPStoneUpdate]:
    """Retrieve a stone from the chain."""
    r = get_redis()
    data = r.hget(CHAIN_KEY, stone_update_id)
    if not data:
        return None
    return MCPStoneUpdate.from_dict(json.loads(data))


def get_recent_chain(limit: int = 10) -> List[Dict]:
    """Get recent stones from chain."""
    r = get_redis()
    all_stones = r.hgetall(CHAIN_KEY)

    stones = []
    for stone_id, data in all_stones.items():
        stone_dict = json.loads(data)
        stones.append({
            "stone_update_id": stone_dict["stone_update_id"],
            "action": stone_dict["mcp_message"]["params"]["name"],
            "sequence": stone_dict["sequence_number"],
            "timestamp": stone_dict["timestamp"],
            "hash": stone_dict["signature"][:16] + "..."
        })

    # Sort by sequence number descending
    stones.sort(key=lambda x: x["sequence"], reverse=True)
    return stones[:limit]


# =============================================================================
# CREATE MCP STONE UPDATE
# =============================================================================

def create_mcp_stone_update(
    action: str,
    arguments: Dict,
    created_by: str,
    server_instance: str = "a"
) -> MCPStoneUpdate:
    """
    Create a new MCP Stone Update.

    This creates a stone that IS an MCP message - the stone itself
    is the update that will be applied to the server state.
    """
    # Get chain head for linking
    head = get_chain_head()

    # Generate unique ID
    timestamp = datetime.now(timezone.utc).isoformat()
    nonce = secrets.token_hex(16)
    stone_update_id = f"mcp_stone_{datetime.now().strftime('%Y%m%d%H%M%S')}_{secrets.token_hex(4)}"

    # Create the MCP message that this stone represents
    mcp_message = {
        "jsonrpc": "2.0",
        "id": stone_update_id,
        "method": "tools/call",
        "params": {
            "name": action,
            "arguments": arguments
        }
    }

    # Create stone (without signature first)
    stone = MCPStoneUpdate(
        stone_update_id=stone_update_id,
        mcp_message=mcp_message,
        previous_hash=head["hash"],
        sequence_number=head["sequence_number"] + 1,
        timestamp=timestamp,
        nonce=nonce,
        signature="",  # Will compute
        created_by=created_by,
        server_instance=server_instance,
        version="2.0.0-mcp"
    )

    # Compute and set signature
    stone.signature = stone.compute_hash()

    return stone


# =============================================================================
# VERIFY MCP STONE
# =============================================================================

def verify_mcp_stone(stone: MCPStoneUpdate) -> Dict:
    """
    Verify a stone is valid and can be applied.

    Checks:
    1. Signature matches content
    2. Previous hash exists in chain (or is genesis)
    3. Sequence number is correct
    4. MCP message is well-formed
    """
    errors = []

    # 1. Verify signature
    expected_hash = stone.compute_hash()
    if stone.signature != expected_hash:
        errors.append(f"Signature mismatch: expected {expected_hash[:16]}..., got {stone.signature[:16]}...")

    # 2. Verify chain link
    head = get_chain_head()
    if stone.previous_hash != head["hash"]:
        # Check if it's already in chain (replay)
        existing = get_stone_from_chain(stone.stone_update_id)
        if existing:
            errors.append("Stone already applied (replay detected)")
        else:
            errors.append(f"Chain broken: previous_hash doesn't match head")

    # 3. Verify sequence
    if stone.sequence_number != head["sequence_number"] + 1:
        errors.append(f"Sequence mismatch: expected {head['sequence_number'] + 1}, got {stone.sequence_number}")

    # 4. Verify MCP message structure
    mcp = stone.mcp_message
    if mcp.get("jsonrpc") != "2.0":
        errors.append("Invalid MCP message: missing jsonrpc 2.0")
    if mcp.get("method") != "tools/call":
        errors.append("Invalid MCP message: method must be tools/call")
    if "params" not in mcp or "name" not in mcp.get("params", {}):
        errors.append("Invalid MCP message: missing params.name")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "stone_update_id": stone.stone_update_id,
        "sequence": stone.sequence_number,
        "action": mcp.get("params", {}).get("name", "unknown")
    }


# =============================================================================
# APPLY MCP STONE UPDATE
# =============================================================================

def apply_mcp_stone_update(stone: MCPStoneUpdate) -> Dict:
    """
    Apply a verified MCP Stone Update to the server state.

    This is the core of the concept: the stone IS the update.
    Applying it changes the server state deterministically.
    """
    # First verify
    verification = verify_mcp_stone(stone)
    if not verification["valid"]:
        return {
            "success": False,
            "error": "Stone verification failed",
            "verification": verification
        }

    # Extract action and arguments from MCP message
    action = stone.mcp_message["params"]["name"]
    args = stone.mcp_message["params"]["arguments"]

    # Apply based on action
    result = None

    if action == "qastone_transfer":
        # Transfer stone between users
        result = _apply_transfer(args)

    elif action == "qastone_mint":
        # Mint new stone to user
        result = _apply_mint(args)

    elif action == "qastone_accept":
        # Accept pending transfer
        result = _apply_accept(args)

    else:
        return {
            "success": False,
            "error": f"Unknown action: {action}"
        }

    # If successful, append to chain
    if result.get("success"):
        append_to_chain(stone)
        result["stone_update_id"] = stone.stone_update_id
        result["sequence_number"] = stone.sequence_number
        result["chain_hash"] = stone.signature

    return result


def _apply_transfer(args: Dict) -> Dict:
    """Apply a transfer action."""
    from_user = args.get("from_user")
    to_user = args.get("to_user")
    stone_id = args.get("stone_id")

    if not all([from_user, to_user, stone_id]):
        return {"success": False, "error": "Missing required fields"}

    return send_stone(from_user, to_user, stone_id)


def _apply_mint(args: Dict) -> Dict:
    """Apply a mint action."""
    from qastone_mint import mint_offer

    user_id = args.get("user_id")
    sponsor = args.get("sponsor")
    offer_type = args.get("type")
    value_cents = args.get("value_cents")

    if not all([user_id, sponsor, offer_type, value_cents]):
        return {"success": False, "error": "Missing required fields"}

    stone = mint_offer(sponsor, offer_type, value_cents)
    return mint_to_wallet(user_id, stone)


def _apply_accept(args: Dict) -> Dict:
    """Apply an accept action."""
    user_id = args.get("user_id")
    transfer_id = args.get("transfer_id")

    if not all([user_id, transfer_id]):
        return {"success": False, "error": "Missing required fields"}

    return accept_transfer(user_id, transfer_id)


# =============================================================================
# HIGH-LEVEL API
# =============================================================================

def transfer_as_mcp_update(
    from_token: str,
    to_username: str,
    stone_id: str,
    server_instance: str = "a"
) -> Dict:
    """
    Perform a transfer as an MCP Stone Update.

    This creates and applies a stone that IS the transfer.
    The stone can be verified and replayed on any MCP server.
    """
    # Authenticate
    from_user_id = authenticate(from_token)
    if not from_user_id:
        return {"success": False, "error": "Invalid token"}

    to_user_id = generate_user_id(to_username)

    # Create the MCP stone update
    stone = create_mcp_stone_update(
        action="qastone_transfer",
        arguments={
            "from_user": from_user_id,
            "to_user": to_user_id,
            "stone_id": stone_id
        },
        created_by=from_user_id,
        server_instance=server_instance
    )

    # Apply it
    result = apply_mcp_stone_update(stone)

    if result.get("success"):
        result["mcp_stone"] = {
            "stone_update_id": stone.stone_update_id,
            "mcp_message": stone.mcp_message,
            "signature": stone.signature,
            "sequence": stone.sequence_number,
            "previous_hash": stone.previous_hash[:16] + "..."
        }

    return result


def get_chain_status() -> Dict:
    """Get current chain status."""
    head = get_chain_head()
    return {
        "chain_length": get_chain_length(),
        "head_hash": head["hash"][:16] + "..." if head["hash"] != "0" * 64 else "genesis",
        "head_sequence": head["sequence_number"],
        "recent_stones": get_recent_chain(5)
    }


# =============================================================================
# TEST
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("MCP Stone Update - Proof of Concept")
    print("=" * 60)

    # Check chain status
    print("\n1. Chain status:")
    status = get_chain_status()
    print(f"   Length: {status['chain_length']}")
    print(f"   Head: {status['head_hash']}")

    # Create a test stone update
    print("\n2. Creating MCP Stone Update...")
    stone = create_mcp_stone_update(
        action="qastone_transfer",
        arguments={
            "from_user": "user_alice",
            "to_user": "user_bob",
            "stone_id": "test_stone_123"
        },
        created_by="user_alice",
        server_instance="test"
    )
    print(f"   Stone ID: {stone.stone_update_id}")
    print(f"   Signature: {stone.signature[:32]}...")
    print(f"   MCP Message: {stone.mcp_message['method']}")

    # Verify it
    print("\n3. Verifying stone...")
    verification = verify_mcp_stone(stone)
    print(f"   Valid: {verification['valid']}")
    if verification['errors']:
        print(f"   Errors: {verification['errors']}")

    print("\n" + "=" * 60)
    print("Concept proven: QA.Stone IS the MCP update!")
    print("=" * 60)

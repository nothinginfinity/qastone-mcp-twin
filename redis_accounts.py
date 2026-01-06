#!/usr/bin/env python3
"""
Redis-Backed Account System for QA.Stone

Provides user accounts, wallets, and stone transfers using Redis.
Configured via environment variables for Railway deployment.
"""

import os
import json
import secrets
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, asdict

import redis

# =============================================================================
# REDIS CONNECTION (from environment)
# =============================================================================

def get_redis_config() -> dict:
    """Get Redis config from environment variables."""
    return {
        "host": os.getenv("REDIS_HOST", "localhost"),
        "port": int(os.getenv("REDIS_PORT", "6379")),
        "username": os.getenv("REDIS_USER", "default"),
        "password": os.getenv("REDIS_PASSWORD", ""),
        "decode_responses": True,
    }


_redis_client: Optional[redis.Redis] = None


def get_redis() -> redis.Redis:
    """Get or create Redis connection."""
    global _redis_client
    if _redis_client is None:
        config = get_redis_config()
        # Filter out empty password
        if not config.get("password"):
            del config["password"]
            del config["username"]
        _redis_client = redis.Redis(**config)
    return _redis_client


def test_connection() -> bool:
    """Test Redis connectivity."""
    try:
        r = get_redis()
        r.ping()
        return True
    except Exception as e:
        print(f"[Redis] Connection failed: {e}")
        return False


# =============================================================================
# USER ACCOUNT MODEL
# =============================================================================

@dataclass
class Account:
    user_id: str
    username: str
    token: str
    created_at: str
    stone_count: int = 0
    total_value_cents: int = 0

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> "Account":
        return cls(**data)


def generate_user_id(username: str) -> str:
    """Generate deterministic user_id from username."""
    return f"user_{username.lower()}"


def generate_token(username: str) -> str:
    """Generate simple demo token."""
    return f"demo_{username.lower()}_{datetime.now().year}"


# =============================================================================
# ACCOUNT CRUD
# =============================================================================

def create_account(username: str, custom_token: Optional[str] = None) -> Dict:
    """Create a new user account."""
    r = get_redis()
    username_lower = username.lower()

    if r.sismember("usernames", username_lower):
        return {"success": False, "error": f"Username '{username}' already exists"}

    user_id = generate_user_id(username)
    token = custom_token or generate_token(username)
    now = datetime.now(timezone.utc).isoformat()

    account = Account(
        user_id=user_id,
        username=username,
        token=token,
        created_at=now,
    )

    pipe = r.pipeline()
    pipe.set(f"account:{user_id}", json.dumps(account.to_dict()))
    pipe.set(f"token:{token}", user_id)
    pipe.sadd("usernames", username_lower)
    pipe.sadd("user_ids", user_id)
    pipe.execute()

    return {
        "success": True,
        "user_id": user_id,
        "username": username,
        "token": token,
    }


def get_account(user_id: str) -> Optional[Account]:
    """Get account by user_id."""
    r = get_redis()
    data = r.get(f"account:{user_id}")
    if not data:
        return None
    return Account.from_dict(json.loads(data))


def authenticate(token: str) -> Optional[str]:
    """Authenticate by token. Returns user_id if valid."""
    r = get_redis()
    return r.get(f"token:{token}")


def list_users() -> List[Dict]:
    """List all users with wallet summaries."""
    r = get_redis()
    user_ids = r.smembers("user_ids")

    users = []
    for user_id in user_ids:
        account = get_account(user_id)
        if account:
            wallet_size = r.scard(f"wallet:{user_id}")
            total_value = _calculate_wallet_value(user_id)

            users.append({
                "user_id": user_id,
                "username": account.username,
                "stone_count": wallet_size,
                "total_value_usd": f"${total_value / 100:.2f}",
                "total_value_cents": total_value,
            })

    return sorted(users, key=lambda x: x["username"])


# =============================================================================
# WALLET OPERATIONS
# =============================================================================

def mint_to_wallet(user_id: str, stone: Dict) -> Dict:
    """Mint a new QA.Stone directly to a user's wallet."""
    r = get_redis()

    if not r.exists(f"account:{user_id}"):
        return {"success": False, "error": f"User {user_id} not found"}

    stone_id = stone.get("offer_stone_id")
    if not stone_id:
        return {"success": False, "error": "Stone missing offer_stone_id"}

    stone["owner"] = user_id
    stone["owned_since"] = datetime.now(timezone.utc).isoformat()

    pipe = r.pipeline()
    pipe.set(f"stone:{stone_id}", json.dumps(stone))
    pipe.sadd(f"wallet:{user_id}", stone_id)
    pipe.execute()

    return {"success": True, "stone_id": stone_id, "user_id": user_id}


def get_wallet(user_id: str) -> List[Dict]:
    """Get all stones in a user's wallet."""
    r = get_redis()
    stone_ids = r.smembers(f"wallet:{user_id}")

    stones = []
    for stone_id in stone_ids:
        stone_data = r.get(f"stone:{stone_id}")
        if stone_data:
            stones.append(json.loads(stone_data))

    return stones


def get_wallet_summary(user_id: str) -> Dict:
    """Get wallet summary."""
    stones = get_wallet(user_id)

    total_value = 0
    by_type = {}

    for stone in stones:
        value = stone.get("offer", {}).get("value_cents", 0)
        total_value += value

        offer_type = stone.get("offer_type", "unknown")
        if offer_type not in by_type:
            by_type[offer_type] = {"count": 0, "value_cents": 0}
        by_type[offer_type]["count"] += 1
        by_type[offer_type]["value_cents"] += value

    return {
        "user_id": user_id,
        "stone_count": len(stones),
        "total_value_cents": total_value,
        "total_value_usd": f"${total_value / 100:.2f}",
        "by_type": by_type,
    }


def _calculate_wallet_value(user_id: str) -> int:
    """Calculate total wallet value in cents."""
    r = get_redis()
    stone_ids = r.smembers(f"wallet:{user_id}")

    total = 0
    for stone_id in stone_ids:
        stone_data = r.get(f"stone:{stone_id}")
        if stone_data:
            stone = json.loads(stone_data)
            total += stone.get("offer", {}).get("value_cents", 0)

    return total


# =============================================================================
# STONE TRANSFERS
# =============================================================================

def send_stone(from_user_id: str, to_user_id: str, stone_id: str) -> Dict:
    """Send a QA.Stone from one user to another."""
    r = get_redis()

    if not r.exists(f"account:{from_user_id}"):
        return {"success": False, "error": f"Sender {from_user_id} not found"}
    if not r.exists(f"account:{to_user_id}"):
        return {"success": False, "error": f"Recipient {to_user_id} not found"}

    if not r.sismember(f"wallet:{from_user_id}", stone_id):
        return {"success": False, "error": f"Stone {stone_id} not in sender's wallet"}

    stone_data = r.get(f"stone:{stone_id}")
    if not stone_data:
        return {"success": False, "error": f"Stone {stone_id} not found"}

    stone = json.loads(stone_data)

    transfer_id = f"tx_{secrets.token_hex(8)}"
    transfer = {
        "transfer_id": transfer_id,
        "from_user": from_user_id,
        "to_user": to_user_id,
        "stone_id": stone_id,
        "stone": stone,
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "status": "pending",
    }

    pipe = r.pipeline()
    pipe.srem(f"wallet:{from_user_id}", stone_id)
    pipe.lpush(f"inbox:{to_user_id}", json.dumps(transfer))
    pipe.execute()

    return {
        "success": True,
        "transfer_id": transfer_id,
        "from": from_user_id,
        "to": to_user_id,
        "stone_id": stone_id,
    }


def send_stone_by_username(from_token: str, to_username: str, stone_id: str) -> Dict:
    """Send stone using token auth and username lookup."""
    from_user_id = authenticate(from_token)
    if not from_user_id:
        return {"success": False, "error": "Invalid token"}

    to_user_id = generate_user_id(to_username)
    return send_stone(from_user_id, to_user_id, stone_id)


def check_inbox(user_id: str) -> List[Dict]:
    """Get all pending transfers in user's inbox."""
    r = get_redis()
    inbox_data = r.lrange(f"inbox:{user_id}", 0, -1)
    return [json.loads(data) for data in inbox_data]


def accept_transfer(user_id: str, transfer_id: str) -> Dict:
    """Accept a pending transfer from inbox."""
    r = get_redis()
    inbox_data = r.lrange(f"inbox:{user_id}", 0, -1)

    transfer = None
    transfer_index = -1

    for i, data in enumerate(inbox_data):
        t = json.loads(data)
        if t.get("transfer_id") == transfer_id:
            transfer = t
            transfer_index = i
            break

    if not transfer:
        return {"success": False, "error": f"Transfer {transfer_id} not found"}

    stone = transfer["stone"]
    stone_id = transfer["stone_id"]

    stone["owner"] = user_id
    stone["owned_since"] = datetime.now(timezone.utc).isoformat()
    stone["transfer_history"] = stone.get("transfer_history", [])
    stone["transfer_history"].append({
        "from": transfer["from_user"],
        "to": user_id,
        "at": transfer["sent_at"],
        "transfer_id": transfer_id,
    })

    pipe = r.pipeline()
    pipe.lrem(f"inbox:{user_id}", 1, inbox_data[transfer_index])
    pipe.sadd(f"wallet:{user_id}", stone_id)
    pipe.set(f"stone:{stone_id}", json.dumps(stone))
    pipe.execute()

    return {
        "success": True,
        "transfer_id": transfer_id,
        "stone_id": stone_id,
        "from": transfer["from_user"],
    }


def accept_all_transfers(user_id: str) -> Dict:
    """Accept all pending transfers in inbox."""
    transfers = check_inbox(user_id)
    results = [accept_transfer(user_id, t["transfer_id"]) for t in transfers]
    accepted = sum(1 for r in results if r.get("success"))

    return {
        "success": True,
        "accepted_count": accepted,
        "total": len(transfers),
    }


# =============================================================================
# STATS
# =============================================================================

def get_stats() -> Dict:
    """Get overall system stats."""
    r = get_redis()

    user_count = r.scard("user_ids")
    stone_count = len(r.keys("stone:*"))

    total_value = 0
    for user_id in r.smembers("user_ids"):
        total_value += _calculate_wallet_value(user_id)

    pending_transfers = 0
    for user_id in r.smembers("user_ids"):
        pending_transfers += r.llen(f"inbox:{user_id}")

    return {
        "users": user_count,
        "stones": stone_count,
        "total_value_usd": f"${total_value / 100:.2f}",
        "pending_transfers": pending_transfers,
        "redis_connected": test_connection(),
    }

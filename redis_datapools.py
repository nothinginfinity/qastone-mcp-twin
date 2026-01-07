#!/usr/bin/env python3
"""
Redis-Backed Data Pools System for QA.Stone / Prax

Data Pools are user-owned collections of data that can be:
- Shared freely, sold, rented, or traded
- Bridged to other pools for federation
- Any data type: text, code, media, location, behavioral, exhaust

Architecture supports multiple storage adapters (Redis first, P2P later)
and multiple payment adapters (mock first, Stripe/crypto later).
"""

import os
import json
import secrets
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, asdict, field
from enum import Enum

from redis_accounts import get_redis, authenticate, get_account


# =============================================================================
# ENUMS & CONSTANTS
# =============================================================================

class PoolType(str, Enum):
    TEXT = "text"          # docs, emails, chats, spam
    CODE = "code"          # repos, configs, snippets
    MEDIA = "media"        # images, audio, video
    LOCATION = "location"  # GPS, checkins, routes
    BEHAVIORAL = "behavioral"  # clicks, usage, scrolls
    EXHAUST = "exhaust"    # deleted files, spam, logs
    MIXED = "mixed"        # multi-type bundles


class PricingModel(str, Enum):
    FREE = "free"          # open access, attribution only
    BUY = "buy"            # one-time purchase
    RENT = "rent"          # time-limited access
    TRADE = "trade"        # exchange for other data
    STAKE = "stake"        # access via token stake


class PoolVisibility(str, Enum):
    PUBLIC = "public"      # visible in marketplace
    PRIVATE = "private"    # only owner can see
    UNLISTED = "unlisted"  # accessible via direct link


# Type icons for UI
POOL_TYPE_ICONS = {
    PoolType.TEXT: "📝",
    PoolType.CODE: "💻",
    PoolType.MEDIA: "🎬",
    PoolType.LOCATION: "📍",
    PoolType.BEHAVIORAL: "🖱️",
    PoolType.EXHAUST: "🗑️",
    PoolType.MIXED: "📦",
}

PRICING_ICONS = {
    PricingModel.FREE: "🆓",
    PricingModel.BUY: "💵",
    PricingModel.RENT: "⏰",
    PricingModel.TRADE: "🔄",
    PricingModel.STAKE: "🎯",
}


# =============================================================================
# DATA MODELS
# =============================================================================

@dataclass
class PoolPricing:
    """Pricing configuration for a data pool."""
    model: str = "free"
    price_cents: int = 0              # for buy/rent
    rent_duration_days: int = 30      # for rent model
    trade_requirements: List[str] = field(default_factory=list)  # pool types accepted
    currency: str = "USD"

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> "PoolPricing":
        return cls(**data)


@dataclass
class PoolMetadata:
    """Metadata about pool contents."""
    size_bytes: int = 0
    file_count: int = 0
    format: str = ""                  # csv, json, txt, mixed
    sample_available: bool = True
    quality_score: float = 0.0        # 0-5 rating
    tags: List[str] = field(default_factory=list)
    description: str = ""

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> "PoolMetadata":
        return cls(**data)


@dataclass
class DataPool:
    """A user-owned data pool."""
    pool_id: str
    owner_id: str
    name: str
    pool_type: str
    visibility: str
    pricing: Dict
    metadata: Dict
    created_at: str
    updated_at: str
    download_count: int = 0
    rating_count: int = 0
    rating_sum: float = 0.0
    bridged_to: List[str] = field(default_factory=list)
    storage_location: str = "railway"  # railway, local, ipfs

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> "DataPool":
        # Handle missing fields for backwards compatibility
        defaults = {
            "download_count": 0,
            "rating_count": 0,
            "rating_sum": 0.0,
            "bridged_to": [],
            "storage_location": "railway"
        }
        for key, default in defaults.items():
            if key not in data:
                data[key] = default
        return cls(**data)

    @property
    def average_rating(self) -> float:
        if self.rating_count == 0:
            return 0.0
        return round(self.rating_sum / self.rating_count, 1)


@dataclass
class PoolBridge:
    """A connection between two data pools."""
    bridge_id: str
    source_pool_id: str
    target_pool_id: str
    source_owner_id: str
    target_owner_id: str
    access_type: str          # read, write, sync
    terms: Dict               # duration, cost, reciprocal
    created_at: str
    status: str = "pending"   # pending, active, expired, revoked

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> "PoolBridge":
        return cls(**data)


@dataclass
class PoolAccess:
    """Record of a user's access to a pool."""
    access_id: str
    pool_id: str
    user_id: str
    access_type: str          # owner, purchased, rented, traded, free
    granted_at: str
    expires_at: Optional[str] = None
    transaction_id: Optional[str] = None

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> "PoolAccess":
        return cls(**data)


# =============================================================================
# POOL CRUD OPERATIONS
# =============================================================================

def generate_pool_id() -> str:
    """Generate unique pool ID."""
    return f"pool_{secrets.token_hex(8)}"


def create_pool(
    owner_id: str,
    name: str,
    pool_type: str,
    visibility: str = "public",
    pricing: Optional[Dict] = None,
    metadata: Optional[Dict] = None,
) -> Dict:
    """Create a new data pool."""
    r = get_redis()

    # Verify owner exists
    if not r.exists(f"account:{owner_id}"):
        return {"success": False, "error": f"Owner {owner_id} not found"}

    # Validate pool type
    try:
        PoolType(pool_type)
    except ValueError:
        return {"success": False, "error": f"Invalid pool_type: {pool_type}"}

    pool_id = generate_pool_id()
    now = datetime.now(timezone.utc).isoformat()

    pool = DataPool(
        pool_id=pool_id,
        owner_id=owner_id,
        name=name,
        pool_type=pool_type,
        visibility=visibility,
        pricing=pricing or PoolPricing().to_dict(),
        metadata=metadata or PoolMetadata().to_dict(),
        created_at=now,
        updated_at=now,
    )

    pipe = r.pipeline()
    pipe.set(f"pool:{pool_id}", json.dumps(pool.to_dict()))
    pipe.sadd(f"user_pools:{owner_id}", pool_id)
    pipe.sadd("all_pools", pool_id)

    # Index by type for browsing
    pipe.sadd(f"pools_by_type:{pool_type}", pool_id)

    # Index by visibility
    if visibility == "public":
        pipe.sadd("public_pools", pool_id)

    pipe.execute()

    # Grant owner access
    grant_pool_access(pool_id, owner_id, "owner")

    return {
        "success": True,
        "pool_id": pool_id,
        "name": name,
        "pool_type": pool_type,
        "owner_id": owner_id,
    }


def get_pool(pool_id: str) -> Optional[DataPool]:
    """Get pool by ID."""
    r = get_redis()
    data = r.get(f"pool:{pool_id}")
    if not data:
        return None
    if isinstance(data, bytes):
        data = data.decode('utf-8')
    return DataPool.from_dict(json.loads(data))


def update_pool(pool_id: str, updates: Dict) -> Dict:
    """Update pool fields."""
    r = get_redis()
    pool = get_pool(pool_id)

    if not pool:
        return {"success": False, "error": f"Pool {pool_id} not found"}

    pool_dict = pool.to_dict()

    # Update allowed fields
    allowed = ["name", "visibility", "pricing", "metadata"]
    for key in allowed:
        if key in updates:
            pool_dict[key] = updates[key]

    pool_dict["updated_at"] = datetime.now(timezone.utc).isoformat()

    r.set(f"pool:{pool_id}", json.dumps(pool_dict))

    # Update visibility index
    if "visibility" in updates:
        if updates["visibility"] == "public":
            r.sadd("public_pools", pool_id)
        else:
            r.srem("public_pools", pool_id)

    return {"success": True, "pool_id": pool_id}


def delete_pool(pool_id: str, owner_id: str) -> Dict:
    """Delete a pool (owner only)."""
    r = get_redis()
    pool = get_pool(pool_id)

    if not pool:
        return {"success": False, "error": f"Pool {pool_id} not found"}

    if pool.owner_id != owner_id:
        return {"success": False, "error": "Only owner can delete pool"}

    pipe = r.pipeline()
    pipe.delete(f"pool:{pool_id}")
    pipe.srem(f"user_pools:{owner_id}", pool_id)
    pipe.srem("all_pools", pool_id)
    pipe.srem("public_pools", pool_id)
    pipe.srem(f"pools_by_type:{pool.pool_type}", pool_id)

    # Remove all access records
    access_keys = r.keys(f"pool_access:{pool_id}:*")
    for key in access_keys:
        pipe.delete(key)

    pipe.execute()

    return {"success": True, "pool_id": pool_id, "deleted": True}


# =============================================================================
# POOL DISCOVERY & BROWSING
# =============================================================================

def list_public_pools(
    pool_type: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    sort_by: str = "recent"  # recent, popular, rating
) -> List[Dict]:
    """List public pools for marketplace browsing."""
    r = get_redis()

    if pool_type:
        pool_ids = r.smembers(f"pools_by_type:{pool_type}")
        # Filter to only public
        public_ids = r.smembers("public_pools")
        pool_ids = pool_ids & public_ids
    else:
        pool_ids = r.smembers("public_pools")

    pools = []
    for pool_id in pool_ids:
        if isinstance(pool_id, bytes):
            pool_id = pool_id.decode('utf-8')
        pool = get_pool(pool_id)
        if pool:
            pools.append(pool.to_dict())

    # Sort
    if sort_by == "recent":
        pools.sort(key=lambda x: x["created_at"], reverse=True)
    elif sort_by == "popular":
        pools.sort(key=lambda x: x["download_count"], reverse=True)
    elif sort_by == "rating":
        pools.sort(key=lambda x: x.get("rating_sum", 0) / max(x.get("rating_count", 1), 1), reverse=True)

    return pools[offset:offset + limit]


def search_pools(query: str, limit: int = 20) -> List[Dict]:
    """Search pools by name/description/tags."""
    r = get_redis()
    pool_ids = r.smembers("public_pools")

    query_lower = query.lower()
    results = []

    for pool_id in pool_ids:
        if isinstance(pool_id, bytes):
            pool_id = pool_id.decode('utf-8')
        pool = get_pool(pool_id)
        if not pool:
            continue

        # Search in name, description, tags
        searchable = [
            pool.name.lower(),
            pool.metadata.get("description", "").lower(),
            " ".join(pool.metadata.get("tags", [])).lower()
        ]

        if any(query_lower in s for s in searchable):
            results.append(pool.to_dict())

    return results[:limit]


def get_user_pools(user_id: str) -> List[Dict]:
    """Get all pools owned by a user."""
    r = get_redis()
    pool_ids = r.smembers(f"user_pools:{user_id}")

    pools = []
    for pool_id in pool_ids:
        if isinstance(pool_id, bytes):
            pool_id = pool_id.decode('utf-8')
        pool = get_pool(pool_id)
        if pool:
            pools.append(pool.to_dict())

    return sorted(pools, key=lambda x: x["created_at"], reverse=True)


def get_accessible_pools(user_id: str) -> List[Dict]:
    """Get all pools a user has access to (owned + acquired)."""
    r = get_redis()

    # Get all access records for user
    access_keys = r.keys(f"pool_access:*:{user_id}")

    pools = []
    seen = set()

    for key in access_keys:
        if isinstance(key, bytes):
            key = key.decode('utf-8')
        # Extract pool_id from key pattern pool_access:{pool_id}:{user_id}
        parts = key.split(":")
        if len(parts) >= 2:
            pool_id = parts[1]
            if pool_id not in seen:
                seen.add(pool_id)
                pool = get_pool(pool_id)
                if pool:
                    access_data = r.get(key)
                    if access_data:
                        if isinstance(access_data, bytes):
                            access_data = access_data.decode('utf-8')
                        access = json.loads(access_data)
                        pool_dict = pool.to_dict()
                        pool_dict["access_type"] = access.get("access_type")
                        pools.append(pool_dict)

    return pools


# =============================================================================
# POOL ACCESS & ACQUISITION
# =============================================================================

def grant_pool_access(
    pool_id: str,
    user_id: str,
    access_type: str,
    expires_at: Optional[str] = None,
    transaction_id: Optional[str] = None
) -> Dict:
    """Grant a user access to a pool."""
    r = get_redis()

    access_id = f"access_{secrets.token_hex(6)}"
    now = datetime.now(timezone.utc).isoformat()

    access = PoolAccess(
        access_id=access_id,
        pool_id=pool_id,
        user_id=user_id,
        access_type=access_type,
        granted_at=now,
        expires_at=expires_at,
        transaction_id=transaction_id,
    )

    r.set(f"pool_access:{pool_id}:{user_id}", json.dumps(access.to_dict()))
    r.sadd(f"user_pool_access:{user_id}", pool_id)

    return {"success": True, "access_id": access_id}


def check_pool_access(pool_id: str, user_id: str) -> Optional[Dict]:
    """Check if user has access to a pool."""
    r = get_redis()
    data = r.get(f"pool_access:{pool_id}:{user_id}")
    if not data:
        return None
    if isinstance(data, bytes):
        data = data.decode('utf-8')
    access = json.loads(data)

    # Check expiration
    if access.get("expires_at"):
        expires = datetime.fromisoformat(access["expires_at"].replace('Z', '+00:00'))
        if datetime.now(timezone.utc) > expires:
            return None

    return access


def acquire_pool(pool_id: str, user_id: str, payment_method: str = "mock") -> Dict:
    """
    Acquire access to a pool (mock payment for now).

    payment_method: mock, credits, stripe, crypto
    """
    r = get_redis()
    pool = get_pool(pool_id)

    if not pool:
        return {"success": False, "error": f"Pool {pool_id} not found"}

    # Check if already has access
    existing = check_pool_access(pool_id, user_id)
    if existing:
        return {"success": False, "error": "Already have access to this pool"}

    pricing = pool.pricing
    model = pricing.get("model", "free")

    # Mock payment processing
    transaction_id = f"tx_{secrets.token_hex(8)}"
    expires_at = None

    if model == "rent":
        days = pricing.get("rent_duration_days", 30)
        expires_at = (datetime.now(timezone.utc).replace(
            day=datetime.now().day + days
        )).isoformat()

    # Grant access
    grant_pool_access(
        pool_id=pool_id,
        user_id=user_id,
        access_type=f"acquired_{model}",
        expires_at=expires_at,
        transaction_id=transaction_id,
    )

    # Update download count
    pool_dict = pool.to_dict()
    pool_dict["download_count"] = pool_dict.get("download_count", 0) + 1
    r.set(f"pool:{pool_id}", json.dumps(pool_dict))

    return {
        "success": True,
        "pool_id": pool_id,
        "access_type": f"acquired_{model}",
        "transaction_id": transaction_id,
        "expires_at": expires_at,
        "payment_method": payment_method,
        "amount_cents": pricing.get("price_cents", 0),
    }


# =============================================================================
# POOL BRIDGING
# =============================================================================

def create_bridge(
    source_pool_id: str,
    target_pool_id: str,
    requester_id: str,
    access_type: str = "read",
    terms: Optional[Dict] = None
) -> Dict:
    """Request a bridge between two pools."""
    r = get_redis()

    source = get_pool(source_pool_id)
    target = get_pool(target_pool_id)

    if not source:
        return {"success": False, "error": f"Source pool {source_pool_id} not found"}
    if not target:
        return {"success": False, "error": f"Target pool {target_pool_id} not found"}

    # Requester must own source pool
    if source.owner_id != requester_id:
        return {"success": False, "error": "Must own source pool to create bridge"}

    bridge_id = f"bridge_{secrets.token_hex(8)}"
    now = datetime.now(timezone.utc).isoformat()

    bridge = PoolBridge(
        bridge_id=bridge_id,
        source_pool_id=source_pool_id,
        target_pool_id=target_pool_id,
        source_owner_id=source.owner_id,
        target_owner_id=target.owner_id,
        access_type=access_type,
        terms=terms or {},
        created_at=now,
        status="pending" if source.owner_id != target.owner_id else "active",
    )

    pipe = r.pipeline()
    pipe.set(f"bridge:{bridge_id}", json.dumps(bridge.to_dict()))
    pipe.sadd(f"bridges_from:{source_pool_id}", bridge_id)
    pipe.sadd(f"bridges_to:{target_pool_id}", bridge_id)
    pipe.sadd(f"user_bridges:{source.owner_id}", bridge_id)

    # If target owner is different, add to their pending bridges
    if source.owner_id != target.owner_id:
        pipe.sadd(f"pending_bridges:{target.owner_id}", bridge_id)

    pipe.execute()

    return {
        "success": True,
        "bridge_id": bridge_id,
        "status": bridge.status,
        "source_pool": source_pool_id,
        "target_pool": target_pool_id,
    }


def accept_bridge(bridge_id: str, user_id: str) -> Dict:
    """Accept a pending bridge request."""
    r = get_redis()

    data = r.get(f"bridge:{bridge_id}")
    if not data:
        return {"success": False, "error": f"Bridge {bridge_id} not found"}

    if isinstance(data, bytes):
        data = data.decode('utf-8')
    bridge = PoolBridge.from_dict(json.loads(data))

    if bridge.target_owner_id != user_id:
        return {"success": False, "error": "Only target pool owner can accept"}

    if bridge.status != "pending":
        return {"success": False, "error": f"Bridge is {bridge.status}, not pending"}

    bridge_dict = bridge.to_dict()
    bridge_dict["status"] = "active"

    pipe = r.pipeline()
    pipe.set(f"bridge:{bridge_id}", json.dumps(bridge_dict))
    pipe.srem(f"pending_bridges:{user_id}", bridge_id)
    pipe.sadd(f"user_bridges:{user_id}", bridge_id)

    # Update pools' bridged_to lists
    source = get_pool(bridge.source_pool_id)
    target = get_pool(bridge.target_pool_id)

    if source:
        source_dict = source.to_dict()
        source_dict["bridged_to"] = list(set(source_dict.get("bridged_to", []) + [bridge.target_pool_id]))
        pipe.set(f"pool:{bridge.source_pool_id}", json.dumps(source_dict))

    if target:
        target_dict = target.to_dict()
        target_dict["bridged_to"] = list(set(target_dict.get("bridged_to", []) + [bridge.source_pool_id]))
        pipe.set(f"pool:{bridge.target_pool_id}", json.dumps(target_dict))

    pipe.execute()

    return {"success": True, "bridge_id": bridge_id, "status": "active"}


def get_pool_bridges(pool_id: str) -> List[Dict]:
    """Get all bridges for a pool."""
    r = get_redis()

    from_ids = r.smembers(f"bridges_from:{pool_id}")
    to_ids = r.smembers(f"bridges_to:{pool_id}")

    all_ids = from_ids | to_ids
    bridges = []

    for bridge_id in all_ids:
        if isinstance(bridge_id, bytes):
            bridge_id = bridge_id.decode('utf-8')
        data = r.get(f"bridge:{bridge_id}")
        if data:
            if isinstance(data, bytes):
                data = data.decode('utf-8')
            bridges.append(json.loads(data))

    return bridges


def get_pending_bridges(user_id: str) -> List[Dict]:
    """Get bridges pending user approval."""
    r = get_redis()
    bridge_ids = r.smembers(f"pending_bridges:{user_id}")

    bridges = []
    for bridge_id in bridge_ids:
        if isinstance(bridge_id, bytes):
            bridge_id = bridge_id.decode('utf-8')
        data = r.get(f"bridge:{bridge_id}")
        if data:
            if isinstance(data, bytes):
                data = data.decode('utf-8')
            bridges.append(json.loads(data))

    return bridges


# =============================================================================
# RATINGS
# =============================================================================

def rate_pool(pool_id: str, user_id: str, rating: float) -> Dict:
    """Rate a pool (1-5 stars)."""
    r = get_redis()

    if rating < 1 or rating > 5:
        return {"success": False, "error": "Rating must be 1-5"}

    pool = get_pool(pool_id)
    if not pool:
        return {"success": False, "error": f"Pool {pool_id} not found"}

    # Check if user has access
    access = check_pool_access(pool_id, user_id)
    if not access:
        return {"success": False, "error": "Must have access to rate"}

    # Check if already rated
    existing = r.get(f"rating:{pool_id}:{user_id}")

    pool_dict = pool.to_dict()

    if existing:
        # Update existing rating
        if isinstance(existing, bytes):
            existing = existing.decode('utf-8')
        old_rating = float(existing)
        pool_dict["rating_sum"] = pool_dict.get("rating_sum", 0) - old_rating + rating
    else:
        # New rating
        pool_dict["rating_count"] = pool_dict.get("rating_count", 0) + 1
        pool_dict["rating_sum"] = pool_dict.get("rating_sum", 0) + rating

    pipe = r.pipeline()
    pipe.set(f"pool:{pool_id}", json.dumps(pool_dict))
    pipe.set(f"rating:{pool_id}:{user_id}", str(rating))
    pipe.execute()

    return {
        "success": True,
        "pool_id": pool_id,
        "rating": rating,
        "new_average": round(pool_dict["rating_sum"] / pool_dict["rating_count"], 1),
    }


# =============================================================================
# STATS
# =============================================================================

def get_pool_stats() -> Dict:
    """Get overall pool marketplace stats."""
    r = get_redis()

    total_pools = r.scard("all_pools")
    public_pools = r.scard("public_pools")

    # Count by type
    by_type = {}
    for pool_type in PoolType:
        count = r.scard(f"pools_by_type:{pool_type.value}")
        if count > 0:
            by_type[pool_type.value] = count

    return {
        "total_pools": total_pools,
        "public_pools": public_pools,
        "by_type": by_type,
    }


def get_user_pool_stats(user_id: str) -> Dict:
    """Get stats for a user's pools."""
    pools = get_user_pools(user_id)

    total_downloads = sum(p.get("download_count", 0) for p in pools)
    total_size = sum(p.get("metadata", {}).get("size_bytes", 0) for p in pools)

    # Mock earnings (for demo)
    mock_earnings_cents = total_downloads * 10  # $0.10 per download demo

    return {
        "pool_count": len(pools),
        "total_downloads": total_downloads,
        "total_size_bytes": total_size,
        "mock_earnings_cents": mock_earnings_cents,
        "mock_earnings_usd": f"${mock_earnings_cents / 100:.2f}",
    }

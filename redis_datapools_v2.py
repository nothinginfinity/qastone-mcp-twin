#!/usr/bin/env python3
"""
Data Pools V2 - Personal Data Vault + Marketplace + Social Graph

Philosophy:
- Data Pools are YOUR comprehensive data layer
- Bridges are how users connect (not followers/friends)
- Privacy-first with granular controls
- Shareable links for social distribution

Pool Categories:
- conversations: LLM chats, message history
- prompts: Prompt libraries, templates, system prompts
- integrations: MCP servers, API configs, tool setups
- media: Photos, videos, audio files
- documents: Books, notes, PDFs, writings
- analytics: Clicks, behavior, usage patterns
- location: GPS traces, places, routes
- training_data: Datasets for ML/AI training
- code: Snippets, repositories, configs
- creative: Art, music, AI-generated content
- contacts: Address books (anonymized)
- raw: Unstructured data, exports, dumps

Bridge Types:
- invite_link: Shareable URL with expiration
- direct_request: User requests access to pool
- mutual: Bidirectional data exchange
- one_way_read: Read-only access
- one_way_derive: Can use to create derivatives
"""

import os
import json
import secrets
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass, asdict, field
from enum import Enum

from redis_accounts import get_redis, authenticate, get_account


# =============================================================================
# ENUMS & CONSTANTS
# =============================================================================

class PoolCategory(str, Enum):
    """Categories of data pools - what kind of data is this?"""
    CONVERSATIONS = "conversations"    # LLM chats, messages
    PROMPTS = "prompts"               # Prompt libraries, templates
    INTEGRATIONS = "integrations"     # MCP servers, API configs
    MEDIA = "media"                   # Photos, videos, audio
    DOCUMENTS = "documents"           # Books, notes, PDFs
    ANALYTICS = "analytics"           # Behavior, clicks, usage
    LOCATION = "location"             # GPS, places, routes
    TRAINING_DATA = "training_data"   # ML datasets
    CODE = "code"                     # Snippets, repos
    CREATIVE = "creative"             # Art, music, generated
    CONTACTS = "contacts"             # Address books
    RAW = "raw"                       # Unstructured dumps


class PoolVisibility(str, Enum):
    """Who can see this pool exists?"""
    PRIVATE = "private"       # Only owner sees it
    UNLISTED = "unlisted"     # Accessible via direct link only
    PUBLIC = "public"         # Visible in marketplace


class SharingMode(str, Enum):
    """Is sharing enabled for this pool?"""
    OFF = "off"               # No sharing allowed
    LINK_ONLY = "link_only"   # Only via invite links
    REQUEST = "request"       # Users can request access
    OPEN = "open"             # Anyone can access (still need bridge)


class AccessLevel(str, Enum):
    """What can bridged users do with this data?"""
    VIEW = "view"             # Can view/read only
    DOWNLOAD = "download"     # Can download copies
    DERIVE = "derive"         # Can use to create derivatives
    FULL = "full"             # Full access (rare)


class BridgeType(str, Enum):
    """How was this bridge created?"""
    INVITE_LINK = "invite_link"       # Via shareable link
    DIRECT_REQUEST = "direct_request" # User requested access
    MUTUAL = "mutual"                 # Bidirectional exchange
    GRANTED = "granted"               # Owner granted directly


class BridgeStatus(str, Enum):
    """Current state of bridge"""
    PENDING = "pending"       # Awaiting acceptance
    ACTIVE = "active"         # Bridge is live
    EXPIRED = "expired"       # Time-limited bridge expired
    REVOKED = "revoked"       # Owner revoked access
    DECLINED = "declined"     # Request was declined


# Category metadata
CATEGORY_INFO = {
    PoolCategory.CONVERSATIONS: {
        "icon": "🗣️",
        "label": "Conversations",
        "description": "LLM chats, message history, dialogues",
        "examples": ["Claude conversations", "GPT chats", "Support tickets"],
    },
    PoolCategory.PROMPTS: {
        "icon": "📝",
        "label": "Prompts",
        "description": "Prompt libraries, templates, system prompts",
        "examples": ["System prompts", "Jailbreaks", "Role templates"],
    },
    PoolCategory.INTEGRATIONS: {
        "icon": "🔌",
        "label": "Integrations",
        "description": "MCP servers, API configurations, tool setups",
        "examples": ["MCP configs", "API keys (encrypted)", "Webhook setups"],
    },
    PoolCategory.MEDIA: {
        "icon": "📸",
        "label": "Media",
        "description": "Photos, videos, audio files",
        "examples": ["Photo albums", "Voice recordings", "Video clips"],
    },
    PoolCategory.DOCUMENTS: {
        "icon": "📚",
        "label": "Documents",
        "description": "Books, notes, PDFs, writings",
        "examples": ["Research papers", "Personal notes", "E-books"],
    },
    PoolCategory.ANALYTICS: {
        "icon": "📊",
        "label": "Analytics",
        "description": "Behavior data, clicks, usage patterns",
        "examples": ["Click streams", "App usage", "Browsing history"],
    },
    PoolCategory.LOCATION: {
        "icon": "📍",
        "label": "Location",
        "description": "GPS traces, places, routes",
        "examples": ["Travel history", "Commute patterns", "Check-ins"],
    },
    PoolCategory.TRAINING_DATA: {
        "icon": "🧠",
        "label": "Training Data",
        "description": "Datasets for ML/AI training",
        "examples": ["Labeled datasets", "Fine-tuning data", "Test sets"],
    },
    PoolCategory.CODE: {
        "icon": "💻",
        "label": "Code",
        "description": "Snippets, repositories, configurations",
        "examples": ["GitHub repos", "Config files", "Scripts"],
    },
    PoolCategory.CREATIVE: {
        "icon": "🎨",
        "label": "Creative",
        "description": "Art, music, AI-generated content",
        "examples": ["AI art", "Generated music", "Stories"],
    },
    PoolCategory.CONTACTS: {
        "icon": "👥",
        "label": "Contacts",
        "description": "Address books, connections (anonymized)",
        "examples": ["Email contacts", "Phone contacts", "Social graph"],
    },
    PoolCategory.RAW: {
        "icon": "📦",
        "label": "Raw Data",
        "description": "Unstructured data, exports, dumps",
        "examples": ["Data exports", "Backup dumps", "Scraped data"],
    },
}


# =============================================================================
# DATA MODELS
# =============================================================================

@dataclass
class PoolPrivacy:
    """Privacy and sharing settings for a pool"""
    visibility: str = "private"
    sharing_mode: str = "off"
    access_level: str = "view"
    require_approval: bool = True
    allow_derivatives: bool = False
    track_usage: bool = True
    watermark: bool = False  # Add watermark to downloads

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> "PoolPrivacy":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class PoolMetadata:
    """Metadata about pool contents"""
    description: str = ""
    tags: List[str] = field(default_factory=list)
    size_bytes: int = 0
    item_count: int = 0
    format: str = ""  # json, csv, txt, mixed, etc.
    schema: Optional[Dict] = None  # Data schema if structured
    source: str = ""  # Where did this data come from?
    date_range: Optional[Dict] = None  # {start: "", end: ""}
    sample_available: bool = True
    quality_score: float = 0.0
    verified: bool = False  # Has data been verified?

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> "PoolMetadata":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class PoolPricing:
    """Pricing for pool access"""
    model: str = "free"  # free, one_time, subscription, trade, credits
    price_cents: int = 0
    currency: str = "USD"
    subscription_days: int = 30
    trade_for: List[str] = field(default_factory=list)  # Pool categories accepted in trade
    credits_required: int = 0

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> "PoolPricing":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class DataPoolV2:
    """Enhanced Data Pool model"""
    pool_id: str
    owner_id: str
    name: str
    category: str  # PoolCategory value
    privacy: Dict  # PoolPrivacy as dict
    metadata: Dict  # PoolMetadata as dict
    pricing: Dict  # PoolPricing as dict
    created_at: str
    updated_at: str

    # Stats
    view_count: int = 0
    download_count: int = 0
    bridge_count: int = 0
    rating_sum: float = 0.0
    rating_count: int = 0

    # Data source info
    source_type: str = "manual"  # manual, api, sync, import
    source_id: Optional[str] = None  # ID of connected source
    auto_sync: bool = False
    last_sync: Optional[str] = None

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> "DataPoolV2":
        defaults = {
            "view_count": 0,
            "download_count": 0,
            "bridge_count": 0,
            "rating_sum": 0.0,
            "rating_count": 0,
            "source_type": "manual",
            "source_id": None,
            "auto_sync": False,
            "last_sync": None,
        }
        for key, default in defaults.items():
            if key not in data:
                data[key] = default
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    @property
    def average_rating(self) -> float:
        if self.rating_count == 0:
            return 0.0
        return round(self.rating_sum / self.rating_count, 1)

    @property
    def category_info(self) -> Dict:
        try:
            return CATEGORY_INFO.get(PoolCategory(self.category), CATEGORY_INFO[PoolCategory.RAW])
        except:
            return CATEGORY_INFO[PoolCategory.RAW]


@dataclass
class BridgeInviteLink:
    """Shareable link to create a bridge"""
    link_id: str
    pool_id: str
    owner_id: str
    created_at: str
    expires_at: Optional[str] = None
    max_uses: int = 0  # 0 = unlimited
    use_count: int = 0
    access_level: str = "view"
    message: str = ""  # Custom message from owner
    active: bool = True

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> "BridgeInviteLink":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    @property
    def short_code(self) -> str:
        """Short code for sharing"""
        return self.link_id[:8]

    @property
    def is_valid(self) -> bool:
        if not self.active:
            return False
        if self.max_uses > 0 and self.use_count >= self.max_uses:
            return False
        if self.expires_at:
            expires = datetime.fromisoformat(self.expires_at.replace('Z', '+00:00'))
            if datetime.now(timezone.utc) > expires:
                return False
        return True


@dataclass
class BridgeV2:
    """Enhanced bridge between users via pools"""
    bridge_id: str
    source_pool_id: str
    target_user_id: str  # Who has access
    owner_id: str  # Pool owner
    bridge_type: str  # BridgeType value
    status: str  # BridgeStatus value
    access_level: str  # AccessLevel value
    created_at: str
    updated_at: str

    # Optional fields
    expires_at: Optional[str] = None
    invite_link_id: Optional[str] = None
    request_message: str = ""
    decline_reason: str = ""

    # Usage tracking
    access_count: int = 0
    last_accessed: Optional[str] = None
    data_transferred_bytes: int = 0

    # Mutual bridge (if applicable)
    mutual_pool_id: Optional[str] = None  # Other user's pool in exchange

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> "BridgeV2":
        defaults = {
            "expires_at": None,
            "invite_link_id": None,
            "request_message": "",
            "decline_reason": "",
            "access_count": 0,
            "last_accessed": None,
            "data_transferred_bytes": 0,
            "mutual_pool_id": None,
        }
        for key, default in defaults.items():
            if key not in data:
                data[key] = default
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


# =============================================================================
# POOL OPERATIONS
# =============================================================================

def generate_pool_id() -> str:
    return f"pool_{secrets.token_hex(12)}"


def generate_link_id() -> str:
    return f"link_{secrets.token_hex(8)}"


def generate_bridge_id() -> str:
    return f"bridge_{secrets.token_hex(10)}"


def create_pool_v2(
    owner_id: str,
    name: str,
    category: str,
    privacy: Optional[Dict] = None,
    metadata: Optional[Dict] = None,
    pricing: Optional[Dict] = None,
    source_type: str = "manual",
) -> Dict:
    """Create a new data pool with full controls"""
    r = get_redis()

    if not r.exists(f"account:{owner_id}"):
        return {"success": False, "error": f"Owner {owner_id} not found"}

    # Validate category
    try:
        PoolCategory(category)
    except ValueError:
        return {"success": False, "error": f"Invalid category: {category}"}

    pool_id = generate_pool_id()
    now = datetime.now(timezone.utc).isoformat()

    pool = DataPoolV2(
        pool_id=pool_id,
        owner_id=owner_id,
        name=name,
        category=category,
        privacy=privacy or PoolPrivacy().to_dict(),
        metadata=metadata or PoolMetadata().to_dict(),
        pricing=pricing or PoolPricing().to_dict(),
        created_at=now,
        updated_at=now,
        source_type=source_type,
    )

    pipe = r.pipeline()
    pipe.set(f"pool_v2:{pool_id}", json.dumps(pool.to_dict()))
    pipe.sadd(f"user_pools_v2:{owner_id}", pool_id)
    pipe.sadd("all_pools_v2", pool_id)
    pipe.sadd(f"pools_by_category:{category}", pool_id)

    # Index by visibility
    visibility = pool.privacy.get("visibility", "private")
    if visibility == "public":
        pipe.sadd("public_pools_v2", pool_id)

    pipe.execute()

    cat_info = CATEGORY_INFO.get(PoolCategory(category), {})
    return {
        "success": True,
        "pool_id": pool_id,
        "name": name,
        "category": category,
        "category_icon": cat_info.get("icon", "📦"),
        "owner_id": owner_id,
    }


def get_pool_v2(pool_id: str) -> Optional[DataPoolV2]:
    """Get pool by ID"""
    r = get_redis()
    data = r.get(f"pool_v2:{pool_id}")
    if not data:
        return None
    if isinstance(data, bytes):
        data = data.decode('utf-8')
    return DataPoolV2.from_dict(json.loads(data))


def update_pool_v2(pool_id: str, updates: Dict) -> Dict:
    """Update pool fields"""
    r = get_redis()
    pool = get_pool_v2(pool_id)

    if not pool:
        return {"success": False, "error": "Pool not found"}

    pool_dict = pool.to_dict()

    # Update allowed fields
    allowed = ["name", "privacy", "metadata", "pricing", "auto_sync"]
    for key in allowed:
        if key in updates:
            pool_dict[key] = updates[key]

    pool_dict["updated_at"] = datetime.now(timezone.utc).isoformat()

    # Update visibility index
    old_visibility = pool.privacy.get("visibility", "private")
    new_visibility = updates.get("privacy", {}).get("visibility", old_visibility)

    pipe = r.pipeline()
    pipe.set(f"pool_v2:{pool_id}", json.dumps(pool_dict))

    if old_visibility != new_visibility:
        if new_visibility == "public":
            pipe.sadd("public_pools_v2", pool_id)
        else:
            pipe.srem("public_pools_v2", pool_id)

    pipe.execute()
    return {"success": True, "pool_id": pool_id}


def get_user_pools_v2(user_id: str) -> List[Dict]:
    """Get all pools owned by a user"""
    r = get_redis()
    pool_ids = r.smembers(f"user_pools_v2:{user_id}")

    pools = []
    for pool_id in pool_ids:
        if isinstance(pool_id, bytes):
            pool_id = pool_id.decode('utf-8')
        pool = get_pool_v2(pool_id)
        if pool:
            pool_dict = pool.to_dict()
            pool_dict["category_info"] = pool.category_info
            pools.append(pool_dict)

    return sorted(pools, key=lambda x: x["created_at"], reverse=True)


def list_public_pools_v2(
    category: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> List[Dict]:
    """List public pools"""
    r = get_redis()

    if category:
        pool_ids = r.smembers(f"pools_by_category:{category}")
        public_ids = r.smembers("public_pools_v2")
        pool_ids = pool_ids & public_ids
    else:
        pool_ids = r.smembers("public_pools_v2")

    pools = []
    for pool_id in pool_ids:
        if isinstance(pool_id, bytes):
            pool_id = pool_id.decode('utf-8')
        pool = get_pool_v2(pool_id)
        if pool:
            # Check sharing mode allows discovery
            sharing = pool.privacy.get("sharing_mode", "off")
            if sharing != "off":
                pool_dict = pool.to_dict()
                pool_dict["category_info"] = pool.category_info
                pools.append(pool_dict)

    pools.sort(key=lambda x: x["created_at"], reverse=True)
    return pools[offset:offset + limit]


# =============================================================================
# INVITE LINKS
# =============================================================================

def create_invite_link(
    pool_id: str,
    owner_id: str,
    access_level: str = "view",
    expires_in_hours: Optional[int] = None,
    max_uses: int = 0,
    message: str = "",
) -> Dict:
    """Create a shareable invite link for a pool"""
    r = get_redis()

    pool = get_pool_v2(pool_id)
    if not pool:
        return {"success": False, "error": "Pool not found"}

    if pool.owner_id != owner_id:
        return {"success": False, "error": "Only owner can create invite links"}

    link_id = generate_link_id()
    now = datetime.now(timezone.utc)

    expires_at = None
    if expires_in_hours:
        expires_at = (now + timedelta(hours=expires_in_hours)).isoformat()

    link = BridgeInviteLink(
        link_id=link_id,
        pool_id=pool_id,
        owner_id=owner_id,
        created_at=now.isoformat(),
        expires_at=expires_at,
        max_uses=max_uses,
        access_level=access_level,
        message=message,
    )

    pipe = r.pipeline()
    pipe.set(f"invite_link:{link_id}", json.dumps(link.to_dict()))
    pipe.sadd(f"pool_links:{pool_id}", link_id)
    pipe.execute()

    # Generate shareable URL
    share_url = f"prax://bridge/{link.short_code}"

    return {
        "success": True,
        "link_id": link_id,
        "short_code": link.short_code,
        "share_url": share_url,
        "expires_at": expires_at,
        "max_uses": max_uses,
    }


def get_invite_link(link_id: str) -> Optional[BridgeInviteLink]:
    """Get invite link by ID or short code"""
    r = get_redis()

    # Try full ID first
    data = r.get(f"invite_link:{link_id}")

    # Try as short code
    if not data:
        # Search for link with this short code
        all_links = r.keys("invite_link:*")
        for key in all_links:
            link_data = r.get(key)
            if link_data:
                if isinstance(link_data, bytes):
                    link_data = link_data.decode('utf-8')
                link = BridgeInviteLink.from_dict(json.loads(link_data))
                if link.short_code == link_id:
                    return link if link.is_valid else None

        return None

    if isinstance(data, bytes):
        data = data.decode('utf-8')

    link = BridgeInviteLink.from_dict(json.loads(data))
    return link if link.is_valid else None


def use_invite_link(link_id: str, user_id: str) -> Dict:
    """Use an invite link to create a bridge"""
    r = get_redis()

    link = get_invite_link(link_id)
    if not link:
        return {"success": False, "error": "Invalid or expired invite link"}

    if link.owner_id == user_id:
        return {"success": False, "error": "Cannot use your own invite link"}

    # Check if bridge already exists
    existing = check_bridge_exists(link.pool_id, user_id)
    if existing:
        return {"success": False, "error": "Bridge already exists"}

    # Create bridge
    bridge_result = create_bridge_v2(
        pool_id=link.pool_id,
        target_user_id=user_id,
        bridge_type="invite_link",
        access_level=link.access_level,
        invite_link_id=link.link_id,
        auto_accept=True,  # Invite links auto-accept
    )

    if bridge_result.get("success"):
        # Update link use count
        link.use_count += 1
        r.set(f"invite_link:{link.link_id}", json.dumps(link.to_dict()))

    return bridge_result


def get_pool_invite_links(pool_id: str) -> List[Dict]:
    """Get all invite links for a pool"""
    r = get_redis()
    link_ids = r.smembers(f"pool_links:{pool_id}")

    links = []
    for link_id in link_ids:
        if isinstance(link_id, bytes):
            link_id = link_id.decode('utf-8')
        data = r.get(f"invite_link:{link_id}")
        if data:
            if isinstance(data, bytes):
                data = data.decode('utf-8')
            link = BridgeInviteLink.from_dict(json.loads(data))
            link_dict = link.to_dict()
            link_dict["is_valid"] = link.is_valid
            link_dict["short_code"] = link.short_code
            links.append(link_dict)

    return links


def revoke_invite_link(link_id: str, owner_id: str) -> Dict:
    """Revoke an invite link"""
    r = get_redis()

    data = r.get(f"invite_link:{link_id}")
    if not data:
        return {"success": False, "error": "Link not found"}

    if isinstance(data, bytes):
        data = data.decode('utf-8')

    link = BridgeInviteLink.from_dict(json.loads(data))

    if link.owner_id != owner_id:
        return {"success": False, "error": "Only owner can revoke"}

    link.active = False
    r.set(f"invite_link:{link_id}", json.dumps(link.to_dict()))

    return {"success": True, "link_id": link_id}


# =============================================================================
# BRIDGE OPERATIONS
# =============================================================================

def check_bridge_exists(pool_id: str, user_id: str) -> bool:
    """Check if a bridge already exists"""
    r = get_redis()
    return r.exists(f"bridge_v2:{pool_id}:{user_id}") > 0


def create_bridge_v2(
    pool_id: str,
    target_user_id: str,
    bridge_type: str = "direct_request",
    access_level: str = "view",
    request_message: str = "",
    invite_link_id: Optional[str] = None,
    mutual_pool_id: Optional[str] = None,
    auto_accept: bool = False,
) -> Dict:
    """Create a bridge (connection) to a pool"""
    r = get_redis()

    pool = get_pool_v2(pool_id)
    if not pool:
        return {"success": False, "error": "Pool not found"}

    if pool.owner_id == target_user_id:
        return {"success": False, "error": "Cannot bridge to your own pool"}

    # Check if bridge exists
    if check_bridge_exists(pool_id, target_user_id):
        return {"success": False, "error": "Bridge already exists"}

    # Check privacy settings
    privacy = PoolPrivacy.from_dict(pool.privacy)
    if privacy.sharing_mode == "off":
        return {"success": False, "error": "Pool has sharing disabled"}

    bridge_id = generate_bridge_id()
    now = datetime.now(timezone.utc).isoformat()

    # Determine initial status
    if auto_accept or not privacy.require_approval:
        status = "active"
    else:
        status = "pending"

    bridge = BridgeV2(
        bridge_id=bridge_id,
        source_pool_id=pool_id,
        target_user_id=target_user_id,
        owner_id=pool.owner_id,
        bridge_type=bridge_type,
        status=status,
        access_level=access_level,
        created_at=now,
        updated_at=now,
        invite_link_id=invite_link_id,
        request_message=request_message,
        mutual_pool_id=mutual_pool_id,
    )

    pipe = r.pipeline()
    pipe.set(f"bridge_v2:{pool_id}:{target_user_id}", json.dumps(bridge.to_dict()))
    pipe.sadd(f"pool_bridges:{pool_id}", bridge_id)
    pipe.sadd(f"user_bridges_in:{target_user_id}", bridge_id)
    pipe.sadd(f"user_bridges_out:{pool.owner_id}", bridge_id)

    if status == "pending":
        pipe.sadd(f"pending_bridges:{pool.owner_id}", bridge_id)
    else:
        # Update pool bridge count
        pool_dict = pool.to_dict()
        pool_dict["bridge_count"] = pool_dict.get("bridge_count", 0) + 1
        pipe.set(f"pool_v2:{pool_id}", json.dumps(pool_dict))

    pipe.execute()

    return {
        "success": True,
        "bridge_id": bridge_id,
        "status": status,
        "pool_id": pool_id,
        "pool_name": pool.name,
        "access_level": access_level,
    }


def request_bridge(
    pool_id: str,
    requester_id: str,
    message: str = "",
    offer_pool_id: Optional[str] = None,  # For trade/mutual bridges
) -> Dict:
    """Request access to a pool (creates pending bridge)"""
    return create_bridge_v2(
        pool_id=pool_id,
        target_user_id=requester_id,
        bridge_type="direct_request" if not offer_pool_id else "mutual",
        request_message=message,
        mutual_pool_id=offer_pool_id,
    )


def respond_to_bridge_request(
    bridge_id: str,
    owner_id: str,
    accept: bool,
    decline_reason: str = "",
) -> Dict:
    """Accept or decline a bridge request"""
    r = get_redis()

    # Find the bridge
    bridge_data = None
    for key in r.keys("bridge_v2:*"):
        data = r.get(key)
        if data:
            if isinstance(data, bytes):
                data = data.decode('utf-8')
            b = json.loads(data)
            if b.get("bridge_id") == bridge_id:
                bridge_data = b
                break

    if not bridge_data:
        return {"success": False, "error": "Bridge not found"}

    bridge = BridgeV2.from_dict(bridge_data)

    if bridge.owner_id != owner_id:
        return {"success": False, "error": "Only pool owner can respond"}

    if bridge.status != "pending":
        return {"success": False, "error": f"Bridge is already {bridge.status}"}

    now = datetime.now(timezone.utc).isoformat()
    bridge.updated_at = now

    if accept:
        bridge.status = "active"

        # Update pool bridge count
        pool = get_pool_v2(bridge.source_pool_id)
        if pool:
            pool_dict = pool.to_dict()
            pool_dict["bridge_count"] = pool_dict.get("bridge_count", 0) + 1
            r.set(f"pool_v2:{bridge.source_pool_id}", json.dumps(pool_dict))
    else:
        bridge.status = "declined"
        bridge.decline_reason = decline_reason

    pipe = r.pipeline()
    pipe.set(f"bridge_v2:{bridge.source_pool_id}:{bridge.target_user_id}", json.dumps(bridge.to_dict()))
    pipe.srem(f"pending_bridges:{owner_id}", bridge_id)
    pipe.execute()

    return {
        "success": True,
        "bridge_id": bridge_id,
        "status": bridge.status,
    }


def revoke_bridge(bridge_id: str, owner_id: str) -> Dict:
    """Revoke an active bridge"""
    r = get_redis()

    # Find and update bridge
    for key in r.keys("bridge_v2:*"):
        data = r.get(key)
        if data:
            if isinstance(data, bytes):
                data = data.decode('utf-8')
            b = json.loads(data)
            if b.get("bridge_id") == bridge_id and b.get("owner_id") == owner_id:
                b["status"] = "revoked"
                b["updated_at"] = datetime.now(timezone.utc).isoformat()
                r.set(key, json.dumps(b))
                return {"success": True, "bridge_id": bridge_id}

    return {"success": False, "error": "Bridge not found or not authorized"}


def get_user_incoming_bridges(user_id: str) -> List[Dict]:
    """Get bridges where user has access to others' pools"""
    r = get_redis()
    bridge_ids = r.smembers(f"user_bridges_in:{user_id}")

    bridges = []
    for bridge_id in bridge_ids:
        if isinstance(bridge_id, bytes):
            bridge_id = bridge_id.decode('utf-8')

        # Find bridge data
        for key in r.keys("bridge_v2:*"):
            data = r.get(key)
            if data:
                if isinstance(data, bytes):
                    data = data.decode('utf-8')
                b = json.loads(data)
                if b.get("bridge_id") == bridge_id:
                    # Add pool info
                    pool = get_pool_v2(b["source_pool_id"])
                    if pool:
                        b["pool_name"] = pool.name
                        b["pool_category"] = pool.category
                        b["pool_category_info"] = pool.category_info
                        b["owner_name"] = pool.owner_id.replace("user_", "")
                    bridges.append(b)
                    break

    return bridges


def get_user_outgoing_bridges(user_id: str) -> List[Dict]:
    """Get bridges where user owns the pool"""
    r = get_redis()
    bridge_ids = r.smembers(f"user_bridges_out:{user_id}")

    bridges = []
    for bridge_id in bridge_ids:
        if isinstance(bridge_id, bytes):
            bridge_id = bridge_id.decode('utf-8')

        for key in r.keys("bridge_v2:*"):
            data = r.get(key)
            if data:
                if isinstance(data, bytes):
                    data = data.decode('utf-8')
                b = json.loads(data)
                if b.get("bridge_id") == bridge_id:
                    # Add user info
                    b["target_username"] = b["target_user_id"].replace("user_", "")
                    pool = get_pool_v2(b["source_pool_id"])
                    if pool:
                        b["pool_name"] = pool.name
                    bridges.append(b)
                    break

    return bridges


def get_pending_bridge_requests(owner_id: str) -> List[Dict]:
    """Get pending bridge requests for pool owner"""
    r = get_redis()
    bridge_ids = r.smembers(f"pending_bridges:{owner_id}")

    bridges = []
    for bridge_id in bridge_ids:
        if isinstance(bridge_id, bytes):
            bridge_id = bridge_id.decode('utf-8')

        for key in r.keys("bridge_v2:*"):
            data = r.get(key)
            if data:
                if isinstance(data, bytes):
                    data = data.decode('utf-8')
                b = json.loads(data)
                if b.get("bridge_id") == bridge_id and b.get("status") == "pending":
                    b["requester_name"] = b["target_user_id"].replace("user_", "")
                    pool = get_pool_v2(b["source_pool_id"])
                    if pool:
                        b["pool_name"] = pool.name
                    bridges.append(b)
                    break

    return bridges


# =============================================================================
# STATS & UTILITIES
# =============================================================================

def get_pool_stats_v2() -> Dict:
    """Get overall pool statistics"""
    r = get_redis()

    total_pools = r.scard("all_pools_v2")
    public_pools = r.scard("public_pools_v2")

    by_category = {}
    for cat in PoolCategory:
        count = r.scard(f"pools_by_category:{cat.value}")
        if count > 0:
            by_category[cat.value] = count

    return {
        "total_pools": total_pools,
        "public_pools": public_pools,
        "by_category": by_category,
        "categories": {
            c.value: CATEGORY_INFO[c]
            for c in PoolCategory
        },
    }


def get_category_info(category: str) -> Dict:
    """Get info about a category"""
    try:
        cat = PoolCategory(category)
        return CATEGORY_INFO.get(cat, {})
    except:
        return {}


def get_all_categories() -> List[Dict]:
    """Get all available categories"""
    return [
        {
            "value": cat.value,
            "icon": info["icon"],
            "label": info["label"],
            "description": info["description"],
            "examples": info["examples"],
        }
        for cat, info in CATEGORY_INFO.items()
    ]

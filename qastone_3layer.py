#!/usr/bin/env python3
"""
QA.Stone 3-Layer Architecture for Scalable MCP Updates

This module implements the 3D Cantor lattice structure with stochastic wormholes
for scalable QA.Stone messaging and MCP updates.

LAYER ARCHITECTURE:
==================

Layer 0 - Chain (Redis):
    - border_hash: SHA256 of entire 3D volume (the "shell")
    - sequence: Position in global chain
    - prev_hash: Link to previous stone
    - glow_channel: Verification/audit channel
    - ~500 bytes per stone (fixed, scalable to millions)

Layer 1 - Manifest (Redis/Postgres):
    - Full stone metadata
    - MCP message content
    - Wormhole addresses pointing to content layers
    - Sender signature (Ed25519)
    - ~2-10KB per stone

Layer 2+ - Content Layers (Object Storage / IPFS / Local):
    - Video, audio, images, websites, documents
    - Accessed via wormhole addresses
    - Unlimited size per layer
    - Validated by hash in manifest

KEY CONCEPTS:
=============

Border Hash:
    The "shell" of the 3D lattice volume. Computed from all layer hashes.
    Verifying border_hash = verifying entire stone without reading all content.

Wormholes:
    Non-linear traversal paths through the lattice.
    Allow jumping directly to content layers.
    Each wormhole has: layer, type, address, hash

Stochastic Escapes:
    Random access points in the lattice for parallel processing.
    Enable concurrent verification of different layers.

Glow Channel:
    Audit/verification channel identifier.
    Used for routing verification requests.

Usage:
    # Create a 3-layer stone
    stone = QAStone3Layer.create(
        action="transfer",
        arguments={"from": "alice", "to": "bob", "stone_id": "..."},
        sender="alice",
        content_layers=[
            {"type": "message", "content": b"Hello Bob!"},
            {"type": "image", "content": image_bytes},
        ]
    )

    # Store it (3-layer)
    store_stone_3layer(stone)

    # Retrieve just chain entry (Layer 0)
    chain_entry = get_chain_entry(stone.stone_id)

    # Retrieve manifest (Layer 1)
    manifest = get_manifest(stone.stone_id)

    # Access content via wormhole (Layer 2+)
    content = traverse_wormhole(manifest.wormholes[0])
"""

import json
import hashlib
import secrets
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, asdict, field
from pathlib import Path

# Try to import Redis, fallback to mock for testing
try:
    from redis_accounts import get_redis
except ImportError:
    def get_redis():
        raise RuntimeError("Redis not configured")

# =============================================================================
# CONFIGURATION
# =============================================================================

# Storage paths for content layers
CONTENT_STORAGE_PATH = Path(os.environ.get("QASTONE_CONTENT_PATH", "/tmp/qastone_content"))
CONTENT_STORAGE_PATH.mkdir(parents=True, exist_ok=True)

# Redis keys
CHAIN_KEY_3L = "qastone:chain:v3"           # Layer 0: Chain entries
MANIFEST_KEY_3L = "qastone:manifest:v3"     # Layer 1: Manifests
CHAIN_HEAD_KEY_3L = "qastone:chain:head:v3" # Current chain head

# Supported content types
CONTENT_TYPES = {
    "message": {"max_size": 64 * 1024, "storage": "inline"},      # 64KB inline
    "image": {"max_size": 10 * 1024 * 1024, "storage": "file"},   # 10MB file
    "video": {"max_size": 1024 * 1024 * 1024, "storage": "file"}, # 1GB file
    "audio": {"max_size": 100 * 1024 * 1024, "storage": "file"},  # 100MB file
    "website": {"max_size": 50 * 1024 * 1024, "storage": "file"}, # 50MB file
    "document": {"max_size": 50 * 1024 * 1024, "storage": "file"},# 50MB file
    "data": {"max_size": 100 * 1024 * 1024, "storage": "file"},   # 100MB file
}


# =============================================================================
# LAYER 0: CHAIN ENTRY (Minimal, for global ordering)
# =============================================================================

@dataclass
class ChainEntry:
    """
    Layer 0: Minimal chain entry stored in Redis.

    This is what gets replicated between twins and used for global ordering.
    Size: ~500 bytes (fixed)
    """
    stone_id: str           # Unique stone identifier
    border_hash: str        # SHA256 of entire 3D volume
    sequence: int           # Global sequence number
    prev_hash: str          # Previous stone's border_hash
    glow_channel: str       # Verification channel (e.g., "audit", "transfer", "message")
    timestamp: str          # ISO timestamp
    created_by: str         # Sender identifier

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> "ChainEntry":
        return cls(**data)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True)


# =============================================================================
# LAYER 1: MANIFEST (Full metadata with wormhole addresses)
# =============================================================================

@dataclass
class Wormhole:
    """
    A wormhole is a traversal path to a content layer.

    Think of it as a "pointer with proof" - it tells you where the content is
    AND provides the hash to verify it.
    """
    layer: int              # Which layer (2, 3, 4, ...)
    content_type: str       # "message", "video", "image", "website", etc.
    address: str            # Storage address (file path, IPFS hash, S3 URL, etc.)
    content_hash: str       # SHA256 of the content at this address
    size_bytes: int         # Content size
    metadata: Dict = field(default_factory=dict)  # Optional metadata (mime type, etc.)

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> "Wormhole":
        return cls(**data)


@dataclass
class Manifest:
    """
    Layer 1: Full stone manifest with MCP message and wormhole addresses.

    This is stored separately from the chain and contains everything needed
    to understand and access the stone's content.
    Size: ~2-10KB
    """
    stone_id: str                    # Matches chain entry
    version: str                     # Schema version

    # MCP Message (the actual operation)
    mcp_message: Dict                # Full MCP tools/call message

    # 3D Lattice Structure
    lattice_dimensions: List[int]    # [width, height, depth] of the lattice

    # Wormholes to content layers
    wormholes: List[Wormhole]        # List of content layer addresses

    # Cryptographic proof
    border_hash: str                 # Hash of entire structure
    sender_signature: str            # Signature from sender (for verification)

    # Chain linking (duplicated from Layer 0 for standalone verification)
    prev_hash: str                   # Previous stone's border_hash
    sequence: int                    # Global sequence number

    # Metadata
    timestamp: str
    created_by: str
    server_instance: str
    glow_channel: str

    def to_dict(self) -> Dict:
        d = asdict(self)
        d['wormholes'] = [w.to_dict() if isinstance(w, Wormhole) else w for w in self.wormholes]
        return d

    @classmethod
    def from_dict(cls, data: Dict) -> "Manifest":
        data = data.copy()
        data['wormholes'] = [Wormhole.from_dict(w) if isinstance(w, dict) else w for w in data.get('wormholes', [])]
        return cls(**data)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True)


# =============================================================================
# LAYER 2+: CONTENT LAYER STORAGE
# =============================================================================

class ContentStorage:
    """
    Abstract interface for content layer storage.

    Implementations:
    - LocalStorage: Store on local filesystem
    - S3Storage: Store in AWS S3
    - IPFSStorage: Store in IPFS network
    """

    def store(self, content: bytes, content_type: str, stone_id: str) -> str:
        """Store content and return address."""
        raise NotImplementedError

    def retrieve(self, address: str) -> bytes:
        """Retrieve content by address."""
        raise NotImplementedError

    def exists(self, address: str) -> bool:
        """Check if content exists."""
        raise NotImplementedError

    def delete(self, address: str) -> bool:
        """Delete content (for cleanup)."""
        raise NotImplementedError


class LocalContentStorage(ContentStorage):
    """Store content layers on local filesystem."""

    def __init__(self, base_path: Path = CONTENT_STORAGE_PATH):
        self.base_path = base_path
        self.base_path.mkdir(parents=True, exist_ok=True)

    def store(self, content: bytes, content_type: str, stone_id: str) -> str:
        """Store content and return file:// address."""
        # Create directory structure: base/type/stone_id_hash
        content_hash = hashlib.sha256(content).hexdigest()
        type_dir = self.base_path / content_type
        type_dir.mkdir(exist_ok=True)

        filename = f"{stone_id}_{content_hash[:16]}.bin"
        filepath = type_dir / filename

        filepath.write_bytes(content)
        return f"file://{filepath.absolute()}"

    def retrieve(self, address: str) -> bytes:
        """Retrieve content from file:// address."""
        if not address.startswith("file://"):
            raise ValueError(f"Invalid local address: {address}")

        filepath = Path(address[7:])  # Remove "file://"
        if not filepath.exists():
            raise FileNotFoundError(f"Content not found: {address}")

        return filepath.read_bytes()

    def exists(self, address: str) -> bool:
        """Check if file exists."""
        if not address.startswith("file://"):
            return False
        return Path(address[7:]).exists()

    def delete(self, address: str) -> bool:
        """Delete content file."""
        if not address.startswith("file://"):
            return False
        try:
            Path(address[7:]).unlink()
            return True
        except FileNotFoundError:
            return False


# Global content storage instance
_content_storage = LocalContentStorage()


def get_content_storage() -> ContentStorage:
    """Get the configured content storage backend."""
    return _content_storage


def set_content_storage(storage: ContentStorage):
    """Set a custom content storage backend (e.g., S3, IPFS)."""
    global _content_storage
    _content_storage = storage


# =============================================================================
# BORDER HASH COMPUTATION (The "Shell" of the 3D Volume)
# =============================================================================

def compute_border_hash(
    stone_id: str,
    mcp_message: Dict,
    wormholes: List[Wormhole],
    lattice_dimensions: List[int],
    prev_hash: str,
    sender_signature: str
) -> str:
    """
    Compute the border hash - the cryptographic "shell" of the 3D lattice.

    This hash represents the entire stone structure without requiring
    access to all content layers. Verification = check border_hash.

    The border hash is computed from:
    1. Stone identity
    2. MCP message hash
    3. All layer hashes (sorted for determinism)
    4. Lattice dimensions
    5. Chain link (prev_hash)
    6. Sender signature
    """
    # Sort layer hashes for deterministic ordering
    layer_hashes = sorted([w.content_hash for w in wormholes])

    # Compute MCP message hash
    mcp_hash = hashlib.sha256(json.dumps(mcp_message, sort_keys=True).encode()).hexdigest()

    # Build border data
    border_data = {
        "stone_id": stone_id,
        "mcp_message_hash": mcp_hash,
        "layer_hashes": layer_hashes,
        "dimensions": lattice_dimensions,
        "prev_hash": prev_hash,
        "sender_signature": sender_signature
    }

    # Compute border hash
    border_json = json.dumps(border_data, sort_keys=True).encode()
    return hashlib.sha256(border_json).hexdigest()


def verify_border_hash(manifest: Manifest) -> bool:
    """
    Verify a manifest's border_hash matches its contents.

    This is O(1) verification - we don't need to read content layers.
    """
    expected = compute_border_hash(
        stone_id=manifest.stone_id,
        mcp_message=manifest.mcp_message,
        wormholes=manifest.wormholes,
        lattice_dimensions=manifest.lattice_dimensions,
        prev_hash=manifest.prev_hash,
        sender_signature=manifest.sender_signature
    )
    return manifest.border_hash == expected


# =============================================================================
# 3-LAYER QA.STONE CLASS
# =============================================================================

@dataclass
class QAStone3Layer:
    """
    Complete 3-layer QA.Stone.

    This is the in-memory representation that gets split into:
    - Layer 0: Chain entry (for global ordering)
    - Layer 1: Manifest (for metadata and wormholes)
    - Layer 2+: Content layers (for actual content)
    """
    chain_entry: ChainEntry
    manifest: Manifest
    content_layers: Dict[int, bytes] = field(default_factory=dict)  # layer_num -> content

    @property
    def stone_id(self) -> str:
        return self.chain_entry.stone_id

    @property
    def border_hash(self) -> str:
        return self.chain_entry.border_hash

    @classmethod
    def create(
        cls,
        action: str,
        arguments: Dict,
        sender: str,
        content_layers: Optional[List[Dict]] = None,
        server_instance: str = "a",
        glow_channel: str = "default",
        prev_hash: str = None,
        sequence: int = None
    ) -> "QAStone3Layer":
        """
        Create a new 3-layer QA.Stone.

        Args:
            action: MCP action name (e.g., "qastone_transfer")
            arguments: MCP action arguments
            sender: Sender identifier
            content_layers: List of {"type": "...", "content": bytes, "metadata": {...}}
            server_instance: Which twin created this
            glow_channel: Verification channel
            prev_hash: Previous stone's border_hash (auto-fetched if None)
            sequence: Sequence number (auto-fetched if None)

        Returns:
            Complete QAStone3Layer ready for storage
        """
        content_layers = content_layers or []

        # Generate IDs
        timestamp = datetime.now(timezone.utc).isoformat()
        stone_id = f"QA_{datetime.now().strftime('%Y%m%d%H%M%S')}_{secrets.token_hex(8)}"

        # Get chain head if not provided
        if prev_hash is None or sequence is None:
            head = get_chain_head_3layer()
            prev_hash = prev_hash or head["border_hash"]
            sequence = sequence if sequence is not None else head["sequence"] + 1

        # Create MCP message
        mcp_message = {
            "jsonrpc": "2.0",
            "id": stone_id,
            "method": "tools/call",
            "params": {
                "name": action,
                "arguments": arguments
            }
        }

        # Process content layers and create wormholes
        wormholes = []
        content_data = {}
        storage = get_content_storage()

        for i, layer_spec in enumerate(content_layers):
            layer_num = i + 2  # Layers 2, 3, 4, ...
            content_type = layer_spec.get("type", "data")
            content = layer_spec.get("content", b"")
            metadata = layer_spec.get("metadata", {})

            # Ensure content is bytes
            if isinstance(content, str):
                content = content.encode('utf-8')

            # Compute content hash
            content_hash = hashlib.sha256(content).hexdigest()

            # Store content and get address
            address = storage.store(content, content_type, stone_id)

            # Create wormhole
            wormhole = Wormhole(
                layer=layer_num,
                content_type=content_type,
                address=address,
                content_hash=content_hash,
                size_bytes=len(content),
                metadata=metadata
            )
            wormholes.append(wormhole)
            content_data[layer_num] = content

        # Determine lattice dimensions based on content
        # Base: 8x8 grid, depth = number of layers
        depth = max(2, len(content_layers) + 2)  # Minimum depth of 2
        lattice_dimensions = [8, 8, depth]

        # Create sender signature (simplified - would use Ed25519 in production)
        signature_data = f"{stone_id}:{sender}:{timestamp}:{secrets.token_hex(16)}"
        sender_signature = hashlib.sha256(signature_data.encode()).hexdigest()

        # Compute border hash
        border_hash = compute_border_hash(
            stone_id=stone_id,
            mcp_message=mcp_message,
            wormholes=wormholes,
            lattice_dimensions=lattice_dimensions,
            prev_hash=prev_hash,
            sender_signature=sender_signature
        )

        # Create chain entry (Layer 0)
        chain_entry = ChainEntry(
            stone_id=stone_id,
            border_hash=border_hash,
            sequence=sequence,
            prev_hash=prev_hash,
            glow_channel=glow_channel,
            timestamp=timestamp,
            created_by=sender
        )

        # Create manifest (Layer 1)
        manifest = Manifest(
            stone_id=stone_id,
            version="3.0.0",
            mcp_message=mcp_message,
            lattice_dimensions=lattice_dimensions,
            wormholes=wormholes,
            border_hash=border_hash,
            sender_signature=sender_signature,
            prev_hash=prev_hash,
            sequence=sequence,
            timestamp=timestamp,
            created_by=sender,
            server_instance=server_instance,
            glow_channel=glow_channel
        )

        return cls(
            chain_entry=chain_entry,
            manifest=manifest,
            content_layers=content_data
        )


# =============================================================================
# STORAGE OPERATIONS (3-Layer)
# =============================================================================

def get_chain_head_3layer() -> Dict:
    """Get current chain head."""
    try:
        r = get_redis()
        head_data = r.get(CHAIN_HEAD_KEY_3L)
        if head_data:
            return json.loads(head_data)
    except Exception:
        pass

    # Genesis
    return {
        "border_hash": "0" * 64,
        "sequence": 0,
        "stone_id": "genesis"
    }


def store_chain_entry(entry: ChainEntry) -> bool:
    """Store Layer 0 chain entry in Redis."""
    r = get_redis()

    # Store entry
    r.hset(CHAIN_KEY_3L, entry.stone_id, entry.to_json())

    # Update head
    new_head = {
        "border_hash": entry.border_hash,
        "sequence": entry.sequence,
        "stone_id": entry.stone_id
    }
    r.set(CHAIN_HEAD_KEY_3L, json.dumps(new_head))

    return True


def store_manifest(manifest: Manifest) -> bool:
    """Store Layer 1 manifest in Redis."""
    r = get_redis()
    r.hset(MANIFEST_KEY_3L, manifest.stone_id, manifest.to_json())
    return True


def store_stone_3layer(stone: QAStone3Layer) -> Dict:
    """
    Store a complete 3-layer stone.

    This stores:
    - Layer 0: Chain entry in Redis
    - Layer 1: Manifest in Redis
    - Layer 2+: Content already stored during creation
    """
    # Verify border hash before storing
    if not verify_border_hash(stone.manifest):
        return {"success": False, "error": "Border hash verification failed"}

    # Store chain entry (Layer 0)
    store_chain_entry(stone.chain_entry)

    # Store manifest (Layer 1)
    store_manifest(stone.manifest)

    return {
        "success": True,
        "stone_id": stone.stone_id,
        "border_hash": stone.border_hash,
        "sequence": stone.chain_entry.sequence,
        "layers_stored": len(stone.manifest.wormholes) + 2  # Chain + Manifest + Content layers
    }


def get_chain_entry(stone_id: str) -> Optional[ChainEntry]:
    """Retrieve Layer 0 chain entry."""
    r = get_redis()
    data = r.hget(CHAIN_KEY_3L, stone_id)
    if data:
        return ChainEntry.from_dict(json.loads(data))
    return None


def get_manifest(stone_id: str) -> Optional[Manifest]:
    """Retrieve Layer 1 manifest."""
    r = get_redis()
    data = r.hget(MANIFEST_KEY_3L, stone_id)
    if data:
        return Manifest.from_dict(json.loads(data))
    return None


def traverse_wormhole(wormhole: Wormhole) -> bytes:
    """
    Traverse a wormhole to retrieve content from Layer 2+.

    This fetches the content and verifies its hash matches.
    """
    storage = get_content_storage()
    content = storage.retrieve(wormhole.address)

    # Verify content hash
    actual_hash = hashlib.sha256(content).hexdigest()
    if actual_hash != wormhole.content_hash:
        raise ValueError(f"Content hash mismatch: expected {wormhole.content_hash}, got {actual_hash}")

    return content


# =============================================================================
# VERIFICATION
# =============================================================================

def verify_stone_3layer(stone_id: str, full_verification: bool = False) -> Dict:
    """
    Verify a 3-layer stone.

    Args:
        stone_id: Stone to verify
        full_verification: If True, also verify all content layer hashes

    Returns:
        Verification result with details
    """
    errors = []

    # Get chain entry
    chain_entry = get_chain_entry(stone_id)
    if not chain_entry:
        return {"valid": False, "errors": ["Stone not found in chain"]}

    # Get manifest
    manifest = get_manifest(stone_id)
    if not manifest:
        return {"valid": False, "errors": ["Manifest not found"]}

    # Verify border hash matches
    if chain_entry.border_hash != manifest.border_hash:
        errors.append("Border hash mismatch between chain and manifest")

    # Verify border hash computation
    if not verify_border_hash(manifest):
        errors.append("Border hash computation failed")

    # Verify chain link (optional - for strict ordering)
    # This would check prev_hash exists in chain

    # Full verification: check all content layers
    if full_verification:
        storage = get_content_storage()
        for wormhole in manifest.wormholes:
            try:
                if not storage.exists(wormhole.address):
                    errors.append(f"Content layer {wormhole.layer} not found: {wormhole.address}")
                    continue

                content = storage.retrieve(wormhole.address)
                actual_hash = hashlib.sha256(content).hexdigest()
                if actual_hash != wormhole.content_hash:
                    errors.append(f"Layer {wormhole.layer} hash mismatch")
            except Exception as e:
                errors.append(f"Layer {wormhole.layer} error: {str(e)}")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "stone_id": stone_id,
        "border_hash": chain_entry.border_hash[:16] + "...",
        "sequence": chain_entry.sequence,
        "layers": len(manifest.wormholes) + 2,
        "glow_channel": chain_entry.glow_channel
    }


# =============================================================================
# TWIN SYNC PROTOCOL
# =============================================================================

def get_sync_payload(stone_id: str) -> Dict:
    """
    Get the minimal payload needed for twin sync.

    This is what Twin A sends to Twin B when a new stone is created.
    Twin B can then fetch the full manifest if needed.
    """
    chain_entry = get_chain_entry(stone_id)
    if not chain_entry:
        return None

    return {
        "stone_id": chain_entry.stone_id,
        "border_hash": chain_entry.border_hash,
        "sequence": chain_entry.sequence,
        "prev_hash": chain_entry.prev_hash,
        "glow_channel": chain_entry.glow_channel,
        "timestamp": chain_entry.timestamp
    }


def apply_sync_payload(payload: Dict) -> Dict:
    """
    Apply a sync payload from another twin.

    This creates a chain entry stub. The full manifest
    will be fetched on-demand when needed.
    """
    # Create chain entry from payload
    chain_entry = ChainEntry(
        stone_id=payload["stone_id"],
        border_hash=payload["border_hash"],
        sequence=payload["sequence"],
        prev_hash=payload["prev_hash"],
        glow_channel=payload["glow_channel"],
        timestamp=payload["timestamp"],
        created_by=payload.get("created_by", "sync")
    )

    # Store it
    store_chain_entry(chain_entry)

    return {
        "success": True,
        "stone_id": chain_entry.stone_id,
        "sequence": chain_entry.sequence
    }


# =============================================================================
# CHAIN QUERIES
# =============================================================================

def get_chain_status_3layer() -> Dict:
    """Get current 3-layer chain status."""
    head = get_chain_head_3layer()

    try:
        r = get_redis()
        chain_length = r.hlen(CHAIN_KEY_3L)
    except Exception:
        chain_length = 0

    return {
        "version": "3.0.0",
        "chain_length": chain_length,
        "head_border_hash": head["border_hash"][:16] + "..." if head["border_hash"] != "0" * 64 else "genesis",
        "head_sequence": head["sequence"],
        "head_stone_id": head["stone_id"]
    }


def get_recent_stones_3layer(limit: int = 10) -> List[Dict]:
    """Get recent stones from chain."""
    try:
        r = get_redis()
        all_entries = r.hgetall(CHAIN_KEY_3L)
    except Exception:
        return []

    stones = []
    for stone_id, data in all_entries.items():
        entry = json.loads(data)
        stones.append({
            "stone_id": entry["stone_id"],
            "border_hash": entry["border_hash"][:16] + "...",
            "sequence": entry["sequence"],
            "glow_channel": entry["glow_channel"],
            "timestamp": entry["timestamp"]
        })

    stones.sort(key=lambda x: x["sequence"], reverse=True)
    return stones[:limit]


# =============================================================================
# HIGH-LEVEL API (COMPATIBLE WITH EXISTING CODE)
# =============================================================================

def create_3layer_transfer(
    from_user: str,
    to_user: str,
    stone_id: str,
    message: Optional[str] = None,
    attachments: Optional[List[Dict]] = None,
    server_instance: str = "a"
) -> QAStone3Layer:
    """
    Create a 3-layer transfer stone.

    This is the high-level API for creating transfers with optional
    message and attachments.
    """
    content_layers = []

    # Add message as content layer if provided
    if message:
        content_layers.append({
            "type": "message",
            "content": message.encode('utf-8'),
            "metadata": {"encoding": "utf-8"}
        })

    # Add attachments as content layers
    for attachment in (attachments or []):
        content_layers.append(attachment)

    return QAStone3Layer.create(
        action="qastone_transfer",
        arguments={
            "from_user": from_user,
            "to_user": to_user,
            "stone_id": stone_id
        },
        sender=from_user,
        content_layers=content_layers,
        server_instance=server_instance,
        glow_channel="transfer"
    )


def create_3layer_message(
    from_user: str,
    to_user: str,
    message: str,
    attachments: Optional[List[Dict]] = None,
    server_instance: str = "a"
) -> QAStone3Layer:
    """
    Create a 3-layer message stone.

    For pure messaging (not transferring existing stones).
    """
    content_layers = [{
        "type": "message",
        "content": message.encode('utf-8'),
        "metadata": {"encoding": "utf-8"}
    }]

    for attachment in (attachments or []):
        content_layers.append(attachment)

    return QAStone3Layer.create(
        action="qastone_message",
        arguments={
            "from_user": from_user,
            "to_user": to_user
        },
        sender=from_user,
        content_layers=content_layers,
        server_instance=server_instance,
        glow_channel="message"
    )


# =============================================================================
# MIGRATION: Convert v2 stones to v3
# =============================================================================

def migrate_v2_to_v3(v2_stone: Dict) -> QAStone3Layer:
    """
    Migrate a v2 MCP stone to v3 3-layer format.

    This allows gradual migration of existing stones.
    """
    return QAStone3Layer.create(
        action=v2_stone["mcp_message"]["params"]["name"],
        arguments=v2_stone["mcp_message"]["params"]["arguments"],
        sender=v2_stone["created_by"],
        server_instance=v2_stone.get("server_instance", "a"),
        glow_channel="migrated",
        prev_hash=v2_stone.get("previous_hash"),
        sequence=v2_stone.get("sequence_number")
    )


# =============================================================================
# TEST
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("QA.Stone 3-Layer Architecture Test")
    print("=" * 60)

    # Create a test stone with content layers
    print("\n1. Creating 3-layer stone with message and image...")

    # Simulated content
    message_content = "Hello Bob! Here's a photo from our trip."
    image_content = b"\x89PNG\r\n\x1a\n" + secrets.token_bytes(1000)  # Fake PNG

    stone = QAStone3Layer.create(
        action="qastone_message",
        arguments={
            "from_user": "user_alice",
            "to_user": "user_bob"
        },
        sender="user_alice",
        content_layers=[
            {"type": "message", "content": message_content},
            {"type": "image", "content": image_content, "metadata": {"mime": "image/png"}}
        ],
        glow_channel="message"
    )

    print(f"   Stone ID: {stone.stone_id}")
    print(f"   Border Hash: {stone.border_hash[:32]}...")
    print(f"   Lattice: {stone.manifest.lattice_dimensions}")
    print(f"   Wormholes: {len(stone.manifest.wormholes)}")

    for wh in stone.manifest.wormholes:
        print(f"     - Layer {wh.layer}: {wh.content_type} ({wh.size_bytes} bytes)")

    # Verify border hash
    print("\n2. Verifying border hash...")
    is_valid = verify_border_hash(stone.manifest)
    print(f"   Border hash valid: {is_valid}")

    # Show chain entry (Layer 0)
    print("\n3. Layer 0 (Chain Entry):")
    print(f"   Size: {len(stone.chain_entry.to_json())} bytes")
    print(f"   {stone.chain_entry.to_json()[:100]}...")

    # Show manifest size (Layer 1)
    print("\n4. Layer 1 (Manifest):")
    print(f"   Size: {len(stone.manifest.to_json())} bytes")

    # Show content layers (Layer 2+)
    print("\n5. Layer 2+ (Content):")
    for wh in stone.manifest.wormholes:
        print(f"   Layer {wh.layer}: {wh.address}")

    # Traverse wormhole to retrieve content
    print("\n6. Traversing wormhole to Layer 2...")
    content = traverse_wormhole(stone.manifest.wormholes[0])
    print(f"   Retrieved: {content.decode('utf-8')[:50]}...")

    print("\n" + "=" * 60)
    print("3-Layer Architecture Ready for Scale!")
    print("=" * 60)
    print("""
Scaling Characteristics:
- Layer 0 (Chain): ~500 bytes/stone → 1M stones = 500MB
- Layer 1 (Manifest): ~5KB/stone → 1M stones = 5GB
- Layer 2+ (Content): Unlimited, stored separately

Twin Sync:
- Only sync Layer 0 between twins
- Layer 1 fetched on-demand
- Layer 2+ accessed via wormholes
""")

#!/usr/bin/env python3
"""
QA.Stone Chunked Live Streaming

Implements HLS-style chunked streaming where each video chunk is a QA.Stone
with full cryptographic provenance.

ARCHITECTURE:
=============

    Live Camera → Encoder → 2-4 second chunks
                                ↓
                    Each chunk = QA.Stone
                    - Layer 0: Chain entry (sequence, timing)
                    - Layer 1: Manifest (codec info, duration)
                    - Layer 2: Video chunk (H.264/VP8/VP9)
                                ↓
            Stream Chain: chunk_0 → chunk_1 → chunk_2 → ...
                                ↓
            Viewer polls chain, traverses wormholes
                                ↓
            Plays chunks in sequence (adaptive bitrate possible)

LATENCY:
========
    - Chunk duration: 2-4 seconds (configurable)
    - Encoding latency: ~1 second
    - Network/chain latency: ~1 second
    - Total: 4-10 seconds (comparable to HLS/YouTube Live)

USE CASES:
==========
    - Webinars with verified speaker identity
    - Product launches with cryptographic timestamps
    - Legal depositions with tamper-proof recording
    - Live events where provenance matters

USAGE:
======
    # Streamer side
    session = LiveStreamSession.create(
        streamer_id="user_alice",
        title="My Live Stream",
        description="Weekly coding session"
    )

    # Add chunks as they're encoded
    for chunk_bytes in encoder.chunks():
        stone = session.add_chunk(chunk_bytes, duration_ms=2000)
        store_stone_3layer(stone)

    # End stream
    session.end()

    # Viewer side
    player = StreamPlayer(session.session_id)
    async for chunk in player.chunks():
        video_player.play(chunk)
"""

import json
import hashlib
import secrets
import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, AsyncIterator, Iterator
from dataclasses import dataclass, asdict, field
from enum import Enum

# Import 3-layer infrastructure
from qastone_3layer import (
    QAStone3Layer,
    ChainEntry,
    Manifest,
    Wormhole,
    store_stone_3layer,
    get_chain_entry,
    get_manifest,
    traverse_wormhole,
    get_content_storage,
    get_chain_head_3layer,
    CONTENT_STORAGE_PATH,
)

try:
    from redis_accounts import get_redis
except ImportError:
    def get_redis():
        raise RuntimeError("Redis not configured")


# =============================================================================
# CONFIGURATION
# =============================================================================

# Default chunk duration (milliseconds)
DEFAULT_CHUNK_DURATION_MS = 2000  # 2 seconds

# Maximum chunk size (bytes)
MAX_CHUNK_SIZE = 10 * 1024 * 1024  # 10MB per chunk

# Stream buffer size (how many chunks to keep readily available)
STREAM_BUFFER_SIZE = 10

# Redis keys for streaming
STREAM_SESSIONS_KEY = "qastone:streams:sessions"
STREAM_CHUNKS_KEY = "qastone:streams:chunks"  # Hash: session_id -> list of chunk stone_ids
STREAM_METADATA_KEY = "qastone:streams:metadata"


# =============================================================================
# STREAM STATUS
# =============================================================================

class StreamStatus(Enum):
    CREATED = "created"      # Session created, not yet streaming
    LIVE = "live"            # Currently streaming
    PAUSED = "paused"        # Temporarily paused
    ENDED = "ended"          # Stream finished
    ERROR = "error"          # Stream error


# =============================================================================
# STREAM SESSION
# =============================================================================

@dataclass
class StreamMetadata:
    """Metadata for a live stream session."""
    session_id: str
    streamer_id: str
    title: str
    description: str
    status: str  # StreamStatus value
    created_at: str
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    chunk_count: int = 0
    total_duration_ms: int = 0
    codec: str = "h264"
    resolution: str = "1280x720"
    bitrate_kbps: int = 2500
    chunk_duration_ms: int = DEFAULT_CHUNK_DURATION_MS
    thumbnail_stone_id: Optional[str] = None
    server_instance: str = "a"

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> "StreamMetadata":
        return cls(**data)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True)


@dataclass
class ChunkInfo:
    """Information about a single stream chunk."""
    stone_id: str
    sequence: int
    duration_ms: int
    size_bytes: int
    timestamp: str
    border_hash: str

    def to_dict(self) -> Dict:
        return asdict(self)


class LiveStreamSession:
    """
    Manages a live stream as a chain of QA.Stone chunks.

    Each chunk is a complete 3-layer stone:
    - Layer 0: Chain entry with sequence and timing
    - Layer 1: Manifest with codec info and wormhole to video
    - Layer 2: Actual video chunk data
    """

    def __init__(self, metadata: StreamMetadata):
        self.metadata = metadata
        self._chunk_stones: List[str] = []  # List of stone_ids
        self._prev_chunk_hash: str = "0" * 64  # Genesis for first chunk

    @property
    def session_id(self) -> str:
        return self.metadata.session_id

    @property
    def is_live(self) -> bool:
        return self.metadata.status == StreamStatus.LIVE.value

    @classmethod
    def create(
        cls,
        streamer_id: str,
        title: str,
        description: str = "",
        codec: str = "h264",
        resolution: str = "1280x720",
        bitrate_kbps: int = 2500,
        chunk_duration_ms: int = DEFAULT_CHUNK_DURATION_MS,
        server_instance: str = "a"
    ) -> "LiveStreamSession":
        """Create a new live stream session."""
        session_id = f"stream_{datetime.now().strftime('%Y%m%d%H%M%S')}_{secrets.token_hex(6)}"

        metadata = StreamMetadata(
            session_id=session_id,
            streamer_id=streamer_id,
            title=title,
            description=description,
            status=StreamStatus.CREATED.value,
            created_at=datetime.now(timezone.utc).isoformat(),
            codec=codec,
            resolution=resolution,
            bitrate_kbps=bitrate_kbps,
            chunk_duration_ms=chunk_duration_ms,
            server_instance=server_instance
        )

        session = cls(metadata)
        session._save_metadata()
        return session

    @classmethod
    def load(cls, session_id: str) -> Optional["LiveStreamSession"]:
        """Load an existing stream session."""
        try:
            r = get_redis()
            data = r.hget(STREAM_METADATA_KEY, session_id)
            if not data:
                return None

            metadata = StreamMetadata.from_dict(json.loads(data))
            session = cls(metadata)

            # Load chunk list
            chunks_data = r.hget(STREAM_CHUNKS_KEY, session_id)
            if chunks_data:
                session._chunk_stones = json.loads(chunks_data)

            return session
        except Exception as e:
            print(f"Error loading session: {e}")
            return None

    def _save_metadata(self):
        """Save session metadata to Redis."""
        try:
            r = get_redis()
            r.hset(STREAM_METADATA_KEY, self.session_id, self.metadata.to_json())
            r.sadd(STREAM_SESSIONS_KEY, self.session_id)
        except Exception as e:
            print(f"Error saving metadata: {e}")

    def _save_chunks(self):
        """Save chunk list to Redis."""
        try:
            r = get_redis()
            r.hset(STREAM_CHUNKS_KEY, self.session_id, json.dumps(self._chunk_stones))
        except Exception as e:
            print(f"Error saving chunks: {e}")

    def start(self) -> Dict:
        """Start the live stream."""
        if self.metadata.status not in [StreamStatus.CREATED.value, StreamStatus.PAUSED.value]:
            return {"success": False, "error": f"Cannot start stream in {self.metadata.status} state"}

        self.metadata.status = StreamStatus.LIVE.value
        self.metadata.started_at = datetime.now(timezone.utc).isoformat()
        self._save_metadata()

        return {
            "success": True,
            "session_id": self.session_id,
            "status": self.metadata.status,
            "started_at": self.metadata.started_at
        }

    def pause(self) -> Dict:
        """Pause the live stream."""
        if self.metadata.status != StreamStatus.LIVE.value:
            return {"success": False, "error": "Stream is not live"}

        self.metadata.status = StreamStatus.PAUSED.value
        self._save_metadata()

        return {"success": True, "status": self.metadata.status}

    def end(self) -> Dict:
        """End the live stream."""
        self.metadata.status = StreamStatus.ENDED.value
        self.metadata.ended_at = datetime.now(timezone.utc).isoformat()
        self._save_metadata()

        return {
            "success": True,
            "session_id": self.session_id,
            "status": self.metadata.status,
            "ended_at": self.metadata.ended_at,
            "total_chunks": self.metadata.chunk_count,
            "total_duration_ms": self.metadata.total_duration_ms
        }

    def add_chunk(
        self,
        video_data: bytes,
        duration_ms: int = None,
        keyframe: bool = False,
        metadata: Dict = None
    ) -> QAStone3Layer:
        """
        Add a video chunk to the stream.

        Args:
            video_data: Raw video chunk bytes (H.264/VP8/VP9)
            duration_ms: Chunk duration in milliseconds
            keyframe: Whether this chunk starts with a keyframe
            metadata: Optional additional metadata

        Returns:
            The created QA.Stone for this chunk
        """
        if self.metadata.status != StreamStatus.LIVE.value:
            raise ValueError(f"Cannot add chunk: stream is {self.metadata.status}")

        if len(video_data) > MAX_CHUNK_SIZE:
            raise ValueError(f"Chunk too large: {len(video_data)} > {MAX_CHUNK_SIZE}")

        duration_ms = duration_ms or self.metadata.chunk_duration_ms
        chunk_sequence = self.metadata.chunk_count

        # Create the chunk stone
        stone = QAStone3Layer.create(
            action="stream_chunk",
            arguments={
                "session_id": self.session_id,
                "streamer_id": self.metadata.streamer_id,
                "chunk_sequence": chunk_sequence,
                "duration_ms": duration_ms,
                "keyframe": keyframe,
                "codec": self.metadata.codec,
                "resolution": self.metadata.resolution
            },
            sender=self.metadata.streamer_id,
            content_layers=[{
                "type": "video_chunk",
                "content": video_data,
                "metadata": {
                    "codec": self.metadata.codec,
                    "duration_ms": duration_ms,
                    "keyframe": keyframe,
                    "sequence": chunk_sequence,
                    **(metadata or {})
                }
            }],
            server_instance=self.metadata.server_instance,
            glow_channel="livestream"
        )

        # Store the stone
        result = store_stone_3layer(stone)
        if not result.get("success"):
            raise RuntimeError(f"Failed to store chunk: {result.get('error')}")

        # Update session state
        self._chunk_stones.append(stone.stone_id)
        self._prev_chunk_hash = stone.border_hash
        self.metadata.chunk_count += 1
        self.metadata.total_duration_ms += duration_ms

        # Save updated state
        self._save_metadata()
        self._save_chunks()

        return stone

    def add_thumbnail(self, image_data: bytes, mime_type: str = "image/jpeg") -> QAStone3Layer:
        """Add a thumbnail image for the stream."""
        stone = QAStone3Layer.create(
            action="stream_thumbnail",
            arguments={
                "session_id": self.session_id,
                "streamer_id": self.metadata.streamer_id
            },
            sender=self.metadata.streamer_id,
            content_layers=[{
                "type": "image",
                "content": image_data,
                "metadata": {"mime": mime_type, "purpose": "thumbnail"}
            }],
            server_instance=self.metadata.server_instance,
            glow_channel="livestream"
        )

        result = store_stone_3layer(stone)
        if result.get("success"):
            self.metadata.thumbnail_stone_id = stone.stone_id
            self._save_metadata()

        return stone

    def get_chunk_info(self, sequence: int) -> Optional[ChunkInfo]:
        """Get info about a specific chunk by sequence number."""
        if sequence < 0 or sequence >= len(self._chunk_stones):
            return None

        stone_id = self._chunk_stones[sequence]
        entry = get_chain_entry(stone_id)
        manifest = get_manifest(stone_id)

        if not entry or not manifest:
            return None

        # Get duration from MCP message arguments
        args = manifest.mcp_message.get("params", {}).get("arguments", {})
        duration_ms = args.get("duration_ms", self.metadata.chunk_duration_ms)

        # Get size from wormhole
        size_bytes = 0
        if manifest.wormholes:
            size_bytes = manifest.wormholes[0].size_bytes

        return ChunkInfo(
            stone_id=stone_id,
            sequence=sequence,
            duration_ms=duration_ms,
            size_bytes=size_bytes,
            timestamp=entry.timestamp,
            border_hash=entry.border_hash
        )

    def get_chunks_since(self, sequence: int) -> List[ChunkInfo]:
        """Get all chunks since a given sequence number."""
        chunks = []
        for seq in range(sequence, len(self._chunk_stones)):
            info = self.get_chunk_info(seq)
            if info:
                chunks.append(info)
        return chunks

    def get_latest_chunks(self, count: int = 5) -> List[ChunkInfo]:
        """Get the most recent chunks."""
        start = max(0, len(self._chunk_stones) - count)
        return self.get_chunks_since(start)


# =============================================================================
# STREAM PLAYER (Viewer Side)
# =============================================================================

class StreamPlayer:
    """
    Plays a QA.Stone stream by fetching and verifying chunks.

    Usage:
        player = StreamPlayer(session_id)

        # Get stream info
        info = player.get_stream_info()

        # Play from beginning
        async for chunk_data in player.play():
            video_decoder.decode(chunk_data)

        # Or play from specific position
        async for chunk_data in player.play(start_sequence=10):
            video_decoder.decode(chunk_data)
    """

    def __init__(self, session_id: str):
        self.session_id = session_id
        self._session: Optional[LiveStreamSession] = None
        self._current_sequence: int = 0

    def _load_session(self) -> LiveStreamSession:
        """Load or refresh session data."""
        session = LiveStreamSession.load(self.session_id)
        if not session:
            raise ValueError(f"Stream not found: {self.session_id}")
        self._session = session
        return session

    def get_stream_info(self) -> Dict:
        """Get information about the stream."""
        session = self._load_session()
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
            "started_at": session.metadata.started_at,
            "ended_at": session.metadata.ended_at,
            "thumbnail_stone_id": session.metadata.thumbnail_stone_id
        }

    def get_chunk(self, sequence: int) -> Optional[bytes]:
        """
        Get a specific chunk's video data.

        This traverses the wormhole to retrieve the actual video bytes
        and verifies the content hash.
        """
        session = self._load_session()
        chunk_info = session.get_chunk_info(sequence)

        if not chunk_info:
            return None

        # Get manifest to find wormhole
        manifest = get_manifest(chunk_info.stone_id)
        if not manifest or not manifest.wormholes:
            return None

        # Traverse wormhole to get video data
        wormhole = manifest.wormholes[0]
        try:
            video_data = traverse_wormhole(wormhole)
            return video_data
        except Exception as e:
            print(f"Error retrieving chunk {sequence}: {e}")
            return None

    async def play(
        self,
        start_sequence: int = 0,
        poll_interval_ms: int = 500
    ) -> AsyncIterator[bytes]:
        """
        Async generator that yields video chunks for playback.

        For live streams, this will poll for new chunks.
        For ended streams, this will yield all chunks then stop.

        Args:
            start_sequence: Chunk sequence to start from
            poll_interval_ms: How often to check for new chunks (live streams)

        Yields:
            Video chunk bytes
        """
        self._current_sequence = start_sequence

        while True:
            session = self._load_session()

            # Get any new chunks
            while self._current_sequence < session.metadata.chunk_count:
                chunk_data = self.get_chunk(self._current_sequence)
                if chunk_data:
                    yield chunk_data
                self._current_sequence += 1

            # If stream ended, stop
            if session.metadata.status == StreamStatus.ENDED.value:
                break

            # If live, poll for more chunks
            if session.metadata.status == StreamStatus.LIVE.value:
                await asyncio.sleep(poll_interval_ms / 1000)
            else:
                # Paused or other state, wait longer
                await asyncio.sleep(1)

    def play_sync(self, start_sequence: int = 0) -> Iterator[bytes]:
        """
        Synchronous iterator for chunk playback.

        For live streams, this will NOT poll - it returns available chunks only.
        Use play() async method for live streaming.
        """
        session = self._load_session()

        for seq in range(start_sequence, session.metadata.chunk_count):
            chunk_data = self.get_chunk(seq)
            if chunk_data:
                yield chunk_data


# =============================================================================
# STREAM DISCOVERY
# =============================================================================

def list_live_streams() -> List[Dict]:
    """List all currently live streams."""
    try:
        r = get_redis()
        session_ids = r.smembers(STREAM_SESSIONS_KEY)

        live_streams = []
        for session_id in session_ids:
            if isinstance(session_id, bytes):
                session_id = session_id.decode()

            session = LiveStreamSession.load(session_id)
            if session and session.metadata.status == StreamStatus.LIVE.value:
                live_streams.append({
                    "session_id": session.session_id,
                    "title": session.metadata.title,
                    "streamer_id": session.metadata.streamer_id,
                    "chunk_count": session.metadata.chunk_count,
                    "duration_ms": session.metadata.total_duration_ms,
                    "started_at": session.metadata.started_at,
                    "thumbnail_stone_id": session.metadata.thumbnail_stone_id
                })

        return live_streams
    except Exception as e:
        print(f"Error listing streams: {e}")
        return []


def list_recent_streams(limit: int = 10) -> List[Dict]:
    """List recent streams (live and ended)."""
    try:
        r = get_redis()
        session_ids = r.smembers(STREAM_SESSIONS_KEY)

        streams = []
        for session_id in session_ids:
            if isinstance(session_id, bytes):
                session_id = session_id.decode()

            session = LiveStreamSession.load(session_id)
            if session:
                streams.append({
                    "session_id": session.session_id,
                    "title": session.metadata.title,
                    "streamer_id": session.metadata.streamer_id,
                    "status": session.metadata.status,
                    "chunk_count": session.metadata.chunk_count,
                    "duration_ms": session.metadata.total_duration_ms,
                    "created_at": session.metadata.created_at
                })

        # Sort by created_at descending
        streams.sort(key=lambda x: x["created_at"], reverse=True)
        return streams[:limit]
    except Exception as e:
        print(f"Error listing streams: {e}")
        return []


def get_stream_manifest(session_id: str) -> Optional[Dict]:
    """
    Get an HLS-style manifest for a stream.

    This returns a playlist of chunk URLs that a video player can use.
    """
    session = LiveStreamSession.load(session_id)
    if not session:
        return None

    chunks = []
    for seq in range(session.metadata.chunk_count):
        info = session.get_chunk_info(seq)
        if info:
            chunks.append({
                "sequence": seq,
                "stone_id": info.stone_id,
                "duration_ms": info.duration_ms,
                "size_bytes": info.size_bytes,
                "border_hash": info.border_hash[:16] + "..."
            })

    return {
        "session_id": session.session_id,
        "title": session.metadata.title,
        "status": session.metadata.status,
        "codec": session.metadata.codec,
        "resolution": session.metadata.resolution,
        "target_duration_ms": session.metadata.chunk_duration_ms,
        "chunk_count": len(chunks),
        "total_duration_ms": session.metadata.total_duration_ms,
        "chunks": chunks,
        "is_live": session.metadata.status == StreamStatus.LIVE.value
    }


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def create_stream(
    streamer_id: str,
    title: str,
    description: str = "",
    **kwargs
) -> LiveStreamSession:
    """Create and return a new stream session."""
    return LiveStreamSession.create(
        streamer_id=streamer_id,
        title=title,
        description=description,
        **kwargs
    )


def start_stream(session_id: str) -> Dict:
    """Start a stream by session ID."""
    session = LiveStreamSession.load(session_id)
    if not session:
        return {"success": False, "error": "Stream not found"}
    return session.start()


def end_stream(session_id: str) -> Dict:
    """End a stream by session ID."""
    session = LiveStreamSession.load(session_id)
    if not session:
        return {"success": False, "error": "Stream not found"}
    return session.end()


def add_stream_chunk(
    session_id: str,
    video_data: bytes,
    duration_ms: int = None,
    keyframe: bool = False
) -> Dict:
    """Add a chunk to an existing stream."""
    session = LiveStreamSession.load(session_id)
    if not session:
        return {"success": False, "error": "Stream not found"}

    try:
        stone = session.add_chunk(video_data, duration_ms, keyframe)
        return {
            "success": True,
            "stone_id": stone.stone_id,
            "sequence": session.metadata.chunk_count - 1,
            "border_hash": stone.border_hash
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


# =============================================================================
# TEST
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("QA.Stone Chunked Live Streaming Test")
    print("=" * 60)

    # Simulate a short stream with fake video data
    print("\n1. Creating stream session...")
    session = LiveStreamSession.create(
        streamer_id="user_alice",
        title="Test Live Stream",
        description="Testing QA.Stone chunked streaming",
        codec="h264",
        resolution="1280x720",
        chunk_duration_ms=2000
    )
    print(f"   Session ID: {session.session_id}")
    print(f"   Status: {session.metadata.status}")

    print("\n2. Starting stream...")
    result = session.start()
    print(f"   Started: {result['started_at']}")

    print("\n3. Adding video chunks...")
    for i in range(5):
        # Simulate video chunk (in reality this would be H.264 data)
        fake_video = f"VIDEO_CHUNK_{i}_".encode() + secrets.token_bytes(1000)

        stone = session.add_chunk(
            video_data=fake_video,
            duration_ms=2000,
            keyframe=(i == 0)  # First chunk is keyframe
        )
        print(f"   Chunk {i}: {stone.stone_id[:30]}... ({len(fake_video)} bytes)")

    print("\n4. Stream status:")
    print(f"   Chunks: {session.metadata.chunk_count}")
    print(f"   Duration: {session.metadata.total_duration_ms}ms")

    print("\n5. Getting stream manifest...")
    manifest = get_stream_manifest(session.session_id)
    print(f"   Title: {manifest['title']}")
    print(f"   Chunks in manifest: {manifest['chunk_count']}")
    print(f"   Is live: {manifest['is_live']}")

    print("\n6. Playing back chunks (sync)...")
    player = StreamPlayer(session.session_id)
    for i, chunk_data in enumerate(player.play_sync()):
        print(f"   Played chunk {i}: {len(chunk_data)} bytes")

    print("\n7. Ending stream...")
    result = session.end()
    print(f"   Ended: {result['ended_at']}")
    print(f"   Total chunks: {result['total_chunks']}")
    print(f"   Total duration: {result['total_duration_ms']}ms")

    print("\n" + "=" * 60)
    print("Chunked Streaming Test Complete!")
    print("=" * 60)
    print("""
Each chunk is a full QA.Stone with:
- Border hash verification
- Chain linking (sequence ordering)
- Wormhole to video content
- Streamer provenance

Latency: ~4-10 seconds (comparable to HLS)
Benefit: Every frame is cryptographically verified!
""")

#!/usr/bin/env python3
"""
Collab Engine for QA.Stone Platform

Provides collaborative sessions with brainstorm, debate, and bidding modes.
All messages and events are wrapped as QA.Stones for verification.

Ported from phi_command_center_desktop/src/collab/collab-session.js
"""

import os
import json
import secrets
import hashlib
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any, Literal
from dataclasses import dataclass, asdict, field
from enum import Enum

import redis


# =============================================================================
# ENUMS
# =============================================================================

class SessionMode(str, Enum):
    BRAINSTORM = "brainstorm"
    DEBATE = "debate"
    BIDDING = "bidding"


class SessionStatus(str, Enum):
    PENDING = "pending"
    ACTIVE = "active"
    ENDED = "ended"


class MessageType(str, Enum):
    CHAT = "chat"
    PROMPT = "prompt"
    RESPONSE = "response"
    TRANSFORM = "transform"
    SYSTEM = "system"


class EventType(str, Enum):
    GUEST_JOINED = "guest_joined"
    GUEST_LEFT = "guest_left"
    PERMISSIONS_CHANGED = "permissions_changed"
    SESSION_ENDED = "session_ended"
    IDEA_ADDED = "idea_added"
    IDEA_VOTED = "idea_voted"
    IDEA_EXPANDED = "idea_expanded"
    TEAM_SETUP = "team_setup"
    ARGUMENT_SUBMITTED = "argument_submitted"
    JUDGE_SCORED = "judge_scored"
    ROUND_ADVANCED = "round_advanced"


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def generate_session_id() -> str:
    """Generate unique session ID"""
    ts = hex(int(datetime.now().timestamp() * 1000))[2:]
    rand = secrets.token_hex(4)
    return f"sess_{ts}_{rand}"


def generate_short_code(length: int = 6) -> str:
    """Generate human-readable invite code (no confusing chars)"""
    chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(secrets.choice(chars) for _ in range(length))


def compute_stone_hash(data: Dict) -> str:
    """Compute border hash for QA.Stone verification"""
    canonical = json.dumps(data, sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


# =============================================================================
# DATA MODELS
# =============================================================================

@dataclass
class Participant:
    user_id: str
    name: str
    avatar: Optional[str] = None
    wallet_id: Optional[str] = None
    joined_at: Optional[str] = None

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class Permissions:
    can_prompt: bool = True
    can_transform: bool = False
    can_export: bool = False
    can_invite_others: bool = False
    max_tokens_per_prompt: int = 500
    allowed_transforms: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class TokenBudget:
    total: int = 2000
    used: int = 0
    host_contribution: int = 0
    guest_contribution: int = 0

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class CollabMessage:
    """A message in a collab session, wrapped as a QA.Stone"""
    message_id: str
    participant: str  # "host" | "guest" | "system"
    message_type: str
    content: str
    token_cost: int = 0
    timestamp: str = ""
    border_hash: str = ""

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()
        if not self.message_id:
            ts = hex(int(datetime.now().timestamp() * 1000))[2:]
            rand = secrets.token_hex(2)
            self.message_id = f"msg_{ts}_{rand}"
        if not self.border_hash:
            self.border_hash = compute_stone_hash({
                "participant": self.participant,
                "type": self.message_type,
                "content": self.content,
                "timestamp": self.timestamp,
            })

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class CollabEvent:
    """An event in a collab session"""
    event_id: str
    event_type: str
    data: Dict
    participant_id: Optional[str] = None
    timestamp: str = ""
    border_hash: str = ""

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()
        if not self.event_id:
            ts = hex(int(datetime.now().timestamp() * 1000))[2:]
            rand = secrets.token_hex(2)
            self.event_id = f"evt_{ts}_{rand}"
        if not self.border_hash:
            self.border_hash = compute_stone_hash({
                "type": self.event_type,
                "data": self.data,
                "timestamp": self.timestamp,
            })

    def to_dict(self) -> Dict:
        return asdict(self)


# =============================================================================
# BRAINSTORM MODE
# =============================================================================

@dataclass
class BrainstormIdea:
    idea_id: str
    author_id: str
    author_name: str
    content: str
    votes: int = 0
    voters: List[str] = field(default_factory=list)
    expansions: List[str] = field(default_factory=list)
    created_at: str = ""
    border_hash: str = ""

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()
        if not self.idea_id:
            self.idea_id = f"idea_{secrets.token_hex(4)}"
        if not self.border_hash:
            self.border_hash = compute_stone_hash({
                "author_id": self.author_id,
                "content": self.content,
                "created_at": self.created_at,
            })

    def to_dict(self) -> Dict:
        return asdict(self)


# =============================================================================
# DEBATE MODE
# =============================================================================

@dataclass
class DebateTeam:
    team_id: str
    name: str
    position: str  # "pro" or "con" or custom
    members: List[str] = field(default_factory=list)
    llm_advocate: Optional[str] = None  # LLM provider if assigned

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class DebateArgument:
    argument_id: str
    team_id: str
    author_id: str
    author_name: str
    content: str
    round_number: int = 1
    scores: Dict[str, int] = field(default_factory=dict)  # judge_id -> score
    created_at: str = ""
    border_hash: str = ""

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()
        if not self.argument_id:
            self.argument_id = f"arg_{secrets.token_hex(4)}"
        if not self.border_hash:
            self.border_hash = compute_stone_hash({
                "team_id": self.team_id,
                "author_id": self.author_id,
                "content": self.content,
                "round": self.round_number,
                "created_at": self.created_at,
            })

    def to_dict(self) -> Dict:
        return asdict(self)


# =============================================================================
# COLLAB SESSION
# =============================================================================

@dataclass
class CollabSession:
    """
    A collaborative session supporting brainstorm, debate, and bidding modes.
    All messages and events are QA.Stones with verification hashes.
    """
    session_id: str
    invite_code: str
    host: Participant
    guest: Optional[Participant] = None
    mode: str = SessionMode.BRAINSTORM.value
    status: str = SessionStatus.PENDING.value
    permissions: Permissions = field(default_factory=Permissions)
    token_budget: TokenBudget = field(default_factory=TokenBudget)
    messages: List[Dict] = field(default_factory=list)
    events: List[Dict] = field(default_factory=list)
    created_at: str = ""
    updated_at: str = ""

    # Brainstorm mode data
    ideas: List[Dict] = field(default_factory=list)

    # Debate mode data
    debate_topic: str = ""
    debate_teams: List[Dict] = field(default_factory=list)
    debate_arguments: List[Dict] = field(default_factory=list)
    debate_round: int = 1

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()
        if not self.updated_at:
            self.updated_at = self.created_at

    def add_message(
        self,
        participant: str,
        message_type: str,
        content: str,
        token_cost: int = 0
    ) -> CollabMessage:
        """Add a message to the session"""
        msg = CollabMessage(
            message_id="",
            participant=participant,
            message_type=message_type,
            content=content,
            token_cost=token_cost,
        )

        self.messages.append(msg.to_dict())

        # Update token budget
        if token_cost > 0:
            self.token_budget.used += token_cost
            if participant == "host":
                self.token_budget.host_contribution += token_cost
            elif participant == "guest":
                self.token_budget.guest_contribution += token_cost

        self.updated_at = datetime.now(timezone.utc).isoformat()
        return msg

    def add_event(
        self,
        event_type: str,
        data: Dict,
        participant_id: Optional[str] = None
    ) -> CollabEvent:
        """Add an event to the session"""
        evt = CollabEvent(
            event_id="",
            event_type=event_type,
            data=data,
            participant_id=participant_id,
        )

        self.events.append(evt.to_dict())
        self.updated_at = datetime.now(timezone.utc).isoformat()
        return evt

    def set_guest(self, guest: Participant) -> Participant:
        """Set guest participant and activate session"""
        guest.joined_at = datetime.now(timezone.utc).isoformat()
        self.guest = guest
        self.status = SessionStatus.ACTIVE.value
        self.updated_at = datetime.now(timezone.utc).isoformat()
        self.add_event(EventType.GUEST_JOINED.value, {"guest": guest.to_dict()})
        return guest

    def remove_guest(self):
        """Remove guest from session"""
        if self.guest:
            guest_name = self.guest.name
            self.guest = None
            self.add_event(EventType.GUEST_LEFT.value, {"guest_name": guest_name})
            self.updated_at = datetime.now(timezone.utc).isoformat()

    def get_guest_permissions(self) -> Permissions:
        """Calculate permissions based on contribution"""
        contribution = self.token_budget.guest_contribution
        perms = Permissions()

        if contribution >= 100:
            perms.can_transform = True
            perms.allowed_transforms = ["spreadsheet", "chart"]
        if contribution >= 500:
            perms.can_export = True
            perms.allowed_transforms.append("image")
        if contribution >= 1000:
            perms.can_invite_others = True
            perms.max_tokens_per_prompt = 1000

        return perms

    def check_permission(self, user_id: str, action: str) -> bool:
        """Check if user has permission for action"""
        # Host has all permissions
        if user_id == self.host.user_id:
            return True

        perms = self.get_guest_permissions()

        if action == "prompt":
            return perms.can_prompt
        elif action == "transform":
            return perms.can_transform
        elif action == "export":
            return perms.can_export
        elif action == "invite":
            return perms.can_invite_others

        return False

    def end(self):
        """End the session"""
        self.status = SessionStatus.ENDED.value
        self.add_event(EventType.SESSION_ENDED.value, {
            "total_messages": len(self.messages),
            "total_tokens": self.token_budget.used,
            "duration_seconds": (
                datetime.fromisoformat(self.updated_at.replace("Z", "+00:00")) -
                datetime.fromisoformat(self.created_at.replace("Z", "+00:00"))
            ).total_seconds(),
        })
        self.updated_at = datetime.now(timezone.utc).isoformat()

    # =========================================================================
    # BRAINSTORM MODE METHODS
    # =========================================================================

    def add_idea(self, author_id: str, author_name: str, content: str) -> BrainstormIdea:
        """Add an idea to brainstorm session"""
        idea = BrainstormIdea(
            idea_id="",
            author_id=author_id,
            author_name=author_name,
            content=content,
        )
        self.ideas.append(idea.to_dict())
        self.add_event(EventType.IDEA_ADDED.value, idea.to_dict(), author_id)
        return idea

    def vote_idea(self, idea_id: str, voter_id: str) -> bool:
        """Vote for an idea"""
        for i, idea in enumerate(self.ideas):
            if idea["idea_id"] == idea_id:
                if voter_id not in idea.get("voters", []):
                    self.ideas[i]["votes"] = idea.get("votes", 0) + 1
                    self.ideas[i].setdefault("voters", []).append(voter_id)
                    self.add_event(EventType.IDEA_VOTED.value, {
                        "idea_id": idea_id,
                        "voter_id": voter_id,
                        "new_vote_count": self.ideas[i]["votes"],
                    }, voter_id)
                    return True
        return False

    def expand_idea(self, idea_id: str, expansion: str) -> bool:
        """Add LLM expansion to an idea"""
        for i, idea in enumerate(self.ideas):
            if idea["idea_id"] == idea_id:
                self.ideas[i].setdefault("expansions", []).append(expansion)
                self.add_event(EventType.IDEA_EXPANDED.value, {
                    "idea_id": idea_id,
                    "expansion": expansion,
                })
                return True
        return False

    def get_ranked_ideas(self) -> List[Dict]:
        """Get ideas sorted by votes"""
        return sorted(self.ideas, key=lambda x: x.get("votes", 0), reverse=True)

    # =========================================================================
    # DEBATE MODE METHODS
    # =========================================================================

    def setup_debate(self, topic: str, teams: List[DebateTeam]):
        """Setup debate with topic and teams"""
        self.debate_topic = topic
        self.debate_teams = [t.to_dict() for t in teams]
        self.debate_round = 1
        self.add_event(EventType.TEAM_SETUP.value, {
            "topic": topic,
            "teams": self.debate_teams,
        })

    def submit_argument(
        self,
        team_id: str,
        author_id: str,
        author_name: str,
        content: str
    ) -> DebateArgument:
        """Submit a debate argument"""
        arg = DebateArgument(
            argument_id="",
            team_id=team_id,
            author_id=author_id,
            author_name=author_name,
            content=content,
            round_number=self.debate_round,
        )
        self.debate_arguments.append(arg.to_dict())
        self.add_event(EventType.ARGUMENT_SUBMITTED.value, arg.to_dict(), author_id)
        return arg

    def judge_argument(
        self,
        argument_id: str,
        judge_id: str,
        score: int,
        reason: str = ""
    ) -> bool:
        """Judge an argument (score 1-10)"""
        for i, arg in enumerate(self.debate_arguments):
            if arg["argument_id"] == argument_id:
                self.debate_arguments[i].setdefault("scores", {})[judge_id] = score
                self.add_event(EventType.JUDGE_SCORED.value, {
                    "argument_id": argument_id,
                    "judge_id": judge_id,
                    "score": score,
                    "reason": reason,
                }, judge_id)
                return True
        return False

    def next_round(self) -> int:
        """Advance to next debate round"""
        self.debate_round += 1
        self.add_event(EventType.ROUND_ADVANCED.value, {
            "new_round": self.debate_round,
        })
        return self.debate_round

    def get_debate_leaderboard(self) -> List[Dict]:
        """Get team scores"""
        scores = {}
        for team in self.debate_teams:
            team_id = team["team_id"]
            scores[team_id] = {
                "team": team,
                "total_score": 0,
                "argument_count": 0,
            }

        for arg in self.debate_arguments:
            team_id = arg["team_id"]
            if team_id in scores:
                arg_scores = arg.get("scores", {})
                total = sum(arg_scores.values())
                scores[team_id]["total_score"] += total
                scores[team_id]["argument_count"] += 1

        return sorted(scores.values(), key=lambda x: x["total_score"], reverse=True)

    # =========================================================================
    # SERIALIZATION
    # =========================================================================

    def get_summary(self) -> Dict:
        """Get session summary"""
        return {
            "session_id": self.session_id,
            "invite_code": self.invite_code,
            "host": self.host.name,
            "guest": self.guest.name if self.guest else None,
            "mode": self.mode,
            "status": self.status,
            "message_count": len(self.messages),
            "token_budget": self.token_budget.to_dict(),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    def to_dict(self) -> Dict:
        """Serialize to dict"""
        return {
            "session_id": self.session_id,
            "invite_code": self.invite_code,
            "host": self.host.to_dict(),
            "guest": self.guest.to_dict() if self.guest else None,
            "mode": self.mode,
            "status": self.status,
            "permissions": self.permissions.to_dict(),
            "token_budget": self.token_budget.to_dict(),
            "messages": self.messages,
            "events": self.events,
            "ideas": self.ideas,
            "debate_topic": self.debate_topic,
            "debate_teams": self.debate_teams,
            "debate_arguments": self.debate_arguments,
            "debate_round": self.debate_round,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "CollabSession":
        """Deserialize from dict"""
        session = cls(
            session_id=data["session_id"],
            invite_code=data["invite_code"],
            host=Participant(**data["host"]),
            mode=data.get("mode", SessionMode.BRAINSTORM.value),
            status=data.get("status", SessionStatus.PENDING.value),
        )

        if data.get("guest"):
            session.guest = Participant(**data["guest"])

        session.permissions = Permissions(**data.get("permissions", {}))
        session.token_budget = TokenBudget(**data.get("token_budget", {}))
        session.messages = data.get("messages", [])
        session.events = data.get("events", [])
        session.ideas = data.get("ideas", [])
        session.debate_topic = data.get("debate_topic", "")
        session.debate_teams = data.get("debate_teams", [])
        session.debate_arguments = data.get("debate_arguments", [])
        session.debate_round = data.get("debate_round", 1)
        session.created_at = data.get("created_at", "")
        session.updated_at = data.get("updated_at", "")

        return session


# =============================================================================
# SESSION MANAGER (Redis-backed)
# =============================================================================

class SessionManager:
    """
    Manages collab sessions with Redis persistence.
    Provides real-time sync capabilities via WebSocket events.
    """

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.prefix = "collab:session"

    def _session_key(self, session_id: str) -> str:
        return f"{self.prefix}:{session_id}"

    def _code_key(self, invite_code: str) -> str:
        return f"{self.prefix}:code:{invite_code.upper()}"

    def _user_sessions_key(self, user_id: str) -> str:
        return f"{self.prefix}:user:{user_id}"

    def create(
        self,
        host_id: str,
        host_name: str,
        mode: str = SessionMode.BRAINSTORM.value,
        starting_context: Optional[Dict] = None
    ) -> CollabSession:
        """Create a new collab session"""
        session = CollabSession(
            session_id=generate_session_id(),
            invite_code=generate_short_code(6),
            host=Participant(user_id=host_id, name=host_name),
            mode=mode,
        )

        # Store session
        self.redis.set(
            self._session_key(session.session_id),
            json.dumps(session.to_dict())
        )

        # Map invite code to session
        self.redis.set(
            self._code_key(session.invite_code),
            session.session_id
        )

        # Add to user's sessions
        self.redis.sadd(self._user_sessions_key(host_id), session.session_id)

        return session

    def get(self, session_id: str) -> Optional[CollabSession]:
        """Get session by ID"""
        data = self.redis.get(self._session_key(session_id))
        if data:
            return CollabSession.from_dict(json.loads(data))
        return None

    def get_by_code(self, invite_code: str) -> Optional[CollabSession]:
        """Get session by invite code"""
        session_id = self.redis.get(self._code_key(invite_code.upper()))
        if session_id:
            return self.get(session_id)
        return None

    def save(self, session: CollabSession):
        """Save session to Redis"""
        self.redis.set(
            self._session_key(session.session_id),
            json.dumps(session.to_dict())
        )

    def join(
        self,
        invite_code: str,
        guest_id: str,
        guest_name: str
    ) -> Dict:
        """Join a session via invite code"""
        session = self.get_by_code(invite_code)

        if not session:
            return {"ok": False, "error": "Session not found"}

        if session.status == SessionStatus.ENDED.value:
            return {"ok": False, "error": "Session has ended"}

        if session.guest:
            return {"ok": False, "error": "Session is full"}

        guest = Participant(user_id=guest_id, name=guest_name)
        session.set_guest(guest)
        self.save(session)

        # Add to user's sessions
        self.redis.sadd(self._user_sessions_key(guest_id), session.session_id)

        return {"ok": True, "session": session.to_dict()}

    def end(self, session_id: str) -> bool:
        """End a session"""
        session = self.get(session_id)
        if session:
            session.end()
            self.save(session)
            return True
        return False

    def delete(self, session_id: str) -> bool:
        """Delete a session"""
        session = self.get(session_id)
        if session:
            self.redis.delete(self._session_key(session_id))
            self.redis.delete(self._code_key(session.invite_code))

            # Remove from user sessions
            self.redis.srem(self._user_sessions_key(session.host.user_id), session_id)
            if session.guest:
                self.redis.srem(self._user_sessions_key(session.guest.user_id), session_id)

            return True
        return False

    def list_user_sessions(self, user_id: str) -> List[Dict]:
        """List all sessions for a user"""
        session_ids = self.redis.smembers(self._user_sessions_key(user_id))
        sessions = []

        for sid in session_ids:
            session = self.get(sid)
            if session:
                sessions.append(session.get_summary())

        return sorted(sessions, key=lambda x: x["updated_at"], reverse=True)

    def cleanup(self, max_age_hours: int = 24) -> int:
        """Clean up old ended sessions"""
        # This would scan for old sessions - for now, return 0
        # In production, use Redis TTL or a cleanup job
        return 0


# =============================================================================
# TEST
# =============================================================================

if __name__ == "__main__":
    # Test without Redis
    session = CollabSession(
        session_id=generate_session_id(),
        invite_code=generate_short_code(),
        host=Participant(user_id="user_alice", name="Alice"),
        mode=SessionMode.BRAINSTORM.value,
    )

    print(f"Created session: {session.session_id}")
    print(f"Invite code: {session.invite_code}")

    # Test brainstorm
    idea = session.add_idea("user_alice", "Alice", "What if we made a QA.Stone marketplace?")
    print(f"Added idea: {idea.idea_id} with hash: {idea.border_hash}")

    session.vote_idea(idea.idea_id, "user_bob")
    print(f"Voted - ideas: {session.get_ranked_ideas()}")

    # Test message
    msg = session.add_message("host", MessageType.CHAT.value, "Welcome to the brainstorm!")
    print(f"Message hash: {msg.border_hash}")

    print(f"\nSession summary: {json.dumps(session.get_summary(), indent=2)}")

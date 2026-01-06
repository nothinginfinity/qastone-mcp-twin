#!/usr/bin/env python3
"""
Multi-LLM Backend for QA.Stone Chat

Supports Gemini, DeepSeek, and Groq for cheap, fast AI chat.
Each response is wrapped in a QA.Stone for verification.
"""

import os
import json
import httpx
import asyncio
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any
from dataclasses import dataclass, asdict
import hashlib

# =============================================================================
# LLM CONFIGURATION
# =============================================================================

@dataclass
class LLMConfig:
    name: str
    provider: str
    model: str
    api_key_env: str
    base_url: str
    max_tokens: int = 2000
    temperature: float = 0.7

# Available LLM backends (cheap and fast)
LLM_CONFIGS = {
    "gemini": LLMConfig(
        name="Gemini",
        provider="google",
        model="gemini-1.5-flash",
        api_key_env="GEMINI_API_KEY",
        base_url="https://generativelanguage.googleapis.com/v1beta",
        max_tokens=2000,
        temperature=0.7
    ),
    "deepseek": LLMConfig(
        name="DeepSeek",
        provider="deepseek",
        model="deepseek-chat",
        api_key_env="DEEPSEEK_API_KEY",
        base_url="https://api.deepseek.com/v1",
        max_tokens=2000,
        temperature=0.7
    ),
    "groq": LLMConfig(
        name="Groq",
        provider="groq",
        model="llama-3.1-70b-versatile",
        api_key_env="GROQ_API_KEY",
        base_url="https://api.groq.com/openai/v1",
        max_tokens=2000,
        temperature=0.7
    ),
}

# System prompts for different bot personalities
BOT_PERSONAS = {
    "welcome": """You are the QA.Stone Welcome Assistant. Your role is to:
1. Welcome new users warmly
2. Explain QA.Stone in simple terms (it's like email for AI - verified, progressive, secure)
3. Guide them through key features: wallet, inbox, sending stones, chat
4. Be friendly, helpful, and concise
5. Use emojis sparingly but effectively

Key concepts to explain:
- Stones: Units of verified content with cryptographic proof
- LOD: Level of Detail - AI loads summaries first, expands as needed (saves 80-95% tokens)
- Wormholes: Links between related content
- Chain of custody: Every message has proof of who sent it and when
""",

    "assistant": """You are a QA.Stone AI Assistant. You help users with:
1. Using the platform (sending messages, managing wallet, etc.)
2. Understanding context management and progressive loading
3. Creating and sharing content
4. Collaborating with other users

Be helpful, accurate, and concise. All your responses become verified stones.""",

    "creative": """You are a Creative AI collaborator on QA.Stone. You help users:
1. Brainstorm ideas
2. Co-create content (writing, code, designs)
3. Explore possibilities
4. Build on their ideas

Be imaginative but grounded. Encourage co-creation.""",

    "debate": """You are a Debate AI on QA.Stone. Your role is to:
1. Present counterarguments thoughtfully
2. Explore multiple perspectives
3. Challenge assumptions constructively
4. Help users think deeper

Be respectful but probing. The goal is better thinking, not winning.""",
}

# =============================================================================
# MESSAGE STONE MODEL
# =============================================================================

@dataclass
class MessageStone:
    """A chat message wrapped as a QA.Stone"""
    stone_id: str
    stone_type: str = "message"

    # Border (verification)
    border_hash: str = ""
    author: str = ""  # username@wallet_hash or bot_name
    created: str = ""
    chain: Optional[str] = None  # Previous message in thread
    signature: str = ""

    # Content
    role: str = "user"  # user, assistant, system
    content: str = ""

    # LOD layers (generated)
    lod5: str = ""  # ~50 tokens summary
    lod4: str = ""  # ~200 tokens

    # Metadata
    conversation_id: str = ""
    llm_provider: Optional[str] = None
    llm_model: Optional[str] = None
    tokens_used: int = 0

    def compute_hash(self) -> str:
        """Compute border hash from content"""
        canonical = json.dumps({
            "author": self.author,
            "created": self.created,
            "role": self.role,
            "content": self.content,
            "chain": self.chain,
        }, sort_keys=True)
        return hashlib.sha256(canonical.encode()).hexdigest()[:16]

    def generate_lod(self):
        """Generate LOD summaries"""
        content = self.content

        # LOD5: First sentence or 50 chars
        if len(content) > 50:
            self.lod5 = content[:47] + "..."
        else:
            self.lod5 = content

        # LOD4: First paragraph or 200 chars
        if len(content) > 200:
            self.lod4 = content[:197] + "..."
        else:
            self.lod4 = content

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def create(cls, author: str, content: str, role: str = "user",
               conversation_id: str = "", chain: Optional[str] = None,
               llm_provider: Optional[str] = None, llm_model: Optional[str] = None) -> "MessageStone":
        """Create a new message stone"""
        now = datetime.now(timezone.utc).isoformat()
        stone_id = f"msg_{now.replace(':', '').replace('-', '').replace('.', '_')[:20]}_{hashlib.sha256(content.encode()).hexdigest()[:8]}"

        stone = cls(
            stone_id=stone_id,
            author=author,
            created=now,
            role=role,
            content=content,
            conversation_id=conversation_id,
            chain=chain,
            llm_provider=llm_provider,
            llm_model=llm_model,
        )
        stone.border_hash = stone.compute_hash()
        stone.generate_lod()
        return stone


# =============================================================================
# LLM CLIENTS
# =============================================================================

async def call_gemini(messages: List[Dict], config: LLMConfig) -> Dict:
    """Call Google Gemini API"""
    api_key = os.getenv(config.api_key_env)
    if not api_key:
        return {"error": f"Missing {config.api_key_env}"}

    # Convert messages to Gemini format
    contents = []
    for msg in messages:
        role = "user" if msg["role"] == "user" else "model"
        contents.append({
            "role": role,
            "parts": [{"text": msg["content"]}]
        })

    url = f"{config.base_url}/models/{config.model}:generateContent?key={api_key}"

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(url, json={
            "contents": contents,
            "generationConfig": {
                "maxOutputTokens": config.max_tokens,
                "temperature": config.temperature,
            }
        })

        if response.status_code != 200:
            return {"error": f"Gemini error: {response.text}"}

        data = response.json()
        text = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")

        return {
            "content": text,
            "provider": "gemini",
            "model": config.model,
            "tokens": len(text) // 4,  # Rough estimate
        }


async def call_openai_compatible(messages: List[Dict], config: LLMConfig) -> Dict:
    """Call OpenAI-compatible API (DeepSeek, Groq, etc.)"""
    api_key = os.getenv(config.api_key_env)
    if not api_key:
        return {"error": f"Missing {config.api_key_env}"}

    url = f"{config.base_url}/chat/completions"

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(
            url,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": config.model,
                "messages": messages,
                "max_tokens": config.max_tokens,
                "temperature": config.temperature,
            }
        )

        if response.status_code != 200:
            return {"error": f"{config.provider} error: {response.text}"}

        data = response.json()
        choice = data.get("choices", [{}])[0]
        text = choice.get("message", {}).get("content", "")
        usage = data.get("usage", {})

        return {
            "content": text,
            "provider": config.provider,
            "model": config.model,
            "tokens": usage.get("total_tokens", len(text) // 4),
        }


async def call_llm(provider: str, messages: List[Dict]) -> Dict:
    """Call the specified LLM provider"""
    if provider not in LLM_CONFIGS:
        return {"error": f"Unknown provider: {provider}"}

    config = LLM_CONFIGS[provider]

    if provider == "gemini":
        return await call_gemini(messages, config)
    else:
        return await call_openai_compatible(messages, config)


# =============================================================================
# CHAT ENGINE
# =============================================================================

class ChatEngine:
    """Manages conversations with LLM backends"""

    def __init__(self, redis_client=None):
        self.redis = redis_client

    def get_conversation_key(self, conversation_id: str) -> str:
        return f"chat:conversation:{conversation_id}"

    def get_user_conversations_key(self, user_id: str) -> str:
        return f"chat:user:{user_id}:conversations"

    async def create_conversation(self, user_id: str, bot_type: str = "assistant") -> Dict:
        """Create a new conversation"""
        conv_id = f"conv_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{hashlib.sha256(user_id.encode()).hexdigest()[:8]}"

        conversation = {
            "conversation_id": conv_id,
            "user_id": user_id,
            "bot_type": bot_type,
            "created": datetime.now(timezone.utc).isoformat(),
            "message_count": 0,
            "last_message": None,
        }

        if self.redis:
            self.redis.set(self.get_conversation_key(conv_id), json.dumps(conversation))
            self.redis.sadd(self.get_user_conversations_key(user_id), conv_id)

        return conversation

    async def send_message(
        self,
        conversation_id: str,
        user_id: str,
        content: str,
        provider: str = "groq",  # Default to Groq (fastest/cheapest)
        bot_type: str = "assistant"
    ) -> Dict:
        """Send a message and get AI response"""

        # Create user message stone
        user_stone = MessageStone.create(
            author=user_id,
            content=content,
            role="user",
            conversation_id=conversation_id,
        )

        # Get conversation history
        history = await self.get_conversation_history(conversation_id, limit=10)

        # Build messages for LLM
        system_prompt = BOT_PERSONAS.get(bot_type, BOT_PERSONAS["assistant"])
        messages = [{"role": "system", "content": system_prompt}]

        for msg in history:
            messages.append({
                "role": msg.get("role", "user"),
                "content": msg.get("content", ""),
            })

        messages.append({"role": "user", "content": content})

        # Call LLM
        response = await call_llm(provider, messages)

        if "error" in response:
            return {"success": False, "error": response["error"]}

        # Create assistant message stone
        assistant_stone = MessageStone.create(
            author=f"bot_{bot_type}",
            content=response["content"],
            role="assistant",
            conversation_id=conversation_id,
            chain=user_stone.stone_id,
            llm_provider=response["provider"],
            llm_model=response["model"],
        )
        assistant_stone.tokens_used = response.get("tokens", 0)

        # Store messages
        if self.redis:
            msg_key = f"chat:messages:{conversation_id}"
            self.redis.rpush(msg_key, json.dumps(user_stone.to_dict()))
            self.redis.rpush(msg_key, json.dumps(assistant_stone.to_dict()))

            # Update conversation metadata
            conv = self.redis.get(self.get_conversation_key(conversation_id))
            if conv:
                conv_data = json.loads(conv)
                conv_data["message_count"] = conv_data.get("message_count", 0) + 2
                conv_data["last_message"] = datetime.now(timezone.utc).isoformat()
                self.redis.set(self.get_conversation_key(conversation_id), json.dumps(conv_data))

        return {
            "success": True,
            "user_message": user_stone.to_dict(),
            "assistant_message": assistant_stone.to_dict(),
            "provider": response["provider"],
            "model": response["model"],
            "tokens": response.get("tokens", 0),
        }

    async def get_conversation_history(self, conversation_id: str, limit: int = 50) -> List[Dict]:
        """Get conversation message history"""
        if not self.redis:
            return []

        msg_key = f"chat:messages:{conversation_id}"
        messages = self.redis.lrange(msg_key, -limit, -1)

        return [json.loads(m) for m in messages]

    async def get_user_conversations(self, user_id: str) -> List[Dict]:
        """Get all conversations for a user"""
        if not self.redis:
            return []

        conv_ids = self.redis.smembers(self.get_user_conversations_key(user_id))
        conversations = []

        for conv_id in conv_ids:
            conv = self.redis.get(self.get_conversation_key(conv_id))
            if conv:
                conversations.append(json.loads(conv))

        # Sort by last message
        conversations.sort(key=lambda c: c.get("last_message", ""), reverse=True)
        return conversations


# =============================================================================
# WELCOME BOT
# =============================================================================

async def send_welcome_message(user_id: str, username: str, redis_client=None) -> Dict:
    """Send welcome message to new user"""
    engine = ChatEngine(redis_client)

    # Create welcome conversation
    conv = await engine.create_conversation(user_id, bot_type="welcome")

    # Generate personalized welcome
    welcome_prompt = f"""A new user named "{username}" just joined QA.Stone.

Give them a warm, brief welcome (2-3 paragraphs max). Explain:
1. What QA.Stone is (email for AI - verified, progressive, secure)
2. What they can do (chat with AI, send messages to others, manage their wallet)
3. Invite them to ask questions

Be friendly and concise. Use their name."""

    result = await engine.send_message(
        conversation_id=conv["conversation_id"],
        user_id="system",
        content=welcome_prompt,
        provider="groq",  # Groq is fastest for welcome
        bot_type="welcome"
    )

    return {
        "success": True,
        "conversation_id": conv["conversation_id"],
        "welcome_message": result.get("assistant_message", {}).get("content", ""),
    }


# =============================================================================
# TEST
# =============================================================================

if __name__ == "__main__":
    import asyncio

    async def test():
        # Test message stone
        stone = MessageStone.create(
            author="user_alice",
            content="Hello! How does QA.Stone work?",
            role="user",
            conversation_id="test_conv_1"
        )
        print(f"Created stone: {stone.stone_id}")
        print(f"Border hash: {stone.border_hash}")
        print(f"LOD5: {stone.lod5}")

        # Test LLM call (if keys available)
        if os.getenv("GROQ_API_KEY"):
            result = await call_llm("groq", [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Say hello in exactly 5 words."}
            ])
            print(f"Groq response: {result}")

    asyncio.run(test())

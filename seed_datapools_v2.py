#!/usr/bin/env python3
"""
Seed Data Pools V2 - Enhanced Demo Data

Creates realistic data pools for demo accounts.
NO AUTO-BRIDGES - users must connect through:
- Invite links (shareable URLs)
- Bridge requests
- Mutual data exchanges

This demonstrates the "social via data" model.
"""

import sys
import random
import argparse
from datetime import datetime, timezone

from redis_accounts import (
    list_users,
    get_redis,
    test_connection,
    generate_user_id,
)
from redis_datapools_v2 import (
    create_pool_v2,
    get_pool_v2,
    get_user_pools_v2,
    create_invite_link,
    get_pool_stats_v2,
    PoolCategory,
    PoolPrivacy,
    PoolMetadata,
    PoolPricing,
    CATEGORY_INFO,
)


# =============================================================================
# POOL TEMPLATES BY CATEGORY
# =============================================================================

POOL_TEMPLATES = {
    "conversations": [
        {
            "name": "Claude Chat Archive 2024",
            "description": "500+ conversations with Claude covering coding, writing, and analysis. Includes system prompts used.",
            "tags": ["claude", "llm", "conversations", "coding", "analysis"],
            "size_bytes": 45_000_000,
            "item_count": 523,
            "format": "json",
        },
        {
            "name": "GPT-4 Creative Writing Sessions",
            "description": "200 creative writing sessions with GPT-4. Stories, poems, and worldbuilding.",
            "tags": ["gpt4", "creative", "writing", "stories"],
            "size_bytes": 28_000_000,
            "item_count": 200,
            "format": "json",
        },
        {
            "name": "Customer Support Dialogs",
            "description": "15K anonymized support conversations with sentiment labels.",
            "tags": ["support", "sentiment", "labeled", "customer-service"],
            "size_bytes": 450_000_000,
            "item_count": 15000,
            "format": "json",
        },
    ],
    "prompts": [
        {
            "name": "System Prompts Collection",
            "description": "150 battle-tested system prompts for various use cases. Coding, analysis, creative.",
            "tags": ["system-prompts", "templates", "coding", "analysis"],
            "size_bytes": 2_500_000,
            "item_count": 150,
            "format": "json",
        },
        {
            "name": "Jailbreak Research Archive",
            "description": "Academic research on LLM safety. Historical jailbreaks and mitigations.",
            "tags": ["safety", "research", "jailbreaks", "academic"],
            "size_bytes": 15_000_000,
            "item_count": 500,
            "format": "markdown",
        },
        {
            "name": "Prompt Engineering Patterns",
            "description": "Categorized prompt patterns: chain-of-thought, few-shot, role-play, etc.",
            "tags": ["patterns", "techniques", "few-shot", "chain-of-thought"],
            "size_bytes": 8_000_000,
            "item_count": 200,
            "format": "json",
        },
    ],
    "integrations": [
        {
            "name": "MCP Server Configs",
            "description": "15 pre-configured MCP servers: GitHub, Slack, databases, APIs.",
            "tags": ["mcp", "servers", "configs", "integrations"],
            "size_bytes": 500_000,
            "item_count": 15,
            "format": "json",
        },
        {
            "name": "API Workflow Templates",
            "description": "50 n8n/Zapier style workflow templates with API configs.",
            "tags": ["workflows", "automation", "apis", "templates"],
            "size_bytes": 12_000_000,
            "item_count": 50,
            "format": "json",
        },
    ],
    "media": [
        {
            "name": "AI Generated Art Collection",
            "description": "5K Midjourney/DALL-E images with prompts. Various styles.",
            "tags": ["ai-art", "midjourney", "dalle", "generated"],
            "size_bytes": 8_500_000_000,
            "item_count": 5000,
            "format": "png/jpg",
        },
        {
            "name": "Voice Clone Training Set",
            "description": "10 hours of voice recordings for TTS training.",
            "tags": ["voice", "tts", "audio", "training"],
            "size_bytes": 3_600_000_000,
            "item_count": 1000,
            "format": "wav",
        },
    ],
    "documents": [
        {
            "name": "Research Papers Archive",
            "description": "500 ML/AI research papers with annotations.",
            "tags": ["research", "papers", "ml", "ai", "annotated"],
            "size_bytes": 2_500_000_000,
            "item_count": 500,
            "format": "pdf",
        },
        {
            "name": "Personal Knowledge Base",
            "description": "2000 Obsidian notes on programming, AI, and productivity.",
            "tags": ["notes", "obsidian", "knowledge-base", "programming"],
            "size_bytes": 150_000_000,
            "item_count": 2000,
            "format": "markdown",
        },
        {
            "name": "Book Highlights & Notes",
            "description": "Highlights and notes from 100 non-fiction books.",
            "tags": ["books", "highlights", "notes", "non-fiction"],
            "size_bytes": 25_000_000,
            "item_count": 100,
            "format": "json",
        },
    ],
    "analytics": [
        {
            "name": "Browser History 2024",
            "description": "Full browsing history with timestamps. 50K URLs.",
            "tags": ["browser", "history", "urls", "timestamps"],
            "size_bytes": 200_000_000,
            "item_count": 50000,
            "format": "json",
        },
        {
            "name": "App Usage Analytics",
            "description": "Mobile app usage patterns. Screen time, opens, sessions.",
            "tags": ["mobile", "usage", "screen-time", "analytics"],
            "size_bytes": 350_000_000,
            "item_count": 100000,
            "format": "json",
        },
        {
            "name": "Click Stream Data",
            "description": "E-commerce click data. 1M events with user journeys.",
            "tags": ["clicks", "ecommerce", "journeys", "events"],
            "size_bytes": 1_500_000_000,
            "item_count": 1000000,
            "format": "parquet",
        },
    ],
    "location": [
        {
            "name": "Location History 2020-2024",
            "description": "5 years of GPS data. 2M location points.",
            "tags": ["gps", "location", "history", "travel"],
            "size_bytes": 500_000_000,
            "item_count": 2000000,
            "format": "json",
        },
        {
            "name": "Favorite Places Collection",
            "description": "500 saved places with photos and notes.",
            "tags": ["places", "favorites", "photos", "notes"],
            "size_bytes": 1_200_000_000,
            "item_count": 500,
            "format": "mixed",
        },
    ],
    "training_data": [
        {
            "name": "Fine-tuning Dataset: Code",
            "description": "10K instruction-response pairs for code generation.",
            "tags": ["fine-tuning", "code", "instructions", "responses"],
            "size_bytes": 150_000_000,
            "item_count": 10000,
            "format": "jsonl",
        },
        {
            "name": "RLHF Preference Data",
            "description": "5K human preference comparisons for RLHF.",
            "tags": ["rlhf", "preferences", "human-feedback", "training"],
            "size_bytes": 75_000_000,
            "item_count": 5000,
            "format": "jsonl",
        },
        {
            "name": "Multilingual QA Dataset",
            "description": "100K Q&A pairs in 10 languages.",
            "tags": ["multilingual", "qa", "translations", "training"],
            "size_bytes": 800_000_000,
            "item_count": 100000,
            "format": "jsonl",
        },
    ],
    "code": [
        {
            "name": "Personal GitHub Archive",
            "description": "50 repositories, 200K lines of code.",
            "tags": ["github", "repositories", "code", "projects"],
            "size_bytes": 250_000_000,
            "item_count": 50,
            "format": "mixed",
        },
        {
            "name": "Dotfiles & Configs",
            "description": "Complete dev environment setup. Shell, editor, tools.",
            "tags": ["dotfiles", "configs", "vim", "zsh", "dev-env"],
            "size_bytes": 5_000_000,
            "item_count": 100,
            "format": "mixed",
        },
        {
            "name": "Code Snippets Library",
            "description": "2K curated code snippets. Python, JS, Go, Rust.",
            "tags": ["snippets", "python", "javascript", "go", "rust"],
            "size_bytes": 15_000_000,
            "item_count": 2000,
            "format": "json",
        },
    ],
    "creative": [
        {
            "name": "Short Stories Collection",
            "description": "100 original short stories. Sci-fi, fantasy, literary.",
            "tags": ["stories", "fiction", "creative-writing", "original"],
            "size_bytes": 50_000_000,
            "item_count": 100,
            "format": "markdown",
        },
        {
            "name": "Music Compositions",
            "description": "50 original music tracks with stems.",
            "tags": ["music", "compositions", "stems", "original"],
            "size_bytes": 5_000_000_000,
            "item_count": 50,
            "format": "wav/midi",
        },
    ],
    "raw": [
        {
            "name": "Email Export 2020-2024",
            "description": "Complete email archive. 50K emails with attachments.",
            "tags": ["email", "archive", "export", "attachments"],
            "size_bytes": 10_000_000_000,
            "item_count": 50000,
            "format": "mbox",
        },
        {
            "name": "Spam Collection",
            "description": "100K spam emails for filter training.",
            "tags": ["spam", "email", "training", "security"],
            "size_bytes": 2_000_000_000,
            "item_count": 100000,
            "format": "json",
        },
        {
            "name": "Deleted Files Recovery",
            "description": "Recovered deleted files. Mixed types.",
            "tags": ["deleted", "recovery", "mixed", "archive"],
            "size_bytes": 15_000_000_000,
            "item_count": 50000,
            "format": "mixed",
        },
    ],
}


# Privacy presets
PRIVACY_PRESETS = {
    "private": {
        "visibility": "private",
        "sharing_mode": "off",
        "access_level": "view",
        "require_approval": True,
    },
    "link_only": {
        "visibility": "unlisted",
        "sharing_mode": "link_only",
        "access_level": "view",
        "require_approval": False,
    },
    "request": {
        "visibility": "public",
        "sharing_mode": "request",
        "access_level": "view",
        "require_approval": True,
    },
    "open": {
        "visibility": "public",
        "sharing_mode": "open",
        "access_level": "download",
        "require_approval": False,
    },
}


# Demo account assignments
DEMO_ASSIGNMENTS = {
    "alice": {
        "persona": "AI researcher & prompt engineer",
        "pools": [
            ("prompts", 0, "open"),           # System Prompts - open
            ("conversations", 0, "request"),  # Claude chats - request only
            ("training_data", 0, "link_only"), # Fine-tuning data - invite only
        ],
    },
    "bob": {
        "persona": "Full-stack developer sharing tools",
        "pools": [
            ("code", 2, "open"),              # Snippets - open
            ("integrations", 0, "request"),   # MCP configs - request
            ("documents", 1, "link_only"),    # Knowledge base - invite
        ],
    },
    "charlie": {
        "persona": "ML engineer with training data",
        "pools": [
            ("training_data", 1, "request"),  # RLHF data - request
            ("analytics", 2, "link_only"),    # Click data - invite
            ("code", 0, "open"),              # GitHub archive - open
        ],
    },
    "diana": {
        "persona": "Content creator & writer",
        "pools": [
            ("creative", 0, "open"),          # Stories - open
            ("documents", 2, "request"),      # Book notes - request
            ("prompts", 2, "link_only"),      # Prompt patterns - invite
        ],
    },
    "eve": {
        "persona": "Security researcher & data hoarder",
        "pools": [
            ("raw", 1, "request"),            # Spam collection - request
            ("analytics", 0, "link_only"),    # Browser history - invite
            ("prompts", 1, "private"),        # Jailbreak research - private
        ],
    },
}


# =============================================================================
# SEEDING FUNCTIONS
# =============================================================================

def seed_user_pools_v2(username: str, assignment: dict, verbose: bool = True) -> dict:
    """Seed pools for a user with V2 schema"""
    results = {"pools_created": 0, "errors": []}
    user_id = generate_user_id(username)

    for category, template_idx, privacy_preset in assignment["pools"]:
        templates = POOL_TEMPLATES.get(category, [])
        if template_idx >= len(templates):
            template_idx = 0

        template = templates[template_idx]
        privacy = PRIVACY_PRESETS.get(privacy_preset, PRIVACY_PRESETS["private"])

        # Pricing based on privacy
        if privacy_preset == "open":
            pricing = {"model": "free", "price_cents": 0}
        elif privacy_preset == "request":
            pricing = {"model": "free", "price_cents": 0}  # Free but need approval
        else:
            pricing = {"model": "credits", "credits_required": random.randint(10, 100)}

        metadata = {
            "description": template["description"],
            "tags": template["tags"],
            "size_bytes": template["size_bytes"],
            "item_count": template["item_count"],
            "format": template["format"],
            "source": "demo_seed",
            "quality_score": round(random.uniform(3.5, 5.0), 1),
        }

        result = create_pool_v2(
            owner_id=user_id,
            name=template["name"],
            category=category,
            privacy=privacy,
            metadata=metadata,
            pricing=pricing,
            source_type="import",
        )

        if result.get("success"):
            results["pools_created"] += 1
            if verbose:
                icon = CATEGORY_INFO.get(PoolCategory(category), {}).get("icon", "📦")
                print(f"   {icon} {template['name']} [{privacy_preset}]")
        else:
            results["errors"].append(result.get("error"))

    return results


def seed_demo_accounts_v2(verbose: bool = True) -> dict:
    """Seed all demo accounts with V2 pools - NO AUTO BRIDGES"""
    results = {
        "users_seeded": 0,
        "pools_created": 0,
        "errors": [],
    }

    for username, config in DEMO_ASSIGNMENTS.items():
        if verbose:
            print(f"\n🔹 {username.capitalize()} - {config['persona']}")

        user_results = seed_user_pools_v2(username, config, verbose)
        results["users_seeded"] += 1
        results["pools_created"] += user_results["pools_created"]
        results["errors"].extend(user_results["errors"])

    if verbose:
        print("\n" + "=" * 50)
        print("📋 NO BRIDGES CREATED (by design)")
        print("Users must connect through:")
        print("   🔗 Invite links")
        print("   📨 Bridge requests")
        print("   🤝 Mutual data exchanges")
        print("=" * 50)

    return results


def clear_all_pools_v2() -> dict:
    """Clear all V2 pool data"""
    r = get_redis()
    patterns = [
        "pool_v2:*",
        "user_pools_v2:*",
        "all_pools_v2",
        "public_pools_v2",
        "pools_by_category:*",
        "bridge_v2:*",
        "pool_bridges:*",
        "user_bridges_in:*",
        "user_bridges_out:*",
        "pending_bridges:*",
        "invite_link:*",
        "pool_links:*",
    ]
    deleted = 0

    for pattern in patterns:
        keys = r.keys(pattern)
        if keys:
            deleted += r.delete(*keys)

    return {"deleted_keys": deleted}


def show_status_v2():
    """Show V2 pool status"""
    print("\n" + "=" * 60)
    print("📦 Data Pools V2 Status")
    print("=" * 60)

    stats = get_pool_stats_v2()
    print(f"\nMarketplace:")
    print(f"  Total Pools: {stats.get('total_pools', 0)}")
    print(f"  Public Pools: {stats.get('public_pools', 0)}")

    by_cat = stats.get("by_category", {})
    if by_cat:
        print(f"\nBy Category:")
        for cat, count in sorted(by_cat.items()):
            info = CATEGORY_INFO.get(PoolCategory(cat), {})
            icon = info.get("icon", "📦")
            print(f"  {icon} {cat}: {count}")

    users = list_users()
    if users:
        print(f"\nPools by User:")
        for user in users:
            pools = get_user_pools_v2(user["user_id"])
            if pools:
                cats = ", ".join(set(p["category"] for p in pools))
                print(f"  {user['username']}: {len(pools)} pools ({cats})")


def main():
    parser = argparse.ArgumentParser(description="Seed Data Pools V2")
    parser.add_argument("--reset", action="store_true", help="Clear and recreate")
    parser.add_argument("--status", action="store_true", help="Show status")
    args = parser.parse_args()

    print("Connecting to Redis...")
    if not test_connection():
        print("ERROR: Cannot connect to Redis")
        sys.exit(1)
    print("Connected!\n")

    if args.status:
        show_status_v2()
        return

    if args.reset:
        print("Clearing all V2 pool data...")
        clear_result = clear_all_pools_v2()
        print(f"  Deleted {clear_result['deleted_keys']} keys")

    results = seed_demo_accounts_v2()

    print("\n" + "=" * 60)
    print("✅ Seeding Complete!")
    print("=" * 60)
    print(f"  Users seeded: {results['users_seeded']}")
    print(f"  Pools created: {results['pools_created']}")
    print(f"  Bridges: 0 (connect through invite links!)")


if __name__ == "__main__":
    main()

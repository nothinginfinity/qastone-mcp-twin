#!/usr/bin/env python3
"""
Seed Data Pools for QA.Stone / Prax System

Creates diverse data pools for demo accounts and new users.
Establishes initial bridges between users for discovery.

Philosophy: "Social via Data, not Followers"
- Users connect through bridges, not friend requests
- Discovery happens through data pools
- Value flows through data exchange

Usage:
    python seed_datapools.py              # Seed pools for demo accounts
    python seed_datapools.py --reset      # Clear pools and recreate
    python seed_datapools.py --status     # Show pool stats
    python seed_datapools.py --new-user USERNAME  # Seed pools for new user
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
from redis_datapools import (
    create_pool,
    get_pool,
    get_user_pools,
    create_bridge,
    accept_bridge,
    get_pool_stats,
    list_public_pools,
    PoolType,
    PricingModel,
)


# =============================================================================
# POOL TEMPLATES - Diverse, realistic sample pools
# =============================================================================

POOL_TEMPLATES = {
    "text": [
        {
            "name": "Email Archive 2020-2024",
            "description": "47K emails with headers stripped. Multi-language, includes spam folder. Great for NLP training.",
            "tags": ["email", "nlp", "training-data", "multi-language"],
            "size_bytes": 2_400_000_000,  # 2.4GB
            "file_count": 47000,
        },
        {
            "name": "Customer Support Tickets",
            "description": "15K anonymized support tickets with sentiment labels. E-commerce domain.",
            "tags": ["support", "sentiment", "labeled", "e-commerce"],
            "size_bytes": 450_000_000,
            "file_count": 15000,
        },
        {
            "name": "Reddit Comments Dataset",
            "description": "500K comments from tech subreddits. Cleaned and deduplicated.",
            "tags": ["reddit", "social-media", "tech", "comments"],
            "size_bytes": 1_200_000_000,
            "file_count": 1,
        },
        {
            "name": "Product Reviews Collection",
            "description": "200K product reviews with star ratings. Multiple categories.",
            "tags": ["reviews", "ratings", "e-commerce", "sentiment"],
            "size_bytes": 380_000_000,
            "file_count": 200000,
        },
        {
            "name": "Chat Logs Archive",
            "description": "Anonymized chat conversations. Good for dialogue systems.",
            "tags": ["chat", "dialogue", "conversational-ai"],
            "size_bytes": 890_000_000,
            "file_count": 25000,
        },
        {
            "name": "News Articles 2023",
            "description": "100K news articles from various sources. Politics, tech, sports.",
            "tags": ["news", "articles", "journalism", "2023"],
            "size_bytes": 750_000_000,
            "file_count": 100000,
        },
    ],
    "code": [
        {
            "name": "Python Snippets Collection",
            "description": "10K Python code snippets with docstrings. Curated for quality.",
            "tags": ["python", "snippets", "learning", "documented"],
            "size_bytes": 45_000_000,
            "file_count": 10000,
        },
        {
            "name": "React Components Library",
            "description": "500 React components with TypeScript. Production-ready.",
            "tags": ["react", "typescript", "components", "frontend"],
            "size_bytes": 120_000_000,
            "file_count": 500,
        },
        {
            "name": "API Endpoint Examples",
            "description": "REST and GraphQL endpoint examples. Multiple frameworks.",
            "tags": ["api", "rest", "graphql", "backend"],
            "size_bytes": 35_000_000,
            "file_count": 2500,
        },
        {
            "name": "Shell Scripts Archive",
            "description": "Bash and Zsh scripts for DevOps. Well-commented.",
            "tags": ["bash", "shell", "devops", "automation"],
            "size_bytes": 15_000_000,
            "file_count": 800,
        },
        {
            "name": "SQL Query Collection",
            "description": "5K SQL queries from real projects. PostgreSQL/MySQL.",
            "tags": ["sql", "database", "queries", "analytics"],
            "size_bytes": 25_000_000,
            "file_count": 5000,
        },
    ],
    "media": [
        {
            "name": "Stock Photos Collection",
            "description": "5K royalty-free images. Various categories.",
            "tags": ["images", "stock-photos", "royalty-free"],
            "size_bytes": 8_500_000_000,
            "file_count": 5000,
        },
        {
            "name": "UI Screenshots Dataset",
            "description": "10K app screenshots for UI/UX research.",
            "tags": ["screenshots", "ui", "ux", "design"],
            "size_bytes": 4_200_000_000,
            "file_count": 10000,
        },
        {
            "name": "Podcast Audio Clips",
            "description": "1000 hours of podcast audio. Tech topics.",
            "tags": ["audio", "podcast", "tech", "speech"],
            "size_bytes": 25_000_000_000,
            "file_count": 3000,
        },
        {
            "name": "Icon Set Collection",
            "description": "15K icons in SVG format. Multiple styles.",
            "tags": ["icons", "svg", "design", "ui"],
            "size_bytes": 150_000_000,
            "file_count": 15000,
        },
    ],
    "location": [
        {
            "name": "City Walking Routes",
            "description": "GPS traces from 50 cities. 10K routes total.",
            "tags": ["gps", "walking", "urban", "routes"],
            "size_bytes": 500_000_000,
            "file_count": 10000,
        },
        {
            "name": "Coffee Shop Locations",
            "description": "Global coffee shop coordinates with ratings.",
            "tags": ["poi", "coffee", "global", "ratings"],
            "size_bytes": 45_000_000,
            "file_count": 1,
        },
        {
            "name": "Commute Patterns Dataset",
            "description": "Anonymized commute data from 5 metro areas.",
            "tags": ["commute", "transportation", "urban", "patterns"],
            "size_bytes": 280_000_000,
            "file_count": 50000,
        },
    ],
    "behavioral": [
        {
            "name": "E-commerce Click Data",
            "description": "User click patterns from shopping sessions. Anonymized.",
            "tags": ["clicks", "e-commerce", "ux", "patterns"],
            "size_bytes": 1_500_000_000,
            "file_count": 1,
        },
        {
            "name": "App Usage Statistics",
            "description": "Mobile app usage patterns. 100K sessions.",
            "tags": ["mobile", "usage", "sessions", "analytics"],
            "size_bytes": 350_000_000,
            "file_count": 100000,
        },
        {
            "name": "Search Query Logs",
            "description": "Anonymized search queries with timestamps.",
            "tags": ["search", "queries", "intent", "nlp"],
            "size_bytes": 680_000_000,
            "file_count": 1,
        },
    ],
    "exhaust": [
        {
            "name": "Spam Email Collection",
            "description": "50K spam emails for filter training. Labeled.",
            "tags": ["spam", "email", "security", "labeled"],
            "size_bytes": 890_000_000,
            "file_count": 50000,
        },
        {
            "name": "Deleted Files Archive",
            "description": "Recovered deleted files. Mixed types.",
            "tags": ["deleted", "recovery", "mixed", "archive"],
            "size_bytes": 5_000_000_000,
            "file_count": 25000,
        },
        {
            "name": "Error Logs Collection",
            "description": "Application error logs. Good for anomaly detection.",
            "tags": ["logs", "errors", "anomaly", "monitoring"],
            "size_bytes": 2_100_000_000,
            "file_count": 500,
        },
        {
            "name": "Failed Login Attempts",
            "description": "Security logs of failed logins. Anonymized IPs.",
            "tags": ["security", "auth", "failed-logins", "anonymized"],
            "size_bytes": 180_000_000,
            "file_count": 1,
        },
        {
            "name": "Browser Cache Dump",
            "description": "Extracted browser cache data. Web artifacts.",
            "tags": ["cache", "browser", "web", "artifacts"],
            "size_bytes": 3_500_000_000,
            "file_count": 100000,
        },
    ],
    "mixed": [
        {
            "name": "ML Training Bundle",
            "description": "Mixed dataset for ML projects. Text, images, labels.",
            "tags": ["ml", "training", "bundle", "labeled"],
            "size_bytes": 15_000_000_000,
            "file_count": 500000,
        },
        {
            "name": "Research Data Package",
            "description": "Academic research data. Papers, datasets, code.",
            "tags": ["research", "academic", "papers", "datasets"],
            "size_bytes": 8_000_000_000,
            "file_count": 10000,
        },
    ],
}

# Pricing variations
PRICING_OPTIONS = [
    {"model": "free", "price_cents": 0},
    {"model": "free", "price_cents": 0},
    {"model": "free", "price_cents": 0},  # More free pools
    {"model": "buy", "price_cents": 100},  # $1
    {"model": "buy", "price_cents": 500},  # $5
    {"model": "buy", "price_cents": 1000},  # $10
    {"model": "buy", "price_cents": 2500},  # $25
    {"model": "rent", "price_cents": 200},  # $2/month
    {"model": "rent", "price_cents": 500},  # $5/month
    {"model": "trade", "price_cents": 0},
]

# Demo account specific pool assignments
DEMO_POOL_ASSIGNMENTS = {
    "alice": {
        "pools": [
            ("text", 0),   # Email Archive
            ("code", 0),   # Python Snippets
            ("exhaust", 0),  # Spam Collection
        ],
        "personality": "Data scientist, shares NLP datasets"
    },
    "bob": {
        "pools": [
            ("code", 1),   # React Components
            ("code", 2),   # API Endpoints
            ("media", 3),  # Icon Set
        ],
        "personality": "Frontend developer, shares UI resources"
    },
    "charlie": {
        "pools": [
            ("location", 0),  # Walking Routes
            ("behavioral", 0),  # Click Data
            ("mixed", 0),  # ML Bundle
        ],
        "personality": "ML engineer, shares training data"
    },
    "diana": {
        "pools": [
            ("text", 2),   # Reddit Comments
            ("text", 3),   # Product Reviews
            ("behavioral", 2),  # Search Queries
        ],
        "personality": "NLP researcher, shares text corpora"
    },
    "eve": {
        "pools": [
            ("exhaust", 2),  # Error Logs
            ("exhaust", 3),  # Failed Logins
            ("code", 3),   # Shell Scripts
        ],
        "personality": "Security analyst, shares security data"
    },
}

# Initial bridges to create between users
INITIAL_BRIDGES = [
    ("alice", "bob", "read"),      # Alice can read Bob's code pools
    ("bob", "charlie", "read"),    # Bob can read Charlie's ML data
    ("charlie", "diana", "read"),  # Charlie can read Diana's NLP data
    ("diana", "eve", "read"),      # Diana can read Eve's security logs
    ("eve", "alice", "read"),      # Eve can read Alice's spam data
    ("alice", "charlie", "sync"),  # Alice and Charlie share ML resources
]


# =============================================================================
# SEEDING FUNCTIONS
# =============================================================================

def seed_pools_for_user(username: str, pool_assignments: list = None, verbose: bool = True) -> dict:
    """
    Seed data pools for a specific user.

    Args:
        username: The username to seed pools for
        pool_assignments: List of (pool_type, template_index) tuples
                         If None, generates random pools
    """
    results = {
        "pools_created": 0,
        "errors": [],
    }

    user_id = generate_user_id(username)

    # Get assignments or generate random ones
    if pool_assignments is None:
        # Generate 2-4 random pools for new users
        num_pools = random.randint(2, 4)
        pool_assignments = []
        pool_types = list(POOL_TEMPLATES.keys())

        for _ in range(num_pools):
            pool_type = random.choice(pool_types)
            templates = POOL_TEMPLATES[pool_type]
            template_idx = random.randint(0, len(templates) - 1)
            pool_assignments.append((pool_type, template_idx))

    for pool_type, template_idx in pool_assignments:
        templates = POOL_TEMPLATES.get(pool_type, [])
        if template_idx >= len(templates):
            template_idx = 0

        template = templates[template_idx]
        pricing = random.choice(PRICING_OPTIONS)

        # Create pool
        result = create_pool(
            owner_id=user_id,
            name=template["name"],
            pool_type=pool_type,
            visibility="public",
            pricing={
                "model": pricing["model"],
                "price_cents": pricing["price_cents"],
                "rent_duration_days": 30,
                "currency": "USD",
            },
            metadata={
                "description": template["description"],
                "tags": template["tags"],
                "size_bytes": template["size_bytes"],
                "file_count": template["file_count"],
                "format": "mixed",
                "quality_score": round(random.uniform(3.5, 5.0), 1),
                "sample_available": True,
            },
        )

        if result.get("success"):
            results["pools_created"] += 1
            if verbose:
                price_str = "Free" if pricing["model"] == "free" else f"${pricing['price_cents']/100:.2f}"
                print(f"   + [{pool_type}] {template['name']} ({price_str})")
        else:
            results["errors"].append(f"Failed to create {template['name']}: {result.get('error')}")

    return results


def seed_demo_pools(verbose: bool = True) -> dict:
    """Seed pools for all demo accounts."""
    results = {
        "users_seeded": 0,
        "pools_created": 0,
        "bridges_created": 0,
        "errors": [],
    }

    for username, config in DEMO_POOL_ASSIGNMENTS.items():
        if verbose:
            print(f"\n🔹 Seeding pools for {username}")
            print(f"   ({config['personality']})")

        user_results = seed_pools_for_user(
            username,
            pool_assignments=config["pools"],
            verbose=verbose
        )

        results["users_seeded"] += 1
        results["pools_created"] += user_results["pools_created"]
        results["errors"].extend(user_results["errors"])

    # Create initial bridges
    if verbose:
        print("\n🔗 Creating initial bridges...")

    for source_user, target_user, access_type in INITIAL_BRIDGES:
        source_id = generate_user_id(source_user)
        target_id = generate_user_id(target_user)

        # Get first pool from each user
        source_pools = get_user_pools(source_id)
        target_pools = get_user_pools(target_id)

        if source_pools and target_pools:
            bridge_result = create_bridge(
                source_pool_id=source_pools[0]["pool_id"],
                target_pool_id=target_pools[0]["pool_id"],
                requester_id=source_id,
                access_type=access_type,
            )

            if bridge_result.get("success"):
                # Auto-accept for demo
                if bridge_result.get("status") == "pending":
                    accept_bridge(bridge_result["bridge_id"], target_id)
                results["bridges_created"] += 1
                if verbose:
                    print(f"   {source_user} ↔ {target_user} ({access_type})")

    return results


def seed_new_user(username: str, verbose: bool = True) -> dict:
    """
    Seed starter pools for a newly created user.
    Called automatically on account creation.
    """
    if verbose:
        print(f"\n🆕 Generating starter pools for {username}...")

    return seed_pools_for_user(username, pool_assignments=None, verbose=verbose)


def clear_all_pools() -> dict:
    """Clear all pool-related data."""
    r = get_redis()
    patterns = [
        "pool:*",
        "user_pools:*",
        "all_pools",
        "public_pools",
        "pools_by_type:*",
        "bridge:*",
        "bridges_from:*",
        "bridges_to:*",
        "user_bridges:*",
        "pending_bridges:*",
        "pool_access:*",
        "user_pool_access:*",
        "rating:*",
    ]
    deleted = 0

    for pattern in patterns:
        keys = r.keys(pattern)
        if keys:
            deleted += r.delete(*keys)

    return {"deleted_keys": deleted}


def show_status():
    """Show current pool statistics."""
    print("\n" + "=" * 60)
    print("📦 Data Pools Status")
    print("=" * 60)

    stats = get_pool_stats()
    print(f"\nMarketplace Stats:")
    print(f"  Total Pools: {stats.get('total_pools', 0)}")
    print(f"  Public Pools: {stats.get('public_pools', 0)}")

    by_type = stats.get("by_type", {})
    if by_type:
        print(f"\n  By Type:")
        for pool_type, count in sorted(by_type.items()):
            print(f"    {pool_type}: {count}")

    users = list_users()
    if users:
        print(f"\nPools by User:")
        print("-" * 50)
        for user in users:
            user_id = user["user_id"]
            pools = get_user_pools(user_id)
            pool_count = len(pools)
            if pool_count > 0:
                pool_types = ", ".join(set(p["pool_type"] for p in pools))
                print(f"  {user['username']:<12} {pool_count} pools ({pool_types})")


def main():
    parser = argparse.ArgumentParser(description="Seed Data Pools for QA.Stone")
    parser.add_argument("--reset", action="store_true", help="Clear pools and recreate")
    parser.add_argument("--status", action="store_true", help="Show pool stats")
    parser.add_argument("--new-user", type=str, help="Seed pools for new user")
    args = parser.parse_args()

    print("Connecting to Redis...")
    if not test_connection():
        print("ERROR: Cannot connect to Redis")
        sys.exit(1)
    print("Connected!\n")

    if args.status:
        show_status()
        return

    if args.new_user:
        results = seed_new_user(args.new_user)
        print(f"\n✅ Created {results['pools_created']} pools for {args.new_user}")
        return

    if args.reset:
        print("Clearing all pool data...")
        clear_result = clear_all_pools()
        print(f"  Deleted {clear_result['deleted_keys']} keys")

    results = seed_demo_pools()

    print("\n" + "=" * 60)
    print("✅ Seeding Complete!")
    print("=" * 60)
    print(f"  Users seeded: {results['users_seeded']}")
    print(f"  Pools created: {results['pools_created']}")
    print(f"  Bridges created: {results['bridges_created']}")

    if results["errors"]:
        print(f"\n⚠️ Errors ({len(results['errors'])}):")
        for error in results["errors"][:5]:
            print(f"  - {error}")


if __name__ == "__main__":
    main()

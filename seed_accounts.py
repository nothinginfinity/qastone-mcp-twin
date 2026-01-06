#!/usr/bin/env python3
"""
Seed Demo Accounts for QA.Stone System

Creates 5 demo accounts with starting balances.

Usage:
    python seed_accounts.py          # Create accounts + seed stones
    python seed_accounts.py --reset  # Clear all and recreate
    python seed_accounts.py --status # Show current state
"""

import sys
import argparse

from redis_accounts import (
    create_account,
    mint_to_wallet,
    list_users,
    get_stats,
    test_connection,
    generate_user_id,
    get_redis,
)
from qastone_mint import mint_offer

# =============================================================================
# DEMO ACCOUNTS
# =============================================================================

DEMO_ACCOUNTS = [
    {
        "username": "alice",
        "token": "demo_alice_2026",
        "starting_stones": [
            {"sponsor": "Amazon", "type": "gift_card", "value_cents": 2000},
            {"sponsor": "Anthropic", "type": "api_token", "value_cents": 1500},
            {"sponsor": "Starbucks", "type": "gift_card", "value_cents": 500},
        ],
    },
    {
        "username": "bob",
        "token": "demo_bob_2026",
        "starting_stones": [
            {"sponsor": "Steam", "type": "gift_card", "value_cents": 2500},
            {"sponsor": "Netflix", "type": "gift_card", "value_cents": 1500},
            {"sponsor": "Uber", "type": "coupon", "value_cents": 1000},
        ],
    },
    {
        "username": "charlie",
        "token": "demo_charlie_2026",
        "starting_stones": [
            {"sponsor": "Apple", "type": "gift_card", "value_cents": 2000},
            {"sponsor": "OpenAI", "type": "api_token", "value_cents": 2000},
            {"sponsor": "Spotify", "type": "gift_card", "value_cents": 1000},
        ],
    },
    {
        "username": "diana",
        "token": "demo_diana_2026",
        "starting_stones": [
            {"sponsor": "Google", "type": "api_token", "value_cents": 1500},
            {"sponsor": "Target", "type": "gift_card", "value_cents": 2000},
            {"sponsor": "Lyft", "type": "coupon", "value_cents": 500},
            {"sponsor": "DoorDash", "type": "coupon", "value_cents": 1000},
        ],
    },
    {
        "username": "eve",
        "token": "demo_eve_2026",
        "starting_stones": [
            {"sponsor": "Microsoft", "type": "api_token", "value_cents": 2500},
            {"sponsor": "Best Buy", "type": "gift_card", "value_cents": 1500},
            {"sponsor": "Grubhub", "type": "coupon", "value_cents": 1000},
        ],
    },
]


def seed_accounts(verbose: bool = True) -> dict:
    """Create all demo accounts with starting stones."""
    results = {
        "accounts_created": 0,
        "stones_minted": 0,
        "total_value_cents": 0,
        "errors": [],
    }

    for account_def in DEMO_ACCOUNTS:
        username = account_def["username"]
        token = account_def["token"]

        if verbose:
            print(f"\n Creating account: {username}")

        account_result = create_account(username, custom_token=token)

        if not account_result.get("success"):
            if "already exists" in account_result.get("error", ""):
                if verbose:
                    print(f"   Account already exists, skipping...")
                continue
            results["errors"].append(f"Failed to create {username}: {account_result.get('error')}")
            continue

        results["accounts_created"] += 1
        user_id = account_result["user_id"]

        if verbose:
            print(f"   User ID: {user_id}")
            print(f"   Token: {token}")

        for stone_def in account_def["starting_stones"]:
            stone = mint_offer(
                stone_def["sponsor"],
                stone_def["type"],
                stone_def["value_cents"],
            )

            mint_result = mint_to_wallet(user_id, stone)

            if mint_result.get("success"):
                results["stones_minted"] += 1
                results["total_value_cents"] += stone_def["value_cents"]
                if verbose:
                    print(f"   + {stone_def['sponsor']} {stone_def['type']}: ${stone_def['value_cents']/100:.2f}")
            else:
                results["errors"].append(f"Failed to mint stone for {username}")

    return results


def clear_all_data() -> dict:
    """Clear all QA.Stone data."""
    r = get_redis()
    patterns = ["account:*", "wallet:*", "stone:*", "inbox:*", "token:*"]
    deleted = 0

    for pattern in patterns:
        keys = r.keys(pattern)
        if keys:
            deleted += r.delete(*keys)

    r.delete("usernames", "user_ids")
    return {"deleted_keys": deleted}


def show_status():
    """Show current state of all accounts."""
    print("\n" + "=" * 60)
    print("QA.Stone Demo Accounts Status")
    print("=" * 60)

    stats = get_stats()
    print(f"\nSystem Stats:")
    print(f"  Users: {stats['users']}")
    print(f"  Total Stones: {stats['stones']}")
    print(f"  Total Value: {stats['total_value_usd']}")
    print(f"  Redis Connected: {stats['redis_connected']}")

    users = list_users()
    if users:
        print(f"\nAccounts ({len(users)}):")
        print("-" * 50)
        for user in users:
            print(f"  {user['username']:<12} {user['stone_count']} stones  {user['total_value_usd']}")

        print(f"\nDemo Tokens:")
        for account_def in DEMO_ACCOUNTS:
            print(f"  {account_def['username']:<12} → {account_def['token']}")


def main():
    parser = argparse.ArgumentParser(description="Seed QA.Stone demo accounts")
    parser.add_argument("--reset", action="store_true", help="Clear and recreate")
    parser.add_argument("--status", action="store_true", help="Show status")
    args = parser.parse_args()

    print("Connecting to Redis...")
    if not test_connection():
        print("ERROR: Cannot connect to Redis")
        sys.exit(1)
    print("Connected!\n")

    if args.status:
        show_status()
        return

    if args.reset:
        print("Clearing all data...")
        clear_result = clear_all_data()
        print(f"  Deleted {clear_result['deleted_keys']} keys")

    results = seed_accounts()

    print("\n" + "=" * 60)
    print("Seeding Complete!")
    print("=" * 60)
    print(f"  Accounts created: {results['accounts_created']}")
    print(f"  Stones minted: {results['stones_minted']}")
    print(f"  Total value: ${results['total_value_cents']/100:.2f}")

    print("\nDemo tokens:")
    for account_def in DEMO_ACCOUNTS:
        print(f"  {account_def['username']:<10} → {account_def['token']}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
QA.Stone Minting - Simplified offer stone generation

Creates QA.Stone offer objects for the demo system.
"""

import hashlib
import random
from datetime import datetime, timezone
from typing import Dict


def mint_offer(sponsor: str, offer_type: str, value_cents: int, **kwargs) -> Dict:
    """
    Mint a QA.Stone offer.

    Args:
        sponsor: Company name (e.g., 'Amazon', 'Anthropic')
        offer_type: Type of offer ('gift_card', 'coupon', 'api_token', 'event_ticket')
        value_cents: Value in cents (e.g., 1000 = $10)

    Returns:
        QA.Stone offer dictionary
    """
    now = datetime.now(timezone.utc).isoformat()
    type_prefix = offer_type[:2].upper()
    sponsor_prefix = sponsor.lower().replace(" ", "")[:8]
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    random_suffix = random.randint(1000, 9999)

    offer_id = f"{type_prefix}_{sponsor_prefix}_{timestamp}_{random_suffix}"

    return {
        "offer_stone_id": offer_id,
        "version": "1.0.0",
        "offer_type": offer_type,
        "created_at": now,
        "offer": {
            "description": f"${value_cents / 100:.2f} {sponsor} {offer_type.replace('_', ' ').title()}",
            "value_cents": value_cents,
            "currency": "USD",
            "sponsor": {
                "id": sponsor.lower().replace(" ", "_"),
                "name": sponsor,
            },
        },
        "state": {
            "current": "minted",
            "claimed_by": None,
        },
        "audit": {
            "sha256_hash": hashlib.sha256(f"{offer_id}:{now}".encode()).hexdigest()[:32],
        },
        "display": {
            "icon": _get_offer_icon(offer_type),
            "color": _get_offer_color(offer_type),
            "rarity": _calculate_rarity(value_cents),
        }
    }


def _get_offer_icon(offer_type: str) -> str:
    """Get emoji icon for offer type."""
    icons = {
        "gift_card": "🎁",
        "coupon": "🏷️",
        "api_token": "🔑",
        "event_ticket": "🎟️",
        "ephemeral_link": "🔗",
        "loyalty_points": "⭐",
    }
    return icons.get(offer_type, "📦")


def _get_offer_color(offer_type: str) -> str:
    """Get color for offer type."""
    colors = {
        "gift_card": "#FFD700",
        "coupon": "#10B981",
        "api_token": "#3B82F6",
        "event_ticket": "#EC4899",
        "ephemeral_link": "#8B5CF6",
    }
    return colors.get(offer_type, "#8B5CF6")


def _calculate_rarity(value_cents: int) -> str:
    """Calculate rarity based on value."""
    if value_cents >= 10000:
        return "legendary"
    elif value_cents >= 5000:
        return "epic"
    elif value_cents >= 2000:
        return "rare"
    elif value_cents >= 500:
        return "uncommon"
    return "common"

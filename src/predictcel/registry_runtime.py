from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

from .models import BasketHealth, BasketMembership, WalletRegistryEntry
from .storage import SignalStore
from .wallet_registry import (
    build_live_basket_roster,
    compute_basket_health_from_static_memberships,
    rebalance_memberships_from_live_roster,
    recommend_basket_promotions,
    refresh_registry_entries_from_trades,
)


def build_wallet_registry_summary(
    config: Any,
    store: SignalStore,
    trades: list[Any],
    persist_rebalance: bool = False,
) -> dict[str, Any]:
    if not getattr(config.wallet_registry, "enabled", False):
        return {
            "enabled": False,
            "registry_wallet_count": 0,
            "memberships_by_topic": {},
            "basket_health": {},
            "live_roster_by_topic": {},
            "promotion_watch_by_topic": {},
            "basket_promotion_by_topic": {},
        }

    captured_at = datetime.now(UTC)
    existing_registry_entries = store.load_wallet_registry_entries()
    existing_memberships = store.load_basket_memberships()
    if config.wallet_registry.seed_from_baskets:
        existing_registry_entries = ensure_static_registry_bootstrap(
            config,
            store,
            existing_registry_entries,
            captured_at=captured_at,
        )
        existing_memberships = ensure_static_membership_bootstrap(
            config,
            store,
            existing_memberships,
            captured_at=captured_at,
        )

    registry_entries = (
        refresh_registry_entries_from_trades(
            config,
            store,
            trades,
            captured_at=captured_at,
        )
        or existing_registry_entries
    )
    memberships = store.load_basket_memberships() or existing_memberships
    if persist_rebalance:
        memberships = (
            rebalance_memberships_from_live_roster(
                config,
                store,
                trades,
                captured_at=captured_at,
            )
            or memberships
        )
        memberships = store.load_basket_memberships() or memberships
    basket_health = compute_basket_health_from_static_memberships(
        config,
        memberships,
        trades,
        registry_entries=registry_entries,
        captured_at=captured_at,
    )
    live_roster = build_live_basket_roster(
        config,
        memberships,
        trades,
        registry_entries=registry_entries,
        captured_at=captured_at,
    )
    promotion_watch = promotion_watch_by_topic(
        memberships,
        registry_entries,
        live_roster,
    )
    basket_promotion_by_topic = {
        topic: basket_promotion_as_dict(recommendation)
        for topic, recommendation in recommend_basket_promotions(
            config,
            memberships,
            trades,
            registry_entries=registry_entries,
            captured_at=captured_at,
        ).items()
    }
    store.save_basket_health(basket_health)
    latest_health = store.latest_basket_health()
    return {
        "enabled": True,
        "registry_wallet_count": len(registry_entries),
        "memberships_by_topic": membership_counts_by_topic(memberships),
        "basket_health": {
            topic: basket_health_as_dict(health)
            for topic, health in latest_health.items()
        },
        "live_roster_by_topic": live_roster,
        "promotion_watch_by_topic": promotion_watch,
        "basket_promotion_by_topic": basket_promotion_by_topic,
    }


def ensure_static_registry_bootstrap(
    config: Any,
    store: SignalStore,
    existing_entries: list[WalletRegistryEntry],
    *,
    captured_at: datetime,
) -> list[WalletRegistryEntry]:
    entries_by_wallet = {entry.wallet: entry for entry in existing_entries}
    updated_entries = list(existing_entries)
    updated = False
    for basket in config.baskets:
        for wallet in basket.wallets:
            normalized_wallet = str(wallet).strip()
            if not normalized_wallet:
                continue
            existing_entry = entries_by_wallet.get(normalized_wallet)
            if existing_entry is None:
                updated_entries.append(
                    WalletRegistryEntry(
                        wallet=normalized_wallet,
                        source_type="static_basket",
                        source_ref="config.baskets",
                        trust_seed=1.0,
                        status="active",
                        first_seen_at=captured_at,
                        last_seen_trade_at=None,
                        last_scored_at=None,
                        notes="seeded from static basket config",
                    )
                )
                entries_by_wallet[normalized_wallet] = updated_entries[-1]
                updated = True
                continue

            if (
                existing_entry.source_type == "static_basket"
                and existing_entry.source_ref == "config.baskets"
                and existing_entry.trust_seed == 1.0
            ):
                continue

            entries_by_wallet[normalized_wallet] = WalletRegistryEntry(
                wallet=existing_entry.wallet,
                source_type="static_basket",
                source_ref="config.baskets",
                trust_seed=1.0,
                status=existing_entry.status,
                first_seen_at=existing_entry.first_seen_at,
                last_seen_trade_at=existing_entry.last_seen_trade_at,
                last_scored_at=existing_entry.last_scored_at,
                notes=existing_entry.notes,
            )
            updated = True

    if not updated:
        return existing_entries

    updated_entries = sorted(entries_by_wallet.values(), key=lambda entry: entry.wallet)
    store.upsert_wallet_registry_entries(updated_entries)
    return updated_entries


def ensure_static_membership_bootstrap(
    config: Any,
    store: SignalStore,
    existing_memberships: list[BasketMembership],
    *,
    captured_at: datetime,
) -> list[BasketMembership]:
    locked_topics = {
        membership.topic
        for membership in existing_memberships
        if membership.promotion_reason
        not in {
            "",
            "seeded from static basket config",
            "wallet discovery assignment",
        }
    }
    memberships_by_key = {
        (membership.topic, membership.wallet): membership
        for membership in existing_memberships
    }
    updated = False
    for basket in config.baskets:
        if basket.topic in locked_topics:
            continue
        for rank, wallet in enumerate(basket.wallets, start=1):
            normalized_wallet = str(wallet).strip()
            membership_key = (basket.topic, normalized_wallet)
            if not normalized_wallet:
                continue
            existing_membership = memberships_by_key.get(membership_key)
            if existing_membership is None:
                memberships_by_key[membership_key] = BasketMembership(
                    topic=basket.topic,
                    wallet=normalized_wallet,
                    tier="core",
                    rank=rank,
                    active=True,
                    joined_at=captured_at,
                    effective_until=None,
                    promotion_reason="seeded from static basket config",
                    demotion_reason="",
                )
                updated = True
                continue

            normalized_membership = BasketMembership(
                topic=existing_membership.topic,
                wallet=existing_membership.wallet,
                tier="core",
                rank=rank,
                active=True,
                joined_at=existing_membership.joined_at,
                effective_until=None,
                promotion_reason="seeded from static basket config",
                demotion_reason="",
            )
            if normalized_membership == existing_membership:
                continue
            memberships_by_key[membership_key] = normalized_membership
            updated = True

    if not updated:
        return existing_memberships

    updated_memberships = sorted(
        memberships_by_key.values(),
        key=lambda membership: (membership.topic, membership.rank, membership.wallet),
    )
    store.upsert_basket_memberships(updated_memberships)
    return updated_memberships


def membership_counts_by_topic(memberships: list[Any]) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = defaultdict(
        lambda: {"core": 0, "rotating": 0, "backup": 0, "explorer": 0}
    )
    for membership in memberships:
        tier = str(membership.tier)
        if tier not in counts[membership.topic]:
            counts[membership.topic][tier] = 0
        counts[membership.topic][tier] += 1
    return {topic: dict(values) for topic, values in counts.items()}


def basket_health_as_dict(health: BasketHealth) -> dict[str, Any]:
    payload = asdict(health)
    payload["captured_at"] = health.captured_at.isoformat()
    return payload


def basket_promotion_as_dict(recommendation: Any) -> dict[str, Any]:
    payload = asdict(recommendation)
    payload["recommended_wallets"] = list(recommendation.recommended_wallets)
    payload["missing_requirements"] = list(recommendation.missing_requirements)
    return payload


def promotion_watch_by_topic(
    memberships: list[Any],
    registry_entries: list[Any],
    live_roster_by_topic: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    explorer_counts_by_topic: dict[str, int] = defaultdict(int)
    discovery_explorer_counts_by_topic: dict[str, int] = defaultdict(int)
    registry_entries_by_wallet = {entry.wallet: entry for entry in registry_entries}
    for membership in memberships:
        if not membership.active or membership.tier != "explorer":
            continue
        explorer_counts_by_topic[membership.topic] += 1
        entry = registry_entries_by_wallet.get(membership.wallet)
        if entry is not None and entry.source_type == "wallet_discovery":
            discovery_explorer_counts_by_topic[membership.topic] += 1

    watch: dict[str, dict[str, Any]] = {}
    for topic, explorer_count in explorer_counts_by_topic.items():
        roster_entry = live_roster_by_topic.get(topic, {})
        if not roster_entry:
            continue
        if not roster_entry.get("needs_refresh"):
            continue
        discovery_explorer_count = discovery_explorer_counts_by_topic.get(topic, 0)
        if discovery_explorer_count <= 0:
            continue
        watch[topic] = {
            "explorer_wallet_count": explorer_count,
            "wallet_discovery_explorer_wallet_count": discovery_explorer_count,
            "live_eligible_wallet_count": int(
                roster_entry.get("live_eligible_wallet_count", 0)
            ),
            "fresh_core_wallet_count": int(
                roster_entry.get("fresh_core_wallet_count", 0)
            ),
            "reason": "bench_depth_available",
        }
    return watch

"""Wallet Registry v2 foundation helpers."""
from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Iterable

from .models import BasketHealth, BasketMembership, WalletRegistryEntry, WalletTrade

if TYPE_CHECKING:
    from .config import AppConfig

TIER_ORDER = ("core", "rotating", "backup", "explorer")
TIER_PRIORITY = {tier: index for index, tier in enumerate(TIER_ORDER)}


def seed_registry_from_config(
    config: AppConfig,
    store,
    captured_at: datetime | None = None,
) -> list[WalletRegistryEntry]:
    """Seed the wallet registry from the current static basket configuration."""
    captured_at = captured_at or datetime.now(UTC)
    entries: list[WalletRegistryEntry] = []
    seen_wallets: set[str] = set()
    for basket in config.baskets:
        for wallet in basket.wallets:
            normalized_wallet = str(wallet).strip()
            if not normalized_wallet or normalized_wallet in seen_wallets:
                continue
            seen_wallets.add(normalized_wallet)
            entries.append(
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
    if entries:
        store.upsert_wallet_registry_entries(entries)
    return entries


def seed_memberships_from_config(
    config: AppConfig,
    store,
    captured_at: datetime | None = None,
) -> list[BasketMembership]:
    """Seed basket memberships from the current static basket configuration."""
    captured_at = captured_at or datetime.now(UTC)
    memberships: list[BasketMembership] = []
    for basket in config.baskets:
        for rank, wallet in enumerate(basket.wallets, start=1):
            normalized_wallet = str(wallet).strip()
            if not normalized_wallet:
                continue
            memberships.append(
                BasketMembership(
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
            )
    if memberships:
        store.upsert_basket_memberships(memberships)
    return memberships


def compute_basket_health_from_static_memberships(
    config: AppConfig,
    memberships: Iterable[BasketMembership],
    trades: Iterable[WalletTrade],
    captured_at: datetime | None = None,
) -> list[BasketHealth]:
    """Compute lightweight health diagnostics for seeded basket memberships."""
    captured_at = captured_at or datetime.now(UTC)
    active_memberships_by_topic: dict[str, list[BasketMembership]] = defaultdict(list)
    for membership in memberships:
        if membership.active:
            active_memberships_by_topic[membership.topic].append(membership)

    trades_by_topic_wallet: dict[tuple[str, str], list[WalletTrade]] = defaultdict(list)
    for trade in trades:
        trades_by_topic_wallet[(trade.topic, trade.wallet)].append(trade)

    health_snapshots: list[BasketHealth] = []
    for topic in sorted(active_memberships_by_topic):
        topic_memberships = active_memberships_by_topic[topic]
        tracked_wallet_count = len(topic_memberships)
        fresh_core_wallets_24h = 0
        fresh_active_wallets_7d = 0
        active_eligible_wallet_count = 0
        eligible_trades_7d = 0
        stale_wallet_count = 0

        for membership in topic_memberships:
            wallet_trades = trades_by_topic_wallet.get((topic, membership.wallet), [])
            has_trade_24h = any(trade.age_seconds <= 86_400 for trade in wallet_trades)
            has_trade_7d = any(trade.age_seconds <= 604_800 for trade in wallet_trades)
            has_eligible_trade = any(
                trade.age_seconds <= config.filters.max_trade_age_seconds
                for trade in wallet_trades
            )

            if membership.tier == "core" and has_trade_24h:
                fresh_core_wallets_24h += 1
            if has_trade_7d:
                fresh_active_wallets_7d += 1
            else:
                stale_wallet_count += 1
            if has_eligible_trade:
                active_eligible_wallet_count += 1

            eligible_trades_7d += sum(
                1 for trade in wallet_trades if trade.age_seconds <= 604_800
            )

        stale_ratio = (
            stale_wallet_count / tracked_wallet_count if tracked_wallet_count else 0.0
        )
        if fresh_core_wallets_24h >= 2:
            health_state = "healthy"
        elif stale_ratio > 0.5:
            health_state = "stale"
        elif active_eligible_wallet_count < 3:
            health_state = "thin"
        else:
            health_state = "thin"

        health_snapshots.append(
            BasketHealth(
                topic=topic,
                tracked_wallet_count=tracked_wallet_count,
                fresh_core_wallets_24h=fresh_core_wallets_24h,
                fresh_active_wallets_7d=fresh_active_wallets_7d,
                active_eligible_wallet_count=active_eligible_wallet_count,
                eligible_trades_7d=eligible_trades_7d,
                stale_ratio=stale_ratio,
                clustered_ratio=0.0,
                health_state=health_state,
                captured_at=captured_at,
            )
        )

    return health_snapshots


def build_live_basket_roster(
    config: AppConfig,
    memberships: Iterable[BasketMembership],
    trades: Iterable[WalletTrade],
) -> dict[str, dict[str, object]]:
    """Derive a live basket roster from recent membership activity."""
    active_memberships_by_topic: dict[str, list[BasketMembership]] = defaultdict(list)
    for membership in memberships:
        if membership.active:
            active_memberships_by_topic[membership.topic].append(membership)

    trades_by_topic_wallet: dict[tuple[str, str], list[WalletTrade]] = defaultdict(list)
    for trade in trades:
        trades_by_topic_wallet[(trade.topic, trade.wallet)].append(trade)

    controller = config.basket_controller
    slot_counts = {
        "core": controller.core_slots,
        "rotating": controller.rotating_slots,
        "backup": controller.backup_slots,
        "explorer": controller.explorer_slots,
    }
    live_tiers = ["core", "rotating"]
    if controller.allow_backup_in_live_consensus:
        live_tiers.append("backup")

    roster: dict[str, dict[str, object]] = {}
    for topic in sorted(active_memberships_by_topic):
        ranked_memberships = sorted(
            active_memberships_by_topic[topic],
            key=lambda membership: _membership_activity_sort_key(
                membership,
                trades_by_topic_wallet.get((topic, membership.wallet), []),
                config.filters.max_trade_age_seconds,
            ),
        )

        selected_wallets: dict[str, list[str]] = {tier: [] for tier in TIER_ORDER}
        selected_memberships: dict[str, list[BasketMembership]] = {
            tier: [] for tier in TIER_ORDER
        }
        start = 0
        for tier in TIER_ORDER:
            slots = slot_counts[tier]
            if slots <= 0:
                continue
            tier_memberships = ranked_memberships[start : start + slots]
            selected_memberships[tier] = tier_memberships
            selected_wallets[tier] = [membership.wallet for membership in tier_memberships]
            start += slots

        fresh_core_wallet_count = sum(
            1
            for membership in selected_memberships["core"]
            if _has_trade_within(
                trades_by_topic_wallet.get((topic, membership.wallet), []),
                86_400,
            )
        )
        live_eligible_wallet_count = sum(
            1
            for tier in live_tiers
            for membership in selected_memberships[tier]
            if _has_trade_within(
                trades_by_topic_wallet.get((topic, membership.wallet), []),
                config.filters.max_trade_age_seconds,
            )
        )
        unfilled_slots = {
            tier: max(0, slot_counts[tier] - len(selected_wallets[tier]))
            for tier in TIER_ORDER
        }
        refresh_reasons: list[str] = []
        if fresh_core_wallet_count < controller.force_refresh_if_fresh_core_below:
            refresh_reasons.append("fresh_core_below_threshold")
        if live_eligible_wallet_count < controller.min_active_eligible_wallets:
            refresh_reasons.append("live_eligible_wallets_below_threshold")
        if unfilled_slots["core"] > 0:
            refresh_reasons.append("core_slots_unfilled")

        roster[topic] = {
            "selected_wallets": selected_wallets,
            "fresh_core_wallet_count": fresh_core_wallet_count,
            "live_eligible_wallet_count": live_eligible_wallet_count,
            "tracked_wallet_count": len(active_memberships_by_topic[topic]),
            "unfilled_slots": unfilled_slots,
            "needs_refresh": bool(refresh_reasons),
            "refresh_reasons": refresh_reasons,
        }

    return roster


def _membership_activity_sort_key(
    membership: BasketMembership,
    trades: list[WalletTrade],
    max_trade_age_seconds: int,
) -> tuple[int, int, int, int, int, int, int, str]:
    eligible_trade_count = sum(
        1 for trade in trades if trade.age_seconds <= max_trade_age_seconds
    )
    recent_trade_count_24h = sum(1 for trade in trades if trade.age_seconds <= 86_400)
    active_trade_count_7d = sum(1 for trade in trades if trade.age_seconds <= 604_800)
    freshest_trade_age_seconds = min(
        (trade.age_seconds for trade in trades),
        default=10**12,
    )
    return (
        0 if eligible_trade_count > 0 else 1,
        freshest_trade_age_seconds,
        -recent_trade_count_24h,
        -active_trade_count_7d,
        -eligible_trade_count,
        TIER_PRIORITY.get(membership.tier, len(TIER_PRIORITY)),
        membership.rank,
        membership.wallet,
    )


def _has_trade_within(trades: list[WalletTrade], max_age_seconds: int) -> bool:
    return any(trade.age_seconds <= max_age_seconds for trade in trades)

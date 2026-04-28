"""Wallet Registry v2 foundation helpers."""
from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Iterable

from .models import BasketHealth, BasketMembership, WalletRegistryEntry, WalletTrade

if TYPE_CHECKING:
    from .config import AppConfig


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

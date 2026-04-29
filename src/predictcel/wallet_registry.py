"""Wallet Registry v2 foundation helpers."""
from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Iterable

from .models import (
    BasketAssignment,
    BasketHealth,
    BasketMembership,
    WalletDiscoveryCandidate,
    WalletRegistryEntry,
    WalletTrade,
)

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


def ingest_wallet_discovery_inputs(
    config: AppConfig,
    store,
    candidates: Iterable[WalletDiscoveryCandidate],
    assignments: Iterable[BasketAssignment],
    captured_at: datetime | None = None,
) -> tuple[list[WalletRegistryEntry], list[BasketMembership]]:
    """Upsert accepted wallet discovery inputs into the registry and explorer tier."""
    captured_at = captured_at or datetime.now(UTC)
    configured_topics = {basket.topic for basket in config.baskets}

    existing_entries = list(store.load_wallet_registry_entries())
    entries_by_wallet = {entry.wallet: entry for entry in existing_entries}
    accepted_candidates = {
        candidate.wallet_address: candidate
        for candidate in candidates
        if not candidate.rejected_reasons
    }

    for wallet, candidate in accepted_candidates.items():
        if wallet in entries_by_wallet:
            continue
        entries_by_wallet[wallet] = WalletRegistryEntry(
            wallet=wallet,
            source_type="wallet_discovery",
            source_ref=candidate.source,
            trust_seed=candidate.score,
            status="probation",
            first_seen_at=captured_at,
            last_seen_trade_at=None,
            last_scored_at=captured_at,
            notes=f"discovered for {candidate.topic_profile.primary_topic}",
        )

    updated_entries = sorted(entries_by_wallet.values(), key=lambda entry: entry.wallet)
    if updated_entries:
        store.upsert_wallet_registry_entries(updated_entries)

    existing_memberships = list(store.load_basket_memberships())
    memberships_by_key = {
        (membership.topic, membership.wallet): membership
        for membership in existing_memberships
    }
    next_rank_by_topic: dict[str, int] = defaultdict(int)
    for membership in existing_memberships:
        next_rank_by_topic[membership.topic] = max(
            next_rank_by_topic[membership.topic],
            membership.rank,
        )

    for assignment in assignments:
        if assignment.wallet_address not in accepted_candidates:
            continue
        for topic in assignment.recommended_baskets:
            if topic not in configured_topics:
                continue
            membership_key = (topic, assignment.wallet_address)
            existing_membership = memberships_by_key.get(membership_key)
            if existing_membership is None:
                next_rank_by_topic[topic] += 1
                memberships_by_key[membership_key] = BasketMembership(
                    topic=topic,
                    wallet=assignment.wallet_address,
                    tier="explorer",
                    rank=next_rank_by_topic[topic],
                    active=True,
                    joined_at=captured_at,
                    effective_until=None,
                    promotion_reason="wallet discovery assignment",
                    demotion_reason="",
                )
                continue

            memberships_by_key[membership_key] = BasketMembership(
                topic=existing_membership.topic,
                wallet=existing_membership.wallet,
                tier=existing_membership.tier,
                rank=existing_membership.rank,
                active=True,
                joined_at=existing_membership.joined_at,
                effective_until=None,
                promotion_reason=existing_membership.promotion_reason,
                demotion_reason="",
            )

    updated_memberships = sorted(
        memberships_by_key.values(),
        key=lambda membership: (membership.topic, membership.rank, membership.wallet),
    )
    if updated_memberships:
        store.upsert_basket_memberships(updated_memberships)
    return updated_entries, updated_memberships


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
    registry_entries: Iterable[WalletRegistryEntry] | None = None,
    captured_at: datetime | None = None,
) -> dict[str, dict[str, object]]:
    """Derive a live basket roster from recent membership activity."""
    captured_at = captured_at or datetime.now(UTC)
    active_memberships_by_topic: dict[str, list[BasketMembership]] = defaultdict(list)
    for membership in memberships:
        if membership.active:
            active_memberships_by_topic[membership.topic].append(membership)

    trades_by_topic_wallet: dict[tuple[str, str], list[WalletTrade]] = defaultdict(list)
    for trade in trades:
        trades_by_topic_wallet[(trade.topic, trade.wallet)].append(trade)

    controller = config.basket_controller
    registry_config = config.wallet_registry
    registry_status_by_wallet = {
        entry.wallet: entry.status for entry in (registry_entries or [])
    }
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
        eligible_market_ids_by_wallet = {
            membership.wallet: _eligible_market_ids(
                trades_by_topic_wallet.get((topic, membership.wallet), []),
                config.filters.max_trade_age_seconds,
            )
            for membership in ranked_memberships
        }

        selected_wallets: dict[str, list[str]] = {tier: [] for tier in TIER_ORDER}
        selected_memberships: dict[str, list[BasketMembership]] = {
            tier: [] for tier in TIER_ORDER
        }
        selected_live_wallets: list[str] = []
        remaining_memberships = list(ranked_memberships)
        for tier in TIER_ORDER:
            slots = slot_counts[tier]
            if slots <= 0:
                continue
            tier_memberships: list[BasketMembership] = []
            for membership in remaining_memberships:
                if len(tier_memberships) >= slots:
                    break
                if not _status_allowed_for_tier(
                    registry_status_by_wallet.get(membership.wallet, "active"),
                    tier,
                    backup_is_live=controller.allow_backup_in_live_consensus,
                ):
                    continue
                if tier in live_tiers and _exceeds_live_cluster_limit(
                    membership.wallet,
                    selected_live_wallets,
                    eligible_market_ids_by_wallet,
                    registry_config.max_cluster_overlap_ratio,
                    registry_config.max_cluster_members_in_live_tiers,
                ):
                    continue
                tier_memberships.append(membership)
            selected_memberships[tier] = tier_memberships
            selected_wallets[tier] = [membership.wallet for membership in tier_memberships]
            if tier in live_tiers:
                selected_live_wallets.extend(selected_wallets[tier])
            selected_set = {membership.wallet for membership in tier_memberships}
            remaining_memberships = [
                membership
                for membership in remaining_memberships
                if membership.wallet not in selected_set
            ]

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
        oldest_rotating_wallet_age_hours = _oldest_membership_age_hours(
            selected_memberships["rotating"],
            captured_at,
        )
        rotation_due = (
            oldest_rotating_wallet_age_hours is not None
            and oldest_rotating_wallet_age_hours >= controller.rotation_interval_hours
        )
        if rotation_due:
            refresh_reasons.append("rotation_due")

        roster[topic] = {
            "selected_wallets": selected_wallets,
            "fresh_core_wallet_count": fresh_core_wallet_count,
            "live_eligible_wallet_count": live_eligible_wallet_count,
            "tracked_wallet_count": len(active_memberships_by_topic[topic]),
            "unfilled_slots": unfilled_slots,
            "rotation_interval_hours": controller.rotation_interval_hours,
            "oldest_rotating_wallet_age_hours": oldest_rotating_wallet_age_hours,
            "rotation_due": rotation_due,
            "needs_refresh": bool(refresh_reasons),
            "refresh_reasons": refresh_reasons,
        }

    return roster


def rebalance_memberships_from_live_roster(
    config: AppConfig,
    store,
    trades: Iterable[WalletTrade],
    captured_at: datetime | None = None,
) -> list[BasketMembership]:
    """Rewrite basket memberships to match the current derived live roster."""
    captured_at = captured_at or datetime.now(UTC)
    memberships = list(store.load_basket_memberships())
    if not memberships:
        return []

    registry_entries = (
        list(store.load_wallet_registry_entries())
        if hasattr(store, "load_wallet_registry_entries")
        else []
    )
    live_roster = build_live_basket_roster(
        config,
        memberships,
        trades,
        registry_entries=registry_entries,
        captured_at=captured_at,
    )
    if not _live_roster_requires_rebalance(live_roster):
        return memberships

    memberships_by_topic: dict[str, list[BasketMembership]] = defaultdict(list)
    for membership in memberships:
        memberships_by_topic[membership.topic].append(membership)

    updated_memberships: list[BasketMembership] = []
    for topic, topic_memberships in memberships_by_topic.items():
        roster_entry = live_roster.get(topic, {})
        selected_wallets = roster_entry.get("selected_wallets", {})
        selected_assignments: dict[str, tuple[str, int]] = {}
        rank = 1
        for tier in TIER_ORDER:
            wallets = selected_wallets.get(tier, []) if isinstance(selected_wallets, dict) else []
            for wallet in wallets:
                normalized_wallet = str(wallet).strip()
                if not normalized_wallet:
                    continue
                selected_assignments[normalized_wallet] = (tier, rank)
                rank += 1
        overflow_rank = rank

        for membership in topic_memberships:
            assignment = selected_assignments.get(membership.wallet)
            if assignment is None:
                updated_memberships.append(
                    BasketMembership(
                        topic=membership.topic,
                        wallet=membership.wallet,
                        tier=membership.tier,
                        rank=overflow_rank,
                        active=False,
                        joined_at=membership.joined_at,
                        effective_until=captured_at,
                        promotion_reason=membership.promotion_reason,
                        demotion_reason="dropped from live roster rebalance",
                    )
                )
                overflow_rank += 1
                continue

            tier, assigned_rank = assignment
            updated_memberships.append(
                BasketMembership(
                    topic=membership.topic,
                    wallet=membership.wallet,
                    tier=tier,
                    rank=assigned_rank,
                    active=True,
                    joined_at=membership.joined_at,
                    effective_until=None,
                    promotion_reason=(
                        membership.promotion_reason
                        if membership.tier == tier and membership.active
                        else "live roster rebalance"
                    ),
                    demotion_reason="",
                )
            )

    updated_memberships.sort(key=lambda membership: (membership.topic, membership.rank, membership.wallet))
    if updated_memberships == memberships:
        return memberships
    store.upsert_basket_memberships(updated_memberships)
    return updated_memberships


def refresh_registry_entries_from_trades(
    config: AppConfig,
    store,
    trades: Iterable[WalletTrade],
    captured_at: datetime | None = None,
) -> list[WalletRegistryEntry]:
    """Refresh registry statuses and last-seen timestamps from recent trade activity."""
    captured_at = captured_at or datetime.now(UTC)
    entries = list(store.load_wallet_registry_entries())
    if not entries:
        return []

    trades_by_wallet: dict[str, list[WalletTrade]] = defaultdict(list)
    for trade in trades:
        trades_by_wallet[trade.wallet].append(trade)

    updated_entries: list[WalletRegistryEntry] = []
    for entry in entries:
        wallet_trades = trades_by_wallet.get(entry.wallet, [])
        eligible_trade_count = sum(
            1
            for trade in wallet_trades
            if trade.age_seconds <= config.filters.max_trade_age_seconds
        )
        freshest_age_seconds = min(
            (trade.age_seconds for trade in wallet_trades),
            default=None,
        )
        last_seen_trade_at = (
            captured_at - timedelta(seconds=freshest_age_seconds)
            if freshest_age_seconds is not None
            else entry.last_seen_trade_at
        )
        status = _registry_status_from_freshness(
            config,
            entry,
            freshest_age_seconds,
            eligible_trade_count,
            captured_at,
        )
        updated_entries.append(
            WalletRegistryEntry(
                wallet=entry.wallet,
                source_type=entry.source_type,
                source_ref=entry.source_ref,
                trust_seed=entry.trust_seed,
                status=status,
                first_seen_at=entry.first_seen_at,
                last_seen_trade_at=last_seen_trade_at,
                last_scored_at=captured_at,
                notes=entry.notes,
            )
        )

    store.upsert_wallet_registry_entries(updated_entries)
    return updated_entries


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


def _eligible_market_ids(
    trades: list[WalletTrade],
    max_trade_age_seconds: int,
) -> set[str]:
    return {
        trade.market_id
        for trade in trades
        if trade.age_seconds <= max_trade_age_seconds and trade.market_id
    }


def _exceeds_live_cluster_limit(
    wallet: str,
    selected_live_wallets: list[str],
    eligible_market_ids_by_wallet: dict[str, set[str]],
    max_cluster_overlap_ratio: float,
    max_cluster_members_in_live_tiers: int,
) -> bool:
    if max_cluster_members_in_live_tiers <= 0:
        return False

    candidate_market_ids = eligible_market_ids_by_wallet.get(wallet, set())
    overlapping_selected_count = sum(
        1
        for selected_wallet in selected_live_wallets
        if _market_overlap_ratio(
            candidate_market_ids,
            eligible_market_ids_by_wallet.get(selected_wallet, set()),
        )
        >= max_cluster_overlap_ratio
    )
    return overlapping_selected_count >= max_cluster_members_in_live_tiers


def _market_overlap_ratio(
    market_ids_a: set[str],
    market_ids_b: set[str],
) -> float:
    if not market_ids_a or not market_ids_b:
        return 0.0
    overlap_count = len(market_ids_a & market_ids_b)
    baseline_count = min(len(market_ids_a), len(market_ids_b))
    if baseline_count < 2:
        return 0.0
    return overlap_count / baseline_count


def _oldest_membership_age_hours(
    memberships: list[BasketMembership],
    captured_at: datetime,
) -> float | None:
    if not memberships:
        return None
    oldest_joined_at = min(membership.joined_at for membership in memberships)
    age_hours = (captured_at - oldest_joined_at).total_seconds() / 3_600
    return round(age_hours, 1)


def _live_roster_requires_rebalance(
    live_roster: dict[str, dict[str, object]],
) -> bool:
    for roster_entry in live_roster.values():
        if not isinstance(roster_entry, dict):
            continue
        refresh_reasons = roster_entry.get("refresh_reasons", [])
        if isinstance(refresh_reasons, list) and refresh_reasons:
            return True
    return False


def _registry_status_from_freshness(
    config: AppConfig,
    entry: WalletRegistryEntry,
    freshest_age_seconds: int | None,
    eligible_trade_count: int,
    captured_at: datetime,
) -> str:
    if freshest_age_seconds is None:
        reference_time = entry.last_seen_trade_at or entry.first_seen_at
        freshest_age_seconds = int((captured_at - reference_time).total_seconds())

    if freshest_age_seconds > config.wallet_registry.retire_after_days * 86_400:
        return "retired"
    if freshest_age_seconds > config.wallet_registry.suspend_after_hours * 3_600:
        return "suspended"
    if freshest_age_seconds > config.wallet_registry.stale_after_hours * 3_600:
        return "stale"

    probation_age_days = (captured_at - entry.first_seen_at).total_seconds() / 86_400
    if (
        probation_age_days < config.wallet_registry.min_probation_days
        or eligible_trade_count < config.wallet_registry.min_eligible_trades_for_approval
    ):
        return "probation"
    return "active"


def _status_allowed_for_tier(
    status: str,
    tier: str,
    *,
    backup_is_live: bool = False,
) -> bool:
    normalized_status = str(status).strip().lower() or "active"
    if tier in {"core", "rotating"}:
        return normalized_status == "active"
    if tier == "backup":
        if backup_is_live:
            return normalized_status in {"active", "stale"}
        return normalized_status in {"active", "probation", "stale"}
    if tier == "explorer":
        return normalized_status in {"active", "probation", "stale"}
    return False

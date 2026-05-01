from datetime import UTC, datetime
from pathlib import Path
from dataclasses import replace

import pytest

from predictcel.config import load_config
from predictcel.models import (
    BasketAssignment,
    BasketManagerAction,
    BasketMembership,
    WalletDiscoveryCandidate,
    WalletRegistryEntry,
    WalletTopicProfile,
    WalletTrade,
)
from predictcel.wallet_registry import (
    apply_basket_manager_actions_to_memberships,
    build_live_basket_roster,
    compute_basket_health_from_static_memberships,
    ingest_wallet_discovery_inputs,
    recommend_basket_promotions,
    rebalance_memberships_from_live_roster,
    refresh_registry_entries_from_trades,
    seed_memberships_from_config,
    seed_registry_from_config,
)


class FakeStore:
    def __init__(self) -> None:
        self.registry_entries = []
        self.memberships = []

    def upsert_wallet_registry_entries(self, entries) -> None:
        self.registry_entries = list(entries)

    def upsert_basket_memberships(self, memberships) -> None:
        self.memberships = list(memberships)

    def load_basket_memberships(self, topic: str | None = None):
        if topic is None:
            return list(self.memberships)
        return [
            membership for membership in self.memberships if membership.topic == topic
        ]

    def load_wallet_registry_entries(self):
        return list(self.registry_entries)


def test_seed_registry_and_memberships_from_static_baskets() -> None:
    config = load_config(Path("config/predictcel.example.json"))
    store = FakeStore()
    captured_at = datetime(2026, 1, 1, tzinfo=UTC)
    expected_wallet_count = len(
        {
            wallet
            for basket in config.baskets
            for wallet in basket.wallets
            if str(wallet).strip()
        }
    )
    expected_membership_count = sum(
        1
        for basket in config.baskets
        for wallet in basket.wallets
        if str(wallet).strip()
    )

    entries = seed_registry_from_config(config, store, captured_at=captured_at)
    memberships = seed_memberships_from_config(config, store, captured_at=captured_at)

    assert len(entries) == expected_wallet_count
    assert len(store.registry_entries) == expected_wallet_count
    assert len(memberships) == expected_membership_count
    assert all(entry.first_seen_at == captured_at for entry in entries)
    assert all(membership.tier == "core" for membership in memberships)


def test_compute_basket_health_from_static_memberships() -> None:
    config = load_config(Path("config/predictcel.example.json"))
    memberships = [
        BasketMembership(
            topic="geopolitics",
            wallet="w1",
            tier="core",
            rank=1,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w2",
            tier="core",
            rank=2,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w3",
            tier="rotating",
            rank=3,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
    ]
    trades = [
        WalletTrade(
            wallet="w1",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.5,
            size_usd=10.0,
            age_seconds=3600,
        ),
        WalletTrade(
            wallet="w2",
            topic="geopolitics",
            market_id="m2",
            side="NO",
            price=0.4,
            size_usd=15.0,
            age_seconds=172800,
        ),
        WalletTrade(
            wallet="w3",
            topic="geopolitics",
            market_id="m3",
            side="YES",
            price=0.6,
            size_usd=12.0,
            age_seconds=691200,
        ),
    ]

    health = compute_basket_health_from_static_memberships(
        config,
        memberships,
        trades,
        captured_at=datetime(2026, 1, 2, tzinfo=UTC),
    )

    assert len(health) == 1
    assert health[0].topic == "geopolitics"
    assert health[0].tracked_wallet_count == 3
    assert health[0].fresh_core_wallets_24h == 1
    assert health[0].fresh_active_wallets_7d == 2
    assert health[0].active_eligible_wallet_count == 1
    assert health[0].eligible_trades_7d == 2
    assert health[0].stale_ratio == pytest.approx(1 / 3, rel=1e-3)
    assert health[0].clustered_ratio == 0.0
    assert health[0].health_state == "thin"


def test_compute_basket_health_marks_stable_active_topic_as_healthy() -> None:
    base_config = load_config(Path("config/predictcel.example.json"))
    config = replace(
        base_config,
        filters=replace(base_config.filters, max_trade_age_seconds=604800),
    )
    memberships = [
        BasketMembership(
            topic="geopolitics",
            wallet="w1",
            tier="core",
            rank=1,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w2",
            tier="core",
            rank=2,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w3",
            tier="rotating",
            rank=3,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
    ]
    trades = [
        WalletTrade("w1", "geopolitics", "m1", "YES", 0.5, 10.0, 3600),
        WalletTrade("w2", "geopolitics", "m2", "NO", 0.4, 12.0, 172800),
        WalletTrade("w3", "geopolitics", "m3", "YES", 0.6, 14.0, 7200),
    ]

    health = compute_basket_health_from_static_memberships(
        config,
        memberships,
        trades,
        captured_at=datetime(2026, 1, 2, tzinfo=UTC),
    )

    assert health[0].fresh_core_wallets_24h == 1
    assert health[0].active_eligible_wallet_count == 3
    assert health[0].stale_ratio == 0.0
    assert health[0].health_state == "healthy"


def test_compute_basket_health_respects_registry_statuses() -> None:
    config = load_config(Path("config/predictcel.example.json"))
    memberships = [
        BasketMembership(
            topic="geopolitics",
            wallet="w1",
            tier="core",
            rank=1,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w2",
            tier="core",
            rank=2,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w3",
            tier="rotating",
            rank=3,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
    ]
    trades = [
        WalletTrade(
            wallet="w1",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.5,
            size_usd=20.0,
            age_seconds=3600,
        ),
        WalletTrade(
            wallet="w2",
            topic="geopolitics",
            market_id="m2",
            side="NO",
            price=0.4,
            size_usd=15.0,
            age_seconds=172800,
        ),
        WalletTrade(
            wallet="w3",
            topic="geopolitics",
            market_id="m3",
            side="YES",
            price=0.6,
            size_usd=12.0,
            age_seconds=691200,
        ),
    ]
    registry_entries = [
        WalletRegistryEntry(
            "w1",
            "static_basket",
            "config.baskets",
            1.0,
            "active",
            datetime(2026, 1, 1, tzinfo=UTC),
        ),
        WalletRegistryEntry(
            "w2",
            "static_basket",
            "config.baskets",
            1.0,
            "stale",
            datetime(2026, 1, 1, tzinfo=UTC),
        ),
        WalletRegistryEntry(
            "w3",
            "static_basket",
            "config.baskets",
            1.0,
            "retired",
            datetime(2026, 1, 1, tzinfo=UTC),
        ),
    ]

    health = compute_basket_health_from_static_memberships(
        config,
        memberships,
        trades,
        registry_entries=registry_entries,
        captured_at=datetime(2026, 1, 2, tzinfo=UTC),
    )

    assert len(health) == 1
    assert health[0].fresh_core_wallets_24h == 1
    assert health[0].fresh_active_wallets_7d == 1
    assert health[0].active_eligible_wallet_count == 1
    assert health[0].eligible_trades_7d == 2
    assert health[0].stale_ratio == pytest.approx(2 / 3, rel=1e-3)
    assert health[0].health_state == "stale"


def test_build_live_basket_roster_ranks_recent_wallets_into_live_slots() -> None:
    config = load_config(Path("config/predictcel.example.json"))
    captured_at = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    config = replace(
        config,
        basket_controller=replace(
            config.basket_controller,
            tracked_basket_target=4,
            core_slots=2,
            rotating_slots=1,
            backup_slots=1,
            explorer_slots=0,
            force_refresh_if_fresh_core_below=1,
            min_active_eligible_wallets=3,
        ),
    )
    memberships = [
        BasketMembership(
            topic="geopolitics",
            wallet="w1",
            tier="core",
            rank=1,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w2",
            tier="rotating",
            rank=2,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w3",
            tier="backup",
            rank=3,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w4",
            tier="explorer",
            rank=4,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
    ]
    trades = [
        WalletTrade("w2", "geopolitics", "m1", "YES", 0.5, 20.0, 700),
        WalletTrade("w2", "geopolitics", "m2", "NO", 0.4, 15.0, 1_400),
        WalletTrade("w3", "geopolitics", "m3", "YES", 0.6, 12.0, 300),
        WalletTrade("w4", "geopolitics", "m4", "YES", 0.6, 12.0, 86_000),
    ]

    roster = build_live_basket_roster(
        config, memberships, trades, captured_at=captured_at
    )

    assert roster["geopolitics"]["selected_wallets"] == {
        "core": ["w3", "w2"],
        "rotating": ["w4"],
        "backup": ["w1"],
        "explorer": [],
    }
    assert roster["geopolitics"]["fresh_core_wallet_count"] == 2
    assert roster["geopolitics"]["live_eligible_wallet_count"] == 3
    assert roster["geopolitics"]["needs_refresh"] is False
    assert roster["geopolitics"]["refresh_reasons"] == []


def test_build_live_basket_roster_uses_trust_seed_as_quality_tiebreaker() -> None:
    config = load_config(Path("config/predictcel.example.json"))
    captured_at = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    config = replace(
        config,
        basket_controller=replace(
            config.basket_controller,
            tracked_basket_target=2,
            core_slots=1,
            rotating_slots=1,
            backup_slots=0,
            explorer_slots=0,
            force_refresh_if_fresh_core_below=1,
            min_active_eligible_wallets=2,
        ),
    )
    memberships = [
        BasketMembership(
            topic="geopolitics",
            wallet="w_low",
            tier="core",
            rank=1,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w_high",
            tier="core",
            rank=2,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
    ]
    registry_entries = [
        WalletRegistryEntry(
            "w_low",
            "wallet_discovery",
            "polymarket_data_api",
            0.55,
            "active",
            captured_at,
        ),
        WalletRegistryEntry(
            "w_high",
            "wallet_discovery",
            "polymarket_data_api",
            0.85,
            "active",
            captured_at,
        ),
    ]
    trades = [
        WalletTrade("w_low", "geopolitics", "m1", "YES", 0.5, 20.0, 300),
        WalletTrade("w_high", "geopolitics", "m2", "YES", 0.5, 20.0, 300),
    ]

    roster = build_live_basket_roster(
        config,
        memberships,
        trades,
        registry_entries=registry_entries,
        captured_at=captured_at,
    )

    assert roster["geopolitics"]["selected_wallets"] == {
        "core": ["w_high"],
        "rotating": ["w_low"],
        "backup": [],
        "explorer": [],
    }
    decisions = {
        decision["wallet"]: decision
        for decision in roster["geopolitics"]["wallet_decisions"]
    }
    assert decisions["w_high"]["trust_seed"] == 0.85
    assert decisions["w_low"]["trust_seed"] == 0.55


def test_build_live_basket_roster_demotes_low_trust_discovered_wallets_from_live_tiers() -> (
    None
):
    config = load_config(Path("config/predictcel.example.json"))
    captured_at = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    config = replace(
        config,
        wallet_discovery=replace(config.wallet_discovery, min_assignment_score=0.5),
        basket_controller=replace(
            config.basket_controller,
            tracked_basket_target=3,
            core_slots=1,
            rotating_slots=1,
            backup_slots=1,
            explorer_slots=1,
            force_refresh_if_fresh_core_below=1,
            min_active_eligible_wallets=2,
        ),
    )
    memberships = [
        BasketMembership(
            "geopolitics", "w_core", "core", 1, True, captured_at, None, "seeded", ""
        ),
        BasketMembership(
            "geopolitics",
            "w_low",
            "rotating",
            2,
            True,
            captured_at,
            None,
            "discovered",
            "",
        ),
        BasketMembership(
            "geopolitics",
            "w_high",
            "explorer",
            3,
            True,
            captured_at,
            None,
            "discovered",
            "",
        ),
    ]
    registry_entries = [
        WalletRegistryEntry(
            "w_core", "static_basket", "config.baskets", 1.0, "active", captured_at
        ),
        WalletRegistryEntry(
            "w_low",
            "wallet_discovery",
            "polymarket_data_api",
            0.45,
            "active",
            captured_at,
        ),
        WalletRegistryEntry(
            "w_high",
            "wallet_discovery",
            "polymarket_data_api",
            0.8,
            "active",
            captured_at,
        ),
    ]
    trades = [
        WalletTrade("w_core", "geopolitics", "m1", "YES", 0.5, 20.0, 120),
        WalletTrade("w_low", "geopolitics", "m2", "YES", 0.5, 20.0, 150),
        WalletTrade("w_high", "geopolitics", "m3", "YES", 0.5, 20.0, 180),
    ]

    roster = build_live_basket_roster(
        config,
        memberships,
        trades,
        registry_entries=registry_entries,
        captured_at=captured_at,
    )

    assert roster["geopolitics"]["selected_wallets"] == {
        "core": ["w_core"],
        "rotating": ["w_high"],
        "backup": ["w_low"],
        "explorer": [],
    }
    decisions = {
        decision["wallet"]: decision
        for decision in roster["geopolitics"]["wallet_decisions"]
    }
    assert "quality_ineligible_for_rotating" in decisions["w_low"]["decision_reasons"]


def test_build_live_basket_roster_flags_refresh_when_core_is_not_fresh_enough() -> None:
    config = load_config(Path("config/predictcel.example.json"))
    captured_at = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    config = replace(
        config,
        basket_controller=replace(
            config.basket_controller,
            tracked_basket_target=4,
            core_slots=2,
            rotating_slots=1,
            backup_slots=1,
            explorer_slots=0,
            force_refresh_if_fresh_core_below=2,
            min_active_eligible_wallets=3,
        ),
    )
    memberships = [
        BasketMembership(
            topic="geopolitics",
            wallet=wallet,
            tier="core",
            rank=index,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        )
        for index, wallet in enumerate(["w1", "w2", "w3", "w4"], start=1)
    ]
    trades = [
        WalletTrade("w1", "geopolitics", "m1", "YES", 0.5, 20.0, 1_200),
        WalletTrade("w2", "geopolitics", "m2", "NO", 0.4, 15.0, 90_000),
    ]

    roster = build_live_basket_roster(
        config, memberships, trades, captured_at=captured_at
    )

    assert roster["geopolitics"]["selected_wallets"]["core"] == ["w1", "w2"]
    assert roster["geopolitics"]["fresh_core_wallet_count"] == 1
    assert roster["geopolitics"]["live_eligible_wallet_count"] == 1
    assert roster["geopolitics"]["needs_refresh"] is True
    assert roster["geopolitics"]["refresh_reasons"] == [
        "fresh_core_below_threshold",
        "live_eligible_wallets_below_threshold",
    ]


def test_build_live_basket_roster_limits_high_overlap_wallets_across_live_tiers() -> (
    None
):
    config = load_config(Path("config/predictcel.example.json"))
    captured_at = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    config = replace(
        config,
        basket_controller=replace(
            config.basket_controller,
            core_slots=2,
            rotating_slots=1,
            backup_slots=0,
            explorer_slots=0,
        ),
        wallet_registry=replace(
            config.wallet_registry,
            max_cluster_overlap_ratio=0.8,
            max_cluster_members_in_live_tiers=1,
        ),
    )
    memberships = [
        BasketMembership(
            topic="geopolitics",
            wallet=wallet,
            tier="core",
            rank=index,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        )
        for index, wallet in enumerate(["w1", "w2", "w3"], start=1)
    ]
    trades = [
        WalletTrade("w1", "geopolitics", "m1", "YES", 0.51, 20.0, 120),
        WalletTrade("w1", "geopolitics", "m2", "YES", 0.52, 20.0, 180),
        WalletTrade("w1", "geopolitics", "m3", "YES", 0.53, 20.0, 240),
        WalletTrade("w2", "geopolitics", "m1", "YES", 0.51, 20.0, 60),
        WalletTrade("w2", "geopolitics", "m2", "YES", 0.52, 20.0, 90),
        WalletTrade("w2", "geopolitics", "m3", "YES", 0.53, 20.0, 150),
        WalletTrade("w3", "geopolitics", "m4", "YES", 0.54, 20.0, 30),
        WalletTrade("w3", "geopolitics", "m5", "YES", 0.55, 20.0, 45),
    ]

    roster = build_live_basket_roster(
        config, memberships, trades, captured_at=captured_at
    )

    live_wallets = (
        roster["geopolitics"]["selected_wallets"]["core"]
        + roster["geopolitics"]["selected_wallets"]["rotating"]
    )
    assert "w2" in live_wallets
    assert "w3" in live_wallets
    assert "w1" not in live_wallets


def test_build_live_basket_roster_keeps_low_overlap_wallets_together_in_live_tiers() -> (
    None
):
    config = load_config(Path("config/predictcel.example.json"))
    captured_at = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    config = replace(
        config,
        basket_controller=replace(
            config.basket_controller,
            core_slots=2,
            rotating_slots=1,
            backup_slots=0,
            explorer_slots=0,
        ),
        wallet_registry=replace(
            config.wallet_registry,
            max_cluster_overlap_ratio=0.8,
            max_cluster_members_in_live_tiers=1,
        ),
    )
    memberships = [
        BasketMembership(
            topic="geopolitics",
            wallet=wallet,
            tier="core",
            rank=index,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        )
        for index, wallet in enumerate(["w1", "w2", "w3"], start=1)
    ]
    trades = [
        WalletTrade("w1", "geopolitics", "m1", "YES", 0.51, 20.0, 60),
        WalletTrade("w1", "geopolitics", "m2", "YES", 0.52, 20.0, 120),
        WalletTrade("w2", "geopolitics", "m2", "YES", 0.52, 20.0, 90),
        WalletTrade("w2", "geopolitics", "m3", "YES", 0.53, 20.0, 150),
        WalletTrade("w3", "geopolitics", "m4", "YES", 0.54, 20.0, 30),
    ]

    roster = build_live_basket_roster(
        config, memberships, trades, captured_at=captured_at
    )

    live_wallets = (
        roster["geopolitics"]["selected_wallets"]["core"]
        + roster["geopolitics"]["selected_wallets"]["rotating"]
    )
    assert set(live_wallets) == {"w1", "w2", "w3"}


def test_build_live_basket_roster_reports_when_rotation_interval_is_due() -> None:
    config = load_config(Path("config/predictcel.example.json"))
    config = replace(
        config,
        basket_controller=replace(
            config.basket_controller,
            core_slots=1,
            rotating_slots=1,
            backup_slots=0,
            explorer_slots=0,
            rotation_interval_hours=24,
        ),
    )
    captured_at = datetime(2026, 1, 20, 12, 0, tzinfo=UTC)
    memberships = [
        BasketMembership(
            topic="geopolitics",
            wallet="w1",
            tier="core",
            rank=1,
            active=True,
            joined_at=datetime(2026, 1, 19, 18, 0, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w2",
            tier="rotating",
            rank=2,
            active=True,
            joined_at=datetime(2026, 1, 19, 6, 0, tzinfo=UTC),
            effective_until=None,
            promotion_reason="promoted",
            demotion_reason="",
        ),
    ]
    trades = [
        WalletTrade("w1", "geopolitics", "m1", "YES", 0.51, 20.0, 60),
        WalletTrade("w2", "geopolitics", "m2", "YES", 0.52, 20.0, 120),
    ]

    roster = build_live_basket_roster(
        config,
        memberships,
        trades,
        captured_at=captured_at,
    )

    assert roster["geopolitics"]["rotation_interval_hours"] == 24
    assert roster["geopolitics"]["oldest_rotating_wallet_age_hours"] == 30.0
    assert roster["geopolitics"]["rotation_due"] is True


def test_recommend_basket_promotions_flags_taxonomy_topic_ready_for_live_rollout() -> (
    None
):
    config = load_config(Path("config/predictcel.example.json"))
    config = replace(
        config,
        basket_promotion=replace(
            config.basket_promotion,
            min_tracked_wallets=5,
            min_fresh_active_wallets_7d=3,
            min_live_eligible_wallets=3,
            min_fresh_core_wallets_24h=2,
            min_eligible_trades_7d=5,
            max_stale_ratio=0.5,
        ),
        basket_controller=replace(
            config.basket_controller,
            tracked_basket_target=5,
            core_slots=2,
            rotating_slots=1,
            backup_slots=1,
            explorer_slots=1,
            min_active_eligible_wallets=3,
            force_refresh_if_fresh_core_below=1,
        ),
    )
    captured_at = datetime(2026, 1, 20, tzinfo=UTC)
    memberships = [
        BasketMembership(
            "health-fda",
            f"w{index}",
            tier,
            index,
            True,
            captured_at,
            None,
            "discovered",
            "",
        )
        for index, tier in enumerate(
            ["core", "core", "rotating", "backup", "explorer"],
            start=1,
        )
    ]
    trades = [
        WalletTrade("w1", "health-fda", "m1", "YES", 0.51, 20.0, 300),
        WalletTrade("w2", "health-fda", "m2", "YES", 0.52, 20.0, 600),
        WalletTrade("w3", "health-fda", "m3", "YES", 0.53, 20.0, 900),
        WalletTrade("w4", "health-fda", "m4", "YES", 0.54, 20.0, 1_200),
        WalletTrade("w5", "health-fda", "m5", "YES", 0.55, 20.0, 1_500),
    ]
    registry_entries = [
        WalletRegistryEntry(
            f"w{index}",
            "wallet_discovery",
            "curated_wallet_file",
            0.8,
            "active",
            captured_at,
        )
        for index in range(1, 6)
    ]

    recommendations = recommend_basket_promotions(
        config,
        memberships,
        trades,
        registry_entries=registry_entries,
        captured_at=captured_at,
    )

    health_fda = recommendations["health-fda"]
    assert health_fda.should_promote is True
    assert health_fda.recommended_wallets == ("w1", "w2", "w3")
    assert health_fda.missing_requirements == ()
    assert (
        health_fda.recommended_quorum_ratio
        == config.basket_controller.min_basket_participation_ratio
    )


def test_recommend_basket_promotions_reports_threshold_gaps_for_thin_topic() -> None:
    config = load_config(Path("config/predictcel.example.json"))
    config = replace(
        config,
        basket_promotion=replace(
            config.basket_promotion,
            min_tracked_wallets=4,
            min_fresh_active_wallets_7d=3,
            min_live_eligible_wallets=3,
            min_fresh_core_wallets_24h=2,
            min_eligible_trades_7d=4,
            max_stale_ratio=0.25,
        ),
        basket_controller=replace(
            config.basket_controller,
            tracked_basket_target=4,
            core_slots=1,
            rotating_slots=1,
            backup_slots=1,
            explorer_slots=1,
            min_active_eligible_wallets=3,
        ),
    )
    captured_at = datetime(2026, 1, 20, tzinfo=UTC)
    memberships = [
        BasketMembership(
            "ai", "w1", "core", 1, True, captured_at, None, "discovered", ""
        ),
        BasketMembership(
            "ai", "w2", "rotating", 2, True, captured_at, None, "discovered", ""
        ),
        BasketMembership(
            "ai", "w3", "explorer", 3, True, captured_at, None, "discovered", ""
        ),
    ]
    trades = [
        WalletTrade("w1", "ai", "m1", "YES", 0.51, 20.0, 300),
    ]
    registry_entries = [
        WalletRegistryEntry(
            "w1", "wallet_discovery", "curated_wallet_file", 0.8, "active", captured_at
        ),
        WalletRegistryEntry(
            "w2", "wallet_discovery", "curated_wallet_file", 0.8, "active", captured_at
        ),
        WalletRegistryEntry(
            "w3", "wallet_discovery", "curated_wallet_file", 0.8, "active", captured_at
        ),
    ]

    recommendations = recommend_basket_promotions(
        config,
        memberships,
        trades,
        registry_entries=registry_entries,
        captured_at=captured_at,
    )

    ai_topic = recommendations["ai"]
    assert ai_topic.should_promote is False
    assert "tracked_wallets_below_threshold" in ai_topic.missing_requirements
    assert "fresh_active_wallets_7d_below_threshold" in ai_topic.missing_requirements
    assert "live_eligible_wallets_below_threshold" in ai_topic.missing_requirements


def test_build_live_basket_roster_adds_refresh_reason_when_rotation_is_due() -> None:
    config = load_config(Path("config/predictcel.example.json"))
    config = replace(
        config,
        basket_controller=replace(
            config.basket_controller,
            core_slots=1,
            rotating_slots=1,
            backup_slots=0,
            explorer_slots=0,
            rotation_interval_hours=24,
            force_refresh_if_fresh_core_below=1,
            min_active_eligible_wallets=2,
        ),
    )
    captured_at = datetime(2026, 1, 20, 12, 0, tzinfo=UTC)
    memberships = [
        BasketMembership(
            topic="geopolitics",
            wallet="w1",
            tier="core",
            rank=1,
            active=True,
            joined_at=datetime(2026, 1, 20, 9, 0, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w2",
            tier="rotating",
            rank=2,
            active=True,
            joined_at=datetime(2026, 1, 19, 6, 0, tzinfo=UTC),
            effective_until=None,
            promotion_reason="promoted",
            demotion_reason="",
        ),
    ]
    trades = [
        WalletTrade("w1", "geopolitics", "m1", "YES", 0.51, 20.0, 60),
        WalletTrade("w2", "geopolitics", "m2", "YES", 0.52, 20.0, 120),
    ]

    roster = build_live_basket_roster(
        config,
        memberships,
        trades,
        captured_at=captured_at,
    )

    assert roster["geopolitics"]["needs_refresh"] is True
    assert roster["geopolitics"]["refresh_reasons"] == ["rotation_due"]


def test_build_live_basket_roster_excludes_probation_from_core_and_rotating() -> None:
    config = load_config(Path("config/predictcel.example.json"))
    config = replace(
        config,
        basket_controller=replace(
            config.basket_controller,
            core_slots=1,
            rotating_slots=1,
            backup_slots=1,
            explorer_slots=1,
            force_refresh_if_fresh_core_below=1,
            min_active_eligible_wallets=2,
        ),
    )
    captured_at = datetime(2026, 1, 20, 12, 0, tzinfo=UTC)
    memberships = [
        BasketMembership(
            "geopolitics",
            "w_probation",
            "core",
            1,
            True,
            captured_at,
            None,
            "discovered",
            "",
        ),
        BasketMembership(
            "geopolitics",
            "w_active_1",
            "core",
            2,
            True,
            captured_at,
            None,
            "seeded",
            "",
        ),
        BasketMembership(
            "geopolitics",
            "w_active_2",
            "rotating",
            3,
            True,
            captured_at,
            None,
            "seeded",
            "",
        ),
        BasketMembership(
            "geopolitics",
            "w_active_3",
            "backup",
            4,
            True,
            captured_at,
            None,
            "seeded",
            "",
        ),
    ]
    registry_entries = [
        WalletRegistryEntry(
            "w_probation",
            "wallet_discovery",
            "polymarket_data_api",
            0.8,
            "probation",
            captured_at,
        ),
        WalletRegistryEntry(
            "w_active_1", "static_basket", "config.baskets", 1.0, "active", captured_at
        ),
        WalletRegistryEntry(
            "w_active_2", "static_basket", "config.baskets", 1.0, "active", captured_at
        ),
        WalletRegistryEntry(
            "w_active_3", "static_basket", "config.baskets", 1.0, "active", captured_at
        ),
    ]
    trades = [
        WalletTrade("w_probation", "geopolitics", "m1", "YES", 0.58, 20.0, 60),
        WalletTrade("w_active_1", "geopolitics", "m2", "YES", 0.59, 20.0, 90),
        WalletTrade("w_active_2", "geopolitics", "m3", "YES", 0.60, 20.0, 120),
        WalletTrade("w_active_3", "geopolitics", "m4", "YES", 0.61, 20.0, 150),
    ]

    roster = build_live_basket_roster(
        config,
        memberships,
        trades,
        registry_entries=registry_entries,
        captured_at=captured_at,
    )

    assert roster["geopolitics"]["selected_wallets"] == {
        "core": ["w_active_1"],
        "rotating": ["w_active_2"],
        "backup": ["w_probation"],
        "explorer": ["w_active_3"],
    }
    probation_decision = next(
        decision
        for decision in roster["geopolitics"]["wallet_decisions"]
        if decision["wallet"] == "w_probation"
    )
    assert probation_decision == {
        "wallet": "w_probation",
        "membership_tier": "core",
        "membership_rank": 1,
        "registry_status": "probation",
        "trust_seed": 0.8,
        "selected": True,
        "selected_tier": "backup",
        "eligible_trade_count": 1,
        "eligible_market_count": 1,
        "decision_reasons": [
            "status_ineligible_for_core",
            "status_ineligible_for_rotating",
        ],
    }


def test_build_live_basket_roster_excludes_probation_from_backup_when_backup_is_live() -> (
    None
):
    config = load_config(Path("config/predictcel.example.json"))
    config = replace(
        config,
        basket_controller=replace(
            config.basket_controller,
            core_slots=1,
            rotating_slots=1,
            backup_slots=1,
            explorer_slots=1,
            allow_backup_in_live_consensus=True,
            force_refresh_if_fresh_core_below=1,
            min_active_eligible_wallets=3,
        ),
    )
    captured_at = datetime(2026, 1, 20, 12, 0, tzinfo=UTC)
    memberships = [
        BasketMembership(
            "geopolitics",
            "w_active_1",
            "core",
            1,
            True,
            captured_at,
            None,
            "seeded",
            "",
        ),
        BasketMembership(
            "geopolitics",
            "w_active_2",
            "rotating",
            2,
            True,
            captured_at,
            None,
            "seeded",
            "",
        ),
        BasketMembership(
            "geopolitics",
            "w_probation",
            "backup",
            3,
            True,
            captured_at,
            None,
            "discovered",
            "",
        ),
        BasketMembership(
            "geopolitics",
            "w_active_3",
            "explorer",
            4,
            True,
            captured_at,
            None,
            "seeded",
            "",
        ),
    ]
    registry_entries = [
        WalletRegistryEntry(
            "w_active_1", "static_basket", "config.baskets", 1.0, "active", captured_at
        ),
        WalletRegistryEntry(
            "w_active_2", "static_basket", "config.baskets", 1.0, "active", captured_at
        ),
        WalletRegistryEntry(
            "w_probation",
            "wallet_discovery",
            "polymarket_data_api",
            0.8,
            "probation",
            captured_at,
        ),
        WalletRegistryEntry(
            "w_active_3", "static_basket", "config.baskets", 1.0, "active", captured_at
        ),
    ]
    trades = [
        WalletTrade("w_active_1", "geopolitics", "m1", "YES", 0.58, 20.0, 60),
        WalletTrade("w_active_2", "geopolitics", "m2", "YES", 0.59, 20.0, 90),
        WalletTrade("w_probation", "geopolitics", "m3", "YES", 0.60, 20.0, 120),
        WalletTrade("w_active_3", "geopolitics", "m4", "YES", 0.61, 20.0, 150),
    ]

    roster = build_live_basket_roster(
        config,
        memberships,
        trades,
        registry_entries=registry_entries,
        captured_at=captured_at,
    )

    assert roster["geopolitics"]["selected_wallets"] == {
        "core": ["w_active_1"],
        "rotating": ["w_active_2"],
        "backup": ["w_active_3"],
        "explorer": ["w_probation"],
    }
    probation_decision = next(
        decision
        for decision in roster["geopolitics"]["wallet_decisions"]
        if decision["wallet"] == "w_probation"
    )
    assert probation_decision == {
        "wallet": "w_probation",
        "membership_tier": "backup",
        "membership_rank": 3,
        "registry_status": "probation",
        "trust_seed": 0.8,
        "selected": True,
        "selected_tier": "explorer",
        "eligible_trade_count": 1,
        "eligible_market_count": 1,
        "decision_reasons": [
            "slots_filled_for_core",
            "slots_filled_for_rotating",
            "status_ineligible_for_backup",
        ],
    }


def test_build_live_basket_roster_reports_cluster_and_slot_exclusions() -> None:
    config = load_config(Path("config/predictcel.example.json"))
    captured_at = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    config = replace(
        config,
        basket_controller=replace(
            config.basket_controller,
            core_slots=1,
            rotating_slots=1,
            backup_slots=1,
            explorer_slots=0,
        ),
        wallet_registry=replace(
            config.wallet_registry,
            max_cluster_overlap_ratio=0.8,
            max_cluster_members_in_live_tiers=1,
        ),
    )
    memberships = [
        BasketMembership(
            topic="geopolitics",
            wallet=wallet,
            tier="core",
            rank=index,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        )
        for index, wallet in enumerate(["w1", "w2", "w3", "w4"], start=1)
    ]
    trades = [
        WalletTrade("w2", "geopolitics", "m1", "YES", 0.51, 20.0, 30),
        WalletTrade("w2", "geopolitics", "m2", "YES", 0.52, 20.0, 45),
        WalletTrade("w2", "geopolitics", "m3", "YES", 0.53, 20.0, 75),
        WalletTrade("w1", "geopolitics", "m1", "YES", 0.51, 20.0, 60),
        WalletTrade("w1", "geopolitics", "m2", "YES", 0.52, 20.0, 90),
        WalletTrade("w1", "geopolitics", "m3", "YES", 0.53, 20.0, 120),
        WalletTrade("w3", "geopolitics", "m4", "YES", 0.54, 20.0, 150),
        WalletTrade("w4", "geopolitics", "m5", "YES", 0.55, 20.0, 180),
    ]

    roster = build_live_basket_roster(
        config, memberships, trades, captured_at=captured_at
    )

    decisions = {
        decision["wallet"]: decision
        for decision in roster["geopolitics"]["wallet_decisions"]
    }
    assert decisions["w2"]["selected_tier"] == "core"
    assert decisions["w1"]["selected_tier"] == "backup"
    assert decisions["w1"]["decision_reasons"] == [
        "slots_filled_for_core",
        "cluster_overlap_limit_for_rotating",
    ]
    assert decisions["w4"]["selected"] is False
    assert decisions["w4"]["decision_reasons"] == [
        "slots_filled_for_core",
        "slots_filled_for_rotating",
        "slots_filled_for_backup",
    ]


def test_rebalance_memberships_from_live_roster_rewrites_tiers_and_deactivates_overflow() -> (
    None
):
    config = load_config(Path("config/predictcel.example.json"))
    config = replace(
        config,
        basket_controller=replace(
            config.basket_controller,
            tracked_basket_target=4,
            core_slots=2,
            rotating_slots=1,
            backup_slots=1,
            explorer_slots=0,
        ),
    )
    store = FakeStore()
    captured_at = datetime(2026, 1, 2, tzinfo=UTC)
    store.memberships = [
        BasketMembership(
            topic="geopolitics",
            wallet="w1",
            tier="core",
            rank=1,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w2",
            tier="core",
            rank=2,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w3",
            tier="rotating",
            rank=3,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w4",
            tier="backup",
            rank=4,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w5",
            tier="explorer",
            rank=5,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
    ]
    trades = [
        WalletTrade("w5", "geopolitics", "m1", "YES", 0.6, 15.0, 60),
        WalletTrade("w5", "geopolitics", "m2", "YES", 0.59, 15.0, 120),
        WalletTrade("w4", "geopolitics", "m3", "YES", 0.58, 15.0, 180),
        WalletTrade("w3", "geopolitics", "m4", "YES", 0.57, 15.0, 240),
        WalletTrade("w2", "geopolitics", "m5", "YES", 0.56, 15.0, 300),
    ]

    updated = rebalance_memberships_from_live_roster(
        config,
        store,
        trades,
        captured_at=captured_at,
    )

    assert [membership.wallet for membership in updated if membership.active] == [
        "w5",
        "w4",
        "w3",
        "w2",
    ]
    assert [
        (membership.wallet, membership.tier, membership.rank)
        for membership in updated[:4]
    ] == [
        ("w5", "core", 1),
        ("w4", "core", 2),
        ("w3", "rotating", 3),
        ("w2", "backup", 4),
    ]
    overflow = next(membership for membership in updated if membership.wallet == "w1")
    assert overflow.active is False
    assert overflow.effective_until == captured_at
    assert overflow.demotion_reason == "dropped from live roster rebalance"


def test_rebalance_memberships_from_live_roster_skips_churn_until_rotation_due() -> (
    None
):
    config = load_config(Path("config/predictcel.example.json"))
    config = replace(
        config,
        basket_controller=replace(
            config.basket_controller,
            tracked_basket_target=3,
            core_slots=1,
            rotating_slots=1,
            backup_slots=1,
            explorer_slots=0,
            force_refresh_if_fresh_core_below=1,
            min_active_eligible_wallets=2,
            rotation_interval_hours=24,
        ),
    )
    store = FakeStore()
    captured_at = datetime(2026, 1, 2, 12, 0, tzinfo=UTC)
    recent_joined_at = datetime(2026, 1, 2, 6, 0, tzinfo=UTC)
    store.registry_entries = [
        WalletRegistryEntry(
            wallet, "static_basket", "config.baskets", 1.0, "active", recent_joined_at
        )
        for wallet in ["w1", "w2", "w3"]
    ]
    store.memberships = [
        BasketMembership(
            "geopolitics", "w1", "core", 1, True, recent_joined_at, None, "seeded", ""
        ),
        BasketMembership(
            "geopolitics",
            "w2",
            "rotating",
            2,
            True,
            recent_joined_at,
            None,
            "seeded",
            "",
        ),
        BasketMembership(
            "geopolitics", "w3", "backup", 3, True, recent_joined_at, None, "seeded", ""
        ),
    ]
    trades = [
        WalletTrade("w3", "geopolitics", "m1", "YES", 0.6, 15.0, 60),
        WalletTrade("w3", "geopolitics", "m2", "YES", 0.59, 15.0, 120),
        WalletTrade("w2", "geopolitics", "m3", "YES", 0.58, 15.0, 180),
        WalletTrade("w1", "geopolitics", "m4", "YES", 0.57, 15.0, 240),
    ]

    updated = rebalance_memberships_from_live_roster(
        config,
        store,
        trades,
        captured_at=captured_at,
    )

    assert [
        (membership.wallet, membership.tier, membership.rank, membership.active)
        for membership in updated
    ] == [
        ("w1", "core", 1, True),
        ("w2", "rotating", 2, True),
        ("w3", "backup", 3, True),
    ]


def test_refresh_registry_entries_from_trades_updates_status_from_freshness() -> None:
    config = load_config(Path("config/predictcel.example.json"))
    config = replace(
        config,
        filters=replace(config.filters, max_trade_age_seconds=3600),
        wallet_discovery=replace(config.wallet_discovery, min_assignment_score=0.5),
        wallet_registry=replace(
            config.wallet_registry,
            min_probation_days=7,
            min_eligible_trades_for_approval=2,
            stale_after_hours=72,
            suspend_after_hours=168,
            retire_after_days=30,
        ),
    )
    captured_at = datetime(2026, 1, 20, tzinfo=UTC)
    store = FakeStore()
    store.registry_entries = [
        WalletRegistryEntry(
            "w_active",
            "static_basket",
            "config.baskets",
            1.0,
            "active",
            datetime(2026, 1, 1, tzinfo=UTC),
        ),
        WalletRegistryEntry(
            "w_probation_age",
            "static_basket",
            "config.baskets",
            1.0,
            "active",
            datetime(2026, 1, 18, tzinfo=UTC),
        ),
        WalletRegistryEntry(
            "w_probation_count",
            "static_basket",
            "config.baskets",
            1.0,
            "active",
            datetime(2026, 1, 1, tzinfo=UTC),
        ),
        WalletRegistryEntry(
            "w_probation_quality",
            "wallet_discovery",
            "polymarket_data_api",
            0.4,
            "active",
            datetime(2026, 1, 1, tzinfo=UTC),
        ),
        WalletRegistryEntry(
            "w_discovered_active",
            "wallet_discovery",
            "polymarket_data_api",
            0.7,
            "active",
            datetime(2026, 1, 1, tzinfo=UTC),
        ),
        WalletRegistryEntry(
            "w_stale",
            "static_basket",
            "config.baskets",
            1.0,
            "active",
            datetime(2026, 1, 1, tzinfo=UTC),
        ),
        WalletRegistryEntry(
            "w_suspended",
            "static_basket",
            "config.baskets",
            1.0,
            "active",
            datetime(2026, 1, 1, tzinfo=UTC),
        ),
        WalletRegistryEntry(
            "w_retired",
            "static_basket",
            "config.baskets",
            1.0,
            "active",
            datetime(2026, 1, 1, tzinfo=UTC),
        ),
    ]
    trades = [
        WalletTrade("w_active", "geopolitics", "m1", "YES", 0.6, 15.0, 300),
        WalletTrade("w_active", "geopolitics", "m2", "YES", 0.6, 15.0, 900),
        WalletTrade("w_probation_age", "geopolitics", "m3", "YES", 0.6, 15.0, 600),
        WalletTrade("w_probation_age", "geopolitics", "m4", "YES", 0.6, 15.0, 1200),
        WalletTrade("w_probation_count", "geopolitics", "m5", "YES", 0.6, 15.0, 600),
        WalletTrade("w_probation_quality", "geopolitics", "m5a", "YES", 0.6, 15.0, 300),
        WalletTrade("w_probation_quality", "geopolitics", "m5b", "YES", 0.6, 15.0, 900),
        WalletTrade("w_discovered_active", "geopolitics", "m5c", "YES", 0.6, 15.0, 300),
        WalletTrade("w_discovered_active", "geopolitics", "m5d", "YES", 0.6, 15.0, 900),
        WalletTrade("w_stale", "geopolitics", "m6", "YES", 0.6, 15.0, 80 * 3600),
        WalletTrade("w_suspended", "geopolitics", "m7", "YES", 0.6, 15.0, 200 * 3600),
        WalletTrade("w_retired", "geopolitics", "m8", "YES", 0.6, 15.0, 40 * 24 * 3600),
    ]

    updated = refresh_registry_entries_from_trades(
        config,
        store,
        trades,
        captured_at=captured_at,
    )

    statuses = {entry.wallet: entry.status for entry in updated}
    assert statuses == {
        "w_active": "active",
        "w_probation_age": "active",
        "w_probation_count": "active",
        "w_probation_quality": "probation",
        "w_discovered_active": "active",
        "w_stale": "stale",
        "w_suspended": "suspended",
        "w_retired": "retired",
    }


def test_ingest_wallet_discovery_inputs_adds_registry_entries_and_explorer_memberships() -> (
    None
):
    config = load_config(Path("config/predictcel.example.json"))
    captured_at = datetime(2026, 1, 20, tzinfo=UTC)
    store = FakeStore()
    store.registry_entries = [
        WalletRegistryEntry(
            wallet="w1",
            source_type="static_basket",
            source_ref="config.baskets",
            trust_seed=1.0,
            status="active",
            first_seen_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
    ]
    store.memberships = [
        BasketMembership(
            topic="geopolitics",
            wallet="w1",
            tier="core",
            rank=1,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        )
    ]
    candidates = [
        WalletDiscoveryCandidate(
            wallet_address="w_new",
            source="polymarket_data_api",
            total_trades=42,
            recent_trades=18,
            avg_trade_size_usd=75.0,
            topic_profile=WalletTopicProfile(
                topic_affinities={"geopolitics": 0.9},
                primary_topic="geopolitics",
                specialization_score=0.82,
            ),
            score=0.77,
            confidence="HIGH",
            rejected_reasons=[],
        )
    ]
    assignments = [
        BasketAssignment(
            wallet_address="w_new",
            primary_topic="geopolitics",
            recommended_baskets=["geopolitics"],
            topic_affinities={"geopolitics": 0.9},
            overall_score=0.77,
            confidence="HIGH",
            reasons=["strong specialization"],
        )
    ]

    entries, memberships = ingest_wallet_discovery_inputs(
        config,
        store,
        candidates,
        assignments,
        captured_at=captured_at,
    )

    assert {entry.wallet for entry in entries} == {"w1", "w_new"}
    new_entry = next(entry for entry in entries if entry.wallet == "w_new")
    assert new_entry.source_type == "wallet_discovery"
    assert new_entry.source_ref == "polymarket_data_api"
    assert new_entry.status == "probation"
    assert new_entry.first_seen_at == captured_at
    assert new_entry.last_scored_at == captured_at

    new_membership = next(
        membership
        for membership in memberships
        if membership.topic == "geopolitics" and membership.wallet == "w_new"
    )
    assert new_membership.tier == "explorer"
    assert new_membership.rank == 2
    assert new_membership.active is True
    assert new_membership.joined_at == captured_at


def test_apply_basket_manager_actions_to_memberships_adds_and_deactivates_registry_memberships() -> (
    None
):
    config = load_config(Path("config/predictcel.example.json"))
    captured_at = datetime(2026, 1, 20, tzinfo=UTC)
    store = FakeStore()
    store.registry_entries = [
        WalletRegistryEntry(
            wallet="w_existing",
            source_type="static_basket",
            source_ref="config.baskets",
            trust_seed=1.0,
            status="active",
            first_seen_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
    ]
    store.memberships = [
        BasketMembership(
            topic="geopolitics",
            wallet="w_existing",
            tier="core",
            rank=1,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        )
    ]
    actions = [
        BasketManagerAction(
            "add",
            "geopolitics",
            "w_new",
            0.8,
            "HIGH",
            "auto-update eligible recommendation",
        ),
        BasketManagerAction(
            "suspend",
            "geopolitics",
            "w_existing",
            0.3,
            "LOW",
            "existing wallet confidence fell to LOW",
        ),
        BasketManagerAction(
            "observe", "geopolitics", "w_observe", 0.2, "LOW", "observe only"
        ),
    ]

    memberships, diagnostics = apply_basket_manager_actions_to_memberships(
        config,
        store,
        actions,
        captured_at=captured_at,
    )

    assert diagnostics == {
        "actions_applied": 2,
        "action_counts": {"add": 1, "suspend": 1},
        "advisory_action_counts": {"observe": 1},
        "ignored_action_counts": {},
        "memberships_activated": 1,
        "memberships_deactivated": 1,
    }
    assert {entry.wallet for entry in store.registry_entries} == {"w_existing", "w_new"}
    assert {
        (membership.topic, membership.wallet): (
            membership.tier,
            membership.active,
            membership.demotion_reason,
        )
        for membership in memberships
    } == {
        ("geopolitics", "w_existing"): (
            "core",
            False,
            "existing wallet confidence fell to LOW",
        ),
        ("geopolitics", "w_new"): (
            "explorer",
            True,
            "",
        ),
    }
    new_membership = next(
        membership
        for membership in memberships
        if membership.topic == "geopolitics" and membership.wallet == "w_new"
    )
    assert new_membership.promotion_reason == "auto-update eligible recommendation"


def test_rebalance_memberships_from_live_roster_demotes_stale_wallets_out_of_live_tiers() -> (
    None
):
    config = load_config(Path("config/predictcel.example.json"))
    config = replace(
        config,
        filters=replace(config.filters, max_trade_age_seconds=3600),
        wallet_registry=replace(
            config.wallet_registry,
            min_probation_days=1,
            min_eligible_trades_for_approval=1,
            stale_after_hours=72,
            suspend_after_hours=168,
            retire_after_days=30,
        ),
        basket_controller=replace(
            config.basket_controller,
            tracked_basket_target=4,
            core_slots=2,
            rotating_slots=1,
            backup_slots=1,
            explorer_slots=0,
        ),
    )
    store = FakeStore()
    captured_at = datetime(2026, 1, 20, tzinfo=UTC)
    store.registry_entries = [
        WalletRegistryEntry(
            "w1",
            "static_basket",
            "config.baskets",
            1.0,
            "active",
            datetime(2026, 1, 1, tzinfo=UTC),
        ),
        WalletRegistryEntry(
            "w2",
            "static_basket",
            "config.baskets",
            1.0,
            "active",
            datetime(2026, 1, 1, tzinfo=UTC),
        ),
        WalletRegistryEntry(
            "w3",
            "static_basket",
            "config.baskets",
            1.0,
            "active",
            datetime(2026, 1, 1, tzinfo=UTC),
        ),
        WalletRegistryEntry(
            "w4",
            "static_basket",
            "config.baskets",
            1.0,
            "active",
            datetime(2026, 1, 1, tzinfo=UTC),
        ),
    ]
    store.memberships = [
        BasketMembership(
            "geopolitics",
            "w1",
            "core",
            1,
            True,
            datetime(2026, 1, 1, tzinfo=UTC),
            None,
            "seeded",
            "",
        ),
        BasketMembership(
            "geopolitics",
            "w2",
            "core",
            2,
            True,
            datetime(2026, 1, 1, tzinfo=UTC),
            None,
            "seeded",
            "",
        ),
        BasketMembership(
            "geopolitics",
            "w3",
            "rotating",
            3,
            True,
            datetime(2026, 1, 1, tzinfo=UTC),
            None,
            "seeded",
            "",
        ),
        BasketMembership(
            "geopolitics",
            "w4",
            "backup",
            4,
            True,
            datetime(2026, 1, 1, tzinfo=UTC),
            None,
            "seeded",
            "",
        ),
    ]
    trades = [
        WalletTrade("w4", "geopolitics", "m1", "YES", 0.6, 15.0, 60),
        WalletTrade("w3", "geopolitics", "m2", "YES", 0.59, 15.0, 120),
        WalletTrade("w2", "geopolitics", "m3", "YES", 0.58, 15.0, 180),
        WalletTrade("w1", "geopolitics", "m4", "YES", 0.57, 15.0, 80 * 3600),
    ]

    refresh_registry_entries_from_trades(config, store, trades, captured_at=captured_at)
    updated = rebalance_memberships_from_live_roster(
        config,
        store,
        trades,
        captured_at=captured_at,
    )

    assert [
        (membership.wallet, membership.tier, membership.rank, membership.active)
        for membership in updated
    ] == [
        ("w4", "core", 1, True),
        ("w3", "core", 2, True),
        ("w2", "rotating", 3, True),
        ("w1", "backup", 4, True),
    ]

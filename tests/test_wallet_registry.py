from datetime import UTC, datetime
from pathlib import Path
from dataclasses import replace

import pytest

from predictcel.config import load_config
from predictcel.models import BasketMembership, WalletTrade
from predictcel.wallet_registry import (
    build_live_basket_roster,
    compute_basket_health_from_static_memberships,
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


def test_seed_registry_and_memberships_from_static_baskets() -> None:
    config = load_config(Path("config/predictcel.example.json"))
    store = FakeStore()
    captured_at = datetime(2026, 1, 1, tzinfo=UTC)

    entries = seed_registry_from_config(config, store, captured_at=captured_at)
    memberships = seed_memberships_from_config(config, store, captured_at=captured_at)

    assert len(entries) == 9
    assert len(store.registry_entries) == 9
    assert len(memberships) == 9
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


def test_build_live_basket_roster_ranks_recent_wallets_into_live_slots() -> None:
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

    roster = build_live_basket_roster(config, memberships, trades)

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


def test_build_live_basket_roster_flags_refresh_when_core_is_not_fresh_enough() -> None:
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

    roster = build_live_basket_roster(config, memberships, trades)

    assert roster["geopolitics"]["selected_wallets"]["core"] == ["w1", "w2"]
    assert roster["geopolitics"]["fresh_core_wallet_count"] == 1
    assert roster["geopolitics"]["live_eligible_wallet_count"] == 1
    assert roster["geopolitics"]["needs_refresh"] is True
    assert roster["geopolitics"]["refresh_reasons"] == [
        "fresh_core_below_threshold",
        "live_eligible_wallets_below_threshold",
    ]

import sys
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from predictcel.config import load_config
from predictcel import discover_wallets
from predictcel.main import (
    _build_wallet_registry_summary,
    _compact_cycle_output,
    _creates_or_updates_paper_position,
    _filter_duplicate_candidates,
    _mark_execution_intents_seen,
    _probe_token_lookup,
    _probe_token_orderbook,
    _propagate_canonical_market_updates,
)
from predictcel.models import (
    BasketHealth,
    BasketMembership,
    CopyCandidate,
    ExecutionIntent,
    ExecutionResult,
    MarketSnapshot,
    WalletRegistryEntry,
    WalletTrade,
)


class FakeStore:
    def __init__(self, recent_fingerprints: set[str] | None = None) -> None:
        self.recent_fingerprints = recent_fingerprints or set()
        self.marked_signals = []

    def make_signal_fingerprint(self, market_id: str, topic: str, side: str) -> str:
        return f"{topic}:{market_id}:{side}"

    def has_recent_signals(self, signals, ttl_minutes: int = 1440):
        return {
            self.make_signal_fingerprint(market_id, topic, side)
            for market_id, topic, side in signals
            if self.make_signal_fingerprint(market_id, topic, side)
            in self.recent_fingerprints
        }

    def mark_signals_seen(self, signals) -> None:
        self.marked_signals.extend(list(signals))


class ProbeSourceClient:
    gamma_base_url = "https://gamma-api.polymarket.com"
    data_base_url = "https://data-api.polymarket.com"
    clob_base_url = "https://clob.polymarket.com"
    timeout_seconds = 15
    max_retries = 1
    retry_base_delay_seconds = 0.5


class RegistrySummaryStore:
    def __init__(
        self,
        registry_entries: list[WalletRegistryEntry],
        memberships: list[BasketMembership],
    ) -> None:
        self.registry_entries = registry_entries
        self.memberships = memberships
        self.saved_health: list[BasketHealth] = []

    def upsert_wallet_registry_entries(self, entries) -> None:
        self.registry_entries = list(entries)

    def upsert_basket_memberships(self, memberships) -> None:
        self.memberships = list(memberships)

    def load_wallet_registry_entries(self) -> list[WalletRegistryEntry]:
        return list(self.registry_entries)

    def load_basket_memberships(self, topic: str | None = None) -> list[BasketMembership]:
        if topic is None:
            return list(self.memberships)
        return [membership for membership in self.memberships if membership.topic == topic]

    def save_basket_health(self, health_snapshots: list[BasketHealth]) -> None:
        self.saved_health = list(health_snapshots)

    def latest_basket_health(self) -> dict[str, BasketHealth]:
        return {health.topic: health for health in self.saved_health}


def make_result(status: str, order_id: str = "order_123") -> ExecutionResult:
    return ExecutionResult(
        market_id="m1",
        topic="geopolitics",
        side="YES",
        token_id="yes_1",
        amount_usd=25.0,
        worst_price=0.52,
        status=status,
        order_id=order_id,
        error="",
        copyability_score=0.9,
        reason="test",
        market_title="One",
    )


def make_candidate(market_id: str, side: str = "YES") -> CopyCandidate:
    return CopyCandidate(
        topic="geopolitics",
        market_id=market_id,
        side=side,
        consensus_ratio=0.8,
        reference_price=0.5,
        current_price=0.5,
        liquidity_usd=10000,
        source_wallets=["w1"],
        wallet_quality_score=0.8,
        copyability_score=0.8,
        reason="test",
    )


def make_intent(market_id: str, side: str = "YES") -> ExecutionIntent:
    return ExecutionIntent(
        market_id=market_id,
        topic="geopolitics",
        side=side,
        token_id=f"token_{market_id}",
        amount_usd=25.0,
        worst_price=0.52,
        copyability_score=0.9,
        order_type="FOK",
        reason="test",
    )


def make_snapshot(
    market_id: str,
    orderbook_ready: bool = False,
    yes_ask_size: float = 0.0,
    no_ask_size: float = 0.0,
) -> MarketSnapshot:
    return MarketSnapshot(
        market_id=market_id,
        topic="geopolitics",
        title="One",
        yes_ask=0.51,
        no_ask=0.49,
        best_bid=0.5,
        liquidity_usd=1000.0,
        minutes_to_resolution=120,
        yes_token_id="yes_token",
        no_token_id="no_token",
        yes_bid=0.5,
        no_bid=0.5,
        yes_ask_size=yes_ask_size,
        no_ask_size=no_ask_size,
        orderbook_ready=orderbook_ready,
    )


def test_submitted_order_does_not_create_position() -> None:
    assert _creates_or_updates_paper_position(make_result("submitted")) is False


def test_filled_and_dry_run_results_create_positions() -> None:
    assert _creates_or_updates_paper_position(make_result("filled")) is True
    assert _creates_or_updates_paper_position(make_result("dry_run", order_id="")) is True


def test_filter_duplicate_candidates_skips_recent_and_same_batch_duplicates() -> None:
    store = FakeStore({"geopolitics:m1:YES"})

    fresh, skipped = _filter_duplicate_candidates(
        store,
        [
            make_candidate("m1", "YES"),
            make_candidate("m2", "YES"),
            make_candidate("m2", "YES"),
            make_candidate("m3", "NO"),
        ],
    )

    assert [(candidate.market_id, candidate.side) for candidate in fresh] == [
        ("m2", "YES"),
        ("m3", "NO"),
    ]
    assert skipped == 2


def test_mark_execution_intents_seen_only_marks_planned_intents() -> None:
    store = FakeStore()

    _mark_execution_intents_seen(store, [make_intent("m2", "YES"), make_intent("m3", "NO")])

    assert store.marked_signals == [
        ("m2", "geopolitics", "YES"),
        ("m3", "geopolitics", "NO"),
    ]


def test_discover_wallets_delegates_to_main(monkeypatch) -> None:
    observed = {}

    def fake_run_main() -> None:
        observed["argv"] = list(sys.argv)

    monkeypatch.setattr(discover_wallets, "run_main", fake_run_main)
    monkeypatch.setattr(
        sys,
        "argv",
        ["discover_wallets.py", "--config", "config/predictcel.example.json"],
    )

    discover_wallets.main()

    assert observed["argv"] == [
        "discover_wallets.py",
        "discover-wallets",
        "--config",
        "config/predictcel.example.json",
    ]


def test_compact_cycle_output_includes_execution_state() -> None:
    compact = _compact_cycle_output(
        {
            "mode": "live",
            "summary": {"copy_candidates": 2, "execution_intents": 0},
            "latency_ms": {"total_cycle_ms": 10},
            "db": {"path": "/data/predictcel.db"},
            "portfolio_summary": {"current_exposure_usd": 0},
            "execution": {
                "live_trading_requested": False,
                "execution_enabled": True,
                "planner_ran": False,
                "diagnostics": {"candidates_seen": 2, "candidates_planned": 0},
            },
        }
    )

    assert compact["execution"] == {
        "live_trading_requested": False,
        "execution_enabled": True,
        "planner_ran": False,
        "diagnostics": {"candidates_seen": 2, "candidates_planned": 0},
    }


def test_compact_cycle_output_includes_wallet_registry_summary() -> None:
    compact = _compact_cycle_output(
        {
            "mode": "live",
            "summary": {"copy_candidates": 0},
            "latency_ms": {"total_cycle_ms": 10},
            "db": {"path": "/data/predictcel.db"},
            "portfolio_summary": {"current_exposure_usd": 0},
            "wallet_registry": {
                "enabled": True,
                "registry_wallet_count": 9,
                "memberships_by_topic": {
                    "geopolitics": {
                        "core": 3,
                        "rotating": 0,
                        "backup": 0,
                        "explorer": 0,
                    }
                },
                "basket_health": {
                    "geopolitics": {
                        "tracked_wallet_count": 3,
                        "health_state": "thin",
                    }
                },
            },
        }
    )

    assert compact["wallet_registry"] == {
        "enabled": True,
        "registry_wallet_count": 9,
        "memberships_by_topic": {
            "geopolitics": {
                "core": 3,
                "rotating": 0,
                "backup": 0,
                "explorer": 0,
            }
        },
        "basket_health": {
            "geopolitics": {
                "tracked_wallet_count": 3,
                "health_state": "thin",
            }
        },
    }


def test_build_wallet_registry_summary_includes_live_roster() -> None:
    config = load_config(Path("config/predictcel.example.json"))
    config = replace(
        config,
        wallet_registry=replace(config.wallet_registry, enabled=True, seed_from_baskets=False),
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
    captured_at = datetime(2026, 1, 1, tzinfo=UTC)
    store = RegistrySummaryStore(
        registry_entries=[
            WalletRegistryEntry(
                wallet=wallet,
                source_type="static_basket",
                source_ref="config.baskets",
                trust_seed=1.0,
                status="active",
                first_seen_at=captured_at,
            )
            for wallet in ["w1", "w2", "w3", "w4"]
        ],
        memberships=[
            BasketMembership(
                topic="geopolitics",
                wallet=wallet,
                tier="core",
                rank=index,
                active=True,
                joined_at=captured_at,
                effective_until=None,
                promotion_reason="seeded",
                demotion_reason="",
            )
            for index, wallet in enumerate(["w1", "w2", "w3", "w4"], start=1)
        ],
    )
    trades = [
        WalletTrade("w1", "geopolitics", "m1", "YES", 0.5, 20.0, 1_200),
        WalletTrade("w2", "geopolitics", "m2", "NO", 0.4, 15.0, 90_000),
    ]

    summary = _build_wallet_registry_summary(config, store, trades)

    assert summary["live_roster_by_topic"]["geopolitics"] == {
        "selected_wallets": {
            "core": ["w1", "w2"],
            "rotating": ["w3"],
            "backup": ["w4"],
            "explorer": [],
        },
        "fresh_core_wallet_count": 1,
        "live_eligible_wallet_count": 1,
        "tracked_wallet_count": 4,
        "unfilled_slots": {
            "core": 0,
            "rotating": 0,
            "backup": 0,
            "explorer": 0,
        },
        "needs_refresh": True,
        "refresh_reasons": [
            "fresh_core_below_threshold",
            "live_eligible_wallets_below_threshold",
        ],
    }


def test_probe_token_orderbook_uses_fresh_client(monkeypatch) -> None:
    observed = {}

    class FreshProbeClient:
        def __init__(
            self,
            gamma_base_url,
            data_base_url,
            clob_base_url,
            timeout_seconds,
            max_retries,
            retry_base_delay_seconds,
        ):
            observed["args"] = (
                gamma_base_url,
                data_base_url,
                clob_base_url,
                timeout_seconds,
                max_retries,
                retry_base_delay_seconds,
            )

        def fetch_order_book(self, token_id: str):
            raise RuntimeError(f"root cause for {token_id}")

    monkeypatch.setattr("predictcel.main.PolymarketPublicClient", FreshProbeClient)

    result = _probe_token_orderbook(ProbeSourceClient(), "token_yes")

    assert result == {
        "token_id": "token_yes",
        "error": "RuntimeError: root cause for token_yes",
    }
    assert observed["args"] == (
        "https://gamma-api.polymarket.com",
        "https://data-api.polymarket.com",
        "https://clob.polymarket.com",
        15,
        1,
        0.5,
    )


def test_probe_token_lookup_uses_fresh_client(monkeypatch) -> None:
    class FreshProbeClient:
        def __init__(self, *args):
            pass

        def fetch_markets_by_clob_token_ids(
            self,
            token_ids: list[str],
            chunk_size: int = 25,
        ):
            assert token_ids == ["token_yes"]
            assert chunk_size == 1
            return [
                {
                    "conditionId": "cond_1",
                    "question": "Question",
                    "clobTokenIds": ["token_yes", "token_no"],
                }
            ]

    monkeypatch.setattr("predictcel.main.PolymarketPublicClient", FreshProbeClient)

    result = _probe_token_lookup(ProbeSourceClient(), "token_yes")

    assert result == {
        "matched_rows": 1,
        "condition_id": "cond_1",
        "slug": "",
        "question": "Question",
        "token_ids": ["token_yes", "token_no"],
        "matched_input_token": True,
    }


def test_propagate_canonical_market_updates_rebinds_stale_aliases() -> None:
    stale_snapshot = make_snapshot("m1", orderbook_ready=False)
    enriched_snapshot = make_snapshot(
        "m1",
        orderbook_ready=True,
        yes_ask_size=10.0,
        no_ask_size=20.0,
    )
    markets = {
        "m1": stale_snapshot,
        "slug-1": stale_snapshot,
        "yes_token": stale_snapshot,
        "no_token": stale_snapshot,
    }

    _propagate_canonical_market_updates(markets, [enriched_snapshot])

    assert markets["m1"] is enriched_snapshot
    assert markets["slug-1"] is enriched_snapshot
    assert markets["yes_token"] is enriched_snapshot
    assert markets["no_token"] is enriched_snapshot

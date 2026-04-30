import sys
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from predictcel.config import BasketRule, load_config
from predictcel import discover_wallets
from predictcel.main import (
    LiveInputFetchThresholdError,
    _analysis_trades,
    _auto_feed_wallet_registry_from_discovery,
    _build_wallet_registry_summary,
    _compact_cycle_output,
    _compact_live_input_diagnostics,
    _compact_scoring_diagnostics,
    _classify_unmatched_token_ids,
    _creates_or_updates_paper_position,
    _filter_duplicate_candidates,
    _is_evm_address,
    _load_live_inputs,
    _mark_execution_intents_seen,
    _probe_token_lookup,
    _probe_token_orderbook,
    _propagate_canonical_market_updates,
    _recover_unresolved_token_market_rows,
    _run_wallet_discovery,
    _wallet_topics_for_live_inputs,
)
from predictcel.models import (
    BasketAssignment,
    BasketManagerAction,
    BasketHealth,
    BasketMembership,
    CopyCandidate,
    ExecutionIntent,
    ExecutionResult,
    MarketSnapshot,
    WalletDiscoveryCandidate,
    WalletRegistryEntry,
    WalletTopicProfile,
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

    def load_basket_memberships(
        self, topic: str | None = None
    ) -> list[BasketMembership]:
        if topic is None:
            return list(self.memberships)
        return [
            membership for membership in self.memberships if membership.topic == topic
        ]

    def save_basket_health(self, health_snapshots: list[BasketHealth]) -> None:
        self.saved_health = list(health_snapshots)

    def latest_basket_health(self) -> dict[str, BasketHealth]:
        return {health.topic: health for health in self.saved_health}


class DiscoveryStore:
    def __init__(
        self,
        registry_entries: list[WalletRegistryEntry] | None = None,
        memberships: list[BasketMembership] | None = None,
    ) -> None:
        self.registry_entries = registry_entries or []
        self.memberships = memberships or []

    def upsert_wallet_registry_entries(self, entries) -> None:
        self.registry_entries = list(entries)

    def upsert_basket_memberships(self, memberships) -> None:
        self.memberships = list(memberships)

    def load_wallet_registry_entries(self) -> list[WalletRegistryEntry]:
        return list(self.registry_entries)

    def load_basket_memberships(
        self, topic: str | None = None
    ) -> list[BasketMembership]:
        if topic is None:
            return list(self.memberships)
        return [
            membership for membership in self.memberships if membership.topic == topic
        ]


class FakeLiveClient:
    def __init__(self, *args, **kwargs) -> None:
        del args, kwargs

    def fetch_active_markets(self, limit: int):
        del limit
        return []

    def fetch_markets_by_identifiers(self, identifiers):
        del identifiers
        return []

    def fetch_markets_by_slugs(self, slugs):
        del slugs
        return []

    def fetch_markets_by_clob_token_ids(
        self,
        token_ids: list[str],
        chunk_size: int = 25,
    ):
        del token_ids, chunk_size
        return []


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
    assert (
        _creates_or_updates_paper_position(make_result("dry_run", order_id="")) is True
    )


def test_is_evm_address_accepts_lowercase_and_valid_checksum_only() -> None:
    assert _is_evm_address("0xde709f2102306220921060314715629080e2fb77") is True
    assert _is_evm_address("0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed") is True
    assert _is_evm_address("0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAEd") is False


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

    _mark_execution_intents_seen(
        store, [make_intent("m2", "YES"), make_intent("m3", "NO")]
    )

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


def test_wallet_topics_for_live_inputs_prefers_active_registry_memberships() -> None:
    registry_wallet = "0x1111111111111111111111111111111111111111"
    static_wallet = "0x2222222222222222222222222222222222222222"
    config = replace(
        load_config(Path("config/predictcel.example.json")),
        baskets=[
            BasketRule(topic="sports", wallets=[static_wallet], quorum_ratio=0.66)
        ],
        wallet_registry=replace(
            load_config(Path("config/predictcel.example.json")).wallet_registry,
            enabled=True,
            seed_from_baskets=False,
        ),
    )
    store = DiscoveryStore(
        memberships=[
            BasketMembership(
                topic="sports",
                wallet=registry_wallet,
                tier="core",
                rank=1,
                active=True,
                joined_at=datetime(2026, 1, 1, tzinfo=UTC),
                effective_until=None,
                promotion_reason="seeded",
                demotion_reason="",
            ),
            BasketMembership(
                topic="sports",
                wallet=static_wallet,
                tier="backup",
                rank=2,
                active=False,
                joined_at=datetime(2026, 1, 1, tzinfo=UTC),
                effective_until=None,
                promotion_reason="seeded",
                demotion_reason="inactive",
            ),
        ]
    )

    wallet_topics, source = _wallet_topics_for_live_inputs(config, store)

    assert source == "registry_memberships"
    assert wallet_topics == {registry_wallet: ["sports"]}


def test_wallet_topics_for_live_inputs_skips_ineligible_registry_statuses() -> None:
    registry_wallet = "0x1111111111111111111111111111111111111111"
    static_wallet = "0x2222222222222222222222222222222222222222"
    config = replace(
        load_config(Path("config/predictcel.example.json")),
        baskets=[
            BasketRule(topic="sports", wallets=[static_wallet], quorum_ratio=0.66)
        ],
        wallet_registry=replace(
            load_config(Path("config/predictcel.example.json")).wallet_registry,
            enabled=True,
            seed_from_baskets=False,
        ),
    )
    store = DiscoveryStore(
        registry_entries=[
            WalletRegistryEntry(
                wallet=registry_wallet,
                source_type="static_basket",
                source_ref="config.baskets",
                trust_seed=1.0,
                status="suspended",
                first_seen_at=datetime(2026, 1, 1, tzinfo=UTC),
            )
        ],
        memberships=[
            BasketMembership(
                topic="sports",
                wallet=registry_wallet,
                tier="core",
                rank=1,
                active=True,
                joined_at=datetime(2026, 1, 1, tzinfo=UTC),
                effective_until=None,
                promotion_reason="seeded",
                demotion_reason="",
            )
        ],
    )

    wallet_topics, source = _wallet_topics_for_live_inputs(config, store)

    assert source == "registry_memberships"
    assert wallet_topics == {}


def test_load_live_inputs_uses_registry_memberships_for_wallet_fetches(
    monkeypatch,
) -> None:
    registry_wallet = "0x1111111111111111111111111111111111111111"
    static_wallet = "0x2222222222222222222222222222222222222222"
    config = replace(
        load_config(Path("config/predictcel.example.json")),
        baskets=[
            BasketRule(topic="sports", wallets=[static_wallet], quorum_ratio=0.66)
        ],
        wallet_registry=replace(
            load_config(Path("config/predictcel.example.json")).wallet_registry,
            enabled=True,
            seed_from_baskets=False,
        ),
    )
    store = DiscoveryStore(
        memberships=[
            BasketMembership(
                topic="sports",
                wallet=registry_wallet,
                tier="core",
                rank=1,
                active=True,
                joined_at=datetime(2026, 1, 1, tzinfo=UTC),
                effective_until=None,
                promotion_reason="seeded",
                demotion_reason="",
            )
        ]
    )
    observed: dict[str, object] = {}

    def fake_fetch_wallet_payloads(client, wallets, limit):
        del client, limit
        observed["wallets"] = list(wallets)
        return {wallet: [] for wallet in wallets}, []

    monkeypatch.setattr("predictcel.main.PolymarketPublicClient", FakeLiveClient)
    monkeypatch.setattr(
        "predictcel.main._fetch_wallet_payloads", fake_fetch_wallet_payloads
    )
    monkeypatch.setattr(
        "predictcel.main.build_wallet_trades", lambda payloads, topics: []
    )
    monkeypatch.setattr("predictcel.main.extract_trade_market_ids", lambda payloads: [])
    monkeypatch.setattr(
        "predictcel.main.extract_trade_market_slugs", lambda payloads: []
    )
    monkeypatch.setattr("predictcel.main.build_market_snapshots", lambda rows: {})

    trades, markets, diagnostics = _load_live_inputs(config, store)

    assert trades == []
    assert markets == {}
    assert observed["wallets"] == [registry_wallet]
    assert diagnostics["wallet_source"] == "registry_memberships"
    assert diagnostics["requested_wallets"] == 1


def test_load_live_inputs_tracks_trade_market_id_source_breakdown(monkeypatch) -> None:
    registry_wallet = "0x1111111111111111111111111111111111111111"
    config = replace(
        load_config(Path("config/predictcel.example.json")),
        wallet_registry=replace(
            load_config(Path("config/predictcel.example.json")).wallet_registry,
            enabled=True,
            seed_from_baskets=False,
        ),
    )
    store = DiscoveryStore(
        memberships=[
            BasketMembership(
                topic="sports",
                wallet=registry_wallet,
                tier="core",
                rank=1,
                active=True,
                joined_at=datetime(2026, 1, 1, tzinfo=UTC),
                effective_until=None,
                promotion_reason="seeded",
                demotion_reason="",
            )
        ]
    )

    def fake_fetch_wallet_payloads(client, wallets, limit):
        del client, limit
        return (
            {
                wallets[0]: [
                    {
                        "asset": "token_yes",
                        "market": {"id": "market_123", "slug": "will-x-happen"},
                    },
                    {
                        "clobTokenId": "token_no",
                        "market": {"conditionId": "cond_1"},
                    },
                    {"asset": "token_fallback"},
                ]
            },
            [],
        )

    monkeypatch.setattr("predictcel.main.PolymarketPublicClient", FakeLiveClient)
    monkeypatch.setattr(
        "predictcel.main._fetch_wallet_payloads", fake_fetch_wallet_payloads
    )
    monkeypatch.setattr(
        "predictcel.main.build_wallet_trades", lambda payloads, topics: []
    )
    monkeypatch.setattr(
        "predictcel.main.extract_trade_market_ids",
        lambda payloads: ["market_123", "cond_1", "token_fallback"],
    )
    monkeypatch.setattr(
        "predictcel.main.extract_trade_market_slugs", lambda payloads: ["will-x-happen"]
    )
    monkeypatch.setattr("predictcel.main.build_market_snapshots", lambda rows: {})

    _, _, diagnostics = _load_live_inputs(config, store)

    assert diagnostics["trade_market_id_source_breakdown"] == {
        "asset": 1,
        "market.conditionId": 1,
        "market.id": 1,
    }


def test_load_live_inputs_recovers_unresolved_token_markets(monkeypatch) -> None:
    registry_wallet = "0x1111111111111111111111111111111111111111"
    config = replace(
        load_config(Path("config/predictcel.example.json")),
        wallet_registry=replace(
            load_config(Path("config/predictcel.example.json")).wallet_registry,
            enabled=True,
            seed_from_baskets=False,
        ),
    )
    store = DiscoveryStore(
        memberships=[
            BasketMembership(
                topic="sports",
                wallet=registry_wallet,
                tier="core",
                rank=1,
                active=True,
                joined_at=datetime(2026, 1, 1, tzinfo=UTC),
                effective_until=None,
                promotion_reason="seeded",
                demotion_reason="",
            )
        ]
    )

    class TokenProbeClient(FakeLiveClient):
        gamma_base_url = "https://gamma-api.polymarket.com"
        data_base_url = "https://data-api.polymarket.com"
        clob_base_url = "https://clob.polymarket.com"
        timeout_seconds = 15
        max_retries = 1
        retry_base_delay_seconds = 0.5

        def fetch_markets_by_clob_token_ids(
            self,
            token_ids: list[str],
            chunk_size: int = 25,
        ):
            assert chunk_size == 1
            if token_ids == ["token_yes"]:
                return [
                    {
                        "conditionId": "cond_1",
                        "question": "Recovered Question",
                        "clobTokenIds": ["token_yes", "token_no"],
                        "outcomePrices": "[0.55, 0.41]",
                        "active": True,
                        "closed": False,
                        "liquidity": 9000,
                        "endDate": "2026-12-31T00:00:00Z",
                    }
                ]
            return []

    monkeypatch.setattr("predictcel.main.PolymarketPublicClient", TokenProbeClient)
    monkeypatch.setattr(
        "predictcel.main._fetch_wallet_payloads",
        lambda client, wallets, limit: ({wallets[0]: [{"asset": "token_yes"}]}, []),
    )
    monkeypatch.setattr(
        "predictcel.main.build_wallet_trades",
        lambda payloads, topics: [
            WalletTrade(
                registry_wallet,
                "sports",
                "token_yes",
                "YES",
                0.55,
                25.0,
                300,
            )
        ],
    )
    monkeypatch.setattr(
        "predictcel.main.extract_trade_market_ids", lambda payloads: ["token_yes"]
    )
    monkeypatch.setattr(
        "predictcel.main.extract_trade_market_slugs", lambda payloads: []
    )

    trades, markets, diagnostics = _load_live_inputs(config, store)

    assert len(trades) == 1
    assert "cond_1" in markets
    assert "token_yes" in markets
    assert diagnostics["token_probe_requested"] == 1
    assert diagnostics["token_probe_tokens_matched"] == 1
    assert diagnostics["token_probe_tokens_unmatched"] == 0
    assert diagnostics["token_probe_rows_loaded"] == 1


def test_load_live_inputs_indexes_token_aliases_from_loaded_market_rows(
    monkeypatch,
) -> None:
    registry_wallet = "0x1111111111111111111111111111111111111111"
    config = replace(
        load_config(Path("config/predictcel.example.json")),
        wallet_registry=replace(
            load_config(Path("config/predictcel.example.json")).wallet_registry,
            enabled=True,
            seed_from_baskets=False,
        ),
    )
    store = DiscoveryStore(
        memberships=[
            BasketMembership(
                topic="sports",
                wallet=registry_wallet,
                tier="core",
                rank=1,
                active=True,
                joined_at=datetime(2026, 1, 1, tzinfo=UTC),
                effective_until=None,
                promotion_reason="seeded",
                demotion_reason="",
            )
        ]
    )

    class AliasRichClient(FakeLiveClient):
        def fetch_active_markets(self, limit: int):
            del limit
            return [
                {
                    "conditionId": "cond_1",
                    "question": "Recovered From Active Rows",
                    "tokens": [
                        {"asset_id": "token_yes"},
                        {"assetId": "token_no"},
                    ],
                    "outcomePrices": "[0.55, 0.41]",
                    "active": True,
                    "closed": False,
                    "liquidity": 9000,
                    "endDate": "2026-12-31T00:00:00Z",
                }
            ]

    monkeypatch.setattr("predictcel.main.PolymarketPublicClient", AliasRichClient)
    monkeypatch.setattr(
        "predictcel.main._fetch_wallet_payloads",
        lambda client, wallets, limit: ({wallets[0]: [{"asset": "token_yes"}]}, []),
    )
    monkeypatch.setattr(
        "predictcel.main.build_wallet_trades",
        lambda payloads, topics: [
            WalletTrade(
                registry_wallet,
                "sports",
                "token_yes",
                "YES",
                0.55,
                25.0,
                300,
            )
        ],
    )
    monkeypatch.setattr(
        "predictcel.main.extract_trade_market_ids", lambda payloads: ["token_yes"]
    )
    monkeypatch.setattr(
        "predictcel.main.extract_trade_market_slugs", lambda payloads: []
    )

    trades, markets, diagnostics = _load_live_inputs(config, store)

    assert len(trades) == 1
    assert "cond_1" in markets
    assert "token_yes" in markets
    assert diagnostics["supplemental_market_ids_requested"] == 0
    assert diagnostics["token_probe_requested"] == 0
    assert diagnostics["token_aliases_added_from_rows"] >= 2


def test_load_live_inputs_raises_when_wallet_fetch_failures_exceed_threshold(
    monkeypatch,
) -> None:
    registry_wallets = [
        "0x1111111111111111111111111111111111111111",
        "0x2222222222222222222222222222222222222222",
    ]
    config = replace(
        load_config(Path("config/predictcel.example.json")),
        wallet_registry=replace(
            load_config(Path("config/predictcel.example.json")).wallet_registry,
            enabled=True,
            seed_from_baskets=False,
        ),
    )
    store = DiscoveryStore(
        memberships=[
            BasketMembership(
                topic="sports",
                wallet=wallet,
                tier="core",
                rank=index + 1,
                active=True,
                joined_at=datetime(2026, 1, 1, tzinfo=UTC),
                effective_until=None,
                promotion_reason="seeded",
                demotion_reason="",
            )
            for index, wallet in enumerate(registry_wallets)
        ]
    )

    monkeypatch.setattr("predictcel.main.PolymarketPublicClient", FakeLiveClient)
    monkeypatch.setattr(
        "predictcel.main._fetch_wallet_payloads",
        lambda client, wallets, limit: ({wallet: [] for wallet in wallets}, wallets),
    )

    with pytest.raises(
        LiveInputFetchThresholdError,
        match="wallet fetch failures exceeded threshold",
    ):
        _load_live_inputs(config, store)


def test_classify_unmatched_token_ids_breaks_down_gap_types() -> None:
    loaded_market_rows = [
        {
            "conditionId": "cond_present",
            "question": "Present In Loaded Rows",
            "tokens": [
                {"asset_id": "token_present"},
                {"assetId": "token_present_no"},
            ],
            "outcomePrices": "[0.60, 0.35]",
            "active": True,
            "closed": False,
            "liquidity": 5000,
            "endDate": "2026-12-31T00:00:00Z",
        }
    ]
    markets = {"cond_present": make_snapshot("cond_present")}
    token_probe_rows = [
        {
            "conditionId": "cond_outside",
            "question": "Outside Loaded Universe",
            "clobTokenIds": ["token_outside", "token_outside_no"],
            "outcomePrices": "[0.62, 0.31]",
            "active": True,
            "closed": False,
            "liquidity": 7000,
            "endDate": "2026-12-31T00:00:00Z",
        }
    ]

    diagnostics = _classify_unmatched_token_ids(
        ["token_present", "token_outside", "token_absent"],
        loaded_market_rows,
        markets,
        token_probe_rows,
    )

    assert diagnostics["breakdown"] == {
        "absent_from_loaded_market_rows": 1,
        "market_outside_loaded_universe": 1,
        "present_on_loaded_row_missing_crossref": 1,
    }
    assert diagnostics["samples"] == {
        "absent_from_loaded_market_rows": ["token_absent"],
        "market_outside_loaded_universe": ["token_outside"],
        "present_on_loaded_row_missing_crossref": ["token_present"],
    }


def test_run_wallet_discovery_persists_registry_inputs_when_db_is_provided(
    monkeypatch,
    capsys,
) -> None:
    config = replace(
        load_config(Path("config/predictcel.example.json")),
        wallet_registry=replace(
            load_config(Path("config/predictcel.example.json")).wallet_registry,
            enabled=True,
        ),
    )
    store = DiscoveryStore()

    class FakePipeline:
        def __init__(self, _config) -> None:
            self.config = _config

        def run(self):
            candidates = [
                WalletDiscoveryCandidate(
                    wallet_address="w_new",
                    source="polymarket_data_api",
                    total_trades=25,
                    recent_trades=10,
                    avg_trade_size_usd=55.0,
                    topic_profile=WalletTopicProfile(
                        topic_affinities={"geopolitics": 0.9},
                        primary_topic="geopolitics",
                        specialization_score=0.8,
                    ),
                    score=0.74,
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
                    overall_score=0.74,
                    confidence="HIGH",
                    reasons=["strong specialization"],
                )
            ]
            return candidates, assignments, []

        def write_reports(self, output_dir, config_path, config_output, results=None):
            return {
                "wallet_discovery_report": str(
                    Path(output_dir) / "wallet_discovery_report.json"
                )
            }

    monkeypatch.setattr("predictcel.main.load_config", lambda _: config)
    monkeypatch.setattr("predictcel.main.WalletDiscoveryPipeline", FakePipeline)
    monkeypatch.setattr("predictcel.main.SignalStore", lambda db_path: store)

    _run_wallet_discovery(
        [
            "--config",
            "config/predictcel.example.json",
            "--db",
            "predictcel.db",
            "--output-dir",
            "data",
        ]
    )

    output = capsys.readouterr().out
    assert {entry.wallet for entry in store.registry_entries} == {"w_new"}
    assert [
        (membership.topic, membership.wallet, membership.tier)
        for membership in store.memberships
    ] == [("geopolitics", "w_new", "explorer")]
    assert '"discovered_wallets_ingested": 1' in output
    assert '"persisted": true' in output
    assert '"mode": "auto_update"' in output
    assert '"new_registry_entries": 1' in output
    assert '"new_explorer_memberships": 1' in output
    assert '"manager_actions_applied": 0' in output
    assert '"skipped_existing_wallets": 0' in output


def test_run_wallet_discovery_skips_existing_and_rejected_wallets(
    monkeypatch,
    capsys,
) -> None:
    config = replace(
        load_config(Path("config/predictcel.example.json")),
        wallet_registry=replace(
            load_config(Path("config/predictcel.example.json")).wallet_registry,
            enabled=True,
        ),
    )
    store = DiscoveryStore(
        registry_entries=[
            WalletRegistryEntry(
                wallet="w_existing",
                source_type="static_basket",
                source_ref="config.baskets",
                trust_seed=1.0,
                status="active",
                first_seen_at=datetime(2026, 1, 1, tzinfo=UTC),
            )
        ],
        memberships=[
            BasketMembership(
                topic="geopolitics",
                wallet="w_existing",
                tier="explorer",
                rank=1,
                active=True,
                joined_at=datetime(2026, 1, 1, tzinfo=UTC),
                effective_until=None,
                promotion_reason="existing",
                demotion_reason="",
            )
        ],
    )

    class FakePipeline:
        def __init__(self, _config) -> None:
            self.config = _config

        def run(self):
            candidates = [
                WalletDiscoveryCandidate(
                    wallet_address="w_existing",
                    source="polymarket_data_api",
                    total_trades=25,
                    recent_trades=10,
                    avg_trade_size_usd=55.0,
                    topic_profile=WalletTopicProfile(
                        topic_affinities={"geopolitics": 0.9},
                        primary_topic="geopolitics",
                        specialization_score=0.8,
                    ),
                    score=0.74,
                    confidence="HIGH",
                    rejected_reasons=[],
                ),
                WalletDiscoveryCandidate(
                    wallet_address="w_rejected",
                    source="polymarket_data_api",
                    total_trades=2,
                    recent_trades=0,
                    avg_trade_size_usd=1.0,
                    topic_profile=WalletTopicProfile(
                        topic_affinities={"geopolitics": 0.9},
                        primary_topic="geopolitics",
                        specialization_score=0.2,
                    ),
                    score=0.1,
                    confidence="LOW",
                    rejected_reasons=["not enough recent trades"],
                ),
            ]
            assignments = [
                BasketAssignment(
                    wallet_address="w_existing",
                    primary_topic="geopolitics",
                    recommended_baskets=["geopolitics"],
                    topic_affinities={"geopolitics": 0.9},
                    overall_score=0.74,
                    confidence="HIGH",
                    reasons=["strong specialization"],
                )
            ]
            return candidates, assignments, []

        def write_reports(self, output_dir, config_path, config_output, results=None):
            return {
                "wallet_discovery_report": str(
                    Path(output_dir) / "wallet_discovery_report.json"
                )
            }

    monkeypatch.setattr("predictcel.main.load_config", lambda _: config)
    monkeypatch.setattr("predictcel.main.WalletDiscoveryPipeline", FakePipeline)
    monkeypatch.setattr("predictcel.main.SignalStore", lambda db_path: store)

    _run_wallet_discovery(
        [
            "--config",
            "config/predictcel.example.json",
            "--db",
            "predictcel.db",
            "--output-dir",
            "data",
        ]
    )

    output = capsys.readouterr().out
    assert [entry.wallet for entry in store.registry_entries] == ["w_existing"]
    assert [
        (membership.topic, membership.wallet, membership.tier, membership.rank)
        for membership in store.memberships
    ] == [("geopolitics", "w_existing", "explorer", 1)]
    assert '"discovered_wallets_ingested": 1' in output
    assert '"persisted": true' in output
    assert '"mode": "auto_update"' in output
    assert '"new_registry_entries": 0' in output
    assert '"new_explorer_memberships": 0' in output
    assert '"manager_actions_applied": 0' in output
    assert '"skipped_existing_wallets": 1' in output


def test_run_wallet_discovery_report_only_does_not_persist_when_db_is_provided(
    monkeypatch,
    capsys,
) -> None:
    config = replace(
        load_config(Path("config/predictcel.example.json")),
        wallet_registry=replace(
            load_config(Path("config/predictcel.example.json")).wallet_registry,
            enabled=True,
        ),
        wallet_discovery=replace(
            load_config(Path("config/predictcel.example.json")).wallet_discovery,
            enabled=True,
            mode="report_only",
        ),
    )
    store = DiscoveryStore()

    class FakePipeline:
        def __init__(self, _config) -> None:
            self.config = _config

        def run(self):
            candidates = [
                WalletDiscoveryCandidate(
                    wallet_address="w_new",
                    source="polymarket_data_api",
                    total_trades=25,
                    recent_trades=10,
                    avg_trade_size_usd=55.0,
                    topic_profile=WalletTopicProfile(
                        topic_affinities={"geopolitics": 0.9},
                        primary_topic="geopolitics",
                        specialization_score=0.8,
                    ),
                    score=0.74,
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
                    overall_score=0.74,
                    confidence="HIGH",
                    reasons=["strong specialization"],
                )
            ]
            return candidates, assignments, []

        def write_reports(self, output_dir, config_path, config_output, results=None):
            return {
                "wallet_discovery_report": str(
                    Path(output_dir) / "wallet_discovery_report.json"
                )
            }

    monkeypatch.setattr("predictcel.main.load_config", lambda _: config)
    monkeypatch.setattr("predictcel.main.WalletDiscoveryPipeline", FakePipeline)
    monkeypatch.setattr("predictcel.main.SignalStore", lambda db_path: store)

    _run_wallet_discovery(
        [
            "--config",
            "config/predictcel.example.json",
            "--db",
            "predictcel.db",
            "--output-dir",
            "data",
        ]
    )

    output = capsys.readouterr().out
    assert store.registry_entries == []
    assert store.memberships == []
    assert '"persisted": false' in output
    assert '"mode": "report_only"' in output
    assert '"discovered_wallets_ingested": 0' in output


def test_run_wallet_discovery_closes_pipeline_when_store_setup_fails(
    monkeypatch,
) -> None:
    config = load_config(Path("config/predictcel.example.json"))
    observed = {"pipeline_closed": False}

    class FakePipeline:
        def __init__(self, _config) -> None:
            self.config = _config

        def close(self) -> None:
            observed["pipeline_closed"] = True

    monkeypatch.setattr("predictcel.main.load_config", lambda _: config)
    monkeypatch.setattr("predictcel.main.WalletDiscoveryPipeline", FakePipeline)
    monkeypatch.setattr(
        "predictcel.main.SignalStore",
        lambda db_path: (_ for _ in ()).throw(RuntimeError(f"db init failed: {db_path}")),
    )

    with pytest.raises(RuntimeError, match="db init failed: predictcel.db"):
        _run_wallet_discovery(
            [
                "--config",
                "config/predictcel.example.json",
                "--db",
                "predictcel.db",
                "--output-dir",
                "data",
            ]
        )

    assert observed["pipeline_closed"] is True


def test_auto_feed_wallet_registry_from_discovery_ingests_accepted_candidates(
    monkeypatch,
) -> None:
    config = replace(
        load_config(Path("config/predictcel.example.json")),
        wallet_registry=replace(
            load_config(Path("config/predictcel.example.json")).wallet_registry,
            enabled=True,
        ),
        wallet_discovery=replace(
            load_config(Path("config/predictcel.example.json")).wallet_discovery,
            enabled=True,
        ),
    )
    store = DiscoveryStore()

    class FakePipeline:
        def __init__(self, _config) -> None:
            self.config = _config

        def run(self):
            candidates = [
                WalletDiscoveryCandidate(
                    wallet_address="w_new",
                    source="polymarket_data_api",
                    total_trades=25,
                    recent_trades=10,
                    avg_trade_size_usd=55.0,
                    topic_profile=WalletTopicProfile(
                        topic_affinities={"geopolitics": 0.9},
                        primary_topic="geopolitics",
                        specialization_score=0.8,
                    ),
                    score=0.74,
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
                    overall_score=0.74,
                    confidence="HIGH",
                    reasons=["strong specialization"],
                )
            ]
            return candidates, assignments, []

    monkeypatch.setattr("predictcel.main.WalletDiscoveryPipeline", FakePipeline)

    diagnostics = _auto_feed_wallet_registry_from_discovery(config, store)

    assert diagnostics == {
        "enabled": True,
        "persisted": True,
        "mode": "auto_update",
        "ran": True,
        "discovered_wallets_ingested": 1,
        "new_registry_entries": 1,
        "new_explorer_memberships": 1,
        "manager_actions_applied": 0,
        "manager_action_counts": {},
        "skipped_existing_wallets": 0,
        "error": None,
    }
    assert {entry.wallet for entry in store.registry_entries} == {"w_new"}
    assert [
        (membership.topic, membership.wallet, membership.tier)
        for membership in store.memberships
    ] == [("geopolitics", "w_new", "explorer")]


def test_auto_feed_wallet_registry_from_discovery_skips_report_only_mode(
    monkeypatch,
) -> None:
    config = replace(
        load_config(Path("config/predictcel.example.json")),
        wallet_registry=replace(
            load_config(Path("config/predictcel.example.json")).wallet_registry,
            enabled=True,
        ),
        wallet_discovery=replace(
            load_config(Path("config/predictcel.example.json")).wallet_discovery,
            enabled=True,
            mode="report_only",
        ),
    )
    store = DiscoveryStore()
    observed = {"constructed": False}

    class FakePipeline:
        def __init__(self, _config) -> None:
            observed["constructed"] = True

    monkeypatch.setattr("predictcel.main.WalletDiscoveryPipeline", FakePipeline)

    diagnostics = _auto_feed_wallet_registry_from_discovery(config, store)

    assert diagnostics == {
        "enabled": True,
        "persisted": False,
        "mode": "report_only",
        "ran": False,
        "discovered_wallets_ingested": 0,
        "new_registry_entries": 0,
        "new_explorer_memberships": 0,
        "manager_actions_applied": 0,
        "manager_action_counts": {},
        "skipped_existing_wallets": 0,
        "error": None,
    }
    assert observed["constructed"] is False
    assert store.registry_entries == []
    assert store.memberships == []


def test_auto_feed_wallet_registry_from_discovery_skips_propose_config_mode(
    monkeypatch,
) -> None:
    config = replace(
        load_config(Path("config/predictcel.example.json")),
        wallet_registry=replace(
            load_config(Path("config/predictcel.example.json")).wallet_registry,
            enabled=True,
        ),
        wallet_discovery=replace(
            load_config(Path("config/predictcel.example.json")).wallet_discovery,
            enabled=True,
            mode="propose_config",
        ),
    )
    store = DiscoveryStore()
    observed = {"constructed": False}

    class FakePipeline:
        def __init__(self, _config) -> None:
            observed["constructed"] = True

    monkeypatch.setattr("predictcel.main.WalletDiscoveryPipeline", FakePipeline)

    diagnostics = _auto_feed_wallet_registry_from_discovery(config, store)

    assert diagnostics == {
        "enabled": True,
        "persisted": False,
        "mode": "propose_config",
        "ran": False,
        "discovered_wallets_ingested": 0,
        "new_registry_entries": 0,
        "new_explorer_memberships": 0,
        "manager_actions_applied": 0,
        "manager_action_counts": {},
        "skipped_existing_wallets": 0,
        "error": None,
    }
    assert observed["constructed"] is False
    assert store.registry_entries == []
    assert store.memberships == []


def test_auto_feed_wallet_registry_from_discovery_applies_manager_actions_to_memberships(
    monkeypatch,
) -> None:
    config = replace(
        load_config(Path("config/predictcel.example.json")),
        wallet_registry=replace(
            load_config(Path("config/predictcel.example.json")).wallet_registry,
            enabled=True,
        ),
        wallet_discovery=replace(
            load_config(Path("config/predictcel.example.json")).wallet_discovery,
            enabled=True,
        ),
    )
    captured_at = datetime(2026, 1, 1, tzinfo=UTC)
    store = DiscoveryStore(
        registry_entries=[
            WalletRegistryEntry(
                wallet="w_existing",
                source_type="static_basket",
                source_ref="config.baskets",
                trust_seed=1.0,
                status="active",
                first_seen_at=captured_at,
            )
        ],
        memberships=[
            BasketMembership(
                topic="geopolitics",
                wallet="w_existing",
                tier="core",
                rank=1,
                active=True,
                joined_at=captured_at,
                effective_until=None,
                promotion_reason="seeded",
                demotion_reason="",
            )
        ],
    )

    class FakePipeline:
        def __init__(self, _config) -> None:
            self.config = _config

        def set_current_wallets_by_topic(self, current_wallets_by_topic) -> None:
            self.current_wallets_by_topic = current_wallets_by_topic

        def run(self):
            candidates = [
                WalletDiscoveryCandidate(
                    wallet_address="w_new",
                    source="polymarket_data_api",
                    total_trades=25,
                    recent_trades=10,
                    avg_trade_size_usd=55.0,
                    topic_profile=WalletTopicProfile(
                        topic_affinities={"geopolitics": 0.9},
                        primary_topic="geopolitics",
                        specialization_score=0.8,
                    ),
                    score=0.74,
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
                    overall_score=0.74,
                    confidence="HIGH",
                    reasons=["strong specialization"],
                )
            ]
            actions = [
                BasketManagerAction(
                    "suspend",
                    "geopolitics",
                    "w_existing",
                    0.3,
                    "LOW",
                    "existing wallet confidence fell to LOW",
                )
            ]
            return candidates, assignments, actions

    monkeypatch.setattr("predictcel.main.WalletDiscoveryPipeline", FakePipeline)

    diagnostics = _auto_feed_wallet_registry_from_discovery(config, store)

    assert diagnostics["manager_actions_applied"] == 1
    assert diagnostics["manager_action_counts"] == {"suspend": 1}
    assert [
        (membership.wallet, membership.active, membership.demotion_reason)
        for membership in store.memberships
    ] == [
        ("w_existing", False, "existing wallet confidence fell to LOW"),
        ("w_new", True, ""),
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


def test_compact_cycle_output_omits_sensitive_state_by_default() -> None:
    compact = _compact_cycle_output(
        {
            "mode": "live",
            "summary": {"copy_candidates": 1},
            "latency_ms": {"total_cycle_ms": 10},
            "db": {"path": "/data/predictcel.db"},
            "portfolio_summary": {"current_exposure_usd": 0},
            "execution_results": [{"status": "filled"}],
            "open_positions": [{"market_id": "m1"}],
        }
    )

    assert "execution_results" not in compact
    assert "open_positions" not in compact


def test_compact_cycle_output_includes_sensitive_state_when_requested() -> None:
    compact = _compact_cycle_output(
        {
            "mode": "live",
            "summary": {"copy_candidates": 1},
            "latency_ms": {"total_cycle_ms": 10},
            "db": {"path": "/data/predictcel.db"},
            "portfolio_summary": {"current_exposure_usd": 0},
            "execution_results": [{"status": "filled"}],
            "open_positions": [{"market_id": "m1"}],
        },
        include_sensitive=True,
    )

    assert compact["execution_results"] == [{"status": "filled"}]
    assert compact["open_positions"] == [{"market_id": "m1"}]


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


def test_compact_live_input_diagnostics_includes_unmatched_token_breakdown() -> None:
    diagnostics = _compact_live_input_diagnostics(
        {
            "requested_wallets": 3,
            "valid_wallets": 3,
            "wallet_payloads_loaded": 12,
            "wallets_with_parsed_trades": 2,
            "parsed_trade_count": 9,
            "active_market_rows_loaded": 4,
            "trade_market_ids_seen": 5,
            "trade_market_slugs_seen": 2,
            "supplemental_market_ids_requested": 1,
            "supplemental_market_rows_loaded": 1,
            "unresolved_market_ids_after_supplemental": 2,
            "unresolved_token_ids_after_supplemental": 2,
            "token_probe_requested": 2,
            "token_probe_rows_loaded": 1,
            "token_probe_tokens_matched": 1,
            "token_probe_tokens_unmatched": 1,
            "supplemental_market_slugs_requested": 0,
            "supplemental_slug_rows_loaded": 0,
            "market_cache_entries": 6,
            "relevant_markets_enriched": 3,
            "orderbook_ready_markets": 2,
            "markets_with_yes_depth": 2,
            "markets_with_no_depth": 1,
            "market_crossref": {"matched_count": 4},
            "trade_market_id_source_breakdown": {"asset": 1, "market.id": 2},
            "token_aliases_added_from_rows": 3,
            "unmatched_token_breakdown": {
                "absent_from_loaded_market_rows": 1,
                "market_outside_loaded_universe": 1,
            },
            "sample_unmatched_tokens_by_class": {
                "absent_from_loaded_market_rows": ["token_absent"],
            },
        }
    )

    assert diagnostics["token_aliases_added_from_rows"] == 3
    assert diagnostics["unmatched_token_breakdown"] == {
        "absent_from_loaded_market_rows": 1,
        "market_outside_loaded_universe": 1,
    }
    assert diagnostics["trade_market_id_source_breakdown"] == {
        "asset": 1,
        "market.id": 2,
    }
    assert diagnostics["sample_unmatched_tokens_by_class"] == {
        "absent_from_loaded_market_rows": ["token_absent"]
    }


def test_compact_scoring_diagnostics_includes_attrition_and_missing_breakdown() -> None:
    diagnostics = _compact_scoring_diagnostics(
        {
            "rejection_counts": {"missing_market": 3, "too_old": 1},
            "wallet_rejection_counts": {
                "w1": {"missing_market": 2},
                "w2": {"missing_market": 1, "too_old": 1},
            },
            "wallet_attrition": {
                "wallets_seen": 5,
                "wallets_scored": 2,
                "wallets_fully_rejected": 3,
            },
            "analysis_trade_count": 7,
            "pre_scoring_too_old_filtered": 4,
            "missing_market_breakdown": {"token_id_like": 2, "slug_like": 1},
            "missing_market_samples": ["0xabc", "slug-1"],
            "missing_market_by_wallet": {"w_hot": 4, "w_mid": 2, "w_low": 1},
            "missing_market_samples_by_wallet": {
                "w_hot": ["0xabc", "0xdef"],
                "w_mid": ["slug-1"],
            },
        }
    )

    assert diagnostics["rejection_counts"] == {"missing_market": 3, "too_old": 1}
    assert diagnostics["wallets_with_rejections"] == 2
    assert diagnostics["wallet_attrition"] == {
        "wallets_seen": 5,
        "wallets_scored": 2,
        "wallets_fully_rejected": 3,
    }
    assert diagnostics["analysis_trade_count"] == 7
    assert diagnostics["pre_scoring_too_old_filtered"] == 4
    assert diagnostics["missing_market_breakdown"] == {
        "token_id_like": 2,
        "slug_like": 1,
    }
    assert diagnostics["missing_market_samples"] == ["0xabc", "slug-1"]
    assert diagnostics["missing_market_by_wallet"] == {
        "w_hot": 4,
        "w_mid": 2,
        "w_low": 1,
    }
    assert diagnostics["missing_market_samples_by_wallet"] == {
        "w_hot": ["0xabc", "0xdef"],
        "w_mid": ["slug-1"],
    }


def test_analysis_trades_filters_too_old_rows() -> None:
    trades = [
        WalletTrade("w1", "sports", "m1", "YES", 0.5, 10.0, 300),
        WalletTrade("w1", "sports", "m2", "YES", 0.5, 10.0, 3600),
        WalletTrade("w1", "sports", "m3", "YES", 0.5, 10.0, 3601),
    ]

    filtered = _analysis_trades(trades, 3600)

    assert [trade.market_id for trade in filtered] == ["m1", "m2"]


def test_build_wallet_registry_summary_includes_live_roster() -> None:
    config = load_config(Path("config/predictcel.example.json"))
    config = replace(
        config,
        wallet_registry=replace(
            config.wallet_registry,
            enabled=True,
            seed_from_baskets=False,
            min_probation_days=1,
            min_eligible_trades_for_approval=1,
        ),
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
    first_seen_at = datetime(2026, 1, 1, tzinfo=UTC)
    store = RegistrySummaryStore(
        registry_entries=[
            WalletRegistryEntry(
                wallet=wallet,
                source_type="static_basket",
                source_ref="config.baskets",
                trust_seed=1.0,
                status="active",
                first_seen_at=first_seen_at,
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
                joined_at=first_seen_at,
                effective_until=None,
                promotion_reason="seeded",
                demotion_reason="",
            )
            for index, wallet in enumerate(["w1", "w2", "w3", "w4"], start=1)
        ],
    )
    trades = [
        WalletTrade("w1", "geopolitics", "m1", "YES", 0.5, 20.0, 1_200),
        WalletTrade("w2", "geopolitics", "m2", "NO", 0.4, 15.0, 1_800),
    ]

    summary = _build_wallet_registry_summary(config, store, trades)

    assert summary["live_roster_by_topic"]["geopolitics"] == {
        "selected_wallets": {
            "core": ["w1", "w2"],
            "rotating": [],
            "backup": [],
            "explorer": [],
        },
        "wallet_decisions": [
            {
                "wallet": "w1",
                "membership_tier": "core",
                "membership_rank": 1,
                "registry_status": "active",
                "trust_seed": 1.0,
                "selected": True,
                "selected_tier": "core",
                "eligible_trade_count": 1,
                "eligible_market_count": 1,
                "decision_reasons": [],
            },
            {
                "wallet": "w2",
                "membership_tier": "core",
                "membership_rank": 2,
                "registry_status": "active",
                "trust_seed": 1.0,
                "selected": True,
                "selected_tier": "core",
                "eligible_trade_count": 1,
                "eligible_market_count": 1,
                "decision_reasons": [],
            },
            {
                "wallet": "w3",
                "membership_tier": "core",
                "membership_rank": 3,
                "registry_status": "retired",
                "trust_seed": 1.0,
                "selected": False,
                "selected_tier": None,
                "eligible_trade_count": 0,
                "eligible_market_count": 0,
                "decision_reasons": [
                    "slots_filled_for_core",
                    "status_ineligible_for_rotating",
                    "status_ineligible_for_backup",
                ],
            },
            {
                "wallet": "w4",
                "membership_tier": "core",
                "membership_rank": 4,
                "registry_status": "retired",
                "trust_seed": 1.0,
                "selected": False,
                "selected_tier": None,
                "eligible_trade_count": 0,
                "eligible_market_count": 0,
                "decision_reasons": [
                    "slots_filled_for_core",
                    "status_ineligible_for_rotating",
                    "status_ineligible_for_backup",
                ],
            },
        ],
        "fresh_core_wallet_count": 2,
        "live_eligible_wallet_count": 2,
        "tracked_wallet_count": 4,
        "unfilled_slots": {
            "core": 0,
            "rotating": 1,
            "backup": 1,
            "explorer": 0,
        },
        "rotation_interval_hours": 24,
        "oldest_rotating_wallet_age_hours": None,
        "rotation_due": False,
        "needs_refresh": True,
        "refresh_reasons": [
            "live_eligible_wallets_below_threshold",
        ],
    }


def test_build_wallet_registry_summary_keeps_memberships_read_only() -> None:
    config = load_config(Path("config/predictcel.example.json"))
    config = replace(
        config,
        wallet_registry=replace(
            config.wallet_registry, enabled=True, seed_from_baskets=False
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
    captured_at = datetime.now(UTC)
    store = RegistrySummaryStore(
        registry_entries=[],
        memberships=[
            BasketMembership(
                topic="geopolitics",
                wallet=wallet,
                tier=tier,
                rank=index,
                active=True,
                joined_at=captured_at,
                effective_until=None,
                promotion_reason="seeded",
                demotion_reason="",
            )
            for index, (wallet, tier) in enumerate(
                [
                    ("w1", "core"),
                    ("w2", "core"),
                    ("w3", "rotating"),
                    ("w4", "backup"),
                    ("w5", "explorer"),
                ],
                start=1,
            )
        ],
    )
    trades = [
        WalletTrade("w5", "geopolitics", "m1", "YES", 0.6, 15.0, 60),
        WalletTrade("w5", "geopolitics", "m2", "YES", 0.59, 15.0, 120),
        WalletTrade("w4", "geopolitics", "m3", "YES", 0.58, 15.0, 180),
        WalletTrade("w3", "geopolitics", "m4", "YES", 0.57, 15.0, 240),
        WalletTrade("w2", "geopolitics", "m5", "YES", 0.56, 15.0, 300),
    ]

    summary = _build_wallet_registry_summary(config, store, trades)

    assert [
        (membership.wallet, membership.tier, membership.rank, membership.active)
        for membership in store.memberships
    ] == [
        ("w1", "core", 1, True),
        ("w2", "core", 2, True),
        ("w3", "rotating", 3, True),
        ("w4", "backup", 4, True),
        ("w5", "explorer", 5, True),
    ]
    assert summary["live_roster_by_topic"]["geopolitics"]["selected_wallets"] == {
        "core": ["w5", "w4"],
        "rotating": ["w3"],
        "backup": ["w2"],
        "explorer": [],
    }


def test_build_wallet_registry_summary_persists_rebalance_when_requested() -> None:
    config = load_config(Path("config/predictcel.example.json"))
    config = replace(
        config,
        wallet_registry=replace(
            config.wallet_registry, enabled=True, seed_from_baskets=False
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
    captured_at = datetime.now(UTC)
    store = RegistrySummaryStore(
        registry_entries=[],
        memberships=[
            BasketMembership(
                topic="geopolitics",
                wallet=wallet,
                tier=tier,
                rank=index,
                active=True,
                joined_at=captured_at,
                effective_until=None,
                promotion_reason="seeded",
                demotion_reason="",
            )
            for index, (wallet, tier) in enumerate(
                [
                    ("w1", "core"),
                    ("w2", "core"),
                    ("w3", "rotating"),
                    ("w4", "backup"),
                    ("w5", "explorer"),
                ],
                start=1,
            )
        ],
    )
    trades = [
        WalletTrade("w5", "geopolitics", "m1", "YES", 0.6, 15.0, 60),
        WalletTrade("w5", "geopolitics", "m2", "YES", 0.59, 15.0, 120),
        WalletTrade("w4", "geopolitics", "m3", "YES", 0.58, 15.0, 180),
        WalletTrade("w3", "geopolitics", "m4", "YES", 0.57, 15.0, 240),
        WalletTrade("w2", "geopolitics", "m5", "YES", 0.56, 15.0, 300),
    ]

    summary = _build_wallet_registry_summary(
        config,
        store,
        trades,
        persist_rebalance=True,
    )

    assert [
        (membership.wallet, membership.tier, membership.rank, membership.active)
        for membership in store.memberships
    ] == [
        ("w5", "core", 1, True),
        ("w4", "core", 2, True),
        ("w3", "rotating", 3, True),
        ("w2", "backup", 4, True),
        ("w1", "core", 5, False),
    ]
    assert summary["live_roster_by_topic"]["geopolitics"]["selected_wallets"] == {
        "core": ["w5", "w4"],
        "rotating": ["w3"],
        "backup": ["w2"],
        "explorer": [],
    }


def test_build_wallet_registry_summary_does_not_reseed_manually_curated_topic() -> None:
    base_config = load_config(Path("config/predictcel.example.json"))
    config = replace(
        base_config,
        baskets=[
            BasketRule("geopolitics", ["w1", "w2"], 0.8, 0.5),
            BasketRule("macro-financial", ["w3"], 0.8, 0.5),
        ],
        wallet_registry=replace(
            base_config.wallet_registry, enabled=True, seed_from_baskets=True
        ),
    )
    captured_at = datetime.now(UTC)
    store = RegistrySummaryStore(
        registry_entries=[
            WalletRegistryEntry(
                wallet="w1",
                source_type="static_basket",
                source_ref="config.baskets",
                trust_seed=1.0,
                status="active",
                first_seen_at=captured_at,
            )
        ],
        memberships=[
            BasketMembership(
                topic="geopolitics",
                wallet="w1",
                tier="explorer",
                rank=7,
                active=True,
                joined_at=captured_at,
                effective_until=None,
                promotion_reason="manual override",
                demotion_reason="",
            )
        ],
    )

    _build_wallet_registry_summary(config, store, [])

    assert [
        (
            membership.topic,
            membership.wallet,
            membership.tier,
            membership.rank,
            membership.promotion_reason,
        )
        for membership in store.memberships
    ] == [
        ("geopolitics", "w1", "explorer", 7, "manual override"),
        (
            "macro-financial",
            "w3",
            "core",
            1,
            "seeded from static basket config",
        ),
    ]


def test_build_wallet_registry_summary_bootstraps_static_baskets_after_discovery_auto_feed() -> (
    None
):
    base_config = load_config(Path("config/predictcel.example.json"))
    config = replace(
        base_config,
        baskets=[
            BasketRule(topic="geopolitics", wallets=["w_seed"], quorum_ratio=0.66)
        ],
        wallet_registry=replace(
            base_config.wallet_registry, enabled=True, seed_from_baskets=True
        ),
    )
    captured_at = datetime.now(UTC)
    store = RegistrySummaryStore(
        registry_entries=[
            WalletRegistryEntry(
                wallet="w_new",
                source_type="wallet_discovery",
                source_ref="polymarket_data_api",
                trust_seed=0.8,
                status="probation",
                first_seen_at=captured_at,
            )
        ],
        memberships=[
            BasketMembership(
                topic="geopolitics",
                wallet="w_new",
                tier="explorer",
                rank=9,
                active=True,
                joined_at=captured_at,
                effective_until=None,
                promotion_reason="wallet discovery assignment",
                demotion_reason="",
            )
        ],
    )

    summary = _build_wallet_registry_summary(config, store, [])

    assert {entry.wallet for entry in store.registry_entries} == {"w_new", "w_seed"}
    assert [
        (membership.topic, membership.wallet, membership.tier)
        for membership in store.memberships
    ] == [
        ("geopolitics", "w_seed", "core"),
        ("geopolitics", "w_new", "explorer"),
    ]
    assert summary["registry_wallet_count"] == 2


def test_build_wallet_registry_summary_refreshes_registry_statuses_from_trade_freshness() -> (
    None
):
    config = load_config(Path("config/predictcel.example.json"))
    config = replace(
        config,
        wallet_registry=replace(
            config.wallet_registry,
            enabled=True,
            seed_from_baskets=False,
            min_probation_days=7,
            min_eligible_trades_for_approval=2,
            stale_after_hours=72,
            suspend_after_hours=168,
            retire_after_days=30,
        ),
        filters=replace(config.filters, max_trade_age_seconds=3600),
    )
    store = RegistrySummaryStore(
        registry_entries=[
            WalletRegistryEntry(
                wallet=wallet,
                source_type="static_basket",
                source_ref="config.baskets",
                trust_seed=1.0,
                status="active",
                first_seen_at=first_seen_at,
            )
            for wallet, first_seen_at in [
                ("w_active", datetime(2025, 12, 1, tzinfo=UTC)),
                ("w_probation", datetime(2025, 12, 30, tzinfo=UTC)),
                ("w_stale", datetime(2025, 12, 1, tzinfo=UTC)),
            ]
        ],
        memberships=[],
    )
    trades = [
        WalletTrade("w_active", "geopolitics", "m1", "YES", 0.6, 15.0, 300),
        WalletTrade("w_active", "geopolitics", "m2", "YES", 0.59, 15.0, 600),
        WalletTrade("w_probation", "geopolitics", "m3", "YES", 0.58, 15.0, 300),
        WalletTrade("w_stale", "geopolitics", "m4", "YES", 0.57, 15.0, 80 * 3600),
    ]

    _build_wallet_registry_summary(config, store, trades)

    assert {entry.wallet: entry.status for entry in store.registry_entries} == {
        "w_active": "active",
        "w_probation": "active",
        "w_stale": "stale",
    }


def test_build_wallet_registry_summary_backfills_missing_static_baskets_into_existing_store() -> (
    None
):
    config = load_config(Path("config/predictcel.example.json"))
    config = replace(
        config,
        baskets=[
            BasketRule("geopolitics", ["w1", "w2"], 0.8, 0.5),
            BasketRule("macro-financial", ["w3"], 0.8, 0.5),
        ],
        wallet_registry=replace(
            config.wallet_registry,
            enabled=True,
            seed_from_baskets=True,
        ),
    )
    first_seen_at = datetime(2026, 1, 1, tzinfo=UTC)
    store = RegistrySummaryStore(
        registry_entries=[
            WalletRegistryEntry(
                wallet="w1",
                source_type="static_basket",
                source_ref="config.baskets",
                trust_seed=1.0,
                status="active",
                first_seen_at=first_seen_at,
            )
        ],
        memberships=[
            BasketMembership(
                topic="geopolitics",
                wallet="w1",
                tier="core",
                rank=1,
                active=True,
                joined_at=first_seen_at,
                effective_until=None,
                promotion_reason="seeded from static basket config",
                demotion_reason="",
            )
        ],
    )

    summary = _build_wallet_registry_summary(config, store, [])

    assert {entry.wallet for entry in store.registry_entries} == {"w1", "w2", "w3"}
    assert [
        (membership.topic, membership.wallet, membership.tier, membership.rank)
        for membership in store.memberships
    ] == [
        ("geopolitics", "w1", "core", 1),
        ("geopolitics", "w2", "core", 2),
        ("macro-financial", "w3", "core", 1),
    ]
    assert summary["memberships_by_topic"] == {
        "geopolitics": {
            "core": 2,
            "rotating": 0,
            "backup": 0,
            "explorer": 0,
        },
        "macro-financial": {
            "core": 1,
            "rotating": 0,
            "backup": 0,
            "explorer": 0,
        },
    }


def test_build_wallet_registry_summary_promotes_discovery_registry_entry_into_static_basket() -> (
    None
):
    base_config = load_config(Path("config/predictcel.example.json"))
    config = replace(
        base_config,
        baskets=[
            BasketRule(topic="geopolitics", wallets=["w_promoted"], quorum_ratio=0.66)
        ],
        wallet_registry=replace(
            base_config.wallet_registry,
            enabled=True,
            seed_from_baskets=True,
        ),
        filters=replace(base_config.filters, max_trade_age_seconds=3600),
    )
    first_seen_at = datetime(2026, 1, 1, tzinfo=UTC)
    store = RegistrySummaryStore(
        registry_entries=[
            WalletRegistryEntry(
                wallet="w_promoted",
                source_type="wallet_discovery",
                source_ref="polymarket_data_api",
                trust_seed=0.62,
                status="probation",
                first_seen_at=first_seen_at,
            )
        ],
        memberships=[],
    )

    _build_wallet_registry_summary(
        config,
        store,
        [WalletTrade("w_promoted", "geopolitics", "m1", "YES", 0.6, 10.0, 300)],
    )

    assert len(store.registry_entries) == 1
    entry = store.registry_entries[0]
    assert entry.wallet == "w_promoted"
    assert entry.source_type == "static_basket"
    assert entry.source_ref == "config.baskets"
    assert entry.trust_seed == 1.0
    assert entry.first_seen_at == first_seen_at
    assert entry.status == "active"
    assert entry.last_scored_at is not None
    assert entry.last_seen_trade_at is not None


def test_build_wallet_registry_summary_reactivates_config_membership_from_discovery_assignment() -> (
    None
):
    base_config = load_config(Path("config/predictcel.example.json"))
    config = replace(
        base_config,
        baskets=[BasketRule(topic="geopolitics", wallets=["w1"], quorum_ratio=0.66)],
        wallet_registry=replace(
            base_config.wallet_registry,
            enabled=True,
            seed_from_baskets=True,
        ),
    )
    first_seen_at = datetime(2026, 1, 1, tzinfo=UTC)
    store = RegistrySummaryStore(
        registry_entries=[
            WalletRegistryEntry(
                wallet="w1",
                source_type="wallet_discovery",
                source_ref="polymarket_data_api",
                trust_seed=0.8,
                status="probation",
                first_seen_at=first_seen_at,
            )
        ],
        memberships=[
            BasketMembership(
                topic="geopolitics",
                wallet="w1",
                tier="explorer",
                rank=9,
                active=False,
                joined_at=first_seen_at,
                effective_until=first_seen_at,
                promotion_reason="wallet discovery assignment",
                demotion_reason="aged out",
            )
        ],
    )

    _build_wallet_registry_summary(config, store, [])

    assert [
        (
            membership.topic,
            membership.wallet,
            membership.tier,
            membership.rank,
            membership.active,
            membership.promotion_reason,
            membership.demotion_reason,
            membership.effective_until,
        )
        for membership in store.memberships
    ] == [
        (
            "geopolitics",
            "w1",
            "core",
            1,
            True,
            "seeded from static basket config",
            "",
            None,
        )
    ]


def test_build_wallet_registry_summary_flags_bench_depth_when_explorer_wallets_exist_but_live_roster_is_thin() -> (
    None
):
    config = load_config(Path("config/predictcel.example.json"))
    config = replace(
        config,
        wallet_registry=replace(
            config.wallet_registry,
            enabled=True,
            seed_from_baskets=False,
            min_probation_days=1,
            min_eligible_trades_for_approval=1,
        ),
        basket_controller=replace(
            config.basket_controller,
            core_slots=1,
            rotating_slots=1,
            backup_slots=0,
            explorer_slots=1,
            force_refresh_if_fresh_core_below=2,
            min_active_eligible_wallets=2,
        ),
    )
    first_seen_at = datetime(2026, 1, 1, tzinfo=UTC)
    store = RegistrySummaryStore(
        registry_entries=[
            WalletRegistryEntry(
                wallet=wallet,
                source_type="wallet_discovery" if wallet == "w3" else "static_basket",
                source_ref="polymarket_data_api"
                if wallet == "w3"
                else "config.baskets",
                trust_seed=1.0,
                status="active",
                first_seen_at=first_seen_at,
            )
            for wallet in ["w1", "w2", "w3"]
        ],
        memberships=[
            BasketMembership(
                topic="geopolitics",
                wallet="w1",
                tier="core",
                rank=1,
                active=True,
                joined_at=first_seen_at,
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
                joined_at=first_seen_at,
                effective_until=None,
                promotion_reason="seeded",
                demotion_reason="",
            ),
            BasketMembership(
                topic="geopolitics",
                wallet="w3",
                tier="explorer",
                rank=3,
                active=True,
                joined_at=first_seen_at,
                effective_until=None,
                promotion_reason="wallet discovery assignment",
                demotion_reason="",
            ),
        ],
    )
    trades = [
        WalletTrade("w1", "geopolitics", "m1", "YES", 0.6, 15.0, 300),
    ]

    summary = _build_wallet_registry_summary(config, store, trades)

    assert summary["promotion_watch_by_topic"] == {
        "geopolitics": {
            "explorer_wallet_count": 1,
            "wallet_discovery_explorer_wallet_count": 1,
            "live_eligible_wallet_count": 1,
            "fresh_core_wallet_count": 1,
            "reason": "bench_depth_available",
        }
    }


def test_build_wallet_registry_summary_includes_basket_promotion_recommendations() -> (
    None
):
    config = load_config(Path("config/predictcel.example.json"))
    config = replace(
        config,
        wallet_registry=replace(
            config.wallet_registry,
            enabled=True,
            seed_from_baskets=False,
            min_probation_days=1,
            min_eligible_trades_for_approval=1,
        ),
        basket_promotion=replace(
            config.basket_promotion,
            min_tracked_wallets=3,
            min_fresh_active_wallets_7d=2,
            min_live_eligible_wallets=2,
            min_fresh_core_wallets_24h=1,
            min_eligible_trades_7d=3,
            max_stale_ratio=0.5,
        ),
        basket_controller=replace(
            config.basket_controller,
            tracked_basket_target=4,
            core_slots=1,
            rotating_slots=1,
            backup_slots=1,
            explorer_slots=1,
            min_active_eligible_wallets=2,
            force_refresh_if_fresh_core_below=1,
        ),
    )
    captured_at = datetime(2026, 1, 1, tzinfo=UTC)
    store = RegistrySummaryStore(
        registry_entries=[
            WalletRegistryEntry(
                wallet=wallet,
                source_type="wallet_discovery",
                source_ref="curated_wallet_file",
                trust_seed=0.8,
                status="active",
                first_seen_at=captured_at,
            )
            for wallet in ["w1", "w2", "w3", "w4"]
        ],
        memberships=[
            BasketMembership(
                "esports", "w1", "core", 1, True, captured_at, None, "discovered", ""
            ),
            BasketMembership(
                "esports", "w2", "core", 2, True, captured_at, None, "discovered", ""
            ),
            BasketMembership(
                "esports",
                "w3",
                "rotating",
                3,
                True,
                captured_at,
                None,
                "discovered",
                "",
            ),
            BasketMembership(
                "esports",
                "w4",
                "explorer",
                4,
                True,
                captured_at,
                None,
                "discovered",
                "",
            ),
        ],
    )
    trades = [
        WalletTrade("w1", "esports", "m1", "YES", 0.61, 15.0, 300),
        WalletTrade("w2", "esports", "m2", "YES", 0.62, 15.0, 600),
        WalletTrade("w3", "esports", "m3", "YES", 0.63, 15.0, 900),
    ]

    summary = _build_wallet_registry_summary(config, store, trades)

    assert summary["basket_promotion_by_topic"]["esports"] == {
        "topic": "esports",
        "should_promote": True,
        "tracked_wallet_count": 4,
        "fresh_active_wallets_7d": 3,
        "live_eligible_wallet_count": 2,
        "fresh_core_wallets_24h": 1,
        "eligible_trades_7d": 3,
        "stale_ratio": 0.25,
        "recommended_quorum_ratio": 0.8,
        "recommended_wallets": ["w1", "w2"],
        "missing_requirements": [],
    }


def test_build_wallet_registry_summary_ignores_static_explorer_bench_depth_for_promotion_watch() -> (
    None
):
    config = load_config(Path("config/predictcel.example.json"))
    config = replace(
        config,
        wallet_registry=replace(
            config.wallet_registry, enabled=True, seed_from_baskets=False
        ),
        basket_controller=replace(
            config.basket_controller,
            core_slots=1,
            rotating_slots=1,
            backup_slots=0,
            explorer_slots=1,
            force_refresh_if_fresh_core_below=2,
            min_active_eligible_wallets=2,
        ),
    )
    captured_at = datetime.now(UTC)
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
            for wallet in ["w1", "w2", "w3"]
        ],
        memberships=[
            BasketMembership(
                topic="geopolitics",
                wallet="w1",
                tier="core",
                rank=1,
                active=True,
                joined_at=captured_at,
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
                joined_at=captured_at,
                effective_until=None,
                promotion_reason="seeded",
                demotion_reason="",
            ),
            BasketMembership(
                topic="geopolitics",
                wallet="w3",
                tier="explorer",
                rank=3,
                active=True,
                joined_at=captured_at,
                effective_until=None,
                promotion_reason="seeded",
                demotion_reason="",
            ),
        ],
    )
    trades = [
        WalletTrade("w1", "geopolitics", "m1", "YES", 0.6, 15.0, 300),
    ]

    summary = _build_wallet_registry_summary(config, store, trades)

    assert summary["promotion_watch_by_topic"] == {}


def test_probe_token_orderbook_uses_passed_client() -> None:
    observed = {}

    class ReusedProbeClient(ProbeSourceClient):
        def fetch_order_book(self, token_id: str):
            observed["token_id"] = token_id
            raise RuntimeError(f"root cause for {token_id}")

    client = ReusedProbeClient()

    result = _probe_token_orderbook(client, "token_yes")

    assert result == {
        "token_id": "token_yes",
        "error": "RuntimeError: root cause for token_yes",
    }
    assert observed == {"token_id": "token_yes"}


def test_probe_token_lookup_uses_passed_client() -> None:
    observed = {}

    class ReusedProbeClient(ProbeSourceClient):
        def fetch_markets_by_clob_token_ids(
            self,
            token_ids: list[str],
            chunk_size: int = 25,
        ):
            observed["call"] = (token_ids, chunk_size)
            return [
                {
                    "conditionId": "cond_1",
                    "question": "Question",
                    "clobTokenIds": ["token_yes", "token_no"],
                }
            ]

    result = _probe_token_lookup(ReusedProbeClient(), "token_yes")

    assert result == {
        "matched_rows": 1,
        "condition_id": "cond_1",
        "slug": "",
        "question": "Question",
        "token_ids": ["token_yes", "token_no"],
        "matched_input_token": True,
    }
    assert observed == {"call": (["token_yes"], 1)}


def test_recover_unresolved_token_market_rows_uses_single_token_probes() -> None:
    class ReusedProbeClient(ProbeSourceClient):
        def __init__(self) -> None:
            super().__init__()
            self.calls: list[tuple[list[str], int]] = []

        def fetch_markets_by_clob_token_ids(
            self,
            token_ids: list[str],
            chunk_size: int = 25,
        ):
            self.calls.append((token_ids, chunk_size))
            if token_ids == ["token_yes"]:
                return [
                    {
                        "conditionId": "cond_1",
                        "question": "Question",
                        "clobTokenIds": ["token_yes", "token_no"],
                    }
                ]
            return []

    client = ReusedProbeClient()

    rows, stats, unresolved = _recover_unresolved_token_market_rows(
        client,
        ["token_yes", "token_missing"],
    )

    assert rows == [
        {
            "conditionId": "cond_1",
            "question": "Question",
            "clobTokenIds": ["token_yes", "token_no"],
        }
    ]
    assert stats == {"requested": 2, "matched": 1, "unmatched": 1}
    assert unresolved == ["token_missing"]
    assert sorted(client.calls) == [
        (["token_missing"], 1),
        (["token_yes"], 1),
    ]


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

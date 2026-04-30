import json
from datetime import UTC, datetime, timedelta

import pytest

from predictcel.basket_assignment import BasketAssignmentEngine
from predictcel.basket_manager import BasketManagerPlanner
from predictcel.config import (
    ArbitrageConfig,
    AppConfig,
    BasketRule,
    ConsensusConfig,
    ExecutionConfig,
    ExposureConfig,
    FilterConfig,
    LiveDataConfig,
    PositionConfig,
    WalletDiscoveryConfig,
)
from predictcel.models import BasketManagerAction, WalletDiscoveryCandidate
from predictcel.wallet_discovery import WalletDiscoveryPipeline
from predictcel.wallet_sources import (
    CuratedWalletFileSource,
    DataApiMarketTradesWalletSource,
)


class FakeSource:
    def fetch_candidates(self, limit: int):
        return [
            {"address": "0xnew", "source": "fake"},
            {"address": "0xexisting", "source": "fake"},
        ]

    def fetch_wallet_trades(self, address: str, limit: int):
        if address == "0xnew":
            return [
                {
                    "question": "NBA finals winner",
                    "size": "20",
                    "createdAt": "2999-01-01T00:00:00Z",
                },
                {
                    "question": "NFL playoff winner",
                    "size": "30",
                    "createdAt": "2999-01-01T00:00:00Z",
                },
                {
                    "question": "NBA MVP",
                    "size": "40",
                    "createdAt": "2999-01-01T00:00:00Z",
                },
            ]
        if address == "0xregistry":
            return [
                {
                    "question": "NBA playoff seed",
                    "size": "18",
                    "createdAt": "2999-01-01T00:00:00Z",
                },
                {
                    "question": "NFL draft pick",
                    "size": "22",
                    "createdAt": "2999-01-01T00:00:00Z",
                },
                {
                    "question": "NBA regular season wins",
                    "size": "28",
                    "createdAt": "2999-01-01T00:00:00Z",
                },
            ]
        if address == "0xexisting":
            return [
                {
                    "question": "NBA regular season wins",
                    "size": "18",
                    "createdAt": "2999-01-01T00:00:00Z",
                },
                {
                    "question": "NFL draft pick",
                    "size": "22",
                    "createdAt": "2999-01-01T00:00:00Z",
                },
                {
                    "question": "NBA playoff seed",
                    "size": "28",
                    "createdAt": "2999-01-01T00:00:00Z",
                },
            ]
        return []


def make_config(mode: str = "auto_update") -> AppConfig:
    discovery = WalletDiscoveryConfig(
        mode=mode,
        candidate_limit=10,
        trade_limit_per_wallet=10,
        min_trades=3,
        min_recent_trades=2,
        min_avg_trade_size_usd=10,
        exclude_existing_wallets=True,
        topics={"sports": ["nba", "nfl"], "crypto": ["btc"]},
    )
    return AppConfig(
        baskets=[BasketRule(topic="sports", wallets=["0xexisting"], quorum_ratio=0.66)],
        filters=FilterConfig(3600, 0.05, 5000, 60, 1440, 100),
        arbitrage=ArbitrageConfig(min_gross_edge=0.02, min_liquidity_usd=5000),
        wallet_trades_path="",
        market_snapshots_path="",
        live_data=LiveDataConfig(
            False,
            "https://gamma-api.polymarket.com",
            "https://data-api.polymarket.com",
            "https://clob.polymarket.com",
            10,
            10,
            15,
        ),
        execution=ExecutionConfig(
            False,
            True,
            0.7,
            1,
            10.0,
            0.02,
            "FOK",
            137,
            0,
            PositionConfig(0.3, 0.1, 1440),
            ExposureConfig(100, 10),
            3,
            1.0,
        ),
        consensus=ConsensusConfig(),
        wallet_discovery=discovery,
    )


def make_pipeline(mode: str = "auto_update") -> WalletDiscoveryPipeline:
    pipeline = WalletDiscoveryPipeline(make_config(mode))
    pipeline.source = FakeSource()
    pipeline.assignment_engine = BasketAssignmentEngine(
        pipeline.config.wallet_discovery
    )
    pipeline.manager = BasketManagerPlanner(pipeline.config)
    return pipeline


def test_pipeline_init_closes_client_when_source_setup_fails(monkeypatch) -> None:
    created_clients = []

    class FakeClient:
        def __init__(self, *args, **kwargs) -> None:
            del args, kwargs
            self.closed = False
            created_clients.append(self)

        def close(self) -> None:
            self.closed = True

    monkeypatch.setattr("predictcel.wallet_discovery.PolymarketPublicClient", FakeClient)
    monkeypatch.setattr(
        WalletDiscoveryPipeline,
        "_build_source",
        lambda self: (_ for _ in ()).throw(RuntimeError("source setup failed")),
    )

    with pytest.raises(RuntimeError, match="source setup failed"):
        WalletDiscoveryPipeline(make_config())

    assert created_clients[0].closed is True


class FakeMarketTradesClient:
    def fetch_market_trades(self, market_ids: list[str], limit: int):
        assert market_ids == ["cond_1", "cond_2"]
        assert limit == 3
        return [
            {"proxyWallet": "0xAAA", "conditionId": "cond_1"},
            {"proxyWallet": "0xaaa", "conditionId": "cond_1"},
            {"proxyWallet": "0xBBB", "conditionId": "cond_2"},
            {"proxyWallet": "", "conditionId": "cond_2"},
            {"proxyWallet": "0xCCC", "conditionId": "cond_2"},
        ]

    def fetch_wallet_trades(self, address: str, limit: int):
        return [{"conditionId": "cond_1", "outcome": "YES", "size": "20"}]


def test_pipeline_filters_existing_wallets_and_assigns_new_candidate() -> None:
    pipeline = make_pipeline()

    candidates, assignments, actions = pipeline.run()

    assert [candidate.wallet_address for candidate in candidates] == ["0xnew"]
    assert isinstance(candidates[0], WalletDiscoveryCandidate)
    assert candidates[0].rejected_reasons == []
    assert candidates[0].history_days >= 1
    assert candidates[0].history_score > 0
    assert candidates[0].activity_score > 0
    assert candidates[0].size_band_score > 0
    new_assignment = next(
        assignment for assignment in assignments if assignment.wallet_address == "0xnew"
    )
    assert new_assignment.recommended_baskets == ["sports"]
    assert any(
        action.action == "add" and action.wallet_address == "0xnew"
        for action in actions
    )


def test_pipeline_rejects_hyper_fast_wallets() -> None:
    pipeline = make_pipeline()
    fast_trades = [
        {
            "question": f"NBA prop {index}",
            "size": "20",
            "createdAt": "2999-01-01T00:00:00Z",
        }
        for index in range(1, 26)
    ]

    candidate = pipeline._build_candidate("0xfast", "fake", fast_trades)

    assert (
        "activity cadence looks too fast to copy safely" in candidate.rejected_reasons
    )
    assert candidate.activity_score <= 0.2


def test_pipeline_rejects_short_history_wallets() -> None:
    pipeline = make_pipeline()
    pipeline.config = AppConfig(
        baskets=pipeline.config.baskets,
        filters=pipeline.config.filters,
        arbitrage=pipeline.config.arbitrage,
        wallet_trades_path=pipeline.config.wallet_trades_path,
        market_snapshots_path=pipeline.config.market_snapshots_path,
        live_data=pipeline.config.live_data,
        execution=pipeline.config.execution,
        consensus=pipeline.config.consensus,
        wallet_discovery=WalletDiscoveryConfig(
            enabled=pipeline.config.wallet_discovery.enabled,
            mode=pipeline.config.wallet_discovery.mode,
            source=pipeline.config.wallet_discovery.source,
            candidate_limit=pipeline.config.wallet_discovery.candidate_limit,
            trade_limit_per_wallet=pipeline.config.wallet_discovery.trade_limit_per_wallet,
            min_trades=pipeline.config.wallet_discovery.min_trades,
            min_recent_trades=pipeline.config.wallet_discovery.min_recent_trades,
            min_history_days=30,
            recent_window_seconds=pipeline.config.wallet_discovery.recent_window_seconds,
            min_avg_trade_size_usd=pipeline.config.wallet_discovery.min_avg_trade_size_usd,
            min_assignment_score=pipeline.config.wallet_discovery.min_assignment_score,
            exclude_existing_wallets=pipeline.config.wallet_discovery.exclude_existing_wallets,
            max_wallets_per_basket=pipeline.config.wallet_discovery.max_wallets_per_basket,
            max_new_wallets_per_run=pipeline.config.wallet_discovery.max_new_wallets_per_run,
            topics=pipeline.config.wallet_discovery.topics,
        ),
    )
    base = datetime.now(UTC)
    short_history_trades = [
        {
            "question": f"NBA prop {index}",
            "size": "25",
            "createdAt": (base - timedelta(hours=index))
            .isoformat()
            .replace("+00:00", "Z"),
        }
        for index in range(3)
    ]

    candidate = pipeline._build_candidate("0xshort", "fake", short_history_trades)

    assert "history too short for registry promotion" in candidate.rejected_reasons
    assert candidate.history_days == 1
    assert candidate.history_score < 1.0


def test_pipeline_still_evaluates_existing_wallets_for_manager_actions() -> None:
    pipeline = make_pipeline()

    _, assignments, _ = pipeline.run()

    assert any(assignment.wallet_address == "0xexisting" for assignment in assignments)


def test_pipeline_can_source_existing_wallets_from_registry_memberships() -> None:
    pipeline = make_pipeline()
    pipeline.set_current_wallets_by_topic({"sports": {"0xregistry"}})

    _, assignments, _ = pipeline.run()

    assert any(assignment.wallet_address == "0xregistry" for assignment in assignments)


def test_auto_update_mutates_config_path_by_default(tmp_path) -> None:
    config_path = tmp_path / "predictcel.json"
    config_path.write_text(
        json.dumps(
            {
                "baskets": [
                    {"topic": "sports", "wallets": ["0xexisting"], "quorum_ratio": 0.66}
                ]
            }
        ),
        encoding="utf-8",
    )
    pipeline = make_pipeline("auto_update")

    reports = pipeline.write_reports(tmp_path / "reports", config_path)
    updated = json.loads(config_path.read_text(encoding="utf-8"))

    assert reports["updated_config"] == str(config_path)
    assert updated["baskets"][0]["wallets"] == ["0xexisting", "0xnew"]


def test_propose_config_writes_separate_proposal(tmp_path) -> None:
    config_path = tmp_path / "predictcel.json"
    config_path.write_text(
        json.dumps(
            {
                "baskets": [
                    {"topic": "sports", "wallets": ["0xexisting"], "quorum_ratio": 0.66}
                ]
            }
        ),
        encoding="utf-8",
    )
    pipeline = make_pipeline("propose_config")

    reports = pipeline.write_reports(tmp_path / "reports", config_path)
    original = json.loads(config_path.read_text(encoding="utf-8"))
    proposed = json.loads(
        (tmp_path / "reports" / "predictcel.proposed.json").read_text(encoding="utf-8")
    )

    assert reports["config_proposal"] == str(
        tmp_path / "reports" / "predictcel.proposed.json"
    )
    assert original["baskets"][0]["wallets"] == ["0xexisting"]
    assert proposed["baskets"][0]["wallets"] == ["0xexisting", "0xnew"]


def test_build_mutated_config_removes_wallets_for_remove_and_suspend_actions() -> None:
    pipeline = make_pipeline()
    payload = {
        "baskets": [
            {
                "topic": "sports",
                "wallets": ["0xexisting", "0xsuspend", "0xremove"],
                "quorum_ratio": 0.66,
            }
        ]
    }
    actions = [
        BasketManagerAction("suspend", "sports", "0xSuspend", 0.3, "LOW", "suspend it"),
        BasketManagerAction("remove", "sports", "0xREMOVE", 0.2, "LOW", "remove it"),
    ]

    mutated = pipeline.build_mutated_config(payload, actions)

    assert mutated["baskets"][0]["wallets"] == ["0xexisting"]


def test_build_mutated_config_normalizes_legacy_uppercase_actions() -> None:
    pipeline = make_pipeline()
    payload = {
        "baskets": [
            {
                "topic": "sports",
                "wallets": ["0xexisting", "0xremove"],
                "quorum_ratio": 0.66,
            }
        ]
    }
    actions = [
        BasketManagerAction("ADD", "sports", "0xNew", 0.9, "HIGH", "add it"),
        BasketManagerAction("REMOVE", "sports", "0xREMOVE", 0.2, "LOW", "remove it"),
    ]

    mutated = pipeline.build_mutated_config(payload, actions)

    assert mutated["baskets"][0]["wallets"] == ["0xexisting", "0xNew"]


def test_market_trades_source_collects_unique_wallet_candidates() -> None:
    source = DataApiMarketTradesWalletSource(
        FakeMarketTradesClient(),
        ["cond_1", "cond_2"],
    )

    candidates = source.fetch_candidates(3)

    assert candidates == [
        {
            "address": "0xaaa",
            "source": "polymarket_data_api_market_trades",
            "raw": {"proxyWallet": "0xAAA", "conditionId": "cond_1"},
        },
        {
            "address": "0xbbb",
            "source": "polymarket_data_api_market_trades",
            "raw": {"proxyWallet": "0xBBB", "conditionId": "cond_2"},
        },
        {
            "address": "0xccc",
            "source": "polymarket_data_api_market_trades",
            "raw": {"proxyWallet": "0xCCC", "conditionId": "cond_2"},
        },
    ]


def test_curated_wallet_file_source_collects_unique_wallet_candidates(tmp_path) -> None:
    candidate_path = tmp_path / "wallets.json"
    candidate_path.write_text(
        json.dumps(
            [
                {"wallet": "0xAAA", "source": "polydata", "dominant_topic": "sports"},
                {"address": "0xbbb", "source": "polyintel"},
                {"walletAddress": "0xAAA", "source": "duplicate"},
            ]
        ),
        encoding="utf-8",
    )
    source = CuratedWalletFileSource(FakeMarketTradesClient(), candidate_path)

    candidates = source.fetch_candidates(10)

    assert candidates == [
        {
            "address": "0xaaa",
            "source": "polydata",
            "raw": {
                "wallet": "0xAAA",
                "source": "polydata",
                "dominant_topic": "sports",
            },
        },
        {
            "address": "0xbbb",
            "source": "polyintel",
            "raw": {"address": "0xbbb", "source": "polyintel"},
        },
    ]


def test_pipeline_uses_market_trade_source_when_configured(monkeypatch) -> None:
    observed = {}

    class CapturingSource:
        def __init__(self, client, market_ids: list[str]) -> None:
            observed["client"] = client
            observed["market_ids"] = market_ids

        def fetch_candidates(self, limit: int):
            return []

        def fetch_wallet_trades(self, address: str, limit: int):
            return []

    monkeypatch.setattr(
        "predictcel.wallet_discovery.DataApiMarketTradesWalletSource", CapturingSource
    )
    monkeypatch.setattr(
        WalletDiscoveryPipeline,
        "_discovery_market_ids",
        lambda self: ["cond_1", "cond_2"],
    )

    config = make_config()
    config = AppConfig(
        baskets=config.baskets,
        filters=config.filters,
        arbitrage=config.arbitrage,
        wallet_trades_path=config.wallet_trades_path,
        market_snapshots_path=config.market_snapshots_path,
        live_data=config.live_data,
        execution=config.execution,
        consensus=config.consensus,
        market_regime=config.market_regime,
        wallet_discovery=WalletDiscoveryConfig(
            enabled=config.wallet_discovery.enabled,
            mode=config.wallet_discovery.mode,
            source="data_api_market_trades",
            candidate_limit=config.wallet_discovery.candidate_limit,
            trade_limit_per_wallet=config.wallet_discovery.trade_limit_per_wallet,
            min_trades=config.wallet_discovery.min_trades,
            min_recent_trades=config.wallet_discovery.min_recent_trades,
            recent_window_seconds=config.wallet_discovery.recent_window_seconds,
            min_avg_trade_size_usd=config.wallet_discovery.min_avg_trade_size_usd,
            min_assignment_score=config.wallet_discovery.min_assignment_score,
            exclude_existing_wallets=config.wallet_discovery.exclude_existing_wallets,
            max_wallets_per_basket=config.wallet_discovery.max_wallets_per_basket,
            max_new_wallets_per_run=config.wallet_discovery.max_new_wallets_per_run,
            topics=config.wallet_discovery.topics,
        ),
        wallet_registry=config.wallet_registry,
        basket_controller=config.basket_controller,
    )

    pipeline = WalletDiscoveryPipeline(config)

    assert isinstance(pipeline.source, CapturingSource)
    assert observed["market_ids"] == ["cond_1", "cond_2"]


def test_pipeline_uses_curated_wallet_file_source_when_configured(tmp_path) -> None:
    candidate_path = tmp_path / "wallets.json"
    candidate_path.write_text(
        json.dumps(
            [
                {"wallet": "0xcurated1", "source": "polydata"},
                {"wallet": "0xcurated2", "source": "polymonit"},
            ]
        ),
        encoding="utf-8",
    )

    config = make_config()
    config = AppConfig(
        baskets=config.baskets,
        filters=config.filters,
        arbitrage=config.arbitrage,
        wallet_trades_path=config.wallet_trades_path,
        market_snapshots_path=config.market_snapshots_path,
        live_data=config.live_data,
        execution=config.execution,
        consensus=config.consensus,
        market_regime=config.market_regime,
        wallet_discovery=WalletDiscoveryConfig(
            enabled=config.wallet_discovery.enabled,
            mode=config.wallet_discovery.mode,
            source="curated_wallet_file",
            wallet_candidates_path=str(candidate_path),
            candidate_limit=config.wallet_discovery.candidate_limit,
            trade_limit_per_wallet=config.wallet_discovery.trade_limit_per_wallet,
            min_trades=config.wallet_discovery.min_trades,
            min_recent_trades=config.wallet_discovery.min_recent_trades,
            min_history_days=config.wallet_discovery.min_history_days,
            recent_window_seconds=config.wallet_discovery.recent_window_seconds,
            min_avg_trade_size_usd=config.wallet_discovery.min_avg_trade_size_usd,
            min_assignment_score=config.wallet_discovery.min_assignment_score,
            exclude_existing_wallets=config.wallet_discovery.exclude_existing_wallets,
            max_wallets_per_basket=config.wallet_discovery.max_wallets_per_basket,
            max_new_wallets_per_run=config.wallet_discovery.max_new_wallets_per_run,
            topics=config.wallet_discovery.topics,
        ),
        wallet_registry=config.wallet_registry,
        basket_controller=config.basket_controller,
    )

    pipeline = WalletDiscoveryPipeline(config)

    assert isinstance(pipeline.source, CuratedWalletFileSource)
    assert [
        candidate["address"] for candidate in pipeline.source.fetch_candidates(10)
    ] == [
        "0xcurated1",
        "0xcurated2",
    ]


def test_pipeline_raises_when_trade_fetch_failures_exceed_threshold() -> None:
    pipeline = make_pipeline()

    class FailingSource(FakeSource):
        def fetch_wallet_trades(self, address: str, limit: int):
            del address, limit
            raise RuntimeError("upstream down")

    pipeline.source = FailingSource()

    with pytest.raises(
        RuntimeError,
        match="wallet discovery trade fetch failures exceeded threshold",
    ):
        pipeline.run()

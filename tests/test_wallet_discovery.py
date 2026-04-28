import json

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
from predictcel.wallet_sources import DataApiMarketTradesWalletSource


class FakeSource:
    def fetch_candidates(self, limit: int):
        return [
            {"address": "0xnew", "source": "fake"},
            {"address": "0xexisting", "source": "fake"},
        ]

    def fetch_wallet_trades(self, address: str, limit: int):
        if address == "0xnew":
            return [
                {"question": "NBA finals winner", "size": "20", "createdAt": "2999-01-01T00:00:00Z"},
                {"question": "NFL playoff winner", "size": "30", "createdAt": "2999-01-01T00:00:00Z"},
                {"question": "NBA MVP", "size": "40", "createdAt": "2999-01-01T00:00:00Z"},
            ]
        if address == "0xexisting":
            return [
                {"question": "NBA regular season wins", "size": "18", "createdAt": "2999-01-01T00:00:00Z"},
                {"question": "NFL draft pick", "size": "22", "createdAt": "2999-01-01T00:00:00Z"},
                {"question": "NBA playoff seed", "size": "28", "createdAt": "2999-01-01T00:00:00Z"},
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
        live_data=LiveDataConfig(False, "https://gamma-api.polymarket.com", "https://data-api.polymarket.com", "https://clob.polymarket.com", 10, 10, 15),
        execution=ExecutionConfig(False, True, 0.7, 1, 10.0, 0.02, "FOK", 137, 0, PositionConfig(0.3, 0.1, 1440), ExposureConfig(100, 10), 3, 1.0),
        consensus=ConsensusConfig(),
        wallet_discovery=discovery,
    )


def make_pipeline(mode: str = "auto_update") -> WalletDiscoveryPipeline:
    pipeline = WalletDiscoveryPipeline(make_config(mode))
    pipeline.source = FakeSource()
    pipeline.assignment_engine = BasketAssignmentEngine(pipeline.config.wallet_discovery)
    pipeline.manager = BasketManagerPlanner(pipeline.config)
    return pipeline


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
    new_assignment = next(assignment for assignment in assignments if assignment.wallet_address == "0xnew")
    assert new_assignment.recommended_baskets == ["sports"]
    assert any(action.action == "add" and action.wallet_address == "0xnew" for action in actions)


def test_pipeline_still_evaluates_existing_wallets_for_manager_actions() -> None:
    pipeline = make_pipeline()

    _, assignments, _ = pipeline.run()

    assert any(assignment.wallet_address == "0xexisting" for assignment in assignments)


def test_auto_update_mutates_config_path_by_default(tmp_path) -> None:
    config_path = tmp_path / "predictcel.json"
    config_path.write_text(json.dumps({"baskets": [{"topic": "sports", "wallets": ["0xexisting"], "quorum_ratio": 0.66}]}), encoding="utf-8")
    pipeline = make_pipeline("auto_update")

    reports = pipeline.write_reports(tmp_path / "reports", config_path)
    updated = json.loads(config_path.read_text(encoding="utf-8"))

    assert reports["updated_config"] == str(config_path)
    assert updated["baskets"][0]["wallets"] == ["0xexisting", "0xnew"]


def test_propose_config_writes_separate_proposal(tmp_path) -> None:
    config_path = tmp_path / "predictcel.json"
    config_path.write_text(json.dumps({"baskets": [{"topic": "sports", "wallets": ["0xexisting"], "quorum_ratio": 0.66}]}), encoding="utf-8")
    pipeline = make_pipeline("propose_config")

    reports = pipeline.write_reports(tmp_path / "reports", config_path)
    original = json.loads(config_path.read_text(encoding="utf-8"))
    proposed = json.loads((tmp_path / "reports" / "predictcel.proposed.json").read_text(encoding="utf-8"))

    assert reports["config_proposal"] == str(tmp_path / "reports" / "predictcel.proposed.json")
    assert original["baskets"][0]["wallets"] == ["0xexisting"]
    assert proposed["baskets"][0]["wallets"] == ["0xexisting", "0xnew"]


def test_build_mutated_config_removes_wallets_for_remove_and_suspend_actions() -> None:
    pipeline = make_pipeline()
    payload = {
        "baskets": [
            {"topic": "sports", "wallets": ["0xexisting", "0xsuspend", "0xremove"], "quorum_ratio": 0.66}
        ]
    }
    actions = [
        BasketManagerAction("suspend", "sports", "0xSuspend", 0.3, "LOW", "suspend it"),
        BasketManagerAction("remove", "sports", "0xREMOVE", 0.2, "LOW", "remove it"),
    ]

    mutated = pipeline.build_mutated_config(payload, actions)

    assert mutated["baskets"][0]["wallets"] == ["0xexisting"]


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

    monkeypatch.setattr("predictcel.wallet_discovery.DataApiMarketTradesWalletSource", CapturingSource)
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

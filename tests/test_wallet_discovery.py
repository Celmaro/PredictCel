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


def test_pipeline_filters_existing_wallets_and_assigns_new_candidate() -> None:
    pipeline = make_pipeline()

    candidates, assignments, actions = pipeline.run()

    assert [candidate.wallet_address for candidate in candidates] == ["0xnew"]
    assert isinstance(candidates[0], WalletDiscoveryCandidate)
    assert candidates[0].rejected_reasons == []
    assert assignments[0].recommended_baskets == ["sports"]
    assert actions[0].action == "add"


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

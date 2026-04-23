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
from predictcel.models import WalletDiscoveryCandidate, WalletTopicProfile


def make_config() -> AppConfig:
    discovery = WalletDiscoveryConfig(
        min_assignment_score=0.5,
        max_wallets_per_basket=3,
        max_new_wallets_per_run=1,
        topics={"sports": ["nba"], "crypto": ["btc"]},
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


def make_candidate(score: float = 0.8) -> WalletDiscoveryCandidate:
    return WalletDiscoveryCandidate(
        wallet_address="0xabc",
        source="test",
        total_trades=30,
        recent_trades=10,
        avg_trade_size_usd=20.0,
        topic_profile=WalletTopicProfile({"sports": 0.8, "crypto": 0.2}, "sports", 0.68),
        score=score,
        confidence="HIGH" if score >= 0.7 else "MEDIUM",
        rejected_reasons=[],
    )


def test_assignment_recommends_matching_baskets() -> None:
    assignment = BasketAssignmentEngine(make_config().wallet_discovery).assign(make_candidate())

    assert assignment.primary_topic == "sports"
    assert assignment.recommended_baskets == ["sports", "crypto"]
    assert assignment.confidence == "HIGH"


def test_manager_proposes_add_in_report_only_mode() -> None:
    config = make_config()
    assignment = BasketAssignmentEngine(config.wallet_discovery).assign(make_candidate())

    actions = BasketManagerPlanner(config).plan([assignment])

    assert actions[0].action == "add"
    assert actions[0].basket == "sports"
    assert "manual approval" in actions[0].reason


def test_manager_observes_low_score_assignment() -> None:
    config = make_config()
    assignment = BasketAssignmentEngine(config.wallet_discovery).assign(make_candidate(score=0.4))

    actions = BasketManagerPlanner(config).plan([assignment])

    assert actions[0].action == "observe"

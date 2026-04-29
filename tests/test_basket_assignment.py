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
from predictcel.models import (
    BasketAssignment,
    WalletDiscoveryCandidate,
    WalletTopicProfile,
)


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


def make_candidate(score: float = 0.8) -> WalletDiscoveryCandidate:
    return WalletDiscoveryCandidate(
        wallet_address="0xabc",
        source="test",
        total_trades=30,
        recent_trades=10,
        avg_trade_size_usd=20.0,
        topic_profile=WalletTopicProfile(
            {"sports": 0.8, "crypto": 0.2}, "sports", 0.68
        ),
        score=score,
        confidence="HIGH" if score >= 0.7 else "MEDIUM",
        rejected_reasons=[],
        sample_score=0.9,
        recency_score=0.8,
        activity_score=1.0,
        size_band_score=0.85,
    )


def test_assignment_recommends_matching_baskets() -> None:
    assignment = BasketAssignmentEngine(make_config().wallet_discovery).assign(
        make_candidate()
    )

    assert assignment.primary_topic == "sports"
    assert assignment.recommended_baskets == ["sports", "crypto"]
    assert assignment.confidence == "HIGH"
    assert "activity_score=1.0000" in assignment.reasons
    assert "size_band_score=0.8500" in assignment.reasons


def test_manager_proposes_add_in_auto_update_mode() -> None:
    config = make_config()
    assignment = BasketAssignmentEngine(config.wallet_discovery).assign(
        make_candidate()
    )

    actions = BasketManagerPlanner(config).plan([assignment])

    assert actions[0].action == "add"
    assert actions[0].basket == "sports"
    assert "auto-update" in actions[0].reason


def test_manager_observes_low_score_assignment() -> None:
    config = make_config()
    assignment = BasketAssignmentEngine(config.wallet_discovery).assign(
        make_candidate(score=0.4)
    )

    actions = BasketManagerPlanner(config).plan([assignment])

    assert actions[0].action == "observe"


def test_manager_observes_borderline_new_assignment_below_promotion_buffer() -> None:
    config = make_config()
    assignment = BasketAssignment(
        wallet_address="0xborderline",
        primary_topic="sports",
        recommended_baskets=["sports"],
        topic_affinities={"sports": 1.0},
        overall_score=0.53,
        confidence="MEDIUM",
        reasons=[],
    )

    actions = BasketManagerPlanner(config).plan([assignment])

    assert actions[0].action == "observe"
    assert "promotion quality buffer" in actions[0].reason


def test_manager_suspends_existing_wallet_when_score_degrades() -> None:
    config = make_config()
    assignment = BasketAssignment(
        wallet_address="0xexisting",
        primary_topic="sports",
        recommended_baskets=["sports"],
        topic_affinities={"sports": 1.0},
        overall_score=0.45,
        confidence="LOW",
        reasons=[],
    )

    actions = BasketManagerPlanner(config).plan([assignment])

    assert actions[0].action == "suspend"
    assert actions[0].basket == "sports"


def test_manager_removes_existing_wallet_when_topic_drift_is_severe() -> None:
    config = make_config()
    assignment = BasketAssignment(
        wallet_address="0xexisting",
        primary_topic="crypto",
        recommended_baskets=["crypto"],
        topic_affinities={"crypto": 1.0},
        overall_score=0.3,
        confidence="LOW",
        reasons=[],
    )

    actions = BasketManagerPlanner(config).plan([assignment])

    assert actions[0].action == "remove"
    assert actions[0].basket == "sports"


def test_manager_uses_registry_backed_current_wallets_when_provided() -> None:
    config = make_config()
    assignment = BasketAssignment(
        wallet_address="0xregistry",
        primary_topic="sports",
        recommended_baskets=["sports"],
        topic_affinities={"sports": 1.0},
        overall_score=0.45,
        confidence="LOW",
        reasons=[],
    )

    actions = BasketManagerPlanner(
        config,
        current_wallets_by_topic={"sports": {"0xregistry"}},
    ).plan([assignment])

    assert actions[0].action == "suspend"
    assert actions[0].basket == "sports"

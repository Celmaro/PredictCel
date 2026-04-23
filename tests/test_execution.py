from predictcel.config import ExecutionConfig, LiveDataConfig
from predictcel.execution import ExecutionPlanner, LiveOrderExecutor
from predictcel.models import CopyCandidate, MarketSnapshot


def make_execution_config() -> ExecutionConfig:
    return ExecutionConfig(
        enabled=True,
        dry_run=True,
        min_copyability_score=0.72,
        max_orders_per_run=2,
        buy_amount_usd=25.0,
        worst_price_buffer=0.02,
        order_type="FOK",
        chain_id=137,
        signature_type=0,
    )


def make_live_data() -> LiveDataConfig:
    return LiveDataConfig(
        enabled=True,
        gamma_base_url="https://gamma-api.polymarket.com",
        data_base_url="https://data-api.polymarket.com",
        clob_base_url="https://clob.polymarket.com",
        market_limit=100,
        trade_limit=10,
        request_timeout_seconds=15,
    )


def test_execution_planner_selects_top_copyable_markets() -> None:
    planner = ExecutionPlanner(make_execution_config())
    candidates = [
        CopyCandidate(
            topic="geopolitics",
            market_id="m1",
            side="YES",
            consensus_ratio=0.67,
            reference_price=0.58,
            current_price=0.6,
            liquidity_usd=12000,
            source_wallets=["w1", "w2"],
            wallet_quality_score=0.8,
            copyability_score=0.83,
            reason="ok",
        ),
        CopyCandidate(
            topic="sports",
            market_id="m2",
            side="NO",
            consensus_ratio=1.0,
            reference_price=0.41,
            current_price=0.43,
            liquidity_usd=15000,
            source_wallets=["w3", "w4", "w5"],
            wallet_quality_score=0.9,
            copyability_score=0.79,
            reason="ok",
        ),
        CopyCandidate(
            topic="sports",
            market_id="m3",
            side="YES",
            consensus_ratio=0.67,
            reference_price=0.51,
            current_price=0.52,
            liquidity_usd=9000,
            source_wallets=["w6", "w7"],
            wallet_quality_score=0.75,
            copyability_score=0.6,
            reason="too weak",
        ),
    ]
    markets = {
        "m1": MarketSnapshot(
            market_id="m1",
            topic="geopolitics",
            title="One",
            yes_ask=0.6,
            no_ask=0.39,
            best_bid=0.58,
            liquidity_usd=12000,
            minutes_to_resolution=180,
            yes_token_id="yes_1",
            no_token_id="no_1",
            yes_ask_size=100,
            no_ask_size=80,
            orderbook_ready=True,
        ),
        "m2": MarketSnapshot(
            market_id="m2",
            topic="sports",
            title="Two",
            yes_ask=0.55,
            no_ask=0.43,
            best_bid=0.53,
            liquidity_usd=15000,
            minutes_to_resolution=240,
            yes_token_id="yes_2",
            no_token_id="no_2",
            yes_ask_size=90,
            no_ask_size=70,
            orderbook_ready=True,
        ),
        "m3": MarketSnapshot(
            market_id="m3",
            topic="sports",
            title="Three",
            yes_ask=0.52,
            no_ask=0.47,
            best_bid=0.5,
            liquidity_usd=9000,
            minutes_to_resolution=150,
            yes_token_id="yes_3",
            no_token_id="no_3",
            yes_ask_size=20,
            no_ask_size=20,
            orderbook_ready=True,
        ),
    }

    intents = planner.plan(candidates, markets)

    assert len(intents) == 2
    assert intents[0].market_id == "m1"
    assert intents[0].token_id == "yes_1"
    assert intents[0].worst_price == 0.62
    assert intents[1].market_id == "m2"
    assert intents[1].token_id == "no_2"


def test_execution_planner_skips_missing_depth_or_token() -> None:
    planner = ExecutionPlanner(make_execution_config())
    candidates = [
        CopyCandidate(
            topic="geopolitics",
            market_id="m1",
            side="YES",
            consensus_ratio=0.67,
            reference_price=0.58,
            current_price=0.6,
            liquidity_usd=12000,
            source_wallets=["w1", "w2"],
            wallet_quality_score=0.8,
            copyability_score=0.83,
            reason="ok",
        )
    ]
    markets = {
        "m1": MarketSnapshot(
            market_id="m1",
            topic="geopolitics",
            title="One",
            yes_ask=0.6,
            no_ask=0.39,
            best_bid=0.58,
            liquidity_usd=12000,
            minutes_to_resolution=180,
            yes_token_id="",
            no_token_id="no_1",
            yes_ask_size=10,
            no_ask_size=80,
            orderbook_ready=True,
        )
    }

    intents = planner.plan(candidates, markets)

    assert intents == []


def test_live_order_executor_returns_dry_run_results() -> None:
    executor = LiveOrderExecutor(make_execution_config(), make_live_data())
    intents = [
        planner_intent for planner_intent in ExecutionPlanner(make_execution_config()).plan(
            [
                CopyCandidate(
                    topic="geopolitics",
                    market_id="m1",
                    side="YES",
                    consensus_ratio=0.67,
                    reference_price=0.58,
                    current_price=0.6,
                    liquidity_usd=12000,
                    source_wallets=["w1", "w2"],
                    wallet_quality_score=0.8,
                    copyability_score=0.83,
                    reason="ok",
                )
            ],
            {
                "m1": MarketSnapshot(
                    market_id="m1",
                    topic="geopolitics",
                    title="One",
                    yes_ask=0.6,
                    no_ask=0.39,
                    best_bid=0.58,
                    liquidity_usd=12000,
                    minutes_to_resolution=180,
                    yes_token_id="yes_1",
                    no_token_id="no_1",
                    yes_ask_size=100,
                    no_ask_size=80,
                    orderbook_ready=True,
                )
            },
        )
    ]

    results = executor.execute(intents)

    assert len(results) == 1
    assert results[0].status == "dry_run"
    assert results[0].order_id == ""
    assert results[0].error == ""

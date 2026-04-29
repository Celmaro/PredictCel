from dataclasses import replace
from datetime import UTC, datetime, timedelta

from predictcel.config import (
    ExecutionConfig,
    ExposureConfig,
    LiveDataConfig,
    PositionConfig,
)
from predictcel.execution import (
    ExecutionPlanner,
    ExitRunner,
    LiveOrderExecutor,
    retry_delay,
)
from predictcel.models import CopyCandidate, ExecutionIntent, MarketSnapshot, Position


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
        position=PositionConfig(
            take_profit_pct=0.3, stop_loss_pct=0.1, max_hold_minutes=1440
        ),
        exposure=ExposureConfig(
            max_total_exposure_usd=75.0, max_single_position_usd=50.0
        ),
        max_retries=3,
        retry_base_delay_seconds=1.0,
        min_signal_allocation_usd=5.0,
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
    config = make_execution_config()
    planner = ExecutionPlanner(config, config.position)
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

    intents = planner.plan(
        candidates, markets, held_market_ids=set(), current_exposure_usd=0.0
    )

    assert len(intents) == 2
    assert intents[0].market_id == "m1"
    assert intents[0].market_title == "One"
    assert intents[0].token_id == "yes_1"
    assert intents[0].worst_price == 0.62
    assert intents[0].amount_usd == 25.0
    assert intents[1].market_id == "m2"
    assert intents[1].market_title == "Two"
    assert intents[1].token_id == "no_2"


def test_execution_planner_uses_suggested_position_size_with_caps() -> None:
    config = make_execution_config()
    planner = ExecutionPlanner(config, config.position)
    candidates = [
        CopyCandidate(
            "topic",
            "m1",
            "YES",
            1.0,
            0.5,
            0.51,
            10000,
            ["w1"],
            1.0,
            0.9,
            "ok",
            suggested_position_usd=60.0,
        ),
        CopyCandidate(
            "topic",
            "m2",
            "YES",
            1.0,
            0.5,
            0.51,
            10000,
            ["w2"],
            1.0,
            0.89,
            "ok",
            suggested_position_usd=40.0,
        ),
    ]
    markets = {
        "m1": MarketSnapshot(
            "m1",
            "topic",
            "One",
            0.51,
            0.48,
            0.5,
            10000,
            180,
            yes_token_id="yes_1",
            yes_ask_size=200,
            orderbook_ready=True,
        ),
        "m2": MarketSnapshot(
            "m2",
            "topic",
            "Two",
            0.51,
            0.48,
            0.5,
            10000,
            180,
            yes_token_id="yes_2",
            yes_ask_size=200,
            orderbook_ready=True,
        ),
    }

    intents = planner.plan(
        candidates, markets, held_market_ids=set(), current_exposure_usd=10.0
    )

    assert [intent.amount_usd for intent in intents] == [25.0, 25.0]


def test_execution_planner_applies_minimum_signal_allocation() -> None:
    config = make_execution_config()
    planner = ExecutionPlanner(config, config.position)
    candidates = [
        CopyCandidate(
            "topic",
            "m1",
            "YES",
            1.0,
            0.5,
            0.51,
            10000,
            ["w1"],
            1.0,
            0.9,
            "ok",
            suggested_position_usd=1.25,
        ),
    ]
    markets = {
        "m1": MarketSnapshot(
            "m1",
            "topic",
            "One",
            0.51,
            0.48,
            0.5,
            10000,
            180,
            yes_token_id="yes_1",
            yes_ask_size=200,
            orderbook_ready=True,
        ),
    }

    intents = planner.plan(
        candidates, markets, held_market_ids=set(), current_exposure_usd=0.0
    )

    assert [intent.amount_usd for intent in intents] == [5.0]


def test_execution_planned_amount_clamps_negative_amounts_to_zero() -> None:
    config = replace(
        make_execution_config(),
        buy_amount_usd=-10.0,
        min_signal_allocation_usd=-5.0,
    )
    planner = ExecutionPlanner(config, config.position)
    candidate = CopyCandidate(
        "topic",
        "m1",
        "YES",
        1.0,
        0.5,
        0.51,
        10000,
        ["w1"],
        1.0,
        0.9,
        "ok",
        suggested_position_usd=-1.0,
    )

    assert planner._planned_amount_usd(candidate, planned_exposure_usd=0.0) == 0.0


def test_execution_planner_respects_exposure_across_planned_orders() -> None:
    config = make_execution_config()
    planner = ExecutionPlanner(config, config.position)
    candidates = [
        CopyCandidate(
            "topic", "m1", "YES", 1.0, 0.5, 0.51, 10000, ["w1"], 1.0, 0.9, "ok"
        ),
        CopyCandidate(
            "topic", "m2", "YES", 1.0, 0.5, 0.51, 10000, ["w2"], 1.0, 0.89, "ok"
        ),
    ]
    markets = {
        "m1": MarketSnapshot(
            "m1",
            "topic",
            "One",
            0.51,
            0.48,
            0.5,
            10000,
            180,
            yes_token_id="yes_1",
            yes_ask_size=100,
            orderbook_ready=True,
        ),
        "m2": MarketSnapshot(
            "m2",
            "topic",
            "Two",
            0.51,
            0.48,
            0.5,
            10000,
            180,
            yes_token_id="yes_2",
            yes_ask_size=100,
            orderbook_ready=True,
        ),
    }

    intents = planner.plan(
        candidates, markets, held_market_ids=set(), current_exposure_usd=50.0
    )

    assert [intent.market_id for intent in intents] == ["m1"]


def test_execution_planner_skips_missing_depth_or_token() -> None:
    config = make_execution_config()
    planner = ExecutionPlanner(config, config.position)
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

    intents = planner.plan(
        candidates, markets, held_market_ids=set(), current_exposure_usd=0.0
    )

    assert intents == []
    assert planner.last_diagnostics["missing_token_id"] == 1
    assert planner.last_diagnostics["candidates_planned"] == 0


def test_execution_planner_reports_skip_reasons() -> None:
    config = make_execution_config()
    planner = ExecutionPlanner(config, config.position)
    candidates = [
        CopyCandidate(
            "topic", "held", "YES", 1.0, 0.5, 0.51, 10000, ["w1"], 1.0, 0.9, "ok"
        ),
        CopyCandidate(
            "topic", "no_book", "YES", 1.0, 0.5, 0.51, 10000, ["w2"], 1.0, 0.9, "ok"
        ),
        CopyCandidate(
            "topic", "resolving", "YES", 1.0, 0.5, 0.51, 10000, ["w3"], 1.0, 0.9, "ok"
        ),
        CopyCandidate(
            "topic",
            "shallow",
            "YES",
            1.0,
            0.5,
            0.51,
            10000,
            ["w4"],
            1.0,
            0.9,
            "ok",
            suggested_position_usd=10.0,
        ),
        CopyCandidate(
            "topic", "late", "YES", 1.0, 0.5, 0.96, 10000, ["w5"], 1.0, 0.9, "ok"
        ),
        CopyCandidate(
            "topic", "good", "YES", 1.0, 0.5, 0.51, 10000, ["w6"], 1.0, 0.9, "ok"
        ),
    ]
    markets = {
        "held": MarketSnapshot(
            "held",
            "topic",
            "Held",
            0.51,
            0.48,
            0.5,
            10000,
            180,
            yes_token_id="yes_held",
            yes_ask_size=200,
            orderbook_ready=True,
        ),
        "no_book": MarketSnapshot(
            "no_book",
            "topic",
            "No Book",
            0.51,
            0.48,
            0.5,
            10000,
            180,
            yes_token_id="yes_no_book",
            yes_ask_size=200,
            orderbook_ready=False,
        ),
        "resolving": MarketSnapshot(
            "resolving",
            "topic",
            "Resolving",
            0.51,
            0.48,
            0.5,
            10000,
            20,
            yes_token_id="yes_resolving",
            yes_ask_size=200,
            orderbook_ready=True,
        ),
        "shallow": MarketSnapshot(
            "shallow",
            "topic",
            "Shallow",
            0.51,
            0.48,
            0.5,
            10000,
            180,
            yes_token_id="yes_shallow",
            yes_ask_size=5,
            orderbook_ready=True,
        ),
        "late": MarketSnapshot(
            "late",
            "topic",
            "Late",
            0.96,
            0.03,
            0.95,
            10000,
            180,
            yes_token_id="yes_late",
            yes_ask_size=200,
            orderbook_ready=True,
        ),
        "good": MarketSnapshot(
            "good",
            "topic",
            "Good",
            0.51,
            0.48,
            0.5,
            10000,
            180,
            yes_token_id="yes_good",
            yes_ask_size=200,
            orderbook_ready=True,
        ),
    }

    intents = planner.plan(
        candidates, markets, held_market_ids={"held"}, current_exposure_usd=0.0
    )

    assert [intent.market_id for intent in intents] == ["good"]
    assert planner.last_diagnostics == {
        "candidates_seen": 6,
        "below_copyability_threshold": 0,
        "already_held": 1,
        "orderbook_not_ready": 1,
        "too_close_to_resolution": 1,
        "zero_amount": 0,
        "price_too_high": 1,
        "missing_token_id": 0,
        "insufficient_side_depth": 1,
        "candidates_planned": 1,
    }


def test_execution_planner_skips_markets_resolving_within_30_minutes() -> None:
    config = make_execution_config()
    planner = ExecutionPlanner(config, config.position)
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
            minutes_to_resolution=29,
            yes_token_id="yes_1",
            no_token_id="no_1",
            yes_ask_size=100,
            no_ask_size=80,
            orderbook_ready=True,
        )
    }

    intents = planner.plan(
        candidates, markets, held_market_ids=set(), current_exposure_usd=0.0
    )

    assert intents == []


def test_execution_planner_skips_late_price_entries() -> None:
    config = make_execution_config()
    planner = ExecutionPlanner(config, config.position)
    candidates = [
        CopyCandidate(
            topic="crypto",
            market_id="m1",
            side="YES",
            consensus_ratio=0.67,
            reference_price=0.54,
            current_price=0.96,
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
            topic="crypto",
            title="Late",
            yes_ask=0.96,
            no_ask=0.03,
            best_bid=0.95,
            liquidity_usd=12000,
            minutes_to_resolution=180,
            yes_token_id="yes_1",
            no_token_id="no_1",
            yes_ask_size=100,
            no_ask_size=80,
            orderbook_ready=True,
        )
    }

    intents = planner.plan(
        candidates, markets, held_market_ids=set(), current_exposure_usd=0.0
    )

    assert intents == []


def test_live_order_executor_returns_dry_run_results() -> None:
    config = make_execution_config()
    executor = LiveOrderExecutor(config, make_live_data())
    intents = [
        planner_intent
        for planner_intent in ExecutionPlanner(config, config.position).plan(
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
            held_market_ids=set(),
            current_exposure_usd=0.0,
        )
    ]

    results = executor.execute(intents)

    assert len(results) == 1
    assert results[0].market_title == "One"
    assert results[0].status == "dry_run"
    assert results[0].order_id == ""
    assert results[0].error == ""


def test_exit_runner_creates_close_intent_without_mutating_status() -> None:
    config = make_execution_config()
    runner = ExitRunner(config, make_live_data())
    opened_at = datetime.now(UTC) - timedelta(minutes=30)
    positions = [
        Position(
            market_id="m1",
            topic="geopolitics",
            side="YES",
            token_id="yes_1",
            entry_price=0.5,
            entry_amount_usd=25.0,
            current_price=0.5,
            unrealized_pnl=0.0,
            opened_at=opened_at,
            last_updated=opened_at,
            take_profit_pct=0.1,
            stop_loss_pct=0.1,
            max_hold_minutes=1440,
            status="open",
        )
    ]
    markets = {
        "m1": MarketSnapshot(
            market_id="m1",
            topic="geopolitics",
            title="One",
            yes_ask=0.56,
            no_ask=0.43,
            best_bid=0.54,
            liquidity_usd=12000,
            minutes_to_resolution=180,
            yes_token_id="yes_1",
            yes_bid=0.54,
            orderbook_ready=True,
        )
    }

    intents, updated = runner.evaluate_and_close(positions, markets)

    assert len(intents) == 1
    assert intents[0].market_title == "One"
    assert intents[0].side == "CLOSE"
    assert intents[0].token_id == "yes_1"
    assert updated[0].market_title == "One"
    assert updated[0].status == "open"
    assert updated[0].unrealized_pnl > 0


def test_live_executor_dry_run_preserves_close_side() -> None:
    executor = LiveOrderExecutor(make_execution_config(), make_live_data())
    results = executor.execute(
        [
            ExecutionIntent(
                market_id="m1",
                topic="geopolitics",
                side="CLOSE",
                token_id="yes_1",
                amount_usd=25.0,
                worst_price=0.54,
                copyability_score=0.0,
                order_type="FOK",
                reason="take profit",
                market_title="One",
            )
        ]
    )

    assert results[0].market_title == "One"
    assert results[0].side == "CLOSE"
    assert results[0].status == "dry_run"


def test_retry_delay_adds_bounded_jitter() -> None:
    for _ in range(20):
        delay = retry_delay(1.0, 2)
        assert 2.0 <= delay <= 6.0

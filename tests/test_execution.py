import sys
import types
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import time

import pytest

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


class RecordingStore:
    def __init__(self) -> None:
        self.records: list[dict[str, object]] = []

    def upsert_open_order(self, **kwargs) -> None:
        self.records.append(dict(kwargs))


def install_fake_clob_modules(monkeypatch) -> None:
    package = types.ModuleType("py_clob_client")
    clob_types = types.ModuleType("py_clob_client.clob_types")
    constants = types.ModuleType("py_clob_client.order_builder.constants")

    class FakeMarketOrderArgs:
        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    class FakeOrderType:
        FOK = "FOK"
        FAK = "FAK"

    clob_types.MarketOrderArgs = FakeMarketOrderArgs
    clob_types.OrderType = FakeOrderType
    constants.BUY = "BUY"
    constants.SELL = "SELL"
    monkeypatch.setitem(sys.modules, "py_clob_client", package)
    monkeypatch.setitem(sys.modules, "py_clob_client.clob_types", clob_types)
    monkeypatch.setitem(
        sys.modules,
        "py_clob_client.order_builder.constants",
        constants,
    )


def install_fake_clob_client_module(
    monkeypatch,
    client_cls,
    *,
    api_creds_cls=None,
) -> None:
    package = types.ModuleType("py_clob_client")
    client_module = types.ModuleType("py_clob_client.client")
    clob_types = sys.modules.get("py_clob_client.clob_types") or types.ModuleType(
        "py_clob_client.clob_types"
    )
    client_module.ClobClient = client_cls
    if api_creds_cls is not None:
        clob_types.ApiCreds = api_creds_cls
    monkeypatch.setitem(sys.modules, "py_clob_client", package)
    monkeypatch.setitem(sys.modules, "py_clob_client.client", client_module)
    monkeypatch.setitem(sys.modules, "py_clob_client.clob_types", clob_types)


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


def test_execution_planner_honors_env_entry_threshold_overrides(monkeypatch) -> None:
    monkeypatch.setenv("PREDICTCEL_MIN_MINUTES_TO_RESOLUTION", "10")
    monkeypatch.setenv("PREDICTCEL_MAX_ENTRY_PRICE", "0.97")
    config = make_execution_config()
    planner = ExecutionPlanner(config, config.position)
    candidates = [
        CopyCandidate(
            topic="geopolitics",
            market_id="m1",
            side="YES",
            consensus_ratio=0.67,
            reference_price=0.95,
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
            topic="geopolitics",
            title="One",
            yes_ask=0.96,
            no_ask=0.03,
            best_bid=0.95,
            liquidity_usd=12000,
            minutes_to_resolution=20,
            yes_token_id="yes_1",
            no_token_id="no_1",
            yes_ask_size=100,
            orderbook_ready=True,
        )
    }

    intents = planner.plan(
        candidates, markets, held_market_ids=set(), current_exposure_usd=0.0
    )

    assert [intent.market_id for intent in intents] == ["m1"]


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
    assert intents[0].amount_usd == 27.0
    assert updated[0].market_title == "One"
    assert updated[0].status == "open"
    assert updated[0].unrealized_pnl > 0


def test_exit_runner_uses_remaining_shares_for_close_sizing() -> None:
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
            entry_shares=50.0,
            remaining_shares=10.0,
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

    intents, _updated = runner.evaluate_and_close(positions, markets)

    assert len(intents) == 1
    assert intents[0].amount_usd == 5.4


def test_exit_runner_handles_multiple_positions_with_mixed_close_reasons() -> None:
    config = make_execution_config()
    runner = ExitRunner(config, make_live_data())
    now = datetime.now(UTC)
    positions = [
        Position(
            market_id="m_tp",
            topic="geopolitics",
            side="YES",
            token_id="yes_tp",
            entry_price=0.5,
            entry_amount_usd=25.0,
            current_price=0.5,
            unrealized_pnl=0.0,
            opened_at=now - timedelta(minutes=30),
            last_updated=now - timedelta(minutes=30),
            take_profit_pct=0.1,
            stop_loss_pct=0.1,
            max_hold_minutes=1440,
            status="open",
            entry_shares=50.0,
            remaining_shares=50.0,
        ),
        Position(
            market_id="m_sl",
            topic="geopolitics",
            side="NO",
            token_id="no_sl",
            entry_price=0.6,
            entry_amount_usd=20.0,
            current_price=0.6,
            unrealized_pnl=0.0,
            opened_at=now - timedelta(minutes=20),
            last_updated=now - timedelta(minutes=20),
            take_profit_pct=0.2,
            stop_loss_pct=0.1,
            max_hold_minutes=1440,
            status="open",
            entry_shares=40.0,
            remaining_shares=40.0,
        ),
        Position(
            market_id="m_hold",
            topic="sports",
            side="YES",
            token_id="yes_hold",
            entry_price=0.45,
            entry_amount_usd=18.0,
            current_price=0.45,
            unrealized_pnl=0.0,
            opened_at=now - timedelta(minutes=120),
            last_updated=now - timedelta(minutes=120),
            take_profit_pct=0.5,
            stop_loss_pct=0.5,
            max_hold_minutes=60,
            status="open",
            entry_shares=40.0,
            remaining_shares=20.0,
        ),
        Position(
            market_id="m_soon",
            topic="sports",
            side="YES",
            token_id="yes_soon",
            entry_price=0.52,
            entry_amount_usd=15.0,
            current_price=0.52,
            unrealized_pnl=0.0,
            opened_at=now - timedelta(minutes=10),
            last_updated=now - timedelta(minutes=10),
            take_profit_pct=0.5,
            stop_loss_pct=0.5,
            max_hold_minutes=1440,
            status="open",
            entry_shares=30.0,
            remaining_shares=30.0,
        ),
        Position(
            market_id="m_missing",
            topic="sports",
            side="YES",
            token_id="yes_missing",
            entry_price=0.5,
            entry_amount_usd=10.0,
            current_price=0.5,
            unrealized_pnl=0.0,
            opened_at=now - timedelta(minutes=15),
            last_updated=now - timedelta(minutes=15),
            take_profit_pct=0.1,
            stop_loss_pct=0.1,
            max_hold_minutes=1440,
            status="open",
        ),
    ]
    markets = {
        "m_tp": MarketSnapshot(
            market_id="m_tp",
            topic="geopolitics",
            title="Take Profit",
            yes_ask=0.56,
            no_ask=0.43,
            best_bid=0.55,
            liquidity_usd=12000,
            minutes_to_resolution=180,
            yes_token_id="yes_tp",
            yes_bid=0.54,
            orderbook_ready=True,
        ),
        "m_sl": MarketSnapshot(
            market_id="m_sl",
            topic="geopolitics",
            title="Stop Loss",
            yes_ask=0.52,
            no_ask=0.5,
            best_bid=0.5,
            liquidity_usd=12000,
            minutes_to_resolution=180,
            no_token_id="no_sl",
            no_bid=0.48,
            orderbook_ready=True,
        ),
        "m_hold": MarketSnapshot(
            market_id="m_hold",
            topic="sports",
            title="Max Hold",
            yes_ask=0.46,
            no_ask=0.5,
            best_bid=0.45,
            liquidity_usd=12000,
            minutes_to_resolution=180,
            yes_token_id="yes_hold",
            yes_bid=0.44,
            orderbook_ready=True,
        ),
        "m_soon": MarketSnapshot(
            market_id="m_soon",
            topic="sports",
            title="Resolving Soon",
            yes_ask=0.53,
            no_ask=0.44,
            best_bid=0.52,
            liquidity_usd=12000,
            minutes_to_resolution=5,
            yes_token_id="yes_soon",
            yes_bid=0.51,
            orderbook_ready=True,
        ),
    }

    intents, updated = runner.evaluate_and_close(positions, markets)

    assert [intent.market_id for intent in intents] == [
        "m_tp",
        "m_sl",
        "m_hold",
        "m_soon",
    ]
    assert [intent.side for intent in intents] == [
        "CLOSE",
        "CLOSE",
        "CLOSE",
        "CLOSE",
    ]
    assert intents[0].amount_usd == 27.0
    assert intents[1].amount_usd == 19.2
    assert intents[2].amount_usd == 8.8
    assert intents[3].amount_usd == 15.3
    assert len(updated) == 5
    assert next(
        position for position in updated if position.market_id == "m_missing"
    ) == positions[4]


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


def test_execution_result_normalizes_legacy_uppercase_status() -> None:
    result = replace(
        LiveOrderExecutor(make_execution_config(), make_live_data())._dry_run_result(
            ExecutionIntent(
                market_id="m1",
                topic="geopolitics",
                side="YES",
                token_id="yes_1",
                amount_usd=25.0,
                worst_price=0.54,
                copyability_score=0.9,
                order_type="FOK",
                reason="first",
                market_title="One",
            )
        ),
        status="SUCCESS",
    )

    assert result.status == "filled"


def test_position_normalizes_legacy_uppercase_status() -> None:
    opened_at = datetime.now(UTC) - timedelta(minutes=30)
    position = Position(
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
        take_profit_pct=0.3,
        stop_loss_pct=0.1,
        max_hold_minutes=1440,
        status="OPEN",
        market_title="One",
    )

    assert position.status == "open"


def test_live_order_executor_closes_clients_and_preserves_result_order(monkeypatch) -> None:
    config = replace(make_execution_config(), dry_run=False)
    executor = LiveOrderExecutor(config, make_live_data())
    built_clients = []

    class FakeClient:
        def __init__(self, index: int) -> None:
            self.index = index
            self.closed = False

        def close(self) -> None:
            self.closed = True

    def fake_build_client():
        client = FakeClient(len(built_clients))
        built_clients.append(client)
        return client

    def fake_submit_intent(client, intent):
        if intent.market_id == "m1":
            time.sleep(0.05)
        result = executor._dry_run_result(intent)
        return replace(result, order_id=str(client.index), status="submitted")

    monkeypatch.setattr(executor, "_build_client", fake_build_client)
    monkeypatch.setattr(executor, "_submit_intent", fake_submit_intent)

    intents = [
        ExecutionIntent(
            market_id="m1",
            topic="geopolitics",
            side="YES",
            token_id="yes_1",
            amount_usd=25.0,
            worst_price=0.54,
            copyability_score=0.9,
            order_type="FOK",
            reason="first",
            market_title="One",
        ),
        ExecutionIntent(
            market_id="m2",
            topic="sports",
            side="NO",
            token_id="no_2",
            amount_usd=20.0,
            worst_price=0.44,
            copyability_score=0.8,
            order_type="FOK",
            reason="second",
            market_title="Two",
        ),
    ]

    results = executor.execute(intents)

    assert [result.market_id for result in results] == ["m1", "m2"]
    assert [result.order_id for result in results] == ["0", "1"]
    assert len(built_clients) == 2
    assert all(client.closed for client in built_clients)


def test_build_client_uses_pre_generated_api_creds_from_env(monkeypatch) -> None:
    observed: dict[str, object] = {}

    class FakeApiCreds:
        def __init__(self, key: str, secret: str, passphrase: str) -> None:
            self.key = key
            self.secret = secret
            self.passphrase = passphrase

    class FakeClobClient:
        def __init__(
            self,
            host: str,
            *,
            key: str,
            chain_id: int,
            signature_type: int,
            funder: str,
        ) -> None:
            observed["init"] = {
                "host": host,
                "key": key,
                "chain_id": chain_id,
                "signature_type": signature_type,
                "funder": funder,
            }

        def set_api_creds(self, creds) -> None:
            observed["creds"] = creds

        def create_or_derive_api_creds(self):
            observed["derived"] = True
            return {"unexpected": True}

    install_fake_clob_client_module(
        monkeypatch,
        FakeClobClient,
        api_creds_cls=FakeApiCreds,
    )
    monkeypatch.setenv("PREDICTCEL_POLY_PRIVATE_KEY", "0xabc")
    monkeypatch.setenv("PREDICTCEL_POLY_FUNDER", "0xfunder")
    monkeypatch.setenv("POLY_API_KEY", "api-key")
    monkeypatch.setenv("POLY_API_SECRET", "api-secret")
    monkeypatch.setenv("POLY_API_PASSPHRASE", "api-pass")

    executor = LiveOrderExecutor(
        replace(make_execution_config(), dry_run=False),
        make_live_data(),
    )
    client = executor._build_client()

    assert client is not None
    assert observed["init"] == {
        "host": "https://clob.polymarket.com",
        "key": "0xabc",
        "chain_id": 137,
        "signature_type": 0,
        "funder": "0xfunder",
    }
    assert observed.get("derived") is None
    creds = observed["creds"]
    assert isinstance(creds, FakeApiCreds)
    assert creds.key == "api-key"
    assert creds.secret == "api-secret"
    assert creds.passphrase == "api-pass"


def test_build_client_requires_complete_pre_generated_api_creds(monkeypatch) -> None:
    class FakeClobClient:
        def __init__(self, *args, **kwargs) -> None:
            raise AssertionError("client should not be constructed")

    install_fake_clob_client_module(monkeypatch, FakeClobClient)
    monkeypatch.setenv("PREDICTCEL_POLY_PRIVATE_KEY", "0xabc")
    monkeypatch.setenv("PREDICTCEL_POLY_FUNDER", "0xfunder")
    monkeypatch.setenv("POLY_API_KEY", "api-key")
    monkeypatch.setenv("POLY_API_SECRET", "api-secret")
    monkeypatch.delenv("POLY_API_PASSPHRASE", raising=False)

    executor = LiveOrderExecutor(
        replace(make_execution_config(), dry_run=False),
        make_live_data(),
    )

    with pytest.raises(
        RuntimeError,
        match="POLY_API_KEY, POLY_API_SECRET, and POLY_API_PASSPHRASE",
    ):
        executor._build_client()


def test_live_order_executor_reconciles_retryable_submission_by_client_order_id(
    monkeypatch,
) -> None:
    install_fake_clob_modules(monkeypatch)
    store = RecordingStore()
    executor = LiveOrderExecutor(
        replace(make_execution_config(), dry_run=False),
        make_live_data(),
        store=store,
    )
    intent = ExecutionIntent(
        market_id="m1",
        topic="geopolitics",
        side="YES",
        token_id="yes_1",
        amount_usd=25.0,
        worst_price=0.5,
        copyability_score=0.9,
        order_type="FOK",
        reason="first",
        market_title="One",
    )
    expected_client_order_id = executor._client_order_id_for_intent(intent)

    class FakeClient:
        def __init__(self) -> None:
            self.post_calls = 0
            self.lookup_calls = 0

        def create_market_order(self, order_args):
            return {"args": order_args.__dict__}

        def post_order(self, _signed_order, _order_type):
            self.post_calls += 1
            raise RuntimeError("connection reset by peer")

        def get_order_by_client_order_id(self, client_order_id: str):
            self.lookup_calls += 1
            return {
                "client_order_id": client_order_id,
                "status": "matched",
                "orderID": "order-123",
                "avgPrice": 0.5,
                "filledShares": 50.0,
            }

    client = FakeClient()

    result = executor._submit_intent(client, intent)

    assert result.status == "filled"
    assert result.order_id == "order-123"
    assert result.client_order_id == expected_client_order_id
    assert result.filled_shares == 50.0
    assert client.post_calls == 1
    assert client.lookup_calls == 1
    assert store.records[-1]["status"] == "filled"
    assert store.records[-1]["client_order_id"] == expected_client_order_id


def test_live_order_executor_returns_pending_for_ambiguous_retry_without_lookup(
    monkeypatch,
) -> None:
    install_fake_clob_modules(monkeypatch)
    store = RecordingStore()
    executor = LiveOrderExecutor(
        replace(make_execution_config(), dry_run=False),
        make_live_data(),
        store=store,
    )
    intent = ExecutionIntent(
        market_id="m1",
        topic="geopolitics",
        side="YES",
        token_id="yes_1",
        amount_usd=25.0,
        worst_price=0.5,
        copyability_score=0.9,
        order_type="FOK",
        reason="first",
        market_title="One",
    )

    class FakeClient:
        def __init__(self) -> None:
            self.post_calls = 0

        def create_market_order(self, order_args):
            return {"args": order_args.__dict__}

        def post_order(self, _signed_order, _order_type):
            self.post_calls += 1
            raise RuntimeError("connection timed out")

    client = FakeClient()

    result = executor._submit_intent(client, intent)

    assert result.status == "pending"
    assert "ambiguous submission" in result.error
    assert client.post_calls == 1
    assert store.records[-1]["status"] == "pending"


def test_retry_delay_adds_bounded_jitter() -> None:
    for _ in range(20):
        delay = retry_delay(1.0, 2)
        assert 2.0 <= delay <= 6.0

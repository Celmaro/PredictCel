import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

from predictcel.config import load_config
from predictcel.cycle_runtime import CycleRuntimeHooks, run_loaded_cycle
from predictcel.models import (
    ArbitrageOpportunity,
    CopyCandidate,
    ExecutionIntent,
    ExecutionResult,
    MarketSnapshot,
    Position,
    WalletQuality,
    WalletTrade,
)


class FakeStore:
    def __init__(self, positions: list[Position]) -> None:
        self.positions = list(positions)
        self.saved_payloads = None
        self.updated_positions: list[dict[str, object]] = []

    def get_open_positions(self) -> list[Position]:
        return [
            position
            for position in self.positions
            if position.status in {"open", "closing"}
        ]

    def get_portfolio_var(self, confidence_level: float = 0.95) -> float:
        assert confidence_level == 0.95
        return 7.5

    def get_total_exposure(self) -> float:
        return sum(
            position.entry_amount_usd for position in self.get_open_positions()
        )

    def update_position(
        self,
        market_id: str,
        current_price: float,
        unrealized_pnl: float,
        status: str,
        *,
        token_id: str | None = None,
        remaining_shares: float | None = None,
    ) -> None:
        self.updated_positions.append(
            {
                "market_id": market_id,
                "token_id": token_id,
                "status": status,
                "remaining_shares": remaining_shares,
            }
        )
        refreshed: list[Position] = []
        for position in self.positions:
            if position.market_id != market_id or (
                token_id is not None and position.token_id != token_id
            ):
                refreshed.append(position)
                continue
            refreshed.append(
                replace(
                    position,
                    current_price=current_price,
                    unrealized_pnl=unrealized_pnl,
                    status=status,
                    remaining_shares=remaining_shares
                    if remaining_shares is not None
                    else position.remaining_shares,
                )
            )
        self.positions = refreshed

    def get_held_market_ids(self) -> set[str]:
        return {position.market_id for position in self.get_open_positions()}

    def save_cycle_payloads(
        self,
        copy_candidates,
        arbitrage_opportunities,
        execution_results,
    ) -> None:
        self.saved_payloads = {
            "copy_candidates": list(copy_candidates),
            "arbitrage_opportunities": list(arbitrage_opportunities),
            "execution_results": list(execution_results),
        }

    def prune_history(
        self,
        *,
        max_rows_per_table: int,
        analyze: bool,
        vacuum: bool,
    ) -> dict[str, int]:
        assert max_rows_per_table == 200
        assert analyze is True
        assert vacuum is False
        return {"copy_candidates": 0}


class FakeMetrics:
    def __init__(self) -> None:
        self.values: dict[str, int] = {}

    def set(self, key: str, value: int) -> None:
        self.values[key] = value

    def get_metrics(self) -> dict[str, int]:
        return dict(self.values)


def test_run_loaded_cycle_integrates_mocked_runtime_components(monkeypatch) -> None:
    base_config = load_config(Path("config/predictcel.example.json"))
    config = replace(
        base_config,
        execution=replace(base_config.execution, enabled=True, dry_run=False),
    )
    opened_at = datetime.now(UTC) - timedelta(minutes=90)
    existing_position = Position(
        market_id="m_open",
        topic="geopolitics",
        side="YES",
        token_id="yes_open",
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
        market_title="Open",
        entry_shares=50.0,
        remaining_shares=50.0,
    )
    store = FakeStore([existing_position])
    metrics = FakeMetrics()
    trades = [
        WalletTrade(
            wallet="w1",
            topic="geopolitics",
            market_id="m_new",
            side="YES",
            price=0.52,
            size_usd=25.0,
            age_seconds=30,
        )
    ]
    markets = {
        "m_open": MarketSnapshot(
            market_id="m_open",
            topic="geopolitics",
            title="Open",
            yes_ask=0.62,
            no_ask=0.35,
            best_bid=0.6,
            liquidity_usd=12000,
            minutes_to_resolution=180,
            yes_token_id="yes_open",
            no_token_id="no_open",
            yes_bid=0.6,
            yes_ask_size=200,
            orderbook_ready=True,
        ),
        "m_new": MarketSnapshot(
            market_id="m_new",
            topic="geopolitics",
            title="New",
            yes_ask=0.55,
            no_ask=0.42,
            best_bid=0.54,
            liquidity_usd=14000,
            minutes_to_resolution=240,
            yes_token_id="yes_new",
            no_token_id="no_new",
            yes_ask_size=200,
            orderbook_ready=True,
        ),
    }

    class FakeScorer:
        def __init__(self, *_args) -> None:
            self.last_rejection_counts = {"missing_market": 0}
            self.last_wallet_rejection_counts = {}
            self.last_missing_market_samples = []
            self.last_missing_market_breakdown = {}
            self.last_missing_market_by_wallet = {}
            self.last_missing_market_samples_by_wallet = {}
            self.last_wallet_attrition = {"wallets_seen": 1, "wallets_scored": 1}

        def score(self, analysis_trades, market_map):
            assert analysis_trades == trades
            assert market_map == markets
            return {
                "w1": WalletQuality(
                    wallet="w1",
                    topic="geopolitics",
                    score=0.82,
                    eligible_trade_count=1,
                    average_age_seconds=30,
                    average_drift=0.01,
                    reason="mocked",
                )
            }

    class FakeCopyEngine:
        def __init__(self, _config) -> None:
            self.last_diagnostics = {"candidates_returned": 1}

        def evaluate(self, analysis_trades, market_map, wallet_qualities, _store):
            assert analysis_trades == trades
            assert market_map == markets
            assert "w1" in wallet_qualities
            return [
                CopyCandidate(
                    topic="geopolitics",
                    market_id="m_new",
                    side="YES",
                    consensus_ratio=1.0,
                    reference_price=0.52,
                    current_price=0.55,
                    liquidity_usd=14000.0,
                    source_wallets=["w1"],
                    wallet_quality_score=0.82,
                    copyability_score=0.91,
                    reason="mocked",
                    market_title="New",
                )
            ]

    class FakeArbSidecar:
        def __init__(self, _config) -> None:
            return None

        def scan(self, market_map):
            assert market_map == markets
            return [
                ArbitrageOpportunity(
                    market_id="m_new",
                    topic="geopolitics",
                    yes_ask=0.45,
                    no_ask=0.44,
                    total_cost=0.89,
                    gross_edge=0.11,
                    liquidity_usd=1000.0,
                    reason="mocked",
                    net_edge=0.08,
                    annualized_return=0.4,
                    safe_position_size=5.0,
                )
            ]

    class FakeBasketManagerPlanner:
        def __init__(self, _config) -> None:
            return None

        def rebalance(self, current_positions):
            assert current_positions == [
                {"topic": "geopolitics", "exposure_usd": 25.0}
            ]
            return []

    class FakeExitRunner:
        def __init__(self, *_args, **_kwargs) -> None:
            return None

        def evaluate_and_close(self, positions, market_map):
            assert positions == [existing_position]
            assert market_map == markets
            return (
                [
                    ExecutionIntent(
                        market_id="m_open",
                        topic="geopolitics",
                        side="CLOSE",
                        token_id="yes_open",
                        amount_usd=30.0,
                        worst_price=0.6,
                        copyability_score=0.0,
                        order_type="FOK",
                        reason="take profit",
                        market_title="Open",
                    )
                ],
                [
                    replace(
                        existing_position,
                        current_price=0.62,
                        unrealized_pnl=6.0,
                    )
                ],
            )

    class FakeExecutionPlanner:
        def __init__(self, *_args, **_kwargs) -> None:
            self.last_diagnostics = {"candidates_planned": 1}

        def plan(
            self,
            fresh_candidates,
            market_map,
            held_market_ids,
            current_exposure_usd,
        ):
            assert len(fresh_candidates) == 1
            assert market_map == markets
            assert held_market_ids == set()
            assert current_exposure_usd == 0.0
            return [
                ExecutionIntent(
                    market_id="m_new",
                    topic="geopolitics",
                    side="YES",
                    token_id="yes_new",
                    amount_usd=10.0,
                    worst_price=0.57,
                    copyability_score=0.91,
                    order_type="FOK",
                    reason="mocked",
                    market_title="New",
                )
            ]

    class FakeLiveOrderExecutor:
        def __init__(self, *_args, **_kwargs) -> None:
            return None

        def execute(self, intents):
            results = []
            for intent in intents:
                results.append(
                    ExecutionResult(
                        market_id=intent.market_id,
                        topic=intent.topic,
                        side=intent.side,
                        token_id=intent.token_id,
                        amount_usd=intent.amount_usd,
                        worst_price=intent.worst_price,
                        status="filled",
                        order_id=f"order-{intent.market_id}",
                        error="",
                        copyability_score=intent.copyability_score,
                        reason=intent.reason,
                        market_title=intent.market_title,
                        client_order_id=f"coid-{intent.market_id}",
                        filled_shares=round(
                            intent.amount_usd / max(intent.worst_price, 0.01),
                            8,
                        ),
                        avg_fill_price=intent.worst_price,
                    )
                )
            return results

    monkeypatch.setattr("predictcel.cycle_runtime.WalletQualityScorer", FakeScorer)
    monkeypatch.setattr("predictcel.cycle_runtime.CopyEngine", FakeCopyEngine)
    monkeypatch.setattr("predictcel.cycle_runtime.ArbitrageSidecar", FakeArbSidecar)
    monkeypatch.setattr(
        "predictcel.cycle_runtime.BasketManagerPlanner",
        FakeBasketManagerPlanner,
    )
    monkeypatch.setattr("predictcel.cycle_runtime.ExitRunner", FakeExitRunner)
    monkeypatch.setattr(
        "predictcel.cycle_runtime.ExecutionPlanner",
        FakeExecutionPlanner,
    )
    monkeypatch.setattr(
        "predictcel.cycle_runtime.LiveOrderExecutor",
        FakeLiveOrderExecutor,
    )

    def persist_execution_side_effects(fake_store, _config, execution_results) -> None:
        for result in execution_results:
            fake_store.positions.append(
                Position(
                    market_id=result.market_id,
                    topic=result.topic,
                    side=result.side,
                    token_id=result.token_id,
                    entry_price=result.avg_fill_price or result.worst_price,
                    entry_amount_usd=result.amount_usd,
                    current_price=result.avg_fill_price or result.worst_price,
                    unrealized_pnl=0.0,
                    opened_at=datetime.now(UTC),
                    last_updated=datetime.now(UTC),
                    take_profit_pct=0.3,
                    stop_loss_pct=0.1,
                    max_hold_minutes=1440,
                    status="open",
                    market_title=result.market_title,
                    entry_shares=result.filled_shares,
                    remaining_shares=result.filled_shares,
                )
            )

    hooks = CycleRuntimeHooks(
        auto_feed_wallet_registry_from_discovery=lambda _config, _store: {"ran": True},
        build_wallet_registry_summary=lambda _config, _store, _trades, persist_rebalance: {
            "persist_rebalance": persist_rebalance,
        },
        analysis_trades=lambda loaded_trades, _max_age: loaded_trades,
        filter_duplicate_candidates=lambda _store, candidates: (candidates, 0),
        creates_or_updates_paper_position=lambda result: result.status == "filled",
        mark_execution_intents_seen=lambda _store, _intents: None,
        persist_execution_side_effects=persist_execution_side_effects,
        decorate_positions_with_titles=lambda positions, _markets: positions,
        portfolio_summary=lambda _store, _config: {"current_exposure_usd": 10.0},
        open_position_pnl=lambda positions: [
            {"market_id": position.market_id, "status": position.status}
            for position in positions
        ],
    )

    output = run_loaded_cycle(
        config=config,
        store=store,
        trades=trades,
        markets=markets,
        live_input_diagnostics={"requested_wallets": 1},
        live_trading_requested=True,
        use_live_data=True,
        db_path="/tmp/predictcel.db",
        retention_max_rows=200,
        retention_analyze=True,
        retention_vacuum=False,
        cycle_started=time.perf_counter(),
        timings={"input_load_ms": 5},
        metrics=metrics,
        hooks=hooks,
    )

    assert output["mode"] == "live"
    assert output["summary"] == {
        "markets_loaded": 2,
        "wallet_trades_loaded": 1,
        "wallets_scored": 1,
        "copy_candidates": 1,
        "skipped_duplicate_signals": 0,
        "arbitrage_opportunities": 1,
        "execution_intents": 1,
        "execution_results": 1,
        "close_intents": 1,
        "close_results": 1,
        "open_positions": 1,
    }
    assert output["execution"]["planner_ran"] is True
    assert output["execution"]["diagnostics"] == {"candidates_planned": 1}
    assert output["db"]["open_position_count_at_start"] == 1
    assert output["db"]["open_position_count_at_end"] == 1
    assert output["db"]["retention"] == {"copy_candidates": 0}
    assert output["metrics"] == {"markets_loaded": 2, "trades_loaded": 1}
    assert output["wallet_registry"]["auto_feed"] == {"ran": True}
    assert output["execution_results"][0]["market_id"] == "m_new"
    assert output["close_results"][0]["market_id"] == "m_open"
    assert output["open_positions"][0]["market_id"] == "m_new"
    assert store.updated_positions == [
        {
            "market_id": "m_open",
            "token_id": "yes_open",
            "status": "closed",
            "remaining_shares": 0.0,
        }
    ]
    assert store.saved_payloads is not None

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Callable

from .arb_sidecar import ArbitrageSidecar
from .basket_manager import BasketManagerPlanner
from .copy_engine import CopyEngine
from .execution import ExecutionPlanner, ExitRunner, LiveOrderExecutor, intents_as_dicts
from .scoring import WalletQualityScorer
from .storage import SignalStore

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CycleRuntimeHooks:
    auto_feed_wallet_registry_from_discovery: Callable[[Any, SignalStore], dict[str, Any]]
    build_wallet_registry_summary: Callable[..., dict[str, Any]]
    analysis_trades: Callable[[list[Any], int], list[Any]]
    filter_duplicate_candidates: Callable[[SignalStore, list[Any]], tuple[list[Any], int]]
    creates_or_updates_paper_position: Callable[[Any], bool]
    mark_execution_intents_seen: Callable[[SignalStore, list[Any]], None]
    persist_execution_side_effects: Callable[[SignalStore, Any, list[Any]], None]
    decorate_positions_with_titles: Callable[[list[Any], Any], list[Any]]
    portfolio_summary: Callable[[SignalStore, Any], dict[str, Any]]
    open_position_pnl: Callable[[list[Any]], list[dict[str, Any]]]


def run_loaded_cycle(
    *,
    config: Any,
    store: SignalStore,
    trades: list[Any],
    markets: Any,
    live_input_diagnostics: dict[str, Any],
    live_trading_requested: bool,
    use_live_data: bool,
    db_path: str,
    retention_max_rows: int,
    retention_analyze: bool,
    retention_vacuum: bool,
    cycle_started: float,
    timings: dict[str, int],
    metrics: Any,
    hooks: CycleRuntimeHooks,
) -> dict[str, Any]:
    wallet_discovery_auto_feed = hooks.auto_feed_wallet_registry_from_discovery(
        config,
        store,
    )
    wallet_registry_summary = hooks.build_wallet_registry_summary(
        config,
        store,
        trades,
        persist_rebalance=True,
    )
    analysis_trades = hooks.analysis_trades(trades, config.filters.max_trade_age_seconds)
    wallet_registry_summary["auto_feed"] = wallet_discovery_auto_feed
    open_positions_at_start = store.get_open_positions()
    db_diagnostics = {
        "path": db_path,
        "is_ephemeral": db_path.startswith("/tmp/"),
        "open_position_count_at_start": len(open_positions_at_start),
    }
    var_95 = store.get_portfolio_var(confidence_level=0.95)
    logger.info("Portfolio VaR (95%%): %.2f USD", var_95)

    current_positions = [
        {"topic": pos.topic, "exposure_usd": pos.entry_amount_usd}
        for pos in open_positions_at_start
    ]
    rebalance_actions = BasketManagerPlanner(config).rebalance(current_positions)
    if rebalance_actions:
        logger.info("Rebalancing actions suggested: %s", len(rebalance_actions))

    started = time.perf_counter()
    scorer = WalletQualityScorer(
        config.filters,
        config.consensus.recency_half_life_seconds,
    )
    wallet_qualities = scorer.score(analysis_trades, markets)
    scoring_diagnostics = {
        "rejection_counts": scorer.last_rejection_counts,
        "wallet_rejection_counts": scorer.last_wallet_rejection_counts,
        "missing_market_samples": scorer.last_missing_market_samples,
        "missing_market_breakdown": scorer.last_missing_market_breakdown,
        "missing_market_by_wallet": scorer.last_missing_market_by_wallet,
        "missing_market_samples_by_wallet": scorer.last_missing_market_samples_by_wallet,
        "wallet_attrition": scorer.last_wallet_attrition,
        "analysis_trade_count": len(analysis_trades),
        "pre_scoring_too_old_filtered": max(0, len(trades) - len(analysis_trades)),
    }
    timings["wallet_scoring_ms"] = _elapsed_ms(started)

    started = time.perf_counter()
    copy_engine = CopyEngine(config)
    copy_candidates = copy_engine.evaluate(
        analysis_trades,
        markets,
        wallet_qualities,
        store,
    )
    copy_engine_diagnostics = getattr(copy_engine, "last_diagnostics", {})
    timings["copy_engine_ms"] = _elapsed_ms(started)

    started = time.perf_counter()
    arbitrage_opportunities = ArbitrageSidecar(config.arbitrage).scan(markets)
    timings["arbitrage_scan_ms"] = _elapsed_ms(started)

    execution_intents: list[Any] = []
    execution_results: list[Any] = []
    close_intents: list[Any] = []
    close_results: list[Any] = []
    skipped_duplicate_signals = 0
    execution_diagnostics: dict[str, int] = {}
    planner_ran = False

    started = time.perf_counter()
    if live_trading_requested:
        if config.execution is None or not config.execution.enabled:
            raise ValueError(
                "--live-trading was requested but execution is not enabled in config."
            )
        current_exposure_usd = store.get_total_exposure()
        open_positions = store.get_open_positions()
        if open_positions:
            close_intents, updated_positions = ExitRunner(
                config.execution,
                config.live_data,
            ).evaluate_and_close(open_positions, markets)
            close_results = (
                LiveOrderExecutor(
                    config.execution,
                    config.live_data,
                    store=store,
                ).execute(
                    close_intents
                )
                if close_intents
                else []
            )
            closed_positions = {
                (result.market_id, result.token_id)
                for result in close_results
                if hooks.creates_or_updates_paper_position(result)
            }
            for pos in updated_positions:
                status = (
                    "closed"
                    if (pos.market_id, pos.token_id) in closed_positions
                    else pos.status
                )
                store.update_position(
                    pos.market_id,
                    pos.current_price,
                    pos.unrealized_pnl,
                    status,
                    token_id=pos.token_id,
                    remaining_shares=0.0 if status == "closed" else pos.remaining_shares,
                )
            current_exposure_usd = store.get_total_exposure()

        fresh_candidates, skipped_duplicate_signals = hooks.filter_duplicate_candidates(
            store,
            copy_candidates,
        )
        planner = ExecutionPlanner(config.execution, config.execution.position)
        execution_intents = planner.plan(
            fresh_candidates,
            markets,
            store.get_held_market_ids(),
            current_exposure_usd,
        )
        execution_diagnostics = planner.last_diagnostics
        planner_ran = True
        execution_results = LiveOrderExecutor(
            config.execution,
            config.live_data,
            store=store,
        ).execute(execution_intents)
        hooks.persist_execution_side_effects(store, config, execution_results)
    timings["execution_ms"] = _elapsed_ms(started)

    started = time.perf_counter()
    store.save_cycle_payloads(
        copy_candidates,
        arbitrage_opportunities,
        execution_results + close_results,
    )
    open_positions = hooks.decorate_positions_with_titles(
        store.get_open_positions(),
        markets,
    )
    db_diagnostics["open_position_count_at_end"] = len(open_positions)
    retention = store.prune_history(
        max_rows_per_table=retention_max_rows,
        analyze=retention_analyze,
        vacuum=retention_vacuum,
    )
    if retention:
        db_diagnostics["retention"] = retention
    timings["storage_ms"] = _elapsed_ms(started)
    timings["total_cycle_ms"] = _elapsed_ms(cycle_started)

    summary = {
        "markets_loaded": len(markets),
        "wallet_trades_loaded": len(trades),
        "wallets_scored": len(wallet_qualities),
        "copy_candidates": len(copy_candidates),
        "skipped_duplicate_signals": skipped_duplicate_signals,
        "arbitrage_opportunities": len(arbitrage_opportunities),
        "execution_intents": len(execution_intents),
        "execution_results": len(execution_results),
        "close_intents": len(close_intents),
        "close_results": len(close_results),
        "open_positions": len(open_positions),
    }
    metrics.set("markets_loaded", len(markets))
    metrics.set("trades_loaded", len(trades))

    return {
        "mode": "live" if use_live_data else "file",
        "summary": summary,
        "latency_ms": timings,
        "db": db_diagnostics,
        "live_input_diagnostics": live_input_diagnostics,
        "scoring_diagnostics": scoring_diagnostics,
        "copy_engine_diagnostics": copy_engine_diagnostics,
        "wallet_registry": wallet_registry_summary,
        "metrics": metrics.get_metrics(),
        "execution": {
            "live_trading_requested": bool(live_trading_requested),
            "execution_enabled": bool(config.execution and config.execution.enabled),
            "planner_ran": planner_ran,
            "input_degraded": bool(live_input_diagnostics.get("degraded_mode")),
            "diagnostics": execution_diagnostics,
        },
        "portfolio_summary": hooks.portfolio_summary(store, config),
        "wallet_qualities": {
            wallet: quality.__dict__ for wallet, quality in wallet_qualities.items()
        },
        "copy_candidates": [candidate.__dict__ for candidate in copy_candidates],
        "arbitrage_opportunities": [
            opportunity.__dict__ for opportunity in arbitrage_opportunities
        ],
        "execution_intents": intents_as_dicts(execution_intents),
        "execution_results": [result.__dict__ for result in execution_results],
        "close_intents": intents_as_dicts(close_intents),
        "close_results": [result.__dict__ for result in close_results],
        "open_positions": [pos.__dict__ for pos in open_positions],
        "open_position_pnl": hooks.open_position_pnl(open_positions),
    }


def _elapsed_ms(started: float) -> int:
    return int((time.perf_counter() - started) * 1000)

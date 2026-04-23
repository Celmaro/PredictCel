from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from typing import Any

from .arb_sidecar import ArbitrageSidecar
from .config import load_config
from .copy_engine import CopyEngine
from .execution import ExecutionPlanner, ExitRunner, LiveOrderExecutor, intents_as_dicts
from .markets import load_market_snapshots
from .models import Position
from .polymarket import PolymarketPublicClient, build_market_snapshots, build_wallet_trades, enrich_market_snapshots_with_orderbooks
from .scoring import WalletQualityScorer
from .storage import SignalStore
from .wallet_discovery import WalletDiscoveryPipeline
from .wallets import load_wallet_trades

TRUSTED_POSITION_STATUSES = {"filled", "matched", "success", "submitted"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="PredictCel V1 paper engine")
    parser.add_argument("--config", required=True, help="Path to config JSON file")
    parser.add_argument("--db", default="predictcel.db", help="SQLite database path")
    parser.add_argument("--live-data", action="store_true", help="Fetch live public market and wallet data from Polymarket instead of local example files")
    parser.add_argument("--live-trading", action="store_true", help="Submit live orders for planned copy trades when execution is enabled and credentials are configured")
    return parser


def build_discovery_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="PredictCel wallet discovery reports")
    parser.add_argument("--config", required=True, help="Path to config JSON file")
    parser.add_argument("--output-dir", default="data", help="Directory for discovery JSON reports")
    parser.add_argument("--config-output", default=None, help="Optional target for proposed or auto-updated config JSON")
    return parser


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "discover-wallets":
        _run_wallet_discovery(sys.argv[2:])
        return

    cycle_started = time.perf_counter()
    timings: dict[str, int] = {}
    args = build_parser().parse_args()
    config = load_config(args.config)
    use_live_data = bool(args.live_data or (config.live_data and config.live_data.enabled))

    started = time.perf_counter()
    trades, markets = _load_live_inputs(config) if use_live_data else (load_wallet_trades(config.wallet_trades_path), load_market_snapshots(config.market_snapshots_path))
    timings["input_load_ms"] = _elapsed_ms(started)

    started = time.perf_counter()
    scorer = WalletQualityScorer(config.filters, config.consensus.recency_half_life_seconds)
    wallet_qualities = scorer.score(trades, markets)
    scoring_diagnostics = {
        "rejection_counts": scorer.last_rejection_counts,
        "wallet_rejection_counts": scorer.last_wallet_rejection_counts,
    }
    timings["wallet_scoring_ms"] = _elapsed_ms(started)

    started = time.perf_counter()
    copy_candidates = CopyEngine(config).evaluate(trades, markets, wallet_qualities)
    timings["copy_engine_ms"] = _elapsed_ms(started)

    started = time.perf_counter()
    arbitrage_opportunities = ArbitrageSidecar(config.arbitrage).scan(markets)
    timings["arbitrage_scan_ms"] = _elapsed_ms(started)

    execution_intents: list = []
    execution_results: list = []
    close_intents: list = []
    close_results: list = []
    skipped_duplicate_signals = 0
    store = SignalStore(args.db)

    started = time.perf_counter()
    if args.live_trading:
        if config.execution is None or not config.execution.enabled:
            raise ValueError("--live-trading was requested but execution is not enabled in config.")
        current_exposure_usd = store.get_total_exposure()
        open_positions = store.get_open_positions()
        if open_positions:
            close_intents, updated_positions = ExitRunner(config.execution, config.live_data).evaluate_and_close(open_positions, markets)
            close_results = LiveOrderExecutor(config.execution, config.live_data).execute(close_intents) if close_intents else []
            closed_market_ids = {result.market_id for result in close_results if _is_trusted_execution_result(result)}
            for pos in updated_positions:
                store.update_position(pos.market_id, pos.current_price, pos.unrealized_pnl, "closed" if pos.market_id in closed_market_ids else pos.status)
            current_exposure_usd = store.get_total_exposure()

        fresh_candidates, skipped_duplicate_signals = _filter_duplicate_candidates(store, copy_candidates)
        execution_intents = ExecutionPlanner(config.execution, config.execution.position).plan(fresh_candidates, markets, store.get_held_market_ids(), current_exposure_usd)
        execution_results = LiveOrderExecutor(config.execution, config.live_data).execute(execution_intents)
        _persist_execution_side_effects(store, config, execution_results)
    timings["execution_ms"] = _elapsed_ms(started)

    started = time.perf_counter()
    store.save_copy_candidates(copy_candidates)
    store.save_arbitrage_opportunities(arbitrage_opportunities)
    store.save_execution_results(execution_results + close_results)
    open_positions = store.get_open_positions()
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
    output = {
        "mode": "live" if use_live_data else "file",
        "summary": summary,
        "latency_ms": timings,
        "scoring_diagnostics": scoring_diagnostics,
        "portfolio_summary": _portfolio_summary(store, config),
        "wallet_qualities": {wallet: quality.__dict__ for wallet, quality in wallet_qualities.items()},
        "copy_candidates": [candidate.__dict__ for candidate in copy_candidates],
        "arbitrage_opportunities": [opportunity.__dict__ for opportunity in arbitrage_opportunities],
        "execution_intents": intents_as_dicts(execution_intents),
        "execution_results": [result.__dict__ for result in execution_results],
        "close_intents": intents_as_dicts(close_intents),
        "close_results": [result.__dict__ for result in close_results],
        "open_positions": [pos.__dict__ for pos in open_positions],
    }
    _log_event("predictcel_cycle_latency", {"mode": output["mode"], "latency_ms": timings, "summary": summary, "scoring_diagnostics": scoring_diagnostics})
    print(json.dumps(output, indent=2, default=str))


def _run_wallet_discovery(argv: list[str]) -> None:
    started = time.perf_counter()
    args = build_discovery_parser().parse_args(argv)
    config = load_config(args.config)
    files = WalletDiscoveryPipeline(config).write_reports(args.output_dir, args.config, args.config_output)
    print(json.dumps({"mode": "wallet_discovery", "reports": files, "latency_ms": {"total_cycle_ms": _elapsed_ms(started)}}, indent=2))


def _persist_execution_side_effects(store: SignalStore, config: Any, execution_results: list) -> None:
    now = datetime.now(UTC)
    position_config = config.execution.position
    for result in execution_results:
        if result.status == "dry_run" or _is_trusted_execution_result(result):
            store.mark_signal_seen(result.market_id, result.topic, result.side)
        if not _is_trusted_execution_result(result):
            continue
        store.save_position(Position(result.market_id, result.topic, result.side, result.token_id, result.worst_price, result.amount_usd, result.worst_price, 0.0, now, now, position_config.take_profit_pct, position_config.stop_loss_pct, position_config.max_hold_minutes, "open"))


def _portfolio_summary(store: SignalStore, config: Any) -> dict:
    if config.execution is None or config.execution.exposure is None:
        return store.get_portfolio_summary(starting_bankroll_usd=0.0)
    return store.get_portfolio_summary(starting_bankroll_usd=config.execution.exposure.max_total_exposure_usd)


def _load_live_inputs(config):
    if config.live_data is None:
        raise ValueError("--live-data was requested but live_data is not configured.")
    client = PolymarketPublicClient(config.live_data.gamma_base_url, config.live_data.data_base_url, config.live_data.clob_base_url, config.live_data.request_timeout_seconds)
    topic_by_wallet = {wallet: basket.topic for basket in config.baskets for wallet in basket.wallets}
    wallet_payloads = _fetch_wallet_payloads(client, list(topic_by_wallet), config.live_data.trade_limit)
    trades = build_wallet_trades(wallet_payloads, topic_by_wallet)
    markets = build_market_snapshots(client.fetch_active_markets(config.live_data.market_limit))
    return trades, enrich_market_snapshots_with_orderbooks(markets, client)


def _fetch_wallet_payloads(client: PolymarketPublicClient, wallets: list[str], limit: int) -> dict[str, list[dict[str, Any]]]:
    if not wallets:
        return {}
    payloads: dict[str, list[dict[str, Any]]] = {}
    workers = max(1, min(8, len(wallets)))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(client.fetch_wallet_trades, wallet, limit): wallet for wallet in wallets}
        for future in as_completed(futures):
            wallet = futures[future]
            try:
                payloads[wallet] = future.result()
            except Exception:
                payloads[wallet] = []
    return payloads


def _filter_duplicate_candidates(store: SignalStore, candidates: list) -> tuple[list, int]:
    fresh = [candidate for candidate in candidates if not store.has_recent_signal(candidate.market_id, candidate.topic, candidate.side)]
    return fresh, len(candidates) - len(fresh)


def _is_trusted_execution_result(result) -> bool:
    return str(result.status).strip().lower() in TRUSTED_POSITION_STATUSES and bool(str(result.order_id).strip())


def _elapsed_ms(started: float) -> int:
    return int((time.perf_counter() - started) * 1000)


def _log_event(event: str, payload: dict[str, Any]) -> None:
    print(json.dumps({"event": event, "ts": datetime.now(UTC).isoformat(), **payload}, sort_keys=True, default=str), flush=True)


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import json
import sys
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
    parser.add_argument("discover-wallets")
    parser.add_argument("--config", required=True, help="Path to config JSON file")
    parser.add_argument("--output-dir", default="data", help="Directory for discovery JSON reports")
    return parser


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "discover-wallets":
        _run_wallet_discovery()
        return

    args = build_parser().parse_args()
    config = load_config(args.config)
    use_live_data = bool(args.live_data or (config.live_data and config.live_data.enabled))
    trades, markets = _load_live_inputs(config) if use_live_data else (load_wallet_trades(config.wallet_trades_path), load_market_snapshots(config.market_snapshots_path))

    wallet_qualities = WalletQualityScorer(config.filters).score(trades, markets)
    copy_candidates = CopyEngine(config).evaluate(trades, markets, wallet_qualities)
    arbitrage_opportunities = ArbitrageSidecar(config.arbitrage).scan(markets)

    execution_intents: list = []
    execution_results: list = []
    close_intents: list = []
    close_results: list = []
    skipped_duplicate_signals = 0
    store = SignalStore(args.db)

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

    store.save_copy_candidates(copy_candidates)
    store.save_arbitrage_opportunities(arbitrage_opportunities)
    store.save_execution_results(execution_results + close_results)
    open_positions = store.get_open_positions()
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
    print(json.dumps({
        "mode": "live" if use_live_data else "file",
        "summary": summary,
        "portfolio_summary": _portfolio_summary(store, config),
        "wallet_qualities": {wallet: quality.__dict__ for wallet, quality in wallet_qualities.items()},
        "copy_candidates": [candidate.__dict__ for candidate in copy_candidates],
        "arbitrage_opportunities": [opportunity.__dict__ for opportunity in arbitrage_opportunities],
        "execution_intents": intents_as_dicts(execution_intents),
        "execution_results": [result.__dict__ for result in execution_results],
        "close_intents": intents_as_dicts(close_intents),
        "close_results": [result.__dict__ for result in close_results],
        "open_positions": [pos.__dict__ for pos in open_positions],
    }, indent=2, default=str))


def _run_wallet_discovery() -> None:
    args = build_discovery_parser().parse_args()
    config = load_config(args.config)
    files = WalletDiscoveryPipeline(config).write_reports(args.output_dir)
    print(json.dumps({"mode": "wallet_discovery", "reports": files}, indent=2))


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
    wallet_payloads = {}
    for wallet in topic_by_wallet:
        try:
            wallet_payloads[wallet] = client.fetch_wallet_trades(wallet, config.live_data.trade_limit)
        except Exception:
            wallet_payloads[wallet] = []
    trades = build_wallet_trades(wallet_payloads, topic_by_wallet)
    markets = build_market_snapshots(client.fetch_active_markets(config.live_data.market_limit))
    return trades, enrich_market_snapshots_with_orderbooks(markets, client)


def _filter_duplicate_candidates(store: SignalStore, candidates: list) -> tuple[list, int]:
    fresh = [candidate for candidate in candidates if not store.has_recent_signal(candidate.market_id, candidate.topic, candidate.side)]
    return fresh, len(candidates) - len(fresh)


def _is_trusted_execution_result(result) -> bool:
    return str(result.status).strip().lower() in TRUSTED_POSITION_STATUSES and bool(str(result.order_id).strip())


if __name__ == "__main__":
    main()

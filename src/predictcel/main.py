from __future__ import annotations

import argparse
import json
from dataclasses import replace
from datetime import UTC, datetime

from .arb_sidecar import ArbitrageSidecar
from .config import load_config
from .copy_engine import CopyEngine
from .execution import ExecutionPlanner, ExitRunner, LiveOrderExecutor, intents_as_dicts
from .markets import load_market_snapshots
from .models import Position
from .polymarket import (
    PolymarketPublicClient,
    build_market_snapshots,
    build_wallet_trades,
    enrich_market_snapshots_with_orderbooks,
)
from .scoring import WalletQualityScorer
from .storage import SignalStore
from .wallets import load_wallet_trades


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="PredictCel V1 paper engine")
    parser.add_argument("--config", required=True, help="Path to config JSON file")
    parser.add_argument("--db", default="predictcel.db", help="SQLite database path")
    parser.add_argument(
        "--live-data",
        action="store_true",
        help="Fetch live public market and wallet data from Polymarket instead of local example files",
    )
    parser.add_argument(
        "--live-trading",
        action="store_true",
        help="Submit live orders for planned copy trades when execution is enabled and credentials are configured",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_config(args.config)
    use_live_data = bool(args.live_data or (config.live_data and config.live_data.enabled))

    if use_live_data:
        trades, markets = _load_live_inputs(config)
    else:
        trades = load_wallet_trades(config.wallet_trades_path)
        markets = load_market_snapshots(config.market_snapshots_path)

    wallet_quality_scorer = WalletQualityScorer(config.filters)
    wallet_qualities = wallet_quality_scorer.score(trades, markets)

    copy_engine = CopyEngine(config)
    arb_sidecar = ArbitrageSidecar(config.arbitrage)

    copy_candidates = copy_engine.evaluate(trades, markets, wallet_qualities)
    arbitrage_opportunities = arb_sidecar.scan(markets)

    execution_intents: list = []
    execution_results: list = []
    close_intents: list = []
    close_results: list = []
    updated_positions: list = []

    store = SignalStore(args.db)

    if args.live_trading:
        if config.execution is None or not config.execution.enabled:
            raise ValueError("--live-trading was requested but execution is not enabled in config.")

        # Load held market IDs and current total exposure
        held_market_ids = store.get_held_market_ids()
        current_exposure_usd = store.get_total_exposure()

        # --- Exit runner: evaluate and close existing positions ---
        exit_runner = ExitRunner(config.execution, config.live_data)
        open_positions = store.get_open_positions()

        if open_positions:
            close_intents, updated_positions = exit_runner.evaluate_and_close(open_positions, markets)
            if close_intents:
                executor = LiveOrderExecutor(config.execution, config.live_data)
                close_results = executor.execute(close_intents)
            # Persist updated position states
            for pos in updated_positions:
                store.update_position(
                    market_id=pos.market_id,
                    current_price=pos.current_price,
                    unrealized_pnl=pos.unrealized_pnl,
                    status=pos.status,
                )
            # Recompute exposure after closes (closed positions drop to 0 exposure)
            current_exposure_usd = store.get_total_exposure()

        # --- Entry planner: enforce exposure cap and skip held markets ---
        planner = ExecutionPlanner(config.execution, config.execution.position)
        execution_intents = planner.plan(
            copy_candidates,
            markets,
            held_market_ids,
            current_exposure_usd,
        )
        executor = LiveOrderExecutor(config.execution, config.live_data)
        execution_results = executor.execute(execution_intents)

        # Persist new positions after fills
        now = datetime.now(UTC)
        position_config = config.execution.position
        for result in execution_results:
            if result.status not in ("dry_run", "error"):
                store.save_position(
                    Position(
                        market_id=result.market_id,
                        topic=result.topic,
                        side=result.side,
                        token_id=result.token_id,
                        entry_price=result.worst_price,
                        entry_amount_usd=result.amount_usd,
                        current_price=result.worst_price,
                        unrealized_pnl=0.0,
                        opened_at=now,
                        last_updated=now,
                        take_profit_pct=position_config.take_profit_pct,
                        stop_loss_pct=position_config.stop_loss_pct,
                        max_hold_minutes=position_config.max_hold_minutes,
                        status="open",
                    )
                )

    store.save_copy_candidates(copy_candidates)
    store.save_arbitrage_opportunities(arbitrage_opportunities)
    store.save_execution_results(execution_results + close_results)

    print(json.dumps({
        "mode": "live" if use_live_data else "file",
        "wallet_qualities": {wallet: quality.__dict__ for wallet, quality in wallet_qualities.items()},
        "copy_candidates": [candidate.__dict__ for candidate in copy_candidates],
        "arbitrage_opportunities": [opportunity.__dict__ for opportunity in arbitrage_opportunities],
        "execution_intents": intents_as_dicts(execution_intents),
        "execution_results": [result.__dict__ for result in execution_results],
        "close_intents": intents_as_dicts(close_intents),
        "close_results": [result.__dict__ for result in close_results],
        "open_positions": [pos.__dict__ for pos in updated_positions],
    }, indent=2))


def _load_live_inputs(config):
    if config.live_data is None:
        raise ValueError("--live-data was requested but live_data is not configured.")

    client = PolymarketPublicClient(
        gamma_base_url=config.live_data.gamma_base_url,
        data_base_url=config.live_data.data_base_url,
        clob_base_url=config.live_data.clob_base_url,
        timeout_seconds=config.live_data.request_timeout_seconds,
    )
    topic_by_wallet = {
        wallet: basket.topic
        for basket in config.baskets
        for wallet in basket.wallets
    }
    wallet_payloads = {
        wallet: client.fetch_wallet_trades(wallet, config.live_data.trade_limit)
        for wallet in topic_by_wallet
    }
    trades = build_wallet_trades(wallet_payloads, topic_by_wallet)
    market_payload = client.fetch_active_markets(config.live_data.market_limit)
    markets = build_market_snapshots(market_payload)
    markets = enrich_market_snapshots_with_orderbooks(markets, client)
    return trades, markets


if __name__ == "__main__":
    main()

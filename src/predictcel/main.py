from __future__ import annotations

import argparse
import json
import logging
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
from .polymarket import (
    PolymarketPublicClient,
    build_market_snapshots,
    build_wallet_trades,
    enrich_market_snapshots_with_orderbooks,
    extract_trade_market_ids,
    extract_trade_market_slugs,
)
from .scoring import WalletQualityScorer
from .storage import SignalStore
from .wallet_discovery import WalletDiscoveryPipeline
from .wallets import load_wallet_trades

TRUSTED_POSITION_STATUSES = {"filled", "matched", "success", "submitted"}
HEX_CHARS = set("0123456789abcdefABCDEF")


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    return logging.getLogger(__name__)


logger = setup_logging()


class MetricsCollector:
    def __init__(self):
        self.metrics = {
            "cycles_total": 0,
            "api_requests_total": 0,
            "api_errors_total": 0,
            "trades_executed_total": 0,
            "pnl_total": 0.0,
            "latency_ms": {},
        }

    def increment(self, key: str, value: float = 1.0):
        if key in self.metrics:
            self.metrics[key] += value

    def set(self, key: str, value: float):
        self.metrics[key] = value

    def get_metrics(self) -> dict:
        return self.metrics.copy()


metrics = MetricsCollector()


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

    logger.info("Starting PredictCel cycle", extra={"mode": "live" if use_live_data else "file", "config": args.config})
    metrics.increment("cycles_total")

    started = time.perf_counter()
    live_input_diagnostics: dict[str, Any] = {}
    if use_live_data:
        try:
            trades, markets, live_input_diagnostics = _load_live_inputs(config)
        except Exception as e:
            logger.warning("Live data fetch failed, falling back to file data", extra={"error": str(e)})
            trades, markets = load_wallet_trades(config.wallet_trades_path), load_market_snapshots(config.market_snapshots_path)
            live_input_diagnostics = {"fallback_reason": str(e)}
    else:
        trades, markets = load_wallet_trades(config.wallet_trades_path), load_market_snapshots(config.market_snapshots_path)
    timings["input_load_ms"] = _elapsed_ms(started)
    logger.info("Input loading complete", extra={"markets_loaded": len(markets), "trades_loaded": len(trades), "latency_ms": timings["input_load_ms"]})
    metrics.set("markets_loaded", len(markets))
    metrics.set("trades_loaded", len(trades))

    # Calculate and log portfolio VaR
    store = SignalStore(args.db)
    var_95 = store.get_portfolio_var(confidence_level=0.95)
    logger.info(f"Portfolio VaR (95%): {var_95:.2f} USD")

    # Check for rebalancing needs
    current_positions = [{"topic": pos.topic, "exposure_usd": pos.entry_amount_usd} for pos in store.get_open_positions()]
    basket_planner = BasketManagerPlanner(config)
    rebalance_actions = basket_planner.rebalance(current_positions)
    if rebalance_actions:
        logger.info(f"Rebalancing actions suggested: {len(rebalance_actions)}")

    started = time.perf_counter()
    scorer = WalletQualityScorer(config.filters, config.consensus.recency_half_life_seconds)
    wallet_qualities = scorer.score(trades, markets)
    scoring_diagnostics = {
        "rejection_counts": scorer.last_rejection_counts,
        "wallet_rejection_counts": scorer.last_wallet_rejection_counts,
        "missing_market_samples": scorer.last_missing_market_samples,
    }
    timings["wallet_scoring_ms"] = _elapsed_ms(started)

    started = time.perf_counter()
    copy_engine = CopyEngine(config)
    copy_candidates = copy_engine.evaluate(trades, markets, wallet_qualities, store)
    copy_engine_diagnostics = getattr(copy_engine, "last_diagnostics", {})
    timings["copy_engine_ms"] = _elapsed_ms(started)

    started = time.perf_counter()
    arbitrage_opportunities = ArbitrageSidecar(config.arbitrage).scan(markets)
    timings["arbitrage_scan_ms"] = _elapsed_ms(started)

    execution_intents: list = []
    execution_results: list = []
    close_intents: list = []
    close_results: list = []
    skipped_duplicate_signals = 0

    started = time.perf_counter()
    if args.live_trading:
        if config.execution is None or not config.execution.enabled:
            raise ValueError("--live-trading was requested but execution is not enabled in config.")
        current_exposure_usd = store.get_total_exposure()
        open_positions = store.get_open_positions()
        if open_positions:
            close_intents, updated_positions = ExitRunner(config.execution, config.live_data).evaluate_and_close(open_positions, markets)
            close_results = LiveOrderExecutor(config.execution, config.live_data).execute(close_intents) if close_intents else []
            closed_market_ids = {result.market_id for result in close_results if _creates_or_updates_paper_position(result)}
            for pos in updated_positions:
                store.update_position(pos.market_id, pos.current_price, pos.unrealized_pnl, "closed" if pos.market_id in closed_market_ids else pos.status)
            current_exposure_usd = store.get_total_exposure()

        fresh_candidates, skipped_duplicate_signals = _filter_duplicate_candidates(store, copy_candidates)
        execution_intents = ExecutionPlanner(config.execution, config.execution.position).plan(fresh_candidates, markets, store.get_held_market_ids(), current_exposure_usd)
        execution_results = LiveOrderExecutor(config.execution, config.live_data).execute(execution_intents)
        _persist_execution_side_effects(store, config, execution_results)
    timings["execution_ms"] = _elapsed_ms(started)

    started = time.perf_counter()
    store.save_cycle_payloads(copy_candidates, arbitrage_opportunities, execution_results + close_results)
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
        "live_input_diagnostics": live_input_diagnostics,
        "scoring_diagnostics": scoring_diagnostics,
        "copy_engine_diagnostics": copy_engine_diagnostics,
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
    _log_event(
        "predictcel_cycle_latency",
        {
            "mode": output["mode"],
            "latency_ms": timings,
            "summary": summary,
            "live_input_diagnostics": _compact_live_input_diagnostics(live_input_diagnostics),
            "scoring_diagnostics": _compact_scoring_diagnostics(scoring_diagnostics),
            "copy_engine_diagnostics": copy_engine_diagnostics,
        },
    )
    logger.info("Cycle complete", extra={"summary": summary, "timings": timings, "metrics": metrics.get_metrics()})
    print(json.dumps(_compact_cycle_output(output), sort_keys=True, default=str), flush=True)


def _run_wallet_discovery(argv: list[str]) -> None:
    started = time.perf_counter()
    args = build_discovery_parser().parse_args(argv)
    config = load_config(args.config)
    files = WalletDiscoveryPipeline(config).write_reports(args.output_dir, args.config, args.config_output)
    print(json.dumps({"mode": "wallet_discovery", "reports": files, "latency_ms": {"total_cycle_ms": _elapsed_ms(started)}}, indent=2))


def _persist_execution_side_effects(store: SignalStore, config: Any, execution_results: list) -> None:
    now = datetime.now(UTC)
    position_config = config.execution.position
    signals: list[tuple[str, str, str]] = []
    positions: list[Position] = []
    for result in execution_results:
        if _creates_or_updates_paper_position(result):
            signals.append((result.market_id, result.topic, result.side))
            positions.append(
                Position(
                    result.market_id,
                    result.topic,
                    result.side,
                    result.token_id,
                    result.worst_price,
                    result.amount_usd,
                    result.worst_price,
                    0.0,
                    now,
                    now,
                    position_config.take_profit_pct,
                    position_config.stop_loss_pct,
                    position_config.max_hold_minutes,
                    "open",
                )
            )
    if signals:
        store.mark_signals_seen(signals)
    if positions:
        store.save_positions(positions)


def _portfolio_summary(store: SignalStore, config: Any) -> dict:
    if config.execution is None or config.execution.exposure is None:
        return store.get_portfolio_summary(starting_bankroll_usd=0.0)
    return store.get_portfolio_summary(starting_bankroll_usd=config.execution.exposure.max_total_exposure_usd)


def _load_live_inputs(config):
    if config.live_data is None:
        raise ValueError("--live-data was requested but live_data is not configured.")
    client = PolymarketPublicClient(config.live_data.gamma_base_url, config.live_data.data_base_url, config.live_data.clob_base_url, config.live_data.request_timeout_seconds)

    raw_topic_by_wallet: dict[str, list[str]] = {}
    invalid_wallets: list[str] = []
    for basket in config.baskets:
        for wallet in basket.wallets:
            if not _is_evm_address(wallet):
                invalid_wallets.append(wallet)
                continue
            topics = raw_topic_by_wallet.setdefault(wallet, [])
            if basket.topic not in topics:
                topics.append(basket.topic)

    wallet_payloads = _fetch_wallet_payloads(client, list(raw_topic_by_wallet), config.live_data.trade_limit)
    trades = build_wallet_trades(wallet_payloads, raw_topic_by_wallet)
    trade_market_ids = extract_trade_market_ids(wallet_payloads)
    trade_market_slugs = extract_trade_market_slugs(wallet_payloads)

    active_market_rows = client.fetch_active_markets(config.live_data.market_limit)
    markets = build_market_snapshots(active_market_rows)

    missing_trade_market_ids = [market_id for market_id in trade_market_ids if market_id not in markets]
    supplemental_rows = client.fetch_markets_by_identifiers(missing_trade_market_ids)
    if supplemental_rows:
        markets.update(build_market_snapshots(supplemental_rows))

    missing_trade_market_slugs = [market_slug for market_slug in trade_market_slugs if market_slug not in markets]
    supplemental_slug_rows = client.fetch_markets_by_slugs(missing_trade_market_slugs)
    if supplemental_slug_rows:
        markets.update(build_market_snapshots(supplemental_slug_rows))

    relevant_market_keys = {market_key for market_key in (*trade_market_ids, *trade_market_slugs) if market_key in markets}
    if relevant_market_keys:
        relevant_snapshots = {market_id: markets[market_id] for market_id in relevant_market_keys}
        markets.update(enrich_market_snapshots_with_orderbooks(relevant_snapshots, client))

    trade_market_keys = sorted(set(trade_market_ids + trade_market_slugs))
    matched_trade_market_keys = [market_key for market_key in trade_market_keys if market_key in markets]
    wallets_with_payloads = sum(1 for items in wallet_payloads.values() if items)
    wallets_with_parsed_trades = len({trade.wallet for trade in trades})
    diagnostics = {
        "requested_wallets": sum(len(basket.wallets) for basket in config.baskets),
        "valid_wallets": len(raw_topic_by_wallet),
        "skipped_invalid_wallets": len(invalid_wallets),
        "sample_skipped_invalid_wallets": invalid_wallets[:5],
        "wallet_payloads_loaded": sum(len(items) for items in wallet_payloads.values()),
        "wallets_with_payloads": wallets_with_payloads,
        "wallets_with_parsed_trades": wallets_with_parsed_trades,
        "parsed_trade_count": len(trades),
        "active_market_rows_loaded": len(active_market_rows),
        "trade_market_ids_seen": len(trade_market_ids),
        "trade_market_slugs_seen": len(trade_market_slugs),
        "supplemental_market_ids_requested": len(missing_trade_market_ids),
        "supplemental_market_rows_loaded": len(supplemental_rows),
        "supplemental_market_slugs_requested": len(missing_trade_market_slugs),
        "supplemental_slug_rows_loaded": len(supplemental_slug_rows),
        "market_cache_entries": len(markets),
        "multi_topic_wallets": sum(1 for topics in raw_topic_by_wallet.values() if len(topics) > 1),
        "market_crossref": {
            "unique_trade_market_keys": len(trade_market_keys),
            "matched_count": len(matched_trade_market_keys),
            "match_rate_pct": round((len(matched_trade_market_keys) / len(trade_market_keys)) * 100, 1) if trade_market_keys else 0.0,
        },
    }
    return trades, markets, diagnostics


def _fetch_wallet_payloads(client: PolymarketPublicClient, wallets: list[str], limit: int) -> dict[str, list[dict[str, Any]]]:
    if not wallets:
        return {}
    payloads: dict[str, list[dict[str, Any]]] = {}
    workers = max(1, min(16, len(wallets)))
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
    fingerprints = [store.make_signal_fingerprint(candidate.market_id, candidate.topic, candidate.side) for candidate in candidates]
    recent_fingerprints = store.has_recent_signals([(candidate.market_id, candidate.topic, candidate.side) for candidate in candidates])
    fresh = [candidate for candidate, fingerprint in zip(candidates, fingerprints) if fingerprint not in recent_fingerprints]
    return fresh, len(candidates) - len(fresh)


def _is_trusted_execution_result(result) -> bool:
    return str(result.status).strip().lower() in TRUSTED_POSITION_STATUSES and bool(str(result.order_id).strip())


def _creates_or_updates_paper_position(result) -> bool:
    return str(result.status).strip().lower() == "dry_run" or _is_trusted_execution_result(result)


def _is_evm_address(value: str) -> bool:
    value = str(value).strip()
    return len(value) == 42 and value.startswith("0x") and all(char in HEX_CHARS for char in value[2:])


def _compact_cycle_output(output: dict[str, Any]) -> dict[str, Any]:
    compact = {
        "mode": output["mode"],
        "summary": output["summary"],
        "latency_ms": output["latency_ms"],
        "portfolio_summary": output["portfolio_summary"],
    }
    if output.get("live_input_diagnostics"):
        compact["live_input_diagnostics"] = _compact_live_input_diagnostics(output["live_input_diagnostics"])
    if output.get("scoring_diagnostics"):
        compact["scoring_diagnostics"] = _compact_scoring_diagnostics(output["scoring_diagnostics"])
    if output.get("copy_engine_diagnostics"):
        compact["copy_engine_diagnostics"] = output["copy_engine_diagnostics"]
    top_wallet_qualities = _top_wallet_qualities(output.get("wallet_qualities", {}))
    if top_wallet_qualities:
        compact["wallet_qualities"] = top_wallet_qualities
    for key in (
        "copy_candidates",
        "arbitrage_opportunities",
        "execution_intents",
        "execution_results",
        "close_intents",
        "close_results",
        "open_positions",
    ):
        if output.get(key):
            compact[key] = output[key]
    return compact


def _compact_live_input_diagnostics(diagnostics: dict[str, Any]) -> dict[str, Any]:
    if not diagnostics:
        return {}
    compact = {
        "requested_wallets": diagnostics.get("requested_wallets", 0),
        "valid_wallets": diagnostics.get("valid_wallets", 0),
        "wallet_payloads_loaded": diagnostics.get("wallet_payloads_loaded", 0),
        "wallets_with_parsed_trades": diagnostics.get("wallets_with_parsed_trades", 0),
        "parsed_trade_count": diagnostics.get("parsed_trade_count", 0),
        "active_market_rows_loaded": diagnostics.get("active_market_rows_loaded", 0),
        "trade_market_ids_seen": diagnostics.get("trade_market_ids_seen", 0),
        "trade_market_slugs_seen": diagnostics.get("trade_market_slugs_seen", 0),
        "supplemental_market_ids_requested": diagnostics.get("supplemental_market_ids_requested", 0),
        "supplemental_market_rows_loaded": diagnostics.get("supplemental_market_rows_loaded", 0),
        "supplemental_market_slugs_requested": diagnostics.get("supplemental_market_slugs_requested", 0),
        "supplemental_slug_rows_loaded": diagnostics.get("supplemental_slug_rows_loaded", 0),
        "market_cache_entries": diagnostics.get("market_cache_entries", 0),
        "market_crossref": diagnostics.get("market_crossref", {}),
    }
    skipped_invalid_wallets = diagnostics.get("skipped_invalid_wallets", 0)
    if skipped_invalid_wallets:
        compact["skipped_invalid_wallets"] = skipped_invalid_wallets
        compact["sample_skipped_invalid_wallets"] = diagnostics.get("sample_skipped_invalid_wallets", [])[:3]
    return compact


def _compact_scoring_diagnostics(diagnostics: dict[str, Any]) -> dict[str, Any]:
    if not diagnostics:
        return {}
    wallet_rejection_counts = diagnostics.get("wallet_rejection_counts", {})
    top_wallet_rejections = sorted(
        (
            {
                "wallet": wallet,
                "total": sum(counts.values()),
                "reasons": counts,
            }
            for wallet, counts in wallet_rejection_counts.items()
            if counts
        ),
        key=lambda item: item["total"],
        reverse=True,
    )[:3]
    compact = {
        "rejection_counts": diagnostics.get("rejection_counts", {}),
        "wallets_with_rejections": len(wallet_rejection_counts),
    }
    if top_wallet_rejections:
        compact["top_wallet_rejections"] = top_wallet_rejections
    return compact


def _top_wallet_qualities(wallet_qualities: dict[str, dict[str, Any]], limit: int = 3) -> dict[str, dict[str, Any]]:
    ranked = sorted(
        wallet_qualities.items(),
        key=lambda item: float(item[1].get("score", 0.0)),
        reverse=True,
    )[:limit]
    return {wallet: quality for wallet, quality in ranked}


def _elapsed_ms(started: float) -> int:
    return int((time.perf_counter() - started) * 1000)


def _log_event(event: str, payload: dict[str, Any]) -> None:
    print(json.dumps({"event": event, "ts": datetime.now(UTC).isoformat(), **payload}, sort_keys=True, default=str), flush=True)


if __name__ == "__main__":
    main()

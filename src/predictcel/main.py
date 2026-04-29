"""Main entry point for PredictCel.

Provides the CLI interface and orchestrates the full trading cycle:
discovery, scoring, basket assignment, market evaluation, and execution.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

from .arb_sidecar import ArbitrageSidecar
from .basket_manager import BasketManagerPlanner
from .config import load_config
from .copy_engine import CopyEngine
from .execution import ExecutionPlanner, ExitRunner, LiveOrderExecutor, intents_as_dicts
from .markets import load_market_snapshots
from .models import BasketHealth, BasketMembership, Position, WalletRegistryEntry
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
from .wallet_registry import (
    apply_basket_manager_actions_to_memberships,
    build_live_basket_roster,
    compute_basket_health_from_static_memberships,
    ingest_wallet_discovery_inputs,
    recommend_basket_promotions,
    refresh_registry_entries_from_trades,
)
from .wallets import load_wallet_trades

__all__ = ["main"]


TRUSTED_POSITION_STATUSES = {"filled", "matched", "success"}
HEX_CHARS = set("0123456789abcdefABCDEF")


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
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


def build_discovery_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="PredictCel wallet discovery reports")
    parser.add_argument("--config", required=True, help="Path to config JSON file")
    parser.add_argument("--output-dir", default="data", help="Directory for discovery JSON reports")
    parser.add_argument("--db", default=None, help="Optional SQLite database path for persisting wallet registry discovery inputs")
    parser.add_argument(
        "--config-output",
        default=None,
        help="Optional target for proposed or auto-updated config JSON",
    )
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

    logger.info(
        "Starting PredictCel cycle",
        extra={
            "mode": "live" if use_live_data else "file",
            "config": args.config,
            "db_path": args.db,
        },
    )
    metrics.increment("cycles_total")
    store = SignalStore(args.db)

    started = time.perf_counter()
    live_input_diagnostics: dict[str, Any] = {}
    if use_live_data:
        try:
            trades, markets, live_input_diagnostics = _load_live_inputs(config, store)
        except Exception as exc:
            logger.warning(
                "Live data fetch failed, falling back to file data",
                extra={"error": str(exc)},
            )
            trades = load_wallet_trades(config.wallet_trades_path)
            markets = load_market_snapshots(config.market_snapshots_path)
            live_input_diagnostics = {"fallback_reason": str(exc)}
    else:
        trades = load_wallet_trades(config.wallet_trades_path)
        markets = load_market_snapshots(config.market_snapshots_path)
    timings["input_load_ms"] = _elapsed_ms(started)
    logger.info(
        "Input loading complete",
        extra={
            "markets_loaded": len(markets),
            "trades_loaded": len(trades),
            "latency_ms": timings["input_load_ms"],
        },
    )
    metrics.set("markets_loaded", len(markets))
    metrics.set("trades_loaded", len(trades))

    wallet_discovery_auto_feed = _auto_feed_wallet_registry_from_discovery(config, store)
    wallet_registry_summary = _build_wallet_registry_summary(config, store, trades)
    wallet_registry_summary["auto_feed"] = wallet_discovery_auto_feed
    open_positions_at_start = store.get_open_positions()
    db_diagnostics = {
        "path": args.db,
        "is_ephemeral": args.db.startswith("/tmp/"),
        "open_position_count_at_start": len(open_positions_at_start),
    }
    var_95 = store.get_portfolio_var(confidence_level=0.95)
    logger.info(f"Portfolio VaR (95%): {var_95:.2f} USD")

    current_positions = [
        {"topic": pos.topic, "exposure_usd": pos.entry_amount_usd}
        for pos in open_positions_at_start
    ]
    basket_planner = BasketManagerPlanner(config)
    rebalance_actions = basket_planner.rebalance(current_positions)
    if rebalance_actions:
        logger.info(f"Rebalancing actions suggested: {len(rebalance_actions)}")

    started = time.perf_counter()
    scorer = WalletQualityScorer(
        config.filters,
        config.consensus.recency_half_life_seconds,
    )
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
    execution_diagnostics: dict[str, int] = {}
    planner_ran = False

    started = time.perf_counter()
    if args.live_trading:
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
                LiveOrderExecutor(config.execution, config.live_data).execute(close_intents)
                if close_intents
                else []
            )
            closed_positions = {
                (result.market_id, result.token_id)
                for result in close_results
                if _creates_or_updates_paper_position(result)
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
                )
            current_exposure_usd = store.get_total_exposure()

        fresh_candidates, skipped_duplicate_signals = _filter_duplicate_candidates(
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
        _mark_execution_intents_seen(store, execution_intents)
        execution_results = LiveOrderExecutor(
            config.execution,
            config.live_data,
        ).execute(execution_intents)
        _persist_execution_side_effects(store, config, execution_results)
    timings["execution_ms"] = _elapsed_ms(started)

    started = time.perf_counter()
    store.save_cycle_payloads(
        copy_candidates,
        arbitrage_opportunities,
        execution_results + close_results,
    )
    open_positions = _decorate_positions_with_titles(store.get_open_positions(), markets)
    db_diagnostics["open_position_count_at_end"] = len(open_positions)
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
        "db": db_diagnostics,
        "live_input_diagnostics": live_input_diagnostics,
        "scoring_diagnostics": scoring_diagnostics,
        "copy_engine_diagnostics": copy_engine_diagnostics,
        "wallet_registry": wallet_registry_summary,
        "execution": {
            "live_trading_requested": bool(args.live_trading),
            "execution_enabled": bool(config.execution and config.execution.enabled),
            "planner_ran": planner_ran,
            "diagnostics": execution_diagnostics,
        },
        "portfolio_summary": _portfolio_summary(store, config),
        "wallet_qualities": {
            wallet: quality.__dict__
            for wallet, quality in wallet_qualities.items()
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
        "open_position_pnl": _open_position_pnl(open_positions),
    }
    _log_event(
        "predictcel_cycle_latency",
        {
            "mode": output["mode"],
            "db": db_diagnostics,
            "latency_ms": timings,
            "summary": summary,
            "live_input_diagnostics": _compact_live_input_diagnostics(
                live_input_diagnostics
            ),
            "scoring_diagnostics": _compact_scoring_diagnostics(scoring_diagnostics),
            "copy_engine_diagnostics": copy_engine_diagnostics,
            "wallet_registry": wallet_registry_summary,
            "execution": output["execution"],
        },
    )
    logger.info(
        "Cycle complete",
        extra={
            "summary": summary,
            "timings": timings,
            "metrics": metrics.get_metrics(),
            "db": db_diagnostics,
            "wallet_registry": wallet_registry_summary,
        },
    )
    print(json.dumps(_compact_cycle_output(output), sort_keys=True, default=str), flush=True)


def _run_wallet_discovery(argv: list[str]) -> None:
    started = time.perf_counter()
    args = build_discovery_parser().parse_args(argv)
    config = load_config(args.config)
    pipeline = WalletDiscoveryPipeline(config)
    store = SignalStore(args.db) if args.db else None
    if store is not None:
        _configure_discovery_pipeline_current_wallets(config, store, pipeline)
    results = pipeline.run()
    files = pipeline.write_reports(
        args.output_dir,
        args.config,
        args.config_output,
        results=results,
    )
    candidates, assignments, actions = results
    registry_ingestion = {
        "enabled": bool(args.db),
        "persisted": False,
        "mode": getattr(getattr(config, "wallet_discovery", None), "mode", "auto_update"),
        "discovered_wallets_ingested": 0,
        "new_registry_entries": 0,
        "new_explorer_memberships": 0,
        "manager_actions_applied": 0,
        "manager_action_counts": {},
        "skipped_existing_wallets": 0,
    }
    if args.db and _wallet_discovery_registry_persistence_enabled(config):
        before_registry_wallets = {
            entry.wallet for entry in store.load_wallet_registry_entries()
        }
        before_explorer_memberships = {
            (membership.topic, membership.wallet)
            for membership in store.load_basket_memberships()
            if membership.tier == "explorer"
        }
        updated_entries, updated_memberships = ingest_wallet_discovery_inputs(
            config,
            store,
            candidates,
            assignments,
        )
        accepted_wallets = {
            candidate.wallet_address
            for candidate in candidates
            if not candidate.rejected_reasons
        }
        after_registry_wallets = {entry.wallet for entry in updated_entries}
        after_explorer_memberships = {
            (membership.topic, membership.wallet)
            for membership in updated_memberships
            if membership.tier == "explorer"
        }
        _, action_diagnostics = apply_basket_manager_actions_to_memberships(
            config,
            store,
            actions,
        )
        registry_ingestion = {
            "enabled": True,
            "persisted": True,
            "mode": getattr(config.wallet_discovery, "mode", "auto_update"),
            "discovered_wallets_ingested": len(accepted_wallets),
            "new_registry_entries": len(after_registry_wallets - before_registry_wallets),
            "new_explorer_memberships": len(
                after_explorer_memberships - before_explorer_memberships
            ),
            "manager_actions_applied": int(action_diagnostics["actions_applied"]),
            "manager_action_counts": action_diagnostics["action_counts"],
            "skipped_existing_wallets": len(
                accepted_wallets & before_registry_wallets
            ),
        }
    print(
        json.dumps(
            {
                "mode": "wallet_discovery",
                "reports": files,
                "registry_ingestion": registry_ingestion,
                "latency_ms": {"total_cycle_ms": _elapsed_ms(started)},
            },
            indent=2,
        )
    )


def _auto_feed_wallet_registry_from_discovery(
    config: Any,
    store: SignalStore,
) -> dict[str, Any]:
    mode = getattr(getattr(config, "wallet_discovery", None), "mode", "auto_update")
    enabled = bool(
        getattr(config.wallet_registry, "enabled", False)
        and getattr(config.wallet_discovery, "enabled", False)
    )
    persisted = bool(enabled and mode == "auto_update")
    diagnostics = {
        "enabled": enabled,
        "persisted": persisted,
        "mode": mode,
        "ran": False,
        "discovered_wallets_ingested": 0,
        "new_registry_entries": 0,
        "new_explorer_memberships": 0,
        "manager_actions_applied": 0,
        "manager_action_counts": {},
        "skipped_existing_wallets": 0,
        "error": None,
    }
    if not persisted:
        return diagnostics

    try:
        pipeline = WalletDiscoveryPipeline(config)
        _configure_discovery_pipeline_current_wallets(config, store, pipeline)
        candidates, assignments, actions = pipeline.run()
        before_registry_wallets = {
            entry.wallet for entry in store.load_wallet_registry_entries()
        }
        before_explorer_memberships = {
            (membership.topic, membership.wallet)
            for membership in store.load_basket_memberships()
            if membership.tier == "explorer"
        }
        updated_entries, updated_memberships = ingest_wallet_discovery_inputs(
            config,
            store,
            candidates,
            assignments,
        )
        accepted_wallets = {
            candidate.wallet_address
            for candidate in candidates
            if not candidate.rejected_reasons
        }
        after_registry_wallets = {entry.wallet for entry in updated_entries}
        after_explorer_memberships = {
            (membership.topic, membership.wallet)
            for membership in updated_memberships
            if membership.tier == "explorer"
        }
        _, action_diagnostics = apply_basket_manager_actions_to_memberships(
            config,
            store,
            actions,
        )
        diagnostics.update(
            {
                "ran": True,
                "discovered_wallets_ingested": len(accepted_wallets),
                "new_registry_entries": len(after_registry_wallets - before_registry_wallets),
                "new_explorer_memberships": len(
                    after_explorer_memberships - before_explorer_memberships
                ),
                "manager_actions_applied": int(action_diagnostics["actions_applied"]),
                "manager_action_counts": action_diagnostics["action_counts"],
                "skipped_existing_wallets": len(
                    accepted_wallets & before_registry_wallets
                ),
            }
        )
    except Exception as exc:
        diagnostics["error"] = f"{type(exc).__name__}: {exc}"
    return diagnostics


def _wallet_discovery_registry_persistence_enabled(config: Any) -> bool:
    wallet_discovery = getattr(config, "wallet_discovery", None)
    wallet_registry = getattr(config, "wallet_registry", None)
    if wallet_discovery is None or wallet_registry is None:
        return False
    return bool(
        getattr(wallet_registry, "enabled", False)
        and getattr(wallet_discovery, "enabled", False)
        and getattr(wallet_discovery, "mode", "auto_update") == "auto_update"
    )


def _configure_discovery_pipeline_current_wallets(
    config: Any,
    store: SignalStore,
    pipeline: Any,
) -> None:
    if not hasattr(pipeline, "set_current_wallets_by_topic"):
        return
    pipeline.set_current_wallets_by_topic(
        _current_wallets_by_topic_for_discovery(config, store)
    )


def _current_wallets_by_topic_for_discovery(
    config: Any,
    store: SignalStore,
) -> dict[str, set[str]]:
    if not getattr(config.wallet_registry, "enabled", False):
        return {
            basket.topic: {str(wallet).lower() for wallet in basket.wallets}
            for basket in config.baskets
        }

    memberships = store.load_basket_memberships()
    current_by_topic: dict[str, set[str]] = defaultdict(set)
    for membership in memberships:
        if membership.active:
            current_by_topic[membership.topic].add(membership.wallet.lower())
    if current_by_topic:
        return dict(current_by_topic)
    return {
        basket.topic: {str(wallet).lower() for wallet in basket.wallets}
        for basket in config.baskets
    }


def _wallet_topics_for_live_inputs(
    config: Any,
    store: SignalStore | None = None,
) -> tuple[dict[str, list[str]], str]:
    if (
        getattr(config.wallet_registry, "enabled", False)
        and store is not None
        and hasattr(store, "load_basket_memberships")
    ):
        memberships = store.load_basket_memberships()
        raw_topic_by_wallet: dict[str, list[str]] = {}
        for membership in memberships:
            if not getattr(membership, "active", False):
                continue
            wallet = str(membership.wallet).strip()
            if not _is_evm_address(wallet):
                continue
            topics = raw_topic_by_wallet.setdefault(wallet, [])
            if membership.topic not in topics:
                topics.append(membership.topic)
        if raw_topic_by_wallet:
            return raw_topic_by_wallet, "registry_memberships"

    raw_topic_by_wallet = {}
    for basket in config.baskets:
        for wallet in basket.wallets:
            normalized_wallet = str(wallet).strip()
            if not _is_evm_address(normalized_wallet):
                continue
            topics = raw_topic_by_wallet.setdefault(normalized_wallet, [])
            if basket.topic not in topics:
                topics.append(basket.topic)
    return raw_topic_by_wallet, "config_baskets"


def _build_wallet_registry_summary(
    config: Any,
    store: SignalStore,
    trades: list[Any],
) -> dict[str, Any]:
    """Build compact wallet registry diagnostics without changing trading behavior."""
    if not getattr(config.wallet_registry, "enabled", False):
        return {
            "enabled": False,
            "registry_wallet_count": 0,
            "memberships_by_topic": {},
            "basket_health": {},
            "live_roster_by_topic": {},
            "promotion_watch_by_topic": {},
            "basket_promotion_by_topic": {},
        }

    captured_at = datetime.now(UTC)
    existing_registry_entries = store.load_wallet_registry_entries()
    existing_memberships = store.load_basket_memberships()
    if config.wallet_registry.seed_from_baskets:
        existing_registry_entries = _ensure_static_registry_bootstrap(
            config,
            store,
            existing_registry_entries,
            captured_at=captured_at,
        )
        existing_memberships = _ensure_static_membership_bootstrap(
            config,
            store,
            existing_memberships,
            captured_at=captured_at,
        )

    registry_entries = refresh_registry_entries_from_trades(
        config,
        store,
        trades,
        captured_at=captured_at,
    ) or existing_registry_entries
    memberships = store.load_basket_memberships() or existing_memberships
    basket_health = compute_basket_health_from_static_memberships(
        config,
        memberships,
        trades,
        captured_at=captured_at,
    )
    live_roster = build_live_basket_roster(
        config,
        memberships,
        trades,
        registry_entries=registry_entries,
        captured_at=captured_at,
    )
    promotion_watch_by_topic = _promotion_watch_by_topic(
        memberships,
        registry_entries,
        live_roster,
    )
    basket_promotion_by_topic = {
        topic: _basket_promotion_as_dict(recommendation)
        for topic, recommendation in recommend_basket_promotions(
            config,
            memberships,
            trades,
            registry_entries=registry_entries,
            captured_at=captured_at,
        ).items()
    }
    store.save_basket_health(basket_health)
    latest_health = store.latest_basket_health()
    return {
        "enabled": True,
        "registry_wallet_count": len(registry_entries),
        "memberships_by_topic": _membership_counts_by_topic(memberships),
        "basket_health": {
            topic: _basket_health_as_dict(health)
            for topic, health in latest_health.items()
        },
        "live_roster_by_topic": live_roster,
        "promotion_watch_by_topic": promotion_watch_by_topic,
        "basket_promotion_by_topic": basket_promotion_by_topic,
    }


def _ensure_static_registry_bootstrap(
    config: Any,
    store: SignalStore,
    existing_entries: list[WalletRegistryEntry],
    *,
    captured_at: datetime,
) -> list[WalletRegistryEntry]:
    if any(entry.source_type == "static_basket" for entry in existing_entries):
        return existing_entries

    entries_by_wallet = {entry.wallet: entry for entry in existing_entries}
    updated_entries = list(existing_entries)
    for basket in config.baskets:
        for wallet in basket.wallets:
            normalized_wallet = str(wallet).strip()
            if not normalized_wallet or normalized_wallet in entries_by_wallet:
                continue
            updated_entries.append(
                WalletRegistryEntry(
                    wallet=normalized_wallet,
                    source_type="static_basket",
                    source_ref="config.baskets",
                    trust_seed=1.0,
                    status="active",
                    first_seen_at=captured_at,
                    last_seen_trade_at=None,
                    last_scored_at=None,
                    notes="seeded from static basket config",
                )
            )
            entries_by_wallet[normalized_wallet] = updated_entries[-1]
    if len(updated_entries) == len(existing_entries):
        return existing_entries
    updated_entries.sort(key=lambda entry: entry.wallet)
    store.upsert_wallet_registry_entries(updated_entries)
    return updated_entries


def _ensure_static_membership_bootstrap(
    config: Any,
    store: SignalStore,
    existing_memberships: list[BasketMembership],
    *,
    captured_at: datetime,
) -> list[BasketMembership]:
    if existing_memberships and any(
        membership.promotion_reason != "wallet discovery assignment"
        for membership in existing_memberships
    ):
        return existing_memberships

    memberships_by_key = {
        (membership.topic, membership.wallet): membership for membership in existing_memberships
    }
    updated_memberships = list(existing_memberships)
    for basket in config.baskets:
        for rank, wallet in enumerate(basket.wallets, start=1):
            normalized_wallet = str(wallet).strip()
            membership_key = (basket.topic, normalized_wallet)
            if not normalized_wallet or membership_key in memberships_by_key:
                continue
            updated_memberships.append(
                BasketMembership(
                    topic=basket.topic,
                    wallet=normalized_wallet,
                    tier="core",
                    rank=rank,
                    active=True,
                    joined_at=captured_at,
                    effective_until=None,
                    promotion_reason="seeded from static basket config",
                    demotion_reason="",
                )
            )
            memberships_by_key[membership_key] = updated_memberships[-1]
    if len(updated_memberships) == len(existing_memberships):
        return existing_memberships
    updated_memberships.sort(
        key=lambda membership: (membership.topic, membership.rank, membership.wallet)
    )
    store.upsert_basket_memberships(updated_memberships)
    return updated_memberships


def _membership_counts_by_topic(memberships: list[Any]) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = defaultdict(
        lambda: {"core": 0, "rotating": 0, "backup": 0, "explorer": 0}
    )
    for membership in memberships:
        tier = str(membership.tier)
        if tier not in counts[membership.topic]:
            counts[membership.topic][tier] = 0
        counts[membership.topic][tier] += 1
    return {topic: dict(values) for topic, values in counts.items()}


def _basket_health_as_dict(health: BasketHealth) -> dict[str, Any]:
    payload = asdict(health)
    payload["captured_at"] = health.captured_at.isoformat()
    return payload


def _basket_promotion_as_dict(recommendation: Any) -> dict[str, Any]:
    payload = asdict(recommendation)
    payload["recommended_wallets"] = list(recommendation.recommended_wallets)
    payload["missing_requirements"] = list(recommendation.missing_requirements)
    return payload


def _promotion_watch_by_topic(
    memberships: list[Any],
    registry_entries: list[Any],
    live_roster_by_topic: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    explorer_counts_by_topic: dict[str, int] = defaultdict(int)
    discovery_explorer_counts_by_topic: dict[str, int] = defaultdict(int)
    registry_entries_by_wallet = {
        entry.wallet: entry for entry in registry_entries
    }
    for membership in memberships:
        if not membership.active or membership.tier != "explorer":
            continue
        explorer_counts_by_topic[membership.topic] += 1
        entry = registry_entries_by_wallet.get(membership.wallet)
        if entry is not None and entry.source_type == "wallet_discovery":
            discovery_explorer_counts_by_topic[membership.topic] += 1

    watch: dict[str, dict[str, Any]] = {}
    for topic, explorer_count in explorer_counts_by_topic.items():
        roster_entry = live_roster_by_topic.get(topic, {})
        if not roster_entry:
            continue
        if not roster_entry.get("needs_refresh"):
            continue
        discovery_explorer_count = discovery_explorer_counts_by_topic.get(topic, 0)
        if discovery_explorer_count <= 0:
            continue
        watch[topic] = {
            "explorer_wallet_count": explorer_count,
            "wallet_discovery_explorer_wallet_count": discovery_explorer_count,
            "live_eligible_wallet_count": int(roster_entry.get("live_eligible_wallet_count", 0)),
            "fresh_core_wallet_count": int(roster_entry.get("fresh_core_wallet_count", 0)),
            "reason": "bench_depth_available",
        }
    return watch


def _mark_execution_intents_seen(store: SignalStore, execution_intents: list) -> None:
    if execution_intents:
        store.mark_signals_seen(
            (intent.market_id, intent.topic, intent.side)
            for intent in execution_intents
        )


def _persist_execution_side_effects(
    store: SignalStore,
    config: Any,
    execution_results: list,
) -> None:
    now = datetime.now(UTC)
    position_config = config.execution.position
    positions: list[Position] = []
    for result in execution_results:
        if _creates_or_updates_paper_position(result):
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
                    result.market_title,
                )
            )
    if positions:
        store.save_positions(positions)


def _portfolio_summary(store: SignalStore, config: Any) -> dict:
    if config.execution is None or config.execution.exposure is None:
        return store.get_portfolio_summary(starting_bankroll_usd=0.0)
    return store.get_portfolio_summary(
        starting_bankroll_usd=config.execution.exposure.max_total_exposure_usd
    )


def _load_live_inputs(config, store: SignalStore | None = None):
    if config.live_data is None:
        raise ValueError("--live-data was requested but live_data is not configured.")
    client = PolymarketPublicClient(
        config.live_data.gamma_base_url,
        config.live_data.data_base_url,
        config.live_data.clob_base_url,
        config.live_data.request_timeout_seconds,
    )

    invalid_wallets: list[str] = []
    raw_topic_by_wallet, wallet_source = _wallet_topics_for_live_inputs(config, store)
    if wallet_source == "config_baskets":
        for basket in config.baskets:
            for wallet in basket.wallets:
                if not _is_evm_address(wallet):
                    invalid_wallets.append(wallet)
    elif store is not None and hasattr(store, "load_basket_memberships"):
        for membership in store.load_basket_memberships():
            wallet = str(membership.wallet).strip()
            if wallet and not _is_evm_address(wallet):
                invalid_wallets.append(wallet)

    wallet_payloads = _fetch_wallet_payloads(
        client,
        list(raw_topic_by_wallet),
        config.live_data.trade_limit,
    )
    trades = build_wallet_trades(wallet_payloads, raw_topic_by_wallet)
    trade_market_ids = extract_trade_market_ids(wallet_payloads)
    trade_market_slugs = extract_trade_market_slugs(wallet_payloads)

    active_market_rows = client.fetch_active_markets(config.live_data.market_limit)
    markets = build_market_snapshots(active_market_rows)

    missing_trade_market_ids = [
        market_id for market_id in trade_market_ids if market_id not in markets
    ]
    supplemental_rows = client.fetch_markets_by_identifiers(missing_trade_market_ids)
    if supplemental_rows:
        markets.update(build_market_snapshots(supplemental_rows))

    missing_trade_market_slugs = [
        market_slug for market_slug in trade_market_slugs if market_slug not in markets
    ]
    supplemental_slug_rows = client.fetch_markets_by_slugs(missing_trade_market_slugs)
    if supplemental_slug_rows:
        markets.update(build_market_snapshots(supplemental_slug_rows))

    relevant_market_keys = {
        market_key
        for market_key in (*trade_market_ids, *trade_market_slugs)
        if market_key in markets
    }
    if relevant_market_keys:
        relevant_snapshots = {
            market_id: markets[market_id] for market_id in relevant_market_keys
        }
        enriched_relevant = enrich_market_snapshots_with_orderbooks(
            relevant_snapshots,
            client,
        )
        markets.update(enriched_relevant)
        _propagate_canonical_market_updates(markets, enriched_relevant.values())

    trade_market_keys = sorted(set(trade_market_ids + trade_market_slugs))
    matched_trade_market_keys = [
        market_key for market_key in trade_market_keys if market_key in markets
    ]
    wallets_with_payloads = sum(1 for items in wallet_payloads.values() if items)
    wallets_with_parsed_trades = len({trade.wallet for trade in trades})
    unique_market_snapshots = {snapshot.market_id: snapshot for snapshot in markets.values()}
    relevant_canonical_market_ids = {
        markets[market_key].market_id for market_key in relevant_market_keys
    }
    relevant_snapshots = [
        unique_market_snapshots[market_id]
        for market_id in sorted(relevant_canonical_market_ids)
    ]
    orderbook_ready_markets = sum(
        1 for snapshot in relevant_snapshots if snapshot.orderbook_ready
    )
    markets_with_yes_depth = sum(
        1
        for snapshot in relevant_snapshots
        if snapshot.yes_ask > 0 and snapshot.yes_ask_size > 0
    )
    markets_with_no_depth = sum(
        1
        for snapshot in relevant_snapshots
        if snapshot.no_ask > 0 and snapshot.no_ask_size > 0
    )
    orderbook_probe_samples = []
    if relevant_snapshots and orderbook_ready_markets == 0:
        orderbook_probe_samples = _build_orderbook_probe_samples(
            client,
            relevant_snapshots,
        )
    diagnostics = {
        "wallet_source": wallet_source,
        "requested_wallets": len(raw_topic_by_wallet),
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
        "multi_topic_wallets": sum(
            1 for topics in raw_topic_by_wallet.values() if len(topics) > 1
        ),
        "relevant_markets_enriched": len(relevant_snapshots),
        "orderbook_ready_markets": orderbook_ready_markets,
        "markets_with_yes_depth": markets_with_yes_depth,
        "markets_with_no_depth": markets_with_no_depth,
        "orderbook_probe_samples": orderbook_probe_samples,
        "market_crossref": {
            "unique_trade_market_keys": len(trade_market_keys),
            "matched_count": len(matched_trade_market_keys),
            "match_rate_pct": round(
                (len(matched_trade_market_keys) / len(trade_market_keys)) * 100,
                1,
            )
            if trade_market_keys
            else 0.0,
        },
    }
    return trades, markets, diagnostics


def _propagate_canonical_market_updates(markets: dict[str, Any], updated_snapshots: Any) -> None:
    canonical_updates = {
        snapshot.market_id: snapshot
        for snapshot in updated_snapshots
        if getattr(snapshot, "market_id", "")
    }
    if not canonical_updates:
        return
    for market_key, snapshot in list(markets.items()):
        canonical_snapshot = canonical_updates.get(getattr(snapshot, "market_id", ""))
        if canonical_snapshot is not None:
            markets[market_key] = canonical_snapshot


def _build_orderbook_probe_samples(
    client: PolymarketPublicClient,
    snapshots: list[Any],
    limit: int = 3,
) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    seen_market_ids: set[str] = set()
    for snapshot in snapshots:
        if snapshot.market_id in seen_market_ids or snapshot.orderbook_ready:
            continue
        seen_market_ids.add(snapshot.market_id)
        samples.append(
            {
                "market_id": snapshot.market_id,
                "market_title": snapshot.title,
                "yes_token_id": snapshot.yes_token_id,
                "no_token_id": snapshot.no_token_id,
                "yes_book": _probe_token_orderbook(client, snapshot.yes_token_id),
                "no_book": _probe_token_orderbook(client, snapshot.no_token_id),
                "yes_token_lookup": _probe_token_lookup(client, snapshot.yes_token_id),
                "no_token_lookup": _probe_token_lookup(client, snapshot.no_token_id),
            }
        )
        if len(samples) >= limit:
            break
    return samples


def _make_probe_client(client: PolymarketPublicClient) -> PolymarketPublicClient:
    return PolymarketPublicClient(
        client.gamma_base_url,
        client.data_base_url,
        client.clob_base_url,
        client.timeout_seconds,
        client.max_retries,
        client.retry_base_delay_seconds,
    )


def _probe_token_orderbook(client: PolymarketPublicClient, token_id: str) -> dict[str, Any]:
    token_id = str(token_id).strip()
    if not token_id:
        return {"missing_token_id": True}
    try:
        probe_client = _make_probe_client(client)
        return _summarize_order_book_payload(probe_client.fetch_order_book(token_id))
    except Exception as exc:
        return {"token_id": token_id, "error": f"{type(exc).__name__}: {exc}"}


def _probe_token_lookup(client: PolymarketPublicClient, token_id: str) -> dict[str, Any]:
    token_id = str(token_id).strip()
    if not token_id:
        return {"missing_token_id": True}
    try:
        probe_client = _make_probe_client(client)
        rows = probe_client.fetch_markets_by_clob_token_ids([token_id], chunk_size=1)
    except Exception as exc:
        return {"token_id": token_id, "error": f"{type(exc).__name__}: {exc}"}
    return _summarize_market_lookup_rows(rows, token_id)


def _summarize_order_book_payload(payload: dict[str, Any]) -> dict[str, Any]:
    bids = payload.get("bids") if isinstance(payload, dict) else None
    asks = payload.get("asks") if isinstance(payload, dict) else None
    return {
        "raw_keys": sorted(payload.keys())[:8] if isinstance(payload, dict) else [],
        "market": str(payload.get("market") or "") if isinstance(payload, dict) else "",
        "asset_id": str(payload.get("asset_id") or payload.get("assetId") or "")
        if isinstance(payload, dict)
        else "",
        "bid_levels": len(bids) if isinstance(bids, list) else 0,
        "ask_levels": len(asks) if isinstance(asks, list) else 0,
        "best_bid_price": _first_book_level_value(bids, "price"),
        "best_bid_size": _first_book_level_value(bids, "size"),
        "best_ask_price": _first_book_level_value(asks, "price"),
        "best_ask_size": _first_book_level_value(asks, "size"),
    }


def _first_book_level_value(levels: Any, key: str) -> float:
    if not isinstance(levels, list) or not levels:
        return 0.0
    best = levels[0]
    if not isinstance(best, dict):
        return 0.0
    return float(best.get(key) or 0.0)


def _summarize_market_lookup_rows(rows: list[dict[str, Any]], token_id: str) -> dict[str, Any]:
    if not rows:
        return {"matched_rows": 0}
    row = rows[0]
    token_ids = _market_row_token_ids(row)
    return {
        "matched_rows": len(rows),
        "condition_id": str(
            row.get("conditionId") or row.get("condition_id") or row.get("id") or ""
        ),
        "slug": str(row.get("slug") or row.get("marketSlug") or ""),
        "question": str(row.get("question") or row.get("title") or ""),
        "token_ids": token_ids,
        "matched_input_token": token_id in token_ids,
    }


def _market_row_token_ids(row: dict[str, Any]) -> list[str]:
    raw = row.get("clobTokenIds") or row.get("tokenIds")
    if isinstance(raw, list):
        return [str(token) for token in raw[:2] if str(token).strip()]
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            return [str(token) for token in parsed[:2] if str(token).strip()]

    tokens = row.get("tokens")
    if isinstance(tokens, list):
        token_ids: list[str] = []
        for token in tokens[:2]:
            if not isinstance(token, dict):
                continue
            token_id = token.get("token_id") or token.get("id") or token.get("tokenId")
            if token_id and str(token_id).strip():
                token_ids.append(str(token_id))
        return token_ids
    return []


def _fetch_wallet_payloads(
    client: PolymarketPublicClient,
    wallets: list[str],
    limit: int,
) -> dict[str, list[dict[str, Any]]]:
    if not wallets:
        return {}
    payloads: dict[str, list[dict[str, Any]]] = {}
    workers = max(1, min(16, len(wallets)))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(client.fetch_wallet_trades, wallet, limit): wallet
            for wallet in wallets
        }
        for future in as_completed(futures):
            wallet = futures[future]
            try:
                payloads[wallet] = future.result()
            except Exception:
                payloads[wallet] = []
    return payloads


def _filter_duplicate_candidates(store: SignalStore, candidates: list) -> tuple[list, int]:
    fingerprints = [
        store.make_signal_fingerprint(candidate.market_id, candidate.topic, candidate.side)
        for candidate in candidates
    ]
    recent_fingerprints = store.has_recent_signals(
        [(candidate.market_id, candidate.topic, candidate.side) for candidate in candidates]
    )
    fresh = []
    seen_in_batch: set[str] = set()
    for candidate, fingerprint in zip(candidates, fingerprints):
        if fingerprint in recent_fingerprints or fingerprint in seen_in_batch:
            continue
        fresh.append(candidate)
        seen_in_batch.add(fingerprint)
    return fresh, len(candidates) - len(fresh)


def _is_trusted_execution_result(result) -> bool:
    return str(result.status).strip().lower() in TRUSTED_POSITION_STATUSES and bool(
        str(result.order_id).strip()
    )


def _creates_or_updates_paper_position(result) -> bool:
    return str(result.status).strip().lower() == "dry_run" or _is_trusted_execution_result(
        result
    )


def _is_evm_address(value: str) -> bool:
    value = str(value).strip()
    return len(value) == 42 and value.startswith("0x") and all(
        char in HEX_CHARS for char in value[2:]
    )


def _decorate_positions_with_titles(
    positions: list[Position],
    markets: dict[str, Any],
) -> list[Position]:
    decorated: list[Position] = []
    for pos in positions:
        market = markets.get(pos.market_id)
        title = pos.market_title or (market.title if market is not None else "")
        decorated.append(
            Position(
                pos.market_id,
                pos.topic,
                pos.side,
                pos.token_id,
                pos.entry_price,
                pos.entry_amount_usd,
                pos.current_price,
                pos.unrealized_pnl,
                pos.opened_at,
                pos.last_updated,
                pos.take_profit_pct,
                pos.stop_loss_pct,
                pos.max_hold_minutes,
                pos.status,
                title,
            )
        )
    return decorated


def _compact_cycle_output(output: dict[str, Any]) -> dict[str, Any]:
    compact = {
        "mode": output["mode"],
        "summary": output["summary"],
        "latency_ms": output["latency_ms"],
        "db": output["db"],
        "portfolio_summary": output["portfolio_summary"],
    }
    if output.get("live_input_diagnostics"):
        compact["live_input_diagnostics"] = _compact_live_input_diagnostics(
            output["live_input_diagnostics"]
        )
    if output.get("scoring_diagnostics"):
        compact["scoring_diagnostics"] = _compact_scoring_diagnostics(
            output["scoring_diagnostics"]
        )
    if output.get("copy_engine_diagnostics"):
        compact["copy_engine_diagnostics"] = output["copy_engine_diagnostics"]
    if output.get("wallet_registry"):
        compact["wallet_registry"] = output["wallet_registry"]
    if output.get("execution"):
        compact["execution"] = output["execution"]
    top_wallet_qualities = _top_wallet_qualities(output.get("wallet_qualities", {}))
    if top_wallet_qualities:
        compact["wallet_qualities"] = top_wallet_qualities
    if output.get("open_position_pnl"):
        compact["open_position_pnl"] = output["open_position_pnl"]
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
        "supplemental_market_ids_requested": diagnostics.get(
            "supplemental_market_ids_requested",
            0,
        ),
        "supplemental_market_rows_loaded": diagnostics.get(
            "supplemental_market_rows_loaded",
            0,
        ),
        "supplemental_market_slugs_requested": diagnostics.get(
            "supplemental_market_slugs_requested",
            0,
        ),
        "supplemental_slug_rows_loaded": diagnostics.get(
            "supplemental_slug_rows_loaded",
            0,
        ),
        "market_cache_entries": diagnostics.get("market_cache_entries", 0),
        "relevant_markets_enriched": diagnostics.get("relevant_markets_enriched", 0),
        "orderbook_ready_markets": diagnostics.get("orderbook_ready_markets", 0),
        "markets_with_yes_depth": diagnostics.get("markets_with_yes_depth", 0),
        "markets_with_no_depth": diagnostics.get("markets_with_no_depth", 0),
        "market_crossref": diagnostics.get("market_crossref", {}),
    }
    if diagnostics.get("orderbook_probe_samples"):
        compact["orderbook_probe_samples"] = diagnostics["orderbook_probe_samples"][:2]
    skipped_invalid_wallets = diagnostics.get("skipped_invalid_wallets", 0)
    if skipped_invalid_wallets:
        compact["skipped_invalid_wallets"] = skipped_invalid_wallets
        compact["sample_skipped_invalid_wallets"] = diagnostics.get(
            "sample_skipped_invalid_wallets",
            [],
        )[:3]
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


def _top_wallet_qualities(
    wallet_qualities: dict[str, dict[str, Any]],
    limit: int = 3,
) -> dict[str, dict[str, Any]]:
    ranked = sorted(
        wallet_qualities.items(),
        key=lambda item: float(item[1].get("score", 0.0)),
        reverse=True,
    )[:limit]
    return {wallet: quality for wallet, quality in ranked}


def _open_position_pnl(positions: list[Position]) -> list[dict[str, Any]]:
    return [
        {
            "market_id": pos.market_id,
            "market_title": pos.market_title,
            "topic": pos.topic,
            "side": pos.side,
            "entry_amount_usd": pos.entry_amount_usd,
            "entry_price": pos.entry_price,
            "current_price": pos.current_price,
            "unrealized_pnl_usd": pos.unrealized_pnl,
            "status": pos.status,
        }
        for pos in positions
    ]


def _elapsed_ms(started: float) -> int:
    return int((time.perf_counter() - started) * 1000)


def _log_event(event: str, payload: dict[str, Any]) -> None:
    print(
        json.dumps(
            {"event": event, "ts": datetime.now(UTC).isoformat(), **payload},
            sort_keys=True,
            default=str,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()

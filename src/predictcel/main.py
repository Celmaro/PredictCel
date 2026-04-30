"""Main entry point for PredictCel.

Provides the CLI interface and orchestrates the full trading cycle:
discovery, scoring, basket assignment, market evaluation, and execution.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from collections import defaultdict
from concurrent.futures import as_completed
from datetime import UTC, datetime
from typing import Any

try:
    from Crypto.Hash import keccak
except ImportError:  # pragma: no cover - optional dependency at runtime
    keccak = None

from .arb_sidecar import ArbitrageSidecar
from .alerting import get_alert_manager
from .basket_manager import BasketManagerPlanner
from .config import load_config
from .copy_engine import CopyEngine
from .cycle_runtime import CycleRuntimeHooks, run_loaded_cycle
from .cycle_support import (
    analysis_trades as _analysis_trades_impl,
    creates_or_updates_paper_position as _creates_or_updates_paper_position_impl,
    decorate_positions_with_titles as _decorate_positions_with_titles_impl,
    filter_duplicate_candidates as _filter_duplicate_candidates_impl,
    is_trusted_execution_result as _is_trusted_execution_result_impl,
    mark_execution_intents_seen as _mark_execution_intents_seen_impl,
    open_position_pnl as _open_position_pnl_impl,
    persist_execution_side_effects as _persist_execution_side_effects_impl,
    portfolio_summary as _portfolio_summary_impl,
)
from .execution import ExecutionPlanner, ExitRunner, LiveOrderExecutor, intents_as_dicts
from .live_inputs import LiveInputHooks, load_live_inputs as _load_live_inputs_impl
from .markets import load_market_snapshots
from .models import Position
from .polymarket import (
    PolymarketPublicClient,
    build_market_snapshots,
    build_wallet_trades,
    enrich_market_snapshots_with_orderbooks,
    extract_trade_market_ids,
    extract_trade_market_slugs,
    get_polymarket_metrics,
    _trade_market_id_source,
)
from .registry_runtime import (
    basket_health_as_dict as _basket_health_as_dict_impl,
    basket_promotion_as_dict as _basket_promotion_as_dict_impl,
    build_wallet_registry_summary as _build_wallet_registry_summary_impl,
    ensure_static_membership_bootstrap as _ensure_static_membership_bootstrap_impl,
    ensure_static_registry_bootstrap as _ensure_static_registry_bootstrap_impl,
    membership_counts_by_topic as _membership_counts_by_topic_impl,
    promotion_watch_by_topic as _promotion_watch_by_topic_impl,
)
from .scoring import WalletQualityScorer
from .storage import SignalStore
from .runtime import shared_io_executor
from .wallet_discovery import WalletDiscoveryPipeline
from .wallet_registry import (
    apply_basket_manager_actions_to_memberships,
    build_live_basket_roster,
    compute_basket_health_from_static_memberships,
    ingest_wallet_discovery_inputs,
    rebalance_memberships_from_live_roster,
    recommend_basket_promotions,
    refresh_registry_entries_from_trades,
)
from .wallets import load_wallet_trades

__all__ = ["main"]


TRUSTED_POSITION_STATUSES = {"filled"}
HEX_CHARS = set("0123456789abcdefABCDEF")
DEFAULT_LIVE_WALLET_FETCH_FAILURE_RATIO_THRESHOLD = 0.5
LIVE_WALLET_FETCH_FAILURE_RATIO_ENV_VAR = "PREDICTCEL_LIVE_FETCH_FAILURE_RATIO"
SENSITIVE_CYCLE_OUTPUT_ENV_VAR = "PREDICTCEL_LOG_SENSITIVE"
FAIL_CLOSED_LIVE_DATA_ENV_VAR = "PREDICTCEL_FAIL_CLOSED_LIVE_DATA"
RETENTION_MAX_ROWS_ENV_VAR = "PREDICTCEL_RETENTION_MAX_ROWS"
RETENTION_ANALYZE_ENV_VAR = "PREDICTCEL_RETENTION_ANALYZE"
RETENTION_VACUUM_ENV_VAR = "PREDICTCEL_RETENTION_VACUUM"


class LiveInputFetchThresholdError(RuntimeError):
    """Raised when live wallet-trade fetch failures make input quality unreliable."""


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


def _close_quietly(resource: Any) -> None:
    close = getattr(resource, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            logger.debug("Failed to close resource", exc_info=True)


def _env_enabled(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return int(raw)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return float(raw)


def _live_wallet_fetch_failure_ratio_threshold() -> float:
    value = _env_float(
        LIVE_WALLET_FETCH_FAILURE_RATIO_ENV_VAR,
        DEFAULT_LIVE_WALLET_FETCH_FAILURE_RATIO_THRESHOLD,
    )
    if 0.0 <= value <= 1.0:
        return value
    return DEFAULT_LIVE_WALLET_FETCH_FAILURE_RATIO_THRESHOLD


def _live_data_fail_closed_enabled() -> bool:
    return _env_enabled(FAIL_CLOSED_LIVE_DATA_ENV_VAR, default=False)


def _live_input_data_degraded(diagnostics: dict[str, Any]) -> bool:
    return bool(diagnostics.get("degraded_mode"))


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
    parser.add_argument(
        "--output-dir", default="data", help="Directory for discovery JSON reports"
    )
    parser.add_argument(
        "--db",
        default=None,
        help="Optional SQLite database path for persisting wallet registry discovery inputs",
    )
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
    use_live_data = bool(
        args.live_data or (config.live_data and config.live_data.enabled)
    )

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
    cycle_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ")
    alert_manager = get_alert_manager()
    try:
        started = time.perf_counter()
        live_input_diagnostics: dict[str, Any] = {}
        if use_live_data:
            try:
                trades, markets, live_input_diagnostics = _load_live_inputs(config, store)
            except LiveInputFetchThresholdError:
                if alert_manager.is_enabled:
                    alert_manager.alert_cycle_failure(
                        cycle_id,
                        "live-inputs",
                        "Wallet fetch failures exceeded safe threshold",
                        metadata={"mode": "live"},
                    )
                raise
            except Exception as exc:
                fail_closed = bool(args.live_trading) or _live_data_fail_closed_enabled()
                if alert_manager.is_enabled:
                    alert_manager.alert_warning(
                        (
                            "PredictCel Live Data Abort"
                            if fail_closed
                            else "PredictCel Live Data Fallback"
                        ),
                        (
                            "Live data fetch failed; aborting because fresh live inputs are required."
                            if fail_closed
                            else "Live data fetch failed and the cycle is falling back to file inputs."
                        ),
                        cycle_id=cycle_id,
                        metadata={
                            "error": str(exc),
                            "mode": "live",
                            "fail_closed": fail_closed,
                        },
                    )
                if fail_closed:
                    raise
                logger.warning(
                    "Live data fetch failed, falling back to file data",
                    extra={"error": str(exc)},
                )
                trades = load_wallet_trades(config.wallet_trades_path)
                markets = load_market_snapshots(config.market_snapshots_path)
                live_input_diagnostics = {
                    "fallback_reason": str(exc),
                    "degraded_mode": True,
                    "input_source": "file_fallback",
                    "transport_metrics": get_polymarket_metrics(reset=True),
                }
        else:
            trades = load_wallet_trades(config.wallet_trades_path)
            markets = load_market_snapshots(config.market_snapshots_path)
        if args.live_trading and _live_input_data_degraded(live_input_diagnostics):
            raise RuntimeError(
                "--live-trading requires fresh live inputs; file fallback is not allowed."
            )
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
        transport_metrics = live_input_diagnostics.get("transport_metrics", {})
        if transport_metrics:
            metrics.set("api_requests_total", transport_metrics.get("requests", 0))
            metrics.set("api_errors_total", transport_metrics.get("errors", 0))

        output = run_loaded_cycle(
            config=config,
            store=store,
            trades=trades,
            markets=markets,
            live_input_diagnostics=live_input_diagnostics,
            live_trading_requested=bool(args.live_trading),
            use_live_data=use_live_data,
            db_path=args.db,
            retention_max_rows=_env_int(RETENTION_MAX_ROWS_ENV_VAR, 0),
            retention_analyze=_env_enabled(RETENTION_ANALYZE_ENV_VAR, default=False),
            retention_vacuum=_env_enabled(RETENTION_VACUUM_ENV_VAR, default=False),
            cycle_started=cycle_started,
            timings=timings,
            metrics=metrics,
            hooks=CycleRuntimeHooks(
                auto_feed_wallet_registry_from_discovery=_auto_feed_wallet_registry_from_discovery,
                build_wallet_registry_summary=_build_wallet_registry_summary,
                analysis_trades=_analysis_trades,
                filter_duplicate_candidates=_filter_duplicate_candidates,
                creates_or_updates_paper_position=_creates_or_updates_paper_position,
                mark_execution_intents_seen=_mark_execution_intents_seen,
                persist_execution_side_effects=_persist_execution_side_effects,
                decorate_positions_with_titles=_decorate_positions_with_titles,
                portfolio_summary=_portfolio_summary,
                open_position_pnl=_open_position_pnl,
            ),
        )
        summary = output["summary"]
        _log_event(
            "predictcel_cycle_latency",
            {
                "mode": output["mode"],
                "db": output["db"],
                "latency_ms": timings,
                "summary": summary,
                "live_input_diagnostics": _compact_live_input_diagnostics(
                    live_input_diagnostics
                ),
                "scoring_diagnostics": _compact_scoring_diagnostics(
                    output["scoring_diagnostics"]
                ),
                "copy_engine_diagnostics": output["copy_engine_diagnostics"],
                "wallet_registry": output["wallet_registry"],
                "execution": output["execution"],
            },
        )
        logger.info(
            "Cycle complete",
            extra={
                "summary": summary,
                "timings": timings,
                "metrics": metrics.get_metrics(),
                "db": output["db"],
                "wallet_registry": output["wallet_registry"],
            },
        )
        if alert_manager.is_enabled:
            if (
                summary["copy_candidates"] == 0
                and summary["execution_intents"] == 0
                and summary["close_intents"] == 0
            ):
                alert_manager.alert_no_signals(
                    cycle_id,
                    reason="No copy or execution signals were produced",
                )
            else:
                alert_manager.alert_cycle_success(
                    cycle_id,
                    summary["copy_candidates"],
                    metadata={
                        "mode": output["mode"],
                        "execution_results": summary["execution_results"],
                    },
                )
        print(
            json.dumps(
                _compact_cycle_output(
                    output,
                    include_sensitive=_should_log_sensitive_cycle_fields(),
                ),
                sort_keys=True,
                default=str,
            ),
            flush=True,
        )
    except Exception as exc:
        if alert_manager.is_enabled:
            alert_manager.alert_cycle_failure(
                cycle_id,
                "main",
                str(exc),
                metadata={"mode": "live" if use_live_data else "file"},
            )
        raise
    finally:
        _close_quietly(store)


def _run_wallet_discovery(argv: list[str]) -> None:
    started = time.perf_counter()
    args = build_discovery_parser().parse_args(argv)
    config = load_config(args.config)
    pipeline = None
    store = None
    try:
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
            "mode": getattr(
                getattr(config, "wallet_discovery", None), "mode", "auto_update"
            ),
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
                "new_registry_entries": len(
                    after_registry_wallets - before_registry_wallets
                ),
                "new_explorer_memberships": len(
                    after_explorer_memberships - before_explorer_memberships
                ),
                "manager_actions_applied": int(action_diagnostics["actions_applied"]),
                "manager_action_counts": action_diagnostics["action_counts"],
                "skipped_existing_wallets": len(accepted_wallets & before_registry_wallets),
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
    finally:
        _close_quietly(pipeline)
        _close_quietly(store)


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
                "new_registry_entries": len(
                    after_registry_wallets - before_registry_wallets
                ),
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
    finally:
        _close_quietly(locals().get("pipeline"))
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
        registry_status_by_wallet = {}
        if hasattr(store, "load_wallet_registry_entries"):
            registry_status_by_wallet = {
                entry.wallet: str(entry.status).strip().lower() or "active"
                for entry in store.load_wallet_registry_entries()
            }
        raw_topic_by_wallet: dict[str, list[str]] = {}
        saw_registry_membership = False
        for membership in memberships:
            if not getattr(membership, "active", False):
                continue
            wallet = str(membership.wallet).strip()
            if not _is_evm_address(wallet):
                continue
            saw_registry_membership = True
            registry_status = registry_status_by_wallet.get(wallet, "active")
            if registry_status in {"suspended", "retired"}:
                continue
            if registry_status == "stale" and str(
                getattr(membership, "tier", "")
            ).strip().lower() in {"core", "rotating"}:
                continue
            topics = raw_topic_by_wallet.setdefault(wallet, [])
            if membership.topic not in topics:
                topics.append(membership.topic)
        if raw_topic_by_wallet or saw_registry_membership:
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
    persist_rebalance: bool = False,
) -> dict[str, Any]:
    return _build_wallet_registry_summary_impl(
        config,
        store,
        trades,
        persist_rebalance=persist_rebalance,
    )


def _ensure_static_registry_bootstrap(
    config: Any,
    store: SignalStore,
    existing_entries: list[Any],
    *,
    captured_at: datetime,
):
    return _ensure_static_registry_bootstrap_impl(
        config,
        store,
        existing_entries,
        captured_at=captured_at,
    )


def _ensure_static_membership_bootstrap(
    config: Any,
    store: SignalStore,
    existing_memberships: list[Any],
    *,
    captured_at: datetime,
):
    return _ensure_static_membership_bootstrap_impl(
        config,
        store,
        existing_memberships,
        captured_at=captured_at,
    )


def _membership_counts_by_topic(memberships: list[Any]) -> dict[str, dict[str, int]]:
    return _membership_counts_by_topic_impl(memberships)


def _basket_health_as_dict(health: BasketHealth) -> dict[str, Any]:
    return _basket_health_as_dict_impl(health)


def _basket_promotion_as_dict(recommendation: Any) -> dict[str, Any]:
    return _basket_promotion_as_dict_impl(recommendation)


def _promotion_watch_by_topic(
    memberships: list[Any],
    registry_entries: list[Any],
    live_roster_by_topic: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    return _promotion_watch_by_topic_impl(
        memberships,
        registry_entries,
        live_roster_by_topic,
    )


def _mark_execution_intents_seen(store: SignalStore, execution_intents: list) -> None:
    _mark_execution_intents_seen_impl(store, execution_intents)


def _persist_execution_side_effects(
    store: SignalStore,
    config: Any,
    execution_results: list,
) -> None:
    _persist_execution_side_effects_impl(
        store,
        config,
        execution_results,
        trusted_statuses=TRUSTED_POSITION_STATUSES,
    )


def _portfolio_summary(store: SignalStore, config: Any) -> dict:
    return _portfolio_summary_impl(store, config)


def _trade_market_id_source_breakdown(
    wallet_payloads: dict[str, list[dict[str, Any]]],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for items in wallet_payloads.values():
        for item in items:
            source = _trade_market_id_source(item)
            if not source:
                continue
            counts[source] = counts.get(source, 0) + 1
    return dict(sorted(counts.items()))


def _load_live_inputs(config, store: SignalStore | None = None):
    return _load_live_inputs_impl(
        config,
        store=store,
        hooks=LiveInputHooks(
            client_cls=PolymarketPublicClient,
            close_quietly=_close_quietly,
            wallet_topics_for_live_inputs=_wallet_topics_for_live_inputs,
            is_evm_address=_is_evm_address,
            fetch_wallet_payloads=_fetch_wallet_payloads,
            build_wallet_trades=build_wallet_trades,
            extract_trade_market_ids=extract_trade_market_ids,
            extract_trade_market_slugs=extract_trade_market_slugs,
            trade_market_id_source_breakdown=_trade_market_id_source_breakdown,
            build_market_snapshots=build_market_snapshots,
            index_market_row_token_aliases=_index_market_row_token_aliases,
            looks_like_unresolved_token_id=_looks_like_unresolved_token_id,
            recover_unresolved_token_market_rows=_recover_unresolved_token_market_rows,
            classify_unmatched_token_ids=_classify_unmatched_token_ids,
            enrich_market_snapshots_with_orderbooks=enrich_market_snapshots_with_orderbooks,
            propagate_canonical_market_updates=_propagate_canonical_market_updates,
            build_orderbook_probe_samples=_build_orderbook_probe_samples,
            get_transport_metrics=lambda: get_polymarket_metrics(reset=True),
            logger=logger,
            failure_threshold=_live_wallet_fetch_failure_ratio_threshold(),
            failure_error_cls=LiveInputFetchThresholdError,
        ),
    )


def _propagate_canonical_market_updates(
    markets: dict[str, Any], updated_snapshots: Any
) -> None:
    canonical_updates = {
        snapshot.market_id: snapshot
        for snapshot in updated_snapshots
        if getattr(snapshot, "market_id", "")
    }
    if not canonical_updates:
        return
    replace_canonical = getattr(markets, "replace_canonical", None)
    if callable(replace_canonical):
        for snapshot in canonical_updates.values():
            replace_canonical(snapshot)
        return
    for market_key, snapshot in list(markets.items()):
        canonical_snapshot = canonical_updates.get(getattr(snapshot, "market_id", ""))
        if canonical_snapshot is not None:
            markets[market_key] = canonical_snapshot


def _recover_unresolved_token_market_rows(
    client: PolymarketPublicClient,
    token_ids: list[str],
    max_workers: int = 16,
) -> tuple[list[dict[str, Any]], dict[str, int], list[str]]:
    unique_token_ids = sorted(
        {str(token_id).strip() for token_id in token_ids if str(token_id).strip()}
    )
    if not unique_token_ids:
        return [], {"requested": 0, "matched": 0, "unmatched": 0}, []

    rows: list[dict[str, Any]] = []
    recovered_token_ids: set[str] = set()
    unresolved_samples: list[str] = []
    workers = max(1, min(max_workers, len(unique_token_ids)))

    def fetch_one(token_id: str) -> list[dict[str, Any]]:
        probe_client = _make_probe_client(client)
        return probe_client.fetch_markets_by_clob_token_ids([token_id], chunk_size=1)

    executor = shared_io_executor()
    futures = {
        executor.submit(fetch_one, token_id): token_id
        for token_id in unique_token_ids[:workers]
    }
    pending_token_ids = iter(unique_token_ids[workers:])
    while futures:
        for future in as_completed(tuple(futures)):
            token_id = futures.pop(future)
            try:
                token_rows = future.result()
            except Exception:
                token_rows = []
            if token_rows:
                rows.extend(token_rows)
                recovered_token_ids.add(token_id)
            elif len(unresolved_samples) < 10:
                unresolved_samples.append(token_id)

            next_token_id = next(pending_token_ids, None)
            if next_token_id is not None:
                futures[executor.submit(fetch_one, next_token_id)] = next_token_id

    return (
        rows,
        {
            "requested": len(unique_token_ids),
            "matched": len(recovered_token_ids),
            "unmatched": max(0, len(unique_token_ids) - len(recovered_token_ids)),
        },
        unresolved_samples,
    )


def _analysis_trades(
    trades: list[Any],
    max_trade_age_seconds: int,
) -> list[Any]:
    return _analysis_trades_impl(trades, max_trade_age_seconds)


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
    return client


def _probe_token_orderbook(
    client: PolymarketPublicClient, token_id: str
) -> dict[str, Any]:
    token_id = str(token_id).strip()
    if not token_id:
        return {"missing_token_id": True}
    try:
        probe_client = _make_probe_client(client)
        return _summarize_order_book_payload(probe_client.fetch_order_book(token_id))
    except Exception as exc:
        return {"token_id": token_id, "error": f"{type(exc).__name__}: {exc}"}


def _probe_token_lookup(
    client: PolymarketPublicClient, token_id: str
) -> dict[str, Any]:
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


def _summarize_market_lookup_rows(
    rows: list[dict[str, Any]], token_id: str
) -> dict[str, Any]:
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


def _looks_like_unresolved_token_id(value: Any) -> bool:
    token_id = str(value).strip().lower()
    return bool(token_id) and (token_id.startswith("0x") or "token" in token_id)


def _market_row_lookup_keys(row: dict[str, Any]) -> list[str]:
    keys: list[str] = []
    for field in (
        "conditionId",
        "condition_id",
        "conditionID",
        "condition",
        "id",
        "market_id",
        "marketId",
        "slug",
        "marketSlug",
    ):
        value = row.get(field)
        if value is None or isinstance(value, dict):
            continue
        normalized = str(value).strip()
        if normalized:
            keys.append(normalized)
    return list(dict.fromkeys(keys))


def _append_market_row_token_candidate(
    token_ids: list[str],
    raw_value: Any,
) -> None:
    if raw_value is None:
        return
    if isinstance(raw_value, str):
        value = raw_value.strip()
        if not value:
            return
        if value.startswith("[") or value.startswith("{"):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                parsed = None
            if parsed is not None:
                _append_market_row_token_candidate(token_ids, parsed)
                return
        token_ids.append(value)
        return
    if isinstance(raw_value, list):
        for item in raw_value:
            _append_market_row_token_candidate(token_ids, item)
        return
    if isinstance(raw_value, dict):
        for field in (
            "token_id",
            "tokenId",
            "tokenID",
            "clobTokenId",
            "clob_token_id",
            "asset",
            "asset_id",
            "assetId",
            "id",
        ):
            if field in raw_value:
                _append_market_row_token_candidate(token_ids, raw_value.get(field))


def _market_row_token_ids(row: dict[str, Any]) -> list[str]:
    token_ids: list[str] = []
    for field in (
        "clobTokenIds",
        "tokenIds",
        "token_ids",
        "clobTokenId",
        "clob_token_id",
        "tokenId",
        "tokenID",
        "token_id",
        "tokens",
        "outcomes",
        "outcomeTokens",
    ):
        if field in row:
            _append_market_row_token_candidate(token_ids, row.get(field))
    return [token_id for token_id in dict.fromkeys(token_ids) if token_id]


def _index_market_row_token_aliases(
    markets: dict[str, Any],
    rows: list[dict[str, Any]],
) -> int:
    added = 0
    add_alias = getattr(markets, "add_alias", None)
    for row in rows:
        snapshot = None
        for key in _market_row_lookup_keys(row):
            snapshot = markets.get(key)
            if snapshot is not None:
                break
        if snapshot is None:
            continue
        for token_id in _market_row_token_ids(row):
            if token_id in markets:
                continue
            if callable(add_alias):
                if add_alias(token_id, snapshot):
                    added += 1
                continue
            markets[token_id] = snapshot
            added += 1
    return added


def _classify_unmatched_token_ids(
    token_ids: list[str],
    loaded_market_rows: list[dict[str, Any]],
    markets: dict[str, Any],
    token_probe_rows: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    unique_token_ids = sorted(
        {str(token_id).strip() for token_id in token_ids if str(token_id).strip()}
    )
    if not unique_token_ids:
        return {"breakdown": {}, "samples": {}}

    loaded_market_ids = {
        str(getattr(snapshot, "market_id", "")).strip()
        for snapshot in markets.values()
        if str(getattr(snapshot, "market_id", "")).strip()
    }
    loaded_row_market_by_token: dict[str, str] = {}
    for row in loaded_market_rows:
        market_keys = _market_row_lookup_keys(row)
        market_id = market_keys[0] if market_keys else ""
        if not market_id:
            continue
        for token_id in _market_row_token_ids(row):
            loaded_row_market_by_token.setdefault(token_id, market_id)

    probed_market_by_token: dict[str, str] = {}
    for row in token_probe_rows:
        market_keys = _market_row_lookup_keys(row)
        market_id = market_keys[0] if market_keys else ""
        if not market_id:
            continue
        for token_id in _market_row_token_ids(row):
            probed_market_by_token.setdefault(token_id, market_id)

    breakdown: dict[str, int] = {}
    samples: dict[str, list[str]] = {}
    for token_id in unique_token_ids:
        if token_id in loaded_row_market_by_token and token_id not in markets:
            category = "present_on_loaded_row_missing_crossref"
        elif token_id in probed_market_by_token:
            if probed_market_by_token[token_id] in loaded_market_ids:
                category = "present_on_loaded_row_missing_crossref"
            else:
                category = "market_outside_loaded_universe"
        else:
            category = "absent_from_loaded_market_rows"
        breakdown[category] = breakdown.get(category, 0) + 1
        bucket = samples.setdefault(category, [])
        if len(bucket) < 5:
            bucket.append(token_id)

    return {
        "breakdown": dict(sorted(breakdown.items())),
        "samples": dict(sorted(samples.items())),
    }


def _fetch_wallet_payloads(
    client: PolymarketPublicClient,
    wallets: list[str],
    limit: int,
) -> tuple[dict[str, list[dict[str, Any]]], list[str]]:
    if not wallets:
        return {}, []
    payloads: dict[str, list[dict[str, Any]]] = {}
    failed_wallets: list[str] = []
    workers = max(1, min(32, len(wallets)))
    executor = shared_io_executor()
    futures = {
        executor.submit(client.fetch_wallet_trades, wallet, limit): wallet
        for wallet in wallets[:workers]
    }
    pending_wallets = iter(wallets[workers:])
    while futures:
        for future in as_completed(tuple(futures)):
            wallet = futures.pop(future)
            try:
                payloads[wallet] = future.result()
            except Exception as exc:
                logger.warning(
                    "Failed to fetch wallet trades",
                    extra={"wallet": wallet, "error": f"{type(exc).__name__}: {exc}"},
                )
                payloads[wallet] = []
                failed_wallets.append(wallet)

            next_wallet = next(pending_wallets, None)
            if next_wallet is not None:
                futures[executor.submit(client.fetch_wallet_trades, next_wallet, limit)] = (
                    next_wallet
                )
    return payloads, failed_wallets


def _filter_duplicate_candidates(
    store: SignalStore, candidates: list
) -> tuple[list, int]:
    return _filter_duplicate_candidates_impl(store, candidates)


def _is_trusted_execution_result(result) -> bool:
    return _is_trusted_execution_result_impl(result, TRUSTED_POSITION_STATUSES)


def _creates_or_updates_paper_position(result) -> bool:
    return _creates_or_updates_paper_position_impl(result, TRUSTED_POSITION_STATUSES)


def _checksummed_evm_address(value: str) -> str | None:
    if keccak is None:
        return None
    normalized = value[2:].lower()
    digest = keccak.new(digest_bits=256)
    digest.update(normalized.encode("ascii"))
    address_hash = digest.hexdigest()
    checksummed = "".join(
        char.upper() if char.isalpha() and int(address_hash[index], 16) >= 8 else char
        for index, char in enumerate(normalized)
    )
    return f"0x{checksummed}"


def _is_evm_address(value: str) -> bool:
    value = str(value).strip()
    if not (
        len(value) == 42
        and value.startswith("0x")
        and all(char in HEX_CHARS for char in value[2:])
    ):
        return False
    body = value[2:]
    if body == body.lower() or body == body.upper():
        return True
    checksummed = _checksummed_evm_address(value)
    return checksummed is not None and value == checksummed


def _decorate_positions_with_titles(
    positions: list[Position],
    markets: dict[str, Any],
) -> list[Position]:
    return _decorate_positions_with_titles_impl(positions, markets)


def _should_log_sensitive_cycle_fields() -> bool:
    value = os.getenv(SENSITIVE_CYCLE_OUTPUT_ENV_VAR, "")
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _compact_cycle_output(
    output: dict[str, Any],
    *,
    include_sensitive: bool = False,
) -> dict[str, Any]:
    compact = {
        "mode": output["mode"],
        "summary": output["summary"],
        "latency_ms": output["latency_ms"],
        "db": output["db"],
        "portfolio_summary": output["portfolio_summary"],
    }
    if output.get("metrics"):
        compact["metrics"] = output["metrics"]
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
    if include_sensitive:
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
        "trade_market_id_source_breakdown": diagnostics.get(
            "trade_market_id_source_breakdown",
            {},
        ),
        "supplemental_market_ids_requested": diagnostics.get(
            "supplemental_market_ids_requested",
            0,
        ),
        "supplemental_market_rows_loaded": diagnostics.get(
            "supplemental_market_rows_loaded",
            0,
        ),
        "unresolved_market_ids_after_supplemental": diagnostics.get(
            "unresolved_market_ids_after_supplemental",
            0,
        ),
        "unresolved_token_ids_after_supplemental": diagnostics.get(
            "unresolved_token_ids_after_supplemental",
            0,
        ),
        "token_probe_requested": diagnostics.get("token_probe_requested", 0),
        "token_probe_rows_loaded": diagnostics.get("token_probe_rows_loaded", 0),
        "token_probe_tokens_matched": diagnostics.get(
            "token_probe_tokens_matched",
            0,
        ),
        "token_probe_tokens_unmatched": diagnostics.get(
            "token_probe_tokens_unmatched",
            0,
        ),
        "token_aliases_added_from_rows": diagnostics.get(
            "token_aliases_added_from_rows",
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
    if diagnostics.get("degraded_mode"):
        compact["degraded_mode"] = True
        compact["input_source"] = diagnostics.get("input_source", "file_fallback")
        compact["fallback_reason"] = diagnostics.get("fallback_reason", "")
    if diagnostics.get("orderbook_probe_samples"):
        compact["orderbook_probe_samples"] = diagnostics["orderbook_probe_samples"][:2]
    skipped_invalid_wallets = diagnostics.get("skipped_invalid_wallets", 0)
    if skipped_invalid_wallets:
        compact["skipped_invalid_wallets"] = skipped_invalid_wallets
        compact["sample_skipped_invalid_wallets"] = diagnostics.get(
            "sample_skipped_invalid_wallets",
            [],
        )[:3]
    unresolved_token_samples = diagnostics.get("sample_unresolved_token_ids", [])
    if unresolved_token_samples:
        compact["sample_unresolved_token_ids"] = unresolved_token_samples[:5]
    unmatched_token_breakdown = diagnostics.get("unmatched_token_breakdown", {})
    if unmatched_token_breakdown:
        compact["unmatched_token_breakdown"] = unmatched_token_breakdown
    sample_unmatched_tokens = diagnostics.get("sample_unmatched_tokens_by_class", {})
    if sample_unmatched_tokens:
        compact["sample_unmatched_tokens_by_class"] = {
            key: values[:3] for key, values in sample_unmatched_tokens.items()
        }
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
    wallet_attrition = diagnostics.get("wallet_attrition", {})
    if wallet_attrition:
        compact["wallet_attrition"] = wallet_attrition
    if "analysis_trade_count" in diagnostics:
        compact["analysis_trade_count"] = diagnostics.get("analysis_trade_count", 0)
    if "pre_scoring_too_old_filtered" in diagnostics:
        compact["pre_scoring_too_old_filtered"] = diagnostics.get(
            "pre_scoring_too_old_filtered",
            0,
        )
    missing_market_breakdown = diagnostics.get("missing_market_breakdown", {})
    if missing_market_breakdown:
        compact["missing_market_breakdown"] = missing_market_breakdown
    missing_market_samples = diagnostics.get("missing_market_samples", [])
    if missing_market_samples:
        compact["missing_market_samples"] = missing_market_samples
    missing_market_by_wallet = diagnostics.get("missing_market_by_wallet", {})
    if missing_market_by_wallet:
        top_missing_wallets = sorted(
            missing_market_by_wallet.items(),
            key=lambda item: item[1],
            reverse=True,
        )[:3]
        compact["missing_market_by_wallet"] = dict(top_missing_wallets)
    missing_market_samples_by_wallet = diagnostics.get(
        "missing_market_samples_by_wallet",
        {},
    )
    if missing_market_samples_by_wallet and missing_market_by_wallet:
        compact["missing_market_samples_by_wallet"] = {
            wallet: missing_market_samples_by_wallet.get(wallet, [])[:3]
            for wallet in compact.get("missing_market_by_wallet", {})
            if missing_market_samples_by_wallet.get(wallet)
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
    return _open_position_pnl_impl(positions)


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

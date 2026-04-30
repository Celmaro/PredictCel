from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, Callable

from .runtime import shared_io_executor


@dataclass(frozen=True)
class LiveInputHooks:
    client_cls: Any
    close_quietly: Callable[[Any], None]
    wallet_topics_for_live_inputs: Callable[[Any, Any], tuple[dict[str, list[str]], str]]
    is_evm_address: Callable[[str], bool]
    fetch_wallet_payloads: Callable[[Any, list[str], int], tuple[dict[str, list[dict[str, Any]]], list[str]]]
    build_wallet_trades: Callable[[dict[str, list[dict[str, Any]]], dict[str, list[str]]], list[Any]]
    extract_trade_market_ids: Callable[[dict[str, list[dict[str, Any]]]], list[str]]
    extract_trade_market_slugs: Callable[[dict[str, list[dict[str, Any]]]], list[str]]
    trade_market_id_source_breakdown: Callable[[dict[str, list[dict[str, Any]]]], dict[str, int]]
    build_market_snapshots: Callable[[list[dict[str, Any]]], Any]
    index_market_row_token_aliases: Callable[[Any, list[dict[str, Any]]], int]
    looks_like_unresolved_token_id: Callable[[str], bool]
    recover_unresolved_token_market_rows: Callable[[Any, list[str]], tuple[list[dict[str, Any]], dict[str, int], list[str]]]
    classify_unmatched_token_ids: Callable[[list[str], list[dict[str, Any]], Any, list[dict[str, Any]]], dict[str, dict[str, Any]]]
    enrich_market_snapshots_with_orderbooks: Callable[[Any, Any], Any]
    propagate_canonical_market_updates: Callable[[Any, Any], None]
    build_orderbook_probe_samples: Callable[[Any, list[Any]], list[dict[str, Any]]]
    get_transport_metrics: Callable[[], dict[str, int]]
    logger: Any
    failure_threshold: float
    failure_error_cls: type[Exception]


def load_live_inputs(
    config: Any,
    *,
    store: Any = None,
    hooks: LiveInputHooks,
) -> tuple[list[Any], Any, dict[str, Any]]:
    if config.live_data is None:
        raise ValueError("--live-data was requested but live_data is not configured.")
    client = hooks.client_cls(
        config.live_data.gamma_base_url,
        config.live_data.data_base_url,
        config.live_data.clob_base_url,
        config.live_data.request_timeout_seconds,
    )
    try:
        invalid_wallets: list[str] = []
        raw_topic_by_wallet, wallet_source = hooks.wallet_topics_for_live_inputs(
            config,
            store,
        )
        if wallet_source == "config_baskets":
            for basket in config.baskets:
                for wallet in basket.wallets:
                    if not hooks.is_evm_address(wallet):
                        invalid_wallets.append(wallet)
        elif store is not None and hasattr(store, "load_basket_memberships"):
            for membership in store.load_basket_memberships():
                wallet = str(membership.wallet).strip()
                if wallet and not hooks.is_evm_address(wallet):
                    invalid_wallets.append(wallet)

        executor = shared_io_executor()
        wallet_payloads_future: Future = executor.submit(
            hooks.fetch_wallet_payloads,
            client,
            list(raw_topic_by_wallet),
            config.live_data.trade_limit,
        )
        active_market_rows_future: Future = executor.submit(
            client.fetch_active_markets,
            config.live_data.market_limit,
        )

        wallet_payloads, failed_wallets = wallet_payloads_future.result()
        wallet_fetch_failure_count = len(failed_wallets)
        wallet_fetch_failure_rate = (
            wallet_fetch_failure_count / len(raw_topic_by_wallet)
            if raw_topic_by_wallet
            else 0.0
        )
        if wallet_fetch_failure_count:
            hooks.logger.warning(
                "Wallet trade fetch failures detected during live input load",
                extra={
                    "failed_wallets": wallet_fetch_failure_count,
                    "requested_wallets": len(raw_topic_by_wallet),
                },
            )
            if wallet_fetch_failure_rate >= hooks.failure_threshold:
                raise hooks.failure_error_cls(
                    "wallet fetch failures exceeded threshold "
                    f"({wallet_fetch_failure_count}/{len(raw_topic_by_wallet)})"
                )
        trades = hooks.build_wallet_trades(wallet_payloads, raw_topic_by_wallet)
        trade_market_ids = hooks.extract_trade_market_ids(wallet_payloads)
        trade_market_slugs = hooks.extract_trade_market_slugs(wallet_payloads)
        trade_market_id_source_breakdown = hooks.trade_market_id_source_breakdown(
            wallet_payloads
        )

        active_market_rows = active_market_rows_future.result()
        markets = hooks.build_market_snapshots(active_market_rows)
        token_aliases_added_from_rows = hooks.index_market_row_token_aliases(
            markets,
            active_market_rows,
        )

        missing_trade_market_ids = [
            market_id for market_id in trade_market_ids if market_id not in markets
        ]
        supplemental_rows = client.fetch_markets_by_identifiers(missing_trade_market_ids)
        if supplemental_rows:
            markets.update(hooks.build_market_snapshots(supplemental_rows))
            token_aliases_added_from_rows += hooks.index_market_row_token_aliases(
                markets,
                supplemental_rows,
            )

        missing_trade_market_slugs = [
            market_slug
            for market_slug in trade_market_slugs
            if market_slug not in markets
        ]
        supplemental_slug_rows = client.fetch_markets_by_slugs(missing_trade_market_slugs)
        if supplemental_slug_rows:
            markets.update(hooks.build_market_snapshots(supplemental_slug_rows))
            token_aliases_added_from_rows += hooks.index_market_row_token_aliases(
                markets,
                supplemental_slug_rows,
            )

        unresolved_trade_market_ids = [
            market_id for market_id in missing_trade_market_ids if market_id not in markets
        ]
        unresolved_token_trade_market_ids = [
            market_id
            for market_id in unresolved_trade_market_ids
            if hooks.looks_like_unresolved_token_id(market_id)
        ]
        token_probe_rows, token_probe_stats, unresolved_token_samples = (
            hooks.recover_unresolved_token_market_rows(
                client,
                unresolved_token_trade_market_ids,
            )
        )
        unmatched_token_diagnostics = hooks.classify_unmatched_token_ids(
            unresolved_token_trade_market_ids,
            [*active_market_rows, *supplemental_rows, *supplemental_slug_rows],
            markets,
            token_probe_rows,
        )
        if token_probe_rows:
            markets.update(hooks.build_market_snapshots(token_probe_rows))
            token_aliases_added_from_rows += hooks.index_market_row_token_aliases(
                markets,
                token_probe_rows,
            )

        relevant_market_keys = {
            market_key
            for market_key in (*trade_market_ids, *trade_market_slugs)
            if market_key in markets
        }
        relevant_snapshots_for_enrichment: dict[str, Any] = {}
        if relevant_market_keys:
            relevant_snapshots = {
                market_id: markets[market_id] for market_id in relevant_market_keys
            }
            relevant_snapshots_for_enrichment = {
                market_id: snapshot
                for market_id, snapshot in relevant_snapshots.items()
                if snapshot.liquidity_usd >= config.filters.min_liquidity_usd
            }
            enriched_relevant = hooks.enrich_market_snapshots_with_orderbooks(
                relevant_snapshots_for_enrichment,
                client,
            )
            markets.update(enriched_relevant)
            hooks.propagate_canonical_market_updates(markets, enriched_relevant.values())

        trade_market_keys = sorted(set(trade_market_ids + trade_market_slugs))
        matched_trade_market_keys = [
            market_key for market_key in trade_market_keys if market_key in markets
        ]
        wallets_with_payloads = sum(1 for items in wallet_payloads.values() if items)
        wallets_with_parsed_trades = len({trade.wallet for trade in trades})
        unique_market_snapshots = {
            snapshot.market_id: snapshot for snapshot in markets.values()
        }
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
            orderbook_probe_samples = hooks.build_orderbook_probe_samples(
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
            "wallet_fetch_failures": wallet_fetch_failure_count,
            "wallet_fetch_failure_rate_pct": round(wallet_fetch_failure_rate * 100, 1),
            "sample_wallet_fetch_failures": failed_wallets[:5],
            "parsed_trade_count": len(trades),
            "active_market_rows_loaded": len(active_market_rows),
            "trade_market_ids_seen": len(trade_market_ids),
            "trade_market_slugs_seen": len(trade_market_slugs),
            "trade_market_id_source_breakdown": trade_market_id_source_breakdown,
            "supplemental_market_ids_requested": len(missing_trade_market_ids),
            "supplemental_market_rows_loaded": len(supplemental_rows),
            "unresolved_market_ids_after_supplemental": len(unresolved_trade_market_ids),
            "unresolved_token_ids_after_supplemental": len(
                unresolved_token_trade_market_ids
            ),
            "token_probe_requested": token_probe_stats["requested"],
            "token_probe_rows_loaded": len(token_probe_rows),
            "token_probe_tokens_matched": token_probe_stats["matched"],
            "token_probe_tokens_unmatched": token_probe_stats["unmatched"],
            "token_aliases_added_from_rows": token_aliases_added_from_rows,
            "unmatched_token_breakdown": unmatched_token_diagnostics["breakdown"],
            "sample_unmatched_tokens_by_class": unmatched_token_diagnostics["samples"],
            "sample_unresolved_token_ids": unresolved_token_samples,
            "supplemental_market_slugs_requested": len(missing_trade_market_slugs),
            "supplemental_slug_rows_loaded": len(supplemental_slug_rows),
            "market_cache_entries": len(markets),
            "multi_topic_wallets": sum(
                1 for topics in raw_topic_by_wallet.values() if len(topics) > 1
            ),
            "relevant_markets_enriched": len(relevant_snapshots_for_enrichment),
            "relevant_markets_skipped_low_liquidity": max(
                0, len(relevant_market_keys) - len(relevant_snapshots_for_enrichment)
            ),
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
            "transport_metrics": hooks.get_transport_metrics(),
        }
        return trades, markets, diagnostics
    finally:
        hooks.close_quietly(client)

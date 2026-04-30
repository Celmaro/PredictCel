from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from .models import Position
from .storage import SignalStore


def mark_execution_intents_seen(store: SignalStore, execution_intents: list) -> None:
    if execution_intents:
        store.mark_signals_seen(
            (intent.market_id, intent.topic, intent.side)
            for intent in execution_intents
        )


def is_trusted_execution_result(
    result: Any, trusted_statuses: set[str]
) -> bool:
    return str(result.status).strip().lower() in trusted_statuses and bool(
        str(result.order_id).strip()
    )


def creates_or_updates_paper_position(
    result: Any, trusted_statuses: set[str]
) -> bool:
    return str(result.status).strip().lower() == "dry_run" or is_trusted_execution_result(
        result,
        trusted_statuses,
    )


def persist_execution_side_effects(
    store: SignalStore,
    config: Any,
    execution_results: list,
    *,
    trusted_statuses: set[str],
) -> None:
    now = datetime.now(UTC)
    position_config = config.execution.position
    positions: list[Position] = []
    for result in execution_results:
        if creates_or_updates_paper_position(result, trusted_statuses):
            entry_shares = _execution_shares(result)
            positions.append(
                Position(
                    market_id=result.market_id,
                    topic=result.topic,
                    side=result.side,
                    token_id=result.token_id,
                    entry_price=_execution_price(result),
                    entry_amount_usd=result.amount_usd,
                    current_price=_execution_price(result),
                    unrealized_pnl=0.0,
                    opened_at=now,
                    last_updated=now,
                    take_profit_pct=position_config.take_profit_pct,
                    stop_loss_pct=position_config.stop_loss_pct,
                    max_hold_minutes=position_config.max_hold_minutes,
                    status="open",
                    market_title=result.market_title,
                    entry_shares=entry_shares,
                    remaining_shares=entry_shares,
                )
            )
    if positions:
        store.save_positions(positions)


def portfolio_summary(store: SignalStore, config: Any) -> dict:
    if config.execution is None or config.execution.exposure is None:
        return store.get_portfolio_summary(starting_bankroll_usd=0.0)
    return store.get_portfolio_summary(
        starting_bankroll_usd=config.execution.exposure.max_total_exposure_usd
    )


def analysis_trades(
    trades: list[Any],
    max_trade_age_seconds: int,
) -> list[Any]:
    return [
        trade
        for trade in trades
        if getattr(trade, "age_seconds", max_trade_age_seconds + 1)
        <= max_trade_age_seconds
    ]


def filter_duplicate_candidates(
    store: SignalStore, candidates: list
) -> tuple[list, int]:
    atomic_filter = getattr(store, "filter_and_mark_candidates_atomically", None)
    if callable(atomic_filter):
        return atomic_filter(candidates)
    fingerprints = [
        store.make_signal_fingerprint(
            candidate.market_id, candidate.topic, candidate.side
        )
        for candidate in candidates
    ]
    recent_fingerprints = store.has_recent_signals(
        [
            (candidate.market_id, candidate.topic, candidate.side)
            for candidate in candidates
        ]
    )
    fresh = []
    seen_in_batch: set[str] = set()
    for candidate, fingerprint in zip(candidates, fingerprints):
        if fingerprint in recent_fingerprints or fingerprint in seen_in_batch:
            continue
        fresh.append(candidate)
        seen_in_batch.add(fingerprint)
    return fresh, len(candidates) - len(fresh)


def decorate_positions_with_titles(
    positions: list[Position],
    markets: dict[str, Any],
) -> list[Position]:
    decorated: list[Position] = []
    for pos in positions:
        market = markets.get(pos.market_id)
        title = pos.market_title or (market.title if market is not None else "")
        decorated.append(
            Position(
                market_id=pos.market_id,
                topic=pos.topic,
                side=pos.side,
                token_id=pos.token_id,
                entry_price=pos.entry_price,
                entry_amount_usd=pos.entry_amount_usd,
                current_price=pos.current_price,
                unrealized_pnl=pos.unrealized_pnl,
                opened_at=pos.opened_at,
                last_updated=pos.last_updated,
                take_profit_pct=pos.take_profit_pct,
                stop_loss_pct=pos.stop_loss_pct,
                max_hold_minutes=pos.max_hold_minutes,
                status=pos.status,
                market_title=title,
                entry_shares=pos.entry_shares,
                remaining_shares=pos.remaining_shares,
            )
        )
    return decorated


def open_position_pnl(positions: list[Position]) -> list[dict[str, Any]]:
    return [
        {
            "market_id": pos.market_id,
            "market_title": pos.market_title,
            "topic": pos.topic,
            "side": pos.side,
            "entry_amount_usd": pos.entry_amount_usd,
            "entry_shares": pos.entry_shares,
            "remaining_shares": pos.remaining_shares,
            "entry_price": pos.entry_price,
            "current_price": pos.current_price,
            "unrealized_pnl_usd": pos.unrealized_pnl,
            "status": pos.status,
        }
        for pos in positions
    ]


def _execution_price(result: Any) -> float:
    avg_fill_price = float(getattr(result, "avg_fill_price", 0.0) or 0.0)
    if avg_fill_price > 0:
        return avg_fill_price
    return float(getattr(result, "worst_price", 0.0) or 0.0)


def _execution_shares(result: Any) -> float:
    filled_shares = float(getattr(result, "filled_shares", 0.0) or 0.0)
    if filled_shares > 0:
        return filled_shares
    price = _execution_price(result)
    amount_usd = float(getattr(result, "amount_usd", 0.0) or 0.0)
    if price <= 0 or amount_usd <= 0:
        return 0.0
    return round(amount_usd / price, 8)

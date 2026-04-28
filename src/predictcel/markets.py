"""Market data fetching and caching.

Handles fetching market data from Polymarket API with caching,
rate limiting, and error handling.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from .models import MarketSnapshot

__all__ = ["load_market_snapshots"]



def load_market_snapshots(path: str) -> dict[str, MarketSnapshot]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    snapshots = {}
    for item in payload:
        snapshot = MarketSnapshot(
            market_id=item["market_id"],
            topic=item["topic"],
            title=item["title"],
            yes_ask=float(item["yes_ask"]),
            no_ask=float(item["no_ask"]),
            best_bid=float(item["best_bid"]),
            liquidity_usd=float(item["liquidity_usd"]),
            minutes_to_resolution=int(item["minutes_to_resolution"]),
            yes_token_id=str(item.get("yes_token_id", "")),
            no_token_id=str(item.get("no_token_id", "")),
            yes_bid=float(item.get("yes_bid", 0.0)),
            no_bid=float(item.get("no_bid", 0.0)),
            yes_ask_size=float(item.get("yes_ask_size", 0.0)),
            no_ask_size=float(item.get("no_ask_size", 0.0)),
            yes_spread=float(item.get("yes_spread", 0.0)),
            no_spread=float(item.get("no_spread", 0.0)),
            orderbook_ready=bool(item.get("orderbook_ready", False)),
            snapshot_time=_parse_datetime(item.get("snapshot_time") or item.get("timestamp") or item.get("created_at")),
            resolved_outcome=_parse_outcome(item),
            resolution_price=_parse_resolution_price(item),
        )
        snapshots[snapshot.market_id] = snapshot
    return snapshots


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _parse_outcome(item: dict[str, object]) -> str | None:
    value = (
        item.get("resolved_outcome")
        or item.get("outcome")
        or item.get("resolution")
        or item.get("result")
        or item.get("outcomeIndex")
        or item.get("outcome_index")
    )
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip().upper()
        if value in {"YES", "NO"}:
            return value
        if value in {"0", "FALSE", "NO_OUTCOME", "INVALID"}:
            return "NO"
        if value in {"1", "TRUE", "YES_OUTCOME"}:
            return "YES"
    if isinstance(value, (int, float)):
        return "YES" if float(value) >= 1.0 else "NO"
    return None


def _parse_resolution_price(item: dict[str, object]) -> float | None:
    value = item.get("resolution_price")
    if isinstance(value, (int, float, str)):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    outcome = _parse_outcome(item)
    if outcome == "YES":
        return 1.0
    if outcome == "NO":
        return 0.0
    return None

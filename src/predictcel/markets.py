from __future__ import annotations

import json
from pathlib import Path

from .models import MarketSnapshot


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
        )
        snapshots[snapshot.market_id] = snapshot
    return snapshots

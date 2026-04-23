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
        )
        snapshots[snapshot.market_id] = snapshot
    return snapshots

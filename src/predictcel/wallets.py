from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from .models import WalletTrade


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def load_wallet_trades(path: str) -> list[WalletTrade]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return [
        WalletTrade(
            wallet=item["wallet"],
            topic=item["topic"],
            market_id=item["market_id"],
            side=item["side"],
            price=float(item["price"]),
            size_usd=float(item["size_usd"]),
            age_seconds=int(item["age_seconds"]),
            timestamp=_parse_datetime(item.get("timestamp") or item.get("trade_time") or item.get("created_at")),
        )
        for item in payload
    ]


def bucket_trades_by_market(trades: list[WalletTrade]) -> dict[str, list[WalletTrade]]:
    buckets: dict[str, list[WalletTrade]] = defaultdict(list)
    for trade in trades:
        buckets[trade.market_id].append(trade)
    return dict(buckets)

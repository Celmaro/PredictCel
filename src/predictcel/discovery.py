"""Wallet discovery orchestration.

Coordinates wallet discovery from multiple sources and manages
the discovery pipeline.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from typing import Any

__all__ = ["DiscoveryRunner", "discover_wallets"]



@dataclass(frozen=True)
class WalletCandidate:
    wallet: str
    username: str
    dominant_topic: str
    score: float
    pnl: float
    volume: float
    trade_count: int
    unique_markets: int
    focus_ratio: float
    average_trade_size: float
    reason: str


def score_wallet_candidates(
    leaderboard: list[dict[str, Any]],
    trades_by_wallet: dict[str, list[dict[str, Any]]],
    min_trade_count: int = 5,
) -> list[WalletCandidate]:
    candidates: list[WalletCandidate] = []
    for row in leaderboard:
        wallet = str(row.get("proxyWallet") or row.get("wallet") or "").lower()
        if not wallet:
            continue
        trades = trades_by_wallet.get(wallet, [])
        if len(trades) < min_trade_count:
            continue

        topics = [_classify_topic(trade) for trade in trades]
        topic_counts = Counter(topic for topic in topics if topic != "unknown")
        dominant_topic, dominant_count = topic_counts.most_common(1)[0] if topic_counts else ("unknown", 0)
        trade_count = len(trades)
        unique_markets = len({str(trade.get("conditionId") or trade.get("slug") or trade.get("title")) for trade in trades})
        focus_ratio = dominant_count / trade_count if trade_count else 0.0
        average_trade_size = _average_trade_size(trades)
        pnl = float(row.get("pnl") or 0.0)
        volume = float(row.get("vol") or row.get("volume") or 0.0)
        score = _candidate_score(focus_ratio, trade_count, average_trade_size, pnl, volume)

        candidates.append(
            WalletCandidate(
                wallet=wallet,
                username=str(row.get("userName") or row.get("name") or wallet),
                dominant_topic=dominant_topic,
                score=round(score, 4),
                pnl=pnl,
                volume=volume,
                trade_count=trade_count,
                unique_markets=unique_markets,
                focus_ratio=round(focus_ratio, 4),
                average_trade_size=round(average_trade_size, 2),
                reason="recent activity, topic focus, trade size, and leaderboard pnl/volume",
            )
        )

    return sorted(candidates, key=lambda item: item.score, reverse=True)


def candidates_as_dicts(candidates: list[WalletCandidate]) -> list[dict[str, Any]]:
    return [asdict(candidate) for candidate in candidates]


def _candidate_score(
    focus_ratio: float,
    trade_count: int,
    average_trade_size: float,
    pnl: float,
    volume: float,
) -> float:
    focus_component = focus_ratio * 0.45
    activity_component = min(trade_count / 25 if trade_count > 0 else 0.0, 1.0) * 0.2
    size_component = min(average_trade_size / 500, 1.0) * 0.15
    pnl_component = min(max(pnl, 0.0) / 100_000, 1.0) * 0.15
    efficiency_component = min(max(pnl / volume if volume else 0.0, 0.0), 1.0) * 0.05
    return focus_component + activity_component + size_component + pnl_component + efficiency_component


def _classify_topic(trade: dict[str, Any]) -> str:
    text = " ".join(
        str(trade.get(key) or "").lower()
        for key in ("slug", "eventSlug", "title", "icon")
    )
    sports_markers = ("nba", "nfl", "nhl", "mlb", "soccer", "ufc", "tennis", "vs.")
    crypto_markers = ("btc", "bitcoin", "eth", "ethereum", "solana", "xrp", "crypto")
    politics_markers = ("election", "trump", "biden", "senate", "congress", "president", "fed", "rates")
    weather_markers = ("weather", "rain", "snow", "temperature", "hurricane")

    if any(marker in text for marker in sports_markers):
        return "sports"
    if any(marker in text for marker in crypto_markers):
        return "crypto"
    if any(marker in text for marker in politics_markers):
        return "politics"
    if any(marker in text for marker in weather_markers):
        return "weather"
    return "unknown"


def _average_trade_size(trades: list[dict[str, Any]]) -> float:
    sizes = [float(trade.get("size") or trade.get("sizeUsd") or trade.get("amount") or 0.0) for trade in trades]
    return sum(sizes) / len(sizes) if sizes else 0.0

from __future__ import annotations

import math
from collections import Counter, defaultdict

from .config import FilterConfig
from .models import MarketSnapshot, WalletQuality, WalletTrade


class WalletQualityScorer:
    def __init__(self, filters: FilterConfig, recency_half_life_seconds: int | None = None) -> None:
        self.filters = filters
        self.recency_half_life_seconds = recency_half_life_seconds or max(filters.max_trade_age_seconds // 2, 1)
        self.last_rejection_counts: dict[str, int] = {}
        self.last_wallet_rejection_counts: dict[str, dict[str, int]] = {}
        self.last_missing_market_samples: list[str] = []

    def score(self, trades: list[WalletTrade], markets: dict[str, MarketSnapshot]) -> dict[str, WalletQuality]:
        grouped: dict[str, list[WalletTrade]] = defaultdict(list)
        rejection_counts: Counter[str] = Counter()
        wallet_rejections: dict[str, Counter[str]] = defaultdict(Counter)
        missing_market_samples: list[str] = []
        for trade in trades:
            grouped[trade.wallet].append(trade)

        scores: dict[str, WalletQuality] = {}
        for wallet, wallet_trades in grouped.items():
            eligible = []
            for trade in _dedupe_wallet_trades(wallet_trades):
                reason = self.rejection_reason(trade, markets)
                if reason is None:
                    eligible.append(trade)
                else:
                    rejection_counts[reason] += 1
                    wallet_rejections[wallet][reason] += 1
                    if reason == "missing_market" and trade.market_id not in missing_market_samples and len(missing_market_samples) < 10:
                        missing_market_samples.append(trade.market_id)
            if not eligible:
                continue

            topic = Counter(trade.topic for trade in eligible).most_common(1)[0][0]
            average_age = sum(trade.age_seconds for trade in eligible) / len(eligible)
            drifts = [self._trade_drift(trade, markets[trade.market_id]) for trade in eligible if trade.market_id in markets]
            average_drift = sum(drifts) / len(drifts) if drifts else self.filters.max_price_drift

            freshness_score = freshness_decay(average_age, self.recency_half_life_seconds)
            drift_score = max(0.0, 1.0 - (average_drift / self.filters.max_price_drift)) if self.filters.max_price_drift else 0.0
            sample_score = min(len(eligible) / 5.0, 1.0)
            quality_score = round((freshness_score * 0.35) + (drift_score * 0.4) + (sample_score * 0.25), 4)

            scores[wallet] = WalletQuality(
                wallet=wallet,
                topic=topic,
                score=quality_score,
                eligible_trade_count=len(eligible),
                average_age_seconds=average_age,
                average_drift=average_drift,
                reason="exponential freshness, drift discipline, and sample size",
            )

        self.last_rejection_counts = dict(sorted(rejection_counts.items()))
        self.last_wallet_rejection_counts = {wallet: dict(sorted(counts.items())) for wallet, counts in sorted(wallet_rejections.items())}
        self.last_missing_market_samples = missing_market_samples
        return scores

    def rejection_reason(self, trade: WalletTrade, markets: dict[str, MarketSnapshot]) -> str | None:
        market = markets.get(trade.market_id)
        if market is None:
            return "missing_market"
        if trade.age_seconds > self.filters.max_trade_age_seconds:
            return "too_old"
        if trade.size_usd < self.filters.min_position_size_usd:
            return "too_small"
        if market.liquidity_usd < self.filters.min_liquidity_usd:
            return "low_liquidity"
        if market.minutes_to_resolution < self.filters.min_minutes_to_resolution:
            return "too_close_to_resolution"
        if market.minutes_to_resolution > self.filters.max_minutes_to_resolution:
            return "too_far_from_resolution"
        return None

    def _is_eligible_trade(self, trade: WalletTrade, markets: dict[str, MarketSnapshot]) -> bool:
        return self.rejection_reason(trade, markets) is None

    def _trade_drift(self, trade: WalletTrade, market: MarketSnapshot) -> float:
        current_price = market.yes_ask if trade.side.upper() == "YES" else market.no_ask
        return abs(current_price - trade.price)


def freshness_decay(age_seconds: float, half_life_seconds: int | float) -> float:
    if half_life_seconds <= 0:
        return 0.0
    return max(0.0, min(1.0, math.exp(-math.log(2) * max(age_seconds, 0.0) / half_life_seconds)))


def compute_copyability_score(
    consensus_ratio: float,
    wallet_quality_score: float,
    average_age_seconds: float,
    drift: float,
    liquidity_usd: float,
    side_spread: float,
    side_depth_usd: float,
    filters: FilterConfig,
    recency_half_life_seconds: int | None = None,
) -> float:
    freshness_score = freshness_decay(average_age_seconds, recency_half_life_seconds or max(filters.max_trade_age_seconds // 2, 1))
    drift_score = max(0.0, 1.0 - (drift / filters.max_price_drift)) if filters.max_price_drift else 0.0
    liquidity_score = min(liquidity_usd / (filters.min_liquidity_usd * 3), 1.0) if filters.min_liquidity_usd else 0.0
    spread_score = max(0.0, 1.0 - (side_spread / 0.1))
    depth_score = min(side_depth_usd / max(filters.min_position_size_usd, 1.0), 1.0)

    score = (
        consensus_ratio * 0.3
        + wallet_quality_score * 0.25
        + freshness_score * 0.15
        + drift_score * 0.1
        + liquidity_score * 0.08
        + spread_score * 0.07
        + depth_score * 0.05
    )
    return round(score, 4)


def _dedupe_wallet_trades(trades: list[WalletTrade]) -> list[WalletTrade]:
    deduped: dict[tuple[str, str, float, float, int], WalletTrade] = {}
    for trade in trades:
        key = (trade.market_id, trade.side.upper(), trade.price, trade.size_usd, trade.age_seconds)
        deduped.setdefault(key, trade)
    return list(deduped.values())

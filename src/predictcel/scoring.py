from __future__ import annotations

from collections import defaultdict

from .config import FilterConfig
from .models import MarketSnapshot, WalletQuality, WalletTrade


class WalletQualityScorer:
    def __init__(self, filters: FilterConfig) -> None:
        self.filters = filters

    def score(self, trades: list[WalletTrade], markets: dict[str, MarketSnapshot]) -> dict[str, WalletQuality]:
        grouped: dict[str, list[WalletTrade]] = defaultdict(list)
        for trade in trades:
            grouped[trade.wallet].append(trade)

        scores: dict[str, WalletQuality] = {}
        for wallet, wallet_trades in grouped.items():
            eligible = [trade for trade in wallet_trades if self._is_eligible_trade(trade, markets)]
            if not eligible:
                continue

            topic = eligible[0].topic
            average_age = sum(trade.age_seconds for trade in eligible) / len(eligible)
            drifts = [self._trade_drift(trade, markets[trade.market_id]) for trade in eligible if trade.market_id in markets]
            average_drift = sum(drifts) / len(drifts) if drifts else self.filters.max_price_drift

            freshness_score = max(0.0, 1.0 - (average_age / self.filters.max_trade_age_seconds))
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
                reason="freshness, drift discipline, and sample size",
            )
        return scores

    def _is_eligible_trade(self, trade: WalletTrade, markets: dict[str, MarketSnapshot]) -> bool:
        market = markets.get(trade.market_id)
        if market is None:
            return False
        if trade.age_seconds > self.filters.max_trade_age_seconds:
            return False
        if trade.size_usd < self.filters.min_position_size_usd:
            return False
        if market.liquidity_usd < self.filters.min_liquidity_usd:
            return False
        if market.minutes_to_resolution < self.filters.min_minutes_to_resolution:
            return False
        if market.minutes_to_resolution > self.filters.max_minutes_to_resolution:
            return False
        return True

    def _trade_drift(self, trade: WalletTrade, market: MarketSnapshot) -> float:
        current_price = market.yes_ask if trade.side.upper() == "YES" else market.no_ask
        return abs(current_price - trade.price)


def compute_copyability_score(
    consensus_ratio: float,
    wallet_quality_score: float,
    average_age_seconds: float,
    drift: float,
    liquidity_usd: float,
    side_spread: float,
    side_depth_usd: float,
    filters: FilterConfig,
) -> float:
    freshness_score = max(0.0, 1.0 - (average_age_seconds / filters.max_trade_age_seconds))
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

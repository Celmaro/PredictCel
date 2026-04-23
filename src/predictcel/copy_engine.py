from __future__ import annotations

from collections import Counter

from .config import AppConfig, BasketRule
from .models import CopyCandidate, MarketSnapshot, WalletQuality, WalletTrade
from .scoring import compute_copyability_score


class CopyEngine:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.baskets_by_topic = {basket.topic: basket for basket in config.baskets}

    def evaluate(
        self,
        trades: list[WalletTrade],
        markets: dict[str, MarketSnapshot],
        wallet_qualities: dict[str, WalletQuality] | None = None,
    ) -> list[CopyCandidate]:
        wallet_qualities = wallet_qualities or {}
        grouped: dict[str, list[WalletTrade]] = {}
        for trade in trades:
            grouped.setdefault(trade.market_id, []).append(trade)

        candidates: list[CopyCandidate] = []
        for market_id, market_trades in grouped.items():
            market = markets.get(market_id)
            if market is None:
                continue

            topic = self._resolve_topic(market_trades)
            basket = self.baskets_by_topic.get(topic)
            if basket is None:
                continue

            candidate = self._evaluate_market(topic, basket, market, market_trades, wallet_qualities)
            if candidate is not None:
                candidates.append(candidate)
        return candidates

    def _resolve_topic(self, trades: list[WalletTrade]) -> str:
        return Counter(trade.topic for trade in trades).most_common(1)[0][0]

    def _evaluate_market(
        self,
        topic: str,
        basket: BasketRule,
        market: MarketSnapshot,
        trades: list[WalletTrade],
        wallet_qualities: dict[str, WalletQuality],
    ) -> CopyCandidate | None:
        valid_trades = [
            trade
            for trade in trades
            if trade.topic == topic
            and trade.wallet in basket.wallets
            and trade.age_seconds <= self.config.filters.max_trade_age_seconds
            and trade.size_usd >= self.config.filters.min_position_size_usd
        ]
        if not valid_trades:
            return None

        if market.liquidity_usd < self.config.filters.min_liquidity_usd:
            return None
        if market.minutes_to_resolution < self.config.filters.min_minutes_to_resolution:
            return None
        if market.minutes_to_resolution > self.config.filters.max_minutes_to_resolution:
            return None

        unique_wallets = sorted({trade.wallet for trade in valid_trades})
        consensus_ratio = len(unique_wallets) / len(basket.wallets)
        if consensus_ratio < basket.quorum_ratio:
            return None

        side_counts = Counter(trade.side.upper() for trade in valid_trades)
        side, _ = side_counts.most_common(1)[0]
        aligned = [trade for trade in valid_trades if trade.side.upper() == side]
        if not aligned:
            return None

        reference_price = sum(trade.price for trade in aligned) / len(aligned)
        current_price = market.yes_ask if side == "YES" else market.no_ask
        drift = abs(current_price - reference_price)
        if drift > self.config.filters.max_price_drift:
            return None

        average_age = sum(trade.age_seconds for trade in aligned) / len(aligned)
        quality_values = [wallet_qualities[wallet].score for wallet in unique_wallets if wallet in wallet_qualities]
        wallet_quality_score = round(sum(quality_values) / len(quality_values), 4) if quality_values else 0.5
        side_spread = market.yes_spread if side == "YES" else market.no_spread
        side_ask_size = market.yes_ask_size if side == "YES" else market.no_ask_size
        side_depth_usd = side_ask_size * current_price
        copyability_score = compute_copyability_score(
            consensus_ratio=consensus_ratio,
            wallet_quality_score=wallet_quality_score,
            average_age_seconds=average_age,
            drift=drift,
            liquidity_usd=market.liquidity_usd,
            side_spread=side_spread,
            side_depth_usd=side_depth_usd,
            filters=self.config.filters,
        )

        return CopyCandidate(
            topic=topic,
            market_id=market.market_id,
            side=side,
            consensus_ratio=consensus_ratio,
            reference_price=reference_price,
            current_price=current_price,
            liquidity_usd=market.liquidity_usd,
            source_wallets=unique_wallets,
            wallet_quality_score=wallet_quality_score,
            copyability_score=copyability_score,
            reason="basket consensus, quality scoring, age, liquidity, drift, and orderbook filters passed",
        )

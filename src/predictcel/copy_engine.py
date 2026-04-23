from __future__ import annotations

from collections import Counter

from .config import AppConfig, BasketRule
from .models import CopyCandidate, MarketSnapshot, WalletTrade


class CopyEngine:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.baskets_by_topic = {basket.topic: basket for basket in config.baskets}

    def evaluate(
        self,
        trades: list[WalletTrade],
        markets: dict[str, MarketSnapshot],
    ) -> list[CopyCandidate]:
        grouped: dict[str, list[WalletTrade]] = {}
        for trade in trades:
            grouped.setdefault(trade.market_id, []).append(trade)

        candidates: list[CopyCandidate] = []
        for market_id, market_trades in grouped.items():
            market = markets.get(market_id)
            if market is None:
                continue

            basket = self.baskets_by_topic.get(market.topic)
            if basket is None:
                continue

            candidate = self._evaluate_market(basket, market, market_trades)
            if candidate is not None:
                candidates.append(candidate)
        return candidates

    def _evaluate_market(
        self,
        basket: BasketRule,
        market: MarketSnapshot,
        trades: list[WalletTrade],
    ) -> CopyCandidate | None:
        valid_trades = [
            trade
            for trade in trades
            if trade.wallet in basket.wallets
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

        return CopyCandidate(
            topic=market.topic,
            market_id=market.market_id,
            side=side,
            consensus_ratio=consensus_ratio,
            reference_price=reference_price,
            current_price=current_price,
            liquidity_usd=market.liquidity_usd,
            source_wallets=unique_wallets,
            reason="basket consensus, age, liquidity, and drift filters passed",
        )

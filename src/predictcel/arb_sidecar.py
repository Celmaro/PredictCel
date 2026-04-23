from __future__ import annotations

from .config import ArbitrageConfig
from .models import ArbitrageOpportunity, MarketSnapshot


class ArbitrageSidecar:
    def __init__(self, config: ArbitrageConfig) -> None:
        self.config = config

    def scan(self, markets: dict[str, MarketSnapshot]) -> list[ArbitrageOpportunity]:
        opportunities: list[ArbitrageOpportunity] = []
        for market in markets.values():
            if market.liquidity_usd < self.config.min_liquidity_usd:
                continue
            total_cost = market.yes_ask + market.no_ask
            gross_edge = round(1.0 - total_cost, 6)
            if gross_edge < self.config.min_gross_edge:
                continue
            opportunities.append(
                ArbitrageOpportunity(
                    market_id=market.market_id,
                    topic=market.topic,
                    yes_ask=market.yes_ask,
                    no_ask=market.no_ask,
                    total_cost=total_cost,
                    gross_edge=gross_edge,
                    liquidity_usd=market.liquidity_usd,
                    reason="yes/no complete set costs less than one dollar",
                )
            )
        return sorted(opportunities, key=lambda item: item.gross_edge, reverse=True)

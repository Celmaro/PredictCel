from __future__ import annotations

from .config import ArbitrageConfig
from .models import ArbitrageOpportunity, MarketSnapshot

MINUTES_PER_YEAR = 525_600


class ArbitrageSidecar:
    def __init__(self, config: ArbitrageConfig) -> None:
        self.config = config

    def scan(self, markets: dict[str, MarketSnapshot]) -> list[ArbitrageOpportunity]:
        opportunities: list[ArbitrageOpportunity] = []
        for market in markets.values():
            opportunity = self._evaluate_market(market)
            if opportunity is not None:
                opportunities.append(opportunity)
        return sorted(opportunities, key=lambda item: (item.quality_score, item.net_edge), reverse=True)

    def _evaluate_market(self, market: MarketSnapshot) -> ArbitrageOpportunity | None:
        if market.liquidity_usd < self.config.min_liquidity_usd:
            return None
        if market.yes_ask <= 0 or market.no_ask <= 0:
            return None

        total_cost = round(market.yes_ask + market.no_ask, 6)
        gross_edge = round(1.0 - total_cost, 6)
        if gross_edge < self.config.min_gross_edge:
            return None

        fixed_cost = self.config.gas_cost_per_tx_usd * self.config.settlement_tx_count
        variable_cost_rate = self.config.variable_cost_rate + self.config.slippage_rate
        net_edge_rate = gross_edge - variable_cost_rate
        if net_edge_rate <= 0:
            return None

        min_profitable_position = max(
            self.config.min_profitable_position_usd,
            fixed_cost / net_edge_rate if fixed_cost > 0 else self.config.min_profitable_position_usd,
        )
        min_profitable_position = round(min_profitable_position, 4)
        if min_profitable_position > self.config.max_position_usd:
            return None

        safe_position_size = min(self.config.max_position_usd, market.liquidity_usd * 0.05)
        safe_position_size = round(max(min_profitable_position, safe_position_size), 4)
        fixed_cost_rate = fixed_cost / safe_position_size if safe_position_size > 0 else 1.0
        net_edge = round(net_edge_rate - fixed_cost_rate, 6)
        if net_edge <= 0:
            return None

        annualized_return = self._annualized_return(net_edge, total_cost, market.minutes_to_resolution)
        if annualized_return < self.config.target_annualized_return:
            return None

        liquidity_score = self._liquidity_score(market)
        speed_score = self._speed_score(market.minutes_to_resolution)
        confidence_score = self._confidence_score(market, net_edge)
        edge_score = min(net_edge / max(self.config.min_gross_edge, 0.000001), 1.0)
        quality_score = round(
            (edge_score * self.config.edge_weight)
            + (liquidity_score * self.config.liquidity_weight)
            + (speed_score * self.config.speed_weight)
            + (confidence_score * self.config.confidence_weight),
            4,
        )
        gas_cost_percentage = round(fixed_cost_rate, 6)

        return ArbitrageOpportunity(
            market_id=market.market_id,
            topic=market.topic,
            yes_ask=market.yes_ask,
            no_ask=market.no_ask,
            total_cost=total_cost,
            gross_edge=gross_edge,
            liquidity_usd=market.liquidity_usd,
            reason="complete-set underpricing passes cost, size, liquidity, and APR filters",
            net_edge=net_edge,
            annualized_return=annualized_return,
            min_profitable_position=min_profitable_position,
            safe_position_size=safe_position_size,
            quality_score=quality_score,
            liquidity_score=liquidity_score,
            speed_score=speed_score,
            confidence_score=confidence_score,
            gas_cost_percentage=gas_cost_percentage,
            resolution_risk=self._resolution_risk(market.minutes_to_resolution),
            estimated_slippage=round(self.config.slippage_rate, 6),
            best_execution_path="direct",
        )

    def _annualized_return(self, net_edge: float, total_cost: float, minutes_to_resolution: int) -> float:
        minutes = max(minutes_to_resolution, 1)
        capital_required = max(total_cost, 0.000001)
        annualized = (net_edge / capital_required) * (MINUTES_PER_YEAR / minutes)
        return round(min(annualized, self.config.max_annualized_return), 6)

    def _liquidity_score(self, market: MarketSnapshot) -> float:
        target = max(self.config.min_liquidity_usd * 3, 1.0)
        return round(min(market.liquidity_usd / target, 1.0), 4)

    def _speed_score(self, minutes_to_resolution: int) -> float:
        if minutes_to_resolution <= 0:
            return 0.0
        if minutes_to_resolution <= 60:
            return 0.65
        if minutes_to_resolution <= 360:
            return 1.0
        if minutes_to_resolution <= 1440:
            return 0.75
        return 0.35

    def _confidence_score(self, market: MarketSnapshot, net_edge: float) -> float:
        orderbook_bonus = 0.15 if market.orderbook_ready else 0.0
        spread_penalty = min(max(market.yes_spread, market.no_spread) / 0.10, 0.35)
        edge_component = min(net_edge / max(self.config.min_gross_edge, 0.000001), 0.85)
        return round(max(0.0, min(edge_component + orderbook_bonus - spread_penalty, 1.0)), 4)

    def _resolution_risk(self, minutes_to_resolution: int) -> str:
        if minutes_to_resolution <= 0:
            return "HIGH"
        if minutes_to_resolution < 60:
            return "HIGH"
        if minutes_to_resolution <= 1440:
            return "LOW"
        return "MEDIUM"

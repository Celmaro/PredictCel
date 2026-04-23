from __future__ import annotations

from collections import Counter, defaultdict

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

        wallet_votes = self._latest_wallet_votes(valid_trades)
        weighted_by_side: dict[str, float] = defaultdict(float)
        raw_by_side: Counter = Counter()
        for trade in wallet_votes.values():
            side = trade.side.upper()
            weighted_by_side[side] += self._trade_weight(trade, wallet_qualities)
            raw_by_side[side] += 1

        if not weighted_by_side:
            return None
        side = max(weighted_by_side, key=weighted_by_side.get)
        aligned = [trade for trade in wallet_votes.values() if trade.side.upper() == side]
        if not aligned:
            return None

        aligned_wallets = sorted({trade.wallet for trade in aligned})
        consensus_ratio = len(aligned_wallets) / len(basket.wallets)
        total_weight = sum(weighted_by_side.values())
        aligned_weight = weighted_by_side[side]
        weighted_consensus = round(aligned_weight / total_weight, 4) if total_weight else 0.0
        if consensus_ratio < basket.quorum_ratio:
            return None
        if weighted_consensus < self.config.consensus.min_weighted_consensus:
            return None

        confidence_score = self._confidence_score(aligned_weight, total_weight)
        if confidence_score < self.config.consensus.min_confidence_score:
            return None

        reference_price = self._weighted_reference_price(aligned, wallet_qualities)
        current_price = market.yes_ask if side == "YES" else market.no_ask
        drift = abs(current_price - reference_price)
        if drift > self.config.filters.max_price_drift:
            return None

        average_age = sum(trade.age_seconds for trade in aligned) / len(aligned)
        quality_values = [wallet_qualities[wallet].score for wallet in aligned_wallets if wallet in wallet_qualities]
        wallet_quality_score = round(sum(quality_values) / len(quality_values), 4) if quality_values else 0.5
        side_spread = market.yes_spread if side == "YES" else market.no_spread
        side_ask_size = market.yes_ask_size if side == "YES" else market.no_ask_size
        side_depth_usd = side_ask_size * current_price
        conflict_penalty = self._conflict_penalty(aligned_weight, total_weight)
        recency_score = self._recency_score(aligned)
        base_score = compute_copyability_score(
            consensus_ratio=weighted_consensus,
            wallet_quality_score=wallet_quality_score,
            average_age_seconds=average_age,
            drift=drift,
            liquidity_usd=market.liquidity_usd,
            side_spread=side_spread,
            side_depth_usd=side_depth_usd,
            filters=self.config.filters,
        )
        copyability_score = round(max(0.0, min(1.0, (base_score * 0.75) + (confidence_score * 0.15) + (recency_score * 0.10) - conflict_penalty)), 4)
        suggested_position_usd = self._suggested_position_size(current_price, confidence_score, copyability_score)

        return CopyCandidate(
            topic=topic,
            market_id=market.market_id,
            side=side,
            consensus_ratio=consensus_ratio,
            reference_price=reference_price,
            current_price=current_price,
            liquidity_usd=market.liquidity_usd,
            source_wallets=aligned_wallets,
            wallet_quality_score=wallet_quality_score,
            copyability_score=copyability_score,
            reason="weighted basket consensus, confidence, recency, liquidity, drift, and orderbook filters passed",
            weighted_consensus=weighted_consensus,
            confidence_score=confidence_score,
            conflict_penalty=conflict_penalty,
            recency_score=recency_score,
            suggested_position_usd=suggested_position_usd,
        )

    def _latest_wallet_votes(self, trades: list[WalletTrade]) -> dict[str, WalletTrade]:
        latest: dict[str, WalletTrade] = {}
        for trade in trades:
            existing = latest.get(trade.wallet)
            if existing is None or trade.age_seconds < existing.age_seconds:
                latest[trade.wallet] = trade
        return latest

    def _trade_weight(self, trade: WalletTrade, wallet_qualities: dict[str, WalletQuality]) -> float:
        quality = wallet_qualities.get(trade.wallet)
        quality_weight = quality.score if quality is not None else 0.5
        recency_weight = 0.5 ** (trade.age_seconds / self.config.consensus.recency_half_life_seconds)
        size_weight = min(max(trade.size_usd / max(self.config.filters.min_position_size_usd, 1.0), 0.25), 3.0)
        return max(quality_weight * recency_weight * size_weight, 0.0001)

    def _weighted_reference_price(self, trades: list[WalletTrade], wallet_qualities: dict[str, WalletQuality]) -> float:
        weighted_sum = 0.0
        total_weight = 0.0
        for trade in trades:
            weight = self._trade_weight(trade, wallet_qualities)
            weighted_sum += trade.price * weight
            total_weight += weight
        return weighted_sum / total_weight if total_weight else sum(trade.price for trade in trades) / len(trades)

    def _confidence_score(self, aligned_weight: float, total_weight: float) -> float:
        prior = self.config.consensus.confidence_prior_strength
        posterior_mean = (aligned_weight + prior * 0.5) / (total_weight + prior) if total_weight + prior else 0.0
        sample_strength = min(total_weight / (total_weight + prior), 1.0) if total_weight + prior else 0.0
        return round(max(0.0, min(1.0, posterior_mean * sample_strength)), 4)

    def _conflict_penalty(self, aligned_weight: float, total_weight: float) -> float:
        if total_weight <= 0:
            return 0.0
        conflict_ratio = max(0.0, 1.0 - (aligned_weight / total_weight))
        return round(conflict_ratio * self.config.consensus.conflict_penalty_weight, 4)

    def _recency_score(self, trades: list[WalletTrade]) -> float:
        if not trades:
            return 0.0
        weights = [0.5 ** (trade.age_seconds / self.config.consensus.recency_half_life_seconds) for trade in trades]
        return round(sum(weights) / len(weights), 4)

    def _suggested_position_size(self, price: float, confidence_score: float, copyability_score: float) -> float:
        if price <= 0 or price >= 1:
            return 0.0
        b = (1.0 - price) / price
        p = max(0.0, min(1.0, (confidence_score + copyability_score) / 2.0))
        q = 1.0 - p
        kelly_fraction = max(0.0, ((b * p) - q) / b) if b > 0 else 0.0
        raw_size = self.config.consensus.bankroll_usd * kelly_fraction * self.config.consensus.kelly_fraction
        return round(min(max(raw_size, 0.0), self.config.consensus.max_suggested_position_usd), 4)

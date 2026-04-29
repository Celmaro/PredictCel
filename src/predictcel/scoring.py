"""
Wallet quality scoring and copyability calculation.

This module provides algorithms for scoring wallet trading quality
and calculating copyability scores for trade candidates.
"""

from __future__ import annotations
import math
from collections import Counter, defaultdict
from .config import FilterConfig
from .models import MarketSnapshot, WalletQuality, WalletTrade


class WalletQualityScorer:
    """Scores wallets based on their trading activity quality.

    Analyzes wallet trades against market data to determine:
    - Trade freshness (recency)
    - Price drift from entry
    - Sample size adequacy
    - Eligibility against filters

    Attributes:
        filters: Configuration for trade filtering
        recency_half_life_seconds: Half-life for freshness decay calculation
        last_rejection_counts: Statistics on why trades were rejected
        last_wallet_rejection_counts: Per-wallet rejection statistics
        last_missing_market_samples: Sample of markets not found
    """

    def __init__(
        self, filters: FilterConfig, recency_half_life_seconds: int | None = None
    ) -> None:
        """Initialize the scorer with filter configuration.

        Args:
            filters: Configuration for filtering trades
            recency_half_life_seconds: Optional override for freshness decay
        """
        self.filters = filters
        self.recency_half_life_seconds = recency_half_life_seconds or max(
            filters.max_trade_age_seconds // 2, 1
        )
        self.last_rejection_counts: dict[str, int] = {}
        self.last_wallet_rejection_counts: dict[str, dict[str, int]] = {}
        self.last_missing_market_samples: list[str] = []
        self.last_missing_market_breakdown: dict[str, int] = {}
        self.last_missing_market_by_wallet: dict[str, int] = {}
        self.last_missing_market_samples_by_wallet: dict[str, list[str]] = {}
        self.last_wallet_attrition: dict[str, int] = {}

    def score(
        self, trades: list[WalletTrade], markets: dict[str, MarketSnapshot]
    ) -> dict[str, WalletQuality]:
        """Score all wallets based on their trading activity.

        Groups trades by wallet, filters eligible trades, and calculates
        quality scores based on freshness, drift, and sample size.

        Args:
            trades: List of all wallet trades to analyze
            markets: Dictionary of market_id -> MarketSnapshot

        Returns:
            Dictionary mapping wallet addresses to WalletQuality scores
        """
        grouped: dict[str, list[WalletTrade]] = defaultdict(list)
        rejection_counts: Counter[str] = Counter()
        wallet_rejections: dict[str, Counter[str]] = defaultdict(Counter)
        missing_market_samples: list[str] = []
        missing_market_breakdown: Counter[str] = Counter()
        missing_market_by_wallet: Counter[str] = Counter()
        missing_market_samples_by_wallet: dict[str, list[str]] = defaultdict(list)
        market_lookup = _build_market_lookup(markets)

        for trade in trades:
            grouped[trade.wallet].append(trade)

        scores: dict[str, WalletQuality] = {}
        for wallet, wallet_trades in grouped.items():
            eligible = []
            for trade in _dedupe_wallet_trades(wallet_trades):
                reason = self.rejection_reason(
                    trade, markets, market_lookup=market_lookup
                )
                if reason is None:
                    eligible.append(trade)
                else:
                    rejection_counts[reason] += 1
                    wallet_rejections[wallet][reason] += 1
                    if (
                        reason == "missing_market"
                        and trade.market_id not in missing_market_samples
                        and len(missing_market_samples) < 10
                    ):
                        missing_market_samples.append(trade.market_id)
                    if reason == "missing_market":
                        missing_market_breakdown[
                            _classify_missing_market_id(trade.market_id)
                        ] += 1
                        missing_market_by_wallet[wallet] += 1
                        if (
                            trade.market_id
                            not in missing_market_samples_by_wallet[wallet]
                            and len(missing_market_samples_by_wallet[wallet]) < 5
                        ):
                            missing_market_samples_by_wallet[wallet].append(
                                trade.market_id
                            )

            if not eligible:
                continue

            topic = Counter(trade.topic for trade in eligible).most_common(1)[0][0]
            average_age = sum(trade.age_seconds for trade in eligible) / len(eligible)
            drifts = [
                self._trade_drift(
                    trade, _resolve_market(trade.market_id, market_lookup)
                )
                for trade in eligible
                if _resolve_market(trade.market_id, market_lookup) is not None
            ]
            average_drift = (
                sum(drifts) / len(drifts) if drifts else self.filters.max_price_drift
            )

            freshness_score = freshness_decay(
                average_age, self.recency_half_life_seconds
            )
            drift_score = (
                max(0.0, 1.0 - (average_drift / self.filters.max_price_drift))
                if self.filters.max_price_drift > 0
                else 1.0
                if self.filters.max_price_drift
                else 0.0
            )
            sample_score = min(len(eligible) / 5.0, 1.0)
            specialization_score = _topic_specialization_score(eligible)
            activity_score = _human_pace_activity_score(eligible)
            liquidity_score = _average_liquidity_score(eligible, markets, self.filters)
            copy_safety_score = _copy_safety_score(eligible, markets)
            quality_score = round(
                (freshness_score * 0.22)
                + (drift_score * 0.22)
                + (sample_score * 0.14)
                + (specialization_score * 0.16)
                + (activity_score * 0.10)
                + (liquidity_score * 0.10)
                + (copy_safety_score * 0.06),
                4,
            )

            scores[wallet] = WalletQuality(
                wallet=wallet,
                topic=topic,
                score=quality_score,
                eligible_trade_count=len(eligible),
                average_age_seconds=average_age,
                average_drift=average_drift,
                reason="exponential freshness, drift discipline, sample size, topic specialization, human trading pace, liquid markets, and copy-safe sizing",
                freshness_score=round(freshness_score, 4),
                drift_score=round(drift_score, 4),
                sample_score=round(sample_score, 4),
                specialization_score=round(specialization_score, 4),
                activity_score=round(activity_score, 4),
                liquidity_score=round(liquidity_score, 4),
                copy_safety_score=round(copy_safety_score, 4),
            )

        self.last_rejection_counts = dict(sorted(rejection_counts.items()))
        self.last_wallet_rejection_counts = {
            wallet: dict(sorted(counts.items()))
            for wallet, counts in sorted(wallet_rejections.items())
        }
        self.last_missing_market_samples = missing_market_samples
        self.last_missing_market_breakdown = dict(
            sorted(missing_market_breakdown.items())
        )
        self.last_missing_market_by_wallet = dict(
            sorted(missing_market_by_wallet.items())
        )
        self.last_missing_market_samples_by_wallet = {
            wallet: samples
            for wallet, samples in sorted(missing_market_samples_by_wallet.items())
            if samples
        }
        self.last_wallet_attrition = {
            "wallets_seen": len(grouped),
            "wallets_scored": len(scores),
            "wallets_fully_rejected": max(0, len(grouped) - len(scores)),
        }
        return scores

    def rejection_reason(
        self,
        trade: WalletTrade,
        markets: dict[str, MarketSnapshot],
        *,
        market_lookup: dict[str, MarketSnapshot] | None = None,
    ) -> str | None:
        """Determine why a trade is ineligible, if it is.

        Args:
            trade: The trade to check
            markets: Available market data

        Returns:
            Rejection reason string, or None if trade is eligible
        """
        market = _resolve_market(
            trade.market_id,
            market_lookup
            if market_lookup is not None
            else _build_market_lookup(markets),
        )
        if market is None:
            return "missing_market"
        if trade.age_seconds > self.filters.max_trade_age_seconds:
            return "too_old"
        if trade.size_usd < self.filters.min_position_size_usd:
            return "too_small"
        if market.liquidity_usd < self.filters.min_liquidity_usd:
            return "low_liquidity"
        return None

    def _is_eligible_trade(
        self, trade: WalletTrade, markets: dict[str, MarketSnapshot]
    ) -> bool:
        """Check if a trade passes all eligibility filters.

        Args:
            trade: The trade to check
            markets: Available market data

        Returns:
            True if trade is eligible, False otherwise
        """
        return self.rejection_reason(trade, markets) is None

    def _trade_drift(self, trade: WalletTrade, market: MarketSnapshot) -> float:
        """Calculate price drift from trade entry to current market.

        Args:
            trade: The historical trade
            market: Current market snapshot

        Returns:
            Absolute price drift
        """
        current_price = market.yes_ask if trade.side.upper() == "YES" else market.no_ask
        return abs(current_price - trade.price)


def freshness_decay(age_seconds: float, half_life_seconds: int | float) -> float:
    """Calculate freshness score using exponential decay.

    The score decays exponentially based on age relative to half-life.
    At half-life age, score is 0.5. At 2x half-life, score is 0.25, etc.

    Args:
        age_seconds: Age of the data in seconds
        half_life_seconds: Half-life for the decay calculation

    Returns:
        Freshness score between 0.0 and 1.0
    """
    if half_life_seconds <= 0:
        return 0.0
    return max(
        0.0,
        min(1.0, math.exp(-math.log(2) * max(age_seconds, 0.0) / half_life_seconds)),
    )


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
    """Compute overall copyability score for a trade candidate.

    Combines multiple factors into a single copyability score:
    - Consensus ratio (30%): Agreement among source wallets
    - Wallet quality (25%): Average quality of source wallets
    - Freshness (15%): Recency of the signal
    - Drift (10%): Price movement since signal
    - Liquidity (8%): Market liquidity
    - Spread (7%): Bid-ask spread
    - Depth (5%): Order book depth

    Args:
        consensus_ratio: Ratio of wallets agreeing on trade
        wallet_quality_score: Average quality of source wallets
        average_age_seconds: Average age of source trades
        drift: Price drift from entry
        liquidity_usd: Available market liquidity
        side_spread: Bid-ask spread for the side
        side_depth_usd: Order book depth for the side
        filters: Configuration for thresholds
        recency_half_life_seconds: Optional override for freshness

    Returns:
        Copyability score between 0.0 and 1.0
    """
    freshness_score = freshness_decay(
        average_age_seconds,
        recency_half_life_seconds or max(filters.max_trade_age_seconds // 2, 1),
    )
    drift_score = (
        max(0.0, 1.0 - (drift / filters.max_price_drift))
        if filters.max_price_drift > 0
        else 1.0
        if filters.max_price_drift
        else 0.0
    )
    liquidity_score = (
        min(liquidity_usd / (filters.min_liquidity_usd * 3), 1.0)
        if filters.min_liquidity_usd
        else 0.0
    )
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


def _normalize_market_lookup_key(value: str) -> str:
    return str(value).strip().lower()


def _build_market_lookup(
    markets: dict[str, MarketSnapshot],
) -> dict[str, MarketSnapshot]:
    lookup: dict[str, MarketSnapshot] = {}
    for market_id, snapshot in markets.items():
        normalized_key = _normalize_market_lookup_key(market_id)
        if normalized_key:
            lookup.setdefault(normalized_key, snapshot)
        canonical_key = _normalize_market_lookup_key(snapshot.market_id)
        if canonical_key:
            lookup.setdefault(canonical_key, snapshot)
    return lookup


def _resolve_market(
    market_id: str,
    market_lookup: dict[str, MarketSnapshot],
) -> MarketSnapshot | None:
    normalized_key = _normalize_market_lookup_key(market_id)
    if not normalized_key:
        return None
    return market_lookup.get(normalized_key)


def _classify_missing_market_id(market_id: str) -> str:
    normalized = _normalize_market_lookup_key(market_id)
    if not normalized:
        return "empty"
    if normalized.startswith("0x") or "token" in normalized:
        return "token_id_like"
    if "-" in normalized:
        return "slug_like"
    if (
        normalized.startswith("cond")
        or normalized.startswith("condition")
        or "_" in normalized
    ):
        return "condition_id_like"
    return "other"


def _dedupe_wallet_trades(trades: list[WalletTrade]) -> list[WalletTrade]:
    """Remove duplicate trades based on key fields.

    Args:
        trades: List of trades that may contain duplicates

    Returns:
        List with duplicates removed, keeping first occurrence
    """
    deduped: dict[tuple[str, str, float, float, int], WalletTrade] = {}
    for trade in trades:
        key = (
            trade.market_id,
            trade.side.upper(),
            trade.price,
            trade.size_usd,
            trade.age_seconds,
        )
        deduped.setdefault(key, trade)
    return list(deduped.values())


def _topic_specialization_score(trades: list[WalletTrade]) -> float:
    if not trades:
        return 0.0
    topic_counts = Counter(trade.topic for trade in trades)
    dominant_share = max(topic_counts.values()) / len(trades)
    unique_topics = len(topic_counts)
    if unique_topics <= 3:
        breadth_penalty = 1.0
    else:
        breadth_penalty = max(0.25, 1.0 - ((unique_topics - 3) * 0.15))
    return round(max(0.0, min(dominant_share * breadth_penalty, 1.0)), 4)


def _human_pace_activity_score(trades: list[WalletTrade]) -> float:
    if not trades:
        return 0.0
    ages = [max(trade.age_seconds, 0) for trade in trades]
    observed_window_seconds = max(max(ages) - min(ages), 86_400)
    trades_per_day = len(trades) * 86_400 / observed_window_seconds
    if trades_per_day < 1.0:
        return round(max(trades_per_day, 0.2), 4)
    if trades_per_day <= 10.0:
        return 1.0
    if trades_per_day <= 20.0:
        return round(max(0.4, 1.0 - ((trades_per_day - 10.0) * 0.06)), 4)
    return round(max(0.0, 0.4 - ((trades_per_day - 20.0) * 0.02)), 4)


def _average_liquidity_score(
    trades: list[WalletTrade],
    markets: dict[str, MarketSnapshot],
    filters: FilterConfig,
) -> float:
    liquidities = [
        markets[trade.market_id].liquidity_usd
        for trade in trades
        if trade.market_id in markets
    ]
    if not liquidities:
        return 0.0
    average_liquidity = sum(liquidities) / len(liquidities)
    if filters.min_liquidity_usd <= 0:
        return 1.0
    return round(min(average_liquidity / (filters.min_liquidity_usd * 4.0), 1.0), 4)


def _copy_safety_score(
    trades: list[WalletTrade],
    markets: dict[str, MarketSnapshot],
) -> float:
    size_to_liquidity = [
        trade.size_usd / max(markets[trade.market_id].liquidity_usd, 1.0)
        for trade in trades
        if trade.market_id in markets
    ]
    if not size_to_liquidity:
        return 0.0
    average_ratio = sum(size_to_liquidity) / len(size_to_liquidity)
    if average_ratio <= 0.01:
        return 1.0
    if average_ratio >= 0.20:
        return 0.0
    return round(max(0.0, 1.0 - ((average_ratio - 0.01) / 0.19)), 4)

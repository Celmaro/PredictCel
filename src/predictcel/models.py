"""
PredictCel data models.

This module defines all data structures used throughout the PredictCel system,
including wallet trades, market snapshots, quality scores, and execution results.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

__all__ = [
    "WalletTrade",
    "MarketSnapshot",
    "WalletQuality",
    "MarketRegime",
    "CopyCandidate",
    "WalletTopicProfile",
    "WalletDiscoveryCandidate",
    "BasketAssignment",
    "BasketManagerAction",
    "ArbitrageOpportunity",
    "ExecutionIntent",
    "ExecutionResult",
    "Position",
]


@dataclass(frozen=True)
class WalletTrade:
    """Represents a single trade made by a wallet.
    
    Attributes:
        wallet: The wallet address that made the trade
        topic: The market topic/category (e.g., "sports", "crypto")
        market_id: Unique identifier for the market
        side: Trade direction - either "YES" or "NO"
        price: Price at which the trade was executed (0.0 to 1.0)
        size_usd: Size of the trade in USD
        age_seconds: How long ago the trade occurred
        timestamp: Optional datetime when the trade occurred
    """
    wallet: str
    topic: str
    market_id: str
    side: Literal["YES", "NO"]
    price: float
    size_usd: float
    age_seconds: int
    timestamp: datetime | None = None


@dataclass(frozen=True)
class MarketSnapshot:
    """Snapshot of market state at a point in time.
    
    Attributes:
        market_id: Unique identifier for the market
        topic: The market topic/category
        title: Human-readable market title
        yes_ask: Current asking price for YES tokens
        no_ask: Current asking price for NO tokens
        best_bid: Best available bid price
        liquidity_usd: Total liquidity in the market
        minutes_to_resolution: Minutes until market resolution
        yes_token_id: Token ID for YES outcome
        no_token_id: Token ID for NO outcome
        yes_bid: Best bid for YES tokens
        no_bid: Best bid for NO tokens
        yes_ask_size: Size available at YES ask price
        no_ask_size: Size available at NO ask price
        yes_spread: Spread for YES side
        no_spread: Spread for NO side
        orderbook_ready: Whether orderbook data is complete
        snapshot_time: When the snapshot was taken
        resolved_outcome: Final outcome if resolved
        resolution_price: Final price if resolved
    """
    market_id: str
    topic: str
    title: str
    yes_ask: float
    no_ask: float
    best_bid: float
    liquidity_usd: float
    minutes_to_resolution: int
    yes_token_id: str = ""
    no_token_id: str = ""
    yes_bid: float = 0.0
    no_bid: float = 0.0
    yes_ask_size: float = 0.0
    no_ask_size: float = 0.0
    yes_spread: float = 0.0
    no_spread: float = 0.0
    orderbook_ready: bool = False
    snapshot_time: datetime | None = None
    resolved_outcome: str | None = None
    resolution_price: float | None = None


@dataclass(frozen=True)
class WalletQuality:
    """Quality assessment for a wallet's trading activity.
    
    Attributes:
        wallet: The wallet address
        topic: Primary topic the wallet trades in
        score: Overall quality score (0.0 to 1.0)
        eligible_trade_count: Number of trades passing filters
        average_age_seconds: Average age of eligible trades
        average_drift: Average price drift from trade time
        reason: Explanation of the scoring
    """
    wallet: str
    topic: str
    score: float
    eligible_trade_count: int
    average_age_seconds: float
    average_drift: float
    reason: str


@dataclass(frozen=True)
class MarketRegime:
    """Classification of current market conditions.
    
    Attributes:
        label: Regime classification (e.g., "TRENDING", "RANGING")
        score: Confidence in the classification (0.0 to 1.0)
        reason: Explanation of the classification
    """
    label: str
    score: float
    reason: str


@dataclass(frozen=True)
class CopyCandidate:
    """A trade candidate identified for copying.
    
    Attributes:
        topic: Market topic
        market_id: Unique market identifier
        side: Trade side ("YES" or "NO")
        consensus_ratio: Ratio of wallets agreeing on this trade
        reference_price: Historical reference price
        current_price: Current market price
        liquidity_usd: Available liquidity
        source_wallets: Wallets supporting this trade
        wallet_quality_score: Average quality of source wallets
        copyability_score: Overall copyability score (0.0 to 1.0)
        reason: Explanation for the recommendation
        market_title: Human-readable market title
        weighted_consensus: Time-weighted consensus score
        confidence_score: Statistical confidence in the signal
        conflict_penalty: Penalty for conflicting trades
        recency_score: Score based on trade recency
        suggested_position_usd: Recommended position size
        market_regime: Current market regime
        regime_score: Confidence in regime classification
        regime_reason: Explanation of regime
    """
    topic: str
    market_id: str
    side: Literal["YES", "NO"]
    consensus_ratio: float
    reference_price: float
    current_price: float
    liquidity_usd: float
    source_wallets: list[str]
    wallet_quality_score: float
    copyability_score: float
    reason: str
    market_title: str = ""
    weighted_consensus: float = 0.0
    confidence_score: float = 0.0
    conflict_penalty: float = 0.0
    recency_score: float = 0.0
    suggested_position_usd: float = 0.0
    market_regime: str = "UNKNOWN"
    regime_score: float = 0.0
    regime_reason: str = ""


@dataclass(frozen=True)
class WalletTopicProfile:
    """Profile of a wallet's trading preferences across topics.
    
    Attributes:
        topic_affinities: Score for each topic (0.0 to 1.0)
        primary_topic: Topic with highest affinity
        specialization_score: How concentrated the wallet is (0.0 to 1.0)
    """
    topic_affinities: dict[str, float]
    primary_topic: str
    specialization_score: float


@dataclass(frozen=True)
class WalletDiscoveryCandidate:
    """A wallet discovered as a potential signal source.
    
    Attributes:
        wallet_address: The discovered wallet address
        source: How the wallet was discovered
        total_trades: Total number of trades observed
        recent_trades: Trades within the recent window
        avg_trade_size_usd: Average trade size
        topic_profile: Trading profile across topics
        score: Overall quality score
        confidence: Confidence level ("HIGH", "MEDIUM", "LOW")
        rejected_reasons: Reasons for rejection if filtered out
    """
    wallet_address: str
    source: str
    total_trades: int
    recent_trades: int
    avg_trade_size_usd: float
    topic_profile: WalletTopicProfile
    score: float
    confidence: Literal["HIGH", "MEDIUM", "LOW"]
    rejected_reasons: list[str]


@dataclass(frozen=True)
class BasketAssignment:
    """Assignment of a wallet to topic baskets.
    
    Attributes:
        wallet_address: The wallet being assigned
        primary_topic: Main topic for this wallet
        recommended_baskets: Baskets the wallet should be added to
        topic_affinities: Affinity scores for each topic
        overall_score: Combined quality score
        confidence: Confidence level
        reasons: Explanation for the assignment
    """
    wallet_address: str
    primary_topic: str
    recommended_baskets: list[str]
    topic_affinities: dict[str, float]
    overall_score: float
    confidence: Literal["HIGH", "MEDIUM", "LOW"]
    reasons: list[str]


@dataclass(frozen=True)
class BasketManagerAction:
    """Action recommended by the basket manager.
    
    Attributes:
        action: Type of action ("ADD", "REMOVE", "REVIEW")
        basket: Target basket name
        wallet_address: Wallet to act on
        score: Quality score triggering the action
        confidence: Confidence level
        reason: Explanation for the action
    """
    action: Literal["ADD", "REMOVE", "REVIEW"]
    basket: str
    wallet_address: str
    score: float
    confidence: Literal["HIGH", "MEDIUM", "LOW"]
    reason: str


@dataclass(frozen=True)
class ArbitrageOpportunity:
    """An identified arbitrage opportunity.
    
    Attributes:
        market_id: Market identifier
        topic: Market topic
        yes_ask: Current YES ask price
        no_ask: Current NO ask price
        total_cost: Combined cost for arbitrage
        gross_edge: Raw profit margin
        liquidity_usd: Available liquidity
        reason: Explanation of the opportunity
        net_edge: Profit after costs
        annualized_return: Return as annualized percentage
        min_profitable_position: Minimum viable position
        safe_position_size: Recommended position size
        quality_score: Overall opportunity quality
        liquidity_score: Liquidity-based score
        speed_score: Execution speed score
        confidence_score: Confidence in the opportunity
        gas_cost_percentage: Gas cost as percentage
        resolution_risk: Risk level from resolution timing
        estimated_slippage: Expected slippage
        best_execution_path: Recommended execution path
    """
    market_id: str
    topic: str
    yes_ask: float
    no_ask: float
    total_cost: float
    gross_edge: float
    liquidity_usd: float
    reason: str
    net_edge: float = 0.0
    annualized_return: float = 0.0
    min_profitable_position: float = 0.0
    safe_position_size: float = 0.0
    quality_score: float = 0.0
    liquidity_score: float = 0.0
    speed_score: float = 0.0
    confidence_score: float = 0.0
    gas_cost_percentage: float = 0.0
    resolution_risk: Literal["LOW", "MEDIUM", "HIGH", "UNKNOWN"] = "UNKNOWN"
    estimated_slippage: float = 0.0
    best_execution_path: str = "direct"


@dataclass(frozen=True)
class ExecutionIntent:
    """Intent to execute a trade.
    
    Attributes:
        market_id: Target market
        topic: Market topic
        side: Trade side ("YES" or "NO")
        token_id: Token ID to trade
        amount_usd: Amount to trade
        worst_price: Worst acceptable price
        copyability_score: Score of the signal being copied
        order_type: Order type ("FOK" or "FAK")
        reason: Explanation for the trade
        market_title: Human-readable market title
    """
    market_id: str
    topic: str
    side: Literal["YES", "NO"]
    token_id: str
    amount_usd: float
    worst_price: float
    copyability_score: float
    order_type: Literal["FOK", "FAK"]
    reason: str
    market_title: str = ""


@dataclass(frozen=True)
class ExecutionResult:
    """Result of a trade execution attempt.
    
    Attributes:
        market_id: Target market
        topic: Market topic
        side: Trade side
        token_id: Token ID traded
        amount_usd: Amount attempted
        worst_price: Worst price allowed
        status: Execution status ("SUCCESS", "FAILED", "PENDING")
        order_id: Exchange order ID if successful
        error: Error message if failed
        copyability_score: Score of the signal
        reason: Explanation of result
        market_title: Human-readable market title
    """
    market_id: str
    topic: str
    side: Literal["YES", "NO"]
    token_id: str
    amount_usd: float
    worst_price: float
    status: Literal["SUCCESS", "FAILED", "PENDING"]
    order_id: str
    error: str
    copyability_score: float
    reason: str
    market_title: str = ""


@dataclass(frozen=True)
class Position:
    """An open trading position.
    
    Attributes:
        market_id: Market identifier
        topic: Market topic
        side: Position side ("YES" or "NO")
        token_id: Token ID held
        entry_price: Price when position opened
        entry_amount_usd: Initial position size
        current_price: Current market price
        unrealized_pnl: Unrealized profit/loss
        opened_at: When position was opened
        last_updated: Last update time
        take_profit_pct: Take profit percentage
        stop_loss_pct: Stop loss percentage
        max_hold_minutes: Maximum holding time
        status: Position status ("OPEN", "CLOSED")
        market_title: Human-readable market title
    """
    market_id: str
    topic: str
    side: Literal["YES", "NO"]
    token_id: str
    entry_price: float
    entry_amount_usd: float
    current_price: float
    unrealized_pnl: float
    opened_at: datetime
    last_updated: datetime
    take_profit_pct: float
    stop_loss_pct: float
    max_hold_minutes: int
    status: Literal["OPEN", "CLOSED", "LIQUIDATED"]
    market_title: str = ""

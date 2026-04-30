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
    "WalletRegistryEntry",
    "BasketMembership",
    "BasketHealth",
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


def _normalized_contract_value(value: str) -> str:
    return str(value).strip().lower()


def _normalized_execution_status(value: str) -> str:
    normalized = _normalized_contract_value(value)
    if normalized in {"success", "matched", "filled"}:
        return "filled"
    if normalized in {"submitted", "accepted", "live"}:
        return "submitted"
    if normalized in {"pending", "open", "queued"}:
        return "pending"
    if normalized in {"failed", "failure", "error", "rejected"}:
        return "error"
    if normalized in {"simulated", "dry_run"}:
        return "dry_run"
    if normalized in {"cancelled", "canceled"}:
        return "canceled"
    return normalized


def _normalized_position_status(value: str) -> str:
    normalized = _normalized_contract_value(value)
    if normalized in {"open", "opening"}:
        return "open"
    if normalized in {"closing"}:
        return "closing"
    if normalized in {"closed"}:
        return "closed"
    if normalized in {"liquidated"}:
        return "liquidated"
    return normalized


@dataclass(frozen=True)
class WalletTrade:
    """Represents a single trade made by a wallet."""

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
    """Snapshot of market state at a point in time."""

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
    """Quality assessment for a wallet's trading activity."""

    wallet: str
    topic: str
    score: float
    eligible_trade_count: int
    average_age_seconds: float
    average_drift: float
    reason: str
    freshness_score: float = 0.0
    drift_score: float = 0.0
    sample_score: float = 0.0
    specialization_score: float = 0.0
    activity_score: float = 0.0
    liquidity_score: float = 0.0
    copy_safety_score: float = 0.0


@dataclass(frozen=True)
class WalletRegistryEntry:
    """Persistent registry metadata for a tracked wallet."""

    wallet: str
    source_type: str
    source_ref: str
    trust_seed: float
    status: str
    first_seen_at: datetime
    last_seen_trade_at: datetime | None = None
    last_scored_at: datetime | None = None
    notes: str = ""


@dataclass(frozen=True)
class BasketMembership:
    """Tracked membership of a wallet within a topic basket."""

    topic: str
    wallet: str
    tier: str
    rank: int
    active: bool
    joined_at: datetime
    effective_until: datetime | None = None
    promotion_reason: str = ""
    demotion_reason: str = ""


@dataclass(frozen=True)
class BasketHealth:
    """Snapshot of current basket health diagnostics."""

    topic: str
    tracked_wallet_count: int
    fresh_core_wallets_24h: int
    fresh_active_wallets_7d: int
    active_eligible_wallet_count: int
    eligible_trades_7d: int
    stale_ratio: float
    clustered_ratio: float
    health_state: str
    captured_at: datetime


@dataclass(frozen=True)
class BasketPromotionRecommendation:
    """Recommendation for promoting a taxonomy topic into a live basket."""

    topic: str
    should_promote: bool
    tracked_wallet_count: int
    fresh_active_wallets_7d: int
    live_eligible_wallet_count: int
    fresh_core_wallets_24h: int
    eligible_trades_7d: int
    stale_ratio: float
    recommended_quorum_ratio: float
    recommended_wallets: tuple[str, ...]
    missing_requirements: tuple[str, ...]


@dataclass(frozen=True)
class MarketRegime:
    """Classification of current market conditions."""

    label: str
    score: float
    reason: str


@dataclass(frozen=True)
class CopyCandidate:
    """A trade candidate identified for copying."""

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
    quality_consensus: float = 0.0
    confidence_score: float = 0.0
    conflict_penalty: float = 0.0
    dominant_wallet_share: float = 0.0
    recency_score: float = 0.0
    suggested_position_usd: float = 0.0
    market_regime: str = "UNKNOWN"
    regime_score: float = 0.0
    regime_reason: str = ""


@dataclass(frozen=True)
class WalletTopicProfile:
    """Profile of a wallet's trading preferences across topics."""

    topic_affinities: dict[str, float]
    primary_topic: str
    specialization_score: float


@dataclass(frozen=True)
class WalletDiscoveryCandidate:
    """A wallet discovered as a potential signal source."""

    wallet_address: str
    source: str
    total_trades: int
    recent_trades: int
    avg_trade_size_usd: float
    topic_profile: WalletTopicProfile
    score: float
    confidence: Literal["HIGH", "MEDIUM", "LOW"]
    rejected_reasons: list[str]
    history_days: int = 0
    sample_score: float = 0.0
    recency_score: float = 0.0
    history_score: float = 0.0
    activity_score: float = 0.0
    size_band_score: float = 0.0


@dataclass(frozen=True)
class BasketAssignment:
    """Assignment of a wallet to topic baskets."""

    wallet_address: str
    primary_topic: str
    recommended_baskets: list[str]
    topic_affinities: dict[str, float]
    overall_score: float
    confidence: Literal["HIGH", "MEDIUM", "LOW"]
    reasons: list[str]


@dataclass(frozen=True)
class BasketManagerAction:
    """Action recommended by the basket manager."""

    action: Literal[
        "ADD",
        "REMOVE",
        "REVIEW",
        "add",
        "remove",
        "review",
        "suspend",
        "observe",
        "rebalance",
    ]
    basket: str
    wallet_address: str
    score: float
    confidence: Literal["HIGH", "MEDIUM", "LOW"]
    reason: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "action", _normalized_contract_value(self.action))


@dataclass(frozen=True)
class ArbitrageOpportunity:
    """An identified arbitrage opportunity."""

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
    """Intent to execute a trade."""

    market_id: str
    topic: str
    side: Literal["YES", "NO", "CLOSE"]
    token_id: str
    amount_usd: float
    worst_price: float
    copyability_score: float
    order_type: Literal["FOK", "FAK"]
    reason: str
    market_title: str = ""


@dataclass(frozen=True)
class ExecutionResult:
    """Result of a trade execution attempt."""

    market_id: str
    topic: str
    side: Literal["YES", "NO", "CLOSE"]
    token_id: str
    amount_usd: float
    worst_price: float
    status: Literal["dry_run", "submitted", "filled", "pending", "error", "canceled"]
    order_id: str
    error: str
    copyability_score: float
    reason: str
    market_title: str = ""
    client_order_id: str = ""
    filled_shares: float = 0.0
    avg_fill_price: float = 0.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", _normalized_execution_status(self.status))


@dataclass(frozen=True)
class Position:
    """An open trading position."""

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
    status: Literal["open", "closing", "closed", "liquidated"]
    market_title: str = ""
    entry_shares: float = 0.0
    remaining_shares: float = 0.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", _normalized_position_status(self.status))

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class WalletTrade:
    wallet: str
    topic: str
    market_id: str
    side: str
    price: float
    size_usd: float
    age_seconds: int


@dataclass(frozen=True)
class MarketSnapshot:
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


@dataclass(frozen=True)
class WalletQuality:
    wallet: str
    topic: str
    score: float
    eligible_trade_count: int
    average_age_seconds: float
    average_drift: float
    reason: str


@dataclass(frozen=True)
class CopyCandidate:
    topic: str
    market_id: str
    side: str
    consensus_ratio: float
    reference_price: float
    current_price: float
    liquidity_usd: float
    source_wallets: list[str]
    wallet_quality_score: float
    copyability_score: float
    reason: str


@dataclass(frozen=True)
class ArbitrageOpportunity:
    market_id: str
    topic: str
    yes_ask: float
    no_ask: float
    total_cost: float
    gross_edge: float
    liquidity_usd: float
    reason: str


@dataclass(frozen=True)
class ExecutionIntent:
    market_id: str
    topic: str
    side: str
    token_id: str
    amount_usd: float
    worst_price: float
    copyability_score: float
    order_type: str
    reason: str


@dataclass(frozen=True)
class ExecutionResult:
    market_id: str
    topic: str
    side: str
    token_id: str
    amount_usd: float
    worst_price: float
    status: str
    order_id: str
    error: str
    copyability_score: float
    reason: str


@dataclass(frozen=True)
class Position:
    market_id: str
    topic: str
    side: str
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
    status: str
from __future__ import annotations

from dataclasses import dataclass


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

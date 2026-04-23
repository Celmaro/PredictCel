from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class BasketRule:
    topic: str
    wallets: list[str]
    quorum_ratio: float


@dataclass(frozen=True)
class FilterConfig:
    max_trade_age_seconds: int
    max_price_drift: float
    min_liquidity_usd: float
    min_minutes_to_resolution: int
    max_minutes_to_resolution: int
    min_position_size_usd: float


@dataclass(frozen=True)
class ArbitrageConfig:
    min_gross_edge: float
    min_liquidity_usd: float


@dataclass(frozen=True)
class AppConfig:
    baskets: list[BasketRule]
    filters: FilterConfig
    arbitrage: ArbitrageConfig
    wallet_trades_path: str
    market_snapshots_path: str


class ConfigError(ValueError):
    pass


def load_config(path: str | Path) -> AppConfig:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))

    baskets = [
        BasketRule(
            topic=item["topic"],
            wallets=item["wallets"],
            quorum_ratio=float(item["quorum_ratio"]),
        )
        for item in payload["baskets"]
    ]
    filters = FilterConfig(**payload["filters"])
    arbitrage = ArbitrageConfig(**payload["arbitrage"])

    if not baskets:
        raise ConfigError("At least one basket is required.")

    for basket in baskets:
        if not 0 < basket.quorum_ratio <= 1:
            raise ConfigError(f"Invalid quorum_ratio for topic {basket.topic}.")
        if not basket.wallets:
            raise ConfigError(f"Basket {basket.topic} has no wallets.")

    if filters.max_trade_age_seconds <= 0:
        raise ConfigError("max_trade_age_seconds must be positive.")
    if not 0 <= filters.max_price_drift <= 1:
        raise ConfigError("max_price_drift must be between 0 and 1.")
    if filters.min_minutes_to_resolution >= filters.max_minutes_to_resolution:
        raise ConfigError("Resolution window is invalid.")
    if arbitrage.min_gross_edge <= 0:
        raise ConfigError("min_gross_edge must be positive.")

    return AppConfig(
        baskets=baskets,
        filters=filters,
        arbitrage=arbitrage,
        wallet_trades_path=payload["wallet_trades_path"],
        market_snapshots_path=payload["market_snapshots_path"],
    )

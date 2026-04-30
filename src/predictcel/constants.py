"""Constants and configuration values for PredictCel.

This module contains all magic numbers and hardcoded values used
across the codebase. Centralizing them makes the code easier to
maintain and modify.
"""

import os

# Environment Variables
MAX_ENTRY_PRICE_ENV_VAR = "PREDICTCEL_MAX_ENTRY_PRICE"
MIN_ENTRY_MINUTES_TO_RESOLUTION_ENV_VAR = "PREDICTCEL_MIN_MINUTES_TO_RESOLUTION"


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def max_entry_price() -> float:
    return _env_float(MAX_ENTRY_PRICE_ENV_VAR, 0.95)


def min_entry_minutes_to_resolution() -> int:
    return _env_int(MIN_ENTRY_MINUTES_TO_RESOLUTION_ENV_VAR, 30)


# Trading Constants
MAX_ENTRY_PRICE = max_entry_price()
MIN_ENTRY_MINUTES_TO_RESOLUTION = min_entry_minutes_to_resolution()
DEFAULT_TRADE_COUNT_THRESHOLD = 25
DEFAULT_MAX_TRADE_SIZE = 500
DEFAULT_PNL_THRESHOLD = 100_000

# Scoring Weights
FOCUS_COMPONENT_WEIGHT = 0.45
ACTIVITY_COMPONENT_WEIGHT = 0.20
SIZE_COMPONENT_WEIGHT = 0.15
PNL_COMPONENT_WEIGHT = 0.15
EFFICIENCY_COMPONENT_WEIGHT = 0.05

# Scoring Thresholds
DEFAULT_CONSENSUS_THRESHOLD = 0.5
DEFAULT_MIN_COPYABILITY_SCORE = 0.6
DEFAULT_WORST_PRICE_BUFFER = 0.02

# Arbitrage Fees
DEFAULT_BUY_FEE = 0.002
DEFAULT_SELL_FEE = 0.002
MIN_ARB_PROFIT_THRESHOLD = 0.001

# API and Network
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 1.0

# Cache Settings
DEFAULT_CACHE_TTL_SECONDS = 300  # 5 minutes
DEFAULT_MAX_CACHE_SIZE = 1000

# Limits
DEFAULT_MAX_WALLETS_PER_BASKET = 50
DEFAULT_MAX_BASKETS = 20
DEFAULT_MAX_ORDERS_PER_RUN = 10
DEFAULT_MIN_SIGNAL_ALLOCATION_USD = 5.0
DEFAULT_BUY_AMOUNT_USD = 10.0

__all__ = [
    "MAX_ENTRY_PRICE_ENV_VAR",
    "MIN_ENTRY_MINUTES_TO_RESOLUTION_ENV_VAR",
    "max_entry_price",
    "min_entry_minutes_to_resolution",
    # Trading
    "MAX_ENTRY_PRICE",
    "MIN_ENTRY_MINUTES_TO_RESOLUTION",
    "DEFAULT_TRADE_COUNT_THRESHOLD",
    "DEFAULT_MAX_TRADE_SIZE",
    "DEFAULT_PNL_THRESHOLD",
    # Scoring Weights
    "FOCUS_COMPONENT_WEIGHT",
    "ACTIVITY_COMPONENT_WEIGHT",
    "SIZE_COMPONENT_WEIGHT",
    "PNL_COMPONENT_WEIGHT",
    "EFFICIENCY_COMPONENT_WEIGHT",
    # Scoring Thresholds
    "DEFAULT_CONSENSUS_THRESHOLD",
    "DEFAULT_MIN_COPYABILITY_SCORE",
    "DEFAULT_WORST_PRICE_BUFFER",
    # Arbitrage
    "DEFAULT_BUY_FEE",
    "DEFAULT_SELL_FEE",
    "MIN_ARB_PROFIT_THRESHOLD",
    # API
    "DEFAULT_TIMEOUT_SECONDS",
    "DEFAULT_MAX_RETRIES",
    "DEFAULT_RETRY_DELAY",
    # Cache
    "DEFAULT_CACHE_TTL_SECONDS",
    "DEFAULT_MAX_CACHE_SIZE",
    # Limits
    "DEFAULT_MAX_WALLETS_PER_BASKET",
    "DEFAULT_MAX_BASKETS",
    "DEFAULT_MAX_ORDERS_PER_RUN",
    "DEFAULT_MIN_SIGNAL_ALLOCATION_USD",
    "DEFAULT_BUY_AMOUNT_USD",
]

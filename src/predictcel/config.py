"""
Configuration loading and validation for PredictCel.

This module handles loading configuration from JSON files and
validating all parameters to ensure they meet requirements.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


def _default_topic_keywords() -> dict[str, list[str]]:
    """Default topic keywords for wallet discovery."""
    return {
        "geopolitics": [
            "election",
            "trump",
            "biden",
            "war",
            "federal",
            "senate",
            "president",
        ],
        "sports": ["nba", "nfl", "mlb", "nhl", "ufc", "soccer", "football", "tennis"],
        "crypto": ["btc", "eth", "sol", "bitcoin", "ethereum", "crypto"],
        "macro": ["economy", "gdp", "inflation", "rates", "stock", "fed", "recession"],
        "weather": ["weather", "rain", "snow", "hurricane", "temperature", "storm"],
    }


@dataclass(frozen=True)
class BasketRule:
    topic: str
    wallets: list[str]
    quorum_ratio: float
    target_allocation: float = 0.0


@dataclass(frozen=True)
class FilterConfig:
    max_trade_age_seconds: int
    max_price_drift: float
    min_liquidity_usd: float
    min_minutes_to_resolution: int
    max_minutes_to_resolution: int
    min_position_size_usd: float


@dataclass(frozen=True)
class ConsensusConfig:
    recency_half_life_seconds: int = 1800
    min_weighted_consensus: float = 0.60
    confidence_prior_strength: float = 2.0
    min_confidence_score: float = 0.50
    conflict_penalty_weight: float = 0.25
    bankroll_usd: float = 100.0
    kelly_fraction: float = 0.25
    max_suggested_position_usd: float = 10.0


@dataclass(frozen=True)
class MarketRegimeConfig:
    enabled: bool = True
    trend_price_skew: float = 0.15
    range_price_skew: float = 0.08
    max_stable_spread: float = 0.06
    min_depth_usd: float = 50.0
    trend_bonus: float = 0.05
    range_bonus: float = 0.02
    unstable_penalty: float = 0.10


@dataclass(frozen=True)
class WalletDiscoveryConfig:
    enabled: bool = False
    mode: str = "auto_update"
    source: str = "data_api_leaderboard"
    wallet_candidates_path: str = "data/wallet_candidates.json"
    candidate_limit: int = 250
    trade_limit_per_wallet: int = 100
    min_trades: int = 20
    min_recent_trades: int = 5
    min_history_days: int = 1
    recent_window_seconds: int = 2_592_000
    min_avg_trade_size_usd: float = 10.0
    min_assignment_score: float = 0.50
    exclude_existing_wallets: bool = True
    max_wallets_per_basket: int = 50
    max_new_wallets_per_run: int = 10
    topics: dict[str, list[str]] = field(default_factory=_default_topic_keywords)


@dataclass(frozen=True)
class WalletRegistryConfig:
    enabled: bool = False
    seed_from_baskets: bool = True
    min_probation_days: int = 7
    min_eligible_trades_for_approval: int = 5
    stale_after_hours: int = 72
    suspend_after_hours: int = 168
    retire_after_days: int = 30
    max_cluster_overlap_ratio: float = 0.8
    max_cluster_members_in_live_tiers: int = 2


@dataclass(frozen=True)
class BasketPromotionConfig:
    enabled: bool = True
    min_tracked_wallets: int = 5
    min_fresh_active_wallets_7d: int = 3
    min_live_eligible_wallets: int = 5
    min_fresh_core_wallets_24h: int = 2
    min_eligible_trades_7d: int = 10
    max_stale_ratio: float = 0.5


@dataclass(frozen=True)
class BasketControllerConfig:
    enabled: bool = False
    tracked_basket_target: int = 15
    core_slots: int = 5
    rotating_slots: int = 6
    backup_slots: int = 2
    explorer_slots: int = 2
    rotation_interval_hours: int = 24
    force_refresh_if_fresh_core_below: int = 3
    allow_backup_in_live_consensus: bool = False
    min_basket_participation_ratio: float = 0.8
    min_weighted_participation_ratio: float = 0.75
    min_active_eligible_wallets: int = 5
    min_aligned_wallet_count: int = 4
    max_entry_price_band_abs: float = 0.03
    max_entry_time_spread_seconds: int = 10800


@dataclass(frozen=True)
class ArbitrageConfig:
    min_gross_edge: float
    min_liquidity_usd: float
    variable_cost_rate: float = 0.0
    gas_cost_per_tx_usd: float = 0.02
    settlement_tx_count: int = 2
    slippage_rate: float = 0.001
    min_profitable_position_usd: float = 5.0
    max_position_usd: float = 50.0
    target_annualized_return: float = 0.10
    max_annualized_return: float = 10.0
    edge_weight: float = 0.35
    liquidity_weight: float = 0.25
    speed_weight: float = 0.20
    confidence_weight: float = 0.20


@dataclass(frozen=True)
class LiveDataConfig:
    enabled: bool
    gamma_base_url: str
    data_base_url: str
    clob_base_url: str
    market_limit: int
    trade_limit: int
    request_timeout_seconds: int


@dataclass(frozen=True)
class PositionConfig:
    take_profit_pct: float
    stop_loss_pct: float
    max_hold_minutes: int


@dataclass(frozen=True)
class ExposureConfig:
    max_total_exposure_usd: float
    max_single_position_usd: float


@dataclass(frozen=True)
class ExecutionConfig:
    enabled: bool
    dry_run: bool
    min_copyability_score: float
    max_orders_per_run: int
    buy_amount_usd: float
    worst_price_buffer: float
    order_type: str
    chain_id: int
    signature_type: int
    position: PositionConfig
    exposure: ExposureConfig | None
    max_retries: int
    retry_base_delay_seconds: float
    min_signal_allocation_usd: float = 5.0


@dataclass(frozen=True)
class AppConfig:
    baskets: list[BasketRule]
    filters: FilterConfig
    arbitrage: ArbitrageConfig
    wallet_trades_path: str
    market_snapshots_path: str
    live_data: LiveDataConfig | None
    execution: ExecutionConfig | None
    consensus: ConsensusConfig = ConsensusConfig()
    market_regime: MarketRegimeConfig = MarketRegimeConfig()
    wallet_discovery: WalletDiscoveryConfig = WalletDiscoveryConfig()
    wallet_registry: WalletRegistryConfig = WalletRegistryConfig()
    basket_promotion: BasketPromotionConfig = BasketPromotionConfig()
    basket_controller: BasketControllerConfig = BasketControllerConfig()


class ConfigError(ValueError):
    """Raised when configuration validation fails."""


def _validate_range(
    value: float,
    min_val: float,
    max_val: float,
    name: str,
) -> None:
    """Validate that a value is within a range."""
    if not min_val <= value <= max_val:
        raise ConfigError(f"{name} must be between {min_val} and {max_val}.")


def _validate_positive(
    value: float,
    name: str,
    allow_zero: bool = False,
) -> None:
    """Validate that a value is positive (or non-negative)."""
    if allow_zero:
        if value < 0:
            raise ConfigError(f"{name} must be non-negative.")
    else:
        if value <= 0:
            raise ConfigError(f"{name} must be positive.")


def _validate_baskets(baskets: list[BasketRule]) -> None:
    """Validate basket configuration."""
    if not baskets:
        raise ConfigError("At least one basket is required.")

    for basket in baskets:
        if not 0 < basket.quorum_ratio <= 1:
            raise ConfigError(f"Invalid quorum_ratio for topic {basket.topic}.")
        if not basket.wallets:
            raise ConfigError(f"Basket {basket.topic} has no wallets.")


def _validate_filters(filters: FilterConfig) -> None:
    """Validate filter configuration."""
    _validate_positive(filters.max_trade_age_seconds, "max_trade_age_seconds")
    _validate_range(filters.max_price_drift, 0, 1, "max_price_drift")

    if filters.min_minutes_to_resolution >= filters.max_minutes_to_resolution:
        raise ConfigError("Resolution window is invalid.")


def _validate_consensus(config: ConsensusConfig) -> None:
    """Validate consensus configuration."""
    _validate_positive(config.recency_half_life_seconds, "recency_half_life_seconds")
    _validate_range(config.min_weighted_consensus, 0, 1, "min_weighted_consensus")
    _validate_positive(
        config.confidence_prior_strength, "confidence_prior_strength", True
    )
    _validate_range(config.min_confidence_score, 0, 1, "min_confidence_score")
    _validate_range(config.conflict_penalty_weight, 0, 1, "conflict_penalty_weight")
    _validate_positive(config.bankroll_usd, "bankroll_usd")
    _validate_range(config.kelly_fraction, 0, 1, "kelly_fraction")
    _validate_positive(config.max_suggested_position_usd, "max_suggested_position_usd")


def _validate_market_regime(config: MarketRegimeConfig) -> None:
    """Validate market regime configuration."""
    _validate_range(config.trend_price_skew, 0, 0.5, "trend_price_skew")
    _validate_range(config.range_price_skew, 0, 0.5, "range_price_skew")

    if config.range_price_skew > config.trend_price_skew:
        raise ConfigError("range_price_skew cannot exceed trend_price_skew.")
    if config.max_stable_spread < 0 or config.min_depth_usd < 0:
        raise ConfigError("Spread and depth thresholds must be non-negative.")
    if config.unstable_penalty < 0:
        raise ConfigError("unstable_penalty must be non-negative.")


def _validate_wallet_discovery(config: WalletDiscoveryConfig) -> None:
    """Validate wallet discovery configuration."""
    valid_modes = {"report_only", "propose_config", "auto_update"}
    if config.mode not in valid_modes:
        raise ConfigError(f"mode must be one of: {', '.join(valid_modes)}.")
    valid_sources = {
        "data_api_leaderboard",
        "data_api_market_trades",
        "curated_wallet_file",
    }
    if config.source not in valid_sources:
        raise ConfigError(
            f"wallet discovery source must be one of: {', '.join(sorted(valid_sources))}."
        )
    if (
        config.source == "curated_wallet_file"
        and not str(config.wallet_candidates_path).strip()
    ):
        raise ConfigError(
            "wallet_candidates_path is required when wallet discovery source is curated_wallet_file."
        )

    _validate_positive(config.candidate_limit, "candidate_limit")
    _validate_positive(config.trade_limit_per_wallet, "trade_limit_per_wallet")
    _validate_positive(config.min_trades, "min_trades", True)
    _validate_positive(config.min_recent_trades, "min_recent_trades", True)
    _validate_positive(config.min_history_days, "min_history_days", True)
    _validate_positive(config.recent_window_seconds, "recent_window_seconds")
    _validate_positive(config.min_avg_trade_size_usd, "min_avg_trade_size_usd", True)
    _validate_range(config.min_assignment_score, 0, 1, "min_assignment_score")
    _validate_positive(config.max_wallets_per_basket, "max_wallets_per_basket")
    _validate_positive(config.max_new_wallets_per_run, "max_new_wallets_per_run")


def _validate_wallet_registry(config: WalletRegistryConfig) -> None:
    """Validate wallet registry configuration."""
    _validate_positive(config.min_probation_days, "min_probation_days")
    _validate_positive(
        config.min_eligible_trades_for_approval,
        "min_eligible_trades_for_approval",
    )
    _validate_positive(config.stale_after_hours, "stale_after_hours")
    _validate_positive(config.suspend_after_hours, "suspend_after_hours")
    _validate_positive(config.retire_after_days, "retire_after_days")
    _validate_range(config.max_cluster_overlap_ratio, 0, 1, "max_cluster_overlap_ratio")
    _validate_positive(
        config.max_cluster_members_in_live_tiers,
        "max_cluster_members_in_live_tiers",
    )

    if config.suspend_after_hours < config.stale_after_hours:
        raise ConfigError("suspend_after_hours cannot be less than stale_after_hours.")


def _validate_basket_controller(config: BasketControllerConfig) -> None:
    """Validate basket controller configuration."""
    _validate_positive(config.tracked_basket_target, "tracked_basket_target")
    _validate_positive(config.core_slots, "core_slots", True)
    _validate_positive(config.rotating_slots, "rotating_slots", True)
    _validate_positive(config.backup_slots, "backup_slots", True)
    _validate_positive(config.explorer_slots, "explorer_slots", True)
    _validate_positive(config.rotation_interval_hours, "rotation_interval_hours")
    _validate_positive(
        config.force_refresh_if_fresh_core_below,
        "force_refresh_if_fresh_core_below",
        True,
    )
    _validate_range(
        config.min_basket_participation_ratio,
        0,
        1,
        "min_basket_participation_ratio",
    )
    _validate_range(
        config.min_weighted_participation_ratio,
        0,
        1,
        "min_weighted_participation_ratio",
    )
    _validate_positive(
        config.min_active_eligible_wallets, "min_active_eligible_wallets"
    )
    _validate_positive(config.min_aligned_wallet_count, "min_aligned_wallet_count")
    _validate_range(config.max_entry_price_band_abs, 0, 1, "max_entry_price_band_abs")
    _validate_positive(
        config.max_entry_time_spread_seconds,
        "max_entry_time_spread_seconds",
    )

    slot_total = (
        config.core_slots
        + config.rotating_slots
        + config.backup_slots
        + config.explorer_slots
    )
    if slot_total != config.tracked_basket_target:
        raise ConfigError(
            "tracked_basket_target must equal core_slots + rotating_slots + backup_slots + explorer_slots."
        )
    if config.force_refresh_if_fresh_core_below > config.core_slots:
        raise ConfigError("force_refresh_if_fresh_core_below cannot exceed core_slots.")
    if config.min_aligned_wallet_count > config.min_active_eligible_wallets:
        raise ConfigError(
            "min_aligned_wallet_count cannot exceed min_active_eligible_wallets."
        )
    if config.min_active_eligible_wallets > config.tracked_basket_target:
        raise ConfigError(
            "min_active_eligible_wallets cannot exceed tracked_basket_target."
        )


def _validate_basket_promotion(config: BasketPromotionConfig) -> None:
    """Validate basket promotion configuration."""
    _validate_positive(config.min_tracked_wallets, "min_tracked_wallets")
    _validate_positive(
        config.min_fresh_active_wallets_7d, "min_fresh_active_wallets_7d"
    )
    _validate_positive(config.min_live_eligible_wallets, "min_live_eligible_wallets")
    _validate_positive(config.min_fresh_core_wallets_24h, "min_fresh_core_wallets_24h")
    _validate_positive(config.min_eligible_trades_7d, "min_eligible_trades_7d")
    _validate_range(config.max_stale_ratio, 0, 1, "max_stale_ratio")

    if config.min_fresh_active_wallets_7d > config.min_tracked_wallets:
        raise ConfigError(
            "min_fresh_active_wallets_7d cannot exceed min_tracked_wallets."
        )
    if config.min_live_eligible_wallets > config.min_tracked_wallets:
        raise ConfigError(
            "min_live_eligible_wallets cannot exceed min_tracked_wallets."
        )
    if config.min_fresh_core_wallets_24h > config.min_live_eligible_wallets:
        raise ConfigError(
            "min_fresh_core_wallets_24h cannot exceed min_live_eligible_wallets."
        )


def _validate_registry_controller_compatibility(
    wallet_registry: WalletRegistryConfig,
    basket_controller: BasketControllerConfig,
) -> None:
    """Validate relationships between registry and controller configuration."""
    live_tier_capacity = basket_controller.core_slots + basket_controller.rotating_slots
    if basket_controller.allow_backup_in_live_consensus:
        live_tier_capacity += basket_controller.backup_slots
    if wallet_registry.max_cluster_members_in_live_tiers > live_tier_capacity:
        raise ConfigError(
            "max_cluster_members_in_live_tiers cannot exceed live basket capacity."
        )


def _validate_arbitrage(config: ArbitrageConfig) -> None:
    """Validate arbitrage configuration."""
    _validate_positive(config.min_gross_edge, "min_gross_edge")
    _validate_positive(config.min_liquidity_usd, "min_liquidity_usd", True)
    _validate_range(config.variable_cost_rate, 0, 1, "variable_cost_rate")
    _validate_positive(config.gas_cost_per_tx_usd, "gas_cost_per_tx_usd", True)
    _validate_positive(config.settlement_tx_count, "settlement_tx_count", True)
    _validate_range(config.slippage_rate, 0, 1, "slippage_rate")
    _validate_positive(
        config.min_profitable_position_usd, "min_profitable_position_usd", True
    )
    _validate_positive(config.max_position_usd, "max_position_usd")
    _validate_positive(
        config.target_annualized_return, "target_annualized_return", True
    )
    _validate_positive(config.max_annualized_return, "max_annualized_return")


def _validate_live_data(config: LiveDataConfig | None) -> None:
    """Validate live data configuration."""
    if config is None:
        return

    _validate_positive(config.market_limit, "market_limit")
    _validate_positive(config.trade_limit, "trade_limit")
    _validate_positive(config.request_timeout_seconds, "request_timeout_seconds")


def _validate_execution(config: ExecutionConfig | None) -> None:
    """Validate execution configuration."""
    if config is None:
        return

    _validate_range(config.min_copyability_score, 0, 1, "min_copyability_score")
    _validate_positive(config.max_orders_per_run, "max_orders_per_run")
    _validate_positive(config.buy_amount_usd, "buy_amount_usd")
    _validate_positive(config.min_signal_allocation_usd, "min_signal_allocation_usd")

    if config.min_signal_allocation_usd > config.buy_amount_usd:
        raise ConfigError("min_signal_allocation_usd cannot exceed buy_amount_usd.")

    _validate_range(config.worst_price_buffer, 0, 1, "worst_price_buffer")

    if config.order_type.upper() not in {"FOK", "FAK"}:
        raise ConfigError("order_type must be FOK or FAK.")
    if config.signature_type not in {0, 1, 2}:
        raise ConfigError("signature_type must be 0, 1, or 2.")

    _validate_positive(config.position.take_profit_pct, "take_profit_pct", True)
    _validate_positive(config.position.stop_loss_pct, "stop_loss_pct", True)
    _validate_positive(config.position.max_hold_minutes, "max_hold_minutes", True)
    _validate_positive(config.max_retries, "max_retries", True)
    _validate_positive(config.retry_base_delay_seconds, "retry_base_delay_seconds")

    if config.exposure is not None:
        _validate_positive(
            config.exposure.max_total_exposure_usd,
            "max_total_exposure_usd",
            True,
        )
        _validate_positive(
            config.exposure.max_single_position_usd,
            "max_single_position_usd",
            True,
        )


def _build_execution_config(payload: dict[str, Any]) -> ExecutionConfig:
    """Build ExecutionConfig from payload dictionary."""
    position_payload = payload.get("position", {})
    position_config = PositionConfig(
        take_profit_pct=float(position_payload.get("take_profit_pct", 0.0)),
        stop_loss_pct=float(position_payload.get("stop_loss_pct", 0.0)),
        max_hold_minutes=int(position_payload.get("max_hold_minutes", 0)),
    )

    exposure_payload = payload.get("exposure")
    exposure_config = (
        ExposureConfig(
            max_total_exposure_usd=float(
                exposure_payload.get("max_total_exposure_usd", 0.0)
            ),
            max_single_position_usd=float(
                exposure_payload.get("max_single_position_usd", 0.0)
            ),
        )
        if exposure_payload
        else None
    )

    return ExecutionConfig(
        enabled=payload["enabled"],
        dry_run=payload["dry_run"],
        min_copyability_score=float(payload["min_copyability_score"]),
        max_orders_per_run=int(payload["max_orders_per_run"]),
        buy_amount_usd=float(payload["buy_amount_usd"]),
        worst_price_buffer=float(payload["worst_price_buffer"]),
        order_type=payload["order_type"],
        chain_id=int(payload["chain_id"]),
        signature_type=int(payload["signature_type"]),
        position=position_config,
        exposure=exposure_config,
        max_retries=int(payload.get("max_retries", 3)),
        retry_base_delay_seconds=float(payload.get("retry_base_delay_seconds", 1.0)),
        min_signal_allocation_usd=float(
            payload.get(
                "min_signal_allocation_usd", min(float(payload["buy_amount_usd"]), 5.0)
            )
        ),
    )


def load_config(path: str | Path) -> AppConfig:
    """Load and validate configuration from a JSON file.

    Args:
        path: Path to the JSON configuration file

    Returns:
        Validated AppConfig instance

    Raises:
        ConfigError: If configuration is invalid
        FileNotFoundError: If the configuration file doesn't exist
        json.JSONDecodeError: If the file contains invalid JSON
    """
    payload = json.loads(Path(path).read_text(encoding="utf-8"))

    baskets = [
        BasketRule(
            topic=item["topic"],
            wallets=item["wallets"],
            quorum_ratio=float(item["quorum_ratio"]),
            target_allocation=float(item.get("target_allocation", 0.0)),
        )
        for item in payload["baskets"]
    ]

    filters = FilterConfig(**payload["filters"])
    arbitrage = ArbitrageConfig(**payload["arbitrage"])
    consensus = ConsensusConfig(**payload.get("consensus", {}))
    market_regime = MarketRegimeConfig(**payload.get("market_regime", {}))
    wallet_discovery = WalletDiscoveryConfig(**payload.get("wallet_discovery", {}))
    wallet_registry = WalletRegistryConfig(**payload.get("wallet_registry", {}))
    basket_promotion = BasketPromotionConfig(**payload.get("basket_promotion", {}))
    basket_controller = BasketControllerConfig(**payload.get("basket_controller", {}))

    live_data_payload = payload.get("live_data")
    live_data = LiveDataConfig(**live_data_payload) if live_data_payload else None

    execution_payload = payload.get("execution")
    execution = (
        _build_execution_config(execution_payload) if execution_payload else None
    )

    _validate_baskets(baskets)
    _validate_filters(filters)
    _validate_consensus(consensus)
    _validate_market_regime(market_regime)
    _validate_wallet_discovery(wallet_discovery)
    _validate_wallet_registry(wallet_registry)
    _validate_basket_promotion(basket_promotion)
    _validate_basket_controller(basket_controller)
    _validate_registry_controller_compatibility(wallet_registry, basket_controller)
    _validate_arbitrage(arbitrage)
    _validate_live_data(live_data)
    _validate_execution(execution)

    return AppConfig(
        baskets=baskets,
        filters=filters,
        arbitrage=arbitrage,
        wallet_trades_path=payload["wallet_trades_path"],
        market_snapshots_path=payload["market_snapshots_path"],
        live_data=live_data,
        execution=execution,
        consensus=consensus,
        market_regime=market_regime,
        wallet_discovery=wallet_discovery,
        wallet_registry=wallet_registry,
        basket_promotion=basket_promotion,
        basket_controller=basket_controller,
    )

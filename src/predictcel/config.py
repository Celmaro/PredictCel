from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path


def _default_topic_keywords() -> dict[str, list[str]]:
    return {
        "geopolitics": ["election", "trump", "biden", "war", "federal", "senate", "president"],
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
    candidate_limit: int = 100
    trade_limit_per_wallet: int = 100
    min_trades: int = 20
    min_recent_trades: int = 5
    recent_window_seconds: int = 2_592_000
    min_avg_trade_size_usd: float = 10.0
    min_assignment_score: float = 0.50
    exclude_existing_wallets: bool = True
    max_wallets_per_basket: int = 20
    max_new_wallets_per_run: int = 3
    topics: dict[str, list[str]] = field(default_factory=_default_topic_keywords)


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
    consensus = ConsensusConfig(**payload.get("consensus", {}))
    market_regime = MarketRegimeConfig(**payload.get("market_regime", {}))
    wallet_discovery = WalletDiscoveryConfig(**payload.get("wallet_discovery", {}))
    live_data_payload = payload.get("live_data")
    live_data = LiveDataConfig(**live_data_payload) if live_data_payload else None
    execution_payload = payload.get("execution")
    execution = _build_execution_config(execution_payload) if execution_payload else None

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
    if consensus.recency_half_life_seconds <= 0:
        raise ConfigError("consensus recency_half_life_seconds must be positive.")
    if not 0 <= consensus.min_weighted_consensus <= 1:
        raise ConfigError("consensus min_weighted_consensus must be between 0 and 1.")
    if consensus.confidence_prior_strength < 0:
        raise ConfigError("consensus confidence_prior_strength must be non-negative.")
    if not 0 <= consensus.min_confidence_score <= 1:
        raise ConfigError("consensus min_confidence_score must be between 0 and 1.")
    if not 0 <= consensus.conflict_penalty_weight <= 1:
        raise ConfigError("consensus conflict_penalty_weight must be between 0 and 1.")
    if consensus.bankroll_usd <= 0:
        raise ConfigError("consensus bankroll_usd must be positive.")
    if not 0 <= consensus.kelly_fraction <= 1:
        raise ConfigError("consensus kelly_fraction must be between 0 and 1.")
    if consensus.max_suggested_position_usd <= 0:
        raise ConfigError("consensus max_suggested_position_usd must be positive.")
    if not 0 <= market_regime.trend_price_skew <= 0.5:
        raise ConfigError("market_regime trend_price_skew must be between 0 and 0.5.")
    if not 0 <= market_regime.range_price_skew <= 0.5:
        raise ConfigError("market_regime range_price_skew must be between 0 and 0.5.")
    if market_regime.range_price_skew > market_regime.trend_price_skew:
        raise ConfigError("market_regime range_price_skew cannot exceed trend_price_skew.")
    if market_regime.max_stable_spread < 0 or market_regime.min_depth_usd < 0:
        raise ConfigError("market_regime spread and depth thresholds must be non-negative.")
    if market_regime.unstable_penalty < 0:
        raise ConfigError("market_regime unstable_penalty must be non-negative.")
    if wallet_discovery.mode not in {"report_only", "propose_config", "auto_update"}:
        raise ConfigError("wallet_discovery mode must be report_only, propose_config, or auto_update.")
    if wallet_discovery.candidate_limit <= 0 or wallet_discovery.trade_limit_per_wallet <= 0:
        raise ConfigError("wallet_discovery candidate and trade limits must be positive.")
    if wallet_discovery.min_trades < 0 or wallet_discovery.min_recent_trades < 0:
        raise ConfigError("wallet_discovery trade filters must be non-negative.")
    if wallet_discovery.recent_window_seconds <= 0:
        raise ConfigError("wallet_discovery recent_window_seconds must be positive.")
    if wallet_discovery.min_avg_trade_size_usd < 0:
        raise ConfigError("wallet_discovery min_avg_trade_size_usd must be non-negative.")
    if not 0 <= wallet_discovery.min_assignment_score <= 1:
        raise ConfigError("wallet_discovery min_assignment_score must be between 0 and 1.")
    if wallet_discovery.max_wallets_per_basket <= 0 or wallet_discovery.max_new_wallets_per_run <= 0:
        raise ConfigError("wallet_discovery basket limits must be positive.")
    if arbitrage.min_gross_edge <= 0:
        raise ConfigError("min_gross_edge must be positive.")
    if arbitrage.min_liquidity_usd < 0:
        raise ConfigError("min_liquidity_usd must be non-negative.")
    if not 0 <= arbitrage.variable_cost_rate <= 1:
        raise ConfigError("arbitrage variable_cost_rate must be between 0 and 1.")
    if arbitrage.gas_cost_per_tx_usd < 0:
        raise ConfigError("arbitrage gas_cost_per_tx_usd must be non-negative.")
    if arbitrage.settlement_tx_count < 0:
        raise ConfigError("arbitrage settlement_tx_count must be non-negative.")
    if not 0 <= arbitrage.slippage_rate <= 1:
        raise ConfigError("arbitrage slippage_rate must be between 0 and 1.")
    if arbitrage.min_profitable_position_usd < 0:
        raise ConfigError("arbitrage min_profitable_position_usd must be non-negative.")
    if arbitrage.max_position_usd <= 0:
        raise ConfigError("arbitrage max_position_usd must be positive.")
    if arbitrage.target_annualized_return < 0:
        raise ConfigError("arbitrage target_annualized_return must be non-negative.")
    if arbitrage.max_annualized_return <= 0:
        raise ConfigError("arbitrage max_annualized_return must be positive.")

    if live_data is not None:
        if live_data.market_limit <= 0 or live_data.trade_limit <= 0:
            raise ConfigError("Live market and trade limits must be positive.")
        if live_data.request_timeout_seconds <= 0:
            raise ConfigError("request_timeout_seconds must be positive.")

    if execution is not None:
        if not 0 <= execution.min_copyability_score <= 1:
            raise ConfigError("min_copyability_score must be between 0 and 1.")
        if execution.max_orders_per_run <= 0:
            raise ConfigError("max_orders_per_run must be positive.")
        if execution.buy_amount_usd <= 0:
            raise ConfigError("buy_amount_usd must be positive.")
        if not 0 <= execution.worst_price_buffer <= 1:
            raise ConfigError("worst_price_buffer must be between 0 and 1.")
        if execution.order_type.upper() not in {"FOK", "FAK"}:
            raise ConfigError("execution order_type must be FOK or FAK.")
        if execution.signature_type not in {0, 1, 2}:
            raise ConfigError("signature_type must be 0, 1, or 2.")
        pc = execution.position
        if pc.take_profit_pct < 0:
            raise ConfigError("take_profit_pct must be non-negative.")
        if pc.stop_loss_pct < 0:
            raise ConfigError("stop_loss_pct must be non-negative.")
        if pc.max_hold_minutes < 0:
            raise ConfigError("max_hold_minutes must be non-negative.")
        if execution.max_retries < 0:
            raise ConfigError("max_retries must be non-negative.")
        if execution.retry_base_delay_seconds <= 0:
            raise ConfigError("retry_base_delay_seconds must be positive.")
        if execution.exposure is not None:
            if execution.exposure.max_total_exposure_usd < 0:
                raise ConfigError("max_total_exposure_usd must be non-negative.")
            if execution.exposure.max_single_position_usd < 0:
                raise ConfigError("max_single_position_usd must be non-negative.")

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
    )


def _build_execution_config(payload: dict) -> ExecutionConfig:
    position_payload = payload.get("position", {})
    position_config = PositionConfig(
        take_profit_pct=float(position_payload.get("take_profit_pct", 0.0)),
        stop_loss_pct=float(position_payload.get("stop_loss_pct", 0.0)),
        max_hold_minutes=int(position_payload.get("max_hold_minutes", 0)),
    )
    exposure_payload = payload.get("exposure")
    exposure_config = ExposureConfig(
        max_total_exposure_usd=float(exposure_payload.get("max_total_exposure_usd", 0.0)) if exposure_payload else 0.0,
        max_single_position_usd=float(exposure_payload.get("max_single_position_usd", 0.0)) if exposure_payload else 0.0,
    ) if exposure_payload else None
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
    )

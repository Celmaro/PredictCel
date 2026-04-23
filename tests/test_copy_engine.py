from predictcel.config import (
    ArbitrageConfig,
    AppConfig,
    BasketRule,
    ExecutionConfig,
    ExposureConfig,
    FilterConfig,
    LiveDataConfig,
    PositionConfig,
)
from predictcel.copy_engine import CopyEngine
from predictcel.models import MarketSnapshot, WalletQuality, WalletTrade


def make_config() -> AppConfig:
    return AppConfig(
        baskets=[BasketRule(topic="geopolitics", wallets=["w1", "w2", "w3"], quorum_ratio=0.66)],
        filters=FilterConfig(
            max_trade_age_seconds=3600,
            max_price_drift=0.05,
            min_liquidity_usd=5000,
            min_minutes_to_resolution=60,
            max_minutes_to_resolution=1440,
            min_position_size_usd=100,
        ),
        arbitrage=ArbitrageConfig(min_gross_edge=0.02, min_liquidity_usd=5000),
        wallet_trades_path="",
        market_snapshots_path="",
        live_data=LiveDataConfig(
            enabled=False,
            gamma_base_url="https://gamma-api.polymarket.com",
            data_base_url="https://data-api.polymarket.com",
            clob_base_url="https://clob.polymarket.com",
            market_limit=100,
            trade_limit=10,
            request_timeout_seconds=15,
        ),
        execution=ExecutionConfig(
            enabled=False,
            dry_run=True,
            min_copyability_score=0.7,
            max_orders_per_run=2,
            buy_amount_usd=25.0,
            worst_price_buffer=0.02,
            order_type="FOK",
            chain_id=137,
            signature_type=0,
            position=PositionConfig(take_profit_pct=0.3, stop_loss_pct=0.1, max_hold_minutes=1440),
            exposure=ExposureConfig(max_total_exposure_usd=500.0, max_single_position_usd=50.0),
            max_retries=3,
            retry_base_delay_seconds=1.0,
        ),
    )


def test_emits_candidate_when_quorum_and_drift_pass() -> None:
    engine = CopyEngine(make_config())
    trades = [
        WalletTrade(wallet="w1", topic="geopolitics", market_id="m1", side="YES", price=0.58, size_usd=200, age_seconds=600),
        WalletTrade(wallet="w2", topic="geopolitics", market_id="m1", side="YES", price=0.6, size_usd=220, age_seconds=800),
    ]
    markets = {
        "m1": MarketSnapshot(
            market_id="m1",
            topic="unknown",
            title="Example",
            yes_ask=0.61,
            no_ask=0.42,
            best_bid=0.59,
            liquidity_usd=10000,
            minutes_to_resolution=180,
            yes_ask_size=250,
            no_ask_size=220,
            yes_spread=0.02,
            no_spread=0.03,
            orderbook_ready=True,
        )
    }
    wallet_qualities = {
        "w1": WalletQuality(wallet="w1", topic="geopolitics", score=0.8, eligible_trade_count=3, average_age_seconds=500, average_drift=0.01, reason="test"),
        "w2": WalletQuality(wallet="w2", topic="geopolitics", score=0.7, eligible_trade_count=2, average_age_seconds=700, average_drift=0.02, reason="test"),
    }

    candidates = engine.evaluate(trades, markets, wallet_qualities)

    assert len(candidates) == 1
    assert candidates[0].side == "YES"
    assert candidates[0].market_id == "m1"
    assert candidates[0].topic == "geopolitics"
    assert candidates[0].wallet_quality_score == 0.75
    assert candidates[0].copyability_score > 0


def test_skips_candidate_when_drift_is_too_large() -> None:
    engine = CopyEngine(make_config())
    trades = [
        WalletTrade(wallet="w1", topic="geopolitics", market_id="m1", side="YES", price=0.4, size_usd=200, age_seconds=600),
        WalletTrade(wallet="w2", topic="geopolitics", market_id="m1", side="YES", price=0.41, size_usd=220, age_seconds=800),
    ]
    markets = {
        "m1": MarketSnapshot(
            market_id="m1",
            topic="unknown",
            title="Example",
            yes_ask=0.61,
            no_ask=0.42,
            best_bid=0.59,
            liquidity_usd=10000,
            minutes_to_resolution=180,
            yes_ask_size=250,
            no_ask_size=220,
            yes_spread=0.02,
            no_spread=0.03,
            orderbook_ready=True,
        )
    }

    candidates = engine.evaluate(trades, markets, {})

    assert candidates == []

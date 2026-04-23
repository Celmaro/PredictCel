from predictcel.config import ArbitrageConfig, AppConfig, BasketRule, FilterConfig, LiveDataConfig
from predictcel.copy_engine import CopyEngine
from predictcel.models import MarketSnapshot, WalletTrade


def make_config() -> AppConfig:
    return AppConfig(
        baskets=[BasketRule(topic="geopolitics", wallets=["w1", "w2", "w3"], quorum_ratio=0.67)],
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
            market_limit=100,
            trade_limit=10,
            request_timeout_seconds=15,
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
        )
    }

    candidates = engine.evaluate(trades, markets)

    assert len(candidates) == 1
    assert candidates[0].side == "YES"
    assert candidates[0].market_id == "m1"
    assert candidates[0].topic == "geopolitics"


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
        )
    }

    candidates = engine.evaluate(trades, markets)

    assert candidates == []

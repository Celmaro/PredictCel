import pytest
from src.predictcel.copy_engine import CopyEngine
from src.predictcel.config import AppConfig, BasketRule
from src.predictcel.models import MarketSnapshot, WalletTrade, WalletQuality
from datetime import datetime, UTC


@pytest.fixture
def sample_config():
    return AppConfig(
        baskets=[BasketRule(topic="crypto", wallets=["wallet1", "wallet2"], quorum_ratio=0.6)],
        filters=None,  # Simplified
        arbitrage=None,
        wallet_trades_path="",
        market_snapshots_path="",
        live_data=None,
        execution=None,
    )


@pytest.fixture
def sample_markets():
    return {
        "market1": MarketSnapshot(
            market_id="market1",
            topic="crypto",
            title="BTC > 100k",
            yes_ask=0.6,
            no_ask=0.4,
            best_bid=0.55,
            liquidity_usd=10000,
            minutes_to_resolution=10000,
        )
    }


@pytest.fixture
def sample_trades():
    now = datetime.now(UTC)
    return [
        WalletTrade(
            wallet="wallet1",
            topic="crypto",
            market_id="market1",
            side="YES",
            price=0.65,
            size_usd=100,
            age_seconds=300,
        )
    ]


@pytest.fixture
def sample_qualities():
    return {
        "wallet1": WalletQuality(
            wallet="wallet1",
            topic="crypto",
            score=0.8,
            eligible_trade_count=10,
            average_age_seconds=500,
            average_drift=0.05,
            reason="good",
        )
    }


def test_evaluate_performance(benchmark, sample_config, sample_markets, sample_trades, sample_qualities):
    engine = CopyEngine(sample_config)
    benchmark(engine.evaluate, sample_trades, sample_markets, sample_qualities)
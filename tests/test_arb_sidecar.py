from predictcel.arb_sidecar import ArbitrageSidecar
from predictcel.config import ArbitrageConfig
from predictcel.models import MarketSnapshot


def test_detects_complete_set_underpricing() -> None:
    sidecar = ArbitrageSidecar(ArbitrageConfig(min_gross_edge=0.02, min_liquidity_usd=5000))
    markets = {
        "m1": MarketSnapshot(
            market_id="m1",
            topic="sports",
            title="Example",
            yes_ask=0.46,
            no_ask=0.49,
            best_bid=0.44,
            liquidity_usd=9000,
            minutes_to_resolution=120,
        )
    }

    opportunities = sidecar.scan(markets)

    assert len(opportunities) == 1
    assert round(opportunities[0].gross_edge, 2) == 0.05


def test_skips_market_when_liquidity_too_low() -> None:
    sidecar = ArbitrageSidecar(ArbitrageConfig(min_gross_edge=0.02, min_liquidity_usd=5000))
    markets = {
        "m1": MarketSnapshot(
            market_id="m1",
            topic="sports",
            title="Example",
            yes_ask=0.46,
            no_ask=0.49,
            best_bid=0.44,
            liquidity_usd=1000,
            minutes_to_resolution=120,
        )
    }

    opportunities = sidecar.scan(markets)

    assert opportunities == []

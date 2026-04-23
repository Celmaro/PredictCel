from predictcel.arb_sidecar import ArbitrageSidecar
from predictcel.config import ArbitrageConfig
from predictcel.models import MarketSnapshot


def test_detects_complete_set_underpricing_with_net_metrics() -> None:
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
            yes_spread=0.02,
            no_spread=0.02,
            orderbook_ready=True,
        )
    }

    opportunities = sidecar.scan(markets)

    assert len(opportunities) == 1
    assert round(opportunities[0].gross_edge, 2) == 0.05
    assert opportunities[0].net_edge > 0
    assert opportunities[0].min_profitable_position >= 5.0
    assert opportunities[0].safe_position_size <= 50.0
    assert opportunities[0].annualized_return >= 0.10
    assert 0 < opportunities[0].quality_score <= 1
    assert opportunities[0].resolution_risk == "LOW"


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


def test_skips_when_costs_destroy_net_edge() -> None:
    sidecar = ArbitrageSidecar(
        ArbitrageConfig(
            min_gross_edge=0.02,
            min_liquidity_usd=5000,
            variable_cost_rate=0.03,
            slippage_rate=0.01,
        )
    )
    markets = {
        "m1": MarketSnapshot("m1", "sports", "Example", 0.46, 0.49, 0.44, 9000, 120)
    }

    assert sidecar.scan(markets) == []


def test_skips_when_min_profitable_position_exceeds_cap() -> None:
    sidecar = ArbitrageSidecar(
        ArbitrageConfig(
            min_gross_edge=0.02,
            min_liquidity_usd=5000,
            gas_cost_per_tx_usd=1.0,
            settlement_tx_count=2,
            max_position_usd=50.0,
        )
    )
    markets = {
        "m1": MarketSnapshot("m1", "sports", "Example", 0.46, 0.49, 0.44, 9000, 120)
    }

    assert sidecar.scan(markets) == []


def test_sorts_by_quality_score_then_net_edge() -> None:
    sidecar = ArbitrageSidecar(ArbitrageConfig(min_gross_edge=0.02, min_liquidity_usd=5000))
    markets = {
        "fast": MarketSnapshot("fast", "sports", "Fast", 0.46, 0.49, 0.44, 9000, 120, orderbook_ready=True),
        "slow": MarketSnapshot("slow", "sports", "Slow", 0.45, 0.49, 0.44, 9000, 3000, orderbook_ready=False),
    }

    opportunities = sidecar.scan(markets)

    assert [opportunity.market_id for opportunity in opportunities] == ["fast", "slow"]

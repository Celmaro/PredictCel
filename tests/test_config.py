from pathlib import Path

from predictcel.config import load_config


def test_example_config_loads() -> None:
    config = load_config(Path("config/predictcel.example.json"))

    assert config.baskets
    assert config.filters.min_liquidity_usd > 0
    assert config.arbitrage.min_gross_edge > 0
    assert config.execution is not None
    assert all(basket.target_allocation > 0 for basket in config.baskets)

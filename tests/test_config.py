import json
from pathlib import Path

import pytest

from predictcel.config import ConfigError, load_config


def test_example_config_loads() -> None:
    config = load_config(Path("config/predictcel.example.json"))

    assert config.baskets
    assert len(config.baskets) == 10
    assert config.filters.min_liquidity_usd > 0
    assert config.arbitrage.min_gross_edge > 0
    assert config.execution is not None
    assert any(basket.target_allocation == 0 for basket in config.baskets)
    assert sum(1 for basket in config.baskets if basket.target_allocation > 0) >= 8
    assert sum(1 for basket in config.baskets if basket.quorum_ratio == 0.8) >= 8
    assert sum(1 for basket in config.baskets if basket.quorum_ratio == 0.5) == 2
    assert config.wallet_registry.enabled is True
    assert config.basket_promotion.enabled is True
    assert config.basket_controller.enabled is True
    assert config.wallet_discovery.enabled is True
    assert config.wallet_discovery.source == "curated_wallet_file"
    assert config.basket_controller.tracked_basket_target == 15


def test_load_config_rejects_invalid_basket_controller_slot_total(tmp_path) -> None:
    payload = json.loads(
        Path("config/predictcel.example.json").read_text(encoding="utf-8")
    )
    payload["basket_controller"] = {
        "tracked_basket_target": 6,
        "core_slots": 3,
        "rotating_slots": 3,
        "backup_slots": 1,
        "explorer_slots": 0,
    }
    config_path = tmp_path / "invalid-config.json"
    config_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ConfigError, match="tracked_basket_target"):
        load_config(config_path)


def test_load_config_rejects_invalid_wallet_discovery_source(tmp_path) -> None:
    payload = json.loads(
        Path("config/predictcel.example.json").read_text(encoding="utf-8")
    )
    payload["wallet_discovery"]["source"] = "unsupported_source"
    config_path = tmp_path / "invalid-wallet-discovery-source.json"
    config_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ConfigError, match="wallet discovery source"):
        load_config(config_path)


def test_load_config_rejects_curated_wallet_source_without_path(tmp_path) -> None:
    payload = json.loads(
        Path("config/predictcel.example.json").read_text(encoding="utf-8")
    )
    payload["wallet_discovery"]["source"] = "curated_wallet_file"
    payload["wallet_discovery"]["wallet_candidates_path"] = ""
    config_path = tmp_path / "invalid-curated-wallet-source.json"
    config_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ConfigError, match="wallet_candidates_path"):
        load_config(config_path)


def test_load_config_rejects_invalid_basket_promotion_thresholds(tmp_path) -> None:
    payload = json.loads(
        Path("config/predictcel.example.json").read_text(encoding="utf-8")
    )
    payload["basket_promotion"]["min_live_eligible_wallets"] = 6
    payload["basket_promotion"]["min_tracked_wallets"] = 5
    config_path = tmp_path / "invalid-basket-promotion.json"
    config_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ConfigError, match="min_live_eligible_wallets"):
        load_config(config_path)


def test_load_config_rejects_recent_trade_threshold_above_total(monkeypatch) -> None:
    payload = json.loads(
        Path("config/predictcel.example.json").read_text(encoding="utf-8")
    )
    payload["wallet_discovery"]["min_trades"] = 5
    payload["wallet_discovery"]["min_recent_trades"] = 6
    monkeypatch.setattr(
        Path,
        "read_text",
        lambda self, encoding="utf-8": json.dumps(payload),
    )

    with pytest.raises(ConfigError, match="min_recent_trades"):
        load_config("synthetic-config.json")


def test_load_config_rejects_execution_single_position_above_total_exposure(
    monkeypatch,
) -> None:
    payload = json.loads(
        Path("config/predictcel.example.json").read_text(encoding="utf-8")
    )
    payload["execution"]["exposure"]["max_total_exposure_usd"] = 50
    payload["execution"]["exposure"]["max_single_position_usd"] = 60
    monkeypatch.setattr(
        Path,
        "read_text",
        lambda self, encoding="utf-8": json.dumps(payload),
    )

    with pytest.raises(ConfigError, match="max_single_position_usd"):
        load_config("synthetic-config.json")


def test_load_config_rejects_arbitrage_min_position_above_max_position(
    monkeypatch,
) -> None:
    payload = json.loads(
        Path("config/predictcel.example.json").read_text(encoding="utf-8")
    )
    payload["arbitrage"]["min_profitable_position_usd"] = 55
    payload["arbitrage"]["max_position_usd"] = 50
    monkeypatch.setattr(
        Path,
        "read_text",
        lambda self, encoding="utf-8": json.dumps(payload),
    )

    with pytest.raises(ConfigError, match="min_profitable_position_usd"):
        load_config("synthetic-config.json")


def test_load_config_rejects_arbitrage_target_return_above_max_return(
    monkeypatch,
) -> None:
    payload = json.loads(
        Path("config/predictcel.example.json").read_text(encoding="utf-8")
    )
    payload["arbitrage"]["target_annualized_return"] = 11
    payload["arbitrage"]["max_annualized_return"] = 10
    monkeypatch.setattr(
        Path,
        "read_text",
        lambda self, encoding="utf-8": json.dumps(payload),
    )

    with pytest.raises(ConfigError, match="target_annualized_return"):
        load_config("synthetic-config.json")


def test_load_config_rejects_invalid_basket_wallet_address(tmp_path) -> None:
    payload = json.loads(
        Path("config/predictcel.example.json").read_text(encoding="utf-8")
    )
    payload["baskets"][0]["wallets"] = ["not-a-wallet"]
    config_path = tmp_path / "invalid-basket-wallet.json"
    config_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ConfigError, match="invalid wallet addresses"):
        load_config(config_path)


def test_load_config_rejects_invalid_live_data_url(tmp_path) -> None:
    payload = json.loads(
        Path("config/predictcel.example.json").read_text(encoding="utf-8")
    )
    payload["live_data"]["gamma_base_url"] = "notaurl"
    config_path = tmp_path / "invalid-live-url.json"
    config_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ConfigError, match="gamma_base_url"):
        load_config(config_path)

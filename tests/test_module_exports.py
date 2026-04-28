from importlib import import_module


def test_public_module_exports_match_defined_symbols() -> None:
    expected_exports = {
        "predictcel.basket_manager": ["BasketManagerPlanner"],
        "predictcel.copy_engine": ["CopyEngine"],
        "predictcel.discovery": ["WalletCandidate", "score_wallet_candidates", "candidates_as_dicts"],
        "predictcel.main": ["main"],
        "predictcel.markets": ["load_market_snapshots"],
        "predictcel.wallet_discovery": ["WalletDiscoveryPipeline"],
        "predictcel.wallet_sources": ["DataApiWalletSource", "extract_wallet_address"],
        "predictcel.wallet_topics": ["classify_wallet_topics", "classify_trade_topic"],
        "predictcel.wallets": ["load_wallet_trades", "bucket_trades_by_market"],
    }

    for module_name, exported_names in expected_exports.items():
        module = import_module(module_name)

        assert module.__all__ == exported_names
        assert all(hasattr(module, name) for name in exported_names)

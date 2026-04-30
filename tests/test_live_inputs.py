from __future__ import annotations

import threading
from pathlib import Path

from predictcel.config import load_config
from predictcel.live_inputs import LiveInputHooks, load_live_inputs


def test_load_live_inputs_overlaps_wallet_and_market_fetches() -> None:
    config = load_config(Path("config/predictcel.example.json"))
    wallet_started = threading.Event()
    active_markets_entered = threading.Event()
    overlap_observed = {"value": False}

    class FakeClient:
        def __init__(self, *args, **kwargs) -> None:
            del args, kwargs

        def fetch_active_markets(self, _limit: int):
            wallet_started.wait(timeout=0.5)
            active_markets_entered.set()
            return []

        def fetch_markets_by_identifiers(self, _identifiers):
            return []

        def fetch_markets_by_slugs(self, _slugs):
            return []

    def fetch_wallet_payloads(_client, _wallets, _limit):
        wallet_started.set()
        overlap_observed["value"] = active_markets_entered.wait(timeout=0.5)
        return {}, []

    hooks = LiveInputHooks(
        client_cls=FakeClient,
        close_quietly=lambda _client: None,
        wallet_topics_for_live_inputs=lambda _config, _store: (
            {"0xde709f2102306220921060314715629080e2fb77": ["sports"]},
            "config_baskets",
        ),
        is_evm_address=lambda _wallet: True,
        fetch_wallet_payloads=fetch_wallet_payloads,
        build_wallet_trades=lambda _payloads, _topics: [],
        extract_trade_market_ids=lambda _payloads: [],
        extract_trade_market_slugs=lambda _payloads: [],
        trade_market_id_source_breakdown=lambda _payloads: {},
        build_market_snapshots=lambda _rows: {},
        index_market_row_token_aliases=lambda _markets, _rows: 0,
        looks_like_unresolved_token_id=lambda _value: False,
        recover_unresolved_token_market_rows=lambda _client, _token_ids: ([], {"requested": 0, "matched": 0, "unmatched": 0}, []),
        classify_unmatched_token_ids=lambda _token_ids, _loaded_rows, _markets, _probe_rows: {
            "breakdown": {},
            "samples": {},
        },
        enrich_market_snapshots_with_orderbooks=lambda snapshots, _client: snapshots,
        propagate_canonical_market_updates=lambda _markets, _snapshots: None,
        build_orderbook_probe_samples=lambda _client, _snapshots: [],
        get_transport_metrics=lambda: {},
        logger=type("Logger", (), {"warning": lambda *args, **kwargs: None})(),
        failure_threshold=0.5,
        failure_error_cls=RuntimeError,
    )

    trades, markets, diagnostics = load_live_inputs(config, store=None, hooks=hooks)

    assert trades == []
    assert len(markets) == 0
    assert diagnostics["active_market_rows_loaded"] == 0
    assert overlap_observed["value"] is True

import json
from datetime import UTC, datetime
from urllib.error import URLError

import pytest

from predictcel.polymarket import (
    PolymarketPublicClient,
    _extract_list,
    build_market_snapshots,
    build_wallet_trades,
    enrich_market_snapshots_with_orderbooks,
    extract_trade_market_ids,
    extract_trade_market_slugs,
    market_snapshot_from_gamma,
    wallet_trade_from_data,
)


class FakeClient:
    def __init__(self, books):
        self.books = books

    def fetch_order_book(self, token_id: str):
        return self.books.get(token_id, {})


class FakeGammaClient(PolymarketPublicClient):
    def __init__(self):
        super().__init__()
        self.urls = []

    def _get_json(self, url: str):
        self.urls.append(url)
        return {"markets": [{"conditionId": "cond_1", "outcomePrices": "[0.61, 0.35]"}]}


class FakeSlugClient(PolymarketPublicClient):
    def __init__(self):
        super().__init__()
        self.urls = []

    def _get_json(self, url: str):
        self.urls.append(url)
        if "/markets/slug/will-event-x-happen" in url:
            return {"conditionId": "cond_1", "slug": "will-event-x-happen", "outcomePrices": "[0.61, 0.35]"}
        return {"markets": []}


class FakeTradeClient(PolymarketPublicClient):
    def __init__(self):
        super().__init__()
        self.urls = []

    def _get_json(self, url: str):
        self.urls.append(url)
        if "user=0xabc" in url and "/trades?" in url:
            return {"trades": [{"asset": "token_yes", "side": "BUY_YES", "price": "0.54", "sizeUsd": "25", "createdAt": "2025-12-31T23:50:00Z", "user": "0xAbc"}]}
        return {"trades": []}


class FakeFilteredTradeClient(PolymarketPublicClient):
    def __init__(self):
        super().__init__()

    def _get_json(self, url: str):
        return {
            "trades": [
                {"asset": "token_wrong", "side": "BUY_YES", "price": "0.54", "sizeUsd": "25", "createdAt": "2025-12-31T23:50:00Z", "user": "0xdef"},
                {"asset": "token_right", "side": "BUY_NO", "price": "0.44", "sizeUsd": "30", "createdAt": "2025-12-31T23:52:00Z", "user": "0xabc"},
            ]
        }


class FakeHTTPResponse:
    def __init__(self, payload):
        self.payload = payload

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def test_market_snapshot_from_gamma_parses_string_prices_and_token_ids() -> None:
    item = {
        "conditionId": "cond_1",
        "question": "Will event X happen?",
        "outcomePrices": "[0.61, 0.35]",
        "bestBid": "0.58",
        "liquidityNum": "12000",
        "endDate": "2030-01-01T00:00:00Z",
        "category": "geopolitics",
        "clobTokenIds": "[\"yes_token\", \"no_token\"]",
    }

    snapshot = market_snapshot_from_gamma(item)

    assert snapshot is not None
    assert snapshot.market_id == "cond_1"
    assert snapshot.yes_ask == 0.61
    assert snapshot.no_ask == 0.35
    assert snapshot.topic == "geopolitics"
    assert snapshot.yes_token_id == "yes_token"
    assert snapshot.no_token_id == "no_token"


def test_fetch_active_markets_includes_active_filters() -> None:
    client = FakeGammaClient()

    client.fetch_active_markets(50)

    assert "limit=50" in client.urls[0]
    assert "closed=false" in client.urls[0]
    assert "active=true" in client.urls[0]


def test_fetch_markets_by_slugs_uses_slug_endpoint_and_deduplicates() -> None:
    client = FakeSlugClient()

    rows = client.fetch_markets_by_slugs(["will-event-x-happen", "will-event-x-happen"])

    assert rows == [{"conditionId": "cond_1", "slug": "will-event-x-happen", "outcomePrices": "[0.61, 0.35]"}]
    assert any("/markets/slug/will-event-x-happen" in url for url in client.urls)


def test_fetch_wallet_trades_accepts_asset_backed_trade_rows() -> None:
    client = FakeTradeClient()

    rows = client.fetch_wallet_trades("0xabc", 5)

    assert rows[0]["asset"] == "token_yes"
    assert any("/trades?user=0xabc" in url for url in client.urls)


def test_fetch_wallet_trades_filters_rows_to_requested_wallet() -> None:
    client = FakeFilteredTradeClient()

    rows = client.fetch_wallet_trades("0xabc", 5)

    assert len(rows) == 1
    assert rows[0]["asset"] == "token_right"


def test_build_market_snapshots_indexes_common_aliases() -> None:
    snapshots = build_market_snapshots(
        [
            {
                "conditionId": "cond_1",
                "slug": "will-event-x-happen",
                "outcomePrices": "[0.61, 0.35]",
                "clobTokenIds": "[\"yes_token\", \"no_token\"]",
            }
        ]
    )

    assert snapshots["cond_1"].market_id == "cond_1"
    assert snapshots["will-event-x-happen"].market_id == "cond_1"
    assert snapshots["yes_token"].market_id == "cond_1"


def test_enrich_market_snapshots_with_orderbooks_adds_spread_and_depth() -> None:
    item = {
        "conditionId": "cond_1",
        "question": "Will event X happen?",
        "outcomePrices": "[0.61, 0.35]",
        "bestBid": "0.58",
        "liquidityNum": "12000",
        "endDate": "2030-01-01T00:00:00Z",
        "category": "geopolitics",
        "clobTokenIds": "[\"yes_token\", \"no_token\"]",
    }
    snapshot = market_snapshot_from_gamma(item)
    assert snapshot is not None

    client = FakeClient(
        {
            "yes_token": {"bids": [{"price": "0.59", "size": "200"}], "asks": [{"price": "0.62", "size": "150"}]},
            "no_token": {"bids": [{"price": "0.33", "size": "180"}], "asks": [{"price": "0.36", "size": "140"}]},
        }
    )

    enriched = enrich_market_snapshots_with_orderbooks({"cond_1": snapshot, "yes_token": snapshot}, client)

    assert enriched["cond_1"].yes_bid == 0.59
    assert enriched["cond_1"].no_bid == 0.33
    assert enriched["cond_1"].yes_ask == 0.62
    assert enriched["cond_1"].no_ask == 0.36
    assert enriched["cond_1"].yes_ask_size == 150
    assert enriched["cond_1"].no_ask_size == 140
    assert enriched["cond_1"].yes_spread == 0.03
    assert enriched["cond_1"].no_spread == 0.03
    assert enriched["cond_1"].orderbook_ready is True
    assert enriched["yes_token"].orderbook_ready is True


def test_enrich_market_snapshots_requires_tradable_ask_depth() -> None:
    item = {
        "conditionId": "cond_1",
        "question": "Will event X happen?",
        "outcomePrices": "[0.61, 0.35]",
        "bestBid": "0.58",
        "liquidityNum": "12000",
        "endDate": "2030-01-01T00:00:00Z",
        "category": "geopolitics",
        "clobTokenIds": "[\"yes_token\", \"no_token\"]",
    }
    snapshot = market_snapshot_from_gamma(item)
    assert snapshot is not None

    client = FakeClient(
        {
            "yes_token": {"bids": [{"price": "0.59", "size": "200"}], "asks": []},
            "no_token": {"bids": [{"price": "0.33", "size": "180"}], "asks": []},
        }
    )

    enriched = enrich_market_snapshots_with_orderbooks({"cond_1": snapshot}, client)

    assert enriched["cond_1"].yes_ask == snapshot.yes_ask
    assert enriched["cond_1"].no_ask == snapshot.no_ask
    assert enriched["cond_1"].yes_ask_size == 0
    assert enriched["cond_1"].no_ask_size == 0
    assert enriched["cond_1"].orderbook_ready is False


def test_gamma_circuit_breaker_does_not_block_clob_requests(monkeypatch) -> None:
    client = PolymarketPublicClient(max_retries=1)
    client._gamma_circuit_breaker.failure_threshold = 1

    def fake_urlopen(request, timeout=15):
        if request.full_url.startswith(client.gamma_base_url):
            raise URLError("gamma down")
        if request.full_url.startswith(client.clob_base_url):
            return FakeHTTPResponse({
                "bids": [{"price": "0.48", "size": "10"}],
                "asks": [{"price": "0.52", "size": "12"}],
            })
        raise AssertionError(f"Unexpected URL: {request.full_url}")

    monkeypatch.setattr("predictcel.polymarket.urlopen", fake_urlopen)

    with pytest.raises(URLError):
        client.fetch_active_markets(1)

    book = client.fetch_order_book("token_yes")

    assert book["asks"][0]["price"] == "0.52"

    with pytest.raises(Exception, match="Circuit breaker is OPEN"):
        client.fetch_active_markets(1)


def test_wallet_trade_from_data_uses_outcome_and_timestamp() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    item = {
        "condition_id": "cond_1",
        "outcome": "YES",
        "price": "0.54",
        "size": "250",
        "createdAt": "2025-12-31T23:50:00Z",
    }

    trade = wallet_trade_from_data("wallet_a", "sports", item, now)

    assert trade is not None
    assert trade.wallet == "wallet_a"
    assert trade.topic == "sports"
    assert trade.side == "YES"
    assert trade.market_id == "cond_1"
    assert trade.age_seconds == 600


def test_wallet_trade_from_data_accepts_asset_ids_and_buy_yes_side() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    item = {
        "asset": "token_yes",
        "side": "BUY_YES",
        "matchedPrice": "0.54",
        "sizeUsd": "25",
        "createdAt": "2025-12-31T23:50:00Z",
    }

    trade = wallet_trade_from_data("wallet_a", "sports", item, now)

    assert trade is not None
    assert trade.market_id == "token_yes"
    assert trade.side == "YES"
    assert trade.price == 0.54
    assert trade.size_usd == 25.0


def test_build_wallet_trades_fans_out_multi_topic_wallets() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    wallet_payloads = {
        "wallet_a": [{"conditionId": "cond_1", "outcome": "YES", "price": "0.54", "size": "250", "createdAt": "2025-12-31T23:50:00Z"}]
    }

    trades = build_wallet_trades(wallet_payloads, {"wallet_a": ["sports", "macro"]}, now)

    assert len(trades) == 2
    assert {trade.topic for trade in trades} == {"sports", "macro"}


def test_extract_trade_market_ids_deduplicates_common_shapes() -> None:
    payloads = {
        "wallet_a": [
            {"conditionId": "cond_1"},
            {"condition_id": "cond_1"},
            {"marketSlug": "ignored"},
            {"market": "market_2"},
            {"asset": "token_yes"},
        ]
    }

    assert extract_trade_market_ids(payloads) == ["cond_1", "market_2", "token_yes"]


def test_extract_trade_market_slugs_deduplicates_common_shapes() -> None:
    payloads = {
        "wallet_a": [
            {"marketSlug": "will-event-x-happen"},
            {"slug": "will-event-x-happen"},
            {"market": {"slug": "another-market"}},
        ]
    }

    assert extract_trade_market_slugs(payloads) == ["another-market", "will-event-x-happen"]


def test_gamma_market_array_filter_uses_repeated_query_params() -> None:
    client = FakeGammaClient()

    rows = client._fetch_markets_by_array_filter("clob_token_ids", ["token_a", "token_b"], 25)

    assert rows == [{"conditionId": "cond_1", "outcomePrices": "[0.61, 0.35]"}]
    assert "clob_token_ids=token_a" in client.urls[0]
    assert "clob_token_ids=token_b" in client.urls[0]


def test_build_wallet_trades_skips_wallets_without_topic_mapping() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    wallet_payloads = {
        "wallet_a": [{"conditionId": "cond_1", "outcome": "YES", "price": "0.54", "size": "250", "createdAt": "2025-12-31T23:50:00Z"}],
        "wallet_b": [{"conditionId": "cond_2", "outcome": "NO", "price": "0.44", "size": "150", "createdAt": "2025-12-31T23:55:00Z"}],
    }

    trades = build_wallet_trades(wallet_payloads, {"wallet_a": "sports"}, now)

    assert len(trades) == 1
    assert trades[0].wallet == "wallet_a"


def test_extract_list_handles_leaderboard_shapes() -> None:
    payload = {"leaderboard": [{"address": "0x1"}], "total": 1}

    assert _extract_list(payload) == [{"address": "0x1"}]
    assert _extract_list({"users": [{"address": "0x2"}]}) == [{"address": "0x2"}]


def test_validate_response_accepts_outcome_prices_market_payload() -> None:
    client = PolymarketPublicClient()

    client._validate_response(
        {"data": [{"conditionId": "cond_1", "outcomePrices": "[0.61, 0.35]"}]},
        "https://gamma-api.polymarket.com/markets?limit=1",
    )

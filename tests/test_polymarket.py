import asyncio
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from urllib.error import HTTPError, URLError

import pytest
import aiohttp

from predictcel.polymarket import (
    PolymarketPublicClient,
    _AsyncHttpTransport,
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
            return {
                "conditionId": "cond_1",
                "slug": "will-event-x-happen",
                "outcomePrices": "[0.61, 0.35]",
            }
        if "/events/slug/event-slug-only" in url:
            return {
                "markets": [
                    {
                        "conditionId": "cond_event",
                        "slug": "market-from-event",
                        "outcomePrices": "[0.55, 0.41]",
                    }
                ]
            }
        return {"markets": []}


class FakeTradeClient(PolymarketPublicClient):
    def __init__(self):
        super().__init__()
        self.urls = []

    def _get_json(self, url: str):
        self.urls.append(url)
        if "user=0xabc" in url and "/trades?" in url:
            return {
                "trades": [
                    {
                        "asset": "token_yes",
                        "side": "BUY_YES",
                        "price": "0.54",
                        "sizeUsd": "25",
                        "createdAt": "2025-12-31T23:50:00Z",
                        "user": "0xAbc",
                    }
                ]
            }
        return {"trades": []}


class FakeFilteredTradeClient(PolymarketPublicClient):
    def __init__(self):
        super().__init__()

    def _get_json(self, url: str):
        return {
            "trades": [
                {
                    "asset": "token_wrong",
                    "side": "BUY_YES",
                    "price": "0.54",
                    "sizeUsd": "25",
                    "createdAt": "2025-12-31T23:50:00Z",
                    "user": "0xdef",
                },
                {
                    "asset": "token_right",
                    "side": "BUY_NO",
                    "price": "0.44",
                    "sizeUsd": "30",
                    "createdAt": "2025-12-31T23:52:00Z",
                    "user": "0xabc",
                },
            ]
        }


class CachedVariantTradeClient(PolymarketPublicClient):
    def __init__(self):
        super().__init__()
        self.urls = []

    def _get_json(self, url: str):
        self.urls.append(url)
        if "address=0xabc" in url:
            return {
                "trades": [
                    {
                        "asset": "token_yes",
                        "side": "BUY_YES",
                        "price": "0.54",
                        "sizeUsd": "25",
                        "createdAt": "2025-12-31T23:50:00Z",
                        "user": "0xabc",
                    }
                ]
            }
        return {"trades": []}


class PerWalletVariantTradeClient(PolymarketPublicClient):
    def __init__(self):
        super().__init__()
        self.urls = []

    def _get_json(self, url: str):
        self.urls.append(url)
        if "address=0xaaa" in url:
            return {
                "trades": [
                    {
                        "asset": "token_a",
                        "side": "BUY_YES",
                        "price": "0.54",
                        "sizeUsd": "25",
                        "createdAt": "2025-12-31T23:50:00Z",
                        "user": "0xaaa",
                    }
                ]
            }
        if "user=0xbbb" in url:
            return {
                "trades": [
                    {
                        "asset": "token_b",
                        "side": "BUY_YES",
                        "price": "0.55",
                        "sizeUsd": "20",
                        "createdAt": "2025-12-31T23:55:00Z",
                        "user": "0xbbb",
                    }
                ]
            }
        return {"trades": []}


class FakeHTTPResponse:
    def __init__(self, payload):
        self.payload = payload

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class FakeWSMessage:
    def __init__(self, message_type, payload=None):
        self.type = message_type
        self._payload = payload

    def json(self):
        return self._payload


class FakeWebSocket:
    def __init__(self, messages):
        self._messages = iter(messages)
        self.sent_messages = []

    async def send_json(self, payload) -> None:
        self.sent_messages.append(payload)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._messages)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


class FakeWebSocketContext:
    def __init__(self, websocket: FakeWebSocket):
        self.websocket = websocket

    async def __aenter__(self) -> FakeWebSocket:
        return self.websocket

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class FakeClientSession:
    def __init__(self, websockets: list[FakeWebSocket]):
        self._websockets = list(websockets)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False

    def ws_connect(self, url: str):
        assert url == "wss://ws.polymarket.com"
        if not self._websockets:
            raise AssertionError("No fake websockets remaining")
        return FakeWebSocketContext(self._websockets.pop(0))


def test_market_snapshot_from_gamma_parses_string_prices_and_token_ids() -> None:
    item = {
        "conditionId": "cond_1",
        "question": "Will event X happen?",
        "outcomePrices": "[0.61, 0.35]",
        "bestBid": "0.58",
        "liquidityNum": "12000",
        "endDate": "2030-01-01T00:00:00Z",
        "category": "geopolitics",
        "clobTokenIds": '["yes_token", "no_token"]',
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

    assert rows == [
        {
            "conditionId": "cond_1",
            "slug": "will-event-x-happen",
            "outcomePrices": "[0.61, 0.35]",
        }
    ]
    assert any("/markets/slug/will-event-x-happen" in url for url in client.urls)


def test_fetch_markets_by_slugs_falls_back_to_event_slug() -> None:
    client = FakeSlugClient()

    rows = client.fetch_markets_by_slugs(["event-slug-only"])

    assert rows == [
        {
            "conditionId": "cond_event",
            "slug": "market-from-event",
            "outcomePrices": "[0.55, 0.41]",
        }
    ]
    assert any("/markets/slug/event-slug-only" in url for url in client.urls)
    assert any("/events/slug/event-slug-only" in url for url in client.urls)


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


def test_fetch_wallet_trades_caches_preferred_endpoint_variant() -> None:
    client = CachedVariantTradeClient()

    first_rows = client.fetch_wallet_trades("0xabc", 5)
    second_rows = client.fetch_wallet_trades("0xabc", 5)

    assert first_rows[0]["asset"] == "token_yes"
    assert second_rows[0]["asset"] == "token_yes"
    assert "user=0xabc" in client.urls[0]
    assert "address=0xabc" in client.urls[1]
    assert "address=0xabc" in client.urls[2]


def test_fetch_wallet_trades_keeps_endpoint_preference_scoped_per_wallet() -> None:
    client = PerWalletVariantTradeClient()

    first_rows = client.fetch_wallet_trades("0xaaa", 5)
    client.urls.clear()
    second_rows = client.fetch_wallet_trades("0xbbb", 5)

    assert first_rows[0]["asset"] == "token_a"
    assert second_rows[0]["asset"] == "token_b"
    assert "user=0xbbb" in client.urls[0]


def test_fetch_wallet_trades_stops_after_retryable_error_budget(monkeypatch) -> None:
    client = PolymarketPublicClient()
    client._wallet_trade_retryable_error_budget = 2
    attempted_urls = []

    def fake_get_json(url: str):
        attempted_urls.append(url)
        raise URLError("upstream unavailable")

    monkeypatch.setattr(client, "_get_json", fake_get_json)

    rows = client.fetch_wallet_trades("0xabc", 5)

    assert rows == []
    assert len(attempted_urls) == 2


def test_fetch_market_trades_uses_repeated_market_query_params() -> None:
    client = FakeTradeClient()

    client.fetch_market_trades(["cond_1", "cond_2"], 5)

    assert "market=cond_1" in client.urls[0]
    assert "market=cond_2" in client.urls[0]


def test_build_market_snapshots_indexes_common_aliases() -> None:
    snapshots = build_market_snapshots(
        [
            {
                "conditionId": "cond_1",
                "slug": "will-event-x-happen",
                "outcomePrices": "[0.61, 0.35]",
                "clobTokenIds": '["yes_token", "no_token"]',
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
        "clobTokenIds": '["yes_token", "no_token"]',
    }
    snapshot = market_snapshot_from_gamma(item)
    assert snapshot is not None

    client = FakeClient(
        {
            "yes_token": {
                "bids": [{"price": "0.59", "size": "200"}],
                "asks": [{"price": "0.62", "size": "150"}],
            },
            "no_token": {
                "bids": [{"price": "0.33", "size": "180"}],
                "asks": [{"price": "0.36", "size": "140"}],
            },
        }
    )

    enriched = enrich_market_snapshots_with_orderbooks(
        {"cond_1": snapshot, "yes_token": snapshot}, client
    )

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
        "clobTokenIds": '["yes_token", "no_token"]',
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


def test_fetch_order_book_treats_missing_clob_book_as_empty_payload(
    monkeypatch,
) -> None:
    client = PolymarketPublicClient(max_retries=1)

    class FakeTransport:
        async def fetch_json(self, url: str):
            raise HTTPError(url, 404, "Not Found", hdrs=None, fp=None)

        def run(self, coroutine):
            return asyncio.run(coroutine)

    monkeypatch.setattr(client, "_http_transport", FakeTransport())

    book = client.fetch_order_book("missing_token")

    assert book == {}
    assert client._clob_circuit_breaker.state == "CLOSED"


def test_missing_clob_book_does_not_block_later_valid_books(monkeypatch) -> None:
    client = PolymarketPublicClient(max_retries=1)

    class FakeTransport:
        async def fetch_json(self, url: str):
            if "missing_token" in url:
                raise HTTPError(url, 404, "Not Found", hdrs=None, fp=None)
            return {
                "bids": [{"price": "0.48", "size": "10"}],
                "asks": [{"price": "0.52", "size": "12"}],
            }

        def run(self, coroutine):
            return asyncio.run(coroutine)

    monkeypatch.setattr(client, "_http_transport", FakeTransport())

    missing_book = client.fetch_order_book("missing_token")
    valid_book = client.fetch_order_book("valid_token")

    assert missing_book == {}
    assert valid_book["asks"][0]["price"] == "0.52"
    assert client._clob_circuit_breaker.state == "CLOSED"


def test_gamma_circuit_breaker_does_not_block_clob_requests(monkeypatch) -> None:
    client = PolymarketPublicClient(max_retries=1)
    client._gamma_circuit_breaker.failure_threshold = 1

    def fake_fetch_json_with_retries(url: str):
        if url.startswith(client.gamma_base_url):
            raise URLError("gamma down")
        if url.startswith(client.clob_base_url):
            return {
                "bids": [{"price": "0.48", "size": "10"}],
                "asks": [{"price": "0.52", "size": "12"}],
            }
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(client, "_fetch_json_with_retries", fake_fetch_json_with_retries)

    with pytest.raises(URLError):
        client.fetch_active_markets(1)

    book = client.fetch_order_book("token_yes")

    assert book["asks"][0]["price"] == "0.52"

    with pytest.raises(Exception, match="Circuit breaker is OPEN"):
        client.fetch_active_markets(1)


def test_subscribe_market_updates_reconnects_after_closed_socket(monkeypatch) -> None:
    client = PolymarketPublicClient()
    first_socket = FakeWebSocket([FakeWSMessage(aiohttp.WSMsgType.CLOSED)])
    second_socket = FakeWebSocket(
        [
            FakeWSMessage(
                aiohttp.WSMsgType.TEXT,
                {"type": "update", "market": "m1"},
            ),
            FakeWSMessage(aiohttp.WSMsgType.CLOSED),
        ]
    )
    sleep_calls = []
    updates = []
    queued_sockets = [first_socket, second_socket]
    callback_called = threading.Event()

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)
        if len(sleep_calls) >= 2:
            raise asyncio.CancelledError()

    def fake_client_session():
        return FakeClientSession([queued_sockets.pop(0)])

    def callback(payload) -> None:
        updates.append(payload)
        callback_called.set()

    monkeypatch.setattr("predictcel.polymarket.aiohttp.ClientSession", fake_client_session)
    monkeypatch.setattr("predictcel.polymarket._retry_delay", lambda base, attempt: 0.25)
    monkeypatch.setattr("predictcel.polymarket.asyncio.sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(client.subscribe_market_updates(["m1"], callback))

    assert callback_called.wait(timeout=1)
    assert sleep_calls == [0.25, 0.25]
    assert first_socket.sent_messages == [{"type": "subscribe", "markets": ["m1"]}]
    assert second_socket.sent_messages == [{"type": "subscribe", "markets": ["m1"]}]
    assert updates == [{"type": "update", "market": "m1"}]


def test_subscribe_market_updates_isolates_blocking_callback(monkeypatch) -> None:
    client = PolymarketPublicClient()
    socket = FakeWebSocket(
        [
            FakeWSMessage(
                aiohttp.WSMsgType.TEXT,
                {"type": "update", "market": "m1"},
            ),
            FakeWSMessage(aiohttp.WSMsgType.CLOSED),
        ]
    )
    callback_started = threading.Event()
    release_callback = threading.Event()
    sleep_calls = []
    updates = []

    async def fake_sleep(delay: float) -> None:
        assert callback_started.wait(timeout=1)
        sleep_calls.append(delay)
        release_callback.set()
        raise asyncio.CancelledError()

    def fake_client_session():
        return FakeClientSession([socket])

    def callback(payload) -> None:
        updates.append(payload)
        callback_started.set()
        release_callback.wait(timeout=1)
        raise RuntimeError("boom")

    monkeypatch.setattr("predictcel.polymarket.aiohttp.ClientSession", fake_client_session)
    monkeypatch.setattr("predictcel.polymarket._retry_delay", lambda base, attempt: 0.25)
    monkeypatch.setattr("predictcel.polymarket.asyncio.sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(client.subscribe_market_updates(["m1"], callback))

    assert sleep_calls == [0.25]
    assert updates == [{"type": "update", "market": "m1"}]


def test_get_json_deduplicates_inflight_requests(monkeypatch) -> None:
    client = PolymarketPublicClient(max_retries=1)
    started = threading.Event()
    release = threading.Event()
    calls = {"count": 0}

    def fake_fetch_json_with_retries(url: str):
        del url
        calls["count"] += 1
        started.set()
        release.wait(timeout=1)
        return {"markets": [{"conditionId": "cond_1"}]}

    monkeypatch.setattr(client, "_fetch_json_with_retries", fake_fetch_json_with_retries)

    def fetch():
        return client._get_json(f"{client.gamma_base_url}/markets?limit=1")

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(fetch)
        assert started.wait(timeout=1)
        second = executor.submit(fetch)
        release.set()
        results = [first.result(), second.result()]

    assert calls["count"] == 1
    assert results == [
        {"markets": [{"conditionId": "cond_1"}]},
        {"markets": [{"conditionId": "cond_1"}]},
    ]


def test_get_json_follower_timeout_falls_back_to_direct_fetch(monkeypatch) -> None:
    client = PolymarketPublicClient(max_retries=1)
    started = threading.Event()
    release = threading.Event()
    calls = {"count": 0}

    def fake_fetch_json_with_retries(url: str):
        del url
        calls["count"] += 1
        if calls["count"] == 1:
            started.set()
            release.wait(timeout=1)
            return {"markets": [{"conditionId": "leader"}]}
        return {"markets": [{"conditionId": "fallback"}]}

    monkeypatch.setattr(client, "_fetch_json_with_retries", fake_fetch_json_with_retries)
    monkeypatch.setattr(client, "_inflight_wait_timeout_seconds", lambda: 0.05)

    def fetch():
        return client._get_json(f"{client.gamma_base_url}/markets?limit=1")

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(fetch)
        assert started.wait(timeout=1)
        second = executor.submit(fetch)
        fallback_result = second.result(timeout=1)
        release.set()
        leader_result = first.result(timeout=1)

    assert calls["count"] == 2
    assert leader_result == {"markets": [{"conditionId": "leader"}]}
    assert fallback_result == {"markets": [{"conditionId": "fallback"}]}
    assert client._inflight_requests == {}


def test_non_retryable_errors_do_not_open_circuit_breaker(monkeypatch) -> None:
    client = PolymarketPublicClient(max_retries=1)
    client._gamma_circuit_breaker.failure_threshold = 1

    def fail(url: str):
        del url
        raise ValueError("bad payload")

    monkeypatch.setattr(client, "_fetch_json_with_retries", fail)

    with pytest.raises(ValueError, match="bad payload"):
        client.fetch_active_markets(1)

    assert client._gamma_circuit_breaker.state == "CLOSED"

    monkeypatch.setattr(
        client,
        "_fetch_json_with_retries",
        lambda url: {"markets": [{"conditionId": "cond_1", "outcomePrices": "[0.6, 0.4]"}]},
    )

    rows = client.fetch_active_markets(1)

    assert rows == [{"conditionId": "cond_1", "outcomePrices": "[0.6, 0.4]"}]
    assert client._gamma_circuit_breaker.state == "CLOSED"


def test_async_http_transport_close_ignores_closed_loop() -> None:
    transport = _AsyncHttpTransport(timeout_seconds=1)

    class FakeLoop:
        def is_closed(self) -> bool:
            return True

    class FakeThread:
        def __init__(self) -> None:
            self.join_calls = []

        def join(self, timeout=None) -> None:
            self.join_calls.append(timeout)

    class FakeSession:
        closed = False

        async def close(self) -> None:
            raise AssertionError("closed loop should not schedule session close")

    thread = FakeThread()
    transport._loop = FakeLoop()
    transport._thread = thread
    transport._session = FakeSession()

    transport.close()

    assert thread.join_calls == [5]


def test_memory_cache_evicts_oldest_entries_when_bounded() -> None:
    client = PolymarketPublicClient()
    client._cache_max_entries = 2

    client._set_cached("one", {"value": 1})
    client._set_cached("two", {"value": 2})
    client._set_cached("three", {"value": 3})

    assert client._get_cached("one") is None
    assert client._get_cached("two") == {"value": 2}
    assert client._get_cached("three") == {"value": 3}


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
        "wallet_a": [
            {
                "conditionId": "cond_1",
                "outcome": "YES",
                "price": "0.54",
                "size": "250",
                "createdAt": "2025-12-31T23:50:00Z",
            }
        ]
    }

    trades = build_wallet_trades(
        wallet_payloads, {"wallet_a": ["sports", "macro"]}, now
    )

    assert len(trades) == 2
    assert {trade.topic for trade in trades} == {"sports", "macro"}


def test_extract_trade_market_ids_deduplicates_common_shapes() -> None:
    payloads = {
        "wallet_a": [
            {"conditionId": "cond_1"},
            {"condition_id": "cond_1"},
            {"marketSlug": "ignored"},
            {"market": "market_2"},
            {"market": {"slug": "ignored-nested-slug", "id": "market_3"}},
            {"asset": "token_yes"},
        ]
    }

    assert extract_trade_market_ids(payloads) == [
        "cond_1",
        "market_2",
        "market_3",
        "token_yes",
    ]


def test_extract_trade_market_ids_prefers_nested_market_id_over_token_fields() -> None:
    payloads = {
        "wallet_a": [
            {
                "asset": "token_yes",
                "tokenId": "token_yes",
                "market": {"id": "market_123", "slug": "will-x-happen"},
            },
            {
                "clobTokenId": "token_no",
                "market": {"conditionId": "cond_1"},
            },
        ]
    }

    assert extract_trade_market_ids(payloads) == ["cond_1", "market_123"]


def test_extract_trade_market_slugs_deduplicates_common_shapes() -> None:
    payloads = {
        "wallet_a": [
            {"marketSlug": "will-event-x-happen"},
            {"slug": "will-event-x-happen"},
            {"market": {"slug": "another-market"}},
        ]
    }

    assert extract_trade_market_slugs(payloads) == [
        "another-market",
        "will-event-x-happen",
    ]


def test_extract_trade_market_slugs_prefers_nested_market_slug() -> None:
    payloads = {
        "wallet_a": [
            {
                "asset": "token_yes",
                "marketSlug": "stale-slug",
                "market": {"conditionId": "cond_1", "slug": "canonical-slug"},
            }
        ]
    }

    assert extract_trade_market_slugs(payloads) == ["canonical-slug"]


def test_gamma_market_array_filter_uses_repeated_query_params() -> None:
    client = FakeGammaClient()

    rows = client._fetch_markets_by_array_filter(
        "clob_token_ids", ["token_a", "token_b"], 25
    )

    assert rows == [{"conditionId": "cond_1", "outcomePrices": "[0.61, 0.35]"}]
    assert "clob_token_ids=token_a" in client.urls[0]
    assert "clob_token_ids=token_b" in client.urls[0]


def test_build_wallet_trades_skips_wallets_without_topic_mapping() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    wallet_payloads = {
        "wallet_a": [
            {
                "conditionId": "cond_1",
                "outcome": "YES",
                "price": "0.54",
                "size": "250",
                "createdAt": "2025-12-31T23:50:00Z",
            }
        ],
        "wallet_b": [
            {
                "conditionId": "cond_2",
                "outcome": "NO",
                "price": "0.44",
                "size": "150",
                "createdAt": "2025-12-31T23:55:00Z",
            }
        ],
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


def test_validate_response_skips_anomaly_scan_when_not_sampled(monkeypatch) -> None:
    client = PolymarketPublicClient()
    called = {"value": False}

    monkeypatch.setattr(client, "_should_sample_validation", lambda: False)

    def fake_validate_price_anomalies(payload, url):
        del payload, url
        called["value"] = True

    monkeypatch.setattr(client, "_validate_price_anomalies", fake_validate_price_anomalies)

    client._validate_response(
        [{"yes": 0.61, "no": 0.35}],
        "https://gamma-api.polymarket.com/data",
    )

    assert called["value"] is False

"""Integration tests for Polymarket API interactions.

Run with: pytest tests/integration/ -v
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


def load_fixture(name: str) -> dict[str, Any]:
    path = FIXTURES_DIR / f"{name}.json"
    assert path.exists(), f"Fixture not found: {path}"
    return json.loads(path.read_text(encoding="utf-8"))


class MockResponse:
    def __init__(self, body: bytes, status: int = 200):
        self._body = BytesIO(body)
        self.status = status

    def read(self, n: int | None = None) -> bytes:
        return self._body.read(n)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class TestGammaAPIIntegration:
    """Integration tests for Gamma API endpoints."""

    def test_fetch_active_markets_returns_realistic_data(self):
        """Verify we can parse realistic Gamma API market responses."""
        fixture = load_fixture("gamma_markets")
        body = json.dumps(fixture).encode()
        mock = MockResponse(body)

        with patch("predictcel.polymarket.urlopen", return_value=mock):
            from predictcel.polymarket import PolymarketPublicClient

            client = PolymarketPublicClient()
            markets = client.fetch_active_markets(limit=50)

        assert len(markets) == 3
        assert markets[0]["conditionId"] == "0xabc123def456"
        assert markets[0]["category"] == "Crypto"

    def test_fetch_markets_by_condition_ids(self):
        """Verify batch market lookup by condition IDs."""
        fixture = load_fixture("gamma_markets")
        body = json.dumps(fixture).encode()
        mock = MockResponse(body)

        with patch("predictcel.polymarket.urlopen", return_value=mock):
            from predictcel.polymarket import PolymarketPublicClient

            client = PolymarketPublicClient()
            markets = client.fetch_markets_by_condition_ids(["0xabc123def456"])

        assert len(markets) >= 1
        assert markets[0]["conditionId"] == "0xabc123def456"


class TestDataAPIIntegration:
    """Integration tests for Data API endpoints."""

    def test_fetch_wallet_trades_filters_correctly(self):
        """Verify wallet trade fetching filters by wallet address."""
        fixture = load_fixture("data_api_trades")
        body = json.dumps(fixture).encode()
        mock = MockResponse(body)

        with patch("predictcel.polymarket.urlopen", return_value=mock):
            from predictcel.polymarket import PolymarketPublicClient

            client = PolymarketPublicClient()
            trades = client.fetch_wallet_trades("0xwallet1abcdef", limit=10)

        assert len(trades) >= 2
        for trade in trades:
            wallet_in_trade = (
                trade.get("user", "").lower()
                or trade.get("address", "").lower()
            )
            assert "wallet1" in wallet_in_trade

    def test_fetch_wallet_trades_empty_for_unknown_wallet(self):
        """Unknown wallet should return empty or filtered results."""
        fixture = load_fixture("data_api_trades")
        body = json.dumps(fixture).encode()
        mock = MockResponse(body)

        with patch("predictcel.polymarket.urlopen", return_value=mock):
            from predictcel.polymarket import PolymarketPublicClient

            client = PolymarketPublicClient()
            trades = client.fetch_wallet_trades("0xUNKNOWNWALLET123", limit=10)

        # Should return empty since no trades match
        assert len(trades) == 0

    def test_fetch_leaderboard_parses_correctly(self):
        """Verify leaderboard data is parsed with proper wallet and PnL."""
        fixture = load_fixture("leaderboard")
        body = json.dumps(fixture).encode()
        mock = MockResponse(body)

        with patch("predictcel.polymarket.urlopen", return_value=mock):
            from predictcel.polymarket import PolymarketPublicClient

            client = PolymarketPublicClient()
            rows = client.fetch_leaderboard(limit=10)

        assert len(rows) == 3
        assert rows[0]["proxyWallet"] == "0xwallet1abcdef"
        assert float(rows[0]["pnl"]) == 150000.00


class TestOrderBookIntegration:
    """Integration tests for CLOB order book endpoints."""

    def test_fetch_order_book_returns_bids_and_asks(self):
        """Verify order book contains bid/ask levels."""
        fixture = load_fixture("orderbook_btc")
        body = json.dumps(fixture).encode()
        mock = MockResponse(body)

        with patch("predictcel.polymarket.urlopen", return_value=mock):
            from predictcel.polymarket import PolymarketPublicClient

            client = PolymarketPublicClient()
            book = client.fetch_order_book("0x1234yes")

        assert "bids" in book
        assert "asks" in book
        assert len(book["bids"]) >= 1
        assert book["bids"][0]["price"] == "0.60"


class TestCircuitBreakerIntegration:
    """Integration tests for circuit breaker resilience pattern."""

    def test_circuit_breaker_opens_after_threshold(self):
        """Circuit breaker should open after repeated failures."""
        from predictcel.polymarket import PolymarketPublicClient

        client = PolymarketPublicClient()
        cb = client._circuit_breaker
        cb.failure_threshold = 3
        cb.recovery_timeout = 3600  # Don't auto-recover during test

        with patch(
            "predictcel.polymarket.urlopen",
            side_effect=Exception("Network error"),
        ):
            for i in range(3):
                try:
                    client._get_json("https://gamma-api.polymarket.com/test")
                except Exception:
                    pass

        assert cb.state == "OPEN"

    def test_circuit_breaker_rejects_calls_when_open(self):
        """Open circuit breaker should reject calls immediately."""
        from predictcel.polymarket import PolymarketPublicClient

        client = PolymarketPublicClient()
        cb = client._circuit_breaker
        cb.state = "OPEN"
        cb.last_failure_time = datetime.now(UTC).timestamp()
        cb.recovery_timeout = 3600

        with pytest.raises(Exception, match="Circuit breaker is OPEN"):
            client._get_json("https://gamma-api.polymarket.com/test")

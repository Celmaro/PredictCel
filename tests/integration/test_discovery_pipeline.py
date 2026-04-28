"""Integration tests for wallet discovery and scoring pipeline.

Verifies the end-to-end flow from Polymarket API responses
through wallet discovery and scoring.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from predictcel.discovery import score_wallet_candidates
from predictcel.polymarket import build_wallet_trades

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


def load_fixture(name: str):
    path = FIXTURES_DIR / f"{name}.json"
    assert path.exists(), f"Fixture not found: {path}"
    return json.loads(path.read_text(encoding="utf-8"))


class TestWalletDiscoveryPipeline:
    """Integration tests for wallet discovery pipeline."""

    def test_full_pipeline_gamma_markets_to_trades(self):
        """Verify trades map to markets and wallet scoring works."""
        now = datetime(2026, 4, 28, tzinfo=UTC)

        # Simulate wallet trade data keyed by wallet
        wallet_payloads = {
            "0xwallet1abcdef": [
                {
                    "conditionId": "0xabc123def456",
                    "outcome": "YES",
                    "price": "0.61",
                    "size": "500",
                    "createdAt": "2026-04-27T10:00:00Z",
                },
                {
                    "conditionId": "0xabc123def456",
                    "outcome": "NO",
                    "price": "0.37",
                    "size": "300",
                    "createdAt": "2026-04-27T09:30:00Z",
                },
            ],
            "0xwallet2ghijkl": [
                {
                    "conditionId": "0xdef789abc012",
                    "outcome": "YES",
                    "price": "0.46",
                    "size": "100",
                    "createdAt": "2026-04-27T08:00:00Z",
                },
            ],
        }

        # Topic mapping from trade classification
        topic_map = {
            "0xwallet1abcdef": ["crypto", "crypto"],
            "0xwallet2ghijkl": ["weather"],
        }

        trades = build_wallet_trades(wallet_payloads, topic_map, now)

        assert len(trades) >= 3
        assert all(t.wallet in ["0xwallet1abcdef", "0xwallet2ghijkl"] for t in trades)

    def test_wallet_scoring_from_leaderboard(self):
        """Verify wallet scoring from leaderboard + trade data."""
        leaderboard = [
            {
                "proxyWallet": "0xwallet1abcdef",
                "userName": "CryptoWhale",
                "pnl": "150000.00",
                "vol": "500000.00",
            },
            {
                "proxyWallet": "0xwallet2ghijkl",
                "userName": "WeatherWatcher",
                "pnl": "25000.00",
                "vol": "80000.00",
            },
        ]

        trades_by_wallet = {
            "0xwallet1abcdef": [
                {"conditionId": "0xabc123def456", "outcome": "YES", "price": "0.61", "size": "500"},
                {"conditionId": "0xabc123def456", "outcome": "NO", "price": "0.37", "size": "300"},
                {"conditionId": "0xabc123def456", "outcome": "YES", "price": "0.60", "size": "450"},
                {"conditionId": "0xabc123def456", "outcome": "YES", "price": "0.62", "size": "600"},
                {"conditionId": "0xabc123def456", "outcome": "YES", "price": "0.59", "size": "550"},
            ],
            "0xwallet2ghijkl": [
                {"conditionId": "0xdef789abc012", "outcome": "YES", "price": "0.46", "size": "100"},
                {"conditionId": "0xdef789abc012", "outcome": "NO", "price": "0.55", "size": "80"},
                {"conditionId": "0xdef789abc012", "outcome": "YES", "price": "0.44", "size": "120"},
                {"conditionId": "0xdef789abc012", "outcome": "YES", "price": "0.47", "size": "90"},
                {"conditionId": "0xdef789abc012", "outcome": "YES", "price": "0.45", "size": "110"},
            ],
        }

        candidates = score_wallet_candidates(
            leaderboard,
            trades_by_wallet,
            min_trade_count=5,
        )

        assert len(candidates) == 2
        # CryptoWhale should score higher due to better PnL and volume
        assert candidates[0].username == "CryptoWhale"
        assert candidates[0].wallet == "0xwallet1abcdef"


class TestMarketSnapshotPipeline:
    """Integration tests for market snapshot building."""

    def test_market_snapshot_creation_from_gamma(self):
        """Verify MarketSnapshot creation from Gamma API data."""
        from predictcel.polymarket import build_market_snapshots

        gamma_data = [
            {
                "conditionId": "0xabc123def456",
                "question": "Will BTC close above $70k?",
                "outcomePrices": "[0.62, 0.38]",
                "bestBid": "0.60",
                "bestAsk": "0.63",
                "liquidityNum": "45000",
                "endDate": "2026-04-30T23:59:59Z",
                "category": "Crypto",
                "clobTokenIds": '["0x1234yes", "0x1234no"]',
            }
        ]

        snapshots = build_market_snapshots(gamma_data)

        assert "0xabc123def456" in snapshots
        assert "0x1234yes" in snapshots  # Token ID alias
        snap = snapshots["0xabc123def456"]
        assert snap.yes_ask == 0.62
        assert snap.no_ask == 0.38
        assert snap.topic == "Crypto"

    def test_market_snapshot_with_orderbook_enrichment(self):
        """Verify order book enrichment adds bid/ask and spread."""
        from predictcel.polymarket import (
            build_market_snapshots,
            enrich_market_snapshots_with_orderbooks,
        )

        gamma_data = [
            {
                "conditionId": "0xtest",
                "outcomePrices": "[0.60, 0.40]",
                "bestBid": "0.58",
                "clobTokenIds": '["yes_test", "no_test"]',
            }
        ]

        snapshots = build_market_snapshots(gamma_data)

        class FakeBookClient:
            def fetch_order_book(self, token_id: str):
                books = {
                    "yes_test": {"bids": [{"price": "0.59", "size": "500"}], "asks": [{"price": "0.62", "size": "400"}]},
                    "no_test": {"bids": [{"price": "0.38", "size": "600"}], "asks": [{"price": "0.42", "size": "350"}]},
                }
                return books.get(token_id, {})

        enriched = enrich_market_snapshots_with_orderbooks(snapshots, FakeBookClient())

        assert enriched["0xtest"].yes_bid == 0.59
        assert enriched["0xtest"].yes_ask == 0.62
        assert enriched["0xtest"].yes_spread > 0

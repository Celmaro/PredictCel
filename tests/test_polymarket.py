from datetime import UTC, datetime

from predictcel.polymarket import (
    build_wallet_trades,
    enrich_market_snapshots_with_orderbooks,
    market_snapshot_from_gamma,
    wallet_trade_from_data,
)


class FakeClient:
    def __init__(self, books):
        self.books = books

    def fetch_order_book(self, token_id: str):
        return self.books.get(token_id, {})


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

    enriched = enrich_market_snapshots_with_orderbooks({"cond_1": snapshot}, client)

    assert enriched["cond_1"].yes_bid == 0.59
    assert enriched["cond_1"].no_bid == 0.33
    assert enriched["cond_1"].yes_ask == 0.62
    assert enriched["cond_1"].no_ask == 0.36
    assert enriched["cond_1"].yes_ask_size == 150
    assert enriched["cond_1"].no_ask_size == 140
    assert enriched["cond_1"].yes_spread == 0.03
    assert enriched["cond_1"].no_spread == 0.03
    assert enriched["cond_1"].orderbook_ready is True


def test_wallet_trade_from_data_uses_outcome_and_timestamp() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    item = {
        "conditionId": "cond_1",
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


def test_build_wallet_trades_skips_wallets_without_topic_mapping() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    wallet_payloads = {
        "wallet_a": [{"conditionId": "cond_1", "outcome": "YES", "price": "0.54", "size": "250", "createdAt": "2025-12-31T23:50:00Z"}],
        "wallet_b": [{"conditionId": "cond_2", "outcome": "NO", "price": "0.44", "size": "150", "createdAt": "2025-12-31T23:55:00Z"}],
    }

    trades = build_wallet_trades(wallet_payloads, {"wallet_a": "sports"}, now)

    assert len(trades) == 1
    assert trades[0].wallet == "wallet_a"

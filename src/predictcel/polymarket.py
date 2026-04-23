from __future__ import annotations

import json
import time
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .models import MarketSnapshot, WalletTrade


class PolymarketPublicClient:
    def __init__(
        self,
        gamma_base_url: str = "https://gamma-api.polymarket.com",
        data_base_url: str = "https://data-api.polymarket.com",
        clob_base_url: str = "https://clob.polymarket.com",
        timeout_seconds: int = 15,
        max_retries: int = 3,
        retry_base_delay_seconds: float = 0.5,
    ) -> None:
        self.gamma_base_url = gamma_base_url.rstrip("/")
        self.data_base_url = data_base_url.rstrip("/")
        self.clob_base_url = clob_base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_retries = max(max_retries, 1)
        self.retry_base_delay_seconds = retry_base_delay_seconds

    def fetch_active_markets(self, limit: int) -> list[dict[str, Any]]:
        query = urlencode({"limit": limit, "closed": "false", "active": "true"})
        payload = self._get_json(f"{self.gamma_base_url}/markets?{query}")
        return _extract_list(payload)

    def fetch_leaderboard(self, limit: int) -> list[dict[str, Any]]:
        query = urlencode({"limit": limit})
        payload = self._get_json(f"{self.data_base_url}/v1/leaderboard?{query}")
        return _extract_list(payload)[:limit]

    def fetch_wallet_trades(self, wallet: str, limit: int) -> list[dict[str, Any]]:
        query = urlencode({"user": wallet, "limit": limit})
        payload = self._get_json(f"{self.data_base_url}/trades?{query}")
        return _extract_list(payload)

    def fetch_order_book(self, token_id: str) -> dict[str, Any]:
        query = urlencode({"token_id": token_id})
        payload = self._get_json(f"{self.clob_base_url}/book?{query}")
        return payload if isinstance(payload, dict) else {}

    def _get_json(self, url: str) -> Any:
        request = Request(url, headers={"User-Agent": "PredictCel/0.1"})
        for attempt in range(self.max_retries):
            try:
                with urlopen(request, timeout=self.timeout_seconds) as response:
                    return json.loads(response.read().decode("utf-8"))
            except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError):
                if attempt >= self.max_retries - 1:
                    raise
                time.sleep(self.retry_base_delay_seconds * (2 ** attempt))
        return None


def build_market_snapshots(items: list[dict[str, Any]]) -> dict[str, MarketSnapshot]:
    snapshots: dict[str, MarketSnapshot] = {}
    for item in items:
        snapshot = market_snapshot_from_gamma(item)
        if snapshot is not None:
            snapshots[snapshot.market_id] = snapshot
    return snapshots


def enrich_market_snapshots_with_orderbooks(
    snapshots: dict[str, MarketSnapshot],
    client: PolymarketPublicClient,
) -> dict[str, MarketSnapshot]:
    enriched: dict[str, MarketSnapshot] = {}
    for market_id, snapshot in snapshots.items():
        try:
            yes_book = client.fetch_order_book(snapshot.yes_token_id) if snapshot.yes_token_id else {}
            no_book = client.fetch_order_book(snapshot.no_token_id) if snapshot.no_token_id else {}
        except Exception:
            enriched[market_id] = snapshot
            continue

        yes_bid = _book_best_price(yes_book, "bids")
        no_bid = _book_best_price(no_book, "bids")
        yes_ask = _book_best_price(yes_book, "asks") or snapshot.yes_ask
        no_ask = _book_best_price(no_book, "asks") or snapshot.no_ask
        yes_ask_size = _book_best_size(yes_book, "asks")
        no_ask_size = _book_best_size(no_book, "asks")
        yes_spread = round(max(yes_ask - yes_bid, 0.0), 4) if yes_ask and yes_bid else 0.0
        no_spread = round(max(no_ask - no_bid, 0.0), 4) if no_ask and no_bid else 0.0

        enriched[market_id] = replace(
            snapshot,
            yes_ask=yes_ask,
            no_ask=no_ask,
            best_bid=max(yes_bid, no_bid, snapshot.best_bid),
            yes_bid=yes_bid,
            no_bid=no_bid,
            yes_ask_size=yes_ask_size,
            no_ask_size=no_ask_size,
            yes_spread=yes_spread,
            no_spread=no_spread,
            orderbook_ready=bool(yes_book or no_book),
        )
    return enriched


def build_wallet_trades(
    wallet_payloads: dict[str, list[dict[str, Any]]],
    topic_by_wallet: dict[str, str],
    now: datetime | None = None,
) -> list[WalletTrade]:
    now = now or datetime.now(UTC)
    trades: list[WalletTrade] = []
    for wallet, items in wallet_payloads.items():
        topic = topic_by_wallet.get(wallet)
        if topic is None:
            continue
        for item in items:
            trade = wallet_trade_from_data(wallet, topic, item, now)
            if trade is not None:
                trades.append(trade)
    return trades


def market_snapshot_from_gamma(item: dict[str, Any]) -> MarketSnapshot | None:
    market_id = str(item.get("conditionId") or item.get("id") or "").strip()
    if not market_id:
        return None

    prices = _parse_outcome_prices(item.get("outcomePrices"))
    if len(prices) < 2:
        prices = _parse_outcome_prices(item.get("outcomes"))
    yes_ask = float(prices[0]) if len(prices) > 0 else 0.0
    no_ask = float(prices[1]) if len(prices) > 1 else 0.0

    token_ids = _parse_token_ids(item)
    minutes_to_resolution = _minutes_to_resolution(item.get("endDate") or item.get("end_date"))
    title = str(item.get("question") or item.get("title") or market_id)
    topic = str(item.get("category") or item.get("tag") or item.get("seriesSlug") or "unknown")

    return MarketSnapshot(
        market_id=market_id,
        topic=topic,
        title=title,
        yes_ask=yes_ask,
        no_ask=no_ask,
        best_bid=float(item.get("bestBid") or item.get("best_bid") or 0.0),
        liquidity_usd=float(item.get("liquidity") or item.get("liquidityNum") or 0.0),
        minutes_to_resolution=minutes_to_resolution,
        yes_token_id=token_ids[0] if len(token_ids) > 0 else "",
        no_token_id=token_ids[1] if len(token_ids) > 1 else "",
    )


def wallet_trade_from_data(
    wallet: str,
    topic: str,
    item: dict[str, Any],
    now: datetime,
) -> WalletTrade | None:
    market_id = str(item.get("conditionId") or item.get("market_id") or item.get("marketId") or item.get("market") or "").strip()
    side = _trade_side(item)
    if not market_id or side not in {"YES", "NO"}:
        return None

    timestamp = item.get("timestamp") or item.get("createdAt") or item.get("created_at")
    age_seconds = _age_seconds(timestamp, now)

    price = float(item.get("price") or 0.0)
    size_usd = float(item.get("size") or item.get("sizeUsd") or item.get("amount") or 0.0)

    return WalletTrade(
        wallet=wallet,
        topic=topic,
        market_id=market_id,
        side=side,
        price=price,
        size_usd=size_usd,
        age_seconds=age_seconds,
    )


def _extract_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("data", "markets", "trades", "users", "leaderboard", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _trade_side(item: dict[str, Any]) -> str:
    raw_side = str(item.get("outcome") or item.get("position") or "").upper().strip()
    if raw_side in {"YES", "NO"}:
        return raw_side
    outcome_index = item.get("outcomeIndex")
    if outcome_index == 0:
        return "YES"
    if outcome_index == 1:
        return "NO"
    return raw_side


def _parse_outcome_prices(value: Any) -> list[float]:
    if isinstance(value, list):
        return [float(item) for item in value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return [float(item) for item in parsed]
    return []


def _parse_token_ids(item: dict[str, Any]) -> list[str]:
    raw = item.get("clobTokenIds") or item.get("tokenIds")
    if isinstance(raw, list):
        return [str(token) for token in raw[:2]]
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            return [str(token) for token in parsed[:2]]
    tokens = item.get("tokens")
    if isinstance(tokens, list):
        result = []
        for token in tokens[:2]:
            if isinstance(token, dict):
                token_id = token.get("token_id") or token.get("id") or token.get("tokenId")
                if token_id:
                    result.append(str(token_id))
        return result
    return []


def _book_best_price(book: dict[str, Any], side: str) -> float:
    levels = book.get(side)
    if not isinstance(levels, list) or not levels:
        return 0.0
    best = levels[0]
    if not isinstance(best, dict):
        return 0.0
    return float(best.get("price") or 0.0)


def _book_best_size(book: dict[str, Any], side: str) -> float:
    levels = book.get(side)
    if not isinstance(levels, list) or not levels:
        return 0.0
    best = levels[0]
    if not isinstance(best, dict):
        return 0.0
    return float(best.get("size") or 0.0)


def _minutes_to_resolution(value: Any) -> int:
    if not value:
        return 0
    try:
        resolution = _parse_datetime(value)
    except ValueError:
        return 0
    delta = resolution - datetime.now(UTC)
    return max(int(delta.total_seconds() // 60), 0)


def _age_seconds(value: Any, now: datetime) -> int:
    if value is None:
        return 10**9
    try:
        dt = _parse_datetime(value)
    except ValueError:
        return 10**9
    return max(int((now - dt).total_seconds()), 0)


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=UTC)
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).astimezone(UTC)
    raise ValueError("Unsupported datetime value")

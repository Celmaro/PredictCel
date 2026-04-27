import asyncio
import json
import logging
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import aiohttp
from .models import MarketSnapshot, WalletTrade

logger = logging.getLogger(__name__)

# Thread-local storage for metrics to avoid global mutable state
_metrics_local = threading.local()


def _get_metrics() -> dict:
    """Get thread-local metrics."""
    if not hasattr(_metrics_local, "metrics"):
        _metrics_local.metrics = {"requests": 0, "errors": 0}
    return _metrics_local.metrics


class CircuitBreaker:
    """Circuit breaker pattern for resilient API calls."""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self._lock = threading.Lock()
    
    def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection."""
        with self._lock:
            if self.state == "OPEN":
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    self.state = "HALF_OPEN"
                else:
                    raise Exception("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e
    
    def _on_success(self):
        """Reset failure count on success."""
        with self._lock:
            self.failure_count = 0
            self.state = "CLOSED"
    
    def _on_failure(self):
        """Increment failure count and open circuit if threshold reached."""
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"


class PolymarketPublicClient:
    """Client for Polymarket public APIs with caching and resilience."""
    
    # Configurable constants
    DEFAULT_Z_THRESHOLD = 3.0
    DEFAULT_CACHE_TTL_SECONDS = 300
    
    def __init__(
        self,
        gamma_base_url: str = "https://gamma-api.polymarket.com",
        data_base_url: str = "https://data-api.polymarket.com",
        clob_base_url: str = "https://clob.polymarket.com",
        timeout_seconds: int = 15,
        max_retries: int = 3,
        retry_base_delay_seconds: float = 0.5,
        use_redis: bool = False,
        redis_host: str = "localhost",
        redis_port: int = 6379,
    ) -> None:
        """Initialize Polymarket client.
        
        Args:
            gamma_base_url: Gamma API base URL
            data_base_url: Data API base URL
            clob_base_url: CLOB API base URL
            timeout_seconds: Request timeout in seconds
            max_retries: Maximum number of retries for failed requests
            retry_base_delay_seconds: Base delay between retries
            use_redis: Whether to use Redis for caching (falls back to memory if unavailable)
            redis_host: Redis host
            redis_port: Redis port
        """
        self.gamma_base_url = gamma_base_url.rstrip("/")
        self.data_base_url = data_base_url.rstrip("/")
        self.clob_base_url = clob_base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_retries = max(max_retries, 1)
        self.retry_base_delay_seconds = retry_base_delay_seconds
        self.use_redis = use_redis
        self.redis_client = None
        self._request_cache: dict[str, Any] = {}
        self._cache_lock = threading.Lock()
        self._circuit_breaker = CircuitBreaker()
        
        # Initialize Redis if requested, with graceful fallback
        if use_redis:
            try:
                import redis
                self.redis_client = redis.Redis(
                    host=redis_host, 
                    port=redis_port, 
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5
                )
                # Test connection
                self.redis_client.ping()
                logger.info("Redis caching enabled")
            except (ModuleNotFoundError, ImportError):
                logger.warning("Redis package not installed, falling back to in-memory cache")
                self.use_redis = False
            except Exception as e:
                logger.warning(f"Redis connection failed ({e}), falling back to in-memory cache")
                self.use_redis = False
                self.redis_client = None
    
    def fetch_active_markets(self, limit: int) -> list[dict[str, Any]]:
        """Fetch active markets from Gamma API."""
        query = urlencode({"limit": limit, "closed": "false", "active": "true"})
        payload = self._get_json(f"{self.gamma_base_url}/markets?{query}")
        return _extract_list(payload)
    
    def fetch_markets_by_condition_ids(self, condition_ids: list[str], chunk_size: int = 25) -> list[dict[str, Any]]:
        """Fetch markets by condition IDs."""
        return self._fetch_markets_by_array_filter("condition_ids", condition_ids, chunk_size)
    
    def fetch_markets_by_clob_token_ids(self, token_ids: list[str], chunk_size: int = 25) -> list[dict[str, Any]]:
        """Fetch markets by CLOB token IDs."""
        return self._fetch_markets_by_array_filter("clob_token_ids", token_ids, chunk_size)
    
    def fetch_market_by_slug(self, slug: str) -> dict[str, Any]:
        """Fetch market by slug."""
        normalized_slug = str(slug).strip()
        if not normalized_slug:
            return {}
        payload = self._get_json(f"{self.gamma_base_url}/markets/slug/{quote(normalized_slug, safe='')}")
        return payload if isinstance(payload, dict) else {}
    
    def fetch_markets_by_slugs(self, slugs: list[str], max_workers: int = 8) -> list[dict[str, Any]]:
        """Fetch markets by slugs with parallel execution."""
        unique_slugs = sorted({str(slug).strip() for slug in slugs if str(slug).strip()})
        if not unique_slugs:
            return []
        
        results: list[dict[str, Any]] = []
        seen_market_keys: set[str] = set()
        workers = max(1, min(max_workers, len(unique_slugs)))
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(self.fetch_market_by_slug, slug): slug for slug in unique_slugs}
            for future in as_completed(futures):
                try:
                    payload = future.result()
                except Exception as e:
                    logger.debug(f"Failed to fetch market for slug {futures[future]}: {e}")
                    continue
                
                rows = _extract_list(payload)
                if not rows and payload:
                    rows = [payload]
                
                for row in rows:
                    key = str(row.get("conditionId") or row.get("condition_id") or row.get("id") or row.get("slug") or row).strip()
                    if key in seen_market_keys:
                        continue
                    seen_market_keys.add(key)
                    results.append(row)
        
        return results
    
    def fetch_markets_by_identifiers(self, identifiers: list[str], chunk_size: int = 25) -> list[dict[str, Any]]:
        """Fetch markets by various identifiers."""
        results: list[dict[str, Any]] = []
        seen_market_keys: set[str] = set()
        
        for rows in (
            self.fetch_markets_by_condition_ids(identifiers, chunk_size),
            self.fetch_markets_by_clob_token_ids(identifiers, chunk_size),
        ):
            for row in rows:
                key = str(row.get("conditionId") or row.get("condition_id") or row.get("id") or row).strip()
                if key in seen_market_keys:
                    continue
                seen_market_keys.add(key)
                results.append(row)
        
        return results
    
    def fetch_leaderboard(self, limit: int) -> list[dict[str, Any]]:
        """Fetch leaderboard from data API."""
        query = urlencode({"limit": limit})
        payload = self._get_json(f"{self.data_base_url}/v1/leaderboard?{query}")
        return _extract_list(payload)[:limit]
    
    def fetch_wallet_trades(self, wallet: str, limit: int) -> list[dict[str, Any]]:
        """Fetch trades for a wallet with fallback endpoints.
        
        Tries multiple endpoint variations and returns best results.
        Logs warnings if all endpoints fail.
        """
        request_variants = [
            (f"{self.data_base_url}/v1/trades", {"user": wallet, "limit": limit}),
            (f"{self.data_base_url}/v1/trades", {"address": wallet, "limit": limit}),
            (f"{self.data_base_url}/trades", {"user": wallet, "limit": limit}),
            (f"{self.data_base_url}/trades", {"address": wallet, "limit": limit}),
            (f"{self.data_base_url}/activity", {"user": wallet, "limit": limit}),
            (f"{self.data_base_url}/activity", {"address": wallet, "limit": limit}),
        ]
        
        wallet_key = wallet.lower()
        best_rows: list[dict[str, Any]] = []
        best_scored_rows: list[dict[str, Any]] = []
        errors: list[str] = []
        
        for base_url, params in request_variants:
            query = urlencode(params)
            try:
                payload = self._get_json(f"{base_url}?{query}")
            except Exception as e:
                errors.append(f"{base_url}: {type(e).__name__}")
                continue
            
            rows = _extract_list(payload)
            if not rows:
                continue
            
            normalized = [_flatten_trade_payload(item) for item in rows]
            matching_rows = [item for item in normalized if _trade_matches_wallet(item, wallet_key)]
            candidate_rows = matching_rows or normalized
            
            if _score_trade_rows(candidate_rows) > _score_trade_rows(best_scored_rows):
                best_scored_rows = candidate_rows
                best_rows = candidate_rows
            elif len(candidate_rows) > len(best_rows):
                best_rows = candidate_rows
            
            if matching_rows and _count_trade_items_with_ids(matching_rows) >= min(limit, len(matching_rows)):
                break
        
        # Log warning if all endpoints failed
        if not best_rows and errors:
            logger.warning(
                f"All API endpoints failed for wallet {wallet[:10]}... "
                f"Errors: {errors[:3]}"
            )
        
        return best_rows[:limit]
    
    def fetch_order_book(self, token_id: str) -> dict[str, Any]:
        """Fetch order book for a token."""
        query = urlencode({"token_id": token_id})
        payload = self._get_json(f"{self.clob_base_url}/book?{query}")
        return payload if isinstance(payload, dict) else {}
    
    async def subscribe_market_updates(self, market_ids: list[str], callback: callable) -> None:
        """Subscribe to real-time market updates via WebSocket."""
        ws_url = "wss://ws.polymarket.com"
        logger.info(f"Connecting to WebSocket at {ws_url} for {len(market_ids)} markets")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(ws_url) as ws:
                    logger.info("WebSocket connected successfully")
                    subscribe_msg = {"type": "subscribe", "markets": market_ids}
                    await ws.send_json(subscribe_msg)
                    logger.info(f"Subscribed to market updates for {market_ids}")
                    
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = msg.json()
                            if data.get("type") == "update":
                                logger.debug(f"Received market update: {data}")
                                callback(data)
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logger.error("WebSocket error encountered")
                            break
                        elif msg.type == aiohttp.WSMsgType.CLOSED:
                            logger.info("WebSocket connection closed")
                            break
        except aiohttp.ClientError as e:
            logger.error(f"WebSocket client error: {e}")
            raise
        except asyncio.TimeoutError:
            logger.error("WebSocket connection timeout")
            raise
        except Exception as e:
            logger.error(f"WebSocket unexpected error: {e}")
            raise
    
    def _fetch_markets_by_array_filter(self, filter_name: str, values: list[str], chunk_size: int) -> list[dict[str, Any]]:
        """Fetch markets using array filter with chunking."""
        results: list[dict[str, Any]] = []
        unique_values = sorted({value.strip() for value in values if value and value.strip()})
        
        for chunk in _chunks(unique_values, max(chunk_size, 1)):
            query = urlencode({filter_name: chunk, "limit": len(chunk)}, doseq=True)
            try:
                payload = self._get_json(f"{self.gamma_base_url}/markets?{query}")
            except Exception as e:
                logger.debug(f"Failed to fetch markets with {filter_name}: {e}")
                continue
            results.extend(_extract_list(payload))
        
        return results
    
    def _get_json(self, url: str) -> Any:
        """Make HTTP GET request with caching and retries."""
        def _fetch_url():
            metrics = _get_metrics()
            metrics["requests"] += 1
            
            # Check cache
            cached = self._get_cached(url)
            if cached is not None:
                return cached
            
            request = Request(url, headers={"User-Agent": "PredictCel/0.1"})
            
            for attempt in range(self.max_retries):
                try:
                    with urlopen(request, timeout=self.timeout_seconds) as response:
                        payload = json.loads(response.read().decode("utf-8"))
                        self._validate_response(payload, url)
                        self._set_cached(url, payload)
                        return payload
                except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError) as e:
                    metrics["errors"] += 1
                    if attempt >= self.max_retries - 1:
                        raise
                    delay = _retry_delay(self.retry_base_delay_seconds, attempt)
                    time.sleep(delay)
            
            return None
        
        return self._circuit_breaker.call(_fetch_url)
    
    def _get_cached(self, url: str) -> Any:
        """Get cached response if available and not expired."""
        if self.use_redis and self.redis_client:
            try:
                cached = self.redis_client.get(url)
                if cached:
                    try:
                        payload = json.loads(cached)
                        if isinstance(payload, dict) and payload.get("_cached_at"):
                            if time.time() - payload["_cached_at"] < self.DEFAULT_CACHE_TTL_SECONDS:
                                return {k: v for k, v in payload.items() if k != "_cached_at"}
                        elif not isinstance(payload, dict):
                            return payload
                    except json.JSONDecodeError:
                        pass
            except Exception as e:
                logger.debug(f"Redis get failed: {e}")
        
        with self._cache_lock:
            if url in self._request_cache:
                cached = self._request_cache[url]
                if isinstance(cached, dict) and cached.get("_cached_at"):
                    if time.time() - cached["_cached_at"] < self.DEFAULT_CACHE_TTL_SECONDS:
                        return {k: v for k, v in cached.items() if k != "_cached_at"}
                elif not isinstance(cached, dict):
                    return cached
        
        return None
    
    def _set_cached(self, url: str, payload: Any) -> None:
        """Cache response with TTL."""
        if self.use_redis and self.redis_client:
            try:
                cache_data = payload.copy() if isinstance(payload, dict) else payload
                if isinstance(cache_data, dict):
                    cache_data["_cached_at"] = time.time()
                self.redis_client.setex(url, self.DEFAULT_CACHE_TTL_SECONDS, json.dumps(cache_data))
                return
            except Exception as e:
                logger.debug(f"Redis set failed: {e}")
        
        with self._cache_lock:
            cache_data = payload.copy() if isinstance(payload, dict) else payload
            if isinstance(cache_data, dict):
                cache_data["_cached_at"] = time.time()
            self._request_cache[url] = cache_data
    
    def _validate_response(self, payload: Any, url: str) -> None:
        """Validate API response with anomaly detection."""
        if not isinstance(payload, (dict, list)):
            raise ValueError(f"Invalid response type for {url}: {type(payload)}")
        
        if isinstance(payload, list):
            prices = []
            for item in payload:
                if isinstance(item, dict):
                    yes_ask = item.get("yes", item.get("yesPrice"))
                    no_ask = item.get("no", item.get("noPrice"))
                    if yes_ask is not None and isinstance(yes_ask, (int, float)):
                        prices.append(yes_ask)
                    if no_ask is not None and isinstance(no_ask, (int, float)):
                        prices.append(no_ask)
            
            if prices:
                import statistics
                mean_price = statistics.mean(prices)
                stdev_price = statistics.stdev(prices) if len(prices) > 1 else 0
                
                anomalies = 0
                for price in prices:
                    if stdev_price > 0:
                        z_score = abs(price - mean_price) / stdev_price
                        if z_score > self.DEFAULT_Z_THRESHOLD:
                            anomalies += 1
                
                if anomalies > 0:
                    logger.warning(f"Detected {anomalies} anomalous prices in response from {url}")
        
        if "markets" in url and isinstance(payload, dict):
            data = payload.get("data", payload)
            if isinstance(data, list) and data:
                sample = data[0]
                has_market_identifier = any(field in sample for field in ("conditionId", "condition_id", "id"))
                has_direct_prices = all(field in sample for field in ("yes", "no"))
                has_outcome_prices = any(field in sample for field in ("outcomePrices", "outcomes"))
                
                if not has_market_identifier or not (has_direct_prices or has_outcome_prices):
                    logger.error(f"Unexpected market data shape from {url}")
                    raise ValueError("Unexpected market data shape")


def build_market_snapshots(items: list[dict[str, Any]]) -> dict[str, MarketSnapshot]:
    """Build market snapshots from Gamma API items."""
    snapshots: dict[str, MarketSnapshot] = {}
    for item in items:
        snapshot = market_snapshot_from_gamma(item)
        if snapshot is None:
            continue
        for market_id in _market_snapshot_aliases(item, snapshot):
            snapshots.setdefault(market_id, snapshot)
    return snapshots


def enrich_market_snapshots_with_orderbooks(
    snapshots: dict[str, MarketSnapshot],
    client: PolymarketPublicClient,
    max_workers: int = 8,
) -> dict[str, MarketSnapshot]:
    """Enrich market snapshots with order book data."""
    if not snapshots:
        return {}
    
    unique_snapshots: dict[str, MarketSnapshot] = {}
    aliases_by_canonical: dict[str, list[str]] = {}
    
    for alias, snapshot in snapshots.items():
        unique_snapshots.setdefault(snapshot.market_id, snapshot)
        aliases_by_canonical.setdefault(snapshot.market_id, []).append(alias)
    
    enriched_unique: dict[str, MarketSnapshot] = {}
    workers = max(1, min(max_workers, len(unique_snapshots)))
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_enrich_one_snapshot, snapshot, client): market_id for market_id, snapshot in unique_snapshots.items()}
        for future in as_completed(futures):
            market_id = futures[future]
            try:
                enriched_unique[market_id] = future.result()
            except Exception as e:
                logger.debug(f"Failed to enrich snapshot {market_id}: {e}")
                enriched_unique[market_id] = unique_snapshots[market_id]
    
    enriched: dict[str, MarketSnapshot] = {}
    for canonical_id, aliases in aliases_by_canonical.items():
        snapshot = enriched_unique.get(canonical_id, unique_snapshots[canonical_id])
        for alias in aliases:
            enriched[alias] = snapshot
    
    return enriched


def _enrich_one_snapshot(snapshot: MarketSnapshot, client: PolymarketPublicClient) -> MarketSnapshot:
    """Enrich a single snapshot with order book data."""
    yes_book = client.fetch_order_book(snapshot.yes_token_id) if snapshot.yes_token_id else {}
    no_book = client.fetch_order_book(snapshot.no_token_id) if snapshot.no_token_id else {}
    
    yes_bid = _book_best_price(yes_book, "bids")
    no_bid = _book_best_price(no_book, "bids")
    yes_ask = _book_best_price(yes_book, "asks") or snapshot.yes_ask
    no_ask = _book_best_price(no_book, "asks") or snapshot.no_ask
    
    yes_ask_size = _book_best_size(yes_book, "asks")
    no_ask_size = _book_best_size(no_book, "asks")
    
    yes_spread = round(max(yes_ask - yes_bid, 0.0), 4) if yes_ask and yes_bid else 0.0
    no_spread = round(max(no_ask - no_bid, 0.0), 4) if no_ask and no_bid else 0.0
    
    # Calculate best_bid from order book data
    best_bid = max(yes_bid, no_bid, snapshot.best_bid)
    
    return replace(
        snapshot,
        yes_ask=yes_ask,
        no_ask=no_ask,
        best_bid=best_bid,
        yes_bid=yes_bid,
        no_bid=no_bid,
        yes_ask_size=yes_ask_size,
        no_ask_size=no_ask_size,
        yes_spread=yes_spread,
        no_spread=no_spread,
        orderbook_ready=bool(yes_book or no_book),
    )


def build_wallet_trades(
    wallet_payloads: dict[str, list[dict[str, Any]]],
    topic_by_wallet: dict[str, str | list[str] | tuple[str, ...] | set[str]],
    now: datetime | None = None,
) -> list[WalletTrade]:
    """Build wallet trades from API payloads."""
    now = now or datetime.now(UTC)
    trades: list[WalletTrade] = []
    
    for wallet, items in wallet_payloads.items():
        topics = _normalize_topics(topic_by_wallet.get(wallet))
        if not topics:
            continue
        
        for item in items:
            for topic in topics:
                trade = wallet_trade_from_data(wallet, topic, item, now)
                if trade is not None:
                    trades.append(trade)
    
    return trades


def extract_trade_market_ids(wallet_payloads: dict[str, list[dict[str, Any]]]) -> list[str]:
    """Extract unique market IDs from trade payloads."""
    market_ids: set[str] = set()
    for items in wallet_payloads.values():
        for item in items:
            market_id = _trade_market_id(item)
            if market_id:
                market_ids.add(market_id)
    return sorted(market_ids)


def extract_trade_market_slugs(wallet_payloads: dict[str, list[dict[str, Any]]]) -> list[str]:
    """Extract unique market slugs from trade payloads."""
    market_slugs: set[str] = set()
    for items in wallet_payloads.values():
        for item in items:
            market_slug = _trade_market_slug(item)
            if market_slug:
                market_slugs.add(market_slug)
    return sorted(market_slugs)


def market_snapshot_from_gamma(item: dict[str, Any]) -> MarketSnapshot | None:
    """Create MarketSnapshot from Gamma API item."""
    market_id = str(item.get("conditionId") or item.get("condition_id") or item.get("id") or "").strip()
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
    
    # Calculate best_bid from order book if available, otherwise use API value
    best_bid_api = float(item.get("bestBid") or item.get("best_bid") or 0.0)
    
    return MarketSnapshot(
        market_id=market_id,
        topic=topic,
        title=title,
        yes_ask=yes_ask,
        no_ask=no_ask,
        best_bid=best_bid_api,  # Will be enriched with order book data later
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
    """Create WalletTrade from API item."""
    market_id = _trade_market_id(item)
    side = _trade_side(item)
    
    if not market_id or side not in {"YES", "NO"}:
        return None
    
    timestamp = (
        item.get("timestamp")
        or item.get("createdAt")
        or item.get("created_at")
        or item.get("timeStamp")
        or item.get("updatedAt")
    )
    age_seconds = _age_seconds(timestamp, now)
    
    price = float(
        item.get("price")
        or item.get("matchedPrice")
        or item.get("avgPrice")
        or item.get("lastPrice")
        or 0.0
    )
    
    size_usd = float(
        item.get("size")
        or item.get("sizeUsd")
        or item.get("usdc_size")
        or item.get("amount")
        or item.get("amountUsd")
        or item.get("notional")
        or 0.0
    )
    
    return WalletTrade(
        wallet=wallet,
        topic=topic,
        market_id=market_id,
        side=side,
        price=price,
        size_usd=size_usd,
        age_seconds=age_seconds,
    )


# Helper functions (unchanged from original)
def _extract_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("data", "markets", "trades", "users", "leaderboard", "results", "history", "activity"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _trade_market_id(item: dict[str, Any]) -> str:
    for key in (
        "conditionId", "condition_id", "conditionID", "condition",
        "market_id", "marketId", "market", "asset", "asset_id",
        "tokenID", "tokenId", "token_id", "clobTokenId", "clob_token_id",
    ):
        value = item.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _trade_market_slug(item: dict[str, Any]) -> str:
    for key in ("marketSlug", "slug"):
        value = item.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    
    nested_market = item.get("market")
    if isinstance(nested_market, dict):
        value = nested_market.get("slug") or nested_market.get("marketSlug")
        if value is not None and str(value).strip():
            return str(value).strip()
    
    return ""


def _market_snapshot_aliases(item: dict[str, Any], snapshot: MarketSnapshot) -> list[str]:
    aliases = [snapshot.market_id]
    for key in ("conditionId", "condition_id", "conditionID", "condition", "id", "market_id", "marketId", "market", "slug", "marketSlug"):
        value = item.get(key)
        if value is not None and str(value).strip():
            aliases.append(str(value).strip())
    aliases.extend(token_id for token_id in (snapshot.yes_token_id, snapshot.no_token_id) if token_id)
    return list(dict.fromkeys(aliases))


def _normalize_topics(value: str | list[str] | tuple[str, ...] | set[str] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, set):
        return sorted(str(item) for item in value if str(item).strip())
    
    normalized: list[str] = []
    for item in value:
        topic = str(item).strip()
        if topic and topic not in normalized:
            normalized.append(topic)
    return normalized


def _trade_side(item: dict[str, Any]) -> str:
    for key in ("outcome", "position", "side", "tradeSide"):
        raw_value = item.get(key)
        if raw_value is None:
            continue
        raw_side = str(raw_value).upper().strip()
        if raw_side in {"YES", "NO"}:
            return raw_side
        if raw_side in {"BUY_YES", "LONG_YES", "BID_YES"}:
            return "YES"
        if raw_side in {"BUY_NO", "LONG_NO", "BID_NO"}:
            return "NO"
    
    outcome_index = item.get("outcomeIndex")
    if outcome_index == 0:
        return "YES"
    if outcome_index == 1:
        return "NO"
    
    return ""


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


def _flatten_trade_payload(item: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(item)
    
    nested_market = item.get("market")
    if isinstance(nested_market, dict):
        for source_key, target_key in (
            ("conditionId", "conditionId"),
            ("condition_id", "condition_id"),
            ("slug", "marketSlug"),
            ("question", "question"),
            ("title", "title"),
        ):
            if target_key not in normalized and nested_market.get(source_key) is not None:
                normalized[target_key] = nested_market.get(source_key)
    
    nested_asset = item.get("asset")
    if isinstance(nested_asset, dict):
        for source_key, target_key in (("id", "asset"), ("tokenId", "tokenId"), ("token_id", "token_id")):
            if target_key not in normalized and nested_asset.get(source_key) is not None:
                normalized[target_key] = nested_asset.get(source_key)
    
    return normalized


def _count_trade_items_with_ids(items: list[dict[str, Any]]) -> int:
    return sum(1 for item in items if _trade_market_id(item))


def _score_trade_rows(items: list[dict[str, Any]]) -> tuple[int, int]:
    return (_count_trade_items_with_ids(items), len(items))


def _trade_matches_wallet(item: dict[str, Any], wallet: str) -> bool:
    owner = _trade_wallet_address(item)
    if not owner:
        return False
    return owner == wallet


def _trade_wallet_address(item: dict[str, Any]) -> str:
    for key in ("user", "userAddress", "wallet", "walletAddress", "address", "proxyWallet", "proxy_wallet", "maker", "taker"):
        value = item.get(key)
        if value is not None and str(value).strip():
            return str(value).strip().lower()
    
    profile = item.get("profile")
    if isinstance(profile, dict):
        for key in ("address", "wallet", "walletAddress", "proxyWallet"):
            value = profile.get(key)
            if value is not None and str(value).strip():
                return str(value).strip().lower()
    
    return ""


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


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def _retry_delay(base_delay: float, attempt: int) -> float:
    return base_delay * (2 ** attempt) * random.uniform(0.5, 1.5)

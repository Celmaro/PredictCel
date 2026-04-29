"""Wallet source definitions.

Defines sources for wallet discovery and their configurations.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .polymarket import PolymarketPublicClient

__all__ = [
    "CuratedWalletFileSource",
    "DataApiWalletSource",
    "DataApiMarketTradesWalletSource",
    "extract_wallet_address",
]


class DataApiWalletSource:
    def __init__(self, client: PolymarketPublicClient) -> None:
        self.client = client

    def fetch_candidates(self, limit: int) -> list[dict[str, Any]]:
        raw_items = self.client.fetch_leaderboard(limit)
        candidates: list[dict[str, Any]] = []
        for item in raw_items:
            address = extract_wallet_address(item)
            if not address:
                continue
            candidates.append(
                {
                    "address": address.lower(),
                    "source": "polymarket_data_api",
                    "raw": item,
                }
            )
        return candidates

    def fetch_wallet_trades(self, address: str, limit: int) -> list[dict[str, Any]]:
        return self.client.fetch_wallet_trades(address, limit)


class DataApiMarketTradesWalletSource:
    def __init__(self, client: PolymarketPublicClient, market_ids: list[str]) -> None:
        self.client = client
        self.market_ids = [
            str(market_id).strip() for market_id in market_ids if str(market_id).strip()
        ]

    def fetch_candidates(self, limit: int) -> list[dict[str, Any]]:
        if not self.market_ids or limit <= 0:
            return []

        raw_items = self.client.fetch_market_trades(self.market_ids, limit)
        candidates: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in raw_items:
            address = extract_wallet_address(item).lower()
            if not address or address in seen:
                continue
            seen.add(address)
            candidates.append(
                {
                    "address": address,
                    "source": "polymarket_data_api_market_trades",
                    "raw": item,
                }
            )
            if len(candidates) >= limit:
                break
        return candidates

    def fetch_wallet_trades(self, address: str, limit: int) -> list[dict[str, Any]]:
        return self.client.fetch_wallet_trades(address, limit)


class CuratedWalletFileSource:
    def __init__(
        self, client: PolymarketPublicClient, wallet_candidates_path: str | Path
    ) -> None:
        self.client = client
        self.wallet_candidates_path = Path(wallet_candidates_path)

    def fetch_candidates(self, limit: int) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        payload = json.loads(self.wallet_candidates_path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            return []

        candidates: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in payload:
            if not isinstance(item, dict):
                continue
            address = extract_wallet_address(item).lower()
            if not address or address in seen:
                continue
            seen.add(address)
            candidates.append(
                {
                    "address": address,
                    "source": str(item.get("source") or "curated_wallet_file"),
                    "raw": item,
                }
            )
            if len(candidates) >= limit:
                break
        return candidates

    def fetch_wallet_trades(self, address: str, limit: int) -> list[dict[str, Any]]:
        return self.client.fetch_wallet_trades(address, limit)


def extract_wallet_address(item: dict[str, Any]) -> str:
    for key in (
        "address",
        "wallet",
        "walletAddress",
        "user",
        "userAddress",
        "proxyWallet",
        "proxy_wallet",
    ):
        value = item.get(key)
        if value:
            return str(value).strip()
    profile = item.get("profile")
    if isinstance(profile, dict):
        for key in ("address", "proxyWallet", "wallet"):
            value = profile.get(key)
            if value:
                return str(value).strip()
    return ""

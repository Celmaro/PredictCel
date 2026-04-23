from __future__ import annotations

from typing import Any

from .polymarket import PolymarketPublicClient


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
            candidates.append({"address": address.lower(), "source": "polymarket_data_api", "raw": item})
        return candidates

    def fetch_wallet_trades(self, address: str, limit: int) -> list[dict[str, Any]]:
        return self.client.fetch_wallet_trades(address, limit)


def extract_wallet_address(item: dict[str, Any]) -> str:
    for key in ("address", "wallet", "walletAddress", "user", "userAddress", "proxyWallet", "proxy_wallet"):
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

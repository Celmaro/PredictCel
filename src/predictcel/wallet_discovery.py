"""Wallet discovery from external sources.

Discovers high-performing wallets from various sources including
Polymarket leaderboards and external analytics.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
import json

from .basket_assignment import BasketAssignmentEngine
from .basket_manager import BasketManagerPlanner
from .config import AppConfig
from .models import BasketAssignment, BasketManagerAction, WalletDiscoveryCandidate
from .polymarket import PolymarketPublicClient, extract_trade_market_ids
from .wallet_sources import CuratedWalletFileSource, DataApiMarketTradesWalletSource, DataApiWalletSource
from .wallet_topics import classify_wallet_topics

__all__ = ["WalletDiscoveryPipeline"]



class WalletDiscoveryPipeline:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        live_data = config.live_data
        self.client = PolymarketPublicClient(
            gamma_base_url=live_data.gamma_base_url if live_data else "https://gamma-api.polymarket.com",
            data_base_url=live_data.data_base_url if live_data else "https://data-api.polymarket.com",
            clob_base_url=live_data.clob_base_url if live_data else "https://clob.polymarket.com",
            timeout_seconds=live_data.request_timeout_seconds if live_data else 15,
        )
        self.source = self._build_source()
        self.assignment_engine = BasketAssignmentEngine(config.wallet_discovery)
        self.current_wallets_by_topic = {
            basket.topic: {wallet.lower() for wallet in basket.wallets}
            for basket in config.baskets
        }
        self.manager = BasketManagerPlanner(
            config,
            current_wallets_by_topic=self.current_wallets_by_topic,
        )

    def set_current_wallets_by_topic(
        self,
        current_wallets_by_topic: dict[str, set[str] | list[str]],
    ) -> None:
        self.current_wallets_by_topic = {
            str(topic): {str(wallet).lower() for wallet in wallets}
            for topic, wallets in current_wallets_by_topic.items()
        }
        self.manager = BasketManagerPlanner(
            self.config,
            current_wallets_by_topic=self.current_wallets_by_topic,
        )

    def run(self) -> tuple[list[WalletDiscoveryCandidate], list[BasketAssignment], list[BasketManagerAction]]:
        raw_candidates = self.source.fetch_candidates(self.config.wallet_discovery.candidate_limit)
        existing = self._existing_wallets()
        candidates: list[WalletDiscoveryCandidate] = []
        manager_candidates: list[WalletDiscoveryCandidate] = []

        seen: set[str] = set()
        for raw in raw_candidates:
            address = raw["address"].lower()
            if address in seen:
                continue
            seen.add(address)
            candidate = self._build_candidate(address, raw.get("source", "unknown"), self._safe_fetch_trades(address))
            manager_candidates.append(candidate)
            if self.config.wallet_discovery.exclude_existing_wallets and address in existing:
                continue
            candidates.append(candidate)

        for address in sorted(existing):
            if address in seen:
                continue
            seen.add(address)
            manager_candidates.append(self._build_candidate(address, "current_basket", self._safe_fetch_trades(address)))

        accepted = [candidate for candidate in manager_candidates if not candidate.rejected_reasons]
        assignments = [self.assignment_engine.assign(candidate) for candidate in accepted]
        actions = self.manager.plan(assignments)
        return candidates, assignments, actions

    def write_reports(
        self,
        output_dir: str | Path,
        config_path: str | Path | None = None,
        config_output_path: str | Path | None = None,
        results: tuple[
            list[WalletDiscoveryCandidate],
            list[BasketAssignment],
            list[BasketManagerAction],
        ] | None = None,
    ) -> dict[str, str]:
        if results is None:
            candidates, assignments, actions = self.run()
        else:
            candidates, assignments, actions = results
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        files = {
            "wallet_discovery_report": output_path / "wallet_discovery_report.json",
            "basket_assignments": output_path / "basket_assignments.json",
            "basket_manager_plan": output_path / "basket_manager_plan.json",
        }
        self._write_json(files["wallet_discovery_report"], [asdict(item) for item in candidates])
        self._write_json(files["basket_assignments"], [asdict(item) for item in assignments])
        self._write_json(files["basket_manager_plan"], [asdict(item) for item in actions])

        mutation_path = self._write_mutated_config_if_enabled(output_path, actions, config_path, config_output_path)
        if mutation_path is not None:
            key = "updated_config" if self.config.wallet_discovery.mode == "auto_update" else "config_proposal"
            files[key] = mutation_path
        return {name: str(path) for name, path in files.items()}

    def build_mutated_config(self, payload: dict[str, Any], actions: list[BasketManagerAction]) -> dict[str, Any]:
        updated = deepcopy(payload)
        baskets = updated.get("baskets")
        if not isinstance(baskets, list):
            return updated

        baskets_by_topic = {str(item.get("topic")): item for item in baskets if isinstance(item, dict)}
        for action in actions:
            basket = baskets_by_topic.get(action.basket)
            if not basket:
                continue
            wallets = basket.setdefault("wallets", [])
            if not isinstance(wallets, list):
                continue

            lowered_wallets = {str(wallet).lower(): wallet for wallet in wallets}
            wallet_key = action.wallet_address.lower()
            if action.action == "add":
                if wallet_key not in lowered_wallets:
                    wallets.append(action.wallet_address)
                continue
            if action.action in {"remove", "suspend"}:
                basket["wallets"] = [wallet for wallet in wallets if str(wallet).lower() != wallet_key]
        return updated

    def _write_mutated_config_if_enabled(
        self,
        output_dir: Path,
        actions: list[BasketManagerAction],
        config_path: str | Path | None,
        config_output_path: str | Path | None,
    ) -> Path | None:
        mode = self.config.wallet_discovery.mode
        if mode == "report_only":
            return None
        if config_path is None:
            return None

        source_path = Path(config_path)
        payload = json.loads(source_path.read_text(encoding="utf-8"))
        mutated = self.build_mutated_config(payload, actions)
        if mode == "auto_update":
            target_path = Path(config_output_path) if config_output_path else source_path
        else:
            target_path = Path(config_output_path) if config_output_path else output_dir / "predictcel.proposed.json"
        self._write_json(target_path, mutated)
        return target_path

    def _build_candidate(self, address: str, source: str, trades: list[dict[str, Any]]) -> WalletDiscoveryCandidate:
        profile = classify_wallet_topics(trades, self.config.wallet_discovery.topics)
        total_trades = len(trades)
        recent_trades = sum(1 for trade in trades if _age_seconds(trade) <= self.config.wallet_discovery.recent_window_seconds)
        history_days = _observed_history_days(trades)
        avg_size = _average_trade_size(trades)
        sample_score = min(total_trades / max(self.config.wallet_discovery.min_trades * 3, 1), 1.0)
        recency_score = min(recent_trades / max(self.config.wallet_discovery.min_recent_trades * 3, 1), 1.0)
        history_score = min(
            history_days / max(self.config.wallet_discovery.min_history_days, 1),
            1.0,
        )
        activity_score = _activity_score(trades)
        size_band_score = _size_band_score(avg_size, self.config.wallet_discovery.min_avg_trade_size_usd)
        rejected = []
        if total_trades < self.config.wallet_discovery.min_trades:
            rejected.append("not enough total trades")
        if recent_trades < self.config.wallet_discovery.min_recent_trades:
            rejected.append("not enough recent trades")
        if history_days < self.config.wallet_discovery.min_history_days:
            rejected.append("history too short for registry promotion")
        if avg_size < self.config.wallet_discovery.min_avg_trade_size_usd:
            rejected.append("average trade size too small")
        if activity_score <= 0.2:
            rejected.append("activity cadence looks too fast to copy safely")

        score = self._candidate_score(
            specialization=profile.specialization_score,
            sample_score=sample_score,
            recency_score=recency_score,
            history_score=history_score,
            activity_score=activity_score,
            size_band_score=size_band_score,
        )
        return WalletDiscoveryCandidate(
            wallet_address=address,
            source=source,
            total_trades=total_trades,
            recent_trades=recent_trades,
            avg_trade_size_usd=round(avg_size, 4),
            topic_profile=profile,
            score=score,
            confidence=_confidence(score),
            rejected_reasons=rejected,
            history_days=history_days,
            sample_score=round(sample_score, 4),
            recency_score=round(recency_score, 4),
            history_score=round(history_score, 4),
            activity_score=round(activity_score, 4),
            size_band_score=round(size_band_score, 4),
        )

    def _candidate_score(
        self,
        specialization: float,
        sample_score: float,
        recency_score: float,
        history_score: float,
        activity_score: float,
        size_band_score: float,
    ) -> float:
        score = (
            (specialization * 0.30)
            + (sample_score * 0.20)
            + (recency_score * 0.18)
            + (history_score * 0.12)
            + (activity_score * 0.12)
            + (size_band_score * 0.08)
        )
        return round(max(0.0, min(score, 1.0)), 4)

    def _safe_fetch_trades(self, address: str) -> list[dict[str, Any]]:
        try:
            return self.source.fetch_wallet_trades(address, self.config.wallet_discovery.trade_limit_per_wallet)
        except Exception:
            return []

    def _build_source(self) -> DataApiWalletSource | DataApiMarketTradesWalletSource | CuratedWalletFileSource:
        source = self.config.wallet_discovery.source
        if source == "data_api_market_trades":
            return DataApiMarketTradesWalletSource(self.client, self._discovery_market_ids())
        if source == "curated_wallet_file":
            return CuratedWalletFileSource(
                self.client,
                self.config.wallet_discovery.wallet_candidates_path,
            )
        return DataApiWalletSource(self.client)

    def _discovery_market_ids(self) -> list[str]:
        wallet_payloads: dict[str, list[dict[str, Any]]] = {}
        for wallet in sorted(self._existing_wallets()):
            try:
                rows = self.client.fetch_wallet_trades(
                    wallet,
                    self.config.wallet_discovery.trade_limit_per_wallet,
                )
            except Exception:
                rows = []
            if rows:
                wallet_payloads[wallet] = rows
        return extract_trade_market_ids(wallet_payloads)

    def _existing_wallets(self) -> set[str]:
        return {
            wallet.lower()
            for wallets in self.current_wallets_by_topic.values()
            for wallet in wallets
        }

    def _write_json(self, path: Path, payload: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _average_trade_size(trades: list[dict[str, Any]]) -> float:
    sizes = [float(trade.get("size") or trade.get("sizeUsd") or trade.get("amount") or 0.0) for trade in trades]
    return sum(sizes) / len(sizes) if sizes else 0.0


def _activity_score(trades: list[dict[str, Any]]) -> float:
    trades_per_day = _estimated_trades_per_day(trades)
    if trades_per_day <= 0:
        return 0.0
    if trades_per_day < 1.0:
        return round(max(trades_per_day, 0.25), 4)
    if trades_per_day <= 10.0:
        return 1.0
    if trades_per_day <= 20.0:
        return round(max(0.3, 1.0 - ((trades_per_day - 10.0) * 0.07)), 4)
    return round(max(0.0, 0.3 - ((trades_per_day - 20.0) * 0.02)), 4)


def _estimated_trades_per_day(trades: list[dict[str, Any]]) -> float:
    if not trades:
        return 0.0
    trade_days = {
        _trade_datetime(trade).date()
        for trade in trades
        if _trade_datetime(trade) is not None
    }
    observed_days = max(len(trade_days), 1)
    return len(trades) / observed_days


def _observed_history_days(trades: list[dict[str, Any]]) -> int:
    trade_datetimes = [
        trade_dt
        for trade in trades
        if (trade_dt := _trade_datetime(trade)) is not None
    ]
    if not trade_datetimes:
        return 0
    earliest = min(trade_datetimes)
    latest = max(trade_datetimes)
    return max((latest.date() - earliest.date()).days + 1, 1)


def _size_band_score(avg_size: float, minimum_size: float) -> float:
    if minimum_size <= 0:
        return 1.0
    if avg_size < minimum_size:
        return 0.0
    preferred_size = minimum_size * 3
    soft_cap = minimum_size * 20
    if avg_size <= preferred_size:
        return round(min(avg_size / preferred_size, 1.0), 4)
    if avg_size <= soft_cap:
        taper = (avg_size - preferred_size) / max(soft_cap - preferred_size, 1.0)
        return round(max(0.6, 1.0 - (taper * 0.4)), 4)
    return 0.6


def _trade_datetime(trade: dict[str, Any]) -> datetime | None:
    value = trade.get("timestamp") or trade.get("createdAt") or trade.get("created_at")
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=UTC)
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _age_seconds(trade: dict[str, Any]) -> int:
    dt = _trade_datetime(trade)
    if dt is None:
        return 10**9
    return max(int((datetime.now(UTC) - dt).total_seconds()), 0)


def _confidence(score: float) -> str:
    if score >= 0.70:
        return "HIGH"
    if score >= 0.50:
        return "MEDIUM"
    return "LOW"

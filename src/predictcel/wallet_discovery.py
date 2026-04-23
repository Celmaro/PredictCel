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
from .polymarket import PolymarketPublicClient
from .wallet_sources import DataApiWalletSource
from .wallet_topics import classify_wallet_topics


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
        self.source = DataApiWalletSource(self.client)
        self.assignment_engine = BasketAssignmentEngine(config.wallet_discovery)
        self.manager = BasketManagerPlanner(config)

    def run(self) -> tuple[list[WalletDiscoveryCandidate], list[BasketAssignment], list[BasketManagerAction]]:
        raw_candidates = self.source.fetch_candidates(self.config.wallet_discovery.candidate_limit)
        existing = self._existing_wallets()
        candidates: list[WalletDiscoveryCandidate] = []

        seen: set[str] = set()
        for raw in raw_candidates:
            address = raw["address"].lower()
            if address in seen:
                continue
            seen.add(address)
            if self.config.wallet_discovery.exclude_existing_wallets and address in existing:
                continue
            trades = self._safe_fetch_trades(address)
            candidates.append(self._build_candidate(address, raw.get("source", "unknown"), trades))

        accepted = [candidate for candidate in candidates if not candidate.rejected_reasons]
        assignments = [self.assignment_engine.assign(candidate) for candidate in accepted]
        actions = self.manager.plan(assignments)
        return candidates, assignments, actions

    def write_reports(
        self,
        output_dir: str | Path,
        config_path: str | Path | None = None,
        config_output_path: str | Path | None = None,
    ) -> dict[str, str]:
        candidates, assignments, actions = self.run()
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
            if action.action != "add":
                continue
            basket = baskets_by_topic.get(action.basket)
            if not basket:
                continue
            wallets = basket.setdefault("wallets", [])
            if not isinstance(wallets, list):
                continue
            existing = {str(wallet).lower() for wallet in wallets}
            if action.wallet_address.lower() not in existing:
                wallets.append(action.wallet_address)
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
        avg_size = _average_trade_size(trades)
        rejected = []
        if total_trades < self.config.wallet_discovery.min_trades:
            rejected.append("not enough total trades")
        if recent_trades < self.config.wallet_discovery.min_recent_trades:
            rejected.append("not enough recent trades")
        if avg_size < self.config.wallet_discovery.min_avg_trade_size_usd:
            rejected.append("average trade size too small")

        score = self._candidate_score(profile.specialization_score, total_trades, recent_trades, avg_size)
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
        )

    def _candidate_score(self, specialization: float, total_trades: int, recent_trades: int, avg_size: float) -> float:
        sample_score = min(total_trades / max(self.config.wallet_discovery.min_trades * 3, 1), 1.0)
        recent_score = min(recent_trades / max(self.config.wallet_discovery.min_recent_trades * 3, 1), 1.0)
        size_score = min(avg_size / max(self.config.wallet_discovery.min_avg_trade_size_usd * 5, 1.0), 1.0)
        score = (specialization * 0.35) + (sample_score * 0.25) + (recent_score * 0.25) + (size_score * 0.15)
        return round(max(0.0, min(score, 1.0)), 4)

    def _safe_fetch_trades(self, address: str) -> list[dict[str, Any]]:
        try:
            return self.source.fetch_wallet_trades(address, self.config.wallet_discovery.trade_limit_per_wallet)
        except Exception:
            return []

    def _existing_wallets(self) -> set[str]:
        return {wallet.lower() for basket in self.config.baskets for wallet in basket.wallets}

    def _write_json(self, path: Path, payload: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _average_trade_size(trades: list[dict[str, Any]]) -> float:
    sizes = [float(trade.get("size") or trade.get("sizeUsd") or trade.get("amount") or 0.0) for trade in trades]
    return sum(sizes) / len(sizes) if sizes else 0.0


def _age_seconds(trade: dict[str, Any]) -> int:
    value = trade.get("timestamp") or trade.get("createdAt") or trade.get("created_at")
    if value is None:
        return 10**9
    try:
        if isinstance(value, (int, float)):
            dt = datetime.fromtimestamp(float(value), tz=UTC)
        else:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return 10**9
    return max(int((datetime.now(UTC) - dt).total_seconds()), 0)


def _confidence(score: float) -> str:
    if score >= 0.70:
        return "HIGH"
    if score >= 0.50:
        return "MEDIUM"
    return "LOW"

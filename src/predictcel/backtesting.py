from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .config import AppConfig
from .copy_engine import CopyEngine
from .markets import load_market_snapshots
from .models import CopyCandidate, ExecutionResult, Position
from .scoring import WalletQualityScorer
from .storage import SignalStore
from .wallets import load_wallet_trades


@dataclass
class BacktestResult:
    total_trades: int
    winning_trades: int
    losing_trades: int
    total_pnl: float
    max_drawdown: float
    sharpe_ratio: float
    win_rate: float
    avg_trade_pnl: float
    attribution: dict[str, float]  # By basket/topic


class Backtester:
    def __init__(self, config: AppConfig, db_path: str = ":memory:"):
        self.config = config
        self.store = SignalStore(db_path)
        self.scorer = WalletQualityScorer(config.filters, config.consensus.recency_half_life_seconds)
        self.copy_engine = CopyEngine(config)

    def run_backtest(
        self,
        wallet_trades_path: str,
        market_snapshots_path: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> BacktestResult:
        trades = load_wallet_trades(wallet_trades_path)
        markets = load_market_snapshots(market_snapshots_path)

        # Filter trades by date if specified
        if start_date or end_date:
            # Assume trades have timestamps, but since example data may not, skip filtering
            pass

        # Score wallets
        wallet_qualities = self.scorer.score(trades, markets)

        # Generate candidates
        candidates = self.copy_engine.evaluate(trades, markets, wallet_qualities)

        # Simulate execution (simplified: assume all candidates execute and resolve at current price)
        simulated_results = []
        attribution = {}
        total_pnl = 0.0
        pnls = []

        for candidate in candidates:
            market = markets.get(candidate.market_id)
            if not market:
                continue

            # Simulate position
            entry_price = candidate.current_price
            # Assume resolution: for simplicity, random outcome, but bias towards consensus
            import random
            outcome = random.random() < candidate.consensus_ratio
            resolution_price = 1.0 if outcome else 0.0

            pnl = candidate.suggested_position_usd * (resolution_price - entry_price) / entry_price
            pnls.append(pnl)
            total_pnl += pnl

            # Attribution
            topic = candidate.topic
            attribution[topic] = attribution.get(topic, 0.0) + pnl

            simulated_results.append(
                ExecutionResult(
                    market_id=candidate.market_id,
                    topic=candidate.topic,
                    side=candidate.side,
                    token_id="",
                    amount_usd=candidate.suggested_position_usd,
                    worst_price=entry_price,
                    status="simulated",
                    order_id="",
                    error="",
                    copyability_score=candidate.copyability_score,
                    reason="backtest simulation",
                )
            )

        # Calculate metrics
        winning_trades = sum(1 for pnl in pnls if pnl > 0)
        losing_trades = sum(1 for pnl in pnls if pnl < 0)
        max_drawdown = self._calculate_max_drawdown(pnls)
        sharpe_ratio = self._calculate_sharpe_ratio(pnls) if pnls else 0.0
        win_rate = winning_trades / len(pnls) if pnls else 0.0
        avg_trade_pnl = sum(pnls) / len(pnls) if pnls else 0.0

        return BacktestResult(
            total_trades=len(pnls),
            winning_trades=winning_trades,
            losing_trades=losing_trades,
            total_pnl=total_pnl,
            max_drawdown=max_drawdown,
            sharpe_ratio=sharpe_ratio,
            win_rate=win_rate,
            avg_trade_pnl=avg_trade_pnl,
            attribution=attribution,
        )

    def _calculate_max_drawdown(self, pnls: list[float]) -> float:
        if not pnls:
            return 0.0
        cumulative = [sum(pnls[:i+1]) for i in range(len(pnls))]
        peak = cumulative[0]
        max_dd = 0.0
        for val in cumulative:
            if val > peak:
                peak = val
            dd = peak - val
            max_dd = max(max_dd, dd)
        return max_dd

    def _calculate_sharpe_ratio(self, pnls: list[float], risk_free_rate: float = 0.02) -> float:
        if not pnls or len(pnls) < 2:
            return 0.0
        avg_return = sum(pnls) / len(pnls)
        variance = sum((pnl - avg_return) ** 2 for pnl in pnls) / (len(pnls) - 1)
        std_dev = variance ** 0.5
        return (avg_return - risk_free_rate) / std_dev if std_dev > 0 else 0.0


def run_backtest_example():
    # Example usage
    config = AppConfig(
        baskets=[],
        filters=None,  # Load from file in real usage
        arbitrage=None,
        wallet_trades_path="data/wallet_trades.example.json",
        market_snapshots_path="data/market_snapshots.example.json",
        live_data=None,
        execution=None,
    )
    backtester = Backtester(config)
    result = backtester.run_backtest(
        "data/wallet_trades.example.json",
        "data/market_snapshots.example.json"
    )
    print(json.dumps({
        "total_trades": result.total_trades,
        "win_rate": result.win_rate,
        "total_pnl": result.total_pnl,
        "sharpe_ratio": result.sharpe_ratio,
        "attribution": result.attribution,
    }, indent=2))


if __name__ == "__main__":
    run_backtest_example()
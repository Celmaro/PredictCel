from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .models import (
    ArbitrageOpportunity,
    CopyCandidate,
    ExecutionResult,
    MarketSnapshot,
    Position,
)

logger = logging.getLogger(__name__)


class SignalStore:
    """SQLite-backed store for cycle state, signals, and execution history."""

    def __init__(self, db_path: str) -> None:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(db_path)
        self._init_schema()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------
    def _init_schema(self) -> None:
        cursor = self.connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS copy_candidates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id TEXT NOT NULL,
                topic TEXT NOT NULL,
                side TEXT NOT NULL,
                consensus_ratio REAL NOT NULL,
                payload TEXT NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS arbitrage_opportunities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id TEXT NOT NULL,
                topic TEXT NOT NULL,
                gross_edge REAL NOT NULL,
                payload TEXT NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS execution_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id TEXT NOT NULL,
                topic TEXT NOT NULL,
                side TEXT NOT NULL,
                status TEXT NOT NULL,
                payload TEXT NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id TEXT NOT NULL,
                topic TEXT NOT NULL,
                side TEXT NOT NULL,
                token_id TEXT NOT NULL,
                entry_price REAL NOT NULL,
                entry_amount_usd REAL NOT NULL,
                current_price REAL NOT NULL,
                unrealized_pnl REAL NOT NULL,
                opened_at TEXT NOT NULL,
                last_updated TEXT NOT NULL,
                take_profit_pct REAL NOT NULL,
                stop_loss_pct REAL NOT NULL,
                max_hold_minutes INTEGER NOT NULL,
                status TEXT NOT NULL,
                market_title TEXT DEFAULT ""
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS signal_fingerprints (
                fingerprint TEXT PRIMARY KEY,
                market_id TEXT NOT NULL,
                topic TEXT NOT NULL,
                side TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS open_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT NOT NULL UNIQUE,
                market_id TEXT NOT NULL,
                topic TEXT NOT NULL,
                side TEXT NOT NULL,
                token_id TEXT NOT NULL,
                amount_usd REAL NOT NULL,
                worst_price REAL NOT NULL,
                copyability_score REAL NOT NULL,
                order_type TEXT NOT NULL,
                reason TEXT NOT NULL,
                market_title TEXT DEFAULT "",
                status TEXT NOT NULL DEFAULT "pending",
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                error TEXT DEFAULT ""
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_signal_fingerprints_created_at ON signal_fingerprints(created_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_signal_fingerprints_market_topic_side ON signal_fingerprints(market_id, topic, side)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_positions_status_market ON positions(status, market_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_positions_opened_at ON positions(opened_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_open_orders_market ON open_orders(market_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_open_orders_status ON open_orders(status)")
        self.connection.execute("PRAGMA synchronous = NORMAL")
        self.connection.commit()

    # ------------------------------------------------------------------
    # Open Orders (new)
    # ------------------------------------------------------------------
    def save_open_order(self, result: ExecutionResult) -> str:
        """Persist or update a submitted order. Returns the stored order_id."""
        now = datetime.now(UTC).isoformat()
        self.connection.execute("""
            INSERT INTO open_orders (
                order_id, market_id, topic, side, token_id,
                amount_usd, worst_price, copyability_score,
                order_type, reason, market_title,
                status, created_at, updated_at, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(order_id) DO UPDATE SET
                status = excluded.status,
                updated_at = excluded.updated_at,
                error = excluded.error
        """, (
            result.order_id,
            result.market_id,
            result.topic,
            result.side,
            result.token_id,
            result.amount_usd,
            result.worst_price,
            result.copyability_score,
            getattr(result, "order_type", ""),
            result.reason,
            getattr(result, "market_title", ""),
            result.status,
            now,
            now,
            result.error,
        ))
        self.connection.commit()
        return result.order_id

    def get_open_orders(self) -> list[dict[str, Any]]:
        """Return all non-terminal orders (pending / submitted / filled)."""
        rows = self.connection.execute(
            "SELECT * FROM open_orders WHERE status NOT IN ('filled','matched','error','cancelled','dry_run') ORDER BY created_at DESC"
        ).fetchall()
        cols = [c[0] for c in self.connection.execute("PRAGMA table_info(open_orders)").fetchall()]
        return [dict(zip(cols, row)) for row in rows]

    def get_open_orders_by_market(self, market_id: str) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM open_orders WHERE market_id = ? AND status NOT IN ('filled','matched','error','cancelled') ORDER BY created_at",
            (market_id,)
        ).fetchall()
        cols = [c[0] for c in self.connection.execute("PRAGMA table_info(open_orders)").fetchall()]
        return [dict(zip(cols, row)) for row in rows]

    def cancel_open_order(self, order_id: str) -> None:
        now = datetime.now(UTC).isoformat()
        self.connection.execute(
            "UPDATE open_orders SET status = 'cancelled', updated_at = ? WHERE order_id = ?",
            (now, order_id)
        )
        self.connection.commit()

    # ------------------------------------------------------------------
    # Positions
    # ------------------------------------------------------------------
    def get_open_positions(self) -> list[Position]:
        rows = self.connection.execute(
            "SELECT * FROM positions WHERE status IN ('open', 'closing')"
        ).fetchall()
        cols = [c[0] for c in self.connection.execute("PRAGMA table_info(positions)").fetchall()]
        positions = []
        for row in rows:
            d = dict(zip(cols, row))
            d.pop("id", None)
            d["opened_at"] = datetime.fromisoformat(d["opened_at"])
            d["last_updated"] = datetime.fromisoformat(d["last_updated"])
            positions.append(Position(**d))
        return positions

    def get_total_exposure(self) -> float:
        row = self.connection.execute(
            "SELECT COALESCE(SUM(entry_amount_usd), 0.0) FROM positions WHERE status IN ('open', 'closing')"
        ).fetchone()
        return float(row[0]) if row else 0.0

    def save_positions(self, positions: list[Position]) -> None:
        cursor = self.connection.cursor()
        cursor.execute("DELETE FROM positions WHERE status IN ('open', 'closing')")
        for pos in positions:
            cursor.execute("""
                INSERT INTO positions (
                    market_id, topic, side, token_id,
                    entry_price, entry_amount_usd, current_price, unrealized_pnl,
                    opened_at, last_updated, take_profit_pct, stop_loss_pct,
                    max_hold_minutes, status, market_title
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                pos.market_id, pos.topic, pos.side, pos.token_id,
                pos.entry_price, pos.entry_amount_usd, pos.current_price, pos.unrealized_pnl,
                pos.opened_at.isoformat(), pos.last_updated.isoformat(),
                pos.take_profit_pct, pos.stop_loss_pct,
                pos.max_hold_minutes, pos.status,
                getattr(pos, "market_title", ""),
            ))
        self.connection.commit()

    # ------------------------------------------------------------------
    # Execution results
    # ------------------------------------------------------------------
    def save_execution_result(self, result: ExecutionResult) -> None:
        self.connection.execute(
            "INSERT INTO execution_results (market_id, topic, side, status, payload) VALUES (?, ?, ?, ?, ?)",
            (result.market_id, result.topic, result.side, result.status, json.dumps(asdict(result)))
        )
        self.connection.commit()

    # ------------------------------------------------------------------
    # Signal fingerprints
    # ------------------------------------------------------------------
    def make_signal_fingerprint(self, topic: str, market_id: str, side: str) -> str:
        return f"{topic.lower()}:{market_id}:{side.upper()}"

    def mark_signal_seen(self, fingerprint: str, topic: str, market_id: str, side: str) -> None:
        self.connection.execute(
            "INSERT OR REPLACE INTO signal_fingerprints (fingerprint, market_id, topic, side, created_at) VALUES (?, ?, ?, ?, ?)",
            (fingerprint, market_id, topic, side, datetime.now(UTC).isoformat())
        )
        self.connection.commit()

    def mark_signals_seen(self, fingerprints: list[str], ttl_minutes: int = 1440) -> None:
        now = datetime.now(UTC)
        cutoff = datetime.fromtimestamp(now.timestamp() - ttl_minutes * 60, tz=UTC).isoformat()
        self.connection.execute(
            "DELETE FROM signal_fingerprints WHERE created_at < ?",
            (cutoff,)
        )
        self.connection.executemany(
            "INSERT OR IGNORE INTO signal_fingerprints (fingerprint, market_id, topic, side, created_at) VALUES (?, ?, ?, ?, ?)",
            [(fp, "", "", "", now.isoformat()) for fp in fingerprints]
        )
        self.connection.commit()

    def has_recent_signal(self, fingerprint: str, ttl_minutes: int = 1440) -> bool:
        cutoff = datetime.fromtimestamp(
            datetime.now(UTC).timestamp() - ttl_minutes * 60,
            tz=UTC
        ).isoformat()
        row = self.connection.execute(
            "SELECT 1 FROM signal_fingerprints WHERE fingerprint = ? AND created_at >= ?",
            (fingerprint, cutoff)
        ).fetchone()
        return row is not None

    def has_recent_signals(
        self, fingerprints: list[str], ttl_minutes: int = 1440
    ) -> list[str]:
        if not fingerprints:
            return []
        cutoff = datetime.fromtimestamp(
            datetime.now(UTC).timestamp() - ttl_minutes * 60,
            tz=UTC
        ).isoformat()
        chunks = [fingerprints[i : i + 500] for i in range(0, len(fingerprints), 500)]
        seen: set[str] = set()
        for chunk in chunks:
            placeholders = ",".join("?" * len(chunk))
            rows = self.connection.execute(
                f"SELECT fingerprint FROM signal_fingerprints WHERE fingerprint IN ({placeholders}) AND created_at >= ?",
                (*chunk, cutoff)
            ).fetchall()
            seen.update(r[0] for r in rows)
        return [fp for fp in fingerprints if fp in seen]

    # ------------------------------------------------------------------
    # Portfolio analytics
    # ------------------------------------------------------------------
    def get_portfolio_summary(self, starting_bankroll_usd: float = 100.0) -> dict[str, Any]:
        positions = self.get_open_positions()
        total_exposure = sum(p.entry_amount_usd for p in positions)
        unrealized_pnl = sum(p.unrealized_pnl for p in positions)
        return {
            "open_positions": len(positions),
            "total_exposure_usd": round(total_exposure, 4),
            "unrealized_pnl_usd": round(unrealized_pnl, 4),
            "cash_remaining_usd": round(max(0.0, starting_bankroll_usd - total_exposure), 4),
        }

    def get_portfolio_var(
        self,
        confidence_level: float = 0.95,
        time_horizon_days: int = 1,
        simulations: int = 10000,
        starting_bankroll_usd: float = 100.0,
    ) -> dict[str, Any]:
        positions = self.get_open_positions()
        if not positions:
            return {"var_usd": 0.0, "method": "no_positions", "confidence_level": confidence_level}
        try:
            import numpy as np
        except (ImportError, ModuleNotFoundError):
            return self._analytical_var(positions, confidence_level, starting_bankroll_usd)

        exposures = [p.entry_amount_usd for p in positions]
        market_returns = np.random.randn(simulations, len(positions))
        correlations = np.full((len(positions), len(positions)), 0.3)
        np.fill_diagonal(correlations, 1.0)
        L = np.linalg.cholesky(correlations)
        correlated_returns = market_returns @ L.T
        position_losses = np.array(exposures)[np.newaxis, :] * (1 - np.exp(correlated_returns))
        portfolio_losses = position_losses.sum(axis=1)
        var = np.percentile(portfolio_losses, (1 - confidence_level) * 100)
        return {
            "var_usd": round(float(np.abs(var)), 4),
            "method": "monte_carlo",
            "confidence_level": confidence_level,
            "simulations": simulations,
            "positions": len(positions),
        }

    def _analytical_var(
        self,
        positions: list[Position],
        confidence_level: float,
        starting_bankroll_usd: float,
    ) -> dict[str, Any]:
        import math
        total = sum(p.entry_amount_usd for p in positions)
        if total <= 0:
            return {"var_usd": 0.0, "method": "analytical", "confidence_level": confidence_level}
        z = {0.90: 1.28, 0.95: 1.65, 0.99: 2.33}.get(confidence_level, 1.65)
        vol = 0.30
        var = total * vol * z * math.sqrt(1 / 365)
        return {
            "var_usd": round(var, 4),
            "method": "analytical",
            "confidence_level": confidence_level,
            "positions": len(positions),
        }

    def save_cycle_payloads(
        self,
        candidates: list[CopyCandidate],
        arbitrage_opportunities: list[ArbitrageOpportunity],
        markets: dict[str, MarketSnapshot],
    ) -> None:
        cursor = self.connection.cursor()
        cursor.execute("DELETE FROM copy_candidates")
        cursor.execute("DELETE FROM arbitrage_opportunities")
        for c in candidates:
            cursor.execute(
                "INSERT INTO copy_candidates (market_id, topic, side, consensus_ratio, payload) VALUES (?, ?, ?, ?, ?)",
                (c.market_id, c.topic, c.side, c.consensus_ratio, json.dumps(asdict(c)))
            )
        for arb in arbitrage_opportunities:
            cursor.execute(
                "INSERT INTO arbitrage_opportunities (market_id, topic, gross_edge, payload) VALUES (?, ?, ?, ?)",
                (arb.market_id, arb.topic, arb.gross_edge, json.dumps(asdict(arb)))
            )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()


def asdict(obj: Any) -> dict[str, Any]:
    result = {}
    for k, v in vars(obj).items():
        if hasattr(v, "__dataclass_fields__"):
            result[k] = asdict(v)
        elif isinstance(v, (list, tuple)):
            result[k] = [
                asdict(i) if hasattr(i, "__dataclass_fields__") else i
                for i in v
            ]
        else:
            result[k] = v
    return result
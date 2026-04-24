from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterable

from .models import ArbitrageOpportunity, CopyCandidate, ExecutionResult, Position

ACTIVE_POSITION_STATUSES = ("open", "closing")


class SignalStore:
    def __init__(self, db_path: str) -> None:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(db_path)
        self._init_schema()

    def _init_schema(self) -> None:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS copy_candidates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id TEXT NOT NULL,
                topic TEXT NOT NULL,
                side TEXT NOT NULL,
                consensus_ratio REAL NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS arbitrage_opportunities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id TEXT NOT NULL,
                topic TEXT NOT NULL,
                gross_edge REAL NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS execution_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id TEXT NOT NULL,
                topic TEXT NOT NULL,
                side TEXT NOT NULL,
                status TEXT NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        cursor.execute(
            """
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
                status TEXT NOT NULL
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_fingerprints (
                fingerprint TEXT PRIMARY KEY,
                market_id TEXT NOT NULL,
                topic TEXT NOT NULL,
                side TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_signal_fingerprints_created_at ON signal_fingerprints(created_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_signal_fingerprints_market_topic_side ON signal_fingerprints(market_id, topic, side)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_positions_status_market ON positions(status, market_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_positions_opened_at ON positions(opened_at)")
        self.connection.execute("PRAGMA synchronous = NORMAL")
        self.connection.commit()

    def save_copy_candidates(self, candidates: Iterable[CopyCandidate]) -> None:
        cursor = self.connection.cursor()
        rows = [
            (
                candidate.market_id,
                candidate.topic,
                candidate.side,
                candidate.consensus_ratio,
                json.dumps(asdict(candidate), sort_keys=True),
            )
            for candidate in candidates
        ]
        cursor.executemany(
            "INSERT INTO copy_candidates (market_id, topic, side, consensus_ratio, payload) VALUES (?, ?, ?, ?, ?)",
            rows,
        )
        self.connection.commit()

    def save_arbitrage_opportunities(self, opportunities: Iterable[ArbitrageOpportunity]) -> None:
        cursor = self.connection.cursor()
        rows = [
            (
                opportunity.market_id,
                opportunity.topic,
                opportunity.gross_edge,
                json.dumps(asdict(opportunity), sort_keys=True),
            )
            for opportunity in opportunities
        ]
        cursor.executemany(
            "INSERT INTO arbitrage_opportunities (market_id, topic, gross_edge, payload) VALUES (?, ?, ?, ?)",
            rows,
        )
        self.connection.commit()

    def save_execution_results(self, results: Iterable[ExecutionResult]) -> None:
        cursor = self.connection.cursor()
        rows = [
            (
                result.market_id,
                result.topic,
                result.side,
                result.status,
                json.dumps(asdict(result), sort_keys=True),
            )
            for result in results
        ]
        cursor.executemany(
            "INSERT INTO execution_results (market_id, topic, side, status, payload) VALUES (?, ?, ?, ?, ?)",
            rows,
        )
        self.connection.commit()

    def save_cycle_payloads(
        self,
        candidates: Iterable[CopyCandidate],
        opportunities: Iterable[ArbitrageOpportunity],
        execution_results: Iterable[ExecutionResult],
    ) -> None:
        cursor = self.connection.cursor()
        candidate_rows = [
            (
                candidate.market_id,
                candidate.topic,
                candidate.side,
                candidate.consensus_ratio,
                json.dumps(asdict(candidate), sort_keys=True),
            )
            for candidate in candidates
        ]
        opportunity_rows = [
            (
                opportunity.market_id,
                opportunity.topic,
                opportunity.gross_edge,
                json.dumps(asdict(opportunity), sort_keys=True),
            )
            for opportunity in opportunities
        ]
        execution_rows = [
            (
                result.market_id,
                result.topic,
                result.side,
                result.status,
                json.dumps(asdict(result), sort_keys=True),
            )
            for result in execution_results
        ]
        if candidate_rows:
            cursor.executemany(
                "INSERT INTO copy_candidates (market_id, topic, side, consensus_ratio, payload) VALUES (?, ?, ?, ?, ?)",
                candidate_rows,
            )
        if opportunity_rows:
            cursor.executemany(
                "INSERT INTO arbitrage_opportunities (market_id, topic, gross_edge, payload) VALUES (?, ?, ?, ?)",
                opportunity_rows,
            )
        if execution_rows:
            cursor.executemany(
                "INSERT INTO execution_results (market_id, topic, side, status, payload) VALUES (?, ?, ?, ?, ?)",
                execution_rows,
            )
        self.connection.commit()

    def get_open_positions(self) -> list[Position]:
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT market_id, topic, side, token_id, entry_price, entry_amount_usd, "
            "current_price, unrealized_pnl, opened_at, last_updated, "
            "take_profit_pct, stop_loss_pct, max_hold_minutes, status "
            "FROM positions WHERE status IN ('open', 'closing') ORDER BY opened_at"
        )
        rows = cursor.fetchall()
        return [
            Position(
                market_id=r[0],
                topic=r[1],
                side=r[2],
                token_id=r[3],
                entry_price=r[4],
                entry_amount_usd=r[5],
                current_price=r[6],
                unrealized_pnl=r[7],
                opened_at=_parse_dt(r[8]),
                last_updated=_parse_dt(r[9]),
                take_profit_pct=r[10],
                stop_loss_pct=r[11],
                max_hold_minutes=r[12],
                status=r[13],
            )
            for r in rows
        ]

    def get_held_market_ids(self) -> set[str]:
        cursor = self.connection.cursor()
        cursor.execute("SELECT DISTINCT market_id FROM positions WHERE status IN ('open', 'closing')")
        return {row[0] for row in cursor.fetchall()}

    def get_total_exposure(self) -> float:
        cursor = self.connection.cursor()
        cursor.execute("SELECT COALESCE(SUM(entry_amount_usd), 0.0) FROM positions WHERE status IN ('open', 'closing')")
        row = cursor.fetchone()
        return float(row[0]) if row else 0.0

    def get_portfolio_var(self, confidence_level: float = 0.95, time_horizon_days: int = 1) -> float:
        """Calculate Value at Risk for current portfolio using historical volatility proxy."""
        positions = self.get_open_positions()
        if not positions:
            return 0.0

        total_exposure = sum(pos.entry_amount_usd for pos in positions)
        if total_exposure == 0:
            return 0.0

        # Simplified VaR: assume 2% daily volatility per position, correlation 0.3
        position_vars = []
        for pos in positions:
            # Estimate position volatility from spread or assume 2%
            volatility = 0.02  # Placeholder: in real impl, use historical data
            position_var = pos.entry_amount_usd * volatility * (time_horizon_days ** 0.5)
            position_vars.append(position_var)

        # Portfolio VaR with correlation
        correlation = 0.3
        portfolio_volatility = (sum(v**2 for v in position_vars) + 2 * correlation * sum(
            position_vars[i] * position_vars[j] for i in range(len(position_vars)) for j in range(i+1, len(position_vars))
        )) ** 0.5

        # Normal distribution VaR
        import math
        z_score = {0.95: 1.645, 0.99: 2.326}.get(confidence_level, 1.645)
        return portfolio_volatility * z_score

    def save_position(self, position: Position) -> None:
        self.save_positions([position])

    def save_positions(self, positions: Iterable[Position]) -> None:
        cursor = self.connection.cursor()
        rows = [
            (
                position.market_id,
                position.topic,
                position.side,
                position.token_id,
                position.entry_price,
                position.entry_amount_usd,
                position.current_price,
                position.unrealized_pnl,
                position.opened_at.isoformat(),
                position.last_updated.isoformat(),
                position.take_profit_pct,
                position.stop_loss_pct,
                position.max_hold_minutes,
                position.status,
            )
            for position in positions
        ]
        cursor.executemany(
            "INSERT INTO positions (market_id, topic, side, token_id, entry_price, "
            "entry_amount_usd, current_price, unrealized_pnl, opened_at, last_updated, "
            "take_profit_pct, stop_loss_pct, max_hold_minutes, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        self.connection.commit()

    def mark_signals_seen(self, signals: Iterable[tuple[str, str, str]]) -> None:
        cursor = self.connection.cursor()
        rows = [
            (
                self.make_signal_fingerprint(market_id, topic, side),
                market_id,
                topic,
                side,
                datetime.now(UTC).isoformat(),
            )
            for market_id, topic, side in signals
        ]
        cursor.executemany(
            "INSERT OR REPLACE INTO signal_fingerprints (fingerprint, market_id, topic, side, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            rows,
        )
        self.connection.commit()

    def update_position(self, market_id: str, current_price: float, unrealized_pnl: float, status: str) -> None:
        cursor = self.connection.cursor()
        cursor.execute(
            "UPDATE positions SET current_price = ?, unrealized_pnl = ?, "
            "last_updated = ?, status = ? WHERE market_id = ? AND status IN ('open', 'closing')",
            (current_price, unrealized_pnl, datetime.now(UTC).isoformat(), status, market_id),
        )
        self.connection.commit()

    def make_signal_fingerprint(self, market_id: str, topic: str, side: str) -> str:
        return f"{topic.strip().lower()}:{market_id.strip()}:{side.strip().upper()}"

    def has_recent_signal(self, market_id: str, topic: str, side: str, ttl_minutes: int = 1440) -> bool:
        cutoff = datetime.now(UTC) - timedelta(minutes=ttl_minutes)
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT created_at FROM signal_fingerprints WHERE fingerprint = ?",
            (self.make_signal_fingerprint(market_id, topic, side),),
        )
        row = cursor.fetchone()
        if not row:
            return False
        return _parse_dt(row[0]) >= cutoff

    def has_recent_signals(self, signals: Iterable[tuple[str, str, str]], ttl_minutes: int = 1440) -> set[str]:
        signals = list(signals)
        if not signals:
            return set()
        cutoff = datetime.now(UTC) - timedelta(minutes=ttl_minutes)
        fingerprints = [self.make_signal_fingerprint(market_id, topic, side) for market_id, topic, side in signals]
        recent: set[str] = set()
        cursor = self.connection.cursor()
        chunk_size = 500
        for i in range(0, len(fingerprints), chunk_size):
            chunk = fingerprints[i : i + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            cursor.execute(
                f"SELECT fingerprint, created_at FROM signal_fingerprints WHERE fingerprint IN ({placeholders})",
                tuple(chunk),
            )
            for fingerprint, created_at in cursor.fetchall():
                if _parse_dt(created_at) >= cutoff:
                    recent.add(fingerprint)
        return recent

    def mark_signal_seen(self, market_id: str, topic: str, side: str) -> None:
        cursor = self.connection.cursor()
        fingerprint = self.make_signal_fingerprint(market_id, topic, side)
        cursor.execute(
            "INSERT OR REPLACE INTO signal_fingerprints (fingerprint, market_id, topic, side, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (fingerprint, market_id, topic, side, datetime.now(UTC).isoformat()),
        )
        self.connection.commit()


def _parse_dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed

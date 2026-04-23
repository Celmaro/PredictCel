from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Iterable

from .models import ArbitrageOpportunity, CopyCandidate, ExecutionResult, Position


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

    def get_open_positions(self) -> list[Position]:
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT market_id, topic, side, token_id, entry_price, entry_amount_usd, "
            "current_price, unrealized_pnl, opened_at, last_updated, "
            "take_profit_pct, stop_loss_pct, max_hold_minutes, status "
            "FROM positions WHERE status = 'open' ORDER BY opened_at"
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
        cursor.execute(
            "SELECT DISTINCT market_id FROM positions WHERE status = 'open'"
        )
        return {row[0] for row in cursor.fetchall()}

    def get_total_exposure(self) -> float:
        """Return the sum of entry_amount_usd for all open positions."""
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT COALESCE(SUM(entry_amount_usd), 0.0) FROM positions WHERE status = 'open'"
        )
        row = cursor.fetchone()
        return float(row[0]) if row else 0.0

    def save_position(self, position: Position) -> None:
        cursor = self.connection.cursor()
        cursor.execute(
            "INSERT INTO positions (market_id, topic, side, token_id, entry_price, "
            "entry_amount_usd, current_price, unrealized_pnl, opened_at, last_updated, "
            "take_profit_pct, stop_loss_pct, max_hold_minutes, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
            ),
        )
        self.connection.commit()

    def update_position(
        self,
        market_id: str,
        current_price: float,
        unrealized_pnl: float,
        status: str,
    ) -> None:
        cursor = self.connection.cursor()
        cursor.execute(
            "UPDATE positions SET current_price = ?, unrealized_pnl = ?, "
            "last_updated = ?, status = ? WHERE market_id = ? AND status = 'open'",
            (
                current_price,
                unrealized_pnl,
                datetime.now().isoformat(),
                status,
                market_id,
            ),
        )
        self.connection.commit()


def _parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)

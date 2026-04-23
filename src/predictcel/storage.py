from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from pathlib import Path
from typing import Iterable

from .models import ArbitrageOpportunity, CopyCandidate


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

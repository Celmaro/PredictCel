from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterable

from .models import (
    ArbitrageOpportunity,
    BasketHealth,
    BasketMembership,
    CopyCandidate,
    ExecutionResult,
    Position,
    WalletRegistryEntry,
)

logger = logging.getLogger(__name__)
ACTIVE_POSITION_STATUSES = ("open", "closing")


class SignalStore:
    """SQLite-backed storage for signals, positions, execution results, and basket registry state."""

    def __init__(self, db_path: str) -> None:
        """Initialize SignalStore with database path."""
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._db_path = db_path
        self._connection: sqlite3.Connection | None = None
        self._connect()
        self._init_schema()

    def _connect(self) -> None:
        """Establish database connection."""
        if self._connection is None:
            self._connection = sqlite3.connect(self._db_path)

    def close(self) -> None:
        """Close database connection."""
        if self._connection is not None:
            try:
                self._connection.commit()
            except sqlite3.Error:
                pass
            finally:
                self._connection.close()
                self._connection = None

    def __enter__(self) -> "SignalStore":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - ensures connection is closed."""
        self.close()

    def __del__(self) -> None:
        """Destructor - attempts to close connection if not already closed."""
        self.close()

    @property
    def connection(self) -> sqlite3.Connection:
        """Get database connection, reconnecting if necessary."""
        if self._connection is None:
            self._connect()
        return self._connection

    def _init_schema(self) -> None:
        """Initialize database schema with indexes."""
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
            CREATE TABLE IF NOT EXISTS open_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id TEXT NOT NULL,
                topic TEXT NOT NULL,
                side TEXT NOT NULL,
                token_id TEXT NOT NULL,
                order_type TEXT NOT NULL,
                price REAL NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
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

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS wallet_registry (
                wallet TEXT PRIMARY KEY,
                source_type TEXT NOT NULL,
                source_ref TEXT NOT NULL,
                trust_seed REAL NOT NULL,
                status TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_trade_at TEXT,
                last_scored_at TEXT,
                notes TEXT NOT NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS basket_memberships (
                topic TEXT NOT NULL,
                wallet TEXT NOT NULL,
                tier TEXT NOT NULL,
                rank INTEGER NOT NULL,
                active INTEGER NOT NULL,
                joined_at TEXT NOT NULL,
                effective_until TEXT,
                promotion_reason TEXT NOT NULL,
                demotion_reason TEXT NOT NULL,
                PRIMARY KEY (topic, wallet)
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS basket_health_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT NOT NULL,
                tracked_wallet_count INTEGER NOT NULL,
                fresh_core_wallets_24h INTEGER NOT NULL,
                fresh_active_wallets_7d INTEGER NOT NULL,
                active_eligible_wallet_count INTEGER NOT NULL,
                eligible_trades_7d INTEGER NOT NULL,
                stale_ratio REAL NOT NULL,
                clustered_ratio REAL NOT NULL,
                health_state TEXT NOT NULL,
                captured_at TEXT NOT NULL
            )
            """
        )

        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_signal_fingerprints_created_at ON signal_fingerprints(created_at)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_signal_fingerprints_market_topic_side ON signal_fingerprints(market_id, topic, side)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_positions_status_market ON positions(status, market_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_positions_opened_at ON positions(opened_at)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_open_orders_status_market ON open_orders(status, market_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_basket_memberships_topic_tier ON basket_memberships(topic, tier)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_basket_health_topic_captured_at ON basket_health_snapshots(topic, captured_at DESC)"
        )

        self.connection.execute("PRAGMA synchronous = NORMAL")
        self.connection.commit()

    def save_copy_candidates(self, candidates: Iterable[CopyCandidate]) -> None:
        """Save copy candidates to database."""
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

    def save_arbitrage_opportunities(
        self,
        opportunities: Iterable[ArbitrageOpportunity],
    ) -> None:
        """Save arbitrage opportunities to database."""
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
        """Save execution results to database."""
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
        """Save all cycle payloads in a single transaction."""
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

    def save_position(self, position: Position) -> None:
        """Save a single position."""
        self.save_positions([position])

    def save_positions(self, positions: Iterable[Position]) -> None:
        """Save multiple positions."""
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
            "INSERT INTO positions (market_id, topic, side, token_id, entry_price, entry_amount_usd, current_price, unrealized_pnl, opened_at, last_updated, take_profit_pct, stop_loss_pct, max_hold_minutes, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        self.connection.commit()

    def get_open_positions(self) -> list[Position]:
        """Get all open positions."""
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT market_id, topic, side, token_id, entry_price, entry_amount_usd, current_price, unrealized_pnl, opened_at, last_updated, take_profit_pct, stop_loss_pct, max_hold_minutes, status FROM positions WHERE status IN ('open', 'closing') ORDER BY opened_at"
        )
        rows = cursor.fetchall()
        return [
            Position(
                market_id=row[0],
                topic=row[1],
                side=row[2],
                token_id=row[3],
                entry_price=row[4],
                entry_amount_usd=row[5],
                current_price=row[6],
                unrealized_pnl=row[7],
                opened_at=_parse_dt(row[8]),
                last_updated=_parse_dt(row[9]),
                take_profit_pct=row[10],
                stop_loss_pct=row[11],
                max_hold_minutes=row[12],
                status=row[13],
            )
            for row in rows
        ]

    def get_held_market_ids(self) -> set[str]:
        """Get set of market IDs with open positions."""
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT DISTINCT market_id FROM positions WHERE status IN ('open', 'closing')"
        )
        return {row[0] for row in cursor.fetchall()}

    def get_total_exposure(self) -> float:
        """Get total exposure from open positions."""
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT COALESCE(SUM(entry_amount_usd), 0.0) FROM positions WHERE status IN ('open', 'closing')"
        )
        row = cursor.fetchone()
        return float(row[0]) if row else 0.0

    def upsert_wallet_registry_entries(
        self,
        entries: Iterable[WalletRegistryEntry],
    ) -> None:
        """Upsert wallet registry entries."""
        cursor = self.connection.cursor()
        rows = [
            (
                entry.wallet,
                entry.source_type,
                entry.source_ref,
                entry.trust_seed,
                entry.status,
                entry.first_seen_at.isoformat(),
                entry.last_seen_trade_at.isoformat()
                if entry.last_seen_trade_at
                else None,
                entry.last_scored_at.isoformat() if entry.last_scored_at else None,
                entry.notes,
            )
            for entry in entries
        ]
        if not rows:
            return
        cursor.executemany(
            """
            INSERT INTO wallet_registry (
                wallet,
                source_type,
                source_ref,
                trust_seed,
                status,
                first_seen_at,
                last_seen_trade_at,
                last_scored_at,
                notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(wallet) DO UPDATE SET
                source_type = excluded.source_type,
                source_ref = excluded.source_ref,
                trust_seed = excluded.trust_seed,
                status = excluded.status,
                first_seen_at = excluded.first_seen_at,
                last_seen_trade_at = excluded.last_seen_trade_at,
                last_scored_at = excluded.last_scored_at,
                notes = excluded.notes
            """,
            rows,
        )
        self.connection.commit()

    def load_wallet_registry_entries(self) -> list[WalletRegistryEntry]:
        """Load wallet registry entries."""
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT wallet, source_type, source_ref, trust_seed, status, first_seen_at, last_seen_trade_at, last_scored_at, notes FROM wallet_registry ORDER BY wallet"
        )
        rows = cursor.fetchall()
        return [
            WalletRegistryEntry(
                wallet=row[0],
                source_type=row[1],
                source_ref=row[2],
                trust_seed=float(row[3]),
                status=row[4],
                first_seen_at=_parse_dt(row[5]),
                last_seen_trade_at=_parse_dt(row[6]) if row[6] else None,
                last_scored_at=_parse_dt(row[7]) if row[7] else None,
                notes=row[8],
            )
            for row in rows
        ]

    def upsert_basket_memberships(
        self,
        memberships: Iterable[BasketMembership],
    ) -> None:
        """Upsert basket memberships."""
        cursor = self.connection.cursor()
        rows = [
            (
                membership.topic,
                membership.wallet,
                membership.tier,
                membership.rank,
                1 if membership.active else 0,
                membership.joined_at.isoformat(),
                membership.effective_until.isoformat()
                if membership.effective_until
                else None,
                membership.promotion_reason,
                membership.demotion_reason,
            )
            for membership in memberships
        ]
        if not rows:
            return
        cursor.executemany(
            """
            INSERT INTO basket_memberships (
                topic,
                wallet,
                tier,
                rank,
                active,
                joined_at,
                effective_until,
                promotion_reason,
                demotion_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(topic, wallet) DO UPDATE SET
                tier = excluded.tier,
                rank = excluded.rank,
                active = excluded.active,
                joined_at = excluded.joined_at,
                effective_until = excluded.effective_until,
                promotion_reason = excluded.promotion_reason,
                demotion_reason = excluded.demotion_reason
            """,
            rows,
        )
        self.connection.commit()

    def load_basket_memberships(
        self, topic: str | None = None
    ) -> list[BasketMembership]:
        """Load basket memberships, optionally scoped to a topic."""
        cursor = self.connection.cursor()
        if topic is None:
            cursor.execute(
                "SELECT topic, wallet, tier, rank, active, joined_at, effective_until, promotion_reason, demotion_reason FROM basket_memberships ORDER BY topic, rank, wallet"
            )
        else:
            cursor.execute(
                "SELECT topic, wallet, tier, rank, active, joined_at, effective_until, promotion_reason, demotion_reason FROM basket_memberships WHERE topic = ? ORDER BY rank, wallet",
                (topic,),
            )
        rows = cursor.fetchall()
        return [
            BasketMembership(
                topic=row[0],
                wallet=row[1],
                tier=row[2],
                rank=int(row[3]),
                active=bool(row[4]),
                joined_at=_parse_dt(row[5]),
                effective_until=_parse_dt(row[6]) if row[6] else None,
                promotion_reason=row[7],
                demotion_reason=row[8],
            )
            for row in rows
        ]

    def save_basket_health(self, health_snapshots: Iterable[BasketHealth]) -> None:
        """Save basket health snapshots."""
        cursor = self.connection.cursor()
        rows = [
            (
                health.topic,
                health.tracked_wallet_count,
                health.fresh_core_wallets_24h,
                health.fresh_active_wallets_7d,
                health.active_eligible_wallet_count,
                health.eligible_trades_7d,
                health.stale_ratio,
                health.clustered_ratio,
                health.health_state,
                health.captured_at.isoformat(),
            )
            for health in health_snapshots
        ]
        if not rows:
            return
        cursor.executemany(
            "INSERT INTO basket_health_snapshots (topic, tracked_wallet_count, fresh_core_wallets_24h, fresh_active_wallets_7d, active_eligible_wallet_count, eligible_trades_7d, stale_ratio, clustered_ratio, health_state, captured_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        self.connection.commit()

    def latest_basket_health(self) -> dict[str, BasketHealth]:
        """Load the latest basket health snapshot for each topic."""
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT topic, tracked_wallet_count, fresh_core_wallets_24h, fresh_active_wallets_7d, active_eligible_wallet_count, eligible_trades_7d, stale_ratio, clustered_ratio, health_state, captured_at FROM basket_health_snapshots ORDER BY topic, captured_at DESC, id DESC"
        )
        rows = cursor.fetchall()
        latest: dict[str, BasketHealth] = {}
        for row in rows:
            topic = row[0]
            if topic in latest:
                continue
            latest[topic] = BasketHealth(
                topic=topic,
                tracked_wallet_count=int(row[1]),
                fresh_core_wallets_24h=int(row[2]),
                fresh_active_wallets_7d=int(row[3]),
                active_eligible_wallet_count=int(row[4]),
                eligible_trades_7d=int(row[5]),
                stale_ratio=float(row[6]),
                clustered_ratio=float(row[7]),
                health_state=row[8],
                captured_at=_parse_dt(row[9]),
            )
        return latest

    def get_portfolio_var(
        self,
        confidence_level: float = 0.95,
        time_horizon_days: int = 1,
        use_monte_carlo: bool = True,
        simulations: int = 10000,
    ) -> float:
        """Calculate Value at Risk for current portfolio."""
        positions = self.get_open_positions()
        if not positions:
            logger.info("No open positions for VaR calculation")
            return 0.0

        total_exposure = sum(pos.entry_amount_usd for pos in positions)
        if total_exposure == 0:
            logger.info("Zero total exposure for VaR calculation")
            return 0.0

        if use_monte_carlo:
            try:
                var = self._monte_carlo_var(
                    positions,
                    confidence_level,
                    time_horizon_days,
                    simulations,
                )
                logger.info(
                    f"Monte Carlo VaR calculated: {var:.2f} USD at {confidence_level:.0%} confidence"
                )
                return var
            except (ImportError, ModuleNotFoundError) as exc:
                logger.warning(
                    f"Monte Carlo VaR unavailable ({exc}); falling back to analytical VaR"
                )

        position_vars = []
        for pos in positions:
            volatility = 0.02
            position_var = pos.entry_amount_usd * volatility * (time_horizon_days**0.5)
            position_vars.append(position_var)

        correlation = 0.3
        portfolio_volatility = (
            sum(value**2 for value in position_vars)
            + 2
            * correlation
            * sum(
                position_vars[i] * position_vars[j]
                for i in range(len(position_vars))
                for j in range(i + 1, len(position_vars))
            )
        ) ** 0.5

        z_score = {0.95: 1.645, 0.99: 2.326}.get(confidence_level, 1.645)
        var = portfolio_volatility * z_score
        logger.info(
            f"Analytical VaR calculated: {var:.2f} USD at {confidence_level:.0%} confidence"
        )
        return var

    def _monte_carlo_var(
        self,
        positions: list[Position],
        confidence_level: float,
        time_horizon_days: int,
        simulations: int,
    ) -> float:
        """Perform Monte Carlo simulation for VaR."""
        import math

        import numpy as np

        stress_volatility = 0.05
        correlation_matrix = np.full((len(positions), len(positions)), 0.3)
        np.fill_diagonal(correlation_matrix, 1.0)
        chol = np.linalg.cholesky(correlation_matrix)

        portfolio_losses = []
        for _ in range(simulations):
            uncorrelated = np.random.normal(0, 1, len(positions))
            correlated = chol @ uncorrelated
            returns = correlated * stress_volatility * math.sqrt(time_horizon_days)
            loss = sum(
                position.entry_amount_usd * (1 - np.exp(ret))
                for position, ret in zip(positions, returns)
            )
            portfolio_losses.append(loss)

        portfolio_losses.sort()
        var_index = int((1 - confidence_level) * simulations)
        raw_var = (
            portfolio_losses[var_index]
            if var_index < len(portfolio_losses)
            else portfolio_losses[-1]
        )
        var = round(abs(float(raw_var)), 4)
        logger.debug(f"Monte Carlo VaR simulation completed with {simulations} runs")
        return var

    def get_portfolio_summary(
        self,
        starting_bankroll_usd: float = 0.0,
    ) -> dict[str, float | int]:
        """Get portfolio summary statistics."""
        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT COALESCE(SUM(CASE WHEN status IN ('open', 'closing') THEN entry_amount_usd ELSE 0 END), 0.0),
                   COALESCE(SUM(CASE WHEN status IN ('open', 'closing') THEN unrealized_pnl ELSE 0 END), 0.0),
                   COALESCE(SUM(CASE WHEN status = 'closed' THEN unrealized_pnl ELSE 0 END), 0.0),
                   SUM(CASE WHEN status IN ('open', 'closing') THEN 1 ELSE 0 END),
                   SUM(CASE WHEN status = 'closed' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN status = 'closed' AND unrealized_pnl > 0 THEN 1 ELSE 0 END),
                   SUM(CASE WHEN status = 'closed' AND unrealized_pnl < 0 THEN 1 ELSE 0 END),
                   SUM(CASE WHEN status = 'closed' AND unrealized_pnl = 0 THEN 1 ELSE 0 END)
            FROM positions
            """
        )
        row = cursor.fetchone() or (0, 0, 0, 0, 0, 0, 0, 0)

        open_count = int(row[3] or 0)
        closed_count = int(row[4] or 0)
        wins = int(row[5] or 0)
        losses = int(row[6] or 0)
        breakeven = int(row[7] or 0)

        realized_pnl = round(float(row[2] or 0.0), 4)
        unrealized_pnl = round(float(row[1] or 0.0), 4)
        decisive_closed_count = wins + losses
        win_rate = (
            round(wins / decisive_closed_count, 4) if decisive_closed_count else 0.0
        )

        return {
            "starting_bankroll_usd": round(starting_bankroll_usd, 4),
            "current_exposure_usd": round(float(row[0] or 0.0), 4),
            "open_position_count": open_count,
            "closed_position_count": closed_count,
            "wins": wins,
            "losses": losses,
            "breakeven_count": breakeven,
            "win_rate": win_rate,
            "realized_pnl_usd": realized_pnl,
            "unrealized_pnl_usd": unrealized_pnl,
            "estimated_equity_usd": round(
                starting_bankroll_usd + realized_pnl + unrealized_pnl,
                4,
            ),
        }

    def filter_and_mark_candidates_atomically(
        self,
        candidates: Iterable[CopyCandidate],
        ttl_minutes: int = 1440,
    ) -> tuple[list[CopyCandidate], int]:
        """Return only fresh candidates and mark them seen in one transaction."""
        candidates = list(candidates)
        if not candidates:
            return [], 0

        now = datetime.now(UTC)
        cutoff = now - timedelta(minutes=ttl_minutes)
        fingerprints = [
            self.make_signal_fingerprint(
                candidate.market_id, candidate.topic, candidate.side
            )
            for candidate in candidates
        ]

        cursor = self.connection.cursor()
        cursor.execute("BEGIN IMMEDIATE")
        try:
            existing_recent: set[str] = set()
            chunk_size = 500
            for index in range(0, len(fingerprints), chunk_size):
                chunk = fingerprints[index : index + chunk_size]
                placeholders = ",".join("?" for _ in chunk)
                cursor.execute(
                    f"SELECT fingerprint, created_at FROM signal_fingerprints WHERE fingerprint IN ({placeholders})",
                    tuple(chunk),
                )
                for fingerprint, created_at in cursor.fetchall():
                    if _parse_dt(created_at) >= cutoff:
                        existing_recent.add(fingerprint)

            fresh: list[CopyCandidate] = []
            seen_in_batch: set[str] = set()
            rows_to_mark: list[tuple[str, str, str, str, str]] = []
            created_at = now.isoformat()
            for candidate, fingerprint in zip(candidates, fingerprints):
                if fingerprint in existing_recent or fingerprint in seen_in_batch:
                    continue
                fresh.append(candidate)
                seen_in_batch.add(fingerprint)
                rows_to_mark.append(
                    (
                        fingerprint,
                        candidate.market_id,
                        candidate.topic,
                        candidate.side,
                        created_at,
                    )
                )

            if rows_to_mark:
                cursor.executemany(
                    "INSERT OR REPLACE INTO signal_fingerprints (fingerprint, market_id, topic, side, created_at) VALUES (?, ?, ?, ?, ?)",
                    rows_to_mark,
                )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

        return fresh, len(candidates) - len(fresh)

    def mark_signals_seen(self, signals: Iterable[tuple[str, str, str]]) -> None:
        """Mark signals as seen to prevent duplicates."""
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
            "INSERT OR REPLACE INTO signal_fingerprints (fingerprint, market_id, topic, side, created_at) VALUES (?, ?, ?, ?, ?)",
            rows,
        )
        self.connection.commit()

    def update_position(
        self,
        market_id: str,
        current_price: float,
        unrealized_pnl: float,
        status: str,
        token_id: str | None = None,
    ) -> None:
        """Update an existing position."""
        cursor = self.connection.cursor()
        if token_id is not None:
            cursor.execute(
                "UPDATE positions SET current_price = ?, unrealized_pnl = ?, last_updated = ?, status = ? WHERE market_id = ? AND token_id = ? AND status IN ('open', 'closing')",
                (
                    current_price,
                    unrealized_pnl,
                    datetime.now(UTC).isoformat(),
                    status,
                    market_id,
                    token_id,
                ),
            )
        else:
            cursor.execute(
                "UPDATE positions SET current_price = ?, unrealized_pnl = ?, last_updated = ?, status = ? WHERE market_id = ? AND status IN ('open', 'closing')",
                (
                    current_price,
                    unrealized_pnl,
                    datetime.now(UTC).isoformat(),
                    status,
                    market_id,
                ),
            )
        self.connection.commit()

    def make_signal_fingerprint(self, market_id: str, topic: str, side: str) -> str:
        """Create unique fingerprint for a signal."""
        return f"{topic.strip().lower()}:{market_id.strip()}:{side.strip().upper()}"

    def has_recent_signal(
        self,
        market_id: str,
        topic: str,
        side: str,
        ttl_minutes: int = 1440,
    ) -> bool:
        """Check if a signal was recently seen."""
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

    def has_recent_signals(
        self,
        signals: Iterable[tuple[str, str, str]],
        ttl_minutes: int = 1440,
    ) -> set[str]:
        """Check which signals were recently seen (batch operation)."""
        signals = list(signals)
        if not signals:
            return set()

        cutoff = datetime.now(UTC) - timedelta(minutes=ttl_minutes)
        fingerprints = [
            self.make_signal_fingerprint(market_id, topic, side)
            for market_id, topic, side in signals
        ]

        recent: set[str] = set()
        cursor = self.connection.cursor()
        chunk_size = 500
        for index in range(0, len(fingerprints), chunk_size):
            chunk = fingerprints[index : index + chunk_size]
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
        """Mark a single signal as seen."""
        cursor = self.connection.cursor()
        fingerprint = self.make_signal_fingerprint(market_id, topic, side)
        cursor.execute(
            "INSERT OR REPLACE INTO signal_fingerprints (fingerprint, market_id, topic, side, created_at) VALUES (?, ?, ?, ?, ?)",
            (fingerprint, market_id, topic, side, datetime.now(UTC).isoformat()),
        )
        self.connection.commit()


def _parse_dt(value: str) -> datetime:
    """Parse ISO datetime string."""
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed

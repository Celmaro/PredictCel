from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from predictcel.models import (
    BasketHealth,
    BasketMembership,
    CopyCandidate,
    Position,
    WalletRegistryEntry,
)
from predictcel.storage import SignalStore


def make_position(
    market_id: str,
    status: str,
    amount: float = 25.0,
    token_id: str = "yes_token",
    entry_shares: float = 0.0,
    remaining_shares: float = 0.0,
) -> Position:
    now = datetime.now(UTC)
    return Position(
        market_id=market_id,
        topic="geopolitics",
        side="YES",
        token_id=token_id,
        entry_price=0.5,
        entry_amount_usd=amount,
        current_price=0.5,
        unrealized_pnl=0.0,
        opened_at=now,
        last_updated=now,
        take_profit_pct=0.3,
        stop_loss_pct=0.1,
        max_hold_minutes=1440,
        status=status,
        entry_shares=entry_shares,
        remaining_shares=remaining_shares,
    )


def make_candidate(market_id: str, side: str = "YES") -> CopyCandidate:
    return CopyCandidate(
        topic="geopolitics",
        market_id=market_id,
        side=side,
        consensus_ratio=0.8,
        reference_price=0.55,
        current_price=0.56,
        liquidity_usd=10000.0,
        source_wallets=["w1", "w2"],
        wallet_quality_score=0.75,
        copyability_score=0.81,
        reason="ok",
    )


def make_registry_entry(wallet: str, status: str = "active") -> WalletRegistryEntry:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return WalletRegistryEntry(
        wallet=wallet,
        source_type="static_basket",
        source_ref="config.baskets",
        trust_seed=1.0,
        status=status,
        first_seen_at=now,
        last_seen_trade_at=None,
        last_scored_at=None,
        notes="seeded",
    )


def make_membership(
    topic: str,
    wallet: str,
    tier: str = "core",
    rank: int = 1,
) -> BasketMembership:
    return BasketMembership(
        topic=topic,
        wallet=wallet,
        tier=tier,
        rank=rank,
        active=True,
        joined_at=datetime(2026, 1, 1, tzinfo=UTC),
        effective_until=None,
        promotion_reason="seeded",
        demotion_reason="",
    )


def make_health(topic: str, state: str, captured_at: datetime) -> BasketHealth:
    return BasketHealth(
        topic=topic,
        tracked_wallet_count=3,
        fresh_core_wallets_24h=1,
        fresh_active_wallets_7d=2,
        active_eligible_wallet_count=1,
        eligible_trades_7d=2,
        stale_ratio=0.3333,
        clustered_ratio=0.0,
        health_state=state,
        captured_at=captured_at,
    )


def make_store() -> tuple[SignalStore, Path]:
    db_path = Path(__file__).with_name(f".predictcel-test-{uuid4().hex}.db")
    return SignalStore(str(db_path)), db_path


def test_store_init_closes_connection_when_schema_setup_fails(monkeypatch) -> None:
    db_path = Path(__file__).with_name(f".predictcel-init-fail-{uuid4().hex}.db")
    observed = {"closed": False}

    class FakeConnection:
        def execute(self, _sql: str) -> None:
            return None

        def close(self) -> None:
            observed["closed"] = True

    monkeypatch.setattr("predictcel.storage.sqlite3.connect", lambda *args, **kwargs: FakeConnection())
    monkeypatch.setattr(
        SignalStore,
        "_init_schema",
        lambda self: (_ for _ in ()).throw(RuntimeError("schema setup failed")),
    )

    with pytest.raises(RuntimeError, match="schema setup failed"):
        SignalStore(str(db_path))

    assert observed["closed"] is True


def test_store_connect_uses_cross_thread_sqlite_settings(monkeypatch) -> None:
    db_path = Path(__file__).with_name(f".predictcel-connect-{uuid4().hex}.db")
    observed: dict[str, object] = {}

    class FakeConnection:
        def execute(self, _sql: str) -> None:
            return None

        def close(self) -> None:
            return None

    def fake_connect(*args, **kwargs):
        observed["args"] = args
        observed["kwargs"] = kwargs
        return FakeConnection()

    monkeypatch.setattr("predictcel.storage.sqlite3.connect", fake_connect)
    monkeypatch.setattr(SignalStore, "_init_schema", lambda self: None)

    store = SignalStore(str(db_path))
    store.close()

    assert observed["kwargs"]["check_same_thread"] is False
    assert observed["kwargs"]["timeout"] == 60.0


def test_active_positions_count_as_held_and_exposed() -> None:
    store, db_path = make_store()
    try:
        store.save_position(make_position("open_market", "open", 25.0))
        store.save_position(make_position("closing_market", "closing", 30.0))
        store.save_position(make_position("closed_market", "closed", 40.0))

        assert store.get_held_market_ids() == {"open_market", "closing_market"}
        assert store.get_total_exposure() == 55.0
        assert [position.market_id for position in store.get_open_positions()] == [
            "open_market",
            "closing_market",
        ]
    finally:
        store.connection.close()
        db_path.unlink(missing_ok=True)


def test_update_position_can_close_active_position() -> None:
    store, db_path = make_store()
    try:
        store.save_position(make_position("m1", "open", 25.0))

        store.update_position(
            "m1", current_price=0.6, unrealized_pnl=5.0, status="closed"
        )

        assert store.get_held_market_ids() == set()
        assert store.get_total_exposure() == 0.0
        assert store.get_open_positions() == []
    finally:
        store.connection.close()
        db_path.unlink(missing_ok=True)


def test_update_position_by_token_id_preserves_same_market_positions() -> None:
    store, db_path = make_store()
    try:
        store.save_position(make_position("m1", "open", 25.0, token_id="yes_token"))
        store.save_position(make_position("m1", "open", 30.0, token_id="no_token"))

        store.update_position(
            "m1",
            current_price=0.6,
            unrealized_pnl=5.0,
            status="closed",
            token_id="yes_token",
        )

        open_positions = store.get_open_positions()
        assert len(open_positions) == 1
        assert open_positions[0].token_id == "no_token"
        assert open_positions[0].status == "open"
    finally:
        store.connection.close()
        db_path.unlink(missing_ok=True)


def test_positions_round_trip_share_counts_and_close_updates_remaining_shares() -> None:
    store, db_path = make_store()
    try:
        store.save_position(
            make_position(
                "m1",
                "open",
                25.0,
                entry_shares=50.0,
                remaining_shares=12.5,
            )
        )

        open_positions = store.get_open_positions()

        assert open_positions[0].entry_shares == 50.0
        assert open_positions[0].remaining_shares == 12.5

        store.update_position(
            "m1",
            current_price=0.6,
            unrealized_pnl=5.0,
            status="closed",
            remaining_shares=0.0,
        )

        assert store.get_open_positions() == []
    finally:
        store.connection.close()
        db_path.unlink(missing_ok=True)


def test_open_orders_round_trip_by_client_order_id() -> None:
    store, db_path = make_store()
    try:
        store.upsert_open_order(
            market_id="m1",
            topic="geopolitics",
            side="YES",
            token_id="yes_token",
            order_type="FOK",
            price=0.55,
            amount=25.0,
            status="submitted",
            client_order_id="pc-123",
            order_id="order-123",
            filled_shares=0.0,
            avg_fill_price=0.0,
            payload='{"status":"submitted"}',
        )
        store.upsert_open_order(
            market_id="m1",
            topic="geopolitics",
            side="YES",
            token_id="yes_token",
            order_type="FOK",
            price=0.55,
            amount=25.0,
            status="filled",
            client_order_id="pc-123",
            order_id="order-123",
            filled_shares=50.0,
            avg_fill_price=0.5,
            payload='{"status":"filled"}',
        )

        record = store.get_open_order_by_client_id("pc-123")

        assert record is not None
        assert record["status"] == "filled"
        assert record["filled_shares"] == 50.0
        assert record["avg_fill_price"] == 0.5
    finally:
        store.connection.close()
        db_path.unlink(missing_ok=True)


def test_portfolio_summary_tracks_closed_winrate_and_pnl() -> None:
    store, db_path = make_store()
    try:
        store.save_position(make_position("winner", "open", 10.0))
        store.save_position(make_position("loser", "open", 10.0))
        store.save_position(make_position("active", "open", 10.0))
        store.update_position(
            "winner", current_price=0.7, unrealized_pnl=4.0, status="closed"
        )
        store.update_position(
            "loser", current_price=0.4, unrealized_pnl=-2.0, status="closed"
        )
        store.update_position(
            "active", current_price=0.55, unrealized_pnl=1.0, status="open"
        )

        summary = store.get_portfolio_summary(starting_bankroll_usd=100.0)

        assert summary["starting_bankroll_usd"] == 100.0
        assert summary["open_position_count"] == 1
        assert summary["closed_position_count"] == 2
        assert summary["wins"] == 1
        assert summary["losses"] == 1
        assert summary["breakeven_count"] == 0
        assert summary["win_rate"] == 0.5
        assert summary["realized_pnl_usd"] == 2.0
        assert summary["unrealized_pnl_usd"] == 1.0
        assert summary["estimated_equity_usd"] == 103.0
    finally:
        store.connection.close()
        db_path.unlink(missing_ok=True)


def test_portfolio_summary_tracks_breakeven_closed_positions_separately() -> None:
    store, db_path = make_store()
    try:
        store.save_position(make_position("winner", "open", 10.0))
        store.save_position(make_position("flat", "open", 10.0))
        store.update_position(
            "winner", current_price=0.7, unrealized_pnl=4.0, status="closed"
        )
        store.update_position(
            "flat", current_price=0.5, unrealized_pnl=0.0, status="closed"
        )

        summary = store.get_portfolio_summary(starting_bankroll_usd=100.0)

        assert summary["closed_position_count"] == 2
        assert summary["wins"] == 1
        assert summary["losses"] == 0
        assert summary["breakeven_count"] == 1
        assert summary["win_rate"] == 1.0
        assert summary["realized_pnl_usd"] == 4.0
    finally:
        store.connection.close()
        db_path.unlink(missing_ok=True)


def test_signal_fingerprints_prevent_recent_duplicates() -> None:
    store, db_path = make_store()
    try:
        assert store.has_recent_signal("m1", "GeoPolitics", "yes") is False

        store.mark_signal_seen("m1", "GeoPolitics", "yes")

        assert store.has_recent_signal("m1", "geopolitics", "YES") is True
        assert store.has_recent_signal("m1", "geopolitics", "NO") is False
    finally:
        store.connection.close()
        db_path.unlink(missing_ok=True)


def test_filter_and_mark_candidates_atomically_skips_recent_and_same_batch_duplicates() -> (
    None
):
    store, db_path = make_store()
    try:
        store.mark_signal_seen("m1", "geopolitics", "YES")

        fresh, skipped = store.filter_and_mark_candidates_atomically(
            [
                make_candidate("m1", "YES"),
                make_candidate("m2", "YES"),
                make_candidate("m2", "YES"),
                make_candidate("m3", "NO"),
            ]
        )

        assert [candidate.market_id for candidate in fresh] == ["m2", "m3"]
        assert [candidate.side for candidate in fresh] == ["YES", "NO"]
        assert skipped == 2
        assert store.has_recent_signal("m2", "geopolitics", "YES") is True
        assert store.has_recent_signal("m3", "geopolitics", "NO") is True
    finally:
        store.connection.close()
        db_path.unlink(missing_ok=True)


def test_signal_dedup_ttl_can_be_overridden_from_env(monkeypatch) -> None:
    store, db_path = make_store()
    try:
        monkeypatch.setenv("PREDICTCEL_SIGNAL_DEDUP_TTL_MINUTES", "0")
        store.mark_signal_seen("m1", "geopolitics", "YES")

        fresh, skipped = store.filter_and_mark_candidates_atomically(
            [make_candidate("m1", "YES")]
        )

        assert [candidate.market_id for candidate in fresh] == ["m1"]
        assert skipped == 0
    finally:
        store.connection.close()
        db_path.unlink(missing_ok=True)


def test_signal_store_handles_concurrent_signal_writes() -> None:
    store, db_path = make_store()
    try:
        def mark_and_read(index: int) -> tuple[bool, bool]:
            market_id = f"m{index}"
            store.mark_signal_seen(market_id, "geopolitics", "YES")
            seen_single = store.has_recent_signal(market_id, "geopolitics", "YES")
            seen_batch = bool(
                store.has_recent_signals([(market_id, "geopolitics", "YES")])
            )
            return seen_single, seen_batch

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(mark_and_read, range(40)))

        assert all(result == (True, True) for result in results)
        total_rows = store.connection.execute(
            "SELECT COUNT(*) FROM signal_fingerprints"
        ).fetchone()[0]
        assert total_rows == 40
    finally:
        store.connection.close()
        db_path.unlink(missing_ok=True)


def test_portfolio_var_falls_back_to_analytical_when_monte_carlo_unavailable() -> None:
    store, db_path = make_store()
    try:
        store.save_position(make_position("m1", "open", 25.0))
        store._monte_carlo_var = lambda *args, **kwargs: (_ for _ in ()).throw(
            ModuleNotFoundError("numpy")
        )

        var = store.get_portfolio_var(use_monte_carlo=True)

        assert var > 0.0
    finally:
        store.connection.close()
        db_path.unlink(missing_ok=True)


def test_wallet_registry_tables_round_trip_latest_state() -> None:
    store, db_path = make_store()
    try:
        store.upsert_wallet_registry_entries(
            [
                make_registry_entry("w1", status="active"),
                make_registry_entry("w2", status="probation"),
            ]
        )
        store.upsert_wallet_registry_entries(
            [make_registry_entry("w1", status="suspended")]
        )
        store.upsert_basket_memberships(
            [
                make_membership("geopolitics", "w1", tier="core", rank=1),
                make_membership("geopolitics", "w2", tier="rotating", rank=2),
                make_membership("sports", "w3", tier="backup", rank=1),
            ]
        )
        store.save_basket_health(
            [
                make_health("geopolitics", "stale", datetime(2026, 1, 1, tzinfo=UTC)),
                make_health("sports", "healthy", datetime(2026, 1, 1, tzinfo=UTC)),
            ]
        )
        store.save_basket_health(
            [make_health("geopolitics", "thin", datetime(2026, 1, 2, tzinfo=UTC))]
        )

        entries = store.load_wallet_registry_entries()
        memberships = store.load_basket_memberships()
        latest_health = store.latest_basket_health()

        assert {entry.wallet: entry.status for entry in entries} == {
            "w1": "suspended",
            "w2": "probation",
        }
        assert {
            (membership.topic, membership.wallet, membership.tier)
            for membership in memberships
        } == {
            ("geopolitics", "w1", "core"),
            ("geopolitics", "w2", "rotating"),
            ("sports", "w3", "backup"),
        }
        assert latest_health["geopolitics"].health_state == "thin"
        assert latest_health["sports"].health_state == "healthy"
    finally:
        store.connection.close()
        db_path.unlink(missing_ok=True)


def test_prune_history_limits_high_churn_tables() -> None:
    store, db_path = make_store()
    try:
        for index in range(5):
            store.mark_signal_seen(f"m{index}", "geopolitics", "YES")
            store.save_copy_candidates([make_candidate(f"copy_{index}")])

        pruned = store.prune_history(max_rows_per_table=2)

        remaining_copy_rows = store.connection.execute(
            "SELECT COUNT(*) FROM copy_candidates"
        ).fetchone()[0]
        remaining_signal_rows = store.connection.execute(
            "SELECT COUNT(*) FROM signal_fingerprints"
        ).fetchone()[0]

        assert pruned["copy_candidates"] == 3
        assert pruned["signal_fingerprints"] == 1
        assert remaining_copy_rows == 2
        assert remaining_signal_rows == 4
    finally:
        store.connection.close()
        db_path.unlink(missing_ok=True)

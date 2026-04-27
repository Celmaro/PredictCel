from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from predictcel.models import CopyCandidate, Position
from predictcel.storage import SignalStore


def make_position(market_id: str, status: str, amount: float = 25.0, token_id: str = "yes_token") -> Position:
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


def make_store() -> tuple[SignalStore, Path]:
    db_path = Path(__file__).with_name(f".predictcel-test-{uuid4().hex}.db")
    return SignalStore(str(db_path)), db_path


def test_active_positions_count_as_held_and_exposed() -> None:
    store, db_path = make_store()
    try:
        store.save_position(make_position("open_market", "open", 25.0))
        store.save_position(make_position("closing_market", "closing", 30.0))
        store.save_position(make_position("closed_market", "closed", 40.0))

        assert store.get_held_market_ids() == {"open_market", "closing_market"}
        assert store.get_total_exposure() == 55.0
        assert [position.market_id for position in store.get_open_positions()] == ["open_market", "closing_market"]
    finally:
        store.connection.close()
        db_path.unlink(missing_ok=True)


def test_update_position_can_close_active_position() -> None:
    store, db_path = make_store()
    try:
        store.save_position(make_position("m1", "open", 25.0))

        store.update_position("m1", current_price=0.6, unrealized_pnl=5.0, status="closed")

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

        store.update_position("m1", current_price=0.6, unrealized_pnl=5.0, status="closed", token_id="yes_token")

        open_positions = store.get_open_positions()
        assert len(open_positions) == 1
        assert open_positions[0].token_id == "no_token"
        assert open_positions[0].status == "open"
    finally:
        store.connection.close()
        db_path.unlink(missing_ok=True)


def test_portfolio_summary_tracks_closed_winrate_and_pnl() -> None:
    store, db_path = make_store()
    try:
        store.save_position(make_position("winner", "open", 10.0))
        store.save_position(make_position("loser", "open", 10.0))
        store.save_position(make_position("active", "open", 10.0))
        store.update_position("winner", current_price=0.7, unrealized_pnl=4.0, status="closed")
        store.update_position("loser", current_price=0.4, unrealized_pnl=-2.0, status="closed")
        store.update_position("active", current_price=0.55, unrealized_pnl=1.0, status="open")

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
        store.update_position("winner", current_price=0.7, unrealized_pnl=4.0, status="closed")
        store.update_position("flat", current_price=0.5, unrealized_pnl=0.0, status="closed")

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


def test_filter_and_mark_candidates_atomically_skips_recent_and_same_batch_duplicates() -> None:
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


def test_portfolio_var_falls_back_to_analytical_when_monte_carlo_unavailable() -> None:
    store, db_path = make_store()
    try:
        store.save_position(make_position("m1", "open", 25.0))
        store._monte_carlo_var = lambda *args, **kwargs: (_ for _ in ()).throw(ModuleNotFoundError("numpy"))

        var = store.get_portfolio_var(use_monte_carlo=True)

        assert var > 0.0
    finally:
        store.connection.close()
        db_path.unlink(missing_ok=True)

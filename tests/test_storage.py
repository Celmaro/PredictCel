from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from predictcel.models import Position
from predictcel.storage import SignalStore


def make_position(market_id: str, status: str, amount: float = 25.0) -> Position:
    now = datetime.now(UTC)
    return Position(
        market_id=market_id,
        topic="geopolitics",
        side="YES",
        token_id="yes_token",
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

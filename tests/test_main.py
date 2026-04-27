import sys

from predictcel import discover_wallets
from predictcel.main import _creates_or_updates_paper_position
from predictcel.models import ExecutionResult


def make_result(status: str, order_id: str = "order_123") -> ExecutionResult:
    return ExecutionResult(
        market_id="m1",
        topic="geopolitics",
        side="YES",
        token_id="yes_1",
        amount_usd=25.0,
        worst_price=0.52,
        status=status,
        order_id=order_id,
        error="",
        copyability_score=0.9,
        reason="test",
        market_title="One",
    )


def test_submitted_order_does_not_create_position() -> None:
    assert _creates_or_updates_paper_position(make_result("submitted")) is False


def test_filled_and_dry_run_results_create_positions() -> None:
    assert _creates_or_updates_paper_position(make_result("filled")) is True
    assert _creates_or_updates_paper_position(make_result("dry_run", order_id="")) is True


def test_discover_wallets_delegates_to_main(monkeypatch) -> None:
    observed = {}

    def fake_run_main() -> None:
        observed["argv"] = list(sys.argv)

    monkeypatch.setattr(discover_wallets, "run_main", fake_run_main)
    monkeypatch.setattr(sys, "argv", ["discover_wallets.py", "--config", "config/predictcel.example.json"])

    discover_wallets.main()

    assert observed["argv"] == [
        "discover_wallets.py",
        "discover-wallets",
        "--config",
        "config/predictcel.example.json",
    ]

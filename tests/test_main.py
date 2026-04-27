import sys
from types import SimpleNamespace

from predictcel import discover_wallets
from predictcel.main import (
    _creates_or_updates_paper_position,
    _filter_duplicate_candidates,
    _mark_execution_intents_seen,
)
from predictcel.models import CopyCandidate, ExecutionIntent, ExecutionResult


class FakeStore:
    def __init__(self, recent_fingerprints: set[str] | None = None) -> None:
        self.recent_fingerprints = recent_fingerprints or set()
        self.marked_signals = []

    def make_signal_fingerprint(self, market_id: str, topic: str, side: str) -> str:
        return f"{topic}:{market_id}:{side}"

    def has_recent_signals(self, signals, ttl_minutes: int = 1440):
        return {
            self.make_signal_fingerprint(market_id, topic, side)
            for market_id, topic, side in signals
            if self.make_signal_fingerprint(market_id, topic, side) in self.recent_fingerprints
        }

    def mark_signals_seen(self, signals) -> None:
        self.marked_signals.extend(list(signals))


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


def make_candidate(market_id: str, side: str = "YES") -> CopyCandidate:
    return CopyCandidate(
        topic="geopolitics",
        market_id=market_id,
        side=side,
        consensus_ratio=0.8,
        reference_price=0.5,
        current_price=0.5,
        liquidity_usd=10000,
        source_wallets=["w1"],
        wallet_quality_score=0.8,
        copyability_score=0.8,
        reason="test",
    )


def make_intent(market_id: str, side: str = "YES") -> ExecutionIntent:
    return ExecutionIntent(
        market_id=market_id,
        topic="geopolitics",
        side=side,
        token_id=f"token_{market_id}",
        amount_usd=25.0,
        worst_price=0.52,
        copyability_score=0.9,
        order_type="FOK",
        reason="test",
    )


def test_submitted_order_does_not_create_position() -> None:
    assert _creates_or_updates_paper_position(make_result("submitted")) is False


def test_filled_and_dry_run_results_create_positions() -> None:
    assert _creates_or_updates_paper_position(make_result("filled")) is True
    assert _creates_or_updates_paper_position(make_result("dry_run", order_id="")) is True


def test_filter_duplicate_candidates_skips_recent_and_same_batch_duplicates() -> None:
    store = FakeStore({"geopolitics:m1:YES"})

    fresh, skipped = _filter_duplicate_candidates(
        store,
        [
            make_candidate("m1", "YES"),
            make_candidate("m2", "YES"),
            make_candidate("m2", "YES"),
            make_candidate("m3", "NO"),
        ],
    )

    assert [(candidate.market_id, candidate.side) for candidate in fresh] == [("m2", "YES"), ("m3", "NO")]
    assert skipped == 2


def test_mark_execution_intents_seen_only_marks_planned_intents() -> None:
    store = FakeStore()

    _mark_execution_intents_seen(store, [make_intent("m2", "YES"), make_intent("m3", "NO")])

    assert store.marked_signals == [
        ("m2", "geopolitics", "YES"),
        ("m3", "geopolitics", "NO"),
    ]


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

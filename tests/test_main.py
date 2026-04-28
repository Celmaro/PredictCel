import sys

from predictcel import discover_wallets
from predictcel.main import (
    _compact_cycle_output,
    _creates_or_updates_paper_position,
    _filter_duplicate_candidates,
    _mark_execution_intents_seen,
    _probe_token_lookup,
    _probe_token_orderbook,
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


class ProbeSourceClient:
    gamma_base_url = "https://gamma-api.polymarket.com"
    data_base_url = "https://data-api.polymarket.com"
    clob_base_url = "https://clob.polymarket.com"
    timeout_seconds = 15
    max_retries = 1
    retry_base_delay_seconds = 0.5


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


def test_compact_cycle_output_includes_execution_state() -> None:
    compact = _compact_cycle_output(
        {
            "mode": "live",
            "summary": {"copy_candidates": 2, "execution_intents": 0},
            "latency_ms": {"total_cycle_ms": 10},
            "db": {"path": "/data/predictcel.db"},
            "portfolio_summary": {"current_exposure_usd": 0},
            "execution": {
                "live_trading_requested": False,
                "execution_enabled": True,
                "planner_ran": False,
                "diagnostics": {"candidates_seen": 2, "candidates_planned": 0},
            },
        }
    )

    assert compact["execution"] == {
        "live_trading_requested": False,
        "execution_enabled": True,
        "planner_ran": False,
        "diagnostics": {"candidates_seen": 2, "candidates_planned": 0},
    }


def test_probe_token_orderbook_uses_fresh_client(monkeypatch) -> None:
    observed = {}

    class FreshProbeClient:
        def __init__(self, gamma_base_url, data_base_url, clob_base_url, timeout_seconds, max_retries, retry_base_delay_seconds):
            observed["args"] = (
                gamma_base_url,
                data_base_url,
                clob_base_url,
                timeout_seconds,
                max_retries,
                retry_base_delay_seconds,
            )

        def fetch_order_book(self, token_id: str):
            raise RuntimeError(f"root cause for {token_id}")

    monkeypatch.setattr("predictcel.main.PolymarketPublicClient", FreshProbeClient)

    result = _probe_token_orderbook(ProbeSourceClient(), "token_yes")

    assert result == {"token_id": "token_yes", "error": "RuntimeError: root cause for token_yes"}
    assert observed["args"] == (
        "https://gamma-api.polymarket.com",
        "https://data-api.polymarket.com",
        "https://clob.polymarket.com",
        15,
        1,
        0.5,
    )


def test_probe_token_lookup_uses_fresh_client(monkeypatch) -> None:
    class FreshProbeClient:
        def __init__(self, *args):
            pass

        def fetch_markets_by_clob_token_ids(self, token_ids: list[str], chunk_size: int = 25):
            assert token_ids == ["token_yes"]
            assert chunk_size == 1
            return [{"conditionId": "cond_1", "question": "Question", "clobTokenIds": ["token_yes", "token_no"]}]

    monkeypatch.setattr("predictcel.main.PolymarketPublicClient", FreshProbeClient)

    result = _probe_token_lookup(ProbeSourceClient(), "token_yes")

    assert result == {
        "matched_rows": 1,
        "condition_id": "cond_1",
        "slug": "",
        "question": "Question",
        "token_ids": ["token_yes", "token_no"],
        "matched_input_token": True,
    }

import builtins
from dataclasses import replace
from io import BytesIO

from predictcel.config import (
    ArbitrageConfig,
    AppConfig,
    BasketRule,
    ConsensusConfig,
    ExecutionConfig,
    ExposureConfig,
    FilterConfig,
    LiveDataConfig,
    PositionConfig,
)
from predictcel import copy_engine as copy_engine_module
from predictcel.copy_engine import CopyEngine
from predictcel.models import MarketSnapshot, WalletQuality, WalletTrade


class CountingStore:
    def __init__(self, win_rate: float = 0.5) -> None:
        self.win_rate = win_rate
        self.calls = 0

    def get_portfolio_summary(self, starting_bankroll_usd: float = 0.0):
        self.calls += 1
        return {
            "starting_bankroll_usd": starting_bankroll_usd,
            "win_rate": self.win_rate,
        }


def make_config(consensus: ConsensusConfig | None = None) -> AppConfig:
    return AppConfig(
        baskets=[BasketRule(topic="geopolitics", wallets=["w1", "w2", "w3"], quorum_ratio=0.66)],
        filters=FilterConfig(
            max_trade_age_seconds=3600,
            max_price_drift=0.05,
            min_liquidity_usd=5000,
            min_minutes_to_resolution=60,
            max_minutes_to_resolution=1440,
            min_position_size_usd=100,
        ),
        arbitrage=ArbitrageConfig(min_gross_edge=0.02, min_liquidity_usd=5000),
        wallet_trades_path="",
        market_snapshots_path="",
        live_data=LiveDataConfig(
            enabled=False,
            gamma_base_url="https://gamma-api.polymarket.com",
            data_base_url="https://data-api.polymarket.com",
            clob_base_url="https://clob.polymarket.com",
            market_limit=100,
            trade_limit=10,
            request_timeout_seconds=15,
        ),
        execution=ExecutionConfig(
            enabled=False,
            dry_run=True,
            min_copyability_score=0.7,
            max_orders_per_run=2,
            buy_amount_usd=25.0,
            worst_price_buffer=0.02,
            order_type="FOK",
            chain_id=137,
            signature_type=0,
            position=PositionConfig(take_profit_pct=0.3, stop_loss_pct=0.1, max_hold_minutes=1440),
            exposure=ExposureConfig(max_total_exposure_usd=500.0, max_single_position_usd=50.0),
            max_retries=3,
            retry_base_delay_seconds=1.0,
        ),
        consensus=consensus or ConsensusConfig(min_confidence_score=0.35),
    )


def make_market() -> MarketSnapshot:
    return MarketSnapshot(
        market_id="m1",
        topic="unknown",
        title="Example",
        yes_ask=0.61,
        no_ask=0.42,
        best_bid=0.59,
        liquidity_usd=10000,
        minutes_to_resolution=180,
        yes_ask_size=250,
        no_ask_size=220,
        yes_spread=0.02,
        no_spread=0.03,
        orderbook_ready=True,
    )


def make_qualities() -> dict[str, WalletQuality]:
    return {
        "w1": WalletQuality(wallet="w1", topic="geopolitics", score=0.8, eligible_trade_count=3, average_age_seconds=500, average_drift=0.01, reason="test"),
        "w2": WalletQuality(wallet="w2", topic="geopolitics", score=0.7, eligible_trade_count=2, average_age_seconds=700, average_drift=0.02, reason="test"),
        "w3": WalletQuality(wallet="w3", topic="geopolitics", score=0.3, eligible_trade_count=2, average_age_seconds=900, average_drift=0.03, reason="test"),
    }


def test_emits_candidate_when_quorum_and_drift_pass() -> None:
    engine = CopyEngine(make_config())
    trades = [
        WalletTrade(wallet="w1", topic="geopolitics", market_id="m1", side="YES", price=0.58, size_usd=200, age_seconds=600),
        WalletTrade(wallet="w2", topic="geopolitics", market_id="m1", side="YES", price=0.6, size_usd=220, age_seconds=800),
    ]

    candidates = engine.evaluate(trades, {"m1": make_market()}, make_qualities())

    assert len(candidates) == 1
    assert candidates[0].side == "YES"
    assert candidates[0].market_id == "m1"
    assert candidates[0].market_title == "Example"
    assert candidates[0].topic == "geopolitics"
    assert candidates[0].wallet_quality_score == 0.75
    assert candidates[0].weighted_consensus >= 0.6
    assert candidates[0].confidence_score > 0
    assert candidates[0].recency_score > 0
    assert candidates[0].suggested_position_usd >= 0
    assert candidates[0].copyability_score > 0
    assert candidates[0].market_regime in {"RANGE", "TRANSITION", "TREND", "UNSTABLE"}
    assert candidates[0].regime_score > 0
    assert engine.last_diagnostics["candidates_returned"] == 1


def test_mixed_market_aliases_share_one_consensus_bucket() -> None:
    engine = CopyEngine(make_config())
    market = replace(make_market(), market_id="cond_1", yes_token_id="token_yes")
    trades = [
        WalletTrade(wallet="w1", topic="geopolitics", market_id="cond_1", side="YES", price=0.58, size_usd=200, age_seconds=600),
        WalletTrade(wallet="w2", topic="geopolitics", market_id="token_yes", side="YES", price=0.6, size_usd=220, age_seconds=800),
    ]

    candidates = engine.evaluate(trades, {"cond_1": market, "token_yes": market}, make_qualities())

    assert len(candidates) == 1
    assert candidates[0].market_id == "cond_1"
    assert engine.last_diagnostics["markets_evaluated"] == 1
    assert engine.last_diagnostics["candidates_returned"] == 1


def test_skips_candidate_when_drift_is_too_large() -> None:
    engine = CopyEngine(make_config())
    trades = [
        WalletTrade(wallet="w1", topic="geopolitics", market_id="m1", side="YES", price=0.4, size_usd=200, age_seconds=600),
        WalletTrade(wallet="w2", topic="geopolitics", market_id="m1", side="YES", price=0.41, size_usd=220, age_seconds=800),
    ]

    candidates = engine.evaluate(trades, {"m1": make_market()}, make_qualities())

    assert candidates == []
    assert engine.last_diagnostics["too_much_drift"] == 1


def test_skips_candidate_when_price_is_too_late() -> None:
    engine = CopyEngine(make_config())
    late_market = replace(make_market(), yes_ask=0.97)
    trades = [
        WalletTrade(wallet="w1", topic="geopolitics", market_id="m1", side="YES", price=0.90, size_usd=200, age_seconds=600),
        WalletTrade(wallet="w2", topic="geopolitics", market_id="m1", side="YES", price=0.91, size_usd=220, age_seconds=800),
    ]

    candidates = engine.evaluate(trades, {"m1": late_market}, make_qualities())

    assert candidates == []
    assert engine.last_diagnostics["too_late_price"] == 1


def test_weighted_consensus_can_reject_raw_quorum_with_stale_low_quality_votes() -> None:
    consensus = ConsensusConfig(min_weighted_consensus=0.75, min_confidence_score=0.20, recency_half_life_seconds=600)
    engine = CopyEngine(make_config(consensus))
    trades = [
        WalletTrade(wallet="w1", topic="geopolitics", market_id="m1", side="YES", price=0.59, size_usd=100, age_seconds=3400),
        WalletTrade(wallet="w2", topic="geopolitics", market_id="m1", side="YES", price=0.6, size_usd=100, age_seconds=3400),
        WalletTrade(wallet="w3", topic="geopolitics", market_id="m1", side="NO", price=0.41, size_usd=300, age_seconds=60),
    ]

    candidates = engine.evaluate(trades, {"m1": make_market()}, make_qualities())

    assert candidates == []
    assert engine.last_diagnostics["below_quorum"] == 1 or engine.last_diagnostics["below_weighted_consensus"] == 1


def test_conflicting_vote_reduces_copyability_score() -> None:
    consensus = ConsensusConfig(min_weighted_consensus=0.50, min_confidence_score=0.20, conflict_penalty_weight=0.25)
    engine = CopyEngine(make_config(consensus))
    base_trades = [
        WalletTrade(wallet="w1", topic="geopolitics", market_id="m1", side="YES", price=0.58, size_usd=200, age_seconds=300),
        WalletTrade(wallet="w2", topic="geopolitics", market_id="m1", side="YES", price=0.59, size_usd=200, age_seconds=300),
    ]
    conflict_trades = base_trades + [
        WalletTrade(wallet="w3", topic="geopolitics", market_id="m1", side="NO", price=0.41, size_usd=100, age_seconds=300),
    ]

    clean = engine.evaluate(base_trades, {"m1": make_market()}, make_qualities())[0]
    conflicted = engine.evaluate(conflict_trades, {"m1": make_market()}, make_qualities())[0]

    assert conflicted.conflict_penalty > clean.conflict_penalty
    assert conflicted.copyability_score < clean.copyability_score


def test_suggested_position_size_is_capped() -> None:
    consensus = ConsensusConfig(min_confidence_score=0.20, bankroll_usd=1000.0, kelly_fraction=1.0, max_suggested_position_usd=12.0)
    engine = CopyEngine(make_config(consensus))
    market = replace(make_market(), yes_ask=0.40)
    trades = [
        WalletTrade(wallet="w1", topic="geopolitics", market_id="m1", side="YES", price=0.39, size_usd=300, age_seconds=60),
        WalletTrade(wallet="w2", topic="geopolitics", market_id="m1", side="YES", price=0.40, size_usd=300, age_seconds=60),
    ]

    candidate = engine.evaluate(trades, {"m1": market}, make_qualities())[0]

    assert candidate.suggested_position_usd <= 12.0


def test_market_regime_detects_trend_and_unstable_books() -> None:
    engine = CopyEngine(make_config(ConsensusConfig(min_confidence_score=0.20)))
    trades = [
        WalletTrade(wallet="w1", topic="geopolitics", market_id="m1", side="YES", price=0.69, size_usd=300, age_seconds=60),
        WalletTrade(wallet="w2", topic="geopolitics", market_id="m1", side="YES", price=0.70, size_usd=300, age_seconds=60),
    ]
    trend_market = replace(make_market(), yes_ask=0.70, yes_spread=0.02, yes_ask_size=200)
    unstable_market = replace(make_market(), yes_ask=0.70, yes_spread=0.20, yes_ask_size=1)

    trend = engine.evaluate(trades, {"m1": trend_market}, make_qualities())[0]
    unstable = engine.evaluate(trades, {"m1": unstable_market}, make_qualities())[0]

    assert trend.market_regime == "TREND"
    assert unstable.market_regime == "UNSTABLE"
    assert trend.regime_score > unstable.regime_score


def test_candidate_reason_describes_scored_orderbook_inputs() -> None:
    engine = CopyEngine(make_config())
    trades = [
        WalletTrade(wallet="w1", topic="geopolitics", market_id="m1", side="YES", price=0.58, size_usd=200, age_seconds=600),
        WalletTrade(wallet="w2", topic="geopolitics", market_id="m1", side="YES", price=0.6, size_usd=220, age_seconds=800),
    ]

    candidate = engine.evaluate(trades, {"m1": make_market()}, make_qualities())[0]

    assert candidate.reason == "weighted basket consensus, market regime, confidence, recency, liquidity, drift, and scored orderbook inputs passed"


def test_records_market_and_basket_rejections() -> None:
    config = AppConfig(
        baskets=[BasketRule(topic="sports", wallets=["w9"], quorum_ratio=1.0)],
        filters=FilterConfig(
            max_trade_age_seconds=3600,
            max_price_drift=0.05,
            min_liquidity_usd=5000,
            min_minutes_to_resolution=60,
            max_minutes_to_resolution=1440,
            min_position_size_usd=100,
        ),
        arbitrage=ArbitrageConfig(min_gross_edge=0.02, min_liquidity_usd=5000),
        wallet_trades_path="",
        market_snapshots_path="",
        live_data=None,
        execution=None,
        consensus=ConsensusConfig(),
    )
    engine = CopyEngine(config)
    trades = [WalletTrade(wallet="w1", topic="geopolitics", market_id="missing", side="YES", price=0.5, size_usd=200, age_seconds=100)]

    engine.evaluate(trades, {"m1": make_market()}, {})

    assert engine.last_diagnostics["market_not_found"] == 1


def test_records_low_liquidity_rejection() -> None:
    engine = CopyEngine(make_config())
    low_liq_market = replace(make_market(), liquidity_usd=100)
    trades = [
        WalletTrade(wallet="w1", topic="geopolitics", market_id="m1", side="YES", price=0.58, size_usd=200, age_seconds=600),
        WalletTrade(wallet="w2", topic="geopolitics", market_id="m1", side="YES", price=0.6, size_usd=220, age_seconds=800),
    ]

    candidates = engine.evaluate(trades, {"m1": low_liq_market}, make_qualities())

    assert candidates == []
    assert engine.last_diagnostics["low_liquidity"] == 1


def test_evaluate_reads_portfolio_summary_once_before_threading() -> None:
    engine = CopyEngine(make_config(ConsensusConfig(min_confidence_score=0.20, bankroll_usd=1000.0, kelly_fraction=1.0, max_suggested_position_usd=12.0)))
    store = CountingStore(win_rate=0.65)
    markets = {
        "m1": replace(make_market(), market_id="m1", yes_ask=0.40),
        "m2": replace(make_market(), market_id="m2", yes_ask=0.41),
    }
    trades = [
        WalletTrade(wallet="w1", topic="geopolitics", market_id="m1", side="YES", price=0.39, size_usd=300, age_seconds=60),
        WalletTrade(wallet="w2", topic="geopolitics", market_id="m1", side="YES", price=0.40, size_usd=300, age_seconds=60),
        WalletTrade(wallet="w1", topic="geopolitics", market_id="m2", side="YES", price=0.40, size_usd=300, age_seconds=60),
        WalletTrade(wallet="w2", topic="geopolitics", market_id="m2", side="YES", price=0.41, size_usd=300, age_seconds=60),
    ]

    candidates = engine.evaluate(trades, markets, make_qualities(), store)

    assert len(candidates) == 2
    assert store.calls == 1
    assert all(candidate.suggested_position_usd > 0 for candidate in candidates)


def test_evaluate_with_no_trades_returns_empty_diagnostics() -> None:
    engine = CopyEngine(make_config())
    candidates = engine.evaluate([], {}, {})

    assert candidates == []
    assert engine.last_diagnostics["markets_evaluated"] == 0
    assert engine.last_diagnostics["candidates_returned"] == 0


def test_copy_engine_ignores_cwd_pickle_fallback(monkeypatch) -> None:
    cwd_model = copy_engine_module.os.path.abspath("position_sizing_model.pkl")
    loaded = {"called": False}

    def fake_exists(path: str) -> bool:
        return path == cwd_model

    def fake_load(handle):
        loaded["called"] = True
        return object()

    monkeypatch.setattr(copy_engine_module.os.path, "exists", fake_exists)
    monkeypatch.setattr(copy_engine_module.os.path, "abspath", lambda path: cwd_model)
    monkeypatch.setattr(copy_engine_module.pickle, "load", fake_load)
    monkeypatch.setattr(builtins, "open", lambda *args, **kwargs: BytesIO(b"pickle-bytes"))

    engine = CopyEngine(make_config())

    assert engine._ml_model is None
    assert loaded["called"] is False

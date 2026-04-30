import builtins
from datetime import UTC, datetime
from dataclasses import replace
from io import BytesIO
import threading

from predictcel import copy_engine as copy_engine_module
from predictcel.config import (
    ArbitrageConfig,
    AppConfig,
    BasketControllerConfig,
    BasketRule,
    ConsensusConfig,
    ExecutionConfig,
    ExposureConfig,
    FilterConfig,
    LiveDataConfig,
    PositionConfig,
)
from predictcel.copy_engine import CopyEngine
from predictcel.models import (
    BasketMembership,
    MarketSnapshot,
    WalletQuality,
    WalletRegistryEntry,
    WalletTrade,
)


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


class MembershipStore(CountingStore):
    def __init__(
        self,
        memberships: list[BasketMembership],
        registry_entries: list[WalletRegistryEntry] | None = None,
    ) -> None:
        super().__init__()
        self.memberships = memberships
        self.registry_entries = registry_entries or []

    def load_basket_memberships(
        self, topic: str | None = None
    ) -> list[BasketMembership]:
        if topic is None:
            return list(self.memberships)
        return [
            membership for membership in self.memberships if membership.topic == topic
        ]

    def load_wallet_registry_entries(self) -> list[WalletRegistryEntry]:
        return list(self.registry_entries)


def make_config(
    consensus: ConsensusConfig | None = None,
    wallets: list[str] | None = None,
    basket_controller: BasketControllerConfig | None = None,
) -> AppConfig:
    wallets = wallets or ["w1", "w2", "w3"]
    return AppConfig(
        baskets=[
            BasketRule(
                topic="geopolitics",
                wallets=wallets,
                quorum_ratio=0.66,
            )
        ],
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
            position=PositionConfig(
                take_profit_pct=0.3,
                stop_loss_pct=0.1,
                max_hold_minutes=1440,
            ),
            exposure=ExposureConfig(
                max_total_exposure_usd=500.0,
                max_single_position_usd=50.0,
            ),
            max_retries=3,
            retry_base_delay_seconds=1.0,
        ),
        consensus=consensus or ConsensusConfig(min_confidence_score=0.35),
        basket_controller=basket_controller or BasketControllerConfig(),
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
        "w1": WalletQuality(
            wallet="w1",
            topic="geopolitics",
            score=0.8,
            eligible_trade_count=3,
            average_age_seconds=500,
            average_drift=0.01,
            reason="test",
        ),
        "w2": WalletQuality(
            wallet="w2",
            topic="geopolitics",
            score=0.7,
            eligible_trade_count=2,
            average_age_seconds=700,
            average_drift=0.02,
            reason="test",
        ),
        "w3": WalletQuality(
            wallet="w3",
            topic="geopolitics",
            score=0.3,
            eligible_trade_count=2,
            average_age_seconds=900,
            average_drift=0.03,
            reason="test",
        ),
        "w4": WalletQuality(
            wallet="w4",
            topic="geopolitics",
            score=0.75,
            eligible_trade_count=2,
            average_age_seconds=800,
            average_drift=0.02,
            reason="test",
        ),
        "w5": WalletQuality(
            wallet="w5",
            topic="geopolitics",
            score=0.7,
            eligible_trade_count=2,
            average_age_seconds=850,
            average_drift=0.02,
            reason="test",
        ),
    }


def make_memberships(wallets: list[str], tier: str = "core") -> list[BasketMembership]:
    return [
        BasketMembership(
            topic="geopolitics",
            wallet=wallet,
            tier=tier,
            rank=index,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="test",
            demotion_reason="",
        )
        for index, wallet in enumerate(wallets, start=1)
    ]


def test_emits_candidate_when_quorum_and_drift_pass() -> None:
    engine = CopyEngine(make_config())
    trades = [
        WalletTrade(
            wallet="w1",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.58,
            size_usd=200,
            age_seconds=600,
        ),
        WalletTrade(
            wallet="w2",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.6,
            size_usd=220,
            age_seconds=800,
        ),
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
        WalletTrade(
            wallet="w1",
            topic="geopolitics",
            market_id="cond_1",
            side="YES",
            price=0.58,
            size_usd=200,
            age_seconds=600,
        ),
        WalletTrade(
            wallet="w2",
            topic="geopolitics",
            market_id="token_yes",
            side="YES",
            price=0.6,
            size_usd=220,
            age_seconds=800,
        ),
    ]

    candidates = engine.evaluate(
        trades,
        {"cond_1": market, "token_yes": market},
        make_qualities(),
    )

    assert len(candidates) == 1
    assert candidates[0].market_id == "cond_1"
    assert engine.last_diagnostics["markets_evaluated"] == 1
    assert engine.last_diagnostics["candidates_returned"] == 1


def test_market_alias_matching_is_case_insensitive_for_token_ids() -> None:
    engine = CopyEngine(make_config())
    market = replace(make_market(), market_id="cond_1", yes_token_id="token_yes")
    trades = [
        WalletTrade(
            wallet="w1",
            topic="geopolitics",
            market_id="TOKEN_YES",
            side="YES",
            price=0.58,
            size_usd=200,
            age_seconds=600,
        ),
        WalletTrade(
            wallet="w2",
            topic="geopolitics",
            market_id="cond_1",
            side="YES",
            price=0.6,
            size_usd=220,
            age_seconds=800,
        ),
    ]

    candidates = engine.evaluate(
        trades,
        {"cond_1": market, "token_yes": market},
        make_qualities(),
    )

    assert len(candidates) == 1
    assert candidates[0].market_id == "cond_1"
    assert engine.last_diagnostics["markets_evaluated"] == 1
    assert engine.last_diagnostics["market_not_found"] == 0


def test_skips_candidate_when_drift_is_too_large() -> None:
    engine = CopyEngine(make_config())
    trades = [
        WalletTrade(
            wallet="w1",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.4,
            size_usd=200,
            age_seconds=600,
        ),
        WalletTrade(
            wallet="w2",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.41,
            size_usd=220,
            age_seconds=800,
        ),
    ]

    candidates = engine.evaluate(trades, {"m1": make_market()}, make_qualities())

    assert candidates == []
    assert engine.last_diagnostics["too_much_drift"] == 1


def test_skips_candidate_when_orderbook_is_not_ready() -> None:
    engine = CopyEngine(make_config())
    market = replace(make_market(), orderbook_ready=False)
    trades = [
        WalletTrade(
            wallet="w1",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.58,
            size_usd=200,
            age_seconds=600,
        ),
        WalletTrade(
            wallet="w2",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.6,
            size_usd=220,
            age_seconds=800,
        ),
    ]

    candidates = engine.evaluate(trades, {"m1": market}, make_qualities())

    assert candidates == []
    assert engine.last_diagnostics["orderbook_not_ready"] == 1


def test_skips_candidate_when_selected_side_has_no_depth() -> None:
    engine = CopyEngine(make_config())
    market = replace(make_market(), yes_ask_size=0)
    trades = [
        WalletTrade(
            wallet="w1",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.58,
            size_usd=200,
            age_seconds=600,
        ),
        WalletTrade(
            wallet="w2",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.6,
            size_usd=220,
            age_seconds=800,
        ),
    ]

    candidates = engine.evaluate(trades, {"m1": market}, make_qualities())

    assert candidates == []
    assert engine.last_diagnostics["insufficient_side_depth"] == 1


def test_skips_candidate_when_price_is_too_late() -> None:
    engine = CopyEngine(make_config())
    late_market = replace(make_market(), yes_ask=0.97)
    trades = [
        WalletTrade(
            wallet="w1",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.90,
            size_usd=200,
            age_seconds=600,
        ),
        WalletTrade(
            wallet="w2",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.91,
            size_usd=220,
            age_seconds=800,
        ),
    ]

    candidates = engine.evaluate(trades, {"m1": late_market}, make_qualities())

    assert candidates == []
    assert engine.last_diagnostics["too_late_price"] == 1


def test_weighted_consensus_can_reject_raw_quorum_with_stale_low_quality_votes() -> (
    None
):
    consensus = ConsensusConfig(
        min_weighted_consensus=0.75,
        min_confidence_score=0.20,
        recency_half_life_seconds=600,
    )
    engine = CopyEngine(make_config(consensus))
    trades = [
        WalletTrade(
            wallet="w1",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.59,
            size_usd=100,
            age_seconds=3400,
        ),
        WalletTrade(
            wallet="w2",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.6,
            size_usd=100,
            age_seconds=3400,
        ),
        WalletTrade(
            wallet="w3",
            topic="geopolitics",
            market_id="m1",
            side="NO",
            price=0.41,
            size_usd=300,
            age_seconds=60,
        ),
    ]

    candidates = engine.evaluate(trades, {"m1": make_market()}, make_qualities())

    assert candidates == []
    assert (
        engine.last_diagnostics["below_quorum"] == 1
        or engine.last_diagnostics["below_weighted_consensus"] == 1
    )


def test_copy_engine_honors_env_max_entry_price_override(monkeypatch) -> None:
    monkeypatch.setenv("PREDICTCEL_MAX_ENTRY_PRICE", "0.97")
    engine = CopyEngine(make_config())
    late_market = replace(make_market(), yes_ask=0.96, best_bid=0.94)
    trades = [
        WalletTrade(
            wallet="w1",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.95,
            size_usd=200,
            age_seconds=600,
        ),
        WalletTrade(
            wallet="w2",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.95,
            size_usd=220,
            age_seconds=800,
        ),
    ]

    candidates = engine.evaluate(trades, {"m1": late_market}, make_qualities())

    assert len(candidates) == 1
    assert candidates[0].market_id == "m1"


def test_conflicting_vote_reduces_copyability_score() -> None:
    consensus = ConsensusConfig(
        min_weighted_consensus=0.50,
        min_confidence_score=0.20,
        conflict_penalty_weight=0.25,
    )
    engine = CopyEngine(make_config(consensus))
    base_trades = [
        WalletTrade(
            wallet="w1",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.58,
            size_usd=200,
            age_seconds=300,
        ),
        WalletTrade(
            wallet="w2",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.59,
            size_usd=200,
            age_seconds=300,
        ),
    ]
    conflict_trades = base_trades + [
        WalletTrade(
            wallet="w3",
            topic="geopolitics",
            market_id="m1",
            side="NO",
            price=0.41,
            size_usd=100,
            age_seconds=300,
        ),
    ]

    clean = engine.evaluate(base_trades, {"m1": make_market()}, make_qualities())[0]
    conflicted = engine.evaluate(
        conflict_trades,
        {"m1": make_market()},
        make_qualities(),
    )[0]

    assert conflicted.conflict_penalty > clean.conflict_penalty
    assert conflicted.copyability_score < clean.copyability_score


def test_consensus_prefers_wallet_quality_over_trade_size_for_vote_direction() -> None:
    consensus = ConsensusConfig(min_weighted_consensus=0.50, min_confidence_score=0.20)
    engine = CopyEngine(make_config(consensus))
    trades = [
        WalletTrade("w1", "geopolitics", "m1", "YES", 0.58, 100, 60),
        WalletTrade("w2", "geopolitics", "m1", "YES", 0.59, 110, 90),
        WalletTrade("w3", "geopolitics", "m1", "NO", 0.41, 2000, 60),
    ]

    candidate = engine.evaluate(trades, {"m1": make_market()}, make_qualities())[0]

    assert candidate.side == "YES"


def test_concentrated_agreement_reduces_confidence_and_position_size() -> None:
    consensus = ConsensusConfig(
        min_weighted_consensus=0.50,
        min_confidence_score=0.20,
        bankroll_usd=1000.0,
        kelly_fraction=1.0,
        max_suggested_position_usd=25.0,
    )
    engine = CopyEngine(make_config(consensus))
    balanced_trades = [
        WalletTrade("w1", "geopolitics", "m1", "YES", 0.58, 200, 60),
        WalletTrade("w2", "geopolitics", "m1", "YES", 0.59, 200, 60),
    ]
    concentrated_trades = [
        WalletTrade("w1", "geopolitics", "m1", "YES", 0.58, 200, 60),
        WalletTrade("w3", "geopolitics", "m1", "YES", 0.59, 200, 1500),
    ]

    balanced = engine.evaluate(
        balanced_trades, {"m1": make_market()}, make_qualities()
    )[0]
    concentrated = engine.evaluate(
        concentrated_trades, {"m1": make_market()}, make_qualities()
    )[0]

    assert concentrated.confidence_score < balanced.confidence_score
    assert concentrated.suggested_position_usd < balanced.suggested_position_usd


def test_suggested_position_size_is_capped() -> None:
    consensus = ConsensusConfig(
        min_confidence_score=0.20,
        bankroll_usd=1000.0,
        kelly_fraction=1.0,
        max_suggested_position_usd=12.0,
    )
    engine = CopyEngine(make_config(consensus))
    market = replace(make_market(), yes_ask=0.40)
    trades = [
        WalletTrade(
            wallet="w1",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.39,
            size_usd=300,
            age_seconds=60,
        ),
        WalletTrade(
            wallet="w2",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.40,
            size_usd=300,
            age_seconds=60,
        ),
    ]

    candidate = engine.evaluate(trades, {"m1": market}, make_qualities())[0]

    assert candidate.suggested_position_usd <= 12.0


def test_market_regime_detects_trend_and_unstable_books() -> None:
    engine = CopyEngine(make_config(ConsensusConfig(min_confidence_score=0.20)))
    trades = [
        WalletTrade(
            wallet="w1",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.69,
            size_usd=300,
            age_seconds=60,
        ),
        WalletTrade(
            wallet="w2",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.70,
            size_usd=300,
            age_seconds=60,
        ),
    ]
    trend_market = replace(
        make_market(), yes_ask=0.70, yes_spread=0.02, yes_ask_size=200
    )
    unstable_market = replace(
        make_market(), yes_ask=0.70, yes_spread=0.20, yes_ask_size=1
    )

    trend = engine.evaluate(trades, {"m1": trend_market}, make_qualities())[0]
    unstable = engine.evaluate(trades, {"m1": unstable_market}, make_qualities())[0]

    assert trend.market_regime == "TREND"
    assert unstable.market_regime == "UNSTABLE"
    assert trend.regime_score > unstable.regime_score


def test_candidate_reason_describes_tradable_orderbook_inputs() -> None:
    engine = CopyEngine(make_config())
    trades = [
        WalletTrade(
            wallet="w1",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.58,
            size_usd=200,
            age_seconds=600,
        ),
        WalletTrade(
            wallet="w2",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.6,
            size_usd=220,
            age_seconds=800,
        ),
    ]

    candidate = engine.evaluate(trades, {"m1": make_market()}, make_qualities())[0]

    assert (
        candidate.reason
        == "weighted basket consensus, market regime, confidence, recency, liquidity, drift, and tradable orderbook inputs passed"
    )


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
    trades = [
        WalletTrade(
            wallet="w1",
            topic="geopolitics",
            market_id="missing",
            side="YES",
            price=0.5,
            size_usd=200,
            age_seconds=100,
        )
    ]

    engine.evaluate(trades, {"m1": make_market()}, {})

    assert engine.last_diagnostics["market_not_found"] == 1


def test_records_low_liquidity_rejection() -> None:
    engine = CopyEngine(make_config())
    low_liq_market = replace(make_market(), liquidity_usd=100)
    trades = [
        WalletTrade(
            wallet="w1",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.58,
            size_usd=200,
            age_seconds=600,
        ),
        WalletTrade(
            wallet="w2",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.6,
            size_usd=220,
            age_seconds=800,
        ),
    ]

    candidates = engine.evaluate(trades, {"m1": low_liq_market}, make_qualities())

    assert candidates == []
    assert engine.last_diagnostics["low_liquidity"] == 1


def test_records_no_valid_trade_breakdown() -> None:
    engine = CopyEngine(make_config())
    trades = [
        WalletTrade(
            wallet="w1",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.58,
            size_usd=200,
            age_seconds=7200,
        ),
        WalletTrade(
            wallet="w2",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.6,
            size_usd=50,
            age_seconds=800,
        ),
        WalletTrade(
            wallet="w9",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.59,
            size_usd=200,
            age_seconds=600,
        ),
        WalletTrade(
            wallet="w1",
            topic="sports",
            market_id="m1",
            side="YES",
            price=0.59,
            size_usd=200,
            age_seconds=600,
        ),
    ]

    candidates = engine.evaluate(trades, {"m1": make_market()}, make_qualities())

    assert candidates == []
    assert engine.last_diagnostics["no_valid_trades"] == 1
    assert engine.last_diagnostics["no_valid_trades_too_old"] == 1
    assert engine.last_diagnostics["no_valid_trades_too_small"] == 1
    assert engine.last_diagnostics["no_valid_trades_wallet_not_in_basket"] == 1
    assert engine.last_diagnostics["no_valid_trades_topic_mismatch"] == 1


def test_evaluate_reads_portfolio_summary_once_before_threading() -> None:
    engine = CopyEngine(
        make_config(
            ConsensusConfig(
                min_confidence_score=0.20,
                bankroll_usd=1000.0,
                kelly_fraction=1.0,
                max_suggested_position_usd=12.0,
            )
        )
    )
    store = CountingStore(win_rate=0.65)
    markets = {
        "m1": replace(make_market(), market_id="m1", yes_ask=0.40),
        "m2": replace(make_market(), market_id="m2", yes_ask=0.41),
    }
    trades = [
        WalletTrade(
            wallet="w1",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.39,
            size_usd=300,
            age_seconds=60,
        ),
        WalletTrade(
            wallet="w2",
            topic="geopolitics",
            market_id="m1",
            side="YES",
            price=0.40,
            size_usd=300,
            age_seconds=60,
        ),
        WalletTrade(
            wallet="w1",
            topic="geopolitics",
            market_id="m2",
            side="YES",
            price=0.40,
            size_usd=300,
            age_seconds=60,
        ),
        WalletTrade(
            wallet="w2",
            topic="geopolitics",
            market_id="m2",
            side="YES",
            price=0.41,
            size_usd=300,
            age_seconds=60,
        ),
    ]

    candidates = engine.evaluate(trades, markets, make_qualities(), store)

    assert len(candidates) == 2
    assert store.calls == 1
    assert all(candidate.suggested_position_usd > 0 for candidate in candidates)


def test_evaluate_does_not_pass_store_into_worker_thread_consensus_gate(
    monkeypatch,
) -> None:
    engine = CopyEngine(
        make_config(
            wallets=["w1", "w2", "w3"],
            basket_controller=BasketControllerConfig(
                enabled=True,
                tracked_basket_target=3,
                core_slots=2,
                rotating_slots=1,
                backup_slots=0,
                explorer_slots=0,
                min_basket_participation_ratio=0.66,
                min_weighted_participation_ratio=0.6,
                min_active_eligible_wallets=2,
                min_aligned_wallet_count=2,
                max_entry_price_band_abs=0.03,
                max_entry_time_spread_seconds=600,
            ),
        )
    )
    owner_thread = threading.get_ident()

    class ThreadBoundStore(MembershipStore):
        def get_portfolio_summary(self, starting_bankroll_usd: float = 0.0):
            assert threading.get_ident() == owner_thread
            return super().get_portfolio_summary(starting_bankroll_usd)

        def load_basket_memberships(
            self, topic: str | None = None
        ) -> list[BasketMembership]:
            assert threading.get_ident() == owner_thread
            return super().load_basket_memberships(topic)

        def load_wallet_registry_entries(self) -> list[WalletRegistryEntry]:
            assert threading.get_ident() == owner_thread
            return super().load_wallet_registry_entries()

    memberships = make_memberships(["w1", "w2", "w3"])
    store = ThreadBoundStore(memberships)
    observed = {"calls": 0}
    original_gate = copy_engine_module.evaluate_basket_consensus_gate

    def wrapped_gate(*args, **kwargs):
        observed["calls"] += 1
        assert kwargs.get("store") is None
        return original_gate(*args, **kwargs)

    monkeypatch.setattr(
        copy_engine_module, "evaluate_basket_consensus_gate", wrapped_gate
    )

    markets = {
        "m1": replace(make_market(), market_id="m1"),
        "m2": replace(make_market(), market_id="m2"),
    }
    trades = [
        WalletTrade("w1", "geopolitics", "m1", "YES", 0.58, 200, 60),
        WalletTrade("w2", "geopolitics", "m1", "YES", 0.59, 220, 120),
        WalletTrade("w1", "geopolitics", "m2", "YES", 0.58, 200, 60),
        WalletTrade("w2", "geopolitics", "m2", "YES", 0.59, 220, 120),
    ]

    candidates = engine.evaluate(trades, markets, make_qualities(), store)

    assert len(candidates) == 2
    assert observed["calls"] == 2


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
    monkeypatch.setattr(
        builtins,
        "open",
        lambda *args, **kwargs: BytesIO(b"pickle-bytes"),
    )

    engine = CopyEngine(make_config())

    assert engine._ml_model is None
    assert loaded["called"] is False


def test_copy_engine_logs_pickle_load_failures(monkeypatch, caplog) -> None:
    model_path = copy_engine_module.os.path.join(
        copy_engine_module.os.path.dirname(copy_engine_module.__file__),
        "position_sizing_model.pkl",
    )

    def fake_exists(path: str) -> bool:
        return path == model_path

    def fake_load(handle):
        del handle
        raise ValueError("corrupt model")

    monkeypatch.setattr(copy_engine_module.os.path, "exists", fake_exists)
    monkeypatch.setattr(copy_engine_module.pickle, "load", fake_load)
    monkeypatch.setattr(
        builtins,
        "open",
        lambda *args, **kwargs: BytesIO(b"pickle-bytes"),
    )

    with caplog.at_level("WARNING"):
        engine = CopyEngine(make_config())

    assert engine._ml_model is None
    assert "Failed to load ML position sizing model" in caplog.text


def test_basket_controller_requires_80_percent_same_outcome() -> None:
    wallets = ["w1", "w2", "w3", "w4", "w5"]
    engine = CopyEngine(
        make_config(
            wallets=wallets,
            basket_controller=BasketControllerConfig(
                enabled=True,
                tracked_basket_target=5,
                core_slots=5,
                rotating_slots=0,
                backup_slots=0,
                explorer_slots=0,
                min_basket_participation_ratio=0.8,
                min_weighted_participation_ratio=0.8,
                min_active_eligible_wallets=5,
                min_aligned_wallet_count=4,
                max_entry_price_band_abs=0.03,
                max_entry_time_spread_seconds=600,
            ),
        )
    )
    trades = [
        WalletTrade("w1", "geopolitics", "m1", "YES", 0.58, 200, 100),
        WalletTrade("w2", "geopolitics", "m1", "YES", 0.59, 220, 120),
        WalletTrade("w3", "geopolitics", "m1", "YES", 0.60, 180, 140),
        WalletTrade("w4", "geopolitics", "m1", "YES", 0.60, 210, 160),
        WalletTrade("w5", "geopolitics", "m1", "NO", 0.41, 100, 180),
    ]

    candidates = engine.evaluate(
        trades,
        {"m1": make_market()},
        make_qualities(),
        MembershipStore(make_memberships(wallets)),
    )

    assert len(candidates) == 1
    assert candidates[0].consensus_ratio == 0.8
    assert candidates[0].source_wallets == ["w1", "w2", "w3", "w4"]


def test_basket_controller_rejects_when_same_outcome_ratio_is_below_threshold() -> None:
    wallets = ["w1", "w2", "w3", "w4", "w5"]
    engine = CopyEngine(
        make_config(
            wallets=wallets,
            basket_controller=BasketControllerConfig(
                enabled=True,
                tracked_basket_target=5,
                core_slots=5,
                rotating_slots=0,
                backup_slots=0,
                explorer_slots=0,
                min_basket_participation_ratio=0.8,
                min_weighted_participation_ratio=0.8,
                min_active_eligible_wallets=5,
                min_aligned_wallet_count=4,
                max_entry_price_band_abs=0.03,
                max_entry_time_spread_seconds=600,
            ),
        )
    )
    trades = [
        WalletTrade("w1", "geopolitics", "m1", "YES", 0.58, 200, 100),
        WalletTrade("w2", "geopolitics", "m1", "YES", 0.59, 220, 120),
        WalletTrade("w3", "geopolitics", "m1", "YES", 0.60, 180, 140),
        WalletTrade("w4", "geopolitics", "m1", "NO", 0.41, 210, 160),
        WalletTrade("w5", "geopolitics", "m1", "NO", 0.42, 100, 180),
    ]

    candidates = engine.evaluate(
        trades,
        {"m1": make_market()},
        make_qualities(),
        MembershipStore(make_memberships(wallets)),
    )

    assert candidates == []
    assert engine.last_diagnostics["below_basket_participation"] == 1


def test_basket_controller_rejects_when_aligned_wallets_do_not_buy_in_tight_price_band() -> (
    None
):
    wallets = ["w1", "w2", "w3", "w4", "w5"]
    engine = CopyEngine(
        make_config(
            wallets=wallets,
            basket_controller=BasketControllerConfig(
                enabled=True,
                tracked_basket_target=5,
                core_slots=5,
                rotating_slots=0,
                backup_slots=0,
                explorer_slots=0,
                min_basket_participation_ratio=0.8,
                min_weighted_participation_ratio=0.8,
                min_active_eligible_wallets=5,
                min_aligned_wallet_count=4,
                max_entry_price_band_abs=0.03,
                max_entry_time_spread_seconds=600,
            ),
        )
    )
    trades = [
        WalletTrade("w1", "geopolitics", "m1", "YES", 0.58, 200, 100),
        WalletTrade("w2", "geopolitics", "m1", "YES", 0.59, 220, 120),
        WalletTrade("w3", "geopolitics", "m1", "YES", 0.60, 180, 140),
        WalletTrade("w4", "geopolitics", "m1", "YES", 0.66, 210, 160),
        WalletTrade("w5", "geopolitics", "m1", "NO", 0.42, 100, 180),
    ]

    candidates = engine.evaluate(
        trades,
        {"m1": make_market()},
        make_qualities(),
        MembershipStore(make_memberships(wallets)),
    )

    assert candidates == []
    assert engine.last_diagnostics["wide_entry_price_band"] == 1


def test_basket_controller_rejects_when_aligned_wallets_are_too_far_apart_in_time() -> (
    None
):
    wallets = ["w1", "w2", "w3", "w4", "w5"]
    engine = CopyEngine(
        make_config(
            wallets=wallets,
            basket_controller=BasketControllerConfig(
                enabled=True,
                tracked_basket_target=5,
                core_slots=5,
                rotating_slots=0,
                backup_slots=0,
                explorer_slots=0,
                min_basket_participation_ratio=0.8,
                min_weighted_participation_ratio=0.8,
                min_active_eligible_wallets=5,
                min_aligned_wallet_count=4,
                max_entry_price_band_abs=0.03,
                max_entry_time_spread_seconds=120,
            ),
        )
    )
    trades = [
        WalletTrade("w1", "geopolitics", "m1", "YES", 0.58, 200, 100),
        WalletTrade("w2", "geopolitics", "m1", "YES", 0.59, 220, 120),
        WalletTrade("w3", "geopolitics", "m1", "YES", 0.60, 180, 140),
        WalletTrade("w4", "geopolitics", "m1", "YES", 0.60, 210, 500),
        WalletTrade("w5", "geopolitics", "m1", "NO", 0.42, 100, 180),
    ]

    candidates = engine.evaluate(
        trades,
        {"m1": make_market()},
        make_qualities(),
        MembershipStore(make_memberships(wallets)),
    )

    assert candidates == []
    assert engine.last_diagnostics["wide_entry_time_spread"] == 1


def test_basket_controller_uses_live_selected_core_and_rotating_wallets() -> None:
    engine = CopyEngine(
        make_config(
            wallets=["w1", "w2", "w3"],
            basket_controller=BasketControllerConfig(
                enabled=True,
                tracked_basket_target=4,
                core_slots=2,
                rotating_slots=1,
                backup_slots=1,
                explorer_slots=0,
                min_basket_participation_ratio=1.0,
                min_weighted_participation_ratio=0.8,
                min_active_eligible_wallets=3,
                min_aligned_wallet_count=3,
                max_entry_price_band_abs=0.03,
                max_entry_time_spread_seconds=600,
            ),
        )
    )
    memberships = [
        BasketMembership(
            topic="geopolitics",
            wallet="w1",
            tier="core",
            rank=1,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w2",
            tier="core",
            rank=2,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w4",
            tier="rotating",
            rank=3,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="promoted",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w5",
            tier="backup",
            rank=4,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="promoted",
            demotion_reason="",
        ),
    ]
    trades = [
        WalletTrade("w2", "geopolitics", "m1", "YES", 0.58, 200, 180),
        WalletTrade("w4", "geopolitics", "m1", "YES", 0.59, 220, 120),
        WalletTrade("w5", "geopolitics", "m1", "YES", 0.60, 240, 60),
    ]

    candidates = engine.evaluate(
        trades,
        {"m1": make_market()},
        make_qualities(),
        MembershipStore(memberships),
    )

    assert len(candidates) == 1
    assert candidates[0].source_wallets == ["w2", "w4", "w5"]
    assert candidates[0].consensus_ratio == 1.0


def test_basket_controller_excludes_stale_seeded_wallets_when_live_roster_has_fresher_replacements() -> (
    None
):
    engine = CopyEngine(
        make_config(
            wallets=["w1", "w2", "w3"],
            basket_controller=BasketControllerConfig(
                enabled=True,
                tracked_basket_target=3,
                core_slots=1,
                rotating_slots=1,
                backup_slots=1,
                explorer_slots=0,
                force_refresh_if_fresh_core_below=1,
                allow_backup_in_live_consensus=True,
                min_basket_participation_ratio=1.0,
                min_weighted_participation_ratio=0.8,
                min_active_eligible_wallets=3,
                min_aligned_wallet_count=3,
                max_entry_price_band_abs=0.03,
                max_entry_time_spread_seconds=600,
            ),
        )
    )
    memberships = [
        BasketMembership(
            topic="geopolitics",
            wallet="w1",
            tier="core",
            rank=1,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w2",
            tier="core",
            rank=2,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w3",
            tier="rotating",
            rank=3,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="active",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w4",
            tier="backup",
            rank=4,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="active",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w5",
            tier="core",
            rank=5,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="active",
            demotion_reason="",
        ),
    ]
    trades = [
        WalletTrade("w3", "geopolitics", "m1", "YES", 0.58, 200, 180),
        WalletTrade("w4", "geopolitics", "m1", "YES", 0.59, 220, 120),
        WalletTrade("w5", "geopolitics", "m1", "YES", 0.60, 240, 60),
    ]

    candidates = engine.evaluate(
        trades,
        {"m1": make_market()},
        make_qualities(),
        MembershipStore(memberships),
    )

    assert len(candidates) == 1
    assert candidates[0].source_wallets == ["w3", "w4", "w5"]
    assert candidates[0].consensus_ratio == 1.0


def test_basket_controller_live_roster_respects_registry_statuses() -> None:
    engine = CopyEngine(
        make_config(
            wallets=["w1", "w2", "w3"],
            basket_controller=BasketControllerConfig(
                enabled=True,
                tracked_basket_target=3,
                core_slots=1,
                rotating_slots=1,
                backup_slots=1,
                explorer_slots=0,
                allow_backup_in_live_consensus=True,
                force_refresh_if_fresh_core_below=1,
                min_basket_participation_ratio=1.0,
                min_weighted_participation_ratio=0.8,
                min_active_eligible_wallets=3,
                min_aligned_wallet_count=3,
                max_entry_price_band_abs=0.03,
                max_entry_time_spread_seconds=600,
            ),
        )
    )
    memberships = [
        BasketMembership(
            topic="geopolitics",
            wallet="w1",
            tier="core",
            rank=1,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w2",
            tier="core",
            rank=2,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w3",
            tier="rotating",
            rank=3,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="active",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w4",
            tier="backup",
            rank=4,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="active",
            demotion_reason="",
        ),
    ]
    registry_entries = [
        WalletRegistryEntry(
            wallet="w1",
            source_type="static_basket",
            source_ref="config.baskets",
            trust_seed=1.0,
            status="suspended",
            first_seen_at=datetime(2026, 1, 1, tzinfo=UTC),
        ),
        WalletRegistryEntry(
            wallet="w2",
            source_type="static_basket",
            source_ref="config.baskets",
            trust_seed=1.0,
            status="active",
            first_seen_at=datetime(2026, 1, 1, tzinfo=UTC),
        ),
        WalletRegistryEntry(
            wallet="w3",
            source_type="static_basket",
            source_ref="config.baskets",
            trust_seed=1.0,
            status="active",
            first_seen_at=datetime(2026, 1, 1, tzinfo=UTC),
        ),
        WalletRegistryEntry(
            wallet="w4",
            source_type="static_basket",
            source_ref="config.baskets",
            trust_seed=1.0,
            status="active",
            first_seen_at=datetime(2026, 1, 1, tzinfo=UTC),
        ),
    ]
    trades = [
        WalletTrade("w1", "geopolitics", "m1", "YES", 0.58, 200, 60),
        WalletTrade("w2", "geopolitics", "m1", "YES", 0.59, 220, 120),
        WalletTrade("w3", "geopolitics", "m1", "YES", 0.60, 240, 180),
        WalletTrade("w4", "geopolitics", "m1", "YES", 0.61, 260, 240),
    ]

    candidates = engine.evaluate(
        trades,
        {"m1": make_market()},
        make_qualities(),
        MembershipStore(memberships, registry_entries=registry_entries),
    )

    assert len(candidates) == 1
    assert candidates[0].source_wallets == ["w2", "w3", "w4"]
    assert "w1" not in candidates[0].source_wallets


def test_basket_controller_live_roster_excludes_probation_wallets_from_consensus() -> (
    None
):
    engine = CopyEngine(
        make_config(
            wallets=["w1", "w2", "w3"],
            basket_controller=BasketControllerConfig(
                enabled=True,
                tracked_basket_target=4,
                core_slots=1,
                rotating_slots=1,
                backup_slots=1,
                explorer_slots=1,
                allow_backup_in_live_consensus=True,
                min_basket_participation_ratio=1.0,
                min_weighted_participation_ratio=0.8,
                min_active_eligible_wallets=3,
                min_aligned_wallet_count=3,
                max_entry_price_band_abs=0.03,
                max_entry_time_spread_seconds=600,
            ),
        )
    )
    memberships = [
        BasketMembership(
            topic="geopolitics",
            wallet="w1",
            tier="core",
            rank=1,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w2",
            tier="rotating",
            rank=2,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w3",
            tier="backup",
            rank=3,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="discovered",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w4",
            tier="explorer",
            rank=4,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
    ]
    registry_entries = [
        WalletRegistryEntry(
            wallet="w1",
            source_type="static_basket",
            source_ref="config.baskets",
            trust_seed=1.0,
            status="active",
            first_seen_at=datetime(2026, 1, 1, tzinfo=UTC),
        ),
        WalletRegistryEntry(
            wallet="w2",
            source_type="static_basket",
            source_ref="config.baskets",
            trust_seed=1.0,
            status="active",
            first_seen_at=datetime(2026, 1, 1, tzinfo=UTC),
        ),
        WalletRegistryEntry(
            wallet="w3",
            source_type="wallet_discovery",
            source_ref="polymarket_data_api",
            trust_seed=0.8,
            status="probation",
            first_seen_at=datetime(2026, 1, 1, tzinfo=UTC),
        ),
        WalletRegistryEntry(
            wallet="w4",
            source_type="static_basket",
            source_ref="config.baskets",
            trust_seed=1.0,
            status="active",
            first_seen_at=datetime(2026, 1, 1, tzinfo=UTC),
        ),
    ]
    trades = [
        WalletTrade("w1", "geopolitics", "m1", "YES", 0.58, 200, 60),
        WalletTrade("w2", "geopolitics", "m1", "YES", 0.59, 220, 120),
        WalletTrade("w3", "geopolitics", "m1", "YES", 0.60, 240, 180),
        WalletTrade("w4", "geopolitics", "m1", "YES", 0.60, 260, 240),
    ]

    candidates = engine.evaluate(
        trades,
        {"m1": make_market()},
        make_qualities(),
        MembershipStore(memberships, registry_entries=registry_entries),
    )

    assert len(candidates) == 1
    assert candidates[0].source_wallets == ["w1", "w2", "w4"]
    assert "w3" not in candidates[0].source_wallets


def test_basket_controller_only_includes_backup_wallets_when_allowed() -> None:
    memberships = [
        BasketMembership(
            topic="geopolitics",
            wallet="w1",
            tier="core",
            rank=1,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w2",
            tier="core",
            rank=2,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="seeded",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w4",
            tier="rotating",
            rank=3,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="promoted",
            demotion_reason="",
        ),
        BasketMembership(
            topic="geopolitics",
            wallet="w5",
            tier="backup",
            rank=4,
            active=True,
            joined_at=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=None,
            promotion_reason="promoted",
            demotion_reason="",
        ),
    ]
    trades = [
        WalletTrade("w2", "geopolitics", "m1", "YES", 0.58, 200, 180),
        WalletTrade("w4", "geopolitics", "m1", "YES", 0.59, 220, 120),
        WalletTrade("w5", "geopolitics", "m1", "YES", 0.60, 240, 60),
    ]

    disallow_backup_engine = CopyEngine(
        make_config(
            wallets=["w1", "w2", "w3"],
            basket_controller=BasketControllerConfig(
                enabled=True,
                tracked_basket_target=3,
                core_slots=1,
                rotating_slots=1,
                backup_slots=1,
                explorer_slots=0,
                min_basket_participation_ratio=1.0,
                min_weighted_participation_ratio=0.8,
                min_active_eligible_wallets=3,
                min_aligned_wallet_count=3,
                max_entry_price_band_abs=0.03,
                max_entry_time_spread_seconds=600,
            ),
        )
    )
    allow_backup_engine = CopyEngine(
        make_config(
            wallets=["w1", "w2", "w3"],
            basket_controller=BasketControllerConfig(
                enabled=True,
                tracked_basket_target=3,
                core_slots=1,
                rotating_slots=1,
                backup_slots=1,
                explorer_slots=0,
                allow_backup_in_live_consensus=True,
                min_basket_participation_ratio=1.0,
                min_weighted_participation_ratio=0.8,
                min_active_eligible_wallets=3,
                min_aligned_wallet_count=3,
                max_entry_price_band_abs=0.03,
                max_entry_time_spread_seconds=600,
            ),
        )
    )

    disallowed = disallow_backup_engine.evaluate(
        trades,
        {"m1": make_market()},
        make_qualities(),
        MembershipStore(memberships),
    )
    allowed = allow_backup_engine.evaluate(
        trades,
        {"m1": make_market()},
        make_qualities(),
        MembershipStore(memberships),
    )

    assert disallowed == []
    assert len(allowed) == 1
    assert allowed[0].source_wallets == ["w2", "w4", "w5"]

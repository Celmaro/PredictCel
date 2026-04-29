from dataclasses import replace

from predictcel.config import FilterConfig
from predictcel.models import MarketSnapshot, WalletTrade
from predictcel.scoring import (
    WalletQualityScorer,
    compute_copyability_score,
    freshness_decay,
)


def make_filters() -> FilterConfig:
    return FilterConfig(
        max_trade_age_seconds=3600,
        max_price_drift=0.05,
        min_liquidity_usd=5000,
        min_minutes_to_resolution=60,
        max_minutes_to_resolution=1440,
        min_position_size_usd=100,
    )


def test_wallet_quality_scores_eligible_wallet() -> None:
    scorer = WalletQualityScorer(make_filters())
    trades = [
        WalletTrade(
            wallet="w1",
            topic="sports",
            market_id="m1",
            side="YES",
            price=0.55,
            size_usd=200,
            age_seconds=300,
        ),
        WalletTrade(
            wallet="w1",
            topic="sports",
            market_id="m1",
            side="YES",
            price=0.56,
            size_usd=180,
            age_seconds=600,
        ),
    ]
    markets = {
        "m1": MarketSnapshot(
            market_id="m1",
            topic="sports",
            title="Example",
            yes_ask=0.57,
            no_ask=0.4,
            best_bid=0.55,
            liquidity_usd=9000,
            minutes_to_resolution=180,
        )
    }

    scores = scorer.score(trades, markets)

    assert "w1" in scores
    assert scores["w1"].score > 0
    assert scores["w1"].eligible_trade_count == 2
    assert "exponential freshness" in scores["w1"].reason
    assert scores["w1"].freshness_score > 0
    assert scores["w1"].drift_score > 0
    assert scores["w1"].sample_score > 0
    assert scorer.last_rejection_counts == {}
    assert scorer.last_missing_market_samples == []


def test_wallet_quality_zero_max_price_drift_keeps_perfect_matches_at_full_score() -> None:
    scorer = WalletQualityScorer(replace(make_filters(), max_price_drift=0.0))
    trades = [
        WalletTrade(
            wallet="w1",
            topic="sports",
            market_id="m1",
            side="YES",
            price=0.55,
            size_usd=200,
            age_seconds=300,
        )
    ]
    markets = {
        "m1": MarketSnapshot(
            market_id="m1",
            topic="sports",
            title="Example",
            yes_ask=0.55,
            no_ask=0.4,
            best_bid=0.55,
            liquidity_usd=9000,
            minutes_to_resolution=180,
        )
    }

    scores = scorer.score(trades, markets)

    assert scores["w1"].drift_score == 1.0


def test_scoring_deduplicates_cross_topic_live_trade_copies() -> None:
    scorer = WalletQualityScorer(make_filters())
    trades = [
        WalletTrade(
            wallet="w1",
            topic="sports",
            market_id="m1",
            side="YES",
            price=0.55,
            size_usd=200,
            age_seconds=300,
        ),
        WalletTrade(
            wallet="w1",
            topic="macro",
            market_id="m1",
            side="YES",
            price=0.55,
            size_usd=200,
            age_seconds=300,
        ),
    ]
    markets = {
        "m1": MarketSnapshot("m1", "sports", "Example", 0.57, 0.4, 0.55, 9000, 180)
    }

    scores = scorer.score(trades, markets)

    assert scores["w1"].eligible_trade_count == 1


def test_scoring_diagnostics_count_rejection_reasons() -> None:
    scorer = WalletQualityScorer(make_filters())
    trades = [
        WalletTrade(
            wallet="w1",
            topic="sports",
            market_id="missing",
            side="YES",
            price=0.55,
            size_usd=200,
            age_seconds=300,
        ),
        WalletTrade(
            wallet="w1",
            topic="sports",
            market_id="m1",
            side="YES",
            price=0.56,
            size_usd=20,
            age_seconds=300,
        ),
        WalletTrade(
            wallet="w2",
            topic="sports",
            market_id="m1",
            side="YES",
            price=0.56,
            size_usd=200,
            age_seconds=7200,
        ),
    ]
    markets = {
        "m1": MarketSnapshot("m1", "sports", "Example", 0.57, 0.4, 0.55, 9000, 180)
    }

    scores = scorer.score(trades, markets)

    assert scores == {}
    assert scorer.last_rejection_counts == {
        "missing_market": 1,
        "too_old": 1,
        "too_small": 1,
    }
    assert scorer.last_wallet_rejection_counts["w1"] == {
        "missing_market": 1,
        "too_small": 1,
    }
    assert scorer.last_wallet_rejection_counts["w2"] == {"too_old": 1}
    assert scorer.last_missing_market_samples == ["missing"]
    assert scorer.last_missing_market_breakdown == {"other": 1}
    assert scorer.last_missing_market_by_wallet == {"w1": 1}
    assert scorer.last_missing_market_samples_by_wallet == {"w1": ["missing"]}
    assert scorer.last_wallet_attrition == {
        "wallets_seen": 2,
        "wallets_scored": 0,
        "wallets_fully_rejected": 2,
    }


def test_wallet_quality_matches_token_ids_case_insensitively() -> None:
    scorer = WalletQualityScorer(make_filters())
    trades = [
        WalletTrade(
            wallet="w1",
            topic="sports",
            market_id="TOKEN_YES",
            side="YES",
            price=0.55,
            size_usd=200,
            age_seconds=300,
        )
    ]
    market = MarketSnapshot(
        market_id="cond_1",
        topic="sports",
        title="Example",
        yes_ask=0.57,
        no_ask=0.4,
        best_bid=0.55,
        liquidity_usd=9000,
        minutes_to_resolution=180,
        yes_token_id="token_yes",
    )
    markets = {
        "cond_1": market,
        "token_yes": market,
    }

    scores = scorer.score(trades, markets)

    assert "w1" in scores
    assert scorer.last_rejection_counts == {}


def test_scoring_diagnostics_classify_token_like_missing_market_ids() -> None:
    scorer = WalletQualityScorer(make_filters())
    trades = [
        WalletTrade(
            wallet="w1",
            topic="sports",
            market_id="0xABC123",
            side="YES",
            price=0.55,
            size_usd=200,
            age_seconds=300,
        )
    ]

    scores = scorer.score(trades, {})

    assert scores == {}
    assert scorer.last_rejection_counts == {"missing_market": 1}
    assert scorer.last_missing_market_breakdown == {"token_id_like": 1}


def test_scoring_tracks_missing_markets_by_wallet_with_deduped_samples() -> None:
    scorer = WalletQualityScorer(make_filters())
    trades = [
        WalletTrade(
            wallet="w_sports",
            topic="sports",
            market_id="missing_a",
            side="YES",
            price=0.55,
            size_usd=200,
            age_seconds=300,
        ),
        WalletTrade(
            wallet="w_sports",
            topic="sports",
            market_id="missing_a",
            side="YES",
            price=0.55,
            size_usd=200,
            age_seconds=310,
        ),
        WalletTrade(
            wallet="w_sports",
            topic="sports",
            market_id="missing_b",
            side="YES",
            price=0.55,
            size_usd=200,
            age_seconds=320,
        ),
        WalletTrade(
            wallet="w_crypto",
            topic="crypto",
            market_id="0xTOKEN",
            side="YES",
            price=0.55,
            size_usd=200,
            age_seconds=300,
        ),
    ]

    scores = scorer.score(trades, {})

    assert scores == {}
    assert scorer.last_missing_market_by_wallet == {
        "w_crypto": 1,
        "w_sports": 3,
    }
    assert scorer.last_missing_market_samples_by_wallet == {
        "w_crypto": ["0xTOKEN"],
        "w_sports": ["missing_a", "missing_b"],
    }


def test_wallet_quality_scoring_keeps_too_close_resolution_trades_for_scoring() -> None:
    scorer = WalletQualityScorer(make_filters())
    trades = [
        WalletTrade(
            wallet="w1",
            topic="sports",
            market_id="m1",
            side="YES",
            price=0.55,
            size_usd=200,
            age_seconds=300,
        )
    ]
    markets = {
        "m1": MarketSnapshot(
            market_id="m1",
            topic="sports",
            title="Example",
            yes_ask=0.57,
            no_ask=0.4,
            best_bid=0.55,
            liquidity_usd=9000,
            minutes_to_resolution=30,
        )
    }

    scores = scorer.score(trades, markets)

    assert "w1" in scores
    assert scorer.last_rejection_counts == {}


def test_wallet_quality_scoring_keeps_too_far_resolution_trades_for_scoring() -> None:
    scorer = WalletQualityScorer(make_filters())
    trades = [
        WalletTrade(
            wallet="w1",
            topic="sports",
            market_id="m1",
            side="YES",
            price=0.55,
            size_usd=200,
            age_seconds=300,
        )
    ]
    markets = {
        "m1": MarketSnapshot(
            market_id="m1",
            topic="sports",
            title="Example",
            yes_ask=0.57,
            no_ask=0.4,
            best_bid=0.55,
            liquidity_usd=9000,
            minutes_to_resolution=2880,
        )
    }

    scores = scorer.score(trades, markets)

    assert "w1" in scores
    assert scorer.last_rejection_counts == {}


def test_wallet_quality_prefers_specialist_human_pace_liquid_copy_safe_wallets() -> (
    None
):
    scorer = WalletQualityScorer(make_filters())
    markets = {
        **{
            f"s{i}": MarketSnapshot(
                f"s{i}", "sports", "Sports", 0.57, 0.4, 0.55, 20000, 180
            )
            for i in range(1, 7)
        },
        **{
            f"g{i}": MarketSnapshot(
                f"g{i}", "mixed", "Mixed", 0.57, 0.4, 0.55, 6000, 180
            )
            for i in range(1, 13)
        },
    }
    specialist_trades = [
        WalletTrade(
            wallet="w_specialist",
            topic="sports",
            market_id=f"s{i}",
            side="YES",
            price=0.55,
            size_usd=220,
            age_seconds=300 + (i * 120),
        )
        for i in range(1, 7)
    ]
    generalist_topics = [
        "sports",
        "crypto",
        "politics",
        "tech",
        "culture",
        "macro-financial",
    ]
    generalist_trades = [
        WalletTrade(
            wallet="w_generalist",
            topic=generalist_topics[(i - 1) % len(generalist_topics)],
            market_id=f"g{i}",
            side="YES",
            price=0.55,
            size_usd=900,
            age_seconds=120 + (i * 10),
        )
        for i in range(1, 13)
    ]

    scores = scorer.score(specialist_trades + generalist_trades, markets)

    assert scores["w_specialist"].score > scores["w_generalist"].score
    assert (
        scores["w_specialist"].specialization_score
        > scores["w_generalist"].specialization_score
    )
    assert (
        scores["w_specialist"].activity_score >= scores["w_generalist"].activity_score
    )
    assert "specialization" in scores["w_specialist"].reason


def test_wallet_quality_penalizes_copy_unsafe_large_size_relative_to_liquidity() -> (
    None
):
    scorer = WalletQualityScorer(make_filters())
    markets = {
        "m_safe_1": MarketSnapshot(
            "m_safe_1", "sports", "Safe 1", 0.57, 0.4, 0.55, 15000, 180
        ),
        "m_safe_2": MarketSnapshot(
            "m_safe_2", "sports", "Safe 2", 0.57, 0.4, 0.55, 15000, 180
        ),
        "m_unsafe_1": MarketSnapshot(
            "m_unsafe_1", "sports", "Unsafe 1", 0.57, 0.4, 0.55, 5500, 180
        ),
        "m_unsafe_2": MarketSnapshot(
            "m_unsafe_2", "sports", "Unsafe 2", 0.57, 0.4, 0.55, 5500, 180
        ),
    }
    trades = [
        WalletTrade(
            wallet="w_safe",
            topic="sports",
            market_id="m_safe_1",
            side="YES",
            price=0.55,
            size_usd=150,
            age_seconds=300,
        ),
        WalletTrade(
            wallet="w_safe",
            topic="sports",
            market_id="m_safe_2",
            side="YES",
            price=0.56,
            size_usd=170,
            age_seconds=450,
        ),
        WalletTrade(
            wallet="w_unsafe",
            topic="sports",
            market_id="m_unsafe_1",
            side="YES",
            price=0.55,
            size_usd=1500,
            age_seconds=300,
        ),
        WalletTrade(
            wallet="w_unsafe",
            topic="sports",
            market_id="m_unsafe_2",
            side="YES",
            price=0.56,
            size_usd=1600,
            age_seconds=450,
        ),
    ]

    scores = scorer.score(trades, markets)

    assert scores["w_safe"].score > scores["w_unsafe"].score
    assert scores["w_safe"].copy_safety_score > scores["w_unsafe"].copy_safety_score
    assert scores["w_safe"].liquidity_score > scores["w_unsafe"].liquidity_score


def test_compute_copyability_score_rewards_better_inputs() -> None:
    filters = make_filters()
    strong = compute_copyability_score(
        consensus_ratio=0.8,
        wallet_quality_score=0.9,
        average_age_seconds=300,
        drift=0.01,
        liquidity_usd=15000,
        side_spread=0.01,
        side_depth_usd=250,
        filters=filters,
    )
    weak = compute_copyability_score(
        consensus_ratio=0.67,
        wallet_quality_score=0.4,
        average_age_seconds=3200,
        drift=0.04,
        liquidity_usd=5000,
        side_spread=0.08,
        side_depth_usd=40,
        filters=filters,
    )

    assert strong > weak


def test_compute_copyability_score_handles_zero_max_price_drift() -> None:
    filters = replace(make_filters(), max_price_drift=0.0)

    zero_drift = compute_copyability_score(
        consensus_ratio=0.8,
        wallet_quality_score=0.9,
        average_age_seconds=300,
        drift=0.0,
        liquidity_usd=15000,
        side_spread=0.01,
        side_depth_usd=250,
        filters=filters,
    )
    non_zero_drift = compute_copyability_score(
        consensus_ratio=0.8,
        wallet_quality_score=0.9,
        average_age_seconds=300,
        drift=0.01,
        liquidity_usd=15000,
        side_spread=0.01,
        side_depth_usd=250,
        filters=filters,
    )

    assert zero_drift > non_zero_drift


def test_freshness_decay_uses_true_half_life() -> None:
    assert freshness_decay(0, 1800) == 1.0
    assert round(freshness_decay(1800, 1800), 4) == 0.5
    assert round(freshness_decay(3600, 1800), 4) == 0.25

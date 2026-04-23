from predictcel.config import FilterConfig
from predictcel.models import MarketSnapshot, WalletTrade
from predictcel.scoring import WalletQualityScorer, compute_copyability_score, freshness_decay


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
        WalletTrade(wallet="w1", topic="sports", market_id="m1", side="YES", price=0.55, size_usd=200, age_seconds=300),
        WalletTrade(wallet="w1", topic="sports", market_id="m1", side="YES", price=0.56, size_usd=180, age_seconds=600),
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
    assert scorer.last_rejection_counts == {}
    assert scorer.last_missing_market_samples == []


def test_scoring_diagnostics_count_rejection_reasons() -> None:
    scorer = WalletQualityScorer(make_filters())
    trades = [
        WalletTrade(wallet="w1", topic="sports", market_id="missing", side="YES", price=0.55, size_usd=200, age_seconds=300),
        WalletTrade(wallet="w1", topic="sports", market_id="m1", side="YES", price=0.56, size_usd=20, age_seconds=300),
        WalletTrade(wallet="w2", topic="sports", market_id="m1", side="YES", price=0.56, size_usd=200, age_seconds=7200),
    ]
    markets = {
        "m1": MarketSnapshot("m1", "sports", "Example", 0.57, 0.4, 0.55, 9000, 180)
    }

    scores = scorer.score(trades, markets)

    assert scores == {}
    assert scorer.last_rejection_counts == {"missing_market": 1, "too_old": 1, "too_small": 1}
    assert scorer.last_wallet_rejection_counts["w1"] == {"missing_market": 1, "too_small": 1}
    assert scorer.last_wallet_rejection_counts["w2"] == {"too_old": 1}
    assert scorer.last_missing_market_samples == ["missing"]


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


def test_freshness_decay_uses_true_half_life() -> None:
    assert freshness_decay(0, 1800) == 1.0
    assert round(freshness_decay(1800, 1800), 4) == 0.5
    assert round(freshness_decay(3600, 1800), 4) == 0.25

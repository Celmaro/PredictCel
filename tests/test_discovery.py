from predictcel.discovery import score_wallet_candidates


def test_scores_focused_recent_wallet_above_unfocused_wallet() -> None:
    leaderboard = [
        {"proxyWallet": "0xfocused", "userName": "sports_one", "pnl": 5000, "vol": 20000, "rank": "1"},
        {"proxyWallet": "0xwide", "userName": "wide_one", "pnl": 8000, "vol": 40000, "rank": "2"},
    ]
    trades_by_wallet = {
        "0xfocused": [
            {"slug": "nba-lal-bos-2026-04-23", "title": "Lakers vs. Celtics", "size": 100, "timestamp": 1777000000},
            {"slug": "nba-nyk-mia-2026-04-23", "title": "Knicks vs. Heat", "size": 120, "timestamp": 1777000100},
            {"slug": "nhl-ana-edm-2026-04-23", "title": "Ducks vs. Oilers", "size": 90, "timestamp": 1777000200},
        ],
        "0xwide": [
            {"slug": "nba-lal-bos-2026-04-23", "title": "Lakers vs. Celtics", "size": 10, "timestamp": 1777000000},
            {"slug": "will-btc-hit-100k-april-23", "title": "Bitcoin $100k?", "size": 10, "timestamp": 1777000100},
            {"slug": "will-it-rain-in-nyc-april-23", "title": "Rain in NYC?", "size": 10, "timestamp": 1777000200},
            {"slug": "fed-rates-may-2026", "title": "Fed rates", "size": 10, "timestamp": 1777000300},
        ],
    }

    candidates = score_wallet_candidates(leaderboard, trades_by_wallet, min_trade_count=3)

    assert [candidate.wallet for candidate in candidates] == ["0xfocused", "0xwide"]
    assert candidates[0].dominant_topic == "sports"
    assert candidates[0].focus_ratio > candidates[1].focus_ratio
    assert candidates[0].score > candidates[1].score


def test_skips_wallets_without_recent_trades() -> None:
    candidates = score_wallet_candidates(
        [{"proxyWallet": "0xempty", "userName": "empty", "pnl": 100, "vol": 1000}],
        {"0xempty": []},
    )

    assert candidates == []


def test_skips_wallets_with_too_few_recent_trades() -> None:
    candidates = score_wallet_candidates(
        [{"proxyWallet": "0xthin", "userName": "thin", "pnl": 100000, "vol": 1000}],
        {"0xthin": [{"slug": "nba-lal-bos", "title": "Lakers vs. Celtics", "size": 500000}]},
    )

    assert candidates == []

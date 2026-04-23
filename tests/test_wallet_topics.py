from predictcel.wallet_topics import classify_wallet_topics, classify_trade_topic


def topic_keywords():
    return {
        "sports": ["nba", "nfl", "soccer"],
        "crypto": ["btc", "bitcoin", "eth"],
        "geopolitics": ["election", "senate"],
    }


def test_classifies_trade_topic_from_keywords() -> None:
    trade = {"question": "Will BTC close above 100k?"}

    assert classify_trade_topic(trade, topic_keywords()) == "crypto"


def test_builds_affinities_and_specialization() -> None:
    trades = [
        {"question": "NBA finals winner"},
        {"question": "NFL playoff game"},
        {"question": "Bitcoin above 100k"},
    ]

    profile = classify_wallet_topics(trades, topic_keywords())

    assert profile.primary_topic == "sports"
    assert profile.topic_affinities["sports"] == 0.6667
    assert profile.specialization_score > 0

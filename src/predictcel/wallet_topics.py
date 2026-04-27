"""Wallet topic analysis.

Analyzes wallet trading patterns by topic/category to enable
topic-based basket assignment.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

from .models import WalletTopicProfile

__all__ = ["analyze_wallet_topics", "get_topic_preferences"]


UNKNOWN_TOPIC = "other"


def classify_wallet_topics(trades: list[dict[str, Any]], topic_keywords: dict[str, list[str]]) -> WalletTopicProfile:
    counts: Counter[str] = Counter()
    for trade in trades:
        topic = _explicit_topic(trade)
        if not topic or topic == "unknown":
            topic = classify_trade_topic(trade, topic_keywords)
        counts[topic] += 1

    total = sum(counts.values())
    if total <= 0:
        return WalletTopicProfile(topic_affinities={UNKNOWN_TOPIC: 1.0}, primary_topic=UNKNOWN_TOPIC, specialization_score=1.0)

    affinities = {topic: round(count / total, 4) for topic, count in counts.items()}
    primary_topic = max(affinities, key=affinities.get)
    specialization = round(sum(value * value for value in affinities.values()), 4)
    return WalletTopicProfile(
        topic_affinities=dict(sorted(affinities.items(), key=lambda item: item[1], reverse=True)),
        primary_topic=primary_topic,
        specialization_score=specialization,
    )


def classify_trade_topic(trade: dict[str, Any], topic_keywords: dict[str, list[str]]) -> str:
    text = _trade_text(trade)
    for topic, keywords in topic_keywords.items():
        if any(keyword.lower() in text for keyword in keywords):
            return topic
    return UNKNOWN_TOPIC


def _explicit_topic(trade: dict[str, Any]) -> str:
    for key in ("topic", "category", "tag", "seriesSlug", "eventSlug"):
        value = trade.get(key)
        if value:
            return str(value).strip().lower()
    return ""


def _trade_text(trade: dict[str, Any]) -> str:
    parts = []
    for key in ("slug", "marketSlug", "title", "question", "market", "eventTitle", "description"):
        value = trade.get(key)
        if value:
            parts.append(str(value))
    return " ".join(parts).lower()

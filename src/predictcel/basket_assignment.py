"""Basket assignment logic for wallet categorization.

Assigns wallets to baskets based on their trading characteristics,
performance metrics, and topic preferences.
"""

from __future__ import annotations

from .config import WalletDiscoveryConfig
from .models import BasketAssignment, WalletDiscoveryCandidate

__all__ = ["assign_baskets", "BasketAssigner"]



class BasketAssignmentEngine:
    def __init__(self, config: WalletDiscoveryConfig) -> None:
        self.config = config

    def assign(self, candidate: WalletDiscoveryCandidate) -> BasketAssignment:
        recommended = [
            topic
            for topic, affinity in candidate.topic_profile.topic_affinities.items()
            if topic in self.config.topics and affinity >= 0.15
        ]
        if not recommended and candidate.topic_profile.primary_topic in self.config.topics:
            recommended = [candidate.topic_profile.primary_topic]

        confidence = self._confidence(candidate.score)
        reasons = [
            f"primary_topic={candidate.topic_profile.primary_topic}",
            f"specialization={candidate.topic_profile.specialization_score:.4f}",
            f"recent_trades={candidate.recent_trades}",
            f"avg_trade_size_usd={candidate.avg_trade_size_usd:.2f}",
        ]
        if candidate.rejected_reasons:
            reasons.extend(f"warning={reason}" for reason in candidate.rejected_reasons)

        return BasketAssignment(
            wallet_address=candidate.wallet_address,
            primary_topic=candidate.topic_profile.primary_topic,
            recommended_baskets=recommended[:3],
            topic_affinities=candidate.topic_profile.topic_affinities,
            overall_score=round(candidate.score, 4),
            confidence=confidence,
            reasons=reasons,
        )

    def _confidence(self, score: float) -> str:
        if score >= 0.70:
            return "HIGH"
        if score >= 0.50:
            return "MEDIUM"
        return "LOW"

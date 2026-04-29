"""Basket management and persistence.

Manages basket definitions, wallet assignments, and basket-level
configuration.
"""

from __future__ import annotations

import logging

from .config import AppConfig
from .models import BasketAssignment, BasketManagerAction

__all__ = ["BasketManagerPlanner"]


logger = logging.getLogger(__name__)


class BasketManagerPlanner:
    def __init__(
        self,
        config: AppConfig,
        current_wallets_by_topic: dict[str, set[str]] | None = None,
    ) -> None:
        self.config = config
        if current_wallets_by_topic is None:
            current_wallets_by_topic = {
                basket.topic: {wallet.lower() for wallet in basket.wallets}
                for basket in config.baskets
            }
        self.current_by_topic = {
            str(topic): {str(wallet).lower() for wallet in wallets}
            for topic, wallets in current_wallets_by_topic.items()
        }

    def plan(self, assignments: list[BasketAssignment]) -> list[BasketManagerAction]:
        actions: list[BasketManagerAction] = []
        added_by_basket: dict[str, int] = {topic: 0 for topic in self.current_by_topic}
        planned_keys: set[tuple[str, str, str]] = set()

        for assignment in sorted(assignments, key=lambda item: item.overall_score, reverse=True):
            wallet_key = assignment.wallet_address.lower()
            current_baskets = self._current_baskets(wallet_key)

            for basket in current_baskets:
                lifecycle_action = self._existing_wallet_action(basket, assignment)
                if lifecycle_action is None:
                    continue
                action_key = (lifecycle_action.action, basket, wallet_key)
                if action_key in planned_keys:
                    continue
                actions.append(lifecycle_action)
                planned_keys.add(action_key)

            if assignment.overall_score < self.config.wallet_discovery.min_assignment_score:
                if not current_baskets:
                    actions.append(self._action("observe", assignment.primary_topic, assignment, "below assignment score threshold"))
                continue
            if assignment.confidence == "LOW":
                if not current_baskets:
                    actions.append(self._action("observe", assignment.primary_topic, assignment, "low confidence assignment"))
                continue
            if not current_baskets and not self._clears_promotion_buffer(assignment):
                actions.append(self._action("observe", assignment.primary_topic, assignment, "below promotion quality buffer"))
                continue

            added = False
            for basket in assignment.recommended_baskets:
                current = self.current_by_topic.get(basket, set())
                action_key = ("add", basket, wallet_key)
                if wallet_key in current or action_key in planned_keys:
                    continue
                if len(current) + added_by_basket.get(basket, 0) >= self.config.wallet_discovery.max_wallets_per_basket:
                    actions.append(self._action("observe", basket, assignment, "basket is at max wallet capacity"))
                    continue
                if added_by_basket.get(basket, 0) >= self.config.wallet_discovery.max_new_wallets_per_run:
                    actions.append(self._action("observe", basket, assignment, "max new wallets per run reached"))
                    continue
                actions.append(self._action("add", basket, assignment, self._add_reason()))
                planned_keys.add(action_key)
                added_by_basket[basket] = added_by_basket.get(basket, 0) + 1
                added = True
            if not added and not assignment.recommended_baskets and not current_baskets:
                actions.append(self._action("observe", assignment.primary_topic, assignment, "no configured basket matched topic profile"))

        return actions

    def rebalance(self, current_positions: list[dict]) -> list[BasketManagerAction]:
        """Rebalance basket allocations to maintain target allocations, reducing concentration risk."""
        actions: list[BasketManagerAction] = []
        total_allocation = sum(basket.target_allocation for basket in self.config.baskets if basket.target_allocation > 0)
        if total_allocation == 0:
            logger.info("No target allocations set for baskets, skipping rebalancing")
            return actions

        current_by_topic = {}
        for pos in current_positions:
            topic = pos.get('topic', 'unknown')
            exposure = pos.get('exposure_usd', 0.0)
            current_by_topic[topic] = current_by_topic.get(topic, 0.0) + exposure

        total_exposure = sum(current_by_topic.values())
        if total_exposure == 0:
            logger.info("No current exposure, skipping rebalancing")
            return actions

        drift_threshold = 0.05
        rebalance_needed = False
        for basket in self.config.baskets:
            if basket.target_allocation == 0:
                continue
            target_exposure = total_exposure * (basket.target_allocation / total_allocation)
            current_exposure = current_by_topic.get(basket.topic, 0.0)
            drift = abs(current_exposure - target_exposure) / target_exposure if target_exposure > 0 else 0
            if drift > drift_threshold:
                rebalance_needed = True
                reason = f"Allocation drift {drift:.2%} exceeds threshold {drift_threshold:.2%}"
                logger.info(f"Rebalancing advisory for basket {basket.topic}: {reason}")
                actions.append(BasketManagerAction(
                    action="rebalance",
                    basket=basket.topic,
                    wallet_address="",
                    score=0.0,
                    confidence="HIGH",
                    reason=reason
                ))
        if not rebalance_needed:
            logger.info("Portfolio allocations within acceptable drift thresholds")
        return actions

    def _current_baskets(self, wallet_address: str) -> list[str]:
        return [topic for topic, wallets in self.current_by_topic.items() if wallet_address in wallets]

    def _existing_wallet_action(self, basket: str, assignment: BasketAssignment) -> BasketManagerAction | None:
        threshold = self.config.wallet_discovery.min_assignment_score
        if basket not in assignment.recommended_baskets:
            if assignment.overall_score < threshold and assignment.confidence == "LOW":
                return self._action("remove", basket, assignment, "wallet no longer matches basket topic and score fell below lifecycle threshold")
            return self._action("suspend", basket, assignment, "wallet topic profile drifted away from basket")
        if assignment.overall_score < threshold:
            return self._action("suspend", basket, assignment, "existing wallet score fell below assignment threshold")
        if assignment.confidence == "LOW":
            return self._action("suspend", basket, assignment, "existing wallet confidence fell to LOW")
        return None

    def _add_reason(self) -> str:
        mode = self.config.wallet_discovery.mode
        if mode == "auto_update":
            return "auto-update eligible recommendation"
        if mode == "propose_config":
            return "config proposal recommendation"
        return "report-only recommendation; manual approval required"

    def _clears_promotion_buffer(self, assignment: BasketAssignment) -> bool:
        threshold = self.config.wallet_discovery.min_assignment_score
        if assignment.confidence == "HIGH":
            return True
        return assignment.overall_score >= min(threshold + 0.05, 1.0)

    def _action(self, action: str, basket: str, assignment: BasketAssignment, reason: str) -> BasketManagerAction:
        return BasketManagerAction(
            action=action,
            basket=basket,
            wallet_address=assignment.wallet_address,
            score=assignment.overall_score,
            confidence=assignment.confidence,
            reason=reason,
        )

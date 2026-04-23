from __future__ import annotations

from .config import AppConfig
from .models import BasketAssignment, BasketManagerAction


class BasketManagerPlanner:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.current_by_topic = {basket.topic: {wallet.lower() for wallet in basket.wallets} for basket in config.baskets}

    def plan(self, assignments: list[BasketAssignment]) -> list[BasketManagerAction]:
        actions: list[BasketManagerAction] = []
        added_by_basket: dict[str, int] = {topic: 0 for topic in self.current_by_topic}

        for assignment in sorted(assignments, key=lambda item: item.overall_score, reverse=True):
            if assignment.overall_score < self.config.wallet_discovery.min_assignment_score:
                actions.append(self._action("observe", assignment.primary_topic, assignment, "below assignment score threshold"))
                continue
            if assignment.confidence == "LOW":
                actions.append(self._action("observe", assignment.primary_topic, assignment, "low confidence assignment"))
                continue

            added = False
            for basket in assignment.recommended_baskets:
                current = self.current_by_topic.get(basket, set())
                if assignment.wallet_address.lower() in current:
                    continue
                if len(current) + added_by_basket.get(basket, 0) >= self.config.wallet_discovery.max_wallets_per_basket:
                    actions.append(self._action("observe", basket, assignment, "basket is at max wallet capacity"))
                    continue
                if added_by_basket.get(basket, 0) >= self.config.wallet_discovery.max_new_wallets_per_run:
                    actions.append(self._action("observe", basket, assignment, "max new wallets per run reached"))
                    continue
                actions.append(self._action("add", basket, assignment, "report-only recommendation; manual approval required"))
                added_by_basket[basket] = added_by_basket.get(basket, 0) + 1
                added = True
            if not added and not assignment.recommended_baskets:
                actions.append(self._action("observe", assignment.primary_topic, assignment, "no configured basket matched topic profile"))

        return actions

    def _action(self, action: str, basket: str, assignment: BasketAssignment, reason: str) -> BasketManagerAction:
        return BasketManagerAction(
            action=action,
            basket=basket,
            wallet_address=assignment.wallet_address,
            score=assignment.overall_score,
            confidence=assignment.confidence,
            reason=reason,
        )

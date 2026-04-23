from __future__ import annotations

import os
import random
import time
from dataclasses import asdict, replace
from datetime import UTC, datetime
from typing import Any

from .config import ExecutionConfig, LiveDataConfig, PositionConfig
from .models import (
    CopyCandidate,
    ExecutionIntent,
    ExecutionResult,
    MarketSnapshot,
    Position,
)


class ExecutionPlanner:
    def __init__(self, config: ExecutionConfig, position_config: PositionConfig) -> None:
        self.config = config
        self.position_config = position_config

    def plan(
        self,
        candidates: list[CopyCandidate],
        markets: dict[str, MarketSnapshot],
        held_market_ids: set[str],
        current_exposure_usd: float,
    ) -> list[ExecutionIntent]:
        ranked = sorted(candidates, key=lambda item: item.copyability_score, reverse=True)
        intents: list[ExecutionIntent] = []
        planned_exposure_usd = current_exposure_usd
        for candidate in ranked:
            if candidate.copyability_score < self.config.min_copyability_score:
                continue
            if candidate.market_id in held_market_ids:
                continue

            if self.config.exposure is not None:
                new_exposure = planned_exposure_usd + self.config.buy_amount_usd
                if self.config.exposure.max_total_exposure_usd > 0:
                    if new_exposure > self.config.exposure.max_total_exposure_usd:
                        continue
                if self.config.exposure.max_single_position_usd > 0:
                    if self.config.buy_amount_usd > self.config.exposure.max_single_position_usd:
                        continue

            market = markets.get(candidate.market_id)
            if market is None or not market.orderbook_ready:
                continue

            token_id = market.yes_token_id if candidate.side == "YES" else market.no_token_id
            current_price = market.yes_ask if candidate.side == "YES" else market.no_ask
            side_depth_shares = market.yes_ask_size if candidate.side == "YES" else market.no_ask_size
            side_depth_usd = side_depth_shares * current_price
            if not token_id or side_depth_usd < self.config.buy_amount_usd:
                continue

            worst_price = min(round(current_price + self.config.worst_price_buffer, 4), 0.99)
            intents.append(
                ExecutionIntent(
                    market_id=candidate.market_id,
                    topic=candidate.topic,
                    side=candidate.side,
                    token_id=token_id,
                    amount_usd=self.config.buy_amount_usd,
                    worst_price=worst_price,
                    copyability_score=candidate.copyability_score,
                    order_type=self.config.order_type.upper(),
                    reason="copyability threshold, no open position, exposure within limits, token id, and top-of-book depth checks passed",
                )
            )
            planned_exposure_usd += self.config.buy_amount_usd
            if len(intents) >= self.config.max_orders_per_run:
                break
        return intents


class LiveOrderExecutor:
    def __init__(self, config: ExecutionConfig, live_data: LiveDataConfig | None) -> None:
        self.config = config
        self.live_data = live_data

    def execute(self, intents: list[ExecutionIntent]) -> list[ExecutionResult]:
        if not intents:
            return []
        if self.config.dry_run:
            return [self._dry_run_result(intent) for intent in intents]

        client = self._build_client()
        return [self._submit_intent(client, intent) for intent in intents]

    def _dry_run_result(self, intent: ExecutionIntent) -> ExecutionResult:
        return ExecutionResult(
            market_id=intent.market_id,
            topic=intent.topic,
            side=intent.side,
            token_id=intent.token_id,
            amount_usd=intent.amount_usd,
            worst_price=intent.worst_price,
            status="dry_run",
            order_id="",
            error="",
            copyability_score=intent.copyability_score,
            reason=intent.reason,
        )

    def _build_client(self) -> Any:
        try:
            from py_clob_client.client import ClobClient
        except ImportError as exc:
            raise RuntimeError("py-clob-client is required for live trading. Install with `pip install -e .[trade]`.") from exc

        private_key = os.getenv("PREDICTCEL_POLY_PRIVATE_KEY", "").strip()
        funder = os.getenv("PREDICTCEL_POLY_FUNDER", "").strip()
        if not private_key or not funder:
            raise RuntimeError("Live trading requires PREDICTCEL_POLY_PRIVATE_KEY and PREDICTCEL_POLY_FUNDER.")

        host = os.getenv("PREDICTCEL_POLY_HOST", (self.live_data.clob_base_url if self.live_data else "https://clob.polymarket.com"))
        client = ClobClient(
            host,
            key=private_key,
            chain_id=self.config.chain_id,
            signature_type=self.config.signature_type,
            funder=funder,
        )
        client.set_api_creds(client.create_or_derive_api_creds())
        return client

    def _submit_intent(self, client: Any, intent: ExecutionIntent) -> ExecutionResult:
        max_retries = max(getattr(self.config, "max_retries", 3), 1)
        base_delay = getattr(self.config, "retry_base_delay_seconds", 1.0)

        for attempt in range(max_retries):
            try:
                from py_clob_client.clob_types import MarketOrderArgs, OrderType
                from py_clob_client.order_builder.constants import BUY, SELL

                order_args = MarketOrderArgs(
                    token_id=intent.token_id,
                    amount=float(intent.amount_usd),
                    price=float(intent.worst_price),
                    side=SELL if intent.side == "CLOSE" else BUY,
                )
                signed = client.create_market_order(order_args)
                order_type = getattr(OrderType, intent.order_type)
                response = client.post_order(signed, order_type)
                order_id = str(response.get("orderID") or response.get("orderId") or "") if isinstance(response, dict) else ""
                status = str(response.get("status") or "submitted") if isinstance(response, dict) else "submitted"
                error = str(response.get("errorMsg") or "") if isinstance(response, dict) else ""

                if isinstance(response, dict) and response.get("status") == 429:
                    raise RuntimeError("Rate limited (429)")

                return ExecutionResult(
                    market_id=intent.market_id,
                    topic=intent.topic,
                    side=intent.side,
                    token_id=intent.token_id,
                    amount_usd=intent.amount_usd,
                    worst_price=intent.worst_price,
                    status=status,
                    order_id=order_id,
                    error=error,
                    copyability_score=intent.copyability_score,
                    reason=intent.reason,
                )
            except Exception as exc:
                error_msg = str(exc)
                is_retryable = (
                    "429" in error_msg
                    or "timeout" in error_msg.lower()
                    or "timed out" in error_msg.lower()
                    or "connection" in error_msg.lower()
                    or "reset" in error_msg.lower()
                    or "503" in error_msg
                    or "502" in error_msg
                    or "504" in error_msg
                )
                if is_retryable and attempt < max_retries - 1:
                    time.sleep(retry_delay(base_delay, attempt))
                    continue
                return ExecutionResult(
                    market_id=intent.market_id,
                    topic=intent.topic,
                    side=intent.side,
                    token_id=intent.token_id,
                    amount_usd=intent.amount_usd,
                    worst_price=intent.worst_price,
                    status="error",
                    order_id="",
                    error=f"attempt {attempt + 1}/{max_retries}: {error_msg}",
                    copyability_score=intent.copyability_score,
                    reason=intent.reason,
                )


class ExitRunner:
    def __init__(self, config: ExecutionConfig, live_data: LiveDataConfig | None) -> None:
        self.config = config
        self.live_data = live_data

    def evaluate_and_close(
        self,
        positions: list[Position],
        markets: dict[str, MarketSnapshot],
    ) -> tuple[list[ExecutionIntent], list[Position]]:
        close_intents: list[ExecutionIntent] = []
        updated_positions: list[Position] = []
        now = datetime.now(UTC)

        for pos in positions:
            market = markets.get(pos.market_id)
            if market is None:
                updated_positions.append(pos)
                continue

            if pos.side == "YES":
                current_price = market.yes_ask
                close_price = market.yes_bid
                close_token = market.yes_token_id
            else:
                current_price = market.no_ask
                close_price = market.no_bid
                close_token = market.no_token_id

            if current_price == 0.0:
                current_price = pos.current_price

            pnl_pct = (current_price - pos.entry_price) / pos.entry_price
            unrealized_pnl = round(pos.entry_amount_usd * pnl_pct, 4)

            updated_pos = replace(
                pos,
                current_price=current_price,
                unrealized_pnl=unrealized_pnl,
                last_updated=now,
            )

            should_close = False
            reason = ""

            if pos.take_profit_pct > 0 and pnl_pct >= pos.take_profit_pct:
                should_close = True
                reason = f"take_profit triggered: pnl={pnl_pct:.4f} >= tp={pos.take_profit_pct}"
            elif pos.stop_loss_pct > 0 and pnl_pct <= -pos.stop_loss_pct:
                should_close = True
                reason = f"stop_loss triggered: pnl={pnl_pct:.4f} <= sl={pos.stop_loss_pct}"
            elif pos.max_hold_minutes > 0:
                elapsed_minutes = (now - pos.opened_at).total_seconds() / 60.0
                if elapsed_minutes >= pos.max_hold_minutes:
                    should_close = True
                    reason = f"max_hold exceeded: {elapsed_minutes:.1f} min >= {pos.max_hold_minutes} min"

            if not should_close and market.minutes_to_resolution > 0 and market.minutes_to_resolution <= 10:
                should_close = True
                reason = f"market resolving soon: {market.minutes_to_resolution} min to resolution"

            if should_close and close_price > 0 and close_token:
                close_intents.append(
                    ExecutionIntent(
                        market_id=pos.market_id,
                        topic=pos.topic,
                        side="CLOSE",
                        token_id=close_token,
                        amount_usd=pos.entry_amount_usd,
                        worst_price=close_price,
                        copyability_score=0.0,
                        order_type=self.config.order_type.upper(),
                        reason=reason,
                    )
                )
            updated_positions.append(updated_pos)

        return close_intents, updated_positions


def retry_delay(base_delay: float, attempt: int) -> float:
    return base_delay * (2 ** attempt) * random.uniform(0.5, 1.5)


def intents_as_dicts(intents: list[ExecutionIntent]) -> list[dict[str, Any]]:
    return [asdict(intent) for intent in intents]

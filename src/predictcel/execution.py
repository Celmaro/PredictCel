from __future__ import annotations

import hashlib
import inspect
import json
import os
import random
import time
from concurrent.futures import as_completed
from dataclasses import asdict, replace
from datetime import UTC, datetime
from typing import Any

from .config import ExecutionConfig, LiveDataConfig, PositionConfig
from .constants import max_entry_price, min_entry_minutes_to_resolution
from .models import (
    CopyCandidate,
    ExecutionIntent,
    ExecutionResult,
    MarketSnapshot,
    Position,
)
from .runtime import shared_io_executor

POLY_API_KEY_ENV_VAR = "POLY_API_KEY"
POLY_API_SECRET_ENV_VAR = "POLY_API_SECRET"
POLY_API_PASSPHRASE_ENV_VAR = "POLY_API_PASSPHRASE"


class ExecutionPlanner:
    def __init__(
        self, config: ExecutionConfig, position_config: PositionConfig
    ) -> None:
        self.config = config
        self.position_config = position_config
        self.last_diagnostics: dict[str, int] = {}

    def plan(
        self,
        candidates: list[CopyCandidate],
        markets: dict[str, MarketSnapshot],
        held_market_ids: set[str],
        current_exposure_usd: float,
    ) -> list[ExecutionIntent]:
        ranked = sorted(
            candidates, key=lambda item: item.copyability_score, reverse=True
        )
        intents: list[ExecutionIntent] = []
        planned_exposure_usd = current_exposure_usd
        diagnostics = {
            "candidates_seen": len(ranked),
            "below_copyability_threshold": 0,
            "already_held": 0,
            "orderbook_not_ready": 0,
            "too_close_to_resolution": 0,
            "zero_amount": 0,
            "price_too_high": 0,
            "missing_token_id": 0,
            "insufficient_side_depth": 0,
            "candidates_planned": 0,
        }
        for candidate in ranked:
            if candidate.copyability_score < self.config.min_copyability_score:
                diagnostics["below_copyability_threshold"] += 1
                continue
            if candidate.market_id in held_market_ids:
                diagnostics["already_held"] += 1
                continue

            market = markets.get(candidate.market_id)
            if market is None or not market.orderbook_ready:
                diagnostics["orderbook_not_ready"] += 1
                continue
            if market.minutes_to_resolution < min_entry_minutes_to_resolution():
                diagnostics["too_close_to_resolution"] += 1
                continue

            amount_usd = self._planned_amount_usd(candidate, planned_exposure_usd)
            if amount_usd <= 0:
                diagnostics["zero_amount"] += 1
                continue

            token_id = (
                market.yes_token_id if candidate.side == "YES" else market.no_token_id
            )
            current_price = market.yes_ask if candidate.side == "YES" else market.no_ask
            if current_price >= max_entry_price():
                diagnostics["price_too_high"] += 1
                continue
            side_depth_shares = (
                market.yes_ask_size if candidate.side == "YES" else market.no_ask_size
            )
            side_depth_usd = side_depth_shares * current_price
            if not token_id:
                diagnostics["missing_token_id"] += 1
                continue
            if side_depth_usd < amount_usd:
                diagnostics["insufficient_side_depth"] += 1
                continue

            worst_price = min(
                round(current_price + self.config.worst_price_buffer, 4), 0.99
            )
            intents.append(
                ExecutionIntent(
                    market_id=candidate.market_id,
                    topic=candidate.topic,
                    side=candidate.side,
                    token_id=token_id,
                    amount_usd=round(amount_usd, 4),
                    worst_price=worst_price,
                    copyability_score=candidate.copyability_score,
                    order_type=self.config.order_type.upper(),
                    reason="copyability threshold, safety gates, 5-10 USD sizing policy, no open position, exposure within limits, token id, and top-of-book depth checks passed",
                    market_title=market.title,
                )
            )
            diagnostics["candidates_planned"] += 1
            planned_exposure_usd += amount_usd
            if len(intents) >= self.config.max_orders_per_run:
                break
        self.last_diagnostics = diagnostics
        return intents

    def _planned_amount_usd(
        self, candidate: CopyCandidate, planned_exposure_usd: float
    ) -> float:
        suggested_amount = (
            candidate.suggested_position_usd
            if candidate.suggested_position_usd > 0
            else self.config.buy_amount_usd
        )
        suggested_amount = max(suggested_amount, 0.0)
        amount_usd = max(suggested_amount, self.config.min_signal_allocation_usd)
        amount_usd = min(amount_usd, self.config.buy_amount_usd)
        if self.config.exposure is not None:
            if self.config.exposure.max_single_position_usd > 0:
                amount_usd = min(
                    amount_usd, self.config.exposure.max_single_position_usd
                )
            if self.config.exposure.max_total_exposure_usd > 0:
                remaining_capacity = (
                    self.config.exposure.max_total_exposure_usd - planned_exposure_usd
                )
                if remaining_capacity < self.config.min_signal_allocation_usd:
                    return 0.0
                amount_usd = min(amount_usd, remaining_capacity)
        return max(amount_usd, 0.0)


class LiveOrderExecutor:
    def __init__(
        self,
        config: ExecutionConfig,
        live_data: LiveDataConfig | None,
        store: Any | None = None,
    ) -> None:
        self.config = config
        self.live_data = live_data
        self.store = store

    def execute(self, intents: list[ExecutionIntent]) -> list[ExecutionResult]:
        if not intents:
            return []
        if self.config.dry_run:
            return [self._dry_run_result(intent) for intent in intents]
        if len(intents) == 1:
            return [self._execute_live_intent(intents[0])]

        executor = shared_io_executor()
        clients = [self._build_client() for _ in intents]
        futures = {
            executor.submit(self._submit_with_client, client, intent): index
            for index, (client, intent) in enumerate(zip(clients, intents))
        }
        results: list[ExecutionResult | None] = [None] * len(intents)
        for future in as_completed(tuple(futures)):
            results[futures[future]] = future.result()
        return [result for result in results if result is not None]

    def _execute_live_intent(self, intent: ExecutionIntent) -> ExecutionResult:
        client = self._build_client()
        return self._submit_with_client(client, intent)

    def _submit_with_client(self, client: Any, intent: ExecutionIntent) -> ExecutionResult:
        try:
            return self._submit_intent(client, intent)
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                close()

    def _dry_run_result(self, intent: ExecutionIntent) -> ExecutionResult:
        client_order_id = self._client_order_id_for_intent(intent)
        filled_shares = (
            round(intent.amount_usd / intent.worst_price, 8)
            if intent.worst_price > 0
            else 0.0
        )
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
            market_title=intent.market_title,
            client_order_id=client_order_id,
            filled_shares=filled_shares,
            avg_fill_price=intent.worst_price,
        )

    def _build_client(self) -> Any:
        try:
            from py_clob_client.client import ClobClient
        except ImportError as exc:
            raise RuntimeError(
                "py-clob-client is required for live trading. Install with `pip install -e .[trade]`."
            ) from exc

        private_key = os.getenv("PREDICTCEL_POLY_PRIVATE_KEY", "").strip()
        funder = os.getenv("PREDICTCEL_POLY_FUNDER", "").strip()
        if not private_key or not funder:
            raise RuntimeError(
                "Live trading requires PREDICTCEL_POLY_PRIVATE_KEY and PREDICTCEL_POLY_FUNDER."
            )
        api_key = os.getenv(POLY_API_KEY_ENV_VAR, "").strip()
        api_secret = os.getenv(POLY_API_SECRET_ENV_VAR, "").strip()
        api_passphrase = os.getenv(POLY_API_PASSPHRASE_ENV_VAR, "").strip()
        provided_api_fields = [api_key, api_secret, api_passphrase]
        if any(provided_api_fields) and not all(provided_api_fields):
            raise RuntimeError(
                "Pre-generated Polymarket API credentials require all of "
                "POLY_API_KEY, POLY_API_SECRET, and POLY_API_PASSPHRASE."
            )

        host = os.getenv(
            "PREDICTCEL_POLY_HOST",
            (
                self.live_data.clob_base_url
                if self.live_data
                else "https://clob.polymarket.com"
            ),
        )
        client = ClobClient(
            host,
            key=private_key,
            chain_id=self.config.chain_id,
            signature_type=self.config.signature_type,
            funder=funder,
        )
        client.set_api_creds(
            self._provided_api_creds(api_key, api_secret, api_passphrase)
            if all(provided_api_fields)
            else client.create_or_derive_api_creds()
        )
        return client

    def _provided_api_creds(
        self,
        api_key: str,
        api_secret: str,
        api_passphrase: str,
    ) -> Any:
        try:
            from py_clob_client.clob_types import ApiCreds
        except ImportError:
            ApiCreds = None

        if ApiCreds is not None:
            for args, kwargs in (
                ((api_key, api_secret, api_passphrase), {}),
                (
                    (),
                    {
                        "key": api_key,
                        "secret": api_secret,
                        "passphrase": api_passphrase,
                    },
                ),
                (
                    (),
                    {
                        "api_key": api_key,
                        "api_secret": api_secret,
                        "api_passphrase": api_passphrase,
                    },
                ),
            ):
                try:
                    return ApiCreds(*args, **kwargs)
                except TypeError:
                    continue
        return {
            "key": api_key,
            "secret": api_secret,
            "passphrase": api_passphrase,
        }

    def _submit_intent(self, client: Any, intent: ExecutionIntent) -> ExecutionResult:
        max_retries = max(getattr(self.config, "max_retries", 3), 1)
        base_delay = getattr(self.config, "retry_base_delay_seconds", 1.0)
        client_order_id = self._client_order_id_for_intent(intent)

        for attempt in range(max_retries):
            submission_started = False
            try:
                from py_clob_client.clob_types import MarketOrderArgs, OrderType
                from py_clob_client.order_builder.constants import BUY, SELL

                order_args = MarketOrderArgs(
                    token_id=intent.token_id,
                    amount=float(intent.amount_usd),
                    price=float(intent.worst_price),
                    side=SELL if intent.side == "CLOSE" else BUY,
                )
                signed = self._create_signed_order(
                    client,
                    order_args,
                    client_order_id,
                )
                order_type = getattr(OrderType, intent.order_type)
                submission_started = True
                response = self._post_signed_order(
                    client,
                    signed,
                    order_type,
                    client_order_id,
                )

                if isinstance(response, dict) and str(response.get("status")) == "429":
                    raise RuntimeError("Rate limited (429)")

                result = self._result_from_response(intent, response, client_order_id)
                self._record_open_order(intent, result, response)
                return result
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
                    if submission_started:
                        reconciled = self._reconcile_order_by_client_id(
                            client,
                            intent,
                            client_order_id,
                        )
                        if reconciled is not None:
                            self._record_open_order(intent, reconciled, None)
                            return reconciled
                        if not self._supports_order_lookup(client):
                            pending_result = self._pending_result(
                                intent,
                                client_order_id,
                                error_msg,
                                attempt + 1,
                                max_retries,
                            )
                            self._record_open_order(intent, pending_result, None)
                            return pending_result
                    time.sleep(retry_delay(base_delay, attempt))
                    continue
                result = (
                    self._pending_result(
                        intent,
                        client_order_id,
                        error_msg,
                        attempt + 1,
                        max_retries,
                    )
                    if submission_started and is_retryable
                    else self._error_result(
                        intent,
                        client_order_id,
                        error_msg,
                        attempt + 1,
                        max_retries,
                    )
                )
                self._record_open_order(intent, result, None)
                return result

    def _client_order_id_for_intent(self, intent: ExecutionIntent) -> str:
        fingerprint = "|".join(
            [
                intent.market_id.strip(),
                intent.topic.strip().lower(),
                intent.side.strip().upper(),
                intent.token_id.strip(),
                f"{intent.amount_usd:.8f}",
                f"{intent.worst_price:.8f}",
                intent.order_type.strip().upper(),
            ]
        )
        digest = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()[:24]
        return f"pc-{digest}"

    def _create_signed_order(
        self,
        client: Any,
        order_args: Any,
        client_order_id: str,
    ) -> Any:
        create_market_order = client.create_market_order
        try:
            signature = inspect.signature(create_market_order)
        except (TypeError, ValueError):
            signature = None
        if signature and "client_order_id" in signature.parameters:
            return create_market_order(order_args, client_order_id=client_order_id)
        signed = create_market_order(order_args)
        if isinstance(signed, dict):
            signed.setdefault("client_order_id", client_order_id)
            signed.setdefault("clientOrderId", client_order_id)
        else:
            for attr in ("client_order_id", "clientOrderId"):
                if hasattr(signed, attr):
                    try:
                        setattr(signed, attr, client_order_id)
                    except Exception:
                        pass
        return signed

    def _post_signed_order(
        self,
        client: Any,
        signed_order: Any,
        order_type: Any,
        client_order_id: str,
    ) -> Any:
        post_order = client.post_order
        try:
            signature = inspect.signature(post_order)
        except (TypeError, ValueError):
            signature = None
        if signature and "client_order_id" in signature.parameters:
            return post_order(signed_order, order_type, client_order_id=client_order_id)
        return post_order(signed_order, order_type)

    def _result_from_response(
        self,
        intent: ExecutionIntent,
        response: Any,
        client_order_id: str,
    ) -> ExecutionResult:
        payload = response if isinstance(response, dict) else {}
        order_id = self._first_string(payload, ("orderID", "orderId", "id"))
        error = self._first_string(payload, ("errorMsg", "error", "message"))
        raw_status = self._first_string(payload, ("status",), default="submitted")
        normalized_status = raw_status.strip().lower()
        avg_fill_price = self._first_float(
            payload,
            ("avgPrice", "averagePrice", "price", "matchedPrice", "fillPrice"),
        )
        filled_shares = self._first_float(
            payload,
            (
                "filledShares",
                "filled_shares",
                "sizeMatched",
                "matchedAmount",
                "matchedSize",
                "filledAmount",
                "filledSize",
            ),
        )
        if normalized_status in {"success", "matched", "filled"} and avg_fill_price <= 0:
            avg_fill_price = intent.worst_price
        if (
            normalized_status in {"success", "matched", "filled"}
            and filled_shares <= 0
            and avg_fill_price > 0
        ):
            filled_shares = round(intent.amount_usd / avg_fill_price, 8)
        if normalized_status not in {"success", "matched", "filled"} and filled_shares <= 0:
            avg_fill_price = 0.0
        return ExecutionResult(
            market_id=intent.market_id,
            topic=intent.topic,
            side=intent.side,
            token_id=intent.token_id,
            amount_usd=intent.amount_usd,
            worst_price=intent.worst_price,
            status=raw_status,
            order_id=order_id,
            error=error,
            copyability_score=intent.copyability_score,
            reason=intent.reason,
            market_title=intent.market_title,
            client_order_id=self._first_string(
                payload,
                ("client_order_id", "clientOrderId", "clientOrderID"),
                default=client_order_id,
            ),
            filled_shares=filled_shares,
            avg_fill_price=avg_fill_price,
        )

    def _pending_result(
        self,
        intent: ExecutionIntent,
        client_order_id: str,
        error_msg: str,
        attempt: int,
        max_retries: int,
    ) -> ExecutionResult:
        return ExecutionResult(
            market_id=intent.market_id,
            topic=intent.topic,
            side=intent.side,
            token_id=intent.token_id,
            amount_usd=intent.amount_usd,
            worst_price=intent.worst_price,
            status="pending",
            order_id="",
            error=(
                f"attempt {attempt}/{max_retries}: ambiguous submission; "
                f"manual reconciliation required: {error_msg}"
            ),
            copyability_score=intent.copyability_score,
            reason=intent.reason,
            market_title=intent.market_title,
            client_order_id=client_order_id,
            filled_shares=0.0,
            avg_fill_price=0.0,
        )

    def _error_result(
        self,
        intent: ExecutionIntent,
        client_order_id: str,
        error_msg: str,
        attempt: int,
        max_retries: int,
    ) -> ExecutionResult:
        return ExecutionResult(
            market_id=intent.market_id,
            topic=intent.topic,
            side=intent.side,
            token_id=intent.token_id,
            amount_usd=intent.amount_usd,
            worst_price=intent.worst_price,
            status="error",
            order_id="",
            error=f"attempt {attempt}/{max_retries}: {error_msg}",
            copyability_score=intent.copyability_score,
            reason=intent.reason,
            market_title=intent.market_title,
            client_order_id=client_order_id,
            filled_shares=0.0,
            avg_fill_price=0.0,
        )

    def _supports_order_lookup(self, client: Any) -> bool:
        return any(
            callable(getattr(client, attr, None))
            for attr in ("get_order_by_client_order_id", "get_orders", "get_order")
        )

    def _reconcile_order_by_client_id(
        self,
        client: Any,
        intent: ExecutionIntent,
        client_order_id: str,
    ) -> ExecutionResult | None:
        direct_lookup = getattr(client, "get_order_by_client_order_id", None)
        if callable(direct_lookup):
            try:
                response = direct_lookup(client_order_id)
            except Exception:
                response = None
            order = self._extract_matching_order(response, client_order_id)
            if order is not None:
                return self._result_from_response(intent, order, client_order_id)

        list_lookup = getattr(client, "get_orders", None)
        if callable(list_lookup):
            try:
                response = list_lookup()
            except Exception:
                response = None
            order = self._extract_matching_order(response, client_order_id)
            if order is not None:
                return self._result_from_response(intent, order, client_order_id)

        generic_lookup = getattr(client, "get_order", None)
        if callable(generic_lookup):
            try:
                response = generic_lookup(client_order_id)
            except Exception:
                response = None
            order = self._extract_matching_order(response, client_order_id)
            if order is not None:
                return self._result_from_response(intent, order, client_order_id)
        return None

    def _extract_matching_order(
        self,
        payload: Any,
        client_order_id: str,
    ) -> dict[str, Any] | None:
        if isinstance(payload, dict):
            response_client_order_id = self._first_string(
                payload,
                ("client_order_id", "clientOrderId", "clientOrderID"),
            )
            if response_client_order_id == client_order_id:
                return payload
            for key in ("order", "data", "orders", "results"):
                nested = payload.get(key)
                match = self._extract_matching_order(nested, client_order_id)
                if match is not None:
                    return match
        if isinstance(payload, list):
            for item in payload:
                match = self._extract_matching_order(item, client_order_id)
                if match is not None:
                    return match
        return None

    def _record_open_order(
        self,
        intent: ExecutionIntent,
        result: ExecutionResult,
        response: Any,
    ) -> None:
        if self.store is None or not result.client_order_id:
            return
        self.store.upsert_open_order(
            market_id=intent.market_id,
            topic=intent.topic,
            side=intent.side,
            token_id=intent.token_id,
            order_type=intent.order_type,
            price=intent.worst_price,
            amount=intent.amount_usd,
            status=result.status,
            client_order_id=result.client_order_id,
            order_id=result.order_id,
            filled_shares=result.filled_shares,
            avg_fill_price=result.avg_fill_price,
            payload=self._serialize_payload(response),
        )

    def _serialize_payload(self, payload: Any) -> str:
        if payload is None:
            return "{}"
        try:
            return json.dumps(payload, sort_keys=True, default=str)
        except TypeError:
            return json.dumps({"value": str(payload)}, sort_keys=True)

    def _first_string(
        self,
        payload: dict[str, Any],
        keys: tuple[str, ...],
        default: str = "",
    ) -> str:
        for key in keys:
            value = payload.get(key)
            if value is not None and str(value).strip():
                return str(value)
        return default

    def _first_float(self, payload: dict[str, Any], keys: tuple[str, ...]) -> float:
        for key in keys:
            value = payload.get(key)
            if value in (None, ""):
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return 0.0


class ExitRunner:
    def __init__(
        self, config: ExecutionConfig, live_data: LiveDataConfig | None
    ) -> None:
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
                market_title=market.title,
            )

            should_close = False
            reason = ""

            if pos.take_profit_pct > 0 and pnl_pct >= pos.take_profit_pct:
                should_close = True
                reason = f"take_profit triggered: pnl={pnl_pct:.4f} >= tp={pos.take_profit_pct}"
            elif pos.stop_loss_pct > 0 and pnl_pct <= -pos.stop_loss_pct:
                should_close = True
                reason = (
                    f"stop_loss triggered: pnl={pnl_pct:.4f} <= sl={pos.stop_loss_pct}"
                )
            elif pos.max_hold_minutes > 0:
                elapsed_minutes = (now - pos.opened_at).total_seconds() / 60.0
                if elapsed_minutes >= pos.max_hold_minutes:
                    should_close = True
                    reason = f"max_hold exceeded: {elapsed_minutes:.1f} min >= {pos.max_hold_minutes} min"

            if (
                not should_close
                and market.minutes_to_resolution > 0
                and market.minutes_to_resolution <= 10
            ):
                should_close = True
                reason = f"market resolving soon: {market.minutes_to_resolution} min to resolution"

            remaining_shares = self._position_shares(pos)
            close_amount_usd = round(remaining_shares * close_price, 4)
            if should_close and close_price > 0 and close_token and close_amount_usd > 0:
                close_intents.append(
                    ExecutionIntent(
                        market_id=pos.market_id,
                        topic=pos.topic,
                        side="CLOSE",
                        token_id=close_token,
                        amount_usd=close_amount_usd,
                        worst_price=close_price,
                        copyability_score=0.0,
                        order_type=self.config.order_type.upper(),
                        reason=reason,
                        market_title=market.title,
                    )
                )
            updated_positions.append(updated_pos)

        return close_intents, updated_positions

    def _position_shares(self, position: Position) -> float:
        if position.remaining_shares > 0:
            return position.remaining_shares
        if position.entry_shares > 0:
            return position.entry_shares
        if position.entry_price <= 0:
            return 0.0
        return round(position.entry_amount_usd / position.entry_price, 8)


def retry_delay(base_delay: float, attempt: int) -> float:
    return base_delay * (2**attempt) * random.uniform(0.5, 1.5)


def intents_as_dicts(intents: list[ExecutionIntent]) -> list[dict[str, Any]]:
    return [asdict(intent) for intent in intents]

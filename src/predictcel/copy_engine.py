"""Copy trading engine.

Evaluates markets and generates copy trading signals based on
wallet baskets and market conditions.
"""

from __future__ import annotations

import logging
import os
import pickle
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from .basket_controller import evaluate_basket_consensus_gate
from .config import AppConfig, BasketRule
from .models import CopyCandidate, MarketRegime, MarketSnapshot, WalletQuality, WalletTrade
from .scoring import compute_copyability_score
from .wallet_registry import build_live_basket_roster

__all__ = ["CopyEngine"]


logger = logging.getLogger(__name__)

LATE_ENTRY_PRICE_THRESHOLD = 0.95
NO_VALID_TRADE_REASON_KEYS = (
    "topic_mismatch",
    "wallet_not_in_basket",
    "too_old",
    "too_small",
)


class CopyEngine:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.baskets_by_topic = {basket.topic: basket for basket in config.baskets}
        self.basket_wallets_by_topic = {basket.topic: set(basket.wallets) for basket in config.baskets}
        self.last_diagnostics: dict[str, int] = {}
        self._ml_model = None
        self._ml_model_loaded = False
        self._load_ml_model()

    def _load_ml_model(self) -> Any | None:
        if self._ml_model_loaded:
            return self._ml_model
        model_path = os.path.join(os.path.dirname(__file__), "position_sizing_model.pkl")
        if not os.path.exists(model_path):
            self._ml_model_loaded = True
            return None
        try:
            with open(model_path, "rb") as f:
                self._ml_model = pickle.load(f)
        except Exception:
            self._ml_model = None
        self._ml_model_loaded = True
        return self._ml_model

    def train_position_sizing_model(self, backtest_data: list[dict]) -> None:
        """Train ML model on backtest data for position sizing."""
        from sklearn.ensemble import RandomForestRegressor
        from sklearn.metrics import mean_squared_error
        from sklearn.model_selection import train_test_split

        features = []
        targets = []
        for record in backtest_data:
            feat = [
                record.get("confidence_score", 0.5),
                record.get("copyability_score", 0.5),
                record.get("price", 0.5),
                record.get("volatility", 0.02),
                record.get("win_rate", 0.5),
                record.get("liquidity_usd", 1000),
            ]
            target = record.get("actual_pnl", 0.0)
            features.append(feat)
            targets.append(target)

        if not features:
            logger.warning("No backtest data available for ML training")
            return

        X_train, X_test, y_train, y_test = train_test_split(
            features,
            targets,
            test_size=0.2,
            random_state=42,
        )
        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        mse = mean_squared_error(y_test, y_pred)
        logger.info(f"ML position sizing model trained with MSE: {mse:.4f}")

        model_path = os.path.join(os.path.dirname(__file__), "position_sizing_model.pkl")
        with open(model_path, "wb") as f:
            pickle.dump(model, f)
        self._ml_model = model

    def evaluate(
        self,
        trades: list[WalletTrade],
        markets: dict[str, MarketSnapshot],
        wallet_qualities: dict[str, WalletQuality] | None = None,
        store: Any = None,
    ) -> list[CopyCandidate]:
        wallet_qualities = wallet_qualities or {}
        grouped: dict[str, list[WalletTrade]] = {}
        for trade in trades:
            canonical_market_id = self._canonical_market_id(trade.market_id, markets)
            grouped.setdefault(canonical_market_id, []).append(trade)

        if not grouped:
            self.last_diagnostics = {
                "markets_evaluated": 0,
                "market_not_found": 0,
                "basket_not_found": 0,
                "filtered_at_evaluate": 0,
                "candidates_returned": 0,
            }
            return []

        portfolio_summary = self._portfolio_summary(store)
        live_tracked_wallets_by_topic = self._live_tracked_wallets_by_topic(trades, store)
        candidates: list[CopyCandidate] = []
        market_not_found = 0
        basket_not_found = 0
        rejection_counts: Counter[str] = Counter()
        no_valid_trade_reason_counts: Counter[str] = Counter()

        def process_market(
            market_id: str,
            market_trades: list[WalletTrade],
        ) -> tuple[list[CopyCandidate], int, int, Counter[str], Counter[str]]:
            local_candidates: list[CopyCandidate] = []
            local_market_not_found = 0
            local_basket_not_found = 0
            local_rejection_counts: Counter[str] = Counter()
            local_no_valid_trade_reason_counts: Counter[str] = Counter()
            market = markets.get(market_id)
            if market is None:
                local_market_not_found += 1
                local_rejection_counts["market_not_found"] += 1
                return (
                    local_candidates,
                    local_market_not_found,
                    local_basket_not_found,
                    local_rejection_counts,
                    local_no_valid_trade_reason_counts,
                )

            topic = self._resolve_topic(market_trades)
            basket = self.baskets_by_topic.get(topic)
            if basket is None:
                local_basket_not_found += 1
                local_rejection_counts["basket_not_found"] += 1
                return (
                    local_candidates,
                    local_market_not_found,
                    local_basket_not_found,
                    local_rejection_counts,
                    local_no_valid_trade_reason_counts,
                )

            candidate, rejection_reason, no_valid_trade_reasons = self._evaluate_market(
                topic,
                basket,
                market,
                market_trades,
                wallet_qualities,
                portfolio_summary,
                tracked_wallets=live_tracked_wallets_by_topic.get(topic),
            )
            if candidate is not None:
                local_candidates.append(candidate)
            elif rejection_reason is not None:
                local_rejection_counts[rejection_reason] += 1
                if rejection_reason == "no_valid_trades":
                    local_no_valid_trade_reason_counts.update(no_valid_trade_reasons)
            return (
                local_candidates,
                local_market_not_found,
                local_basket_not_found,
                local_rejection_counts,
                local_no_valid_trade_reason_counts,
            )

        with ThreadPoolExecutor(max_workers=max(1, min(8, len(grouped)))) as executor:
            futures = {
                executor.submit(process_market, market_id, market_trades): market_id
                for market_id, market_trades in grouped.items()
            }
            for future in as_completed(futures):
                (
                    local_candidates,
                    local_market_not_found,
                    local_basket_not_found,
                    local_rejection_counts,
                    local_no_valid_trade_reason_counts,
                ) = future.result()
                candidates.extend(local_candidates)
                market_not_found += local_market_not_found
                basket_not_found += local_basket_not_found
                rejection_counts.update(local_rejection_counts)
                no_valid_trade_reason_counts.update(local_no_valid_trade_reason_counts)

        filtered_at_evaluate = sum(
            count
            for reason, count in rejection_counts.items()
            if reason not in {"market_not_found", "basket_not_found"}
        )
        self.last_diagnostics = {
            "markets_evaluated": len(grouped),
            "market_not_found": market_not_found,
            "basket_not_found": basket_not_found,
            "filtered_at_evaluate": filtered_at_evaluate,
            "candidates_returned": len(candidates),
            **dict(sorted(rejection_counts.items())),
            **{
                f"no_valid_trades_{reason}": no_valid_trade_reason_counts[reason]
                for reason in NO_VALID_TRADE_REASON_KEYS
                if no_valid_trade_reason_counts[reason]
            },
        }
        return candidates

    def _canonical_market_id(
        self,
        market_id: str,
        markets: dict[str, MarketSnapshot],
    ) -> str:
        market = markets.get(market_id)
        if market is None:
            return market_id
        return market.market_id

    def _resolve_topic(self, trades: list[WalletTrade]) -> str:
        counts: dict[str, int] = {}
        for trade in trades:
            counts[trade.topic] = counts.get(trade.topic, 0) + 1
        return max(counts, key=counts.get)

    def _evaluate_market(
        self,
        topic: str,
        basket: BasketRule,
        market: MarketSnapshot,
        trades: list[WalletTrade],
        wallet_qualities: dict[str, WalletQuality],
        portfolio_summary: dict[str, float | int] | None = None,
        tracked_wallets: list[str] | None = None,
    ) -> tuple[CopyCandidate | None, str | None, Counter[str]]:
        wallet_set = (
            set(tracked_wallets)
            if tracked_wallets is not None
            else self.basket_wallets_by_topic.get(topic, set())
        )
        max_age = self.config.filters.max_trade_age_seconds
        min_size = self.config.filters.min_position_size_usd
        valid_trades: list[WalletTrade] = []
        invalid_reason_counts: Counter[str] = Counter()
        for trade in trades:
            if trade.topic != topic:
                invalid_reason_counts["topic_mismatch"] += 1
                continue
            if trade.wallet not in wallet_set:
                invalid_reason_counts["wallet_not_in_basket"] += 1
                continue
            if trade.age_seconds > max_age:
                invalid_reason_counts["too_old"] += 1
                continue
            if trade.size_usd < min_size:
                invalid_reason_counts["too_small"] += 1
                continue
            valid_trades.append(trade)
        if not valid_trades:
            return None, "no_valid_trades", invalid_reason_counts

        if market.liquidity_usd < self.config.filters.min_liquidity_usd:
            return None, "low_liquidity", Counter()
        if market.minutes_to_resolution < self.config.filters.min_minutes_to_resolution:
            return None, "too_close_to_resolution", Counter()
        if market.minutes_to_resolution > self.config.filters.max_minutes_to_resolution:
            return None, "too_far_from_resolution", Counter()

        wallet_votes = self._latest_wallet_votes(valid_trades)
        weighted_by_side: dict[str, float] = defaultdict(float)
        wallet_weights: dict[str, float] = {}
        trade_weights: list[tuple[WalletTrade, float, str]] = []
        for trade in wallet_votes.values():
            side = trade.side.upper()
            weight = self._trade_weight(trade, wallet_qualities)
            weighted_by_side[side] += weight
            wallet_weights[trade.wallet] = weight
            trade_weights.append((trade, weight, side))

        if not weighted_by_side:
            return None, "no_weighted_votes", Counter()
        side = max(weighted_by_side, key=weighted_by_side.get)
        aligned = [trade for trade, _, trade_side in trade_weights if trade_side == side]
        if not aligned:
            return None, "no_aligned_trades", Counter()

        aligned_wallets = sorted({trade.wallet for trade in aligned})
        controller_gate = None
        if self.config.basket_controller.enabled:
            controller_gate, controller_rejection = evaluate_basket_consensus_gate(
                self.config,
                topic,
                basket,
                wallet_votes,
                aligned,
                wallet_weights,
                store=portfolio_summary.get("_store") if portfolio_summary else None,
                tracked_wallets=tracked_wallets,
            )
            if controller_rejection is not None:
                return None, controller_rejection, Counter()
            tracked_wallet_count = len(controller_gate.tracked_wallets)
        else:
            tracked_wallet_count = len(basket.wallets)

        consensus_ratio = len(aligned_wallets) / tracked_wallet_count if tracked_wallet_count else 0.0
        total_weight = sum(weighted_by_side.values())
        aligned_weight = weighted_by_side[side]
        if controller_gate is not None:
            weighted_consensus = controller_gate.weighted_participation_ratio
        else:
            weighted_consensus = round(aligned_weight / total_weight, 4) if total_weight else 0.0
        if consensus_ratio < basket.quorum_ratio:
            return None, "below_quorum", Counter()
        if weighted_consensus < self.config.consensus.min_weighted_consensus:
            return None, "below_weighted_consensus", Counter()

        confidence_score = self._confidence_score(aligned_weight, total_weight)
        if confidence_score < self.config.consensus.min_confidence_score:
            return None, "below_confidence", Counter()

        reference_price = self._weighted_reference_price(list(wallet_votes.values()), trade_weights)
        current_price = market.yes_ask if side == "YES" else market.no_ask
        side_spread = market.yes_spread if side == "YES" else market.no_spread
        side_ask_size = market.yes_ask_size if side == "YES" else market.no_ask_size
        side_depth_usd = side_ask_size * current_price
        if not market.orderbook_ready:
            return None, "orderbook_not_ready", Counter()
        if side_depth_usd <= 0:
            return None, "insufficient_side_depth", Counter()
        if current_price >= LATE_ENTRY_PRICE_THRESHOLD:
            return None, "too_late_price", Counter()
        drift = abs(current_price - reference_price)
        if drift > self.config.filters.max_price_drift:
            return None, "too_much_drift", Counter()

        average_age = sum(trade.age_seconds for trade in aligned) / len(aligned)
        quality_values = [
            wallet_qualities[wallet].score
            for wallet in aligned_wallets
            if wallet in wallet_qualities
        ]
        wallet_quality_score = round(
            sum(quality_values) / len(quality_values),
            4,
        ) if quality_values else 0.5
        conflict_penalty = self._conflict_penalty(aligned_weight, total_weight)
        recency_score = self._recency_score(aligned)
        regime = self._classify_market_regime(
            market,
            side,
            current_price,
            side_spread,
            side_depth_usd,
        )
        base_score = compute_copyability_score(
            consensus_ratio=weighted_consensus,
            wallet_quality_score=wallet_quality_score,
            average_age_seconds=average_age,
            drift=drift,
            liquidity_usd=market.liquidity_usd,
            side_spread=side_spread,
            side_depth_usd=side_depth_usd,
            filters=self.config.filters,
            recency_half_life_seconds=self.config.consensus.recency_half_life_seconds,
        )
        copyability_score = round(
            max(
                0.0,
                min(
                    1.0,
                    (base_score * 0.70)
                    + (confidence_score * 0.15)
                    + (recency_score * 0.10)
                    + (regime.score * 0.05)
                    - conflict_penalty,
                ),
            ),
            4,
        )
        suggested_position_usd = self._suggested_position_size(
            current_price,
            confidence_score,
            copyability_score,
            portfolio_summary,
        )

        return (
            CopyCandidate(
                topic=topic,
                market_id=market.market_id,
                side=side,
                consensus_ratio=consensus_ratio,
                reference_price=reference_price,
                current_price=current_price,
                liquidity_usd=market.liquidity_usd,
                source_wallets=aligned_wallets,
                wallet_quality_score=wallet_quality_score,
                copyability_score=copyability_score,
                reason="weighted basket consensus, market regime, confidence, recency, liquidity, drift, and tradable orderbook inputs passed",
                market_title=market.title,
                weighted_consensus=weighted_consensus,
                confidence_score=confidence_score,
                conflict_penalty=conflict_penalty,
                recency_score=recency_score,
                suggested_position_usd=suggested_position_usd,
                market_regime=regime.label,
                regime_score=regime.score,
                regime_reason=regime.reason,
            ),
            None,
            Counter(),
        )

    def _latest_wallet_votes(self, trades: list[WalletTrade]) -> dict[str, WalletTrade]:
        latest: dict[str, WalletTrade] = {}
        for trade in trades:
            existing = latest.get(trade.wallet)
            if existing is None or trade.age_seconds < existing.age_seconds:
                latest[trade.wallet] = trade
        return latest

    def _trade_weight(
        self,
        trade: WalletTrade,
        wallet_qualities: dict[str, WalletQuality],
    ) -> float:
        quality = wallet_qualities.get(trade.wallet)
        quality_weight = quality.score if quality is not None else 0.5
        recency_weight = 0.5 ** (
            trade.age_seconds / self.config.consensus.recency_half_life_seconds
        )
        size_weight = min(
            max(
                trade.size_usd / max(self.config.filters.min_position_size_usd, 1.0),
                0.25,
            ),
            3.0,
        )
        return max(quality_weight * recency_weight * size_weight, 0.0001)

    def _weighted_reference_price(
        self,
        trades: list[WalletTrade],
        trade_weights: list[tuple[WalletTrade, float, str]] | None = None,
    ) -> float:
        weighted_sum = 0.0
        total_weight = 0.0
        if trade_weights is not None:
            for trade, weight, _ in trade_weights:
                weighted_sum += trade.price * weight
                total_weight += weight
        else:
            for trade in trades:
                weight = self._trade_weight(trade, {})
                weighted_sum += trade.price * weight
                total_weight += weight
        return (
            weighted_sum / total_weight
            if total_weight
            else sum(trade.price for trade in trades) / len(trades)
        )

    def _confidence_score(self, aligned_weight: float, total_weight: float) -> float:
        prior = self.config.consensus.confidence_prior_strength
        posterior_mean = (
            (aligned_weight + prior * 0.5) / (total_weight + prior)
            if total_weight + prior
            else 0.0
        )
        sample_strength = (
            min(total_weight / (total_weight + prior), 1.0)
            if total_weight + prior
            else 0.0
        )
        return round(max(0.0, min(1.0, posterior_mean * sample_strength)), 4)

    def _conflict_penalty(self, aligned_weight: float, total_weight: float) -> float:
        if total_weight <= 0:
            return 0.0
        conflict_ratio = max(0.0, 1.0 - (aligned_weight / total_weight))
        return round(
            conflict_ratio * self.config.consensus.conflict_penalty_weight,
            4,
        )

    def _recency_score(self, trades: list[WalletTrade]) -> float:
        if not trades:
            return 0.0
        weights = [
            0.5 ** (
                trade.age_seconds / self.config.consensus.recency_half_life_seconds
            )
            for trade in trades
        ]
        return round(sum(weights) / len(weights), 4)

    def _classify_market_regime(
        self,
        market: MarketSnapshot,
        side: str,
        current_price: float,
        side_spread: float,
        side_depth_usd: float,
    ) -> MarketRegime:
        config = self.config.market_regime
        if not config.enabled:
            return MarketRegime("DISABLED", 0.5, "market regime scoring disabled")

        volatility_score = min(side_spread / 0.05, 1.0)
        depth_score = min(side_depth_usd / 1000, 1.0)

        if (
            side_spread > config.max_stable_spread
            or side_depth_usd < config.min_depth_usd
        ):
            score = max(
                0.0,
                0.5 - config.unstable_penalty - 0.1 * volatility_score,
            )
            return MarketRegime(
                "UNSTABLE",
                round(score, 4),
                f"spread={side_spread:.4f}, depth=${side_depth_usd:.0f}, volatility={volatility_score:.2f}",
            )

        skew = abs(current_price - 0.5)
        if skew >= config.trend_price_skew:
            directional_match = (
                (side == "YES" and current_price > 0.5)
                or (side == "NO" and current_price < 0.5)
            )
            score = 0.75 + config.trend_bonus if directional_match else 0.60
            score += 0.05 * (1 - volatility_score)
            return MarketRegime(
                "TREND",
                round(min(score, 1.0), 4),
                f"price skew={skew:.3f}, directional_match={directional_match}, volatility={volatility_score:.2f}",
            )

        if skew <= config.range_price_skew:
            score = round(
                min(0.65 + config.range_bonus + 0.1 * depth_score, 1.0),
                4,
            )
            return MarketRegime(
                "RANGE",
                score,
                f"price near midpoint, depth_score={depth_score:.2f}",
            )

        score = 0.55 + 0.05 * (1 - volatility_score)
        return MarketRegime(
            "TRANSITION",
            round(min(score, 1.0), 4),
            f"between range/trend thresholds, volatility={volatility_score:.2f}",
        )

    def _portfolio_summary(self, store: Any = None) -> dict[str, float | int]:
        if store is None:
            return {}
        summary = store.get_portfolio_summary(
            starting_bankroll_usd=self.config.consensus.bankroll_usd,
        )
        if isinstance(summary, dict):
            summary = dict(summary)
            summary["_store"] = store
        return summary

    def _live_tracked_wallets_by_topic(
        self,
        trades: list[WalletTrade],
        store: Any = None,
    ) -> dict[str, list[str]]:
        if (
            not self.config.basket_controller.enabled
            or store is None
            or not hasattr(store, "load_basket_memberships")
        ):
            return {}

        memberships = store.load_basket_memberships()
        if not memberships:
            return {}

        live_roster = build_live_basket_roster(self.config, memberships, trades)
        tracked_wallets_by_topic: dict[str, list[str]] = {}
        for topic, roster in live_roster.items():
            selected_wallets = roster.get("selected_wallets")
            if not isinstance(selected_wallets, dict):
                continue
            tracked_wallets = list(selected_wallets.get("core", [])) + list(
                selected_wallets.get("rotating", [])
            )
            if self.config.basket_controller.allow_backup_in_live_consensus:
                tracked_wallets.extend(selected_wallets.get("backup", []))
            tracked_wallets = list(dict.fromkeys(tracked_wallets))
            if tracked_wallets:
                tracked_wallets_by_topic[topic] = tracked_wallets
        return tracked_wallets_by_topic

    def _suggested_position_size(
        self,
        price: float,
        confidence_score: float,
        copyability_score: float,
        portfolio_summary: dict[str, float | int] | None = None,
    ) -> float:
        if price <= 0 or price >= 1:
            return 0.0
        b = (1.0 - price) / price
        p = max(0.0, min(1.0, (confidence_score + copyability_score) / 2.0))

        kelly_multiplier = 1.0
        win_rate = 0.5
        if portfolio_summary:
            win_rate = float(portfolio_summary.get("win_rate", 0.5) or 0.5)
            if win_rate < 0.4:
                kelly_multiplier = 0.5
            elif win_rate > 0.6:
                kelly_multiplier = 1.2

        if self._ml_model:
            features = [
                confidence_score,
                copyability_score,
                price,
                0.02,
                win_rate,
                1000,
            ]
            ml_prediction = self._ml_model.predict([features])[0]
            raw_size = (
                self.config.consensus.bankroll_usd
                * max(0.0, min(1.0, ml_prediction))
                * kelly_multiplier
            )
            logger.debug(
                f"ML position sizing: predicted fraction {ml_prediction:.4f}, size {raw_size:.2f}",
            )
        else:
            adjusted_kelly = self.config.consensus.kelly_fraction * kelly_multiplier
            q = 1.0 - p
            kelly_fraction = max(0.0, ((b * p) - q) / b) if b > 0 else 0.0
            raw_size = (
                self.config.consensus.bankroll_usd
                * kelly_fraction
                * adjusted_kelly
            )
            logger.debug(
                f"Kelly position sizing: fraction {kelly_fraction:.4f}, size {raw_size:.2f}",
            )

        return round(
            min(
                max(raw_size, 0.0),
                self.config.consensus.max_suggested_position_usd,
            ),
            4,
        )

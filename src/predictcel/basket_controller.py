"""Basket Controller v2 consensus helpers."""
from __future__ import annotations

from dataclasses import dataclass

from .config import AppConfig, BasketRule
from .models import WalletTrade

LIVE_CONSENSUS_TIERS = {"core", "rotating"}


@dataclass(frozen=True)
class BasketConsensusGate:
    """Computed live-basket consensus state for a market."""

    tracked_wallets: list[str]
    participating_wallet_count: int
    aligned_wallet_count: int
    basket_participation_ratio: float
    weighted_participation_ratio: float
    price_band_abs: float
    time_spread_seconds: int


def evaluate_basket_consensus_gate(
    config: AppConfig,
    topic: str,
    basket: BasketRule,
    wallet_votes: dict[str, WalletTrade],
    aligned_trades: list[WalletTrade],
    wallet_weights: dict[str, float],
    store=None,
) -> tuple[BasketConsensusGate, str | None]:
    """Evaluate the opt-in live basket controller gate."""
    tracked_wallets = _tracked_wallets_for_topic(config, topic, basket, store)
    tracked_wallet_set = set(tracked_wallets)
    participating_wallets = [
        wallet for wallet in tracked_wallets if wallet in wallet_votes
    ]
    aligned_wallets = [
        trade.wallet for trade in aligned_trades if trade.wallet in tracked_wallet_set
    ]

    tracked_wallet_count = len(tracked_wallets)
    aligned_wallet_count = len(aligned_wallets)
    participating_wallet_count = len(participating_wallets)
    basket_participation_ratio = (
        aligned_wallet_count / tracked_wallet_count if tracked_wallet_count else 0.0
    )

    participating_weight = sum(wallet_weights.get(wallet, 0.0) for wallet in participating_wallets)
    aligned_weight = sum(wallet_weights.get(wallet, 0.0) for wallet in aligned_wallets)
    weighted_participation_ratio = (
        aligned_weight / participating_weight if participating_weight else 0.0
    )

    aligned_prices = [trade.price for trade in aligned_trades if trade.wallet in tracked_wallet_set]
    price_band_abs = (
        max(aligned_prices) - min(aligned_prices) if len(aligned_prices) >= 2 else 0.0
    )
    aligned_ages = [trade.age_seconds for trade in aligned_trades if trade.wallet in tracked_wallet_set]
    time_spread_seconds = (
        max(aligned_ages) - min(aligned_ages) if len(aligned_ages) >= 2 else 0
    )

    gate = BasketConsensusGate(
        tracked_wallets=tracked_wallets,
        participating_wallet_count=participating_wallet_count,
        aligned_wallet_count=aligned_wallet_count,
        basket_participation_ratio=round(basket_participation_ratio, 4),
        weighted_participation_ratio=round(weighted_participation_ratio, 4),
        price_band_abs=round(price_band_abs, 4),
        time_spread_seconds=time_spread_seconds,
    )

    controller = config.basket_controller
    if participating_wallet_count < controller.min_active_eligible_wallets:
        return gate, "insufficient_basket_participants"
    if aligned_wallet_count < controller.min_aligned_wallet_count:
        return gate, "below_basket_participation"
    if basket_participation_ratio < controller.min_basket_participation_ratio:
        return gate, "below_basket_participation"
    if weighted_participation_ratio < controller.min_weighted_participation_ratio:
        return gate, "below_weighted_basket_participation"
    if price_band_abs > controller.max_entry_price_band_abs:
        return gate, "wide_entry_price_band"
    if time_spread_seconds > controller.max_entry_time_spread_seconds:
        return gate, "wide_entry_time_spread"
    return gate, None


def _tracked_wallets_for_topic(
    config: AppConfig,
    topic: str,
    basket: BasketRule,
    store=None,
) -> list[str]:
    if store is None or not hasattr(store, "load_basket_memberships"):
        return list(basket.wallets)

    memberships = store.load_basket_memberships(topic)
    if not memberships:
        return list(basket.wallets)

    live_tiers = set(LIVE_CONSENSUS_TIERS)
    if config.basket_controller.allow_backup_in_live_consensus:
        live_tiers.add("backup")

    live_memberships = [
        membership
        for membership in memberships
        if membership.active and membership.tier in live_tiers
    ]
    if not live_memberships:
        return list(basket.wallets)

    live_memberships.sort(key=lambda membership: (membership.rank, membership.wallet))
    return [membership.wallet for membership in live_memberships]

from __future__ import annotations

import argparse
import json

from .arb_sidecar import ArbitrageSidecar
from .config import load_config
from .copy_engine import CopyEngine
from .markets import load_market_snapshots
from .polymarket import (
    PolymarketPublicClient,
    build_market_snapshots,
    build_wallet_trades,
    enrich_market_snapshots_with_orderbooks,
)
from .scoring import WalletQualityScorer
from .storage import SignalStore
from .wallets import load_wallet_trades


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="PredictCel V1 paper engine")
    parser.add_argument("--config", required=True, help="Path to config JSON file")
    parser.add_argument("--db", default="predictcel.db", help="SQLite database path")
    parser.add_argument(
        "--live-data",
        action="store_true",
        help="Fetch live public market and wallet data from Polymarket instead of local example files",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_config(args.config)
    use_live_data = bool(args.live_data or (config.live_data and config.live_data.enabled))

    if use_live_data:
        trades, markets = _load_live_inputs(config)
    else:
        trades = load_wallet_trades(config.wallet_trades_path)
        markets = load_market_snapshots(config.market_snapshots_path)

    wallet_quality_scorer = WalletQualityScorer(config.filters)
    wallet_qualities = wallet_quality_scorer.score(trades, markets)

    copy_engine = CopyEngine(config)
    arb_sidecar = ArbitrageSidecar(config.arbitrage)

    copy_candidates = copy_engine.evaluate(trades, markets, wallet_qualities)
    arbitrage_opportunities = arb_sidecar.scan(markets)

    store = SignalStore(args.db)
    store.save_copy_candidates(copy_candidates)
    store.save_arbitrage_opportunities(arbitrage_opportunities)

    print(json.dumps({
        "mode": "live" if use_live_data else "file",
        "wallet_qualities": {wallet: quality.__dict__ for wallet, quality in wallet_qualities.items()},
        "copy_candidates": [candidate.__dict__ for candidate in copy_candidates],
        "arbitrage_opportunities": [opportunity.__dict__ for opportunity in arbitrage_opportunities],
    }, indent=2))


def _load_live_inputs(config):
    if config.live_data is None:
        raise ValueError("--live-data was requested but live_data is not configured.")

    client = PolymarketPublicClient(
        gamma_base_url=config.live_data.gamma_base_url,
        data_base_url=config.live_data.data_base_url,
        clob_base_url=config.live_data.clob_base_url,
        timeout_seconds=config.live_data.request_timeout_seconds,
    )
    topic_by_wallet = {
        wallet: basket.topic
        for basket in config.baskets
        for wallet in basket.wallets
    }
    wallet_payloads = {
        wallet: client.fetch_wallet_trades(wallet, config.live_data.trade_limit)
        for wallet in topic_by_wallet
    }
    trades = build_wallet_trades(wallet_payloads, topic_by_wallet)
    market_payload = client.fetch_active_markets(config.live_data.market_limit)
    markets = build_market_snapshots(market_payload)
    markets = enrich_market_snapshots_with_orderbooks(markets, client)
    return trades, markets


if __name__ == "__main__":
    main()

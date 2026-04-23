from __future__ import annotations

import argparse
import json

from .arb_sidecar import ArbitrageSidecar
from .config import load_config
from .copy_engine import CopyEngine
from .markets import load_market_snapshots
from .storage import SignalStore
from .wallets import load_wallet_trades


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="PredictCel V1 paper engine")
    parser.add_argument("--config", required=True, help="Path to config JSON file")
    parser.add_argument("--db", default="predictcel.db", help="SQLite database path")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_config(args.config)
    trades = load_wallet_trades(config.wallet_trades_path)
    markets = load_market_snapshots(config.market_snapshots_path)

    copy_engine = CopyEngine(config)
    arb_sidecar = ArbitrageSidecar(config.arbitrage)

    copy_candidates = copy_engine.evaluate(trades, markets)
    arbitrage_opportunities = arb_sidecar.scan(markets)

    store = SignalStore(args.db)
    store.save_copy_candidates(copy_candidates)
    store.save_arbitrage_opportunities(arbitrage_opportunities)

    print(json.dumps({
        "copy_candidates": [candidate.__dict__ for candidate in copy_candidates],
        "arbitrage_opportunities": [opportunity.__dict__ for opportunity in arbitrage_opportunities],
    }, indent=2))


if __name__ == "__main__":
    main()

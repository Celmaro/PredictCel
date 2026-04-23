from __future__ import annotations

import argparse
import json
from pathlib import Path

from .discovery import candidates_as_dicts, score_wallet_candidates
from .polymarket import PolymarketPublicClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Discover candidate Polymarket wallets for PredictCel baskets")
    parser.add_argument("--limit", type=int, default=25, help="Leaderboard rows to inspect")
    parser.add_argument("--trade-limit", type=int, default=25, help="Recent trades to fetch per wallet")
    parser.add_argument("--min-score", type=float, default=0.45, help="Minimum candidate score to output")
    parser.add_argument("--output", default="data/wallet_candidates.json", help="Path to write ranked candidates")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    client = PolymarketPublicClient()
    leaderboard = client.fetch_leaderboard(args.limit)
    trades_by_wallet = {
        str(row.get("proxyWallet") or "").lower(): client.fetch_wallet_trades(str(row.get("proxyWallet")), args.trade_limit)
        for row in leaderboard
        if row.get("proxyWallet")
    }
    candidates = [
        candidate
        for candidate in score_wallet_candidates(leaderboard, trades_by_wallet)
        if candidate.score >= args.min_score
    ]
    payload = candidates_as_dicts(candidates)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps({"candidate_count": len(payload), "output": str(output_path), "candidates": payload}, indent=2))


if __name__ == "__main__":
    main()

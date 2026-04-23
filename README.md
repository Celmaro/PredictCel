# PredictCel

PredictCel is a minimal V1 research bot for Polymarket-style strategies.

This repository starts with two paper-mode components:
- basket-consensus copy signal generation
- a simple arbitrage sidecar scanner

The current version is intentionally small and safe:
- no live order execution
- no private-key handling
- no market making
- no LLM hot path
- no cross-platform execution

Instead, V1 focuses on the alpha layer we actually want to test:
- topic baskets of source wallets
- quorum-based consensus signals
- copyability filters like drift, liquidity, and category match
- deterministic arbitrage detection from market snapshots

## What V1 does

- loads baskets, filters, and risk settings from `config/predictcel.example.json`
- reads source-wallet trade snapshots from `data/wallet_trades.example.json`
- reads market snapshots from `data/market_snapshots.example.json`
- evaluates basket consensus per market
- emits paper-mode copy candidates
- scans for simple YES/NO underpricing opportunities
- stores all emitted signals into SQLite

## What V1 does not do

- place live orders
- connect to Kalshi
- do market making
- do dispute or resolution trading
- run on 5 minute crypto latency games

## Quick start

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e .
python -m predictcel.main --config config/predictcel.example.json --db predictcel.db
```

On Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .
python -m predictcel.main --config config/predictcel.example.json --db predictcel.db
```

## Project layout

- `src/predictcel/config.py` - config loading and validation
- `src/predictcel/models.py` - small dataclasses used by the engines
- `src/predictcel/markets.py` - market snapshot loading
- `src/predictcel/wallets.py` - wallet trade loading and basket bucketing
- `src/predictcel/copy_engine.py` - basket-consensus paper signal engine
- `src/predictcel/arb_sidecar.py` - simple arbitrage scanner
- `src/predictcel/storage.py` - SQLite logging
- `src/predictcel/main.py` - CLI entrypoint
- `tests/` - focused unit tests for consensus and arbitrage

## Next steps

Once the paper engine looks sane, the next layers should be:
1. live Polymarket adapters for wallet and market data
2. richer copyability scoring
3. optional live execution behind an explicit flag
4. cross-platform sidecars only after the core engine is validated

# PredictCel

PredictCel is a minimal V1 research bot for Polymarket-style strategies.

This repository starts with two paper-mode components:
- basket-consensus copy signal generation
- a simple arbitrage sidecar scanner

The current version is intentionally small and safe:
- paper mode is still the default
- live trading only runs behind an explicit flag and config block
- no private-key handling in config files
- no market making
- no LLM hot path
- no cross-platform execution

Instead, V1 focuses on the alpha layer we actually want to test:
- topic baskets of source wallets
- quorum-based consensus signals
- copyability filters like drift, liquidity, category match, and orderbook quality
- wallet quality ranking from recent behavior
- deterministic arbitrage detection from market snapshots

## What V1 does

- loads baskets, filters, risk settings, and optional execution settings from `config/predictcel.example.json`
- supports two input modes:
  - local file-backed example mode
  - live public Polymarket read mode
- evaluates wallet quality from recent eligible trades
- evaluates basket consensus per market
- emits paper-mode copy candidates with copyability scores
- scans for simple YES/NO underpricing opportunities
- can plan live copy orders from top-ranked signals
- stores signals and execution results into SQLite

## What V1 does not do

- auto-enable live trading
- connect to Kalshi
- do market making
- do dispute or resolution trading
- run on 5 minute crypto latency games
- manage open orders, exits, or portfolio hedging yet

## Quick start

### File-backed mode

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

### Live public data mode

This mode uses public Polymarket endpoints only.
It reads:
- active markets from Gamma
- recent wallet trades from the Data API
- public order books from the CLOB API

It still does not place trades.

```bash
python -m predictcel.main --config config/predictcel.example.json --db predictcel.db --live-data
```

### Guarded live trading mode

This path remains opt-in and should be treated carefully.

1. Enable the `execution` block in your config.
2. Install the trading dependency:

```bash
pip install -e .[trade]
```

3. Set credentials in environment variables, not in the config file:
- `PREDICTCEL_POLY_PRIVATE_KEY`
- `PREDICTCEL_POLY_FUNDER`
- optional: `PREDICTCEL_POLY_HOST`

4. Start with dry run enabled in config, then explicitly invoke:

```bash
python -m predictcel.main --config config/predictcel.example.json --db predictcel.db --live-data --live-trading
```

When `execution.dry_run` is true, the bot only emits execution results with `dry_run` status and does not post orders.

To use this meaningfully, replace the example basket wallets in `config/predictcel.example.json` with real wallet addresses.

## Project layout

- `src/predictcel/config.py` - config loading and validation
- `src/predictcel/models.py` - small dataclasses used by the engines
- `src/predictcel/markets.py` - file-backed market snapshot loading
- `src/predictcel/wallets.py` - file-backed wallet trade loading
- `src/predictcel/polymarket.py` - public Polymarket live ingestion, token normalization, and orderbook enrichment
- `src/predictcel/scoring.py` - wallet quality and copyability scoring
- `src/predictcel/copy_engine.py` - basket-consensus paper signal engine
- `src/predictcel/arb_sidecar.py` - simple arbitrage scanner
- `src/predictcel/execution.py` - execution planning and guarded live order submission
- `src/predictcel/storage.py` - SQLite logging
- `src/predictcel/main.py` - CLI entrypoint
- `tests/` - focused unit tests for consensus, execution, arbitrage, and scoring

## Notes on scoring

Wallet quality is currently based on:
- freshness of recent trades
- drift discipline versus current market pricing proxy
- sample size of eligible recent trades

Copyability score is currently based on:
- basket consensus ratio
- average source wallet quality
- freshness of aligned trades
- drift from reference entry
- available market liquidity
- side-specific spread from the public order book
- side-specific top-of-book ask depth

These are intentionally simple V1 heuristics. They are meant to rank and filter, not to pretend we already have production alpha.

## Notes on live mode

The live mode is intentionally approximate and conservative:
- it normalizes `clobTokenIds` from Gamma into YES and NO token identifiers when possible
- it enriches market snapshots with public CLOB top-of-book data
- it uses wallet trade history for basket detection only
- it skips markets whose metadata cannot be normalized safely

This is enough for signal generation and cautious execution planning, not enough for full production trading.

## Next steps

Once this guarded path looks sane, the next layers should be:
1. position and open-order awareness before repeat submissions
2. explicit exit logic and portfolio caps
3. richer basket maintenance and wallet rotation rules
4. copyability features based on fuller order book depth and spread history
5. cross-platform sidecars only after the core engine is validated

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
- copyability filters like drift, liquidity, category match, orderbook quality, and market regime
- wallet quality ranking from recent behavior
- deterministic arbitrage detection from market snapshots

## What V1 does

- loads baskets, filters, risk settings, and optional execution settings from `config/predictcel.example.json`
- supports two input modes:
  - local file-backed example mode
  - live public Polymarket read mode
- evaluates wallet quality from recent eligible trades
- evaluates basket consensus per market
- classifies copy-signal markets as trend, range, transition, or unstable regimes
- emits paper-mode copy candidates with copyability scores
- scans for simple YES/NO underpricing opportunities
- can plan live copy orders from top-ranked signals
- stores signals, duplicate-signal fingerprints, positions, and execution results into SQLite
- emits per-cycle latency timings for the load, scoring, copy, arbitrage, execution, and storage stages

## What V1 does not do

- auto-enable live trading
- connect to Kalshi
- do market making
- do dispute or resolution trading
- run on 5 minute crypto latency games
- manage full portfolio hedging yet

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

It still does not place trades. Wallet and orderbook reads are fetched with bounded parallelism, and public API retries include jittered exponential backoff.

```bash
python -m predictcel.main --config config/predictcel.example.json --db predictcel.db --live-data
```

### Wallet discovery and basket auto-update

Wallet discovery defaults to `auto_update`. It reads candidate wallets from public Polymarket data, classifies their trade topics, scores candidate quality, recommends basket assignments, writes JSON reports, and appends approved `add` actions to the configured basket JSON.

```bash
python -m predictcel.main discover-wallets --config config/predictcel.example.json --output-dir data
```

The command writes:
- `wallet_discovery_report.json` for scored candidates and rejection reasons
- `basket_assignments.json` for topic affinities and recommended baskets
- `basket_manager_plan.json` for add/observe actions
- `updated_config` when `wallet_discovery.mode` is `auto_update`

Use `wallet_discovery.mode: "propose_config"` to write `predictcel.proposed.json` without touching the source config. Use `wallet_discovery.mode: "report_only"` to write reports only.

Use `--config-output path/to/config.json` to send auto-updated or proposed config output to a separate file.

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

When `execution.dry_run` is true, the bot only emits execution results with `dry_run` status and does not post orders. Dry-run results are logged and de-duplicated, but they are not stored as open positions.

To use this meaningfully, replace the example basket wallets in `config/predictcel.example.json` with real wallet addresses.

## Railway deployment

`railway.toml` runs PredictCel as a Railway worker, not a web service. Do not attach a public domain unless you add an HTTP status endpoint.

Create a Railway volume mounted at `/data` before relying on persisted state. The default worker database path is `/data/predictcel.db`; without a volume, SQLite state can be lost on redeploy or restart.

Railway installs the paper/test dependency profile by default with `pip install -e .[dev]`. The optional `py-clob-client` trading dependency is intentionally not installed during normal Railway builds; install `.[trade]` only for a separately validated live-trading deployment.

Recommended worker variables:
- `PREDICTCEL_MODE=paper` for file-backed example mode
- `PREDICTCEL_MODE=live-data` for public Polymarket reads without trading
- `PREDICTCEL_MODE=dry-run-trading` for live data plus execution planning while `execution.dry_run` remains true
- `PREDICTCEL_MODE=live-trading` only after credentials, config, and jurisdictional eligibility are verified
- `PREDICTCEL_RUN_INTERVAL_SECONDS=300` for paper mode; omit it for live modes to use the 60 second live default
- `PREDICTCEL_RUN_ONCE=false`
- `PREDICTCEL_CONFIG=config/predictcel.example.json`
- `PREDICTCEL_DB=/data/predictcel.db`

Legacy variables still work when `PREDICTCEL_MODE` is not set:
- `PREDICTCEL_LIVE_DATA=true`
- `PREDICTCEL_LIVE_TRADING=true`

Live trading variables:
- `PREDICTCEL_POLY_PRIVATE_KEY`
- `PREDICTCEL_POLY_FUNDER`
- optional: `PREDICTCEL_POLY_HOST`

The Railway worker catches per-cycle exceptions, logs JSON events, waits for the next interval, and continues. Each normal run prints a compact `summary` object with counts for markets, wallet trades, copy candidates, duplicate skips, execution intents, and open positions, plus a `latency_ms` object for stage-level timing.

## Project layout

- `src/predictcel/config.py` - config loading and validation
- `src/predictcel/models.py` - small dataclasses used by the engines
- `src/predictcel/markets.py` - file-backed market snapshot loading
- `src/predictcel/wallets.py` - file-backed wallet trade loading
- `src/predictcel/polymarket.py` - public Polymarket live ingestion, token normalization, retries, and orderbook enrichment
- `src/predictcel/scoring.py` - wallet quality and copyability scoring
- `src/predictcel/copy_engine.py` - basket-consensus paper signal engine with market regime scoring
- `src/predictcel/arb_sidecar.py` - simple arbitrage scanner
- `src/predictcel/wallet_topics.py` - wallet topic classification from trade metadata
- `src/predictcel/wallet_sources.py` - public wallet source adapters
- `src/predictcel/wallet_discovery.py` - wallet discovery pipeline, JSON reports, and config mutation
- `src/predictcel/basket_assignment.py` - wallet-to-basket assignment scoring
- `src/predictcel/basket_manager.py` - basket action planner
- `src/predictcel/execution.py` - execution planning and guarded live order submission
- `src/predictcel/storage.py` - SQLite logging, positions, and duplicate-signal fingerprints
- `src/predictcel/main.py` - CLI entrypoint
- `tests/` - focused unit tests for consensus, execution, arbitrage, storage, and scoring

## Notes on scoring

Wallet quality is currently based on:
- exponential freshness decay using `consensus.recency_half_life_seconds`
- drift discipline versus current market pricing proxy
- sample size of eligible recent trades

Copyability score is currently based on:
- basket consensus ratio
- average source wallet quality
- exponential freshness decay using `consensus.recency_half_life_seconds`
- drift from reference entry
- available market liquidity
- side-specific spread from the public order book
- side-specific top-of-book ask depth
- market regime score from trend/range/unstable classification

These are intentionally simple V1 heuristics. They are meant to rank and filter, not to pretend we already have production alpha.

## Notes on live mode

The live mode is intentionally approximate and conservative:
- it normalizes `clobTokenIds` from Gamma into YES and NO token identifiers when possible
- it enriches market snapshots with public CLOB top-of-book data
- it uses wallet trade history for basket detection only
- it skips failed wallet fetches and keeps the rest of the cycle alive
- it skips markets whose metadata cannot be normalized safely

This is enough for signal generation and cautious execution planning, not enough for full production trading.

## Next steps

Once this guarded path looks sane, the next layers should be:
1. fuller exchange fill-state reconciliation before repeat submissions
2. explicit exit logic and portfolio caps
3. richer basket maintenance and wallet rotation rules
4. copyability features based on fuller order book depth and spread history
5. cross-platform sidecars only after the core engine is validated



# PredictCel

[![CI](https://github.com/Celmaro/PredictCel/actions/workflows/ci.yml/badge.svg)](https://github.com/Celmaro/PredictCel/actions)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Prediction market analysis tool powered by Polymarket. PredictCel analyzes wallet trading patterns, identifies consensus signals, and executes copy trades with risk management.

## Features

- **Wallet Analysis**: Score and rank wallets based on trading performance
- **Consensus Signals**: Detect when multiple quality wallets agree on a trade
- **Arbitrage Detection**: Find risk-free arbitrage opportunities across markets
- **Live Trading**: Execute trades on Polymarket (paper or live mode)
- **Risk Management**: Portfolio VaR calculation and position sizing
- **Basket Management**: Organize wallets by topic/theme

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/Celmaro/PredictCel.git
cd PredictCel

# Install with pip
pip install -e .

# Or install with optional dependencies
pip install -e ".[trade,cache,ml]"
```

### Configuration

Create a configuration file `config.json`:

```json
{
  "baskets": [
    {
      "topic": "crypto",
      "wallets": ["0x...", "0x..."],
      "quorum_ratio": 0.6
    }
  ],
  "filters": {
    "max_trade_age_seconds": 3600,
    "max_price_drift": 0.05,
    "min_liquidity_usd": 1000,
    "min_minutes_to_resolution": 60,
    "max_minutes_to_resolution": 43200,
    "min_position_size_usd": 10
  },
  "arbitrage": {
    "min_gross_edge": 0.02,
    "min_liquidity_usd": 5000
  },
  "wallet_trades_path": "data/wallet_trades.json",
  "market_snapshots_path": "data/market_snapshots.json",
  "live_data": {
    "enabled": true,
    "gamma_base_url": "https://gamma-api.polymarket.com",
    "data_base_url": "https://data-api.polymarket.com",
    "clob_base_url": "https://clob.polymarket.com",
    "market_limit": 1000,
    "trade_limit": 100,
    "request_timeout_seconds": 15
  }
}
```

### Usage

#### Run Analysis Cycle (Paper Mode)

```bash
python -m predictcel --config config.json --db predictcel.db --live-data
```

#### Run with Live Trading

```bash
python -m predictcel --config config.json --db predictcel.db --live-data --live-trading
```

#### Wallet Discovery

```bash
python -m predictcel discover-wallets --config config.json --output-dir data/discovery
```

## Architecture

```
PredictCel/
├── src/predictcel/
│   ├── main.py              # Entry point and cycle orchestration
│   ├── polymarket.py        # Polymarket API client
│   ├── storage.py           # SQLite persistence
│   ├── scoring.py           # Wallet quality scoring
│   ├── copy_engine.py       # Consensus detection
│   ├── arb_sidecar.py       # Arbitrage detection
│   ├── execution.py         # Order execution
│   └── models.py            # Data models
├── tests/                   # Test suite
└── config/                  # Example configurations
```

## Configuration Options

### Baskets
Organize wallets by topic with quorum requirements:
- `topic`: Category name (e.g., "crypto", "sports")
- `wallets`: List of wallet addresses
- `quorum_ratio`: Minimum consensus ratio (0.0-1.0)
- `target_allocation`: Target portfolio allocation

### Filters
Control which trades are considered:
- `max_trade_age_seconds`: Ignore trades older than this
- `max_price_drift`: Maximum price change since trade
- `min_liquidity_usd`: Minimum market liquidity
- `min/max_minutes_to_resolution`: Resolution time window

### Consensus
Configure signal detection:
- `recency_half_life_seconds`: Weight for trade age
- `min_weighted_consensus`: Minimum consensus threshold
- `confidence_prior_strength`: Bayesian prior strength
- `kelly_fraction`: Position sizing fraction

### Execution
Live trading configuration:
- `enabled`: Enable live trading
- `dry_run`: Simulate without executing
- `buy_amount_usd`: Default position size
- `position`: Take profit, stop loss, max hold time

## Environment Variables

```bash
# Optional: Redis for caching
REDIS_HOST=localhost
REDIS_PORT=6379

# Optional: Logging level
LOG_LEVEL=INFO
```

## Development

### Setup

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Run linting
ruff check src/
ruff format src/
```

### Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=predictcel --cov-report=html

# Run specific test
pytest tests/test_polymarket.py -v
```

## Deployment

### Railway

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/template/predictcel)

### GitHub Actions

The repository includes CI/CD workflows:
- **CI**: Lint and test on every push
- **Schedule**: Run analysis every 30 minutes
- **Manual**: Trigger runs via workflow dispatch

## API Reference

### PolymarketPublicClient

```python
from predictcel.polymarket import PolymarketPublicClient

client = PolymarketPublicClient(
    use_redis=True,  # Enable Redis caching
    timeout_seconds=15,
    max_retries=3
)

# Fetch markets
markets = client.fetch_active_markets(limit=100)

# Fetch wallet trades
trades = client.fetch_wallet_trades("0x...", limit=50)

# Get order book
book = client.fetch_order_book(token_id)
```

### SignalStore

```python
from predictcel.storage import SignalStore

# Using context manager (recommended)
with SignalStore("predictcel.db") as store:
    store.save_positions(positions)
    open_positions = store.get_open_positions()

# Or manual cleanup
store = SignalStore("predictcel.db")
try:
    store.save_positions(positions)
finally:
    store.close()
```

## Troubleshooting

### SQLite Database Locked

If you encounter "database is locked" errors:
- Ensure only one process accesses the database
- Use `SignalStore.close()` when done
- Consider using WAL mode for better concurrency

### Redis Connection Failed

If Redis is unavailable:
- The client automatically falls back to in-memory cache
- Check Redis connection settings
- Install redis package: `pip install redis`

### API Rate Limiting

If you hit rate limits:
- Increase `request_timeout_seconds`
- Reduce `market_limit` and `trade_limit`
- Enable Redis caching to reduce API calls

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Disclaimer

This software is for educational purposes only. Trading prediction markets involves significant risk. Past performance does not guarantee future results. Always do your own research and never trade more than you can afford to lose.

## Support

- GitHub Issues: [Report bugs or request features](https://github.com/Celmaro/PredictCel/issues)
- Discussions: [Ask questions or share ideas](https://github.com/Celmaro/PredictCel/discussions)

---

Built with ❤️ by [Celmaro](https://github.com/Celmaro)

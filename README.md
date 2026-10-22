# Delta BTC Options Position Manager

Python async position-management bot for Delta Exchange BTC options that:

- Dynamically monitors all open BTC call and put positions, regardless of side, expiry, or strategy structure
- Includes new and changed positions and tracks realized PnL during the bot session
- Closes managed positions when the profit target, stop loss, or optional BTC index price trigger is reached

## Key Features

- Modular architecture:
  - `exchange_client.py` (Delta API wrapper + retries)
  - `position_manager.py` (position discovery and live state)
  - `strategy_engine.py` (trigger and exit logic)
  - `order_executor.py` (market order placement and fill confirmation)
- Uses Delta's BTC spot/index price from the `BTCUSD` ticker as the trigger reference
- Reconciles realized PnL from exchange fill history
- Retries transient API failures with exponential backoff
- Detailed structured logging

## Position Management

The bot manages the BTC option positions returned by Delta while it is running. It does not require a ratio spread or assume a particular strategy structure. Long and short calls and puts, including positions with different expiry dates, are monitored independently.

For each poll, the bot reconciles the current open-position snapshot against the previous snapshot, calculates liquidation-side unrealized PnL using bid for longs and ask for shorts, and includes session realized PnL from observed closes. Newly opened and resized positions are included from the latest exchange snapshot; fully closed positions leave the open PnL calculation.

The bot triggers closure when any configured exit condition is met:

- Combined session PnL reaches `PNL_PROFIT_THRESHOLD_USD`.
- Combined session PnL reaches or falls below the negative magnitude of `PNL_LOSS_THRESHOLD_USD` (for example, `20` or `-20` both mean a $20 stop loss).
- When `BTC_INDEX_PRICE_TRIGGER` is set, the observed BTC index price reaches or crosses that level between polls.

Each close uses reduce-only market IOC orders and retries an unfilled remainder a bounded number of times. The bot does not open or rebalance positions.

## Setup

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Copy env template and set credentials:

```bash
copy .env.example .env
```

3. Run bot:

```bash
python -m src.main
```

## Environment Variables

- `DELTA_API_KEY`
- `DELTA_API_SECRET`
- `DELTA_BASE_URL` (default: `https://cdn-ind.testnet.deltaex.org`)
- `DELTA_SSL_VERIFY` (default: `false` for testnet environments with missing CA chain)
- `POLL_INTERVAL_SECONDS` (default: `2`)
- `PNL_PROFIT_THRESHOLD_USD` (default: `10`)
- `PNL_LOSS_THRESHOLD_USD` (default: `-20`; the configured magnitude is treated as the stop-loss amount)
- `BTC_INDEX_PRICE_TRIGGER` (optional; closes positions when BTC index crosses the configured price)
- `RATIO_SPREADS` is retained for compatibility but is not used by the general position manager.

Heartbeat logs run on the same interval as `POLL_INTERVAL_SECONDS`.

Realized PnL tracking is in-memory for the lifetime of the bot process. It requires identifiable, timestamped fills from Delta's fills API; if fill history cannot cover an observed reduction, the bot does not use an incomplete PnL total to trigger an exit. Commissions and fees are not included in the realized PnL calculation.

The `/summary` response includes `closed_positions` and `closed_position_count`. Each closed-position record is fill-level and includes `symbol`, `type`, `strike`, `expiry`, `side`, signed `size`, `entry`, `closed_price`, `closed_time`, and `realized_pnl`, along with fill/product IDs and additional descriptive aliases. Records are retained in memory for the bot process lifetime.

## Testing

After installing dependencies, you can run the test suite with:

python -m pytest

To generate a coverage report for the source code, run:

```bash
python -m pytest --cov=src --cov-report=term-missing
```

You can also run only specific test groups using markers:

```bash
python -m pytest -m unit -q
python -m pytest -m api -q
python -m pytest -m integration -q
python -m pytest -m async -q
```

## Notes

- The code uses `delta-rest-client` and wraps sync calls with `asyncio.to_thread`.
- Delta REST field names can vary by account/product type. Mapping logic is centralized in `src/models.py` and `src/exchange_client.py`.
- Test only on testnet first.

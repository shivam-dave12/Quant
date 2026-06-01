# Groww NIFTY Option-Only Quant Bot — live-data-only build

This bot does **not** forecast NIFTY direction first. It directly ranks NIFTY CE/PE contracts by predicted future option premium expansion after costs.

This build is strict: **no synthetic/sample data is used for training**. Models train only from option-chain and quote snapshots recorded from Groww into the local database.

## What it uses from Groww

- Option chain with Greeks: `get_option_chain(exchange=NSE, underlying=NIFTY, expiry_date=...)`
- Live quote: `get_quote(exchange=NSE, segment=FNO, trading_symbol=...)`
- Market depth / quote fields where available through quote/feed payloads
- Orders: `place_order(...)`, `get_order_detail(...)`
- Exit protection: Smart Order OCO via `create_smart_order(... SMART_ORDER_TYPE_OCO ...)`

## Data policy

Training accepts only rows tagged by the live collector:

```text
groww_option_chain
groww_quote
```

Blocked by default:

```text
synthetic rows
sample rows
dummy rows
fake rows
untagged manual inserts
```

The enforcement setting is in `bot/config.py`:

```python
require_groww_source: bool = True
```

Keep this enabled. Disabling it is only for migration/debugging of older real databases.

## Model target

For each option contract at each timestamp:

```text
future_return_after_costs = future_executable_exit / executable_entry - 1 - estimated_costs
```

The model suite ranks all liquid CE/PE contracts. Live order execution is still blocked unless the trained model passes real-data gates.

## Architecture

```text
Groww option chain + quotes
        ↓
DuckDB recorder with source tags
        ↓
Option-only feature engine
        ↓
Live-data-only model suite
        ↓
Real-data quality/live gate
        ↓
Paper/live execution with OCO exit
```

## Install

```bash
cd groww_nifty_option_bot
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Put only your Groww secret in `.env`:

```bash
GROWW_API_AUTH_TOKEN=your_token
```

Change all non-secret runtime/model/risk settings in `bot/config.py`, not in `.env`.

## Inspect your NSE contract file

The package already includes the NSE contract file you uploaded:

```bash
python -m bot.cli inspect-contracts
```

## Start live data recording

```bash
python -m bot.cli collect-loop --expiry 2026-06-02
```

Or omit expiry to use nearest expiry from `cfg.nse_contract_file`:

```bash
python -m bot.cli collect-loop
```

Keep this running during market hours. The model cannot train until enough real Groww snapshots exist.

## Audit training data provenance

```bash
python -m bot.cli data-audit
```

This shows row counts by source and flags suspicious `synthetic/dummy/fake` rows.

## Train the model suite from real Groww data

```bash
python -m bot.cli train-models
python -m bot.cli model-report
python -m bot.cli score-models-latest --limit 20
```

The model suite trains:

```text
premium return regressor
profit probability classifier
cross-sectional option ranker
return quantile models q20/q50/q80
IV expansion classifier
cost/liquidity estimator
```

Live trading gates use real labelled data only. The thresholds are configured in `bot/config.py`:

```text
top1_per_snapshot_count >= min_backtest_trades
top1_per_snapshot_win_rate >= min_model_win_rate
top1_per_snapshot_sharpe >= min_model_sharpe
top1_per_snapshot_mean_return > 0
top1_per_snapshot_alpha_vs_universe > 0
```

## Paper live mode with auto-training

```bash
python -m bot.cli live-loop
```

In paper mode, the bot:

```text
collects Groww option-chain snapshots
collects Groww quotes for liquid candidates
tries to train when enough labelled real rows exist
scores latest contracts when a trained model exists
paper-buys only after gates pass
```

## Actual live mode

There is a two-key safety switch. You need both:

1. Edit `bot/config.py`:

```python
live_trading_enabled: bool = True
paper_trading: bool = False
```

2. Run with the live CLI flag:

```bash
python -m bot.cli live-loop --live
```

The bot will still refuse live orders if the model did not pass the real-data live gates.

## Removed synthetic path

The old synthetic smoke-test script has been removed. There is no synthetic model training path in this build.

## Data caveat

A good win rate, Sharpe, and alpha cannot be guaranteed. This build enforces the right process: collect real Groww data, train only on that data, measure out-of-sample performance, and trade only when the model passes gates.

## Docker .env handling

`.env` is secrets-only. It should contain only API keys/secrets such as `GROWW_API_AUTH_TOKEN`. All runtime/model/risk settings are in `bot/config.py`.

This package Dockerfile copies `.env` into `/app/.env` when `.env` exists in the build context. Create it before building:

```bash
cp .env.example .env
# edit .env and add only GROWW_API_AUTH_TOKEN

docker build -t groww-nifty-option-bot:latest .
docker run --rm groww-nifty-option-bot:latest
```

For production, prefer passing secrets at runtime with `--env-file .env` instead of baking credentials into the image, but the Dockerfile supports copying `.env` when you explicitly need a self-contained image.

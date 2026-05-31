# BTC HFT Live-Learning Bot v5.7 — Promotion Diagnostics + Venue Cost Profiles

This package is a complete event-driven BTCUSD microstructure trading system designed for Delta India. It does not claim that an unvalidated strategy is already profitable. It runs the whole stack immediately: raw data capture, order-book reconstruction, trade-flow features, online model training, fee/fill reconciliation, dynamic bracket learning, shadow/PAPER scoring, and guarded LIVE execution.


## v5.7 fixes: promotion transparency and cost profiles

This build fixes the opaque promotion behaviour seen in v5.6:

- `prequential_count` now reports the **total** evaluation count instead of looking frozen at the 25,000 rolling window cap.
- Promotion diagnostics now separately report:
  - total prequential count,
  - rolling-window count,
  - total mean return,
  - rolling mean return,
  - eligible prediction count,
  - eligible rate,
  - eligible winners/losers,
  - per-horizon prequential stats,
  - explicit `failure_reasons`.
- Decision records now include `signal_diagnostics`, so you can see whether the model failed because of cold start, cost hurdle, neutral/non-eligible predictions, risk/bracket, or live gate.
- Cost scenarios are now reported for:
  - `DELTA_TAKER`,
  - `DELTA_MAKER`,
  - `HYPERLIQUID_TAKER`,
  - `HYPERLIQUID_MAKER`.
- `execution_cost_profile` can be changed in `btchft/config.py` for SHADOW/PAPER comparison. Hyperliquid cost mode is shadow-only until a real Hyperliquid L2/fill/funding/execution adapter is added.

Example config-only cost comparison:

```python
execution_venue: str = "DELTA"
execution_cost_profile: str = "DELTA_TAKER"
# or, for shadow-cost testing only:
execution_venue: str = "HYPERLIQUID"
execution_cost_profile: str = "HYPERLIQUID_TAKER"
```

## Configuration rule

**Only API keys and secrets go in `.env`. Every non-secret setting lives in `btchft/config.py`.**

That includes:

- `trading_mode`
- `allow_live`
- `allow_unvalidated_bootstrap_live`
- `delta_testnet`
- `delta_symbol`
- data-lake paths
- model paths
- cost assumptions
- risk limits
- learning gates
- promotion rules
- status cadence

This is intentional: production behaviour is now reproducible from versioned code instead of hidden environment switches.

## What is complete

- Delta public websocket ingestion for `ob_updates`, `trades`, `funding_rate`, and `mark_price`.
- Delta private websocket ingestion for `v2/user_trades` and position updates.
- Raw compressed event journal with real-data provenance.
- Local L2 order-book reconstruction with sequence-gap and CRC32 checksum validation.
- Trade-flow features from public trades: signed volume, CVD, burst/exhaustion measures.
- L2 features: spread, microprice, L1/L5/L10/L20 imbalance, book slope, L1 OFI.
- Online multi-horizon return regressors and event classifiers.
- Real public-trade bootstrap model trained from the uploaded BTCUSD trade file.
- REST-fill based exact commission/slippage feedback ledger.
- Adaptive TP/SL bracket policy from realised post-signal paths.
- Atomic bracket-only execution path; no naked-order fallback.
- Sticky halt on order-book integrity failure or private-fill sequence gaps.

## What is deliberately not claimed

The bootstrap model is **not live-approved**. It was trained from real public trades, but it has no L2 spread/depth, private fills, actual commission, funding or order rejection evidence. LIVE order submission is therefore gated by real online labels, realised fills and model promotion unless you explicitly set `allow_unvalidated_bootstrap_live=True` in `btchft/config.py`.

## Install

```bash
cd btc_hft_live_learning_v5
python -m venv .venv
source .venv/bin/activate
pip install -e .
cp .env.example .env
```

Fill `.env` with Delta credentials only:

```bash
DELTA_API_KEY=...
DELTA_SECRET_KEY=...
```

## Set runtime mode

Edit `btchft/config.py`:

```python
@dataclass(frozen=True)
class Settings:
    trading_mode: str = "SHADOW"   # SHADOW, PAPER, or LIVE
    allow_live: bool = False
    allow_unvalidated_bootstrap_live: bool = False
    delta_testnet: bool = False
```

For live production with safety gates enabled:

```python
trading_mode: str = "LIVE"
allow_live: bool = True
allow_unvalidated_bootstrap_live: bool = False
```

## Inspect public trade/order-flow file

Default output path comes from `Settings.tradeflow_inspection_out` in `config.py`.

```bash
btc-live-hft inspect-tradeflow futures-trades-monthly-BTCUSD-2026-05.csv.zip
```

## Train / refresh real-data bootstrap model

Default model, manifest and horizon come from `config.py`.

```bash
btc-live-hft train-bootstrap-tradeflow futures-trades-monthly-BTCUSD-2026-05.csv.zip
```

The model is used only for shadow cold-start scoring. It is not sufficient for live promotion.

## Run the full bot

```bash
btc-live-hft run
```

The bot mode is controlled by `Settings.trading_mode` in `btchft/config.py`.

In LIVE with the safe default, the bot captures and trains from real production data but does not place orders until internal gates pass:

- promoted live-learning model exists,
- enough matured real L2 labels exist,
- enough REST-reconciled real fills exist,
- no order-book checksum/sequence gap,
- no private fill sequence gap,
- daily drawdown halt not triggered,
- atomic bracket plan can be created.

## Non-secret config map

All of these are in `btchft/config.py`:

| Config field | Meaning |
|---|---|
| `trading_mode` | `SHADOW`, `PAPER`, or `LIVE` |
| `allow_live` | Must be `True` for real orders |
| `allow_unvalidated_bootstrap_live` | Emergency override. Default `False`. Do not enable unless you knowingly accept unvalidated live risk. |
| `auto_promote_model` | Allows online challenger to become internal promoted model after positive real prequential evidence |
| `raw_event_journal` | Raw Delta websocket capture path |
| `feature_journal` | Engineered causal feature/prediction journal |
| `execution_ledger` | REST-reconciled fill/commission ledger |
| `model_dir` | Online model checkpoints |
| `taker_fee_bps_pre_gst` / `gst_rate` / `impact_floor_bps` | Cost assumptions before actual fill evidence is sufficient |
| `min_labels_to_trade` / `min_real_fills_to_live_trade` | Live evidence gates |
| `max_risk_per_trade` / `max_gross_leverage` / `daily_drawdown_halt` | Risk controls |

## Strategy stack

The live decision is not a single retail rule. It combines:

1. **Regime context** from 1h 14-day momentum once live candles are fed.
2. **Trade-flow layer** from public trades and CVD/exhaustion features.
3. **L2 layer** from order-book imbalance, microprice and OFI.
4. **Event models** for cost-aware directional outcomes.
5. **Execution-cost model** from Delta REST fills.
6. **Adaptive bracket model** from realised post-signal paths.
7. **Risk engine** with max-risk, leverage and daily drawdown controls.

## Data policy

Models only accept real data. The shipped bootstrap model is trained from the uploaded BTCUSD public trade file and is marked `is_synthetic=false` and `live_approved=false`. The runtime rejects synthetic bootstrap manifests.

## Important venue/data caveat

The uploaded trade file is public trade/order-flow data, not full L2. True HFT alpha requires Delta `ob_updates` plus trades, private fills and REST commissions. This package captures and learns from all of them once running.


## v5.3 feed-health note

Default `Settings.delta_testnet` is now `False` so SHADOW mode captures the real production BTCUSD public feed. Live orders still require `trading_mode="LIVE"` and `allow_live=True`. Runtime status includes `feed_health` and blocks live mode if the book stream stalls.

## v5.4 feed/cost audit patch

The runtime feed in `quant-20260530-065610.log` is healthy: `ob_updates` and features are flowing and online labels are maturing. This patch fixes a separate cost-model issue found in that same log: product metadata had reduced the scheduled one-way taker+GST fee floor to 1.18 bps. Product metadata may now only raise the configured scheduled fee floor; it cannot reduce it. Any lower realised cost can be adopted only from REST-reconciled real fills after the configured minimum fill count.

Expected no-fill cost floor:

```text
one_way_fee_bps >= 5.9
round_trip_bps >= 15.8
```

## v5.5 ML observability: know exactly what is training and tested

The runtime now writes a third journal in addition to raw events and features:

```text
artifacts/live/decisions.jsonl.gz
```

This file records every book-decision cycle with:

- active model tests by horizon
- return-regressor samples and residual error
- event-classifier samples and accuracy
- current cost hurdle
- chosen signal, if any
- risk/bracket/live gate reason
- shadow plan, if a signal passed in SHADOW/PAPER

Use the built-in control-room command:

```bash
btc-live-hft inspect-live --live-dir artifacts/live
```

For JSON output:

```bash
btc-live-hft inspect-live --live-dir artifacts/live --json > artifacts/live/ml_control_room.json
```

On EC2/Podman anonymous volume:

```bash
ACTIVE="/home/ec2-user/.local/share/containers/storage/volumes/<volume_id>/_data"
podman run --rm \
  -v "$ACTIVE:/app/artifacts/live:ro" \
  btc-live-hft:v5.5 inspect-live --live-dir /app/artifacts/live
```

Healthy ML training means:

```text
raw events increasing
features increasing
decisions increasing
matured_labels increasing
regressor/classifier samples increasing
prequential_count increasing
checksum_failures = 0
sequence_gaps = 0
recorders dropped = 0
```

A tradable model needs more than healthy training. It needs:

```text
prequential_mean_return > 0 after costs
promotion_test.passed = true
promoted_horizon_ms != null
real_fill_count above gate for LIVE
live_gate_reason = null in LIVE mode
```


## v5.6 observability fix

Top-level status now passes the active round-trip cost hurdle into `models.tests_running.current_cost_hurdle_bps`. Earlier v5.5 decision records used the correct live cost during signal scoring, but the status summary displayed `0.0`, which was misleading.

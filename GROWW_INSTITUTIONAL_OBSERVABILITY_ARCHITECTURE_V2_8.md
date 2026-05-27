# Groww Institutional Execution & Observability Architecture — v2.8

## Why v2.8 exists

The live v2.7 run proved that official Groww infrastructure was healthy:

- TOTP authentication succeeded.
- F&O balance and position verification succeeded.
- Official NIFTY index-value feed was live.
- Official instrument CSV and option chain were loaded.
- Official option-market stream discovery received 24 two-sided books and 19 LTPs.

The NIFTY desk was disabled by our own contract-selection policy after valid market data arrived. A static theta-to-premium session veto rejected live option vehicles before any directional edge was evaluated, and the failure did not state the rejected calculation per contract.

## Official Groww data contract followed

The implementation uses:

- `GrowwFeed.subscribe_index_value()` for the NIFTY index analysis feed.
- official instrument CSV exchange tokens for routing.
- `GrowwFeed.subscribe_ltp()` and `GrowwFeed.subscribe_market_depth()` for FNO option execution-universe discovery and active vehicle execution.
- `get_option_chain()` only for option-chain model inputs such as IV, Greeks, OI and volume.
- `get_historical_candles()` with native intervals for underlying and premium-domain ATR warmup.
- OCO Smart Orders only for protection after a confirmed long-option fill.

No REST last-price or quote fallback authorises entry when an option websocket is stale.

## Execution universe architecture

### 1. Underlying analysis plane

NIFTY is analysed from the official index-value stream plus official historical candles. This plane may remain live even when an executable option vehicle is temporarily unavailable.

### 2. Option discovery plane

The official option chain is screened for verified identity, expiry, lot size, affordability and model inputs. A bounded candidate universe is subscribed to official FNO LTP and market-depth streams. All books are routed by official exchange token to prevent one option's data from contaminating another.

### 3. Session CE/PE book

A call and put are selected only from live, two-sided option books with verified lot size, controlled spread, minimum visible depth and acceptable delta exposure. If no pair is currently eligible, the NIFTY analysis plane remains active and execution-universe selection is retried on a bounded cadence.

There is no fallback contract and no substituted price.

### 4. Directional activation

Only after the NIFTY structural model produces a bullish or bearish thesis does the corresponding CE or PE become the active execution vehicle. That exact contract must still have independently fresh streamed LTP and streamed depth at order time.

### 5. Position safety

Sizing uses the verified contract lot size and option-premium risk. Protection uses option-premium ATR. A long-option fill requires confirmed Groww OCO protection; failure to confirm protection blocks new entries and invokes safety liquidation of the unprotected filled option.

## Key correction: theta is a cost, not a hidden kill-switch

Previous behavior:

```
reject contract when theta_to_premium_per_day > 0.08
```

This could disable the NIFTY desk while live books were available, before estimating whether a trade's expected displacement compensated for carry cost.

v2.8 behavior:

```
theta_carry_bps_expected_hold = abs(theta_to_premium_per_day)
                                * expected_hold_seconds / 86400
                                * 10000

premium_delta_edge_bps = underlying_edge_bps
                         * abs(option_delta)
                         * underlying_price / option_premium

net_edge_bps = premium_delta_edge_bps
               - spread_fee_slippage_cost_bps
               - theta_carry_bps_expected_hold
```

Theta is now visible in logs, influences vehicle ranking, and is deducted from the final trade edge. It does not silently terminate the session execution universe.

## No-pair behavior

Previous behavior:

```
valid official books -> no pair selected -> disable NIFTY desk permanently
```

v2.8 behavior:

```
valid official books -> no pair currently clears the model
-> NIFTY analysis remains live
-> entries remain blocked
-> official execution universe is rescanned every GROWW_SESSION_BOOK_RESCAN_SEC
-> full rejection audit is logged
```

## Telemetry emitted

With `INSTITUTIONAL_DECISION_TELEMETRY_ENABLED=true`, every decision produces an INFO log payload:

```
🧮 DECISION_CALC {
  "desk": "DESK_B_NIFTY_OPTIONS",
  "decision": "...",
  "direction": "...",
  "model": {
    "option_volatility_context": {...},
    "underlying_edge_bps": ...,
    "premium_delta_edge_bps": ...,
    "execution_cost_components": {
      "spread_bps": ...,
      "fee_bps": ...,
      "slippage_bps": ...,
      "total_cost_bps": ...
    },
    "theta_to_premium_per_day": ...,
    "theta_expected_hold_sec": ...,
    "theta_carry_bps_expected_hold": ...,
    "net_edge_bps": ...,
    "uncertainty_bps": ...,
    "execution_feed": {...},
    "sizing_decision": {...},
    "protection_plan": {...}
  },
  "reasons": [...]
}
```

Contract construction and rescans log:

- number of chain rows and streamed books;
- accepted/rejected contract counts per CE/PE side;
- rejection reason counters;
- selected bid/ask/spread/depth/cost/delta/IV/theta carry;
- whether the execution plane is monitoring, armed or fully active.

## Configuration introduced

```bash
INSTITUTIONAL_DECISION_TELEMETRY_ENABLED=true
GROWW_OPTION_SELECTION_CARRY_REFERENCE_BPS=100.0
GROWW_SESSION_BOOK_RESCAN_SEC=30.0
```

The existing risk, spread, depth, identity, lot-size, OCO protection and live-feed freshness checks remain fail-closed.

## Deployment

The systemd service must continue to pass credentials from its external `.env` file. Do not place `.env` inside the Docker build directory.

```bash
cd ~
rm -rf ~/quant-v2_8-deploy
mkdir -p ~/quant-v2_8-deploy
unzip institutional_setup_v2_8_groww_observable_execution_architecture.zip -d ~/quant-v2_8-deploy

rsync -a --delete \
  --exclude '.env' \
  --exclude '.git/' \
  ~/quant-v2_8-deploy/v2_8_groww_observable_execution_architecture/ \
  ~/quant/Quant/

cd ~/quant/Quant
rm -f .env
podman build --no-cache -t localhost/quant:latest .
systemctl --user restart quant.service
podman logs -f quant
```

## Validation

```bash
python -m compileall -q .
pytest -q
```

Result during packaging: `53 passed`.

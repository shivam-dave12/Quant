# NIFTY ICICI Session Contract Book — Institutional Flow Audit and Implementation

Date: 2026-05-22 (IST)

## Decision

Preselecting the execution contracts at the start of the NFO trading session is more efficient and is institutionally valid **only when both directions are prepared**:

- one executable CE vehicle for a future bullish NIFTY thesis;
- one executable PE vehicle for a future bearish NIFTY thesis;
- direction remains controlled by the live underlying strategy, not by a morning directional guess.

Selecting only CE or only PE for the entire day would not be institutional because it would lock the system into a directional bias before liquidity, displacement, DOL and market structure confirm the trade.

## Implemented flow

```text
Breeze session + ICICI F&O funds preflight
→ NIFTY underlying candle warmup
→ daily NFO Security Master via HTTPS (identity + verified lot size)
→ filtered OptionChain live quotes for eligible expiries/right
→ Black–Scholes/delta/theta/moneyness + affordability + live-spread/depth score
→ prewarm premium candles and live quotes for best CE and best PE
→ publish transactional SESSION CONTRACT BOOK only if both pass
→ continue scanning the NIFTY underlying only
→ bullish thesis activates preselected CE / bearish thesis activates preselected PE
→ fresh quote, delta, spread/ATR, depth and budget revalidation at execution
→ premium-native SL/TP calculation and BUY-option execution
→ broker-side protective stoploss only; TP monitored without parallel naked SELL risk
→ flat confirmation releases vehicle and restores underlying-scanning mode
→ next valid signal may activate the other session vehicle
```

## Calculation integrity

### Contract definition and quantity

- Contract lot size is sourced only from the current-day NFO Security Master.
- A quote payload cannot override `runtime_lot_size`.
- Missing verified lot size fails closed; the system does not assume lot size `1`.
- Quantity remains lot-floored and cannot be rounded above calculated risk.

### Black–Scholes selection and intraday revalidation

- The session book selects CE and PE using actual spot/strike/expiry inputs, stress IV prior and the configured risk-free rate.
- Selection uses delta proximity, theta-to-premium burden and moneyness.
- An option is not published if it already falls outside the permitted delta band.
- Intraday activation recomputes effective delta against the current NIFTY price; delta-invalid or materially spot-invalid vehicles trigger a controlled urgent book refresh with anti-thrash cooldown.

### Executability and market impact guards

- Two-sided quote is required.
- Visible bid and offer depth must each support at least one verified broker lot.
- Static spread ceiling is enforced.
- Dynamic spread-to-option-premium-1m-ATR test is enforced; spread cannot consume an excessive fraction of observed premium movement.
- Execution-time quote and F&O available-funds check are repeated before entry.

### Data and time-frame correctness

- Directional structure remains based on NIFTY underlying data; option premium is used only after vehicle activation for execution and risk geometry.
- Both selected CE and PE vehicles are prewarmed with option-premium minute candles before the book becomes `READY`.
- Breeze historical fallback interval translation is correct: v1 `minute/5minute/30minute/day` becomes v2 `1minute/5minute/30minute/1day`.
- Breeze timestamps without timezone are interpreted as exchange-local IST, preventing future-candle/sweep-age corruption.

## Wiring and lifecycle safeguards

- A rejected/unfilled option entry releases the activated contract immediately, preventing future NIFTY scanning from reading option-premium prices as underlying prices.
- After confirmed flat, the selected CE/PE is cleared, so a later opposite-direction entry activates the opposite vehicle.
- Polling uses a generation token. A retired CE polling thread cannot resume after a rapid switch to PE, or vice versa.
- Contract-book publication is transactional: a failed refresh does not overwrite the last verified valid book.
- Security Master live sources use HTTPS only.
- ICICI remains a long-premium strategy: one live broker-side protective STOPLOSS is retained; no simultaneous unverified TP SELL order set can accidentally create a short option after SL execution.
- INR P&L remains isolated from Delta USD P&L.

## Files changed for session-book implementation

- `agents/icici_chain_architect.py`
- `exchanges/icici/api.py`
- `exchanges/icici/data_manager.py`
- `aggregator/market_aggregator.py`
- `orchestration/multi_asset_bot.py`
- `strategy/quant_strategy.py`
- `execution/instrument_registry.py`
- `config.py`
- `tests/test_icici_options.py`

The package also contains all prior verified DOL-first, institutional SL and ICICI F&O data-integrity fixes on which this change depends.

## Validation executed

```bash
python -m compileall -q .
DELTA_API_KEY=dummy DELTA_SECRET_KEY=dummy python -m pytest -q
DELTA_API_KEY=dummy DELTA_SECRET_KEY=dummy python -m pytest -q
```

Result:

```text
171 passed
171 passed
```

Coverage includes:

- session-start CE and PE selection;
- live thesis-to-direction activation;
- minute-premium prewarm for both vehicles;
- second-entry CE→PE release/reactivation;
- delta-drift urgent refresh and anti-thrash cooldown;
- failed transactional refresh preserving the previous valid book;
- official Breeze bid/offer/depth field use without fabricated liquidity;
- spread-to-premium-ATR rejection;
- execution-time affordability rejection;
- Security Master lot-size precedence;
- HTTPS-only live Security Master data source;
- rapid CE→PE polling-thread generation invalidation;
- ICICI flat/position, SL/TP and currency-separation coverage from prior audits.

Static call-wiring audit on `QuantStrategy` also passed:

```text
undefined_direct_private_calls=[]
```

## Live validation requirement

Automated tests prove the amended paths and calculations against controlled broker responses. A new market-session runtime log is still required to field-confirm live Breeze payload behaviour and live execution routing. Expected new logs include:

```text
ICICI SESSION CONTRACT BOOK READY ... CE=... PE=...
ICICI SESSION VEHICLE ACTIVATED ... thesis=long|short ...
ICICI execution vehicle released after confirmed FLAT ...
```

The bot must not emit a ghost ICICI position without exact NFO contract fields, and it must not rescan the option chain at the moment of an otherwise qualified trade unless an execution vehicle has become invalid and requires refresh.

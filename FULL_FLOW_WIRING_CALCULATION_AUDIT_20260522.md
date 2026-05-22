# Full Flow Wiring & Calculation Audit — v508

## Scope audited

Runtime chain reviewed from broker/session preflight through data warmup, thesis generation, contract selection, risk sizing, order routing, protective exit management, reconciliation, P&L and portfolio reporting, with special focus on the ICICI NFO options desk.

Runtime evidence reviewed: `quant-20260522-041317.log`.

## Defects found and corrected

### 1. Exact NFO position identity: safe acceptance without false flat state

The prior strict patch rejected malformed ghost rows correctly, but it could also reject a real Breeze `PortfolioPositions` option row when the response omitted `segment` while still providing exact `exchange_code=NFO`, `product_type=Options`, underlying, right, strike and expiry.

Correction: `segment` is now validated when present; it is not mandatory when exact NFO option identity is otherwise proven. Explicit non-F&O segment values remain rejected. Blank/partial rows remain FLAT and cannot be adopted.

### 2. No invented NFO lot size

The code previously fell back to lot size `1` when no lot size was available from contract/security-master data. That can misprice affordability and route an incorrect order quantity.

Correction: default NFO lot size is now zero/fail-closed. Contract selection, risk sizing, recovered-position management and order routing all refuse automation until an exact positive lot size is supplied.

### 3. Breeze historical fallback interval mismatch

The ICICI v1 historical endpoint uses `minute`, `5minute`, `30minute`, `day`, while v2 requires `1minute`, `5minute`, `30minute`, `1day`. The v2 fallback was passed v1 interval labels.

Correction: option and underlying data managers now convert v1 intervals to v2 vocabulary before fallback calls.

### 4. Missing active-exit implementation

`QuantStrategy._manage_active()` called `_exit_trade()` on regime-flip, flow-reversal and profit-defence exits, but no `_exit_trade()` method existed. A live position meeting those conditions would raise `AttributeError` instead of exiting safely.

Correction: added guarded strategy-exit flow that cancels existing exits, submits the close only after cancellation is verified, tracks manual exit order id for confirmed P&L, and restores SL when close submission fails.

### 5. ICICI parallel SELL exit exposure

For long-premium NFO positions, the previous path could arm a protective SL, final TP and internal TP sell orders concurrently. In the absence of a verified broker-side OCO/reduce-only guarantee, an SL fill followed by a residual TP sell can create unintended short option exposure.

Correction: ICICI now enforces a single-live-broker-exit invariant. The protective STOPLOSS remains broker-resident. TP1/TP2/TP3/final levels are still calculated for the thesis, but are not submitted as parallel broker SELL orders while the SL is live. On a TP reach, the strategy first cancels the SL and then routes sell-to-close. Delta TP ladders remain unchanged.

### 6. ICICI active management below ₹1

The active management loop ignored prices below `1.0`, valid for ICICI option premiums close to expiry.

Correction: ICICI active management now uses its configured tick size as the minimum valid premium rather than a hard `1.0` threshold.

### 7. Mixed-currency P&L arithmetic

Portfolio reporting previously added INR NIFTY option P&L to USD Delta P&L and displayed the result with a dollar sign.

Correction: trade records carry `INR/₹` or `USD/$`; portfolio reporting aggregates and displays separate currency ledgers. No conversion is invented and no cross-currency monetary total is shown.

### 8. Dead competing Indian-options model removed

`strategy/indian_index_barrier_model.py` had no import or call path and represented unused competing logic. It has been removed to avoid future accidental divergence from the active option desk path.

## Regression verification

New regression cases cover:

- malformed ICICI quantity rows remain FLAT;
- exact NFO option rows without `segment` remain detectable;
- contracts with no verified lot size cannot be selected or routed;
- v2 historical fallback sends `1minute`/`1day` interval values;
- active strategy exits cancel protection before a full close and track the exit order;
- ICICI does not place parallel broker TP SELL orders while its SL is live;
- USD and INR P&L appear as separate ledgers.

Validation commands:

```bash
python -m compileall -q .
DELTA_API_KEY=dummy DELTA_SECRET_KEY=dummy python -m pytest -q
DELTA_API_KEY=dummy DELTA_SECRET_KEY=dummy python -m pytest -q
```

Expected result for this package:

```text
154 passed
154 passed
```

## Live-runtime limitation

The supplied runtime log predates these changes. It proves the previous ghost-position event, not post-fix broker behaviour. A new run must show the ICICI F&O preflight, confirmed FLAT/exact option position output, contract lot source, single-live-exit message for any actual option entry, and INR-separated P&L reporting before the live ICICI flow can be considered field-confirmed.

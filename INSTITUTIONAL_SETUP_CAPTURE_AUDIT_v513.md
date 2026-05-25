# Institutional Setup Capture Audit — v513

## Audit scope

This audit traces the missed-entry path visible in `quant-20260522-175808.log` against the supplied `v512 - Pure ICT and liquidity` source. It targets the active Delta BTC/GOLD/SILVER structural decision chain:

`market data → closed-candle lineage → liquidity pools/sweeps → HTF context → 5m raid → MSS/displacement → FVG rebalance → geometry/RR/risk gate → native bracket execution`.

It does **not** assert profitability or certify live execution on venues not exercised by this log.

## Observed outcome in the live log

- BTC recognised a valid candidate short raid at **22:05:45**: a 5m BSL sweep near **76,923.5**, wick **76,942.0**, while both 4H and 15m context were bearish (`STRICT_4H_15M_DOL`, direction `short`).
- The trade did not reach execution; it stopped at `AWAITING_5M_MSS_DISPLACEMENT` with an MSS reference of **76,612.5**.
- The log contains **zero** `ICT_ORDER_THESIS`, filled-entry, bracket-entry-failure or native-bracket events. Therefore sizing, risk gate and Delta bracket placement were not the cause of the BTC no-trade: no executable structural signal was produced upstream.
- GOLD reached both `AWAITING_5M_DISPLACEMENT_FVG` and later `AWAITING_FVG_REBALANCE`: the engine did see partial setups, but execution required a valid gap/reprice.
- SILVER repeatedly reached MSS/FVG waiting states, with material spread impairment during several observations.
- Two Delta websocket disconnects occurred later; each reconnected and resubscribed within seconds, and subsequent `DATA_LINEAGE integrity=PASS` confirms these were not the causal missed-entry fault.

## Root causes fixed

### 1. Sweep-context lifetime contradicted the entry model

`LiquidityMap.update()` evicted **all** recent sweeps after 300 seconds. The entry engine itself allowed a 5m raid for 600 seconds and declared much longer parent HTF horizons. A close-confirmed `raid → MSS → FVG → reprice` sequence cannot reliably complete inside one 5m retention bar.

**Fix:** `strategy/liquidity_map.py` now holds sweep events by timeframe:

- 5m structural raid: 1,200 seconds / four 5m bars.
- 15m through 4h parent context: timeframe-aware confirmation windows.
- 1d context: one day.

`strategy/entry_engine.py` consumes the same shared horizon definitions so the map and decision engine cannot silently disagree.

### 2. MSS reference was structurally wrong

For a short raid, v512 required the close to break the **lowest low anywhere in the preceding 12 candles**; for a long raid it required the highest high. This is an arbitrary range-extreme break, not a market-structure shift over the latest protected internal swing. In the BTC event, it forced the short confirmation down to 76,612.5 despite the sweep occurring around 76,923.5.

**Fix:** `strategy/entry_engine.py` now derives MSS from the **latest confirmed pre-raid internal swing**. If no confirmed protected internal swing exists, it records `NO_CONFIRMED_PRE_RAID_INTERNAL_SWING` rather than inventing a permissive or overly remote trigger. Logs now expose `mss_source` and `mss_age_bars`.

### 3. Candle/ATR information sets could diverge

The strategy ATR calculation and timeframe-native ATR calculation blindly removed the last candle as “forming.” Some live streams already present a closed tail; in that case the ATR lagged by an entire bar while sweep and price handling used newer evidence. Also, pool construction could include a forming candle while sweep checks were close-confirmed.

**Fix:**

- `strategy/quant_strategy.py`: 5m ATR now uses timestamp-resolved latest closed-candle selection.
- `strategy/liquidity_map.py`: timeframe-native ATR uses the same resolution logic.
- `strategy/liquidity_map.py`: new pool geometry is constructed from closed candles only.

This keeps pool levels, sweep evidence, MSS/FVG geometry and ATR normalisation on one information set.

## What was not changed blindly

The supplied engine is intentionally a **single entry authority**:

`4H/15m draw-on-liquidity context → 5m external raid → MSS/displacement → FVG repricing → structural SL/HTF liquidity TP`.

A visible continuation, order-block mitigation, breaker/retest or displacement-only setup without a qualifying 5m raid cannot trade under that architecture. I did not force these additional archetypes into live execution without labelled replay tests, because that would add unvalidated alpha paths rather than repair a miscalculation.

## Execution/risk review for this incident

- Delta BTC/GOLD/SILVER were correctly discovered and received live/warmup candles.
- Decision logs repeatedly state `DATA_LINEAGE integrity=PASS`.
- Open-slot capacity remained available (`open=0/6`).
- The code enforces native bracket execution for the active Delta path and refuses a naked fallback when Delta native bracket placement fails.
- No bracket or risk rejection occurred in this log because no setup passed the structural entry engine.

### Residual venue-control issue

The code retains a non-Delta path that may place a normal limit entry followed by standalone protection where a venue lacks native bracket support. This did not affect the observed incident because active traded assets were Delta and NIFTY was dormant/market-closed. It must be treated as a live-enablement blocker for any CoinSwitch or other non-native-bracket venue unless an exchange-attached protection method is implemented or that venue is restricted from live entries.

## Modified files

| File | Change |
| --- | --- |
| `strategy/liquidity_map.py` | Timeframe-aware sweep retention; closed-only pool construction; timestamp-resolved native ATR. |
| `strategy/entry_engine.py` | Shared sweep lifetime; protected-internal-swing MSS; explicit structural diagnostics. |
| `strategy/quant_strategy.py` | Timestamp-resolved ATR; MSS provenance in logs. |
| `tests/test_ict_liquidity_architecture.py` | Existing tests aligned to corrected raid lifetime and confirmed MSS definition. |
| `tests/test_institutional_setup_capture_v513.py` | New regression suite for the repaired defects. |

## Validation performed

- Original v512 baseline before modification: **124 tests passed**.
- Patched v513: **131 tests passed**.
- New regression coverage verifies:
  - a 5m raid remains available beyond one bar for closed confirmation;
  - HTF parent sweep context is retained for its declared horizon;
  - MSS selects the latest confirmed internal swing rather than an arbitrary window extreme;
  - forming candles cannot create pool geometry;
  - strategy ATR and native timeframe ATR include the latest bar when it is already closed.

## Live-use interpretation

This build removes the demonstrated upstream reasons why a supported institutional setup can be missed. It does not prove that the particular historical BTC short would have generated a filled order, because the supplied log contains decisions, not a full candle/orderbook replay sufficient to recompute the corrected MSS/FVG/reprice path. Run this build first in paper/live-observation mode and inspect the new `MSS source=LATEST_CONFIRMED_INTERNAL_SWING age=…b` and `ICT_ORDER_THESIS` records before enabling live ordering.

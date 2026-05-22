# v509 — Institutional ICT + Liquidity Rebuild

## Executable strategy authority

The prior composite entry stack has been removed from executable routing. Every activated desk now uses one structural entry authority:

`4H delivery context → 15m structural confirmation → fresh 5m external liquidity raid → 5m displacement and MSS → 5m FVG repricing → structural stop → opposing 15m-or-higher liquidity target`.

Removed strategy modules include direction/conviction/post-exit/adaptive-learning, prior ICT bridge, cross-asset alpha, target-selector overlays, market-intelligence and legacy display modules.

## Calculation controls

- 4H and 15m context are normalised using their own closed-bar ATR, never 5m execution ATR.
- Only fresh 5m external raids may trigger entry; older higher-timeframe events remain context, not triggers.
- Structural stops sit behind the raided wick plus volatility clearance based on the 5m ATR percentile.
- Targets must be opposing 15m-or-higher liquidity, or explicitly HTF-promoted liquidity, and pass positive structural delivery utility.
- NIFTY analysis remains in underlying-index units; ICICI orders, premium SL/TP conversion and realised P&L remain in option-premium INR units.
- Delta BTC retains inverse-contract accounting; Delta metals and CoinSwitch retain linear accounting; ICICI options retain INR premium accounting.
- Realised P&L remains broker-fill reconciled; disappearance of a position does not create estimated realised performance.

## Execution and operator surfaces

- Bracket/protective-order requirements, lot rules, margin limits, fee feasibility and exact-fill reconciliation remain execution controls, not entry alpha.
- Telegram outputs expose ICT/Liquidity geometry, protected execution, broker funds and broker-reconciled P&L only.
- Removed Telegram legacy commands/data surfaces: order-flow `/flow` and adaptive-learning `/learn`.
- Operator outputs use instrument currency (`₹` for ICICI, `$` for USD-settled desks) or unit-neutral structural levels where no account currency applies.

## Validation

The active architecture regression suite contains 68 passing tests, including full 4H→15m→5m structural signal formation, timeframe-native ATR invariance, structural stop/target constraints, NIFTY option routing, multi-desk P&L/accounting and exact exit reconciliation.

This validates code paths and calculation invariants; it does not substitute for a broker-live filled-entry/filled-exit certification cycle on each enabled venue.

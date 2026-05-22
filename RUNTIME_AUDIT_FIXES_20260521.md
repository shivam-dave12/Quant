# Runtime audit fixes — 2026-05-21

Scope: audit of `quant-20260521-141607.log` after the institutional SL patch.

## Findings

1. **SL raid-zone model is active**
   - Runtime shows `RAW_SL_ANCHOR`, `cluster_n`, `cluster_edge`, `cluster_width`, and `raid-zone shield` diagnostics.
   - Runtime also shows `SL raid-zone shield` and `SL envelope` adjustments before execution.
   - Later candidates were deferred when SL would sit inside a liquidity raid zone or would breach liquidation room.

2. **Gold SL hit was an adopted existing exchange position**
   - The runtime adopted an already-open Delta GOLD position and kept the existing fixed SL.
   - This is not a clean sample of a newly selected SL from the patched SL engine.
   - Internal TP cleanup after the SL hit worked correctly.

3. **Silver bracket order did not fill within the 60s maker timeout**
   - The bracket order was placed on Delta, but the entry limit did not fill within the configured timeout.
   - The system cancelled the order and refused a non-bracket fallback, so no unprotected position was opened.
   - This was safe behavior, but the previous log labeled the event as a native bracket failure, which was misleading.

4. **Trade-record log used BTC as a hardcoded quantity unit**
   - GOLD trade accounting showed `Qty: ... BTC`; this was a reporting bug only.

## Code changes in this package

1. `execution/order_manager.py`
   - Added structured `last_order_error` for `delta_native_bracket_fill_timeout`.
   - Logs a safe unfilled-entry cancellation instead of making it look like a schema/API failure.

2. `strategy/quant_strategy.py`
   - Distinguishes true Delta native bracket failure from a bracket entry that reached the exchange but timed out unfilled.
   - Keeps the no-fallback protection invariant intact.

3. `risk/risk_manager.py`
   - Removed hardcoded `BTC` quantity unit from trade-record logs.
   - Uses the active portfolio instrument asset ID when available; falls back to `contracts`.

## Validation

```text
132 passed
```

No SL/TP selection model, entry criteria, sizing formula, or bracket-safety policy was loosened.

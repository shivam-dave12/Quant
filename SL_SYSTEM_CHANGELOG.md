# Institutional SL System Rebuild — v508 Patch

## Problem fixed
The previous SL flow could place the stop at, or just behind, a visible SSL/BSL pool. That is still technically protective, but institutionally wrong: the exchange stop becomes part of the liquidity pocket that price is likely to raid. This is why repeated trades were getting stopped before delivery.

## Files changed

### `strategy/liquidity_pool_selector.py`
- Rebuilt SL pool selection from **single-pool anchoring** to **same-side stop-cluster anchoring**.
- For longs, the selector now treats SSL below price as a raid zone and places SL beyond the **outer/lower edge** of the SSL cluster.
- For shorts, the selector treats BSL above price as a raid zone and places SL beyond the **outer/upper edge** of the BSL cluster.
- Changed the old inverse buffer model:
  - Old: high-quality pool → smaller buffer.
  - New: high-quality pool → larger raid clearance, because high-quality liquidity is exactly where stops concentrate.
- Added cluster-aware diagnostics: cluster count, cluster edge, cluster width, quality, buffer, and `raid-zone shield` reason.
- Kept existing TP logic untouched.

### `strategy/entry_engine.py`
- Added a final **raid-zone shield** inside `_apply_institutional_sl_envelope()`.
- The engine now validates final SL against nearby same-side liquidity before emitting a trade.
- If the proper institutional SL must be farther away:
  - it widens to the raid-boundary SL only if liquidation guard and payoff geometry still pass;
  - otherwise it abstains/refines instead of taking a trade with an SL sitting inside liquidity.
- Upgraded `_push_sl_behind_pools()` so any OB/fallback stop mutation also clears visible SSL/BSL with dynamic quality/regime-aware buffers.
- Preserved bracket-order policy and fixed-SL TP-ladder model.

### `tests/test_all.py`
- Added regression coverage proving:
  - SL is placed beyond the outer edge of a liquidity cluster, not at the nearest pool.
  - The engine abstains when the only valid raid-boundary SL would destroy payoff geometry instead of falling back to a tight liquidity stop.

## What did not change
- No entry signal criteria were tightened.
- No TP model was weakened.
- No bracket-order fallback was introduced.
- No risk/sizing parameters were changed.
- No exchange execution flow was changed.

## Validation
Executed:

```bash
DELTA_API_KEY=test DELTA_SECRET_KEY=test TELEGRAM_BOT_TOKEN= TELEGRAM_CHAT_ID= pytest -q
```

Result:

```text
130 passed
```

## Operational impact
Expect fewer low-quality trades where the SL is too close to the liquidity pool. Some trades that previously entered will now be deferred/refined when the true institutional SL is too far for liquidation/payoff geometry. That is intentional: the bot should not trade when the only executable stop is the liquidity itself.

## Verification hardening after re-check
- Fixed cluster-width accounting: width now measures the full same-side liquidity pocket (`max(pool_prices)-min(pool_prices)`), not distance from the chosen anchor to itself. This matters when the best anchor is already the outer-edge pool.
- Upgraded clustering to a contiguous/chain model so a ladder of nearby SSL/BSL pools is treated as one raid zone instead of only one-hop pools around the selected anchor.
- Aligned the selector-side maximum raid clearance with the final entry-engine raid shield so high-quality multi-timeframe clusters receive enough clearance before bracket placement.

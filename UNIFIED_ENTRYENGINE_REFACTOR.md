# Unified EntryEngine Refactor

## Intent
Preserve the liquidity-hunt core strategy while removing redundant route-blocking layers after EntryEngine has already produced an executable candidate.

## What changed
- EntryEngine remains the single alpha owner for sweep, direction, entry, liquidity-aware SL, liquidity TP, and R:R.
- QuantStrategy no longer hard-blocks EntryEngine candidates through:
  - entry confirmation tick gate,
  - institutional decision matrix hard veto,
  - duplicate unified entry gate,
  - TargetSurface positive-utility veto.
- QuantStrategy now performs only:
  - mechanical order geometry safety,
  - liquidation sanity,
  - account/risk-manager availability,
  - dynamic size multiplier/advisory context.
- TargetSurface remains advisory only. Weak target-surface utility reduces size but does not kill an EntryEngine-approved liquidity setup.

## Files changed
- strategy/quant_strategy.py
- tests/test_v76_unified_entryengine_route.py

## Validation
- PYTHONPATH=. pytest -q
- Result: 137 passed

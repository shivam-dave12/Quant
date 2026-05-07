# v75 Rollback Clean Package

This is a rollback package based on the v75 code line. It intentionally removes the v76-v81 experimental strategy layers from the runtime package and restores the simpler v75 flow.

Kept:
- v75 margin allocation model
- native-bracket / no-naked-fallback execution architecture from the v75 line
- v75-era entry/TP/SL architecture without the added v77-v81 staged/shelf/preflight filter stack

Minimal non-strategy repair added:
- portfolio aggregate risk fallback when `portfolio_remaining_risk_cap` is missing from the risk-manager balance dict. This is a sizing/accounting safety repair, not a strategy filter.

Cleaned:
- `__pycache__`
- `*.pyc`
- `.pytest_cache`
- later audit-report clutter

Note: the runtime fingerprint inside the original v75 code line may still display the v74 execution-truth label because that was the embedded fingerprint in the archived v75 package. This package identity is v75 rollback clean.

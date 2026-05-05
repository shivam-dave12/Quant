# v15 institutional hardening build

Trailing is **enabled by default** in this package.

Changes from v14:
- Asset-aware xStock spread gate no longer hard-blocks only because tick count is high.
- Bracket entry refusal / entry failure / non-bracket Delta response now sends critical Telegram operator alerts.
- Delta REST candle gap logging is rate-limited and keeps last good buffers.
- Delta websocket transient disconnects are warning-level until repeated failure threshold.
- Tests include v15 spread/alert hardening checks.

## v16 hotfix
- Fixed Delta WebSocket `_on_error` indentation regression that raised `NameError: name '_msg' is not defined` and prevented every Delta data manager from starting.
- Added `tests/test_v16_websocket_runtime.py` to simulate WebSocket error/open callbacks without network and catch this runtime class of bug.

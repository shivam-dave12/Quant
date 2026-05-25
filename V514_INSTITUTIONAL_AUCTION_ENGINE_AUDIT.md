# v514 — Institutional Auction Engine: Core Strategy Rebuild Audit

**Source baseline:** `v513 - Institutional Setup Capture`  
**Target build:** `v514 - Institutional Auction Engine`  
**Validation result:** `140 passed` (`DELTA_API_KEY=test DELTA_SECRET_KEY=test python -m pytest -q`)

## Executive verdict

v513 repaired two missed-signal defects in a single ICT sequence. It was not an institutional core strategy because it could only trade a fresh-raid/MSS/FVG reversal path, evaluated primarily on polling cadence, and it used a deterministic setup score as if it were delivery probability in target/risk logic.

v514 replaces that bottleneck with a **single execution authority over multiple observable structural auction archetypes**. Risk remains structural and venue constrained; a model score is explicitly an evidence score, not a win probability. No uncalibrated score can scale live allocation or be displayed as expected utility.

No live-trading implementation can truthfully guarantee zero error, zero exchange/network latency or capture of every discretionary setup. v514 makes the enforceable parts deterministic and testable: setup definitions, target ranking, cost controls, venue protection, event-driven wake-ups and latency telemetry.

## Root strategy deficiencies found

1. **Single-archetype blind spot:** A trade could be entered only after `4H/15m DOL → fresh 5m raid → MSS/displacement → FVG reprice`. Continuation displacement or a consumed-liquidity expansion retest was structurally invisible.
2. **Poll-led candidate handling:** Market data feeds referenced strategy callbacks, but the decision authority lacked a real-time event-wake path for active candidates; a valid reprice opportunity could wait for the next scanner cadence.
3. **Uncalibrated pseudo-probability:** The existing target/utility route treated deterministic evidence as delivery probability. This is not statistically valid and can distort risk and target selection.
4. **Nearest-target bias:** Structural targets could be preferred by proximity before the best measured net-R destination.
5. **Final integration defect:** Once probability use was correctly disabled for uncalibrated signals, the fee engine still assumed a numeric probability. That path has been corrected to be probability-neutral when no calibrated model exists.

## Institutional auction architecture delivered

### 1. Unified order authority with three auditable archetypes

The single authority now permits only the following measurable setup families:

| Archetype | Structural meaning | Required confirmation |
|---|---|---|
| `LIQUIDITY_RAID_REVERSAL` | External liquidity is swept and rejected | Raid ownership, protected-swing structure, displacement/FVG and executable reprice |
| `DISPLACEMENT_CONTINUATION` | Delivery expands without manufacturing a raid narrative | Robust displacement, internal protected-swing break, imbalance/reprice and positive net-R target |
| `LIQUIDITY_EXPANSION_RETEST` | Liquidity is consumed and held through rather than rejected | Consumed-pool attribution, expansion structure, retrace execution and structural invalidation |

A contemporary explicit raid owns its own attribution; a generic continuation candidate cannot override it. This avoids classifying a stop-run reversal as continuation simply because displacement is present.

### 2. Observable auction evidence, not fabricated probability

New `strategy/auction_state.py` builds execution evidence from:

- higher-timeframe structural destination pull;
- live order-book depth imbalance and microprice displacement from mid;
- exponentially decayed signed aggressive trade flow;
- robust displacement thresholds derived from candle bodies using median/MAD rather than a fixed retail-style constant.

The output is `delivery_score`, used to rank structural opportunities. It is explicitly not `delivery_probability`. Probability and expected utility remain unavailable unless a future replay/out-of-sample calibration model explicitly provides them.

### 3. Structural target selection based on positive net R

The target chooser now ranks eligible opposing-liquidity destinations using structural evidence and **measured net-win R after execution cost**. It rejects a target when net reward is not positive. It no longer forces the nearest pool simply because it is closest.

### 4. Event-woken candidate monitoring

Polling remains as the resilience/recovery route, but active structural candidates can now wake evaluation immediately on market events:

- **Delta:** order-book and trade events notify the strategy outside feed locks.
- **CoinSwitch:** order-book and trade events notify the strategy outside feed locks.
- **ICICI underlying analysis:** live candle events notify the strategy outside feed locks for index-option thesis analysis.
- **Orchestrator:** an event is used to wake the scanner; urgent active candidates use an event-driven evaluation path with `eventDelayMs` telemetry.

External strategy evaluation is not performed inside a websocket data lock. This prevents a low-latency improvement from introducing feed-thread lock contention or deadlock risk.

### 5. Probability-neutral execution and operator reporting

- Fee floors and maker/taker routing accept `delivery_probability=None` safely.
- Allocation is driven by structural SL distance, available margin, venue lot rules, measured cost/spread impairment, leverage/liquidation safety and configured desk policy.
- A calibrated probability may reduce risk only when explicitly present; it is not assumed.
- Telegram, status reports and trade history show `delivery_score` and `calibratedP=N/A` unless calibration exists.

## Primary files changed

| Area | Files | Change |
|---|---|---|
| Auction evidence | `strategy/auction_state.py` | New microstructure/delivery evidence and robust displacement primitives |
| Entry authority | `strategy/entry_engine.py` | Three-archetype opportunity competition, structural attribution and net-R liquidity target ranking |
| Runtime/execution | `strategy/quant_strategy.py`, `strategy/fee_engine.py` | Event-driven evaluation, latency telemetry, calibrated-only probability controls, probability-neutral cost path |
| Market events | `exchanges/delta/data_manager.py`, `exchanges/coinswitch/data_manager.py`, `exchanges/icici/underlying_data_manager.py` | Safe callback wake-ups outside data locks |
| Orchestration | `orchestration/multi_asset_bot.py` | Event wake-loop and institutional auction reporting |
| Operator surfaces | `telegram/controller.py`, `telegram/notifier.py`, `main.py` | Removes fake probability/utility presentation and reports archetype/evidence/latency |
| Policy/config | `core/market_policy.py`, `config.py` | `INSTITUTIONAL_AUCTION_V514` strategy identity |
| Certification | `tests/test_institutional_auction_engine_v514.py` and revised architecture/integrity tests | Regression coverage for evidence, archetypes, event paths and calibration boundary |

## Certification performed

| Test layer | Result |
|---|---:|
| v513 supplied baseline | 131 passed |
| v514 full regression suite | **140 passed** |
| New certification areas | Microprice/aggressive flow evidence, continuation without fabricated raid, expansion retest attribution, uncalibrated net-R execution, probability-neutral fee floor, websocket event wake-up and multi-venue callback coverage |

## What is not claimed or certified

- No claim of zero latency across internet, broker/exchange processing or websocket transport.
- No claim of zero defects under unseen live-market states.
- No claim that three codified archetypes equal every discretionary institutional observation.
- No claim of win rate or profitability without tick/orderbook replay, out-of-sample validation and live shadow results.
- No statistical delivery probability until properly labelled, replay-calibrated and out-of-sample tested.

## Deployment gate

Run v514 first with live ordering disabled and capture:

- `AUCTION_DECISION` and `INSTITUTIONAL_ORDER_THESIS` transitions;
- archetype attribution per candidate;
- `deliveryScore` with `calibratedP=N/A`;
- `eventDelayMs` distribution by venue/asset;
- rejected-target reasons and measured cost impairment;
- bracket-protection and exact-fill reconciliation paths.

Only after replay/shadow evidence demonstrates clean attribution, acceptable event-to-evaluation latency and venue-protected order behaviour should live order enabling be considered.

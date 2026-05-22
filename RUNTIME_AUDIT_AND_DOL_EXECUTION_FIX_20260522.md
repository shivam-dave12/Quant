# Runtime Audit and DOL Execution Fix — 2026-05-22

## Runtime evidence reviewed

Source log: `quant-20260522-031843.log` (observed log timestamp window 05:38:22–08:48:41).

Observed in that run:

- 10,497 log lines.
- 0 `POSTERIOR ACCEPTED`, 0 executable candidates, 0 bracket orders, 0 TP ladders, 0 SL/exit lifecycle events.
- 35 GOLD quantitative rejections:
  - 29 rejected as `cap=0.76/uncalibrated_bucket`.
  - 6 rejected by the null-auction information floor.
- 7 Delta WebSocket disconnect events, each followed by a successful reconnect in the observed log.
- No Python traceback or unhandled application exception.

## Defects found

### 1. DOL-first was not wired first

`entry_engine.py` called `evaluate_post_sweep_quant()` before `DOL_PRECHECK`.
Because the posterior rejected every candidate in the runtime log, the DOL engine was never reached. The run therefore did not validate DOL, executable TP/SL geometry, order creation, or exit lifecycle.

### 2. Cold-start posterior was mathematically locked out

The uncalibrated setup bucket capped posterior confidence at 0.76 while the SPRT likelihood barrier required a higher probability. Even a clean destination/structure test could not become executable.

### 3. DOL import failure was fail-open

If the DOL module failed to import, the strategy returned to a legacy candidate path. A live institutional strategy must not trade when its thesis engine is missing.

## Changes applied

### True DOL-first sequencing

The reversal and continuation paths now execute in this order:

1. `DOL_CONTEXT`: require a real, directionally useful target-side liquidity draw.
2. Quantitative delivery posterior conditioned on that DOL context.
3. Dynamic trade-quality confirmation.
4. `DOL_CONFIRMATION`: confirm delivery evidence still supports the destination.
5. Structural SL construction and institutional raid-zone envelope.
6. TP selection and payoff geometry validation.
7. `EXECUTABLE_THESIS`: re-check DOL with actual SL and TP before signal creation.
8. Refined/second-entry path repeats executable DOL validation before creating a signal.

### Direction-conditioned quantitative model

`evaluate_post_sweep_quant()` now receives `dol_context` and models:

- draw clarity;
- target quality;
- first-target delivery probability;
- target distance/reachability;
- competing protective-side liquidity pressure.

These influence destination-conditioned evidence and expectancy, rather than allowing a direction signal to invent its TP afterwards.

### Coherent cold-start admission

For a setup bucket without sufficient closed outcomes:

- the model does **not** treat the result as calibrated win probability;
- it may enter only as `PROVISIONAL_DOL` when DOL context, delivery structure, auction information and positive expectancy align;
- DOL alone cannot authorize an entry;
- structure-void setups remain rejected.

Once outcome history exists, the calibrated SPRT pathway remains active.

### Fail-closed safety

If the DOL engine is unavailable, candidate generation is refused rather than falling back to a legacy live-trading path.

## Verification

Executed twice after patching:

```bash
python -m compileall -q .
DELTA_API_KEY=dummy DELTA_SECRET_KEY=dummy python -m pytest -q
```

Result on both runs:

```text
141 passed
```

Added regressions verify:

- DOL context is evaluated before posterior calculation.
- A clean DOL context can be identified without a posterior score.
- A strong aligned cold-start thesis can enter only in `PROVISIONAL_DOL` mode.
- DOL context without delivery structure cannot enter.
- Reversal, continuation, and refined/second-entry paths revalidate executable DOL after SL/TP are known.
- The DOL engine fails closed if unavailable.

## What still requires live validation

The supplied runtime log contains no order or position lifecycle after the DOL-first upgrade. It therefore cannot prove live fill, bracket attachment, TP ladder fills, SL hit handling, or second-entry execution. Those paths are covered by regression tests, but the next live/paper runtime log must show them before any claim of live profitability or stable win rate is justified.

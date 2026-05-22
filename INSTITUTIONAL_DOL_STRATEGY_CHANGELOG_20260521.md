# Institutional DOL-First Strategy Upgrade — 2026-05-21

## Why this change was made
The live logs showed the bot could accept post-sweep candidates with high posterior confidence even when structural confirmation and HTF alignment were near zero. The previous flow was effectively:

```text
sweep/posterior signal -> build SL -> try to find TP -> defer if geometry fails
```

That is not high-win-rate behaviour. A higher-win-rate liquidity strategy must be:

```text
clean draw-on-liquidity -> market delivery proof -> executable SL/TP geometry -> order candidate
```

## Core changes

### 1. New DOL-first thesis engine
Added `strategy/dol_engine.py`.

The engine scores every proposed trade before it becomes executable using:

- live TP-side BSL/SSL availability;
- first-target distance and noise/cost viability;
- liquidity clarity versus opposing/protective liquidity pressure;
- 15m/4h structure alignment;
- CHoCH/BOS playbook support;
- order-flow and CVD alignment;
- premium/discount location;
- posterior and quality score as inputs, not sole truth;
- final SL/TP payoff expectancy once structural SL is known.

It returns an explicit DOL grade and rejects candidates with no clean liquidity destination.

### 2. DOL pre-check before posterior acceptance becomes actionable
`entry_engine.py` now calls the DOL thesis gate after quantitative posterior acceptance and before logging/returning actionable post-sweep decisions.

This prevents the old pattern where the bot said `POSTERIOR ACCEPTED` and only later discovered `no eligible TP pool`.

### 3. Real quality gate instead of diagnostic-only score
`_institutional_entry_quality_gate()` now has an adaptive A/B/C/D grade floor.

It no longer always returns `True`. Weak delivery, chase distance, contra flow/CVD, poor premium/discount location, and HTF conflict now materially prevent live signals when the setup is not A/B-grade.

The floor adapts by playbook:

- reversal after confirmed sweep: slightly more tolerant if CISD/OTE or strong displacement exists;
- continuation after sweep: stricter because continuation after a raid is commonly a trap;
- early phase without CISD/OTE: stricter;
- weak flow continuation: stricter.

### 4. Quant posterior structural caps
`quantitative_models.py` now caps/rejects score-only confidence when:

- `structural` proof is missing;
- HTF alignment is effectively zero;
- continuation lacks acceptance structure;
- the setup bucket has no enough closed-trade history;
- the bucket has negative realised outcome history.

This directly addresses the observed failure mode: high `p=0.85+` while `struct=0.00` and `htf=0.00`.

### 5. Executable thesis validation after final SL/TP selection
Even after SL and TP are selected, `entry_engine.py` runs the DOL engine again with the final SL/TP geometry.

This blocks trades where:

- the final TP is not aligned with the active liquidity draw;
- first-target probability is too weak;
- structural SL makes payoff expectancy poor;
- TP is too close/noisy after spread/fee/auction noise;
- there is too much protective-side liquidity pressure.

### 6. Refined second-entry path protected
The refined/pullback re-entry path now also validates DOL thesis after repricing SL/TP. A retry no longer becomes valid just because the entry price improved; the liquidity draw must still be alive.

### 7. Regression tests added
Added tests covering:

- DOL rejection when no TP-side liquidity exists;
- DOL acceptance for clean reversal thesis;
- posterior rejection for structure-void score-only continuation;
- existing 132 package regressions still pass.

## Validation

```text
python -m compileall -q .
DELTA_API_KEY=dummy DELTA_SECRET_KEY=dummy python -m pytest -q
DELTA_API_KEY=dummy DELTA_SECRET_KEY=dummy python -m pytest -q

135 passed
135 passed
```

## Important behaviour change
The bot will now take fewer trades when the market does not offer a clean destination. This is intentional. The aim is not more entries; the aim is higher-quality entries where TP1 has a stronger probability of getting paid before the market can raid the structural SL.

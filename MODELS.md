# Option-only model suite — real Groww data only

This project separates **models** from **strategy**. The model suite predicts and ranks NIFTY option contracts. Strategy thresholds, exits, SL/TP, sizing, and risk policy sit on top.

This build has **no synthetic training path**. The trainer loads only rows recorded by the live Groww collector when `require_groww_source=True` in `bot/config.py`.

## Accepted training sources

```text
option_chain_snapshots.source = groww_option_chain
quote_snapshots.source        = groww_quote
```

Rows are excluded if their raw payload contains:

```text
synthetic
dummy
fake
```

## Models included

1. **Premium return regressor**
   - Target: `future_return_after_costs`
   - Meaning: expected executable option premium return after estimated round-trip cost.

2. **Profit probability classifier**
   - Target: `future_positive`
   - Meaning: probability that the option premium is positive after costs over the label horizon.

3. **Cross-sectional contract ranker**
   - Target: `future_rank_pct`
   - Meaning: which option is better than other contracts in the same snapshot.

4. **Return quantile models**
   - Targets: q20, q50, q80 of future return.
   - Meaning: downside/base/upside return estimate for uncertainty-aware scoring.

5. **IV expansion classifier**
   - Target: `future_iv_up`
   - Meaning: probability that the option IV expands over the horizon.

6. **Deterministic cost/liquidity estimator**
   - Uses spread, depth imbalance, and volume rank.
   - Replace/augment with broker fill-trained slippage model after enough real fills exist.

## Data required before training

Run the collector first:

```bash
python -m bot.cli collect-loop
```

Required DB tables:

```text
option_chain_snapshots
quote_snapshots strongly preferred
```

The model can build from option-chain snapshots only, but quote/depth data improves labels, costs, and execution quality.

## Train models

```bash
python -m bot.cli train-models
```

Artifacts:

```text
models/option_model_suite.joblib
models/model_suite_meta.json
```

## Inspect model quality

```bash
python -m bot.cli model-report
```

Important metrics:

```text
return_spearman_ic
rank_spearman_ic
top_decile_mean_return
top_decile_win_rate
top_decile_alpha_vs_universe
top1_per_snapshot_mean_return
top1_per_snapshot_win_rate
top1_per_snapshot_sharpe
top1_per_snapshot_alpha_vs_universe
passed_live_gate
```

## Score latest snapshot without trading

```bash
python -m bot.cli score-models-latest --limit 20
```

This prints top model-ranked contracts with:

```text
predicted_return
prob_profit
prob_iv_expansion
rank_score
return_q20/q50/q80
estimated_cost
model_score
```

No order is placed by this command.

## Live auto-training behavior

`live-loop` records new Groww snapshots and attempts training when enough labelled rows exist. If no real trained model exists, it returns:

```text
NO_TRADE: no_real_groww_trained_model
```

If the model exists but fails gates, live orders are blocked:

```text
PAPER_ONLY_MODEL_GATE_FAILED
```

## What is intentionally not guaranteed

The code targets good win rate, Sharpe, and alpha through real-data gates, but it does not guarantee them. If the live-recorded data does not show edge, the correct behavior is to refuse trading.

# v75 Margin Allocation Audit

## Scope
Audited account/margin/risk sizing path:

- `PortfolioManager.allocate_balance()`
- `PortfolioRiskManager.get_available_balance()`
- `QuantStrategy._compute_quantity()`
- per-instrument policy margin settings
- aggregate portfolio risk cap
- trade-history margin metrics

## Findings

### 1. Policy margin was logged but not enforced in sizing
The runtime printed instrument margin policy, e.g. BTC 20%, commodities 14%, equities 12%, but `_compute_quantity()` used only the legacy `MAX_ENTRY_MARGIN_USAGE_PCT` cap against the slot-scoped available balance.

Example from latest logs before this fix:

```text
slot_available=$33.20, cash_budget=$19.92
```

That is 60% of the slot, not the commodity/equity policy margin. This made policy margin partly cosmetic and could distort cross-asset allocation.

### 2. Aggregate portfolio risk cap existed in config but was not enforced
`PORTFOLIO_MAX_AGGREGATE_RISK_PCT = 3.0` existed, but no code calculated existing open risk or remaining risk budget before sizing a new entry. With six simultaneous slots, min-lot exceptions could theoretically exceed intended account-level risk.

### 3. Margin/PnL trade history calculations are directionally correct
RiskManager records:

```text
notional = entry_price * quantity
margin_used = notional / leverage
return_on_margin = pnl / margin_used
```

and the v28 tests cover capital-weighted win rate so a small-margin winner cannot hide a large-margin loser.

## Fixes in v75

### Enforced per-instrument margin budget
For portfolio-scoped multi-asset sizing, executable cash budget is now:

```text
min(slot_available,
    raw_available,
    raw_available * MAX_ENTRY_MARGIN_USAGE_PCT,
    raw_total * active_instrument_policy.margin_pct)
```

This means:

- BTC can use up to the BTC policy / slot cap.
- Commodities use commodity policy margin.
- Equities use equity policy margin.
- Legacy 60% remains as an emergency hard ceiling, not the actual policy allocator.

### Enforced aggregate open-risk cap
`PortfolioManager.allocate_balance()` now calculates:

```text
open_risk_usd = sum(abs(entry - sl) * qty for open positions)
aggregate_risk_cap = raw_total * PORTFOLIO_MAX_AGGREGATE_RISK_PCT / 100
remaining_risk_cap = aggregate_risk_cap - open_risk_usd
```

`_compute_quantity()` now:

- rejects if aggregate risk is exhausted,
- haircuts target risk to remaining risk if necessary,
- caps min-lot exceptions by remaining aggregate risk.

### Better sizing diagnostics
Sizing logs now include:

```text
policy_cap=...
agg_open=...
agg_rem=...
```

so margin/risk budget decisions are visible in logs.

## Validation

```text
python -m py_compile: passed
node --check dashboard/frontend/app.js: passed
unittest margin/portfolio/capital metrics: passed
```

Targeted tests:

```text
tests.test_v75_margin_allocation
tests.test_multi_asset_portfolio
tests.test_v28_risk_budget_capital_metrics
```

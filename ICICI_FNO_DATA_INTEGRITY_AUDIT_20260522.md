# ICICI Breeze F&O Data Integrity and Execution Audit — 22 May 2026

## Scope

This audit addresses the live-runtime defect where the NIFTY/ICICI desk reported an open option position even though no valid open option vehicle was identified. The patch is applied on top of `v508_dol_first_runtime_fixed_reaudited` and intentionally does not alter the DOL/entry alpha model for Delta assets.

## Runtime defect confirmed

The supplied runtime log repeatedly emitted a broker position with no option identity:

- `symbol=- right=- strike=- expiry=- qty=639 adoptable=False`
- later the same unnamed object changed to `qty=119`
- reconciliation then raised a ghost-position critical message using the same identity-less row.

A tradeable/managable ICICI options position cannot be established from quantity alone. It must be identified as the NFO derivatives/options segment and carry the exact underlying, option right, strike and expiry.

The same runtime log contained no ICICI F&O funds verification message before the NIFTY desk became ready. Although an F&O balance adapter existed, it was only called lazily by sizing; it did not prove account source at startup, and RiskManager stripped its `FNO/source` provenance when returning cached balances.

## Official Breeze data contract used

The implementation is aligned to the official Breeze API contract:

- `GET /funds` exposes `allocated_fno`, `block_by_trade_fno`, `unallocated_balance`, and `total_bank_balance`.
- `GET /margin` with `exchange_code="NFO"` exposes NFO margin availability.
- `GET /portfoliopositions` option examples identify an actual F&O option row through `segment="fno"`, `product_type="Options"`, `exchange_code="NFO"`, `stock_code`, `expiry_date`, `strike_price`, `right`, and `quantity`.
- Breeze prohibits market orders; official priced order types include `limit` and `stoploss`.
- `GET /order` order-list responses do not rely on a PortfolioPositions `segment` field; open-order recovery therefore validates NFO/options/exact contract identity without incorrectly demanding `segment`.

## Corrected data architecture

### Position source-of-truth

`execution/order_manager.py` now creates ICICI position state only from strict, exact rows:

```text
segment = fno or nfo
exchange_code = NFO
product_type/product = Options
stock_code = selected underlying (for this desk: NIFTY)
right = call/put
strike_price > 0
expiry_date present
quantity != 0
```

Rows with positive quantities but missing this identity are ignored and counted as `ignored_non_option_rows`; they no longer raise a fake option-position alarm or become strategy state.

Valid but unexpected short-option exposure is not silently treated as the long-premium strategy; it is flagged unadoptable because the desk is buy-premium only.

Valid exact NFO option exposure that does not match the bot-selected vehicle blocks automatic adoption rather than guessing the contract.

### Funds and margin source-of-truth

The ICICI adapter already calculated spendable derivatives funds as:

```text
Primary:  available = allocated_fno - block_by_trade_fno
Fallback: available = NFO cash_limit/amount_allocated - NFO block_by_trade
```

The patch preserves this provenance through `RiskManager` caching and startup reporting:

```text
currency=INR
segment=FNO
source=funds.allocated_fno_minus_block_by_trade_fno or margin.NFO...
fno_allocated, fno_blocked, nfo_cash_limit retained
```

The NIFTY desk now runs an ICICI F&O account preflight before market-data analysis begins. It logs the verified derivatives balance source and verifies exact NFO positions as either `FLAT` or a precisely identified position. If funds or portfolio positions cannot be verified, the ICICI desk fails closed for trading.

### Order and exit safety fixes

1. **Stop-loss order wire format corrected.** Protective ICICI stops are now routed as official Breeze `order_type="stoploss"` orders with a positive `stoploss` trigger. The previous path silently encoded standalone stops as `limit` orders.
2. **Market orders prohibited.** Opening market orders remain rejected. Emergency reductions no longer attempt an unsupported Breeze MARKET order; they route a priced, aggressive sell-to-close LIMIT only after an exact adoptable NFO option position is established.
3. **Lot risk cannot round upward.** ICICI option order quantity is floored to whole lots. A calculated quantity of 99 with a 50-unit lot now routes 50, not 100; it cannot exceed the risk quantity because of `round()`.
4. **Open-order recovery is implemented.** The adapter can query the official NFO order list and recover outstanding exact-contract protective orders after restart, while filtering unrelated orders.
5. **Stale ghost warning state clears.** Once a strict NFO query verifies flat, stale unmanaged-position alarms from malformed prior responses are cleared.

## Files changed

- `exchanges/icici/api.py`
- `execution/order_manager.py`
- `risk/risk_manager.py`
- `orchestration/multi_asset_bot.py`
- `strategy/quant_strategy.py`
- `tests/test_icici_options.py`

## Verification

Automated checks completed:

```text
python -m py_compile execution/order_manager.py exchanges/icici/api.py risk/risk_manager.py orchestration/multi_asset_bot.py strategy/quant_strategy.py
DELTA_API_KEY=dummy DELTA_SECRET_KEY=dummy BREEZE_API_KEY=dummy BREEZE_SECRET_KEY=dummy python -m pytest -q
147 passed
```

Targeted malformed-row smoke verification:

```text
Input:  {quantity: 639, average_price: 1792.08} with no NFO option identity
Output: side=None size=0.0 ignored_non_option_rows=1 position_scope_verified=True

Input funds: allocated_fno=10000, block_by_trade_fno=2500
Output funds: segment=FNO source=funds.allocated_fno_minus_block_by_trade_fno available=7500 total=10000
```

## Expected new live logs

At the next ICICI-enabled runtime startup during market hours, the correct path should show messages similar to:

```text
ICICI F&O balance source=funds.allocated_fno_minus_block_by_trade_fno available=... allocated=... blocked=... nfo_cash_limit=...
NIFTY ICICI F&O funds verified: source=... available=₹... allocated=₹... blocked=₹...
ICICI F&O position filter ignored 1 non-executable broker row(s) [...]; only exact NFO Options rows can create position state
NIFTY ICICI F&O positions verified FLAT: exact NFO option positions=0 ignored_non_option_rows=1
```

It must no longer emit a ghost position for rows with blank `symbol/right/strike/expiry`.

## Limitation

The provided live log proves the old false-position path; it does not contain a post-patch live Breeze response. Automated and synthetic-response verification confirms the code behavior, but a fresh runtime log is still required to prove ICICI’s real account response maps to the corrected F&O path on your broker session.

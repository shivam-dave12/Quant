# Multi-Asset Institutional Scanner Update

## What changed

This package upgrades the BTC-only runtime into a live-catalog multi-asset scanner.
It does not invent OIL, GOLD, SILVER, SPX, or stock instruments. At startup it queries
Delta and CoinSwitch product/instrument catalogs and activates only contracts returned
by the exchange.

## New files

- `core/instruments.py` — instrument dataclasses, symbol aliases, per-thread instrument context.
- `execution/instrument_registry.py` — live exchange catalog discovery and requested-asset matching.
- `orchestration/multi_asset_bot.py` — multi-contract orchestrator, one isolated strategy state per instrument.

## Modified files

- `config.py` — added `MULTI_ASSET_ENABLED`, requested universe, portfolio exposure caps.
- `main.py` — starts `MultiAssetQuantBot` when `MULTI_ASSET_ENABLED=True`.
- `exchanges/delta/data_manager.py` — accepts per-instrument symbols.
- `exchanges/coinswitch/data_manager.py` — accepts per-instrument REST/WS symbols.
- `execution/order_manager.py` — accepts per-instrument execution symbols, product IDs, tick/lot metadata.
- `aggregator/market_aggregator.py` — carries instrument metadata in logs/context.
- `strategy/quant_strategy.py` — instrument-aware symbol, tick size, lot step, min/max qty via context.
- `telegram/controller.py` — added `/assets` command.
- `exchanges/coinswitch/api.py` — added discovery helper methods for futures instruments/tickers.

## Runtime behavior

1. Discover exchange catalogs.
2. Match requested assets using aliases as search keys only.
3. Activate confirmed tradable contracts.
4. Skip unavailable contracts with a terminal + Telegram reason.
5. Run independent strategy/data/risk/execution state per confirmed contract.
6. Enforce portfolio-level max-open-position, one-position-per-contract, and asset-class exposure caps.
7. Allocate cash/margin capacity across portfolio slots while keeping dollar-risk sizing portfolio-aware, so BTC min lot does not get rejected simply because the account is split across assets.

## Multi-position budget model

Default settings now allow up to `PORTFOLIO_MAX_OPEN_POSITIONS = 4` concurrent reserved slots. A reserved slot is any contract in `ENTERING`, `ACTIVE`, or `EXITING` phase. `PORTFOLIO_MAX_OPEN_PER_CONTRACT = 1` prevents duplicate positions in the same contract while still allowing different contracts to trade simultaneously.

`PORTFOLIO_BUDGET_MODE = "equal_slots"` gives each contract a slot-scoped cash/margin view before `QuantStrategy._compute_quantity()` applies leverage, fee reserve, tick/lot constraints and the margin cap. Dollar-risk sizing uses `risk_available` / `risk_total`, which remain tied to portfolio equity rather than slot equity. This is deliberate: with a small account, BTC's `0.001` minimum lot can be above the confidence-haircut target after slot slicing while still being inside the raw portfolio risk cap. Example: with 4 slots and 60% balance usage, each contract can use roughly 15% of account equity as margin, while its risk cap is measured against the portfolio equity base. `PORTFOLIO_MIN_LOT_MAX_RISK_MULT` allows a minimum exchange lot only when it is still within the raw per-position portfolio risk envelope.

## Validation performed

- `python -m compileall -q .`
- `python -m unittest tests.test_hardening tests.test_multi_asset_portfolio -q` → 37 tests passed.
- Instrument registry smoke-tested with fake Delta/CoinSwitch product catalogs.

## Important limitation

If Delta/CoinSwitch do not list commodities, indices, or US equities in their live API catalogs,
this package will not trade them. That is intentional: no synthetic data and no fake tickers.

## v3 symbol-audit / Telegram-start fix

This package fixes the issue where Telegram `/start` launched the legacy single-symbol `QuantBot` even when `MULTI_ASSET_ENABLED=True`.  `/start` now launches `MultiAssetQuantBot`, while importing `main` first so the production terminal/file logging format remains active.

Audit/logging changes:

- Every strategy tick executed inside an instrument scope emits a symbol-aware log prefix: `[ASSET|EXCHANGE:SYMBOL]`.
- Terminal heartbeat box titles now include the asset and executable symbol, for example `BTC / DELTA:BTCUSD LIQUIDITY ENGINE`.
- The market line inside the heartbeat includes `symbol BTC DELTA BTCUSD`.
- Data-manager warmup/readiness logs include the exact exchange symbol, e.g. `Delta warmup BTCUSD 5m: 199 candles`.
- The multi-asset loop emits `ANALYSIS_TICK asset=... primary=... symbol=...` every `SCANNER_ASSET_ANALYSIS_LOG_SEC` seconds per ready contract, proving that every active contract is being stepped even when there is no sweep/posterior event.
- `/assets` remains the operator command to see active, ready and unavailable instruments.

If the logs still show only BTC, that means the live exchange catalogs returned only BTC from the requested universe.  OIL/GOLD/SILVER/SPX/stocks are never faked; they are shown under unavailable until the exchange actually lists matching contracts.

## v4 symbol integrity update

- Delta `SPXUSD` is treated as SPX6900 crypto, **not** S&P 500. It is no longer matched to the requested `SPX_INDEX` universe.
- US equity exposure uses Delta xStock/RWA symbols when the live catalog confirms them: `AAPLXUSD`, `NVDAXUSD`, `TSLAXUSD`, `AMZNXUSD`, `METAXUSD`, `GOOGLXUSD`, and `MSFTXUSD` if listed.
- Gold/silver exposure uses tokenized/RWA derivatives only when live-confirmed: `PAXGUSD` / `XAUTUSD` for gold and `SLVONUSD` for silver if listed.
- CoinSwitch is crypto futures only in this implementation. Because its docs expose ticker/orderbook/klines as per-symbol endpoints, the registry now live-validates `BTCUSDT` with CoinSwitch ticker even if the bulk instrument endpoint returns an incomplete subset. When validated, CoinSwitch is wired as secondary feed for BTC under the Delta primary.
- Leverage is instrument capped. The bot targets `min(config.LEVERAGE, exchange_max_leverage)` per contract so contracts like SPX6900/xStock cannot be sent with BTC's 40x default when their exchange cap is lower.
- Portfolio loop remains non-blocking per asset: a live/trailing position in one context does not stop `on_tick` analysis for other ready contexts unless the portfolio slot cap is fully consumed for new entries. Existing positions always continue to be managed.

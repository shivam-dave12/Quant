# v78 Cleanup Report — Delta-only unified EntryEngine

## Removed
- Removed `exchanges/alternate_exchange/` package.
- Removed alternate exchange discovery from `execution/instrument_registry.py`.
- Removed runtime exchange switching and alternate venue routing from `execution/router.py`.
- Removed alternate exchange data manager construction from `orchestration/multi_asset_bot.py`.
- Removed `.pytest_cache` and all `__pycache__` bytecode from the deliverable.

## Preserved
- Core liquidity-hunt strategy: sweep detection, post-sweep auction, liquidity map, EntryEngine, liquidity-aware TP/SL, Delta native bracket execution, portfolio risk accounting and adaptive exit.

## New authority model
`LiquidityMap / Sweep -> EntryEngine opportunity score -> TP/SL liquidity EV frontier -> mechanical risk/execution -> adaptive exit`

## Quantitative execution improvements
- TP selection uses EV_R, required delivery probability, minimum institutional delivery probability, reachable-liquidity penalty, gauntlet penalty and fee-adjusted payoff.
- SL selection scores protective liquidity per unit of capital risk so high-quality far pools must earn their additional risk.
- EntryEngine adds one continuous opportunity score combining posterior probability, expected R, evidence separation and phase maturity; this prevents jumping into random trades without adding duplicated filters.
- Telegram text now reflects the new unified flow and Delta-only native-bracket execution.

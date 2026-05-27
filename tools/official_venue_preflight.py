#!/usr/bin/env python3
"""Read-only official API preflight for CoinSwitch Futures and Hyperliquid perps.

This command performs authenticated reads only.  It never changes leverage,
places orders, cancels orders or modifies positions.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config
from exchanges.coinswitch.api import FuturesAPI
from exchanges.hyperliquid.api import HyperliquidAPI

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")


def compact(value: Any) -> Any:
    if isinstance(value, dict):
        drop = {"raw", "raw_response", "abstraction_state"}
        return {k: compact(v) for k, v in value.items() if k not in drop}
    if isinstance(value, list):
        return [compact(v) for v in value[:10]]
    return value


def run_coinswitch(symbols: list[str]) -> dict[str, Any]:
    api = FuturesAPI()
    balance = api.get_balance("USDT")
    out: dict[str, Any] = {"balance": compact(balance), "symbols": {}}
    for symbol in symbols:
        sym = symbol.replace("/", "").upper()
        out["symbols"][sym] = {
            "ticker": compact(api.get_futures_ticker(symbol=sym, exchange=config.COINSWITCH_EXCHANGE)),
            "orderbook": compact(api.get_orderbook(symbol=sym, exchange=config.COINSWITCH_EXCHANGE)),
            "positions": compact(api.get_positions(exchange=config.COINSWITCH_EXCHANGE, symbol=sym)),
            "open_orders": compact(api.get_open_orders(exchange=config.COINSWITCH_EXCHANGE, symbol=sym)),
        }
    return out


def run_hyperliquid(symbols: list[str]) -> dict[str, Any]:
    api = HyperliquidAPI.from_config()
    out: dict[str, Any] = {"symbols": {}}
    for symbol in symbols:
        out["symbols"][symbol] = {
            "balance": compact(api.get_balance(symbol)),
            "asset_context": compact(api.current_asset_context(symbol)),
            "position_state": compact(api.user_state(coin=symbol)),
            "open_orders": compact(api.open_orders(coin=symbol)),
        }
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only balance/order/position verification using official venue contracts")
    parser.add_argument("--coinswitch", nargs="*", default=["BTCUSDT", "PAXGUSDT", "XAGUSDT"])
    parser.add_argument("--hyperliquid", nargs="*", default=["BTC", "xyz:GOLD", "xyz:SILVER"])
    args = parser.parse_args()
    report: dict[str, Any] = {
        "execution_policy": {
            "live_execution_venues": list(getattr(config, "LIVE_EXECUTION_VENUES", ())),
            "discovery_primary_exchange": getattr(config, "DISCOVERY_PRIMARY_EXCHANGE", ""),
            "cross_venue_raw_price_routing_enabled": getattr(config, "CROSS_VENUE_RAW_PRICE_ROUTING_ENABLED", False),
            "composite_intelligence_enabled": getattr(config, "INSTITUTIONAL_COMPOSITE_INTELLIGENCE_ENABLED", False),
            "factor_by_asset": getattr(config, "INSTITUTIONAL_FACTOR_BY_ASSET", {}),
            "execution_equivalence_group_by_asset": getattr(config, "INSTITUTIONAL_EXECUTION_EQUIVALENCE_GROUP_BY_ASSET", {}),
            "factor_transfer_mode_by_asset": getattr(config, "INSTITUTIONAL_FACTOR_TRANSFER_MODE_BY_ASSET", {}),
            "validated_factor_translation_models": getattr(config, "INSTITUTIONAL_VALIDATED_FACTOR_TRANSLATION_MODELS", {}),
            "exposure_groups": [
                {"asset_id": row.get("asset_id"), "aliases": row.get("aliases", [])}
                for row in getattr(config, "MULTI_ASSET_REQUESTS", [])
                if str(row.get("asset_id", "")).startswith(("SILVER", "GOLD"))
            ],
        }
    }
    try:
        report["coinswitch"] = run_coinswitch(args.coinswitch)
    except Exception as exc:
        report["coinswitch"] = {"error": str(exc)}
    try:
        report["hyperliquid"] = run_hyperliquid(args.hyperliquid)
    except Exception as exc:
        report["hyperliquid"] = {"error": str(exc)}
    print(json.dumps(report, indent=2, sort_keys=True, default=str))
    has_error = any(isinstance(v, dict) and v.get("error") for v in report.values())
    return 2 if has_error else 0


if __name__ == "__main__":
    raise SystemExit(main())

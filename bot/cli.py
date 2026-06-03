from __future__ import annotations

import argparse
import json
import logging
import threading
import time
import webbrowser
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pandas as pd

from .collector import OptionDataCollector
from .config import load_config
from .groww_adapter import GrowwAdapter
from .groww_instruments import ensure_groww_instruments_csv, load_option_contracts_from_groww_instruments
from .logging_utils import setup_logging
from .model import OptionReturnTrainer
from .option_models import (
    OptionModelSuite,
    build_option_model_frame,
    load_option_model_raw_data,
    train_model_suite_from_store,
    training_readiness_from_store,
)
from .nse_contracts import load_nifty_options_contracts
from .risk import normalize_quote, quote_is_executable
from .run_state import build_rundown, compact_startup_summary, load_rundown, save_rundown, utc_now_iso
from .storage import Store
from .live import LiveOptionBot
from .zerodha_adapter import ZerodhaAdapter
from .zerodha_instruments import ensure_zerodha_instruments_csv, load_zerodha_mcx_options

log = logging.getLogger(__name__)


def cmd_inspect_contracts(args) -> None:
    cfg = load_config()
    path = Path(args.path or cfg.nse_contract_file)
    df = load_nifty_options_contracts(path, cfg.underlying)
    print(f"Loaded {len(df):,} {cfg.underlying} option contracts from {path}")
    print(f"Expiries: {', '.join(map(str, sorted(pd.to_datetime(df['expiry']).dt.date.unique())[:8]))}")
    print(df.head(20).to_string(index=False))


def _candidate_contract_files(cfg) -> list[Path]:
    """Return contract-file candidates in safe precedence order.

    Important for Docker/Podman: /app/data is often a bind mount, so a contract
    file bundled under /app/data/raw can be hidden by the host mount. Therefore
    the release bundle keeps a copy under /app/assets and this resolver also
    scans the mounted data/raw directory for operator-provided files.
    """
    candidates: list[Path] = []
    cfg_path = Path(cfg.nse_contract_file) if cfg.nse_contract_file else None
    if cfg_path is not None:
        candidates.append(cfg_path)
    candidates.extend([
        Path("assets/NSE_FO_contract_29052026.csv.gz"),
        Path("/app/assets/NSE_FO_contract_29052026.csv.gz"),
        Path("data/raw/NSE_FO_contract_29052026.csv.gz"),
        Path("/app/data/raw/NSE_FO_contract_29052026.csv.gz"),
    ])
    for base in (Path("data/raw"), Path("/app/data/raw"), Path("assets"), Path("/app/assets")):
        if base.exists():
            candidates.extend(sorted(base.glob("NSE_FO_contract*.csv*")))
    out: list[Path] = []
    seen: set[str] = set()
    for c in candidates:
        key = str(c)
        if key not in seen:
            seen.add(key)
            out.append(c)
    return out


def _parse_expiry_values(values: Any) -> list[str]:
    if values is None:
        return []
    if isinstance(values, str):
        raw_values = [values]
    else:
        raw_values = list(values)

    expiries: list[str] = []
    for value in raw_values:
        for part in str(value).replace(";", ",").split(","):
            item = part.strip()
            if not item:
                continue
            expiries.append(str(pd.to_datetime(item).date()))
    return list(dict.fromkeys(expiries))


def _parse_asset_values(values: Any) -> list[str]:
    if values is None:
        return []
    if isinstance(values, str):
        raw_values = [values]
    else:
        raw_values = list(values)
    assets: list[str] = []
    for value in raw_values:
        for part in str(value).replace(";", ",").split(","):
            item = part.strip().lower()
            if item:
                assets.append(item)
    return list(dict.fromkeys(assets))


def resolve_assets(args, cfg):
    requested = _parse_asset_values(getattr(args, "asset", None))
    if not requested:
        return cfg.active_asset_profiles()
    return [cfg.get_asset_profile(asset_id) for asset_id in requested]


def _available_expiries(cfg) -> set[str]:
    for path in _candidate_contract_files(cfg):
        if path.exists():
            df = load_nifty_options_contracts(path, cfg.underlying)
            return {str(x) for x in sorted(pd.to_datetime(df["expiry"]).dt.date.unique())}
    return set()


def resolve_expiries(args, cfg, asset=None) -> list[str]:
    expiries = _parse_expiry_values(getattr(args, "expiry", None))
    if not expiries:
        expiries = _parse_expiry_values(asset.expiry_dates if asset is not None else getattr(cfg, "expiry_dates", ()))
    if not expiries:
        raise ValueError(
            "No expiry basket configured. Pass one or more --expiry YYYY-MM-DD values "
            "or set expiry_dates in bot/config.py. Nearest/current expiry is intentionally not auto-selected."
        )

    available = set() if asset is not None and asset.exchange.upper() != "NSE" else _available_expiries(cfg)
    if available:
        missing = [expiry for expiry in expiries if expiry not in available]
        if missing:
            log.warning("configured expiries not found in contract file | missing=%s available_sample=%s", missing, sorted(available)[:10])
    log.info("using_expiry_basket asset=%s expiries=%s", asset.asset_id if asset is not None else "legacy", ",".join(expiries))
    return expiries


def cmd_collect_once(args) -> None:
    cfg = load_config()
    assets = resolve_assets(args, cfg)
    store = Store(cfg.db_path)
    adapter = GrowwAdapter(cfg.groww_totp_token, cfg.groww_totp_secret)
    report = {}
    for asset in assets:
        expiries = resolve_expiries(args, cfg, asset)
        collector = OptionDataCollector(cfg, store, adapter, asset)
        report[asset.asset_id] = collector.collect_expiries_once(expiries, quote_top_symbols=not args.no_quotes)
    print(json.dumps(report, indent=2))


def cmd_collect_loop(args) -> None:
    cfg = load_config()
    assets = resolve_assets(args, cfg)
    store = Store(cfg.db_path)
    adapter = GrowwAdapter(cfg.groww_totp_token, cfg.groww_totp_secret)
    while True:
        try:
            for asset in assets:
                try:
                    expiries = resolve_expiries(args, cfg, asset)
                    OptionDataCollector(cfg, store, adapter, asset).collect_expiries_once(expiries, quote_top_symbols=not args.no_quotes)
                except Exception:
                    log.exception("collector loop error | asset=%s", asset.asset_id)
        except KeyboardInterrupt:
            raise
        time.sleep(cfg.option_chain_interval_seconds)


def cmd_train(args) -> None:
    cfg = load_config()
    store = Store(cfg.db_path)
    result = OptionReturnTrainer(cfg, store).train()
    print(json.dumps(result.metrics, indent=2))
    print(f"Saved model: {result.model_path}")
    print(f"Saved meta : {result.meta_path}")




def cmd_train_models(args) -> None:
    cfg = load_config()
    store = Store(cfg.db_path)
    assets = resolve_assets(args, cfg)
    report = {}
    for asset in assets:
        try:
            result = train_model_suite_from_store(cfg, store, asset)
            report[asset.asset_id] = {
                "model_path": str(result.model_path),
                "meta_path": str(result.meta_path),
                "metrics": result.metrics,
            }
        except ValueError as exc:
            report[asset.asset_id] = {"trained": False, "reason": str(exc)}
    print(json.dumps(report, indent=2, default=str))


def cmd_training_status(args) -> None:
    cfg = load_config()
    store = Store(cfg.db_path)
    assets = resolve_assets(args, cfg)
    report = {asset.asset_id: training_readiness_from_store(cfg, store, asset) for asset in assets}
    print(json.dumps(report, indent=2, default=str))


def cmd_model_report(args) -> None:
    cfg = load_config()
    assets = resolve_assets(args, cfg)
    report = {}
    for asset in assets:
        path = cfg.model_suite_meta_path_for(asset.asset_id)
        report[asset.asset_id] = json.loads(path.read_text()) if path.exists() else {"error": "No model-suite meta found. Run train-models first."}
    print(json.dumps(report, indent=2, default=str))


def _latest_rows_per_expiry(frame: pd.DataFrame, expiries: list[str] | None = None, window_seconds: int = 90) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    data = frame.copy()
    data["ts"] = pd.to_datetime(data["ts"])
    data["expiry"] = pd.to_datetime(data["expiry"])
    if expiries:
        expiry_dates = {pd.to_datetime(expiry).date() for expiry in expiries}
        data = data[data["expiry"].dt.date.isin(expiry_dates)].copy()
    frames: list[pd.DataFrame] = []
    for _, sub in data.groupby(data["expiry"].dt.date, sort=True):
        latest_ts = sub["ts"].max()
        scan = sub[sub["ts"].ge(latest_ts - pd.Timedelta(seconds=window_seconds))].copy()
        if scan.empty:
            scan = sub[sub["ts"].eq(latest_ts)].copy()
        scan = scan.sort_values(["trading_symbol", "ts"]).groupby("trading_symbol", as_index=False).tail(1)
        scan["ts"] = latest_ts
        frames.append(scan.copy())
    return pd.concat(frames, ignore_index=True) if frames else data.iloc[0:0].copy()


def cmd_score_models_latest(args) -> None:
    cfg = load_config()
    store = Store(cfg.db_path)
    assets = resolve_assets(args, cfg)
    scored_frames = []
    for asset in assets:
        model_path = cfg.model_suite_path_for(asset.asset_id)
        if not model_path.exists():
            log.warning("model suite missing | asset=%s path=%s", asset.asset_id, model_path)
            continue
        raw = load_option_model_raw_data(cfg, store, asset)
        if raw.empty:
            log.warning("no option snapshots available | asset=%s", asset.asset_id)
            continue
        frame = build_option_model_frame(raw, horizon_rows=cfg.label_horizon_rows, cost_bps=cfg.estimated_round_trip_cost_bps)
        expiries = _parse_expiry_values(args.expiry) or _parse_expiry_values(asset.expiry_dates)
        latest = _latest_rows_per_expiry(frame, expiries or None)
        if latest.empty:
            continue
        suite = OptionModelSuite.load(model_path)
        cfg_payload = suite.metrics.get("config") if isinstance(suite.metrics, dict) else {}
        min_width = float((cfg_payload or {}).get("min_snapshot_width") or 4)
        median_width = suite.metrics.get("median_options_per_snapshot") if isinstance(suite.metrics, dict) else None
        if median_width is None or float(median_width) < min_width:
            log.warning(
                "model suite skipped for scoring: insufficient cross-section metadata | asset=%s path=%s median_width=%s min_snapshot_width=%.1f",
                asset.asset_id,
                model_path,
                median_width,
                min_width,
            )
            continue
        scored_frames.append(suite.predict(latest))
    if not scored_frames:
        print("No latest scored rows available for the requested asset/expiry basket.")
        return
    scored = pd.concat(scored_frames, ignore_index=True).sort_values("model_score", ascending=False).head(args.limit)
    cols = [
        "ts", "asset_id", "trading_symbol", "expiry", "strike", "option_type", "ltp",
        "predicted_return", "edge_score", "raw_prob_profit", "prob_profit", "prob_iv_expansion", "rank_score",
        "return_q20", "return_q50", "return_q80", "estimated_cost",
        "adaptive_tp_pct", "adaptive_sl_pct", "adaptive_rr", "adaptive_breakeven_prob", "adaptive_exit_score",
        "policy_calibrated_ev", "policy_raw_ev",
        "meta_policy_active", "meta_policy_prob", "meta_policy_ev", "meta_policy_edge",
        "model_score", "model_rank_at_ts",
    ]
    print(scored[[c for c in cols if c in scored.columns]].to_string(index=False))



def cmd_data_audit(args) -> None:
    cfg = load_config()
    store = Store(cfg.db_path)
    report: dict[str, object] = {}
    for table in ("option_chain_snapshots", "quote_snapshots"):
        try:
            by_source = store.query_df(
                f"""
                SELECT coalesce(asset_id, 'UNKNOWN') AS asset_id,
                       coalesce(source, 'UNTAGGED') AS source,
                       count(*) AS rows
                FROM {table}
                GROUP BY 1, 2
                ORDER BY asset_id, rows DESC
                """
            )
            suspicious = store.query_df(
                f"""
                SELECT count(*) AS rows
                FROM {table}
                WHERE lower(coalesce(raw_json, '')) LIKE '%synthetic%'
                   OR lower(coalesce(raw_json, '')) LIKE '%dummy%'
                   OR lower(coalesce(raw_json, '')) LIKE '%fake%'
                """
            )
            report[table] = {
                "rows_by_source": by_source.to_dict(orient="records"),
                "suspicious_rows": int(suspicious.iloc[0]["rows"]),
            }
        except Exception as exc:
            report[table] = {"error": str(exc)}
    report["policy"] = {
        "require_groww_source": cfg.require_groww_source,
        "auto_train_enabled": cfg.auto_train_enabled,
        "model_suite_shadow_min_rows": cfg.model_suite_shadow_min_rows,
        "live_train_min_rows": cfg.live_train_min_rows,
    }
    print(json.dumps(report, indent=2, default=str))


def _json_obj(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return None
    return value


def cmd_repair_db(args) -> None:
    cfg = load_config()
    store = Store(cfg.db_path)
    try:
        quotes = store.query_df(
            """
            SELECT ts, trading_symbol, last_price, bid_price, bid_quantity, offer_price, offer_quantity, depth_json
            FROM quote_snapshots
            WHERE depth_json IS NOT NULL
              AND (
                bid_price IS NULL OR offer_price IS NULL
                OR bid_price <= 0 OR offer_price <= 0
                OR spread IS NULL OR spread_pct IS NULL
              )
            """
        )
    except Exception as exc:
        print(json.dumps({"repaired_rows": 0, "reason": f"quote_scan_failed:{exc}"}, indent=2))
        return

    repaired = 0
    for _, row in quotes.iterrows():
        normalized = normalize_quote({
            "last_price": row.get("last_price"),
            "bid_price": row.get("bid_price"),
            "bid_quantity": row.get("bid_quantity"),
            "offer_price": row.get("offer_price"),
            "offer_quantity": row.get("offer_quantity"),
            "depth": _json_obj(row.get("depth_json")),
        })
        if not normalized.get("bid_price") or not normalized.get("offer_price"):
            continue
        store.execute(
            """
            UPDATE quote_snapshots
            SET bid_price = ?,
                bid_quantity = ?,
                offer_price = ?,
                offer_quantity = ?,
                spread = ?,
                spread_pct = ?
            WHERE trading_symbol = ?
              AND ts = ?
            """,
            (
                normalized.get("bid_price"),
                normalized.get("bid_quantity"),
                normalized.get("offer_price"),
                normalized.get("offer_quantity"),
                normalized.get("spread"),
                normalized.get("spread_pct"),
                row.get("trading_symbol"),
                row.get("ts"),
            ),
        )
        repaired += 1
    if store.backend == "sqlite":
        store.con.commit()
    print(json.dumps({"repaired_rows": repaired, "scanned_rows": int(len(quotes)), "db_path": str(store.db_path)}, indent=2))


def cmd_groww_preflight(args) -> None:
    cfg = load_config()
    assets = resolve_assets(args, cfg)
    report: dict[str, Any] = {
        "db_path": str(cfg.db_path),
        "model_suite_path": str(cfg.model_suite_path),
        "model_suite_exists": cfg.model_suite_path.exists(),
        "contract_file_exists": cfg.nse_contract_file.exists(),
        "paper_trading": cfg.paper_trading,
        "live_trading_enabled": cfg.live_trading_enabled,
        "allow_groww_commodity_live_orders": cfg.allow_groww_commodity_live_orders,
        "max_open_positions": cfg.max_open_positions,
        "assets": [asset.asset_id for asset in assets],
        "asset_checks": {},
        "checks": {},
        "errors": [],
    }
    asset_checks = report["asset_checks"]
    checks = report["checks"]
    errors = report["errors"]

    for asset in assets:
        detail: dict[str, Any] = {
            "label": asset.label,
            "underlying": asset.underlying,
            "exchange": asset.exchange,
            "segment": asset.segment,
            "option_chain_mode": asset.option_chain_mode,
            "broker_order_supported": asset.broker_order_supported,
            "model_suite_path": str(cfg.model_suite_path_for(asset.asset_id)),
            "model_suite_exists": cfg.model_suite_path_for(asset.asset_id).exists(),
            "configured_expiries": list(asset.expiry_dates),
        }
        if args.for_live and asset.segment.upper() == "COMMODITY":
            detail["groww_commodity_live_order_override"] = cfg.allow_groww_commodity_live_orders
            detail["groww_commodity_standard_orders_supported_by_docs"] = True
            detail["groww_commodity_smart_orders_supported"] = False
            detail["commodity_exit_management"] = "bot_managed_standard_exit_orders"
            detail["groww_commodity_live_note"] = "COMMODITY uses standard Groww orders; Smart Orders/OCO are not used for MCX."
            if not cfg.allow_groww_commodity_live_orders:
                errors.append(f"groww_commodity_live_orders_disabled:{asset.asset_id}:{asset.segment}")
        elif args.for_live and not asset.broker_order_supported:
            errors.append(f"broker_order_unsupported:{asset.asset_id}:{asset.exchange}_{asset.segment}")
        if asset.option_chain_mode.lower() == "instrument_quotes":
            detail["instrument_master_path"] = str(cfg.groww_instruments_csv)
            try:
                _, refreshed, reason = ensure_groww_instruments_csv(
                    cfg.groww_instruments_csv,
                    cfg.groww_instruments_url,
                    cfg.groww_instruments_max_age_hours,
                    force=bool(getattr(args, "refresh_instruments", False)),
                )
                detail["instrument_master_refresh"] = reason
                detail["instrument_master_refreshed"] = refreshed
            except Exception as exc:
                detail["instrument_master_refresh_error"] = str(exc)
            detail["instrument_master_exists"] = cfg.groww_instruments_csv.exists()
            if not cfg.groww_instruments_csv.exists():
                errors.append(f"instrument_master_missing:{asset.asset_id}:{cfg.groww_instruments_csv}")
            else:
                try:
                    contracts = load_option_contracts_from_groww_instruments(cfg.groww_instruments_csv, asset)
                    by_expiry = (
                        contracts.groupby(contracts["expiry"].astype(str))["trading_symbol"]
                        .nunique()
                        .astype(int)
                        .to_dict()
                    )
                    detail["instrument_contracts_by_expiry"] = by_expiry
                    for expiry in asset.expiry_dates:
                        if int(by_expiry.get(str(pd.to_datetime(expiry).date()), 0)) <= 0:
                            errors.append(f"instrument_expiry_missing:{asset.asset_id}:{expiry}")
                except Exception as exc:
                    detail["instrument_master_error"] = str(exc)
                    errors.append(f"instrument_master_load_failed:{asset.asset_id}:{exc}")
        asset_checks[asset.asset_id] = detail

    for package in ("growwapi", "pyotp"):
        try:
            __import__(package)
            checks[f"{package}_import"] = True
        except Exception as exc:
            checks[f"{package}_import"] = False
            errors.append(f"{package}_import_failed:{exc}")

    if args.for_live and not cfg.live_trading_enabled:
        errors.append("live_trading_enabled_false")
    if args.for_live and not args.online:
        errors.append("for_live_requires_online")

    if args.online:
        try:
            adapter = GrowwAdapter(cfg.groww_totp_token, cfg.groww_totp_secret)
            checks["groww_auth"] = True
        except Exception as exc:
            checks["groww_auth"] = False
            errors.append(f"groww_auth_failed:{exc}")
            print(json.dumps(report, indent=2, default=str))
            raise SystemExit(1)

        for attr in (
            "get_quote",
            "place_buy_option_limit",
            "place_sell_option_limit",
            "get_order_detail",
            "get_order_list",
            "get_positions_for_user",
            "get_available_margin_details",
            "get_order_margin_details",
            "create_exit_oco",
            "create_short_exit_oco",
        ):
            ok = hasattr(adapter, attr)
            checks[f"adapter_{attr}"] = ok
            if not ok:
                errors.append(f"adapter_missing:{attr}")

        try:
            margin = adapter.get_available_margin_details()
            checks["available_margin"] = True
            report["available_margin_keys"] = sorted(margin.keys()) if isinstance(margin, dict) else str(type(margin))
        except Exception as exc:
            checks["available_margin"] = False
            errors.append(f"available_margin_failed:{exc}")

        try:
            positions = adapter.get_positions_for_user(assets[0].segment)
            checks["positions"] = True
            report["positions_count"] = len(positions.get("positions", [])) if isinstance(positions, dict) else None
        except Exception as exc:
            checks["positions"] = False
            errors.append(f"positions_failed:{exc}")

        if args.symbol:
            try:
                asset = assets[0]
                quote = normalize_quote(adapter.get_quote(args.symbol, segment=asset.segment, exchange=asset.exchange))
                ok, reason = quote_is_executable(quote, max_spread_pct=asset.max_spread_pct)
                checks["quote"] = True
                report["quote_check"] = {
                    "asset_id": asset.asset_id,
                    "symbol": args.symbol,
                    "bid_price": quote.get("bid_price"),
                    "offer_price": quote.get("offer_price"),
                    "offer_quantity": quote.get("offer_quantity"),
                    "spread_pct": quote.get("spread_pct"),
                    "executable": ok,
                    "reason": reason,
                }
                if not ok:
                    errors.append(f"quote_not_executable:{reason}")
            except Exception as exc:
                checks["quote"] = False
                errors.append(f"quote_failed:{exc}")

    print(json.dumps(report, indent=2, default=str))
    if errors:
        raise SystemExit(1)


def cmd_refresh_instruments(args) -> None:
    cfg = load_config()
    path, refreshed, reason = ensure_groww_instruments_csv(
        cfg.groww_instruments_csv,
        cfg.groww_instruments_url,
        cfg.groww_instruments_max_age_hours,
        force=args.force,
    )
    print(json.dumps({"path": str(path), "refreshed": refreshed, "reason": reason}, indent=2))


def cmd_rundown(args) -> None:
    cfg = load_config()
    store = Store(cfg.db_path)
    assets = resolve_assets(args, cfg)
    previous = load_rundown(cfg.run_rundown_path)
    if args.current:
        payload = build_rundown(
            cfg,
            store,
            assets,
            started_at_utc=previous.get("started_at_utc") or utc_now_iso(),
            cycle=int(previous.get("cycle") or 0),
            last_results={asset_id: (asset.get("last_result") if isinstance(asset, dict) else None) for asset_id, asset in (previous.get("assets") or {}).items()},
            previous=previous,
        )
        save_rundown(cfg.run_rundown_path, payload)
    else:
        payload = previous or build_rundown(cfg, store, assets, started_at_utc=utc_now_iso())
    print(json.dumps(payload, indent=2, default=str))


def cmd_live_once(args) -> None:
    cfg = load_config()
    if args.live:
        # Two-key live switch: CLI --live + live_trading_enabled=True in bot/config.py.
        object.__setattr__(cfg, "paper_trading", False)
    else:
        object.__setattr__(cfg, "paper_trading", True)
    assets = resolve_assets(args, cfg)
    store = Store(cfg.db_path)
    adapter = GrowwAdapter(cfg.groww_totp_token, cfg.groww_totp_secret)
    results = {}
    for asset in assets:
        expiries = resolve_expiries(args, cfg, asset)
        bot = LiveOptionBot(cfg, store, adapter, asset)
        results[asset.asset_id] = bot.trade_once(expiries)
    previous = load_rundown(cfg.run_rundown_path)
    save_rundown(
        cfg.run_rundown_path,
        build_rundown(
            cfg,
            store,
            assets,
            started_at_utc=previous.get("started_at_utc") or utc_now_iso(),
            cycle=int(previous.get("cycle") or 0),
            last_results=results,
            previous=previous,
        ),
    )
    print(json.dumps(results, indent=2, default=str))


def cmd_live_loop(args) -> None:
    cfg = load_config()
    if args.live:
        object.__setattr__(cfg, "paper_trading", False)
    else:
        object.__setattr__(cfg, "paper_trading", True)
    assets = resolve_assets(args, cfg)
    store = Store(cfg.db_path)
    adapter = GrowwAdapter(cfg.groww_totp_token, cfg.groww_totp_secret)
    previous = load_rundown(cfg.run_rundown_path)
    started_at = utc_now_iso()
    log.info(
        "startup rundown | file=%s previous=%s",
        cfg.run_rundown_path,
        compact_startup_summary(previous),
    )
    bots = [(asset, LiveOptionBot(cfg, store, adapter, asset), resolve_expiries(args, cfg, asset)) for asset in assets]
    results: dict[str, Any] = {}
    cycle = 0
    save_rundown(
        cfg.run_rundown_path,
        build_rundown(cfg, store, assets, started_at_utc=started_at, cycle=cycle, last_results=results, previous=previous),
    )
    while True:
        try:
            for asset, bot, expiries in bots:
                try:
                    result = bot.trade_once(expiries)
                    results[asset.asset_id] = result
                    log.info("asset=%s decision=%s reason=%s", asset.asset_id, result.get("decision"), result.get("reason"))
                except Exception:
                    log.exception("live loop error | asset=%s", asset.asset_id)
            cycle += 1
            save_rundown(
                cfg.run_rundown_path,
                build_rundown(cfg, store, assets, started_at_utc=started_at, cycle=cycle, last_results=results, previous=previous),
            )
        except KeyboardInterrupt:
            raise
        time.sleep(cfg.option_chain_interval_seconds)


def cmd_metrics(args) -> None:
    cfg = load_config()
    if not cfg.model_meta_path.exists():
        print("No model meta found. Run train first.")
        return
    print(cfg.model_meta_path.read_text())


def _write_env_value(path: Path, key: str, value: str) -> None:
    path = Path(path)
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    prefix = f"{key}="
    updated = False
    out: list[str] = []
    for line in lines:
        if line.startswith(prefix):
            out.append(f"{key}={value}")
            updated = True
        else:
            out.append(line)
    if not updated:
        out.append(f"{key}={value}")
    path.write_text("\n".join(out).rstrip() + "\n", encoding="utf-8")


def cmd_zerodha_login_url(args) -> None:
    cfg = load_config()
    adapter = ZerodhaAdapter(cfg.zerodha_api_key, cfg.zerodha_api_secret, cfg.zerodha_access_token)
    print(json.dumps({
        "login_url": adapter.login_url(),
        "access_token_loaded": bool(cfg.zerodha_access_token),
        "next_step": "After login, copy request_token from the redirect URL and run zerodha-generate-session.",
    }, indent=2))


def cmd_zerodha_generate_session(args) -> None:
    cfg = load_config()
    request_token = str(args.request_token or cfg.zerodha_request_token or "").strip()
    if not request_token:
        raise ValueError("Missing request token. Pass --request-token or set ZERODHA_REQUEST_TOKEN.")
    adapter = ZerodhaAdapter(cfg.zerodha_api_key, cfg.zerodha_api_secret, cfg.zerodha_access_token)
    session = adapter.generate_session(request_token)
    access_token = str(session.get("access_token") or "").strip()
    if not access_token:
        raise ValueError("Zerodha session response did not include access_token.")
    env_written = False
    if args.write_env:
        _write_env_value(Path(args.env_file), "ZERODHA_ACCESS_TOKEN", access_token)
        env_written = True
    print(json.dumps({
        "session_generated": True,
        "access_token_loaded": True,
        "access_token_written": env_written,
        "env_file": str(Path(args.env_file)) if env_written else None,
        "user_id": session.get("user_id"),
        "user_name": session.get("user_name"),
        "email_present": bool(session.get("email")),
        "note": "Access token is intentionally not printed.",
    }, indent=2, default=str))


def _capture_zerodha_request_token(login_url: str, host: str, port: int, path: str, timeout_seconds: int, open_browser: bool) -> dict[str, Any]:
    result: dict[str, Any] = {}
    ready = threading.Event()

    callback_path = "/" + str(path or "zerodha/callback").strip("/")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            token = (params.get("request_token") or [""])[0]
            status = (params.get("status") or [""])[0]
            error = (params.get("error") or params.get("error_type") or [""])[0]
            if parsed.path.rstrip("/") != callback_path.rstrip("/"):
                self.send_response(404)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(f"Unexpected callback path. Expected {callback_path}".encode("utf-8"))
                return
            result.update({
                "request_token": token,
                "status": status,
                "error": error,
                "query_keys": sorted(params.keys()),
            })
            ready.set()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            body = """
<!doctype html>
<html>
  <head><title>Zerodha token captured</title></head>
  <body style="font-family: system-ui, sans-serif; margin: 40px;">
    <h2>Zerodha request token captured.</h2>
    <p>You can close this tab and return to the bot terminal.</p>
  </body>
</html>
"""
            self.wfile.write(body.encode("utf-8"))

    server = ThreadingHTTPServer((host, int(port)), Handler)
    server.timeout = 1
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        if open_browser:
            webbrowser.open(login_url)
        if not ready.wait(max(1, int(timeout_seconds))):
            raise TimeoutError(
                f"Timed out waiting for Zerodha redirect at http://{host}:{port}{callback_path}. "
                "Make sure the Kite app redirect URL is set to that exact callback URL, or use --request-token."
            )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
    if result.get("error"):
        raise ValueError(f"Zerodha callback returned error={result.get('error')} status={result.get('status')}")
    if not result.get("request_token"):
        raise ValueError(f"Zerodha callback did not include request_token. Query keys={result.get('query_keys')}")
    return result


def cmd_zerodha_auto_session(args) -> None:
    cfg = load_config()
    adapter = ZerodhaAdapter(cfg.zerodha_api_key, cfg.zerodha_api_secret, cfg.zerodha_access_token)
    request_token = str(args.request_token or cfg.zerodha_request_token or "").strip()
    capture: dict[str, Any] | None = None
    if not request_token:
        capture = _capture_zerodha_request_token(
            adapter.login_url(),
            args.callback_host,
            int(args.callback_port),
            args.callback_path,
            int(args.timeout_seconds),
            bool(args.open_browser),
        )
        request_token = str(capture["request_token"])
    session = adapter.generate_session(request_token)
    access_token = str(session.get("access_token") or "").strip()
    if not access_token:
        raise ValueError("Zerodha session response did not include access_token.")
    env_written = False
    if args.write_env:
        _write_env_value(Path(args.env_file), "ZERODHA_ACCESS_TOKEN", access_token)
        env_written = True
    print(json.dumps({
        "session_generated": True,
        "request_token_captured": capture is not None,
        "access_token_loaded": True,
        "access_token_written": env_written,
        "env_file": str(Path(args.env_file)) if env_written else None,
        "callback_url": f"http://{args.callback_host}:{int(args.callback_port)}/" + str(args.callback_path or "zerodha/callback").strip("/"),
        "user_id": session.get("user_id"),
        "user_name": session.get("user_name"),
        "email_present": bool(session.get("email")),
        "note": "Access token and request token are intentionally not printed.",
    }, indent=2, default=str))


def cmd_zerodha_preflight(args) -> None:
    cfg = load_config()
    assets = [asset for asset in resolve_assets(args, cfg) if asset.segment.upper() == "COMMODITY"]
    report: dict[str, Any] = {
        "commodity_execution_broker": cfg.commodity_execution_broker,
        "zerodha_commodity_product": cfg.zerodha_commodity_product,
        "zerodha_commodity_max_lots": cfg.zerodha_commodity_max_lots,
        "paper_trading": cfg.paper_trading,
        "live_trading_enabled": cfg.live_trading_enabled,
        "assets": [asset.asset_id for asset in assets],
        "checks": {},
        "asset_checks": {},
        "errors": [],
    }
    checks = report["checks"]
    errors = report["errors"]
    try:
        __import__("kiteconnect")
        checks["kiteconnect_import"] = True
    except Exception as exc:
        checks["kiteconnect_import"] = False
        errors.append(f"kiteconnect_import_failed:{exc}")

    checks["zerodha_api_key_present"] = bool(cfg.zerodha_api_key)
    checks["zerodha_api_secret_present"] = bool(cfg.zerodha_api_secret)
    checks["zerodha_access_token_present"] = bool(cfg.zerodha_access_token)
    if args.for_live:
        if cfg.commodity_execution_broker != "zerodha":
            errors.append(f"commodity_execution_broker_not_zerodha:{cfg.commodity_execution_broker}")
        if not cfg.live_trading_enabled:
            errors.append("live_trading_enabled_false")
        if not cfg.zerodha_api_key:
            errors.append("zerodha_api_key_missing")
        if not cfg.zerodha_api_secret:
            errors.append("zerodha_api_secret_missing")
        if not cfg.zerodha_access_token:
            errors.append("zerodha_access_token_missing")

    try:
        path, refreshed, reason = ensure_zerodha_instruments_csv(
            cfg.zerodha_instruments_csv,
            cfg.zerodha_instruments_url,
            cfg.zerodha_instruments_max_age_hours,
            force=bool(args.refresh_instruments),
        )
        report["zerodha_instrument_master"] = {
            "path": str(path),
            "refreshed": refreshed,
            "refresh_reason": reason,
            "exists": path.exists(),
        }
        for asset in assets:
            detail: dict[str, Any] = {
                "underlying": asset.underlying,
                "exchange": asset.exchange,
                "segment": asset.segment,
                "configured_expiries": list(asset.expiry_dates),
            }
            contracts = load_zerodha_mcx_options(path, asset)
            by_expiry = (
                contracts.groupby(contracts["expiry"].astype(str))["tradingsymbol"]
                .nunique()
                .astype(int)
                .to_dict()
            )
            detail["contracts"] = int(len(contracts))
            detail["contracts_by_expiry"] = by_expiry
            detail["lot_sizes"] = sorted({int(x) for x in contracts["lot_size"].dropna().unique()})[:10]
            detail["tick_sizes"] = sorted({float(x) for x in contracts["tick_size"].dropna().unique()})[:10]
            for expiry in asset.expiry_dates:
                expiry_key = str(pd.to_datetime(expiry).date())
                if int(by_expiry.get(expiry_key, 0)) <= 0:
                    errors.append(f"zerodha_instrument_expiry_missing:{asset.asset_id}:{expiry_key}")
            report["asset_checks"][asset.asset_id] = detail
    except Exception as exc:
        errors.append(f"zerodha_instrument_master_failed:{exc}")

    if args.online:
        try:
            adapter = ZerodhaAdapter(cfg.zerodha_api_key, cfg.zerodha_api_secret, cfg.zerodha_access_token)
            checks["zerodha_adapter"] = True
        except Exception as exc:
            checks["zerodha_adapter"] = False
            errors.append(f"zerodha_adapter_failed:{exc}")
            print(json.dumps(report, indent=2, default=str))
            raise SystemExit(1)
        if cfg.zerodha_access_token:
            try:
                profile = adapter.profile()
                checks["zerodha_profile"] = True
                report["profile"] = {
                    "user_id": profile.get("user_id"),
                    "user_name": profile.get("user_name"),
                    "email_present": bool(profile.get("email")),
                    "broker": profile.get("broker"),
                }
            except Exception as exc:
                checks["zerodha_profile"] = False
                errors.append(f"zerodha_profile_failed:{exc}")
            try:
                margins = adapter.margins()
                checks["zerodha_margins"] = True
                report["margin_segments"] = sorted(margins.keys()) if isinstance(margins, dict) else str(type(margins))
            except Exception as exc:
                checks["zerodha_margins"] = False
                errors.append(f"zerodha_margins_failed:{exc}")
            try:
                orders = adapter.orders()
                checks["zerodha_orders"] = True
                report["orders_count"] = len(orders)
            except Exception as exc:
                checks["zerodha_orders"] = False
                errors.append(f"zerodha_orders_failed:{exc}")
        else:
            checks["zerodha_online_skipped"] = "access_token_missing"

    print(json.dumps(report, indent=2, default=str))
    if errors:
        raise SystemExit(1)


def main() -> None:
    setup_logging()
    parser = argparse.ArgumentParser(description="Multi-asset option-only Groww bot")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("inspect-contracts")
    p.add_argument("--path", default=None)
    p.set_defaults(func=cmd_inspect_contracts)

    p = sub.add_parser("collect-once")
    p.add_argument("--asset", action="append", default=None, help="Asset id. Repeat or comma-separate. Defaults to active_asset_ids.")
    p.add_argument("--expiry", action="append", default=None, help="YYYY-MM-DD. Repeat or comma-separate. Required unless bot/config.py expiry_dates is set.")
    p.add_argument("--no-quotes", action="store_true")
    p.set_defaults(func=cmd_collect_once)

    p = sub.add_parser("collect-loop")
    p.add_argument("--asset", action="append", default=None, help="Asset id. Repeat or comma-separate. Defaults to active_asset_ids.")
    p.add_argument("--expiry", action="append", default=None, help="YYYY-MM-DD. Repeat or comma-separate. Required unless bot/config.py expiry_dates is set.")
    p.add_argument("--no-quotes", action="store_true")
    p.set_defaults(func=cmd_collect_loop)

    p = sub.add_parser("train")
    p.set_defaults(func=cmd_train)

    p = sub.add_parser("train-models", help="Train the full option-only model suite: return, classifier, ranker, quantiles, IV expansion.")
    p.add_argument("--asset", action="append", default=None, help="Asset id. Repeat or comma-separate. Defaults to active_asset_ids.")
    p.set_defaults(func=cmd_train_models)

    p = sub.add_parser("training-status", help="Show fast SQL training warm-up/readiness by asset.")
    p.add_argument("--asset", action="append", default=None, help="Asset id. Repeat or comma-separate. Defaults to active_asset_ids.")
    p.set_defaults(func=cmd_training_status)

    p = sub.add_parser("model-report", help="Print the latest model-suite metrics JSON.")
    p.add_argument("--asset", action="append", default=None, help="Asset id. Repeat or comma-separate. Defaults to active_asset_ids.")
    p.set_defaults(func=cmd_model_report)

    p = sub.add_parser("score-models-latest", help="Score the latest option snapshot with the model suite; no trade decision is made.")
    p.add_argument("--asset", action="append", default=None, help="Asset id. Repeat or comma-separate. Defaults to active_asset_ids.")
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--expiry", action="append", default=None, help="Optional expiry basket filter. Repeat or comma-separate.")
    p.set_defaults(func=cmd_score_models_latest)

    p = sub.add_parser("data-audit", help="Show real Groww-source rows and suspicious synthetic/dummy/fake rows.")
    p.set_defaults(func=cmd_data_audit)

    p = sub.add_parser("repair-db", help="Backfill executable bid/ask/spread fields from stored Groww depth JSON.")
    p.set_defaults(func=cmd_repair_db)

    p = sub.add_parser("groww-preflight", help="Check Groww SDK/order readiness without placing an order.")
    p.add_argument("--asset", action="append", default=None, help="Asset id for segment/exchange checks. Defaults to active_asset_ids.")
    p.add_argument("--online", action="store_true", help="Authenticate to Groww and call read-only account APIs.")
    p.add_argument("--for-live", action="store_true", help="Fail unless live execution prerequisites are enabled.")
    p.add_argument("--symbol", default=None, help="Optional FNO trading symbol for quote/depth executability check.")
    p.add_argument("--refresh-instruments", action="store_true", help="Force refresh Groww's public instrument master before checks.")
    p.set_defaults(func=cmd_groww_preflight)

    p = sub.add_parser("zerodha-login-url", help="Print Zerodha Kite login URL for generating the daily request token.")
    p.set_defaults(func=cmd_zerodha_login_url)

    p = sub.add_parser("zerodha-generate-session", help="Exchange Zerodha request_token for the daily access token.")
    p.add_argument("--request-token", default=None, help="request_token from the Zerodha redirect URL.")
    p.add_argument("--write-env", action="store_true", help="Write ZERODHA_ACCESS_TOKEN into the env file.")
    p.add_argument("--env-file", default=".env", help="Env file to update when --write-env is set.")
    p.set_defaults(func=cmd_zerodha_generate_session)

    p = sub.add_parser("zerodha-auto-session", help="Open Kite login, capture request_token callback, and write the daily access token.")
    p.add_argument("--request-token", default=None, help="Optional request_token override; skips callback capture when provided.")
    p.add_argument("--callback-host", default="127.0.0.1")
    p.add_argument("--callback-port", type=int, default=8765)
    p.add_argument("--callback-path", default="/zerodha/callback")
    p.add_argument("--timeout-seconds", type=int, default=180)
    p.add_argument("--open-browser", action="store_true", help="Open the Kite login URL in the default browser.")
    p.add_argument("--write-env", action="store_true", help="Write ZERODHA_ACCESS_TOKEN into the env file.")
    p.add_argument("--env-file", default=".env", help="Env file to update when --write-env is set.")
    p.set_defaults(func=cmd_zerodha_auto_session)

    p = sub.add_parser("zerodha-preflight", help="Check Zerodha MCX execution readiness without placing an order.")
    p.add_argument("--asset", action="append", default=None, help="Commodity asset id. Repeat or comma-separate. Defaults to active commodity assets.")
    p.add_argument("--online", action="store_true", help="Call read-only Zerodha account APIs when an access token is loaded.")
    p.add_argument("--for-live", action="store_true", help="Fail unless commodity live execution prerequisites are enabled.")
    p.add_argument("--refresh-instruments", action="store_true", help="Force refresh Zerodha MCX instrument master before checks.")
    p.set_defaults(func=cmd_zerodha_preflight)

    p = sub.add_parser("refresh-instruments", help="Download or refresh Groww's public instrument master CSV.")
    p.add_argument("--force", action="store_true")
    p.set_defaults(func=cmd_refresh_instruments)

    p = sub.add_parser("rundown", help="Print the saved run rundown, or rebuild a current one from the DB.")
    p.add_argument("--asset", action="append", default=None, help="Asset id. Repeat or comma-separate. Defaults to active_asset_ids.")
    p.add_argument("--current", action="store_true", help="Rebuild the rundown from current DB/model state before printing.")
    p.set_defaults(func=cmd_rundown)

    p = sub.add_parser("live-once")
    p.add_argument("--asset", action="append", default=None, help="Asset id. Repeat or comma-separate. Defaults to active_asset_ids.")
    p.add_argument("--expiry", action="append", default=None, help="YYYY-MM-DD. Repeat or comma-separate. Required unless bot/config.py expiry_dates is set.")
    p.add_argument("--live", action="store_true", help="Requires live_trading_enabled=True in bot/config.py too.")
    p.set_defaults(func=cmd_live_once)

    p = sub.add_parser("live-loop")
    p.add_argument("--asset", action="append", default=None, help="Asset id. Repeat or comma-separate. Defaults to active_asset_ids.")
    p.add_argument("--expiry", action="append", default=None, help="YYYY-MM-DD. Repeat or comma-separate. Required unless bot/config.py expiry_dates is set.")
    p.add_argument("--live", action="store_true", help="Requires live_trading_enabled=True in bot/config.py too.")
    p.set_defaults(func=cmd_live_loop)

    p = sub.add_parser("metrics")
    p.set_defaults(func=cmd_metrics)

    args = parser.parse_args()
    try:
        args.func(args)
    except (ValueError, TimeoutError) as exc:
        parser.exit(2, f"ERROR: {exc}\n")


if __name__ == "__main__":
    main()

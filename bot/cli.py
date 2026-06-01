from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from .collector import OptionDataCollector
from .config import load_config
from .groww_adapter import GrowwAdapter
from .logging_utils import setup_logging
from .model import OptionReturnTrainer
from .option_models import (
    OptionModelSuite,
    build_option_model_frame,
    load_option_model_raw_data,
    train_model_suite_from_store,
)
from .nse_contracts import load_nifty_options_contracts, nearest_expiry
from .storage import Store
from .live import LiveOptionBot

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


def resolve_expiry(args, cfg) -> str:
    if args.expiry:
        return args.expiry
    checked: list[str] = []
    for path in _candidate_contract_files(cfg):
        checked.append(str(path))
        if path.exists():
            df = load_nifty_options_contracts(path, cfg.underlying)
            exp = str(nearest_expiry(df, cfg.expiry_index))
            log.info("auto_resolved_expiry=%s from_contract_file=%s", exp, path)
            return exp
    raise ValueError(
        "Pass --expiry YYYY-MM-DD or provide a readable NSE contract file. "
        f"Checked: {checked}"
    )


def cmd_collect_once(args) -> None:
    cfg = load_config()
    store = Store(cfg.db_path)
    adapter = GrowwAdapter(cfg.groww_totp_token, cfg.groww_totp_secret)
    expiry = resolve_expiry(args, cfg)
    collector = OptionDataCollector(cfg, store, adapter)
    collector.collect_option_chain_once(expiry)


def cmd_collect_loop(args) -> None:
    cfg = load_config()
    store = Store(cfg.db_path)
    adapter = GrowwAdapter(cfg.groww_totp_token, cfg.groww_totp_secret)
    expiry = resolve_expiry(args, cfg)
    OptionDataCollector(cfg, store, adapter).collect_loop(expiry, quote_top_symbols=not args.no_quotes)


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
    result = train_model_suite_from_store(cfg, store)
    print(json.dumps(result.metrics, indent=2, default=str))
    print(f"Saved model suite: {result.model_path}")
    print(f"Saved suite meta : {result.meta_path}")


def cmd_model_report(args) -> None:
    cfg = load_config()
    path = cfg.model_suite_meta_path
    if not path.exists():
        print("No model-suite meta found. Run train-models first.")
        return
    print(path.read_text())


def cmd_score_models_latest(args) -> None:
    cfg = load_config()
    store = Store(cfg.db_path)
    if not cfg.model_suite_path.exists():
        raise FileNotFoundError(f"Model suite missing: {cfg.model_suite_path}. Run train-models first.")
    raw = load_option_model_raw_data(cfg, store)
    if raw.empty:
        print("No option snapshots available in DB. Run collect-loop first.")
        return
    frame = build_option_model_frame(raw, horizon_rows=cfg.label_horizon_rows, cost_bps=cfg.estimated_round_trip_cost_bps)
    latest_ts = pd.to_datetime(frame["ts"]).max()
    latest = frame[pd.to_datetime(frame["ts"]).eq(latest_ts)].copy()
    suite = OptionModelSuite.load(cfg.model_suite_path)
    scored = suite.predict(latest).sort_values("model_score", ascending=False).head(args.limit)
    cols = [
        "ts", "trading_symbol", "expiry", "strike", "option_type", "ltp",
        "predicted_return", "prob_profit", "prob_iv_expansion", "rank_score",
        "return_q20", "return_q50", "return_q80", "estimated_cost", "model_score", "model_rank_at_ts",
    ]
    print(scored[[c for c in cols if c in scored.columns]].to_string(index=False))



def cmd_data_audit(args) -> None:
    cfg = load_config()
    store = Store(cfg.db_path)
    report: dict[str, object] = {}
    for table in ("option_chain_snapshots", "quote_snapshots"):
        try:
            by_source = store.query_df(f"SELECT coalesce(source, 'UNTAGGED') AS source, count(*) AS rows FROM {table} GROUP BY 1 ORDER BY rows DESC")
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
        "live_train_min_rows": cfg.live_train_min_rows,
    }
    print(json.dumps(report, indent=2, default=str))

def cmd_live_once(args) -> None:
    cfg = load_config()
    if args.live:
        # Two-key live switch: CLI --live + live_trading_enabled=True in bot/config.py.
        object.__setattr__(cfg, "paper_trading", False)
    store = Store(cfg.db_path)
    adapter = GrowwAdapter(cfg.groww_totp_token, cfg.groww_totp_secret)
    expiry = resolve_expiry(args, cfg)
    bot = LiveOptionBot(cfg, store, adapter)
    result = bot.trade_once(expiry)
    print(json.dumps(result, indent=2, default=str))


def cmd_live_loop(args) -> None:
    cfg = load_config()
    if args.live:
        object.__setattr__(cfg, "paper_trading", False)
    store = Store(cfg.db_path)
    adapter = GrowwAdapter(cfg.groww_totp_token, cfg.groww_totp_secret)
    expiry = resolve_expiry(args, cfg)
    bot = LiveOptionBot(cfg, store, adapter)
    while True:
        try:
            result = bot.trade_once(expiry)
            log.info("decision=%s", result.get("decision"))
        except KeyboardInterrupt:
            raise
        except Exception:
            log.exception("live loop error")
        time.sleep(cfg.option_chain_interval_seconds)


def cmd_metrics(args) -> None:
    cfg = load_config()
    if not cfg.model_meta_path.exists():
        print("No model meta found. Run train first.")
        return
    print(cfg.model_meta_path.read_text())


def main() -> None:
    setup_logging()
    parser = argparse.ArgumentParser(description="NIFTY option-only Groww bot")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("inspect-contracts")
    p.add_argument("--path", default=None)
    p.set_defaults(func=cmd_inspect_contracts)

    p = sub.add_parser("collect-once")
    p.add_argument("--expiry", default=None, help="YYYY-MM-DD. If omitted, uses nearest expiry from cfg.nse_contract_file.")
    p.set_defaults(func=cmd_collect_once)

    p = sub.add_parser("collect-loop")
    p.add_argument("--expiry", default=None)
    p.add_argument("--no-quotes", action="store_true")
    p.set_defaults(func=cmd_collect_loop)

    p = sub.add_parser("train")
    p.set_defaults(func=cmd_train)

    p = sub.add_parser("train-models", help="Train the full option-only model suite: return, classifier, ranker, quantiles, IV expansion.")
    p.set_defaults(func=cmd_train_models)

    p = sub.add_parser("model-report", help="Print the latest model-suite metrics JSON.")
    p.set_defaults(func=cmd_model_report)

    p = sub.add_parser("score-models-latest", help="Score the latest option snapshot with the model suite; no trade decision is made.")
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_score_models_latest)

    p = sub.add_parser("data-audit", help="Show real Groww-source rows and suspicious synthetic/dummy/fake rows.")
    p.set_defaults(func=cmd_data_audit)

    p = sub.add_parser("live-once")
    p.add_argument("--expiry", default=None)
    p.add_argument("--live", action="store_true", help="Requires live_trading_enabled=True in bot/config.py too.")
    p.set_defaults(func=cmd_live_once)

    p = sub.add_parser("live-loop")
    p.add_argument("--expiry", default=None)
    p.add_argument("--live", action="store_true", help="Requires live_trading_enabled=True in bot/config.py too.")
    p.set_defaults(func=cmd_live_loop)

    p = sub.add_parser("metrics")
    p.set_defaults(func=cmd_metrics)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

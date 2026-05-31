from __future__ import annotations

import argparse
import gzip
import json
import logging
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from .config import Settings, load_delta_credentials
from .delta_ws import DeltaWebSocketRuntime
from .engine import FullBTCStrategyEngine
from .execution import DeltaRestClient
from .offline import train_bootstrap_tradeflow, inspect_tradeflow_file
from .telemetry import summarize_live_dir, print_human
from .features import FEATURE_COLUMNS
from .models import LiveLearningModelStack
from .costs import CostModel


def _iter_jsonl(path: Path):
    if not path.exists():
        return
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def warmstart_from_live_artifacts(settings: Settings, live_dir: str | Path, *, max_rows: int | None = None, reset: bool = False) -> dict[str, Any]:
    """Replay existing real live feature rows into the online model stack and checkpoint it."""
    live = Path(live_dir)
    feature_path = live / "features.jsonl.gz"
    model_dir = live / "models"
    model_dir.mkdir(parents=True, exist_ok=True)
    if not feature_path.exists():
        raise FileNotFoundError(f"feature journal not found: {feature_path}")

    models = LiveLearningModelStack(
        settings.label_horizons_ms,
        min_labels_to_score=settings.min_labels_to_score,
        model_dir=model_dir,
        auto_promote=settings.auto_promote_model,
        rolling_window=settings.promotion_rolling_window,
        min_promotion_evals=settings.min_promotion_evals,
        min_eligible_predictions_for_promotion=settings.min_eligible_predictions_for_promotion,
        min_eligible_rate_for_promotion=settings.min_eligible_rate_for_promotion,
    )
    if not reset:
        models.restore_checkpoint()
    models.maybe_load_bootstrap(settings.bootstrap_tradeflow_model, settings.bootstrap_tradeflow_manifest)

    costs = CostModel(
        taker_fee_bps_pre_gst=settings.taker_fee_bps_pre_gst,
        maker_fee_bps_pre_gst=settings.maker_fee_bps_pre_gst,
        gst_rate=settings.gst_rate,
        impact_floor_bps=settings.impact_floor_bps,
        min_real_fills=settings.min_real_fill_count_for_cost_model,
        ledger_path=live / "rest_fills.jsonl",
        execution_cost_profile=settings.execution_cost_profile,
        hyperliquid_taker_fee_bps=settings.hyperliquid_taker_fee_bps,
        hyperliquid_maker_fee_bps=settings.hyperliquid_maker_fee_bps,
        hyperliquid_impact_floor_bps=settings.hyperliquid_impact_floor_bps,
    )

    rows_seen = rows_used = rows_skipped = 0
    first_ns = last_ns = None
    for row in _iter_jsonl(feature_path):
        if row.get("type") == "metadata":
            continue
        rows_seen += 1
        if max_rows is not None and rows_used >= max_rows:
            break
        try:
            mid = float(row["mid"])
            ts_ns = int(row.get("recorded_at_ns") or row.get("ts_ns") or row.get("receive_ts_ns"))
            feat = row.get("features") or {}
            feature_row = {c: float(feat.get(c, 0.0) or 0.0) for c in FEATURE_COLUMNS}
            cost_row = row.get("costs") if isinstance(row.get("costs"), dict) else {}
            if cost_row.get("round_trip_bps") is not None:
                cost_bps = float(cost_row["round_trip_bps"])
            else:
                cost_bps = costs.round_trip_bps(float(row.get("spread_bps") or feature_row.get("spread_bps") or 0.0))
        except Exception:
            rows_skipped += 1
            continue
        models.learn_matured(ts_ns, mid)
        models.observe_decision(ts_ns, mid, feature_row, cost_bps)
        rows_used += 1
        first_ns = ts_ns if first_ns is None else first_ns
        last_ns = ts_ns
        if rows_used % 50000 == 0:
            models.checkpoint()
    models.checkpoint()
    report = {
        "type": "warmstart_replay_report_v5_8",
        "live_dir": str(live),
        "feature_journal": str(feature_path),
        "rows_seen": rows_seen,
        "rows_used": rows_used,
        "rows_skipped": rows_skipped,
        "first_recorded_at_ns": first_ns,
        "last_recorded_at_ns": last_ns,
        "checkpoint_path": str(models.checkpoint_path()),
        "model_status": models.status(cost_bps=costs.snapshot().get("round_trip_bps", 0.0)),
    }
    (model_dir / "warmstart_replay_report.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    return report


def main() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    p = argparse.ArgumentParser(prog="btc-live-hft", description="Full Delta BTCUSD live-learning HFT strategy stack")
    sub = p.add_subparsers(dest="cmd", required=True)

    ins = sub.add_parser("inspect-tradeflow", help="Inspect a Delta public trades CSV/ZIP for usable order-flow fields")
    ins.add_argument("file")
    ins.add_argument("--out", default=None, help="Optional override; default comes from btchft/config.py")

    boot = sub.add_parser("train-bootstrap-tradeflow", help="Train real-data bootstrap trade-flow model from public trades")
    boot.add_argument("file")
    boot.add_argument("--out-model", default=None, help="Optional override; default comes from btchft/config.py")
    boot.add_argument("--out-manifest", default=None, help="Optional override; default comes from btchft/config.py")
    boot.add_argument("--horizon-seconds", type=int, default=None, help="Optional override; default comes from btchft/config.py")

    run = sub.add_parser("run", help="Run SHADOW/PAPER/LIVE bot according to btchft/config.py")
    run.add_argument("--status-every", type=int, default=None, help="Optional override; default comes from btchft/config.py")
    run.add_argument("--offline-replay-trades", help="Optional public trade CSV/ZIP replay for test/training without websocket")

    mon = sub.add_parser("inspect-live", help="Explain what data, models, tests, signals and gates are active in a live artifacts directory")
    mon.add_argument("--live-dir", default="artifacts/live")
    mon.add_argument("--tail-rows", type=int, default=5000)
    mon.add_argument("--json", action="store_true")

    warm = sub.add_parser("warmstart-live", help="Replay existing real features.jsonl.gz into the online model stack and checkpoint it before running")
    warm.add_argument("--live-dir", default="artifacts/live")
    warm.add_argument("--max-rows", type=int, default=None)
    warm.add_argument("--reset", action="store_true", help="Ignore any existing model checkpoint and rebuild from the feature journal")
    warm.add_argument("--json", action="store_true")

    args = p.parse_args()
    s = Settings(); s.validate()
    if args.cmd == "inspect-tradeflow":
        out = Path(args.out) if args.out else s.tradeflow_inspection_out
        report = inspect_tradeflow_file(args.file)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        print(json.dumps(report, indent=2, default=str))
        return
    if args.cmd == "train-bootstrap-tradeflow":
        out_model = Path(args.out_model) if args.out_model else s.bootstrap_out_model
        out_manifest = Path(args.out_manifest) if args.out_manifest else s.bootstrap_out_manifest
        horizon_seconds = int(args.horizon_seconds or s.bootstrap_horizon_seconds)
        report = train_bootstrap_tradeflow(args.file, out_model, out_manifest, horizon_seconds=horizon_seconds)
        print(json.dumps(report, indent=2, default=str))
        return
    if args.cmd == "inspect-live":
        summary = summarize_live_dir(args.live_dir, tail_rows=args.tail_rows)
        if args.json:
            print(json.dumps(summary, indent=2, default=str))
        else:
            print(print_human(summary))
        return
    if args.cmd == "warmstart-live":
        report = warmstart_from_live_artifacts(s, args.live_dir, max_rows=args.max_rows, reset=args.reset)
        if args.json:
            print(json.dumps(report, indent=2, default=str))
        else:
            print("WARMSTART COMPLETE")
            print(f"live_dir={report['live_dir']}")
            print(f"rows_used={report['rows_used']} rows_skipped={report['rows_skipped']}")
            print(f"checkpoint_path={report['checkpoint_path']}")
            ms = report['model_status']
            print(f"matured_labels={ms.get('matured_labels')} total_prequential={ms.get('prequential_count_total')} eligible={ms.get('eligible_prediction_total')} promoted={ms.get('promoted_horizon_ms')}")
        return
    if args.cmd == "run":
        engine = FullBTCStrategyEngine(s)
        try:
            if args.offline_replay_trades:
                from .offline import replay_trades_into_engine
                replay_trades_into_engine(args.offline_replay_trades, engine)
                print(json.dumps(engine.status(), indent=2, default=str))
                return
            api_key, secret_key = load_delta_credentials()
            api = DeltaRestClient(api_key, secret_key, testnet=s.delta_testnet)
            try:
                product = api.get_product(s.delta_symbol)
                engine.configure_product(product)
            except Exception as e:
                logging.getLogger(__name__).warning("Product preflight failed; using safe BTCUSD defaults: %s", e)
            ws = DeltaWebSocketRuntime(
                symbol=s.delta_symbol,
                api_key=api_key,
                secret_key=secret_key,
                testnet=s.delta_testnet,
                on_public=engine.on_public_message,
                on_private=engine.on_private_message,
            )
            ws.start()
            last = 0.0
            while True:
                now = time.time()
                status_every = int(args.status_every or s.status_every_seconds)
                if now - last >= status_every:
                    print(json.dumps(engine.status(), indent=2, default=str))
                    last = now
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            try:
                ws.stop()  # type: ignore[name-defined]
            except Exception:
                pass
            engine.close()


if __name__ == "__main__":
    main()

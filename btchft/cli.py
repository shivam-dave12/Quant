from __future__ import annotations

import argparse
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

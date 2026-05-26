"""Direct diagnostic/operator runner; production container lifecycle is owned by Telegram."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging

from dashboard.server import DashboardServer
from orchestration.bootstrap import RuntimeBootstrap


async def run_direct(*, mode: str, configuration_module: str, dashboard: bool, host: str, port: int) -> None:
    """Explicit direct runner retained for offline/paper diagnostics outside Telegram deployment."""
    bundle = RuntimeBootstrap().build_runtime(mode=mode, configuration_module=configuration_module)
    tasks = [asyncio.create_task(bundle.coordinator.run(live=mode == "live"))]
    server: DashboardServer | None = None
    if dashboard:
        server = DashboardServer(bundle.platform, host=host, port=port)
        tasks.append(asyncio.create_task(server.run()))
    try:
        await asyncio.gather(*tasks)
    finally:
        if server:
            server.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser(description="Direct institutional runtime diagnostics; production uses Telegram controller")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--mode", choices=("shadow", "paper", "live"), default="shadow")
    parser.add_argument("--configuration-module")
    parser.add_argument("--dashboard", action="store_true")
    parser.add_argument("--dashboard-host", default="127.0.0.1")
    parser.add_argument("--dashboard-port", type=int, default=8088)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    bootstrap = RuntimeBootstrap()
    if args.status:
        print(json.dumps(bootstrap.preflight_status(), indent=2, default=str))
        return
    if not args.configuration_module:
        raise RuntimeError("VERIFIED_STREAM_CONFIGURATION_MODULE_REQUIRED")
    asyncio.run(
        run_direct(
            mode=args.mode,
            configuration_module=args.configuration_module,
            dashboard=args.dashboard,
            host=args.dashboard_host,
            port=args.dashboard_port,
        )
    )


if __name__ == "__main__":
    main()

"""Institutional multi-desk platform entrypoint. Missing evidence blocks execution; no legacy route exists."""
from __future__ import annotations
import argparse, asyncio, importlib, json, logging, os
from pathlib import Path
from core.config import CONFIG
from core.observability import Observability
from adapters.delta.client import DeltaAdapter
from adapters.groww.client import GrowwAdapter
from adapters.coinswitch.reference import CoinSwitchReferenceAdapter
from adapters.hyperliquid.reference import HyperliquidReferenceAdapter
from adapters.reference_metals.client import ReferenceMetalsAdapter
from orchestration.model_loader import PromotedAuthorityLoader
from orchestration.platform import InstitutionalPlatform
from orchestration.live_factory import VerifiedLiveFeedFactory
from orchestration.runtime import LiveCoordinator
from telegram.controller import TelegramController
from dashboard.server import DashboardServer

def preflight_status() -> dict[str, object]:
    required=("btc_model_bundle.joblib","liquidity_model_bundle.joblib","metals_model_bundle.joblib","india_model_bundle.joblib","option_iv_change_model.joblib","covariance.joblib","expected_shortfall.joblib")
    model_dir=Path(CONFIG.policy.model_dir); missing=[name for name in required if not (model_dir/name).exists()]
    registry_ready=(model_dir / "model_registry.json").exists()
    return {"live_flags":{"delta":CONFIG.policy.delta_live_orders_enabled,"metals":CONFIG.policy.metals_live_orders_enabled,"groww":CONFIG.policy.groww_live_orders_enabled},"promoted_artifacts_ready":not missing and registry_ready,"missing_artifacts":missing,"promoted_registry_ready":registry_ready,"metals_reference_provider_configured":bool(os.getenv("METALS_REFERENCE_PROVIDER_MODULE")),"verified_stream_configuration_required":True}

def _approved_metals_provider() -> object | None:
    module_name=os.getenv("METALS_REFERENCE_PROVIDER_MODULE", "")
    if not module_name: return None
    module=importlib.import_module(module_name); return getattr(module,"build_provider")()

def build_platform() -> InstitutionalPlatform:
    models,covariance,es=PromotedAuthorityLoader(CONFIG.policy.model_dir).load(); groww=None; delta=None
    if CONFIG.secrets.groww_access_token or CONFIG.secrets.groww_api_key:
        groww=GrowwAdapter.from_secrets(CONFIG.secrets,live_orders_enabled=CONFIG.policy.groww_live_orders_enabled,approved_static_ips=CONFIG.policy.groww_approved_static_outbound_ips)
    if CONFIG.secrets.delta_api_key and CONFIG.secrets.delta_secret_key:
        delta=DeltaAdapter.with_signed_native_brackets(CONFIG.secrets.delta_api_key,CONFIG.secrets.delta_secret_key)
    platform=InstitutionalPlatform(CONFIG,models=models,covariance=covariance,expected_shortfall=es,observability=Observability(),groww_adapter=groww,delta_adapter=delta); platform.reconcile_groww_startup(); return platform

async def run_services(platform: InstitutionalPlatform, coordinator: LiveCoordinator, *, live: bool, telegram: bool, dashboard: bool, host: str, port: int) -> None:
    services=[coordinator.run(live=live)]
    server=None
    if telegram: services.append(TelegramController(platform=platform,token=CONFIG.secrets.telegram_bot_token,chat_id=CONFIG.secrets.telegram_chat_id).run())
    if dashboard:
        server=DashboardServer(platform,host=host,port=port); services.append(server.run())
    try: await asyncio.gather(*services)
    finally:
        if server: server.shutdown()

def main() -> None:
    parser=argparse.ArgumentParser(description="Institutional multi-desk strategy runtime")
    parser.add_argument("--status",action="store_true"); parser.add_argument("--mode",choices=("shadow","paper","live"),default="shadow")
    parser.add_argument("--configuration-module",help="Module exposing configure_streams(factory, coordinator, platform, mode)")
    parser.add_argument("--telegram",action="store_true"); parser.add_argument("--dashboard",action="store_true"); parser.add_argument("--dashboard-host",default="127.0.0.1"); parser.add_argument("--dashboard-port",type=int,default=8088)
    args=parser.parse_args(); logging.basicConfig(level=logging.INFO,format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    if args.status: print(json.dumps(preflight_status(),indent=2,default=str)); return
    if not args.configuration_module: raise RuntimeError("VERIFIED_STREAM_CONFIGURATION_MODULE_REQUIRED")
    platform=build_platform(); delta=platform.delta_executor.adapter if platform.delta_executor else DeltaAdapter(); provider=_approved_metals_provider()
    factory=VerifiedLiveFeedFactory(delta=delta,coinswitch=CoinSwitchReferenceAdapter(api_key=CONFIG.secrets.coinswitch_api_key,secret_key=CONFIG.secrets.coinswitch_secret_key) if CONFIG.secrets.coinswitch_api_key else None,hyperliquid=HyperliquidReferenceAdapter("BTC"),metals_reference=ReferenceMetalsAdapter(provider) if provider else None,groww=platform.groww_executor.adapter if platform.groww_executor else None)
    coordinator=LiveCoordinator(platform); importlib.import_module(args.configuration_module).configure_streams(factory,coordinator,platform,args.mode)
    if args.mode == "live" and not any((CONFIG.policy.delta_live_orders_enabled,CONFIG.policy.metals_live_orders_enabled,CONFIG.policy.groww_live_orders_enabled)): raise RuntimeError("LIVE_MODE_REQUIRES_EXPLICIT_LIVE_POLICY_PROMOTION")
    asyncio.run(run_services(platform,coordinator,live=args.mode=="live",telegram=args.telegram,dashboard=args.dashboard,host=args.dashboard_host,port=args.dashboard_port))
if __name__ == "__main__": main()

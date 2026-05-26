"""Builds the institutional runtime only after an authorised lifecycle request."""
from __future__ import annotations

from dataclasses import dataclass
import importlib
import os
from pathlib import Path
from typing import Any

from adapters.coinswitch.reference import CoinSwitchReferenceAdapter
from adapters.delta.client import DeltaAdapter
from adapters.groww.client import GrowwAdapter
from adapters.hyperliquid.reference import HyperliquidReferenceAdapter
from adapters.reference_metals.client import ReferenceMetalsAdapter
from core.config import CONFIG, PlatformConfig
from core.environment import environment_diagnostics
from core.observability import Observability
from orchestration.live_factory import VerifiedLiveFeedFactory
from orchestration.model_loader import PromotedAuthorityLoader
from orchestration.platform import InstitutionalPlatform
from orchestration.runtime import LiveCoordinator


@dataclass(frozen=True)
class RuntimeBundle:
    """Fully wired runtime returned only after configuration validation succeeds."""

    platform: InstitutionalPlatform
    coordinator: LiveCoordinator
    mode: str


class RuntimeBootstrap:
    """Fail-closed runtime constructor used by Telegram and direct diagnostics."""

    REQUIRED_ARTIFACTS = (
        "btc_model_bundle.joblib",
        "liquidity_model_bundle.joblib",
        "metals_model_bundle.joblib",
        "india_model_bundle.joblib",
        "option_iv_change_model.joblib",
        "covariance.joblib",
        "expected_shortfall.joblib",
    )

    def __init__(self, config: PlatformConfig = CONFIG) -> None:
        self.config = config

    def preflight_status(self) -> dict[str, object]:
        model_dir = Path(self.config.policy.model_dir)
        missing = [name for name in self.REQUIRED_ARTIFACTS if not (model_dir / name).exists()]
        registry_ready = (model_dir / "model_registry.json").exists()
        return {
            "live_flags": {
                "delta": self.config.policy.delta_live_orders_enabled,
                "metals": self.config.policy.metals_live_orders_enabled,
                "groww": self.config.policy.groww_live_orders_enabled,
            },
            "promoted_artifacts_ready": not missing and registry_ready,
            "missing_artifacts": missing,
            "promoted_registry_ready": registry_ready,
            "metals_reference_provider_configured": bool(os.getenv("METALS_REFERENCE_PROVIDER_MODULE")),
            "verified_stream_configuration_required": True,
            "credential_environment": environment_diagnostics((
                "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID",
                "GROWW_ACCESS_TOKEN", "GROWW_API_KEY",
                "DELTA_API_KEY", "DELTA_SECRET_KEY",
            )),
        }

    @staticmethod
    def _approved_metals_provider() -> object | None:
        module_name = os.getenv("METALS_REFERENCE_PROVIDER_MODULE", "")
        if not module_name:
            return None
        module = importlib.import_module(module_name)
        return getattr(module, "build_provider")()

    def build_platform(self) -> InstitutionalPlatform:
        models, covariance, expected_shortfall = PromotedAuthorityLoader(self.config.policy.model_dir).load()
        groww: GrowwAdapter | None = None
        delta: DeltaAdapter | None = None
        if self.config.secrets.groww_access_token or self.config.secrets.groww_api_key:
            groww = GrowwAdapter.from_secrets(
                self.config.secrets,
                live_orders_enabled=self.config.policy.groww_live_orders_enabled,
                approved_static_ips=self.config.policy.groww_approved_static_outbound_ips,
            )
        if self.config.secrets.delta_api_key and self.config.secrets.delta_secret_key:
            delta = DeltaAdapter.with_signed_native_brackets(
                self.config.secrets.delta_api_key,
                self.config.secrets.delta_secret_key,
            )
        platform = InstitutionalPlatform(
            self.config,
            models=models,
            covariance=covariance,
            expected_shortfall=expected_shortfall,
            observability=Observability(),
            groww_adapter=groww,
            delta_adapter=delta,
        )
        platform.reconcile_groww_startup()
        return platform

    def build_runtime(self, *, mode: str, configuration_module: str) -> RuntimeBundle:
        if mode not in {"shadow", "paper", "live"}:
            raise RuntimeError("UNSUPPORTED_RUNTIME_MODE")
        if not configuration_module:
            raise RuntimeError("VERIFIED_STREAM_CONFIGURATION_MODULE_REQUIRED")
        if mode == "live" and not any(
            (
                self.config.policy.delta_live_orders_enabled,
                self.config.policy.metals_live_orders_enabled,
                self.config.policy.groww_live_orders_enabled,
            )
        ):
            raise RuntimeError("LIVE_MODE_REQUIRES_EXPLICIT_LIVE_POLICY_PROMOTION")
        platform = self.build_platform()
        delta = platform.delta_executor.adapter if platform.delta_executor else DeltaAdapter()
        provider = self._approved_metals_provider()
        factory = VerifiedLiveFeedFactory(
            delta=delta,
            coinswitch=(
                CoinSwitchReferenceAdapter(
                    api_key=self.config.secrets.coinswitch_api_key,
                    secret_key=self.config.secrets.coinswitch_secret_key,
                )
                if self.config.secrets.coinswitch_api_key
                else None
            ),
            hyperliquid=HyperliquidReferenceAdapter("BTC"),
            metals_reference=ReferenceMetalsAdapter(provider) if provider else None,
            groww=platform.groww_executor.adapter if platform.groww_executor else None,
        )
        coordinator = LiveCoordinator(platform)
        module: Any = importlib.import_module(configuration_module)
        configure_streams = getattr(module, "configure_streams", None)
        if not callable(configure_streams):
            raise RuntimeError("CONFIGURATION_MODULE_MISSING_CONFIGURE_STREAMS")
        configure_streams(factory, coordinator, platform, mode)
        if not coordinator.streams:
            raise RuntimeError("NO_VERIFIED_DESK_STREAMS_REGISTERED")
        return RuntimeBundle(platform=platform, coordinator=coordinator, mode=mode)

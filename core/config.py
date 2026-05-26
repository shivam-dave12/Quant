"""Immutable platform policy. Environment variables contain credentials only."""
from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
import os

@dataclass(frozen=True)
class Secrets:
    delta_api_key: str = field(default_factory=lambda: os.getenv("DELTA_API_KEY", ""))
    delta_secret_key: str = field(default_factory=lambda: os.getenv("DELTA_SECRET_KEY", ""))
    groww_access_token: str = field(default_factory=lambda: os.getenv("GROWW_ACCESS_TOKEN", ""))
    groww_api_key: str = field(default_factory=lambda: os.getenv("GROWW_API_KEY", ""))
    groww_api_secret: str = field(default_factory=lambda: os.getenv("GROWW_API_SECRET", ""))
    groww_totp_secret: str = field(default_factory=lambda: os.getenv("GROWW_TOTP_SECRET", ""))
    coinswitch_api_key: str = field(default_factory=lambda: os.getenv("COINSWITCH_API_KEY", ""))
    coinswitch_secret_key: str = field(default_factory=lambda: os.getenv("COINSWITCH_SECRET_KEY", ""))
    telegram_bot_token: str = field(default_factory=lambda: os.getenv("TELEGRAM_BOT_TOKEN", ""))
    telegram_chat_id: str = field(default_factory=lambda: os.getenv("TELEGRAM_CHAT_ID", ""))

@dataclass(frozen=True)
class PlatformPolicy:
    groww_live_orders_enabled: bool = False
    delta_live_orders_enabled: bool = False
    metals_live_orders_enabled: bool = False
    coinswitch_reference_only: bool = True
    hyperliquid_reference_only: bool = True
    enabled_india_underlyings: tuple[str, ...] = ("NIFTY",)
    enable_banknifty_after_validation: bool = False
    delta_btc_symbol: str = "BTCUSD"
    groww_approved_static_outbound_ips: tuple[str, ...] = ()
    validated_costs_required: bool = True
    protected_execution_required: bool = True
    trained_model_promotion_required: bool = True
    minimum_net_edge_bps: float = 2.0
    minimum_execution_quality: float = 0.55
    minimum_feed_quality: float = 0.50
    maximum_stop_sweep_probability: float = 0.38
    minimum_tp_before_sl_probability: float = 0.53
    portfolio_risk_cap_usd: float = 20.0
    portfolio_es_cap_usd: float = 25.0
    max_risk_per_opportunity_usd: float = 4.0
    desk_daily_loss_caps_usd: dict[str, float] = field(default_factory=lambda: {"BTC": 8.0, "METALS": 6.0})
    india_portfolio_risk_cap_inr: float = 5000.0
    india_max_risk_per_opportunity_inr: float = 1500.0
    india_daily_loss_cap_inr: float = 3000.0
    max_option_premium_risk_inr: float = 1500.0
    max_option_holding_minutes: int = 60
    max_option_spread_bps: float = 125.0
    minimum_option_liquidity_score: float = 0.35
    liquidity_bands_bps: tuple[tuple[float, float], ...] = ((0.0, 1.0), (1.0, 3.0), (3.0, 10.0), (10.0, 25.0))
    forecast_horizons_seconds: tuple[int, ...] = (1, 10, 60, 300)
    hmm_states: tuple[str, ...] = ("balance", "trend", "expansion", "shock", "illiquid")
    ewma_lambda: float = 0.94
    covariance_lambda: float = 0.94
    es_alpha: float = 0.975
    state_dir: Path = Path("data/state")
    research_dir: Path = Path("data/research")
    model_dir: Path = Path("data/models")
    log_dir: Path = Path("logs")

@dataclass(frozen=True)
class PlatformConfig:
    policy: PlatformPolicy = field(default_factory=PlatformPolicy)
    secrets: Secrets = field(default_factory=Secrets)
    def ensure_directories(self) -> None:
        for path in (self.policy.state_dir, self.policy.research_dir, self.policy.model_dir, self.policy.log_dir):
            path.mkdir(parents=True, exist_ok=True)

CONFIG = PlatformConfig()

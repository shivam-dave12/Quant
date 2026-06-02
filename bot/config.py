from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# .env is intentionally secrets-only. Runtime/model/risk settings live here so
# deployments are explicit and auditable.
load_dotenv()


@dataclass(frozen=True)
class AssetProfile:
    asset_id: str
    label: str
    underlying: str
    exchange: str
    segment: str
    expiry_dates: tuple[str, ...]
    model_strategy: str
    option_chain_mode: str = "groww_option_chain"
    product: str = "MIS"
    enabled: bool = True
    min_ltp: float = 1.0
    max_ltp: float = 5000.0
    min_volume: float = 1.0
    min_oi: float = 1.0
    max_quote_symbols: int = 40
    max_spread_pct: float = 0.025
    min_edge_return: float = 0.006
    risk_per_trade_pct: float = 0.005
    max_premium_value_per_trade: float = 10000.0
    min_backtest_trades: int | None = None
    allow_short_option_entries: bool = False
    short_tp_pct: float = 0.01
    short_sl_pct: float = 0.025
    tp_pct: float = 0.18
    sl_pct: float = 0.09
    session_timezone: str = "Asia/Kolkata"
    session_open: str = "09:15"
    session_close: str = "15:30"
    late_session_start: str = "15:30"
    trading_weekdays: tuple[int, ...] = (0, 1, 2, 3, 4)


@dataclass(frozen=True)
class BotConfig:
    # Secrets / credentials.
    groww_totp_token: str = os.getenv("GROWW_TOTP_TOKEN", "")
    groww_totp_secret: str = os.getenv("GROWW_TOTP_SECRET", "")

    # Runtime mode. Live orders still require CLI --live plus this switch.
    live_trading_enabled: bool = True
    paper_trading: bool = False

    # Files / artifacts.
    db_path: Path = Path("data/nifty_option_bot.duckdb")
    nse_contract_file: Path = Path("assets/NSE_FO_contract_29052026.csv.gz")
    groww_instruments_csv: Path = Path("data/raw/groww_instruments.csv")
    groww_instruments_url: str = "https://growwapi-assets.groww.in/instruments/instrument.csv"
    groww_instruments_max_age_hours: float = 24.0
    run_rundown_path: Path = Path("data/runtime/run_rundown.json")
    model_path: Path = Path("models/option_return_model.joblib")
    model_meta_path: Path = Path("models/model_meta.json")
    model_suite_path: Path = Path("models/option_model_suite.joblib")
    model_suite_meta_path: Path = Path("models/model_suite_meta.json")

    # Legacy defaults for single-asset code paths. New code uses asset_profiles.
    underlying: str = "NIFTY"
    max_strikes_each_side: int = 12
    expiry_dates: tuple[str, ...] = ("2026-06-09", "2026-06-16", "2026-06-23")
    expiry_index: int = 0
    min_ltp: float = 3.0
    max_ltp: float = 450.0
    min_volume: float = 20.0
    min_oi: float = 100.0

    # Data collection.
    option_chain_interval_seconds: int = 5
    max_quote_symbols: int = 40
    groww_quote_delay_seconds: float = 0.25
    groww_quote_rate_limit_backoff_seconds: float = 4.0
    groww_quote_retry_attempts: int = 2

    # Training / labels.
    label_horizon_rows: int = 12
    estimated_round_trip_cost_bps: float = 35.0
    model_suite_shadow_min_rows: int = 200
    min_train_rows: int = 5000
    test_fraction: float = 0.25
    train_status_log_interval_seconds: int = 60

    # Live edge/model gates.
    min_edge_return: float = 0.006
    uncertainty_buffer: float = 0.003
    min_model_win_rate: float = 0.54
    min_model_sharpe: float = 0.60
    min_meta_policy_prob: float = 0.55
    min_meta_policy_ev: float = 0.0
    min_meta_policy_candidates: int = 200
    min_meta_policy_class_count: int = 25
    min_backtest_trades: int = 80
    auto_train_enabled: bool = True
    require_groww_source: bool = True
    live_train_min_rows: int = 5000

    # Risk / execution.
    account_capital: float = 100000.0
    risk_per_trade_pct: float = 0.005
    max_premium_value_per_trade: float = 10000.0
    max_open_positions: int = 3
    entry_order_type: str = "LIMIT"
    product: str = "MIS"
    max_spread_pct: float = 0.025
    entry_tick_buffer: int = 1
    require_live_margin_check: bool = True
    live_margin_buffer_pct: float = 0.05
    tp_pct: float = 0.18
    sl_pct: float = 0.09

    # Independent asset engines. Update expiry_dates as contracts roll.
    active_asset_ids: tuple[str, ...] = ("nifty", "naturalgas", "crudeoil")
    asset_profiles: tuple[AssetProfile, ...] = (
        AssetProfile(
            asset_id="nifty",
            label="NIFTY",
            underlying="NIFTY",
            exchange="NSE",
            segment="FNO",
            expiry_dates=("2026-06-09", "2026-06-16", "2026-06-23"),
            model_strategy="index_cross_sectional_premium_expansion",
            min_ltp=3.0,
            max_ltp=450.0,
            min_volume=20.0,
            min_oi=100.0,
            max_quote_symbols=40,
            max_spread_pct=0.025,
            min_edge_return=0.006,
            risk_per_trade_pct=0.005,
            max_premium_value_per_trade=10000.0,
            session_open="09:15",
            session_close="15:30",
            late_session_start="15:30",
        ),
        AssetProfile(
            asset_id="naturalgas",
            label="Natural Gas",
            underlying="NATURALGAS",
            exchange="MCX",
            segment="COMMODITY",
            expiry_dates=("2026-06-23", "2026-07-24"),
            model_strategy="energy_volatility_liquidity_breakout",
            option_chain_mode="instrument_quotes",
            product="MIS",
            min_ltp=0.5,
            max_ltp=500.0,
            min_volume=0.0,
            min_oi=0.0,
            max_quote_symbols=60,
            max_spread_pct=0.045,
            min_edge_return=0.010,
            risk_per_trade_pct=0.003,
            max_premium_value_per_trade=5000.0,
            min_backtest_trades=75,
            allow_short_option_entries=True,
            short_tp_pct=0.010,
            short_sl_pct=0.025,
            tp_pct=0.22,
            sl_pct=0.11,
            session_open="09:00",
            session_close="23:30",
            late_session_start="15:30",
        ),
        AssetProfile(
            asset_id="crudeoil",
            label="Crude Oil",
            underlying="CRUDEOIL",
            exchange="MCX",
            segment="COMMODITY",
            expiry_dates=("2026-06-16", "2026-07-16"),
            model_strategy="energy_trend_vol_premium_expansion",
            option_chain_mode="instrument_quotes",
            product="MIS",
            min_ltp=1.0,
            max_ltp=7000.0,
            min_volume=0.0,
            min_oi=0.0,
            max_quote_symbols=30,
            max_spread_pct=0.040,
            min_edge_return=0.010,
            risk_per_trade_pct=0.005,
            max_premium_value_per_trade=15000.0,
            tp_pct=0.20,
            sl_pct=0.10,
            session_open="09:00",
            session_close="23:30",
            late_session_start="15:30",
        ),
    )

    def ensure_dirs(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.model_path.parent.mkdir(parents=True, exist_ok=True)
        self.model_meta_path.parent.mkdir(parents=True, exist_ok=True)
        self.model_suite_path.parent.mkdir(parents=True, exist_ok=True)
        self.model_suite_meta_path.parent.mkdir(parents=True, exist_ok=True)
        for profile in self.asset_profiles:
            self.model_suite_path_for(profile.asset_id).parent.mkdir(parents=True, exist_ok=True)
            self.model_suite_meta_path_for(profile.asset_id).parent.mkdir(parents=True, exist_ok=True)
        Path("logs").mkdir(parents=True, exist_ok=True)
        Path("data/raw").mkdir(parents=True, exist_ok=True)
        self.run_rundown_path.parent.mkdir(parents=True, exist_ok=True)

    def asset_profile_map(self) -> dict[str, AssetProfile]:
        return {profile.asset_id: profile for profile in self.asset_profiles}

    def get_asset_profile(self, asset_id: str) -> AssetProfile:
        profiles = self.asset_profile_map()
        key = asset_id.lower()
        if key not in profiles:
            raise ValueError(f"Unknown asset_id={asset_id}. Available: {', '.join(sorted(profiles))}")
        return profiles[key]

    def active_asset_profiles(self) -> list[AssetProfile]:
        return [self.get_asset_profile(asset_id) for asset_id in self.active_asset_ids if self.get_asset_profile(asset_id).enabled]

    def model_suite_path_for(self, asset_id: str) -> Path:
        return Path("models") / asset_id / "option_model_suite.joblib"

    def model_suite_meta_path_for(self, asset_id: str) -> Path:
        return Path("models") / asset_id / "model_suite_meta.json"


def load_config() -> BotConfig:
    cfg = BotConfig()
    cfg.ensure_dirs()
    return cfg

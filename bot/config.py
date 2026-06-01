from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# .env is intentionally secrets-only. Do not put runtime knobs in .env.
# Runtime/model/risk settings live in this file so Docker/runtime behavior is explicit and auditable.
load_dotenv()


@dataclass(frozen=True)
class BotConfig:
    # ──────────────────────────────────────────────────────────────────────
    # Secrets / credentials: keep these in .env or runtime secret injection.
    # ──────────────────────────────────────────────────────────────────────
    groww_token: str = os.getenv("GROWW_API_AUTH_TOKEN", "")

    # ──────────────────────────────────────────────────────────────────────
    # Runtime mode
    # Live trading still requires the CLI flag: python -m bot.cli live-loop --live
    # plus live_trading_enabled=True here. Keep disabled until real-data gates pass.
    # ──────────────────────────────────────────────────────────────────────
    live_trading_enabled: bool = False
    paper_trading: bool = True

    # ──────────────────────────────────────────────────────────────────────
    # Files / artifacts
    # ──────────────────────────────────────────────────────────────────────
    db_path: Path = Path("data/nifty_option_bot.duckdb")
    nse_contract_file: Path = Path("data/raw/NSE_FO_contract_29052026.csv.gz")
    groww_instruments_csv: Path = Path("data/raw/groww_instruments.csv")
    model_path: Path = Path("models/option_return_model.joblib")
    model_meta_path: Path = Path("models/model_meta.json")
    model_suite_path: Path = Path("models/option_model_suite.joblib")
    model_suite_meta_path: Path = Path("models/model_suite_meta.json")

    # ──────────────────────────────────────────────────────────────────────
    # Universe
    # ──────────────────────────────────────────────────────────────────────
    underlying: str = "NIFTY"
    max_strikes_each_side: int = 12
    expiry_index: int = 0
    min_ltp: float = 3.0
    max_ltp: float = 450.0
    min_volume: float = 20.0
    min_oi: float = 100.0

    # ──────────────────────────────────────────────────────────────────────
    # Data collection
    # ──────────────────────────────────────────────────────────────────────
    option_chain_interval_seconds: int = 5
    max_quote_symbols: int = 40

    # ──────────────────────────────────────────────────────────────────────
    # Training / labels
    # ──────────────────────────────────────────────────────────────────────
    label_horizon_rows: int = 12
    estimated_round_trip_cost_bps: float = 35.0
    min_train_rows: int = 5000
    test_fraction: float = 0.25

    # ──────────────────────────────────────────────────────────────────────
    # Live edge/model gates
    # ──────────────────────────────────────────────────────────────────────
    min_edge_return: float = 0.006
    uncertainty_buffer: float = 0.003
    min_model_win_rate: float = 0.54
    min_model_sharpe: float = 0.60
    min_backtest_trades: int = 80
    auto_train_enabled: bool = True
    require_groww_source: bool = True
    live_train_min_rows: int = 5000

    # ──────────────────────────────────────────────────────────────────────
    # Risk / execution
    # ──────────────────────────────────────────────────────────────────────
    account_capital: float = 100000.0
    risk_per_trade_pct: float = 0.005
    max_premium_value_per_trade: float = 10000.0
    max_open_positions: int = 1
    entry_order_type: str = "LIMIT"
    product: str = "MIS"
    tp_pct: float = 0.18
    sl_pct: float = 0.09

    def ensure_dirs(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.model_path.parent.mkdir(parents=True, exist_ok=True)
        self.model_meta_path.parent.mkdir(parents=True, exist_ok=True)
        self.model_suite_path.parent.mkdir(parents=True, exist_ok=True)
        self.model_suite_meta_path.parent.mkdir(parents=True, exist_ok=True)
        Path("logs").mkdir(parents=True, exist_ok=True)
        Path("data/raw").mkdir(parents=True, exist_ok=True)


def load_config() -> BotConfig:
    cfg = BotConfig()
    cfg.ensure_dirs()
    return cfg

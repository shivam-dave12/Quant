from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def _bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except Exception:
        return default


def _int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, default)))
    except Exception:
        return default


@dataclass(frozen=True)
class BotConfig:
    groww_token: str = os.getenv("GROWW_API_AUTH_TOKEN", "")
    live_trading_enabled: bool = _bool("BOT_LIVE_TRADING_ENABLED", False)
    paper_trading: bool = _bool("BOT_PAPER_TRADING", True)

    db_path: Path = Path(os.getenv("BOT_DB_PATH", "data/nifty_option_bot.duckdb"))
    nse_contract_file: Path = Path(os.getenv("NSE_CONTRACT_FILE", "data/raw/NSE_FO_contract_29052026.csv.gz"))
    groww_instruments_csv: Path = Path(os.getenv("GROWW_INSTRUMENTS_CSV", "data/raw/groww_instruments.csv"))
    model_path: Path = Path(os.getenv("MODEL_PATH", "models/option_return_model.joblib"))
    model_meta_path: Path = Path(os.getenv("MODEL_META_PATH", "models/model_meta.json"))
    model_suite_path: Path = Path(os.getenv("MODEL_SUITE_PATH", "models/option_model_suite.joblib"))
    model_suite_meta_path: Path = Path(os.getenv("MODEL_SUITE_META_PATH", "models/model_suite_meta.json"))

    underlying: str = os.getenv("BOT_UNDERLYING", "NIFTY").upper()
    max_strikes_each_side: int = _int("BOT_MAX_STRIKES_EACH_SIDE", 12)
    expiry_index: int = _int("BOT_EXPIRY_INDEX", 0)
    min_ltp: float = _float("BOT_MIN_LTP", 3.0)
    max_ltp: float = _float("BOT_MAX_LTP", 450.0)
    min_volume: float = _float("BOT_MIN_VOLUME", 20.0)
    min_oi: float = _float("BOT_MIN_OI", 100.0)

    option_chain_interval_seconds: int = _int("BOT_OPTION_CHAIN_INTERVAL_SECONDS", 5)
    max_quote_symbols: int = _int("BOT_MAX_QUOTE_SYMBOLS", 40)

    label_horizon_rows: int = _int("BOT_LABEL_HORIZON_ROWS", 12)
    estimated_round_trip_cost_bps: float = _float("BOT_ESTIMATED_ROUND_TRIP_COST_BPS", 35.0)
    min_train_rows: int = _int("BOT_MIN_TRAIN_ROWS", 5000)
    test_fraction: float = _float("BOT_TEST_FRACTION", 0.25)

    min_edge_return: float = _float("BOT_MIN_EDGE_RETURN", 0.006)
    uncertainty_buffer: float = _float("BOT_UNCERTAINTY_BUFFER", 0.003)
    min_model_win_rate: float = _float("BOT_MIN_MODEL_WIN_RATE", 0.54)
    min_model_sharpe: float = _float("BOT_MIN_MODEL_SHARPE", 0.60)
    min_backtest_trades: int = _int("BOT_MIN_BACKTEST_TRADES", 80)
    auto_train_enabled: bool = _bool("BOT_AUTO_TRAIN_ENABLED", True)
    require_groww_source: bool = _bool("BOT_REQUIRE_GROWW_SOURCE", True)
    live_train_min_rows: int = _int("BOT_LIVE_TRAIN_MIN_ROWS", _int("BOT_MIN_TRAIN_ROWS", 5000))

    account_capital: float = _float("BOT_ACCOUNT_CAPITAL", 100000.0)
    risk_per_trade_pct: float = _float("BOT_RISK_PER_TRADE_PCT", 0.005)
    max_premium_value_per_trade: float = _float("BOT_MAX_PREMIUM_VALUE_PER_TRADE", 10000.0)
    max_open_positions: int = _int("BOT_MAX_OPEN_POSITIONS", 1)
    entry_order_type: str = os.getenv("BOT_ENTRY_ORDER_TYPE", "LIMIT").upper()
    product: str = os.getenv("BOT_PRODUCT", "MIS").upper()
    tp_pct: float = _float("BOT_TP_PCT", 0.18)
    sl_pct: float = _float("BOT_SL_PCT", 0.09)

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

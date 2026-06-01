from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor, RandomForestRegressor
from sklearn.inspection import permutation_importance
from sklearn.metrics import (
    accuracy_score,
    log_loss,
    mean_absolute_error,
    mean_squared_error,
    precision_score,
    roc_auc_score,
)

from .config import BotConfig
from .storage import Store

log = logging.getLogger(__name__)

MODEL_VERSION = "option_model_suite_v3_live_only"

# This is intentionally option-only. underlying_ltp is used only for moneyness/Greeks context.
CORE_FEATURES: list[str] = [
    # Quote/premium state
    "ltp",
    "mid_price",
    "quoted_entry_price",
    "quoted_exit_price",
    "spread",
    "spread_pct",
    "bid_quantity",
    "ask_quantity",
    "total_buy_quantity",
    "total_sell_quantity",
    "last_trade_quantity",
    # Option chain participation
    "open_interest",
    "volume",
    "oi_change_1",
    "oi_change_3",
    "volume_change_1",
    "volume_change_3",
    "volume_velocity_3",
    "oi_velocity_3",
    # Greeks / option physics
    "iv",
    "delta",
    "gamma",
    "theta",
    "vega",
    "rho",
    "abs_delta",
    "abs_theta",
    "theta_to_premium",
    "gamma_theta_ratio",
    "vega_to_premium",
    "gamma_to_premium",
    "iv_change_1",
    "iv_change_3",
    "iv_velocity_3",
    # Contract geometry
    "strike",
    "moneyness",
    "abs_moneyness",
    "intrinsic_value",
    "extrinsic_value",
    "extrinsic_ratio",
    "time_to_expiry_days",
    "is_ce",
    "is_pe",
    # Premium micro-momentum
    "premium_return_1",
    "premium_return_3",
    "premium_return_6",
    "premium_velocity_3",
    "premium_acceleration",
    "premium_range_pos_20",
    "premium_realized_vol_20",
    # Relative option-chain state
    "volume_rank",
    "oi_rank",
    "ltp_rank",
    "iv_rank",
    "gamma_theta_rank",
    "spread_rank",
    "premium_momentum_rank",
    "same_strike_ce_pe_ratio",
    "same_strike_side_strength",
    "nearby_strike_strength",
    # IV surface / skew state
    "iv_z",
    "iv_residual_linear",
    "iv_residual_quad",
    "skew_slope_linear",
    "skew_curvature_quad",
    # Book pressure / liquidity
    "depth_bid_qty_5",
    "depth_ask_qty_5",
    "depth_imbalance_5",
    "top_book_imbalance",
    "depth_weighted_bid_px",
    "depth_weighted_ask_px",
    "book_pressure_score",
]

TARGET_COLUMNS: list[str] = [
    "future_return_after_costs",
    "future_positive",
    "future_rank_pct",
    "future_iv_change",
    "future_iv_up",
    "future_mfe_after_costs",
    "future_mae_after_costs",
]


@dataclass(frozen=True)
class ModelSuiteConfig:
    horizon_rows: int = 12
    cost_bps: float = 35.0
    min_rows: int = 5000
    test_fraction: float = 0.25
    top_quantile: float = 0.10
    random_state: int = 42
    permutation_importance_rows: int = 2500
    min_live_trades: int = 80
    min_live_win_rate: float = 0.54
    min_live_sharpe: float = 0.60
    require_groww_source: bool = True

    @classmethod
    def from_bot_config(cls, cfg: BotConfig) -> "ModelSuiteConfig":
        return cls(
            horizon_rows=cfg.label_horizon_rows,
            cost_bps=cfg.estimated_round_trip_cost_bps,
            min_rows=max(cfg.min_train_rows, cfg.live_train_min_rows),
            test_fraction=cfg.test_fraction,
            min_live_trades=cfg.min_backtest_trades,
            min_live_win_rate=cfg.min_model_win_rate,
            min_live_sharpe=cfg.min_model_sharpe,
            require_groww_source=cfg.require_groww_source,
        )


@dataclass
class ModelTrainingResult:
    model_path: Path
    meta_path: Path
    metrics: dict[str, Any]


class DeterministicCostModel:
    """A transparent execution-cost model used until enough real fill data exists.

    This is not a trading strategy. It estimates the price drag that every predictive model
    must beat. Later, replace/augment this with a fit on broker fill data.
    """

    def predict_cost(self, df: pd.DataFrame) -> np.ndarray:
        spread_pct = _num(df.get("spread_pct"), 0.0).clip(lower=0.0, upper=0.50)
        depth_imb = _num(df.get("depth_imbalance_5"), 0.0).abs().clip(lower=0.0, upper=1.0)
        low_liq_penalty = 0.0025 * (1.0 - _num(df.get("volume_rank"), 0.5).clip(0, 1))
        imbalance_penalty = 0.0015 * depth_imb
        return (spread_pct + low_liq_penalty + imbalance_penalty).to_numpy(dtype=float)


class OptionModelSuite:
    """Model-only layer for NIFTY options.

    It produces predictions/ranks. It does not decide entries, exits, targets, stops,
    position sizing, or live trading rules.
    """

    def __init__(self, cfg: ModelSuiteConfig | None = None):
        self.cfg = cfg or ModelSuiteConfig()
        self.feature_cols = list(CORE_FEATURES)
        self.models: dict[str, Any] = {}
        self.metrics: dict[str, Any] = {}
        self.cost_model = DeterministicCostModel()

    def fit(self, frame: pd.DataFrame) -> "OptionModelSuite":
        data = _prepare_training_frame(frame, self.feature_cols)
        if len(data) < self.cfg.min_rows:
            raise ValueError(f"Not enough labelled rows to train model suite: {len(data)} < {self.cfg.min_rows}")

        data = data.sort_values("ts").reset_index(drop=True)
        split_idx = int(len(data) * (1.0 - self.cfg.test_fraction))
        train = data.iloc[:split_idx].copy()
        test = data.iloc[split_idx:].copy()
        if train.empty or test.empty:
            raise ValueError("Train/test split produced an empty partition.")

        X_train = clean_matrix(train, self.feature_cols)
        X_test = clean_matrix(test, self.feature_cols)
        y_ret_train = train["future_return_after_costs"].astype(float)
        y_ret_test = test["future_return_after_costs"].astype(float)
        y_cls_train = train["future_positive"].astype(int)
        y_cls_test = test["future_positive"].astype(int)
        y_rank_train = train["future_rank_pct"].astype(float)
        y_rank_test = test["future_rank_pct"].astype(float)
        y_iv_train = train["future_iv_up"].astype(int)
        y_iv_test = test["future_iv_up"].astype(int)

        return_model, return_model_type = _make_return_regressor(self.cfg.random_state)
        return_model.fit(X_train, y_ret_train)
        self.models["return_regressor"] = return_model

        classifier, classifier_type = _make_profit_classifier(self.cfg.random_state)
        classifier.fit(X_train, y_cls_train)
        self.models["profit_classifier"] = classifier

        ranker, ranker_type = _make_ranker(self.cfg.random_state)
        _fit_ranker(ranker, X_train, y_rank_train, train)
        self.models["cross_sectional_ranker"] = ranker

        q20, q20_type = _make_quantile_regressor(0.20, self.cfg.random_state)
        q50, q50_type = _make_quantile_regressor(0.50, self.cfg.random_state)
        q80, q80_type = _make_quantile_regressor(0.80, self.cfg.random_state)
        q20.fit(X_train, y_ret_train)
        q50.fit(X_train, y_ret_train)
        q80.fit(X_train, y_ret_train)
        self.models["return_q20"] = q20
        self.models["return_q50"] = q50
        self.models["return_q80"] = q80

        iv_model, iv_model_type = _make_profit_classifier(self.cfg.random_state + 1)
        iv_model.fit(X_train, y_iv_train)
        self.models["iv_expansion_classifier"] = iv_model

        pred = self.predict(test)
        self.metrics = _evaluate_predictions(test, pred, self.cfg)
        self.metrics["model_types"] = {
            "return_regressor": return_model_type,
            "profit_classifier": classifier_type,
            "cross_sectional_ranker": ranker_type,
            "return_q20": q20_type,
            "return_q50": q50_type,
            "return_q80": q80_type,
            "iv_expansion_classifier": iv_model_type,
            "cost_model": "deterministic_spread_depth_cost_model",
        }
        self.metrics["created_at_utc"] = datetime.utcnow().isoformat()
        self.metrics["version"] = MODEL_VERSION
        self.metrics["config"] = asdict(self.cfg)
        self.metrics["feature_cols"] = self.feature_cols
        self.metrics["train_rows"] = int(len(train))
        self.metrics["test_rows"] = int(len(test))
        try:
            self.metrics["feature_importance"] = _feature_importance(return_model, X_test, y_ret_test, self.feature_cols, self.cfg)
        except Exception as exc:
            self.metrics["feature_importance_error"] = str(exc)
        return self

    def predict(self, frame: pd.DataFrame) -> pd.DataFrame:
        if not self.models:
            raise ValueError("Model suite is not fitted/loaded.")
        df = frame.copy()
        X = clean_matrix(df, self.feature_cols)
        out = df[[c for c in ["ts", "expiry", "strike", "option_type", "trading_symbol", "ltp"] if c in df.columns]].copy()
        out["predicted_return"] = self.models["return_regressor"].predict(X)
        out["rank_score"] = _predict_any(self.models["cross_sectional_ranker"], X)
        out["return_q20"] = self.models["return_q20"].predict(X)
        out["return_q50"] = self.models["return_q50"].predict(X)
        out["return_q80"] = self.models["return_q80"].predict(X)
        out["prob_profit"] = _predict_positive_proba(self.models["profit_classifier"], X)
        out["prob_iv_expansion"] = _predict_positive_proba(self.models["iv_expansion_classifier"], X)
        out["estimated_cost"] = self.cost_model.predict_cost(df)
        # Model composite only; strategy layer can later decide thresholds/entry rules.
        out["model_score"] = (
            0.40 * _pct_rank(out["predicted_return"])
            + 0.25 * _pct_rank(out["rank_score"])
            + 0.20 * out["prob_profit"].clip(0, 1)
            + 0.10 * out["prob_iv_expansion"].clip(0, 1)
            + 0.05 * _pct_rank(out["return_q80"] - out["return_q20"])
            - 0.15 * _pct_rank(out["estimated_cost"])
        )
        if "ts" in out.columns:
            out["model_rank_at_ts"] = out.groupby("ts")["model_score"].rank(ascending=False, method="first")
        return out.sort_values(["ts", "model_score"] if "ts" in out.columns else ["model_score"], ascending=[True, False] if "ts" in out.columns else False)

    def save(self, model_path: Path, meta_path: Path) -> None:
        model_path.parent.mkdir(parents=True, exist_ok=True)
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": MODEL_VERSION,
            "suite_config": asdict(self.cfg),
            "feature_cols": self.feature_cols,
            "models": self.models,
            "metrics": self.metrics,
        }
        joblib.dump(payload, model_path)
        meta_path.write_text(json.dumps(self.metrics, indent=2, default=str), encoding="utf-8")

    @classmethod
    def load(cls, model_path: str | Path) -> "OptionModelSuite":
        payload = joblib.load(model_path)
        cfg = ModelSuiteConfig(**payload.get("suite_config", {}))
        suite = cls(cfg)
        suite.feature_cols = list(payload.get("feature_cols", CORE_FEATURES))
        suite.models = dict(payload.get("models", {}))
        suite.metrics = dict(payload.get("metrics", {}))
        return suite


def train_model_suite_from_store(bot_cfg: BotConfig, store: Store) -> ModelTrainingResult:
    suite_cfg = ModelSuiteConfig.from_bot_config(bot_cfg)
    raw = load_option_model_raw_data(bot_cfg, store)
    frame = build_option_model_frame(raw, horizon_rows=suite_cfg.horizon_rows, cost_bps=suite_cfg.cost_bps)
    suite = OptionModelSuite(suite_cfg).fit(frame)
    suite.save(bot_cfg.model_suite_path, bot_cfg.model_suite_meta_path)

    metrics = suite.metrics
    # Persist a model-run row without changing the old table contract.
    store.append_df(
        "backtest_metrics",
        pd.DataFrame([
            {
                "ts": datetime.utcnow(),
                "model_path": str(bot_cfg.model_suite_path),
                "trades": int(metrics.get("top_decile_count", 0)),
                "win_rate": float(metrics.get("top_decile_win_rate", 0.0)),
                "mean_return": float(metrics.get("top_decile_mean_return", 0.0)),
                "sharpe": float(metrics.get("top_decile_sharpe", 0.0)),
                "alpha_vs_universe": float(metrics.get("top_decile_alpha_vs_universe", 0.0)),
                "passed": bool(metrics.get("model_quality_gate_passed", False)),
                "raw_json": json.dumps(metrics, default=str),
            }
        ]),
    )
    return ModelTrainingResult(bot_cfg.model_suite_path, bot_cfg.model_suite_meta_path, metrics)


def load_option_model_raw_data(cfg: BotConfig, store: Store) -> pd.DataFrame:
    """Load only real market snapshots collected by the Groww collector.

    Synthetic/sample rows are deliberately not supported. With BOT_REQUIRE_GROWW_SOURCE=true
    the trainer accepts only rows tagged by the live collector as groww_option_chain/groww_quote.
    """
    chain_source_clause = "AND source = 'groww_option_chain'" if cfg.require_groww_source else ""
    chain = store.query_df(
        f"""
        SELECT * FROM option_chain_snapshots
        WHERE trading_symbol IS NOT NULL
          AND ltp IS NOT NULL
          AND ltp BETWEEN ? AND ?
          AND volume >= ?
          AND open_interest >= ?
          {chain_source_clause}
          AND lower(coalesce(raw_json, '')) NOT LIKE '%synthetic%'
          AND lower(coalesce(raw_json, '')) NOT LIKE '%dummy%'
          AND lower(coalesce(raw_json, '')) NOT LIKE '%fake%'
        ORDER BY trading_symbol, ts
        """,
        (cfg.min_ltp, cfg.max_ltp, cfg.min_volume, cfg.min_oi),
    )
    if chain.empty:
        return chain

    try:
        quote_source_clause = "AND source = 'groww_quote'" if cfg.require_groww_source else ""
        quotes = store.query_df(
            f"""
            SELECT * FROM quote_snapshots
            WHERE trading_symbol IS NOT NULL
              {quote_source_clause}
              AND lower(coalesce(raw_json, '')) NOT LIKE '%synthetic%'
              AND lower(coalesce(raw_json, '')) NOT LIKE '%dummy%'
              AND lower(coalesce(raw_json, '')) NOT LIKE '%fake%'
            ORDER BY trading_symbol, ts
            """
        )
    except Exception:
        quotes = pd.DataFrame()

    if quotes.empty:
        return chain
    return merge_chain_quotes(chain, quotes)


def merge_chain_quotes(chain: pd.DataFrame, quotes: pd.DataFrame, tolerance_seconds: int = 15) -> pd.DataFrame:
    chain = chain.copy()
    quotes = quotes.copy()
    chain["ts"] = pd.to_datetime(chain["ts"])
    quotes["ts"] = pd.to_datetime(quotes["ts"])
    frames: list[pd.DataFrame] = []
    quote_cols = [
        "trading_symbol",
        "ts",
        "last_price",
        "bid_price",
        "bid_quantity",
        "offer_price",
        "offer_quantity",
        "spread",
        "spread_pct",
        "total_buy_quantity",
        "total_sell_quantity",
        "last_trade_quantity",
        "depth_json",
    ]
    q = quotes[[c for c in quote_cols if c in quotes.columns]].sort_values("ts")
    for sym, sub in chain.sort_values("ts").groupby("trading_symbol", sort=False):
        qs = q[q["trading_symbol"].eq(sym)].sort_values("ts")
        if qs.empty:
            frames.append(sub)
            continue
        merged = pd.merge_asof(
            sub.sort_values("ts"),
            qs.drop(columns=["trading_symbol"], errors="ignore"),
            on="ts",
            direction="backward",
            tolerance=pd.Timedelta(seconds=tolerance_seconds),
            suffixes=("", "_quote"),
        )
        frames.append(merged)
    return pd.concat(frames, ignore_index=True) if frames else chain


def build_option_model_frame(raw: pd.DataFrame, horizon_rows: int = 12, cost_bps: float = 35.0) -> pd.DataFrame:
    if raw.empty:
        return raw.copy()
    df = raw.copy()
    df["ts"] = pd.to_datetime(df["ts"])
    df["expiry"] = pd.to_datetime(df["expiry"])
    df = df.sort_values(["trading_symbol", "ts"]).reset_index(drop=True)

    for col in [
        "strike", "underlying_ltp", "ltp", "open_interest", "volume", "delta", "gamma", "theta", "vega", "rho", "iv",
        "last_price", "bid_price", "bid_quantity", "offer_price", "offer_quantity", "spread", "spread_pct",
        "total_buy_quantity", "total_sell_quantity", "last_trade_quantity",
    ]:
        if col not in df.columns:
            df[col] = np.nan
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Quote features: prefer executable quote, fall back to LTP so the model can train on option-chain snapshots.
    df["quoted_entry_price"] = df["offer_price"].where(df["offer_price"].gt(0), df["ltp"])
    df["quoted_exit_price"] = df["bid_price"].where(df["bid_price"].gt(0), df["ltp"])
    df["mid_price"] = np.where(
        df["bid_price"].gt(0) & df["offer_price"].gt(0),
        (df["bid_price"] + df["offer_price"]) / 2.0,
        df["ltp"],
    )
    df["spread"] = df["spread"].where(df["spread"].notna(), df["offer_price"] - df["bid_price"])
    df["spread_pct"] = df["spread_pct"].where(df["spread_pct"].notna(), _safe_div(df["spread"], df["mid_price"]))
    df["ask_quantity"] = df["offer_quantity"]

    # Depth parsing from raw JSON if present.
    depth = parse_depth_features(df.get("depth_json"))
    for c in depth.columns:
        df[c] = depth[c]
    for c in ["depth_bid_qty_5", "depth_ask_qty_5", "depth_imbalance_5", "top_book_imbalance", "depth_weighted_bid_px", "depth_weighted_ask_px"]:
        if c not in df.columns:
            df[c] = np.nan
    df["depth_bid_qty_5"] = df["depth_bid_qty_5"].fillna(df["bid_quantity"])
    df["depth_ask_qty_5"] = df["depth_ask_qty_5"].fillna(df["ask_quantity"])
    df["depth_imbalance_5"] = _safe_div(df["depth_bid_qty_5"] - df["depth_ask_qty_5"], df["depth_bid_qty_5"] + df["depth_ask_qty_5"])
    df["top_book_imbalance"] = _safe_div(df["bid_quantity"] - df["ask_quantity"], df["bid_quantity"] + df["ask_quantity"])
    df["book_pressure_score"] = 0.60 * df["depth_imbalance_5"].fillna(0) + 0.40 * df["top_book_imbalance"].fillna(0)

    # Contract geometry and Greeks.
    df["is_ce"] = df["option_type"].astype(str).str.upper().eq("CE").astype(int)
    df["is_pe"] = df["option_type"].astype(str).str.upper().eq("PE").astype(int)
    df["moneyness"] = _safe_div(df["strike"] - df["underlying_ltp"], df["underlying_ltp"])
    df["abs_moneyness"] = df["moneyness"].abs()
    intrinsic_ce = np.maximum(0.0, df["underlying_ltp"] - df["strike"])
    intrinsic_pe = np.maximum(0.0, df["strike"] - df["underlying_ltp"])
    df["intrinsic_value"] = np.where(df["is_ce"].eq(1), intrinsic_ce, intrinsic_pe)
    df["extrinsic_value"] = df["mid_price"] - df["intrinsic_value"]
    df["extrinsic_ratio"] = _safe_div(df["extrinsic_value"], df["mid_price"])
    # Use IST market close expiry approximation: options expire at end of trading day; model uses relative not absolute exactness.
    df["time_to_expiry_days"] = (df["expiry"] - df["ts"].dt.normalize()).dt.total_seconds() / 86400.0
    df["abs_delta"] = df["delta"].abs()
    df["abs_theta"] = df["theta"].abs()
    df["theta_to_premium"] = _safe_div(df["abs_theta"], df["mid_price"])
    df["gamma_theta_ratio"] = _safe_div(df["gamma"], df["abs_theta"])
    df["vega_to_premium"] = _safe_div(df["vega"], df["mid_price"])
    df["gamma_to_premium"] = _safe_div(df["gamma"], df["mid_price"])

    g = df.groupby("trading_symbol", group_keys=False)
    df["premium_return_1"] = g["mid_price"].pct_change(1)
    df["premium_return_3"] = g["mid_price"].pct_change(3)
    df["premium_return_6"] = g["mid_price"].pct_change(6)
    df["premium_velocity_3"] = df["premium_return_3"] / 3.0
    df["premium_acceleration"] = df["premium_return_1"] - g["premium_return_1"].shift(1)
    roll_min = g["mid_price"].transform(lambda x: x.rolling(20, min_periods=5).min())
    roll_max = g["mid_price"].transform(lambda x: x.rolling(20, min_periods=5).max())
    df["premium_range_pos_20"] = _safe_div(df["mid_price"] - roll_min, roll_max - roll_min)
    df["premium_realized_vol_20"] = g["premium_return_1"].transform(lambda x: x.rolling(20, min_periods=5).std(ddof=0))
    df["iv_change_1"] = g["iv"].diff(1)
    df["iv_change_3"] = g["iv"].diff(3)
    df["iv_velocity_3"] = df["iv_change_3"] / 3.0
    df["oi_change_1"] = g["open_interest"].diff(1)
    df["oi_change_3"] = g["open_interest"].diff(3)
    df["volume_change_1"] = g["volume"].diff(1)
    df["volume_change_3"] = g["volume"].diff(3)
    df["volume_velocity_3"] = df["volume_change_3"] / 3.0
    df["oi_velocity_3"] = df["oi_change_3"] / 3.0

    # Cross-sectional ranks inside each chain snapshot / expiry / side.
    group_cols = ["ts", "expiry", "option_type"]
    for src, dst, asc in [
        ("volume", "volume_rank", True),
        ("open_interest", "oi_rank", True),
        ("ltp", "ltp_rank", True),
        ("iv", "iv_rank", True),
        ("gamma_theta_ratio", "gamma_theta_rank", True),
        ("spread_pct", "spread_rank", False),
        ("premium_return_3", "premium_momentum_rank", True),
    ]:
        df[dst] = df.groupby(group_cols)[src].rank(pct=True, ascending=asc)

    # Same-strike CE/PE relative premium strength.
    pivot = df.pivot_table(index=["ts", "expiry", "strike"], columns="option_type", values="mid_price", aggfunc="last").reset_index()
    if "CE" in pivot.columns and "PE" in pivot.columns:
        pivot["same_strike_ce_pe_ratio"] = _safe_div(pivot["CE"], pivot["PE"])
        df = df.merge(pivot[["ts", "expiry", "strike", "same_strike_ce_pe_ratio"]], on=["ts", "expiry", "strike"], how="left")
        df["same_strike_side_strength"] = np.where(
            df["is_ce"].eq(1),
            df["same_strike_ce_pe_ratio"],
            _safe_div(1.0, df["same_strike_ce_pe_ratio"]),
        )
    else:
        df["same_strike_ce_pe_ratio"] = np.nan
        df["same_strike_side_strength"] = np.nan

    # Nearby-strike relative strength within CE/PE chain.
    df["nearby_strike_strength"] = np.nan
    for _, idx in df.groupby(group_cols).groups.items():
        sub = df.loc[idx].sort_values("strike")
        neigh = sub["premium_return_3"].rolling(3, center=True, min_periods=1).mean()
        df.loc[sub.index, "nearby_strike_strength"] = sub["premium_return_3"].to_numpy() - neigh.to_numpy()

    add_iv_surface_features(df, group_cols)
    add_forward_labels(df, horizon_rows=horizon_rows, cost_bps=cost_bps)

    for c in CORE_FEATURES + TARGET_COLUMNS:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def parse_depth_features(depth_json: pd.Series | None) -> pd.DataFrame:
    rows: list[dict[str, float]] = []
    if depth_json is None:
        return pd.DataFrame()
    for val in depth_json:
        rows.append(_parse_one_depth(val))
    return pd.DataFrame(rows)


def _parse_one_depth(val: Any) -> dict[str, float]:
    out = {
        "depth_bid_qty_5": np.nan,
        "depth_ask_qty_5": np.nan,
        "depth_imbalance_5": np.nan,
        "top_book_imbalance": np.nan,
        "depth_weighted_bid_px": np.nan,
        "depth_weighted_ask_px": np.nan,
    }
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return out
    try:
        obj = json.loads(val) if isinstance(val, str) else val
    except Exception:
        return out
    if not isinstance(obj, dict):
        return out

    buy = obj.get("buyBook") or obj.get("buy") or obj.get("bids") or obj.get("bid") or []
    sell = obj.get("sellBook") or obj.get("sell") or obj.get("asks") or obj.get("ask") or []

    def levels(x: Any) -> list[dict[str, Any]]:
        if isinstance(x, dict):
            # Groww may return level keyed object.
            vals = list(x.values())
        elif isinstance(x, list):
            vals = x
        else:
            vals = []
        return [v for v in vals[:5] if isinstance(v, dict)]

    b = levels(buy)
    a = levels(sell)

    def qty(v: dict[str, Any]) -> float:
        for k in ("quantity", "qty", "volume", "bidQty", "askQty"):
            if k in v:
                try:
                    return float(v[k] or 0)
                except Exception:
                    return 0.0
        return 0.0

    def px(v: dict[str, Any]) -> float:
        for k in ("price", "bidPrice", "askPrice"):
            if k in v:
                try:
                    return float(v[k] or 0)
                except Exception:
                    return 0.0
        return 0.0

    bq = np.array([qty(v) for v in b], dtype=float)
    aq = np.array([qty(v) for v in a], dtype=float)
    bp = np.array([px(v) for v in b], dtype=float)
    ap = np.array([px(v) for v in a], dtype=float)
    bsum = float(np.nansum(bq)) if len(bq) else np.nan
    asum = float(np.nansum(aq)) if len(aq) else np.nan
    out["depth_bid_qty_5"] = bsum
    out["depth_ask_qty_5"] = asum
    out["depth_imbalance_5"] = float((bsum - asum) / (bsum + asum)) if np.isfinite(bsum + asum) and (bsum + asum) > 0 else np.nan
    top_sum = (bq[0] if len(bq) else 0) + (aq[0] if len(aq) else 0)
    out["top_book_imbalance"] = float(((bq[0] if len(bq) else 0) - (aq[0] if len(aq) else 0)) / top_sum) if top_sum > 0 else np.nan
    out["depth_weighted_bid_px"] = float(np.nansum(bp * bq) / np.nansum(bq)) if len(bq) and np.nansum(bq) > 0 else np.nan
    out["depth_weighted_ask_px"] = float(np.nansum(ap * aq) / np.nansum(aq)) if len(aq) and np.nansum(aq) > 0 else np.nan
    return out


def add_iv_surface_features(df: pd.DataFrame, group_cols: list[str]) -> None:
    df["iv_z"] = df.groupby(group_cols)["iv"].transform(lambda x: (x - x.mean()) / (x.std(ddof=0) + 1e-9))
    df["iv_residual_linear"] = np.nan
    df["iv_residual_quad"] = np.nan
    df["skew_slope_linear"] = np.nan
    df["skew_curvature_quad"] = np.nan
    for _, idx in df.groupby(group_cols).groups.items():
        sub = df.loc[idx]
        ok = sub[["moneyness", "iv"]].dropna()
        if len(ok) >= 5 and ok["moneyness"].nunique() >= 3:
            try:
                coef1 = np.polyfit(ok["moneyness"], ok["iv"], deg=1)
                pred1 = np.polyval(coef1, sub["moneyness"].astype(float))
                df.loc[idx, "iv_residual_linear"] = sub["iv"].astype(float) - pred1
                df.loc[idx, "skew_slope_linear"] = float(coef1[0])
            except Exception:
                pass
        if len(ok) >= 7 and ok["moneyness"].nunique() >= 5:
            try:
                coef2 = np.polyfit(ok["moneyness"], ok["iv"], deg=2)
                pred2 = np.polyval(coef2, sub["moneyness"].astype(float))
                df.loc[idx, "iv_residual_quad"] = sub["iv"].astype(float) - pred2
                df.loc[idx, "skew_curvature_quad"] = float(coef2[0])
            except Exception:
                pass


def add_forward_labels(df: pd.DataFrame, horizon_rows: int, cost_bps: float) -> None:
    g = df.groupby("trading_symbol", group_keys=False)
    future_exit = g["quoted_exit_price"].shift(-horizon_rows)
    entry = df["quoted_entry_price"]
    cost = cost_bps / 10000.0
    df["future_return_after_costs"] = _safe_div(future_exit - entry, entry) - cost
    df["future_positive"] = (df["future_return_after_costs"] > 0).astype(int)
    df["future_iv_change"] = g["iv"].shift(-horizon_rows) - df["iv"]
    df["future_iv_up"] = (df["future_iv_change"] > 0).astype(int)

    # Future MFE/MAE inside the next horizon; useful for model diagnostics and later strategy design.
    future_mfe = []
    future_mae = []
    for _, sub in df.groupby("trading_symbol", sort=False):
        prices = sub["quoted_exit_price"].to_numpy(dtype=float)
        entries = sub["quoted_entry_price"].to_numpy(dtype=float)
        n = len(sub)
        mfe = np.full(n, np.nan)
        mae = np.full(n, np.nan)
        for i in range(n):
            j = min(n, i + horizon_rows + 1)
            if j <= i + 1 or not np.isfinite(entries[i]) or entries[i] <= 0:
                continue
            window = prices[i + 1 : j]
            if len(window) == 0:
                continue
            ret = (window - entries[i]) / entries[i] - cost
            mfe[i] = np.nanmax(ret)
            mae[i] = np.nanmin(ret)
        future_mfe.extend(mfe.tolist())
        future_mae.extend(mae.tolist())
    df["future_mfe_after_costs"] = future_mfe
    df["future_mae_after_costs"] = future_mae

    # Cross-sectional label: rank options by future return inside each timestamp.
    df["future_rank_pct"] = df.groupby("ts")["future_return_after_costs"].rank(pct=True)


def clean_matrix(df: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in feature_cols:
        if c not in out.columns:
            out[c] = np.nan
    X = out[feature_cols].replace([np.inf, -np.inf], np.nan)
    return X.fillna(0.0).astype(float)


def _prepare_training_frame(frame: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    required = list(feature_cols) + ["future_return_after_costs", "future_positive", "future_rank_pct", "future_iv_up"]
    df = frame.copy()
    for c in required:
        if c not in df.columns:
            df[c] = np.nan
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=["future_return_after_costs", "future_positive", "future_rank_pct", "future_iv_up"])
    return df


def _make_return_regressor(seed: int):
    try:
        from lightgbm import LGBMRegressor  # type: ignore
        return LGBMRegressor(
            objective="regression",
            n_estimators=260,
            learning_rate=0.025,
            num_leaves=41,
            min_child_samples=60,
            subsample=0.85,
            colsample_bytree=0.85,
            reg_alpha=0.05,
            reg_lambda=0.20,
            random_state=seed,
            verbosity=-1,
            n_jobs=1,
        ), "lightgbm.LGBMRegressor"
    except Exception:
        return HistGradientBoostingRegressor(
            max_iter=220,
            learning_rate=0.035,
            l2_regularization=0.10,
            max_leaf_nodes=31,
            random_state=seed,
        ), "sklearn.HistGradientBoostingRegressor"


def _make_profit_classifier(seed: int):
    try:
        from lightgbm import LGBMClassifier  # type: ignore
        return LGBMClassifier(
            objective="binary",
            n_estimators=240,
            learning_rate=0.03,
            num_leaves=31,
            min_child_samples=60,
            subsample=0.85,
            colsample_bytree=0.85,
            reg_alpha=0.05,
            reg_lambda=0.20,
            random_state=seed,
            verbosity=-1,
            n_jobs=1,
        ), "lightgbm.LGBMClassifier"
    except Exception:
        return HistGradientBoostingClassifier(
            max_iter=220,
            learning_rate=0.035,
            l2_regularization=0.10,
            max_leaf_nodes=31,
            random_state=seed,
        ), "sklearn.HistGradientBoostingClassifier"


def _make_ranker(seed: int):
    try:
        from lightgbm import LGBMRanker  # type: ignore
        return LGBMRanker(
            objective="lambdarank",
            n_estimators=220,
            learning_rate=0.035,
            num_leaves=31,
            min_child_samples=40,
            subsample=0.85,
            colsample_bytree=0.85,
            random_state=seed,
            verbosity=-1,
            n_jobs=1,
        ), "lightgbm.LGBMRanker"
    except Exception:
        return RandomForestRegressor(
            n_estimators=250,
            min_samples_leaf=20,
            max_features="sqrt",
            random_state=seed,
            n_jobs=1,
        ), "sklearn.RandomForestRegressor_rank_fallback"


def _fit_ranker(model: Any, X: pd.DataFrame, y: pd.Series, train: pd.DataFrame) -> None:
    # LightGBM Ranker requires group sizes. Fallback regressors do not.
    if model.__class__.__name__ == "LGBMRanker":
        ordered = train.copy()
        ordered["_row"] = np.arange(len(train))
        ordered = ordered.sort_values(["ts", "_row"])
        X_ord = X.loc[ordered.index]
        y_ord = y.loc[ordered.index]
        # Convert percentile label to integer relevance grades 0..4.
        rel = np.floor(y_ord.clip(0, 1).to_numpy() * 5).clip(0, 4).astype(int)
        group = ordered.groupby("ts").size().to_list()
        model.fit(X_ord, rel, group=group)
    else:
        model.fit(X, y)


def _make_quantile_regressor(q: float, seed: int):
    try:
        from lightgbm import LGBMRegressor  # type: ignore
        return LGBMRegressor(
            objective="quantile",
            alpha=q,
            n_estimators=180,
            learning_rate=0.03,
            num_leaves=31,
            min_child_samples=60,
            subsample=0.85,
            colsample_bytree=0.85,
            random_state=seed,
            verbosity=-1,
            n_jobs=1,
        ), f"lightgbm.LGBMRegressor_quantile_{q:.2f}"
    except Exception:
        return HistGradientBoostingRegressor(
            loss="quantile",
            quantile=q,
            max_iter=180,
            learning_rate=0.04,
            l2_regularization=0.05,
            random_state=seed,
        ), f"sklearn.HistGradientBoostingRegressor_quantile_{q:.2f}"


def _predict_positive_proba(model: Any, X: pd.DataFrame) -> pd.Series:
    if hasattr(model, "predict_proba"):
        proba = model.predict_proba(X)
        if proba.ndim == 2 and proba.shape[1] > 1:
            return pd.Series(proba[:, 1], index=X.index)
        return pd.Series(proba.ravel(), index=X.index)
    pred = model.predict(X)
    return pd.Series(pred, index=X.index).clip(0, 1)


def _predict_any(model: Any, X: pd.DataFrame) -> pd.Series:
    return pd.Series(model.predict(X), index=X.index)


def _evaluate_predictions(test: pd.DataFrame, pred: pd.DataFrame, cfg: ModelSuiteConfig) -> dict[str, Any]:
    joined = test.copy().reset_index(drop=True)
    p = pred.reset_index(drop=True)
    joined["predicted_return"] = p["predicted_return"]
    joined["prob_profit"] = p["prob_profit"]
    joined["rank_score"] = p["rank_score"]
    joined["model_score"] = p["model_score"]
    joined["estimated_cost"] = p["estimated_cost"]
    y = joined["future_return_after_costs"].astype(float)
    yhat = joined["predicted_return"].astype(float)
    cls = joined["future_positive"].astype(int)
    proba = joined["prob_profit"].astype(float).clip(1e-6, 1 - 1e-6)

    metrics: dict[str, Any] = {
        "return_mae": float(mean_absolute_error(y, yhat)),
        "return_rmse": float(np.sqrt(mean_squared_error(y, yhat))),
        "return_spearman_ic": _spearman(y, yhat),
        "rank_spearman_ic": _spearman(joined["future_rank_pct"], joined["rank_score"]),
        "profit_accuracy_0p5": float(accuracy_score(cls, proba >= 0.5)),
        "profit_log_loss": float(log_loss(cls, proba, labels=[0, 1])) if cls.nunique() > 1 else None,
        "profit_auc": float(roc_auc_score(cls, proba)) if cls.nunique() > 1 else None,
        "universe_mean_return": float(y.mean()),
        "universe_win_rate": float((y > 0).mean()),
        "rows_evaluated": int(len(joined)),
    }

    # Model-quality view: top-decile predictions per timestamp, not a trade strategy.
    q = 1.0 - cfg.top_quantile
    joined["score_rank_pct"] = joined.groupby("ts")["model_score"].rank(pct=True)
    top = joined[joined["score_rank_pct"] >= q].copy()
    metrics.update(_selection_metrics(top, y, "top_decile"))

    # Top-1 per timestamp: what the model says is the best option in each snapshot.
    top1 = joined.sort_values(["ts", "model_score"], ascending=[True, False]).groupby("ts", as_index=False).head(1)
    metrics.update(_selection_metrics(top1, y, "top1_per_snapshot"))

    # Calibration bins for probability model.
    metrics["probability_calibration"] = _calibration_bins(joined, "prob_profit", "future_positive")
    metrics["model_quality_gate_passed"] = bool(
        metrics.get("return_spearman_ic", 0) > 0
        and metrics.get("rank_spearman_ic", 0) > 0
        and metrics.get("top_decile_mean_return", 0) > metrics.get("universe_mean_return", 0)
        and metrics.get("top_decile_win_rate", 0) > metrics.get("universe_win_rate", 0)
    )
    metrics["passed_live_gate"] = bool(
        metrics.get("model_quality_gate_passed", False)
        and metrics.get("top1_per_snapshot_count", 0) >= cfg.min_live_trades
        and metrics.get("top1_per_snapshot_win_rate", 0.0) >= cfg.min_live_win_rate
        and metrics.get("top1_per_snapshot_sharpe", 0.0) >= cfg.min_live_sharpe
        and metrics.get("top1_per_snapshot_mean_return", 0.0) > 0
        and metrics.get("top1_per_snapshot_alpha_vs_universe", 0.0) > 0
    )
    metrics["live_gate"] = {
        "min_trades": cfg.min_live_trades,
        "min_win_rate": cfg.min_live_win_rate,
        "min_sharpe": cfg.min_live_sharpe,
        "requires_positive_mean_return": True,
        "requires_positive_alpha_vs_universe": True,
        "requires_groww_source": cfg.require_groww_source,
    }
    metrics["training_data_policy"] = "live_groww_collected_rows_only_no_synthetic"
    return metrics


def _selection_metrics(sel: pd.DataFrame, universe_y: pd.Series, prefix: str) -> dict[str, Any]:
    if sel.empty:
        return {
            f"{prefix}_count": 0,
            f"{prefix}_mean_return": 0.0,
            f"{prefix}_win_rate": 0.0,
            f"{prefix}_sharpe": 0.0,
            f"{prefix}_alpha_vs_universe": 0.0,
        }
    r = sel["future_return_after_costs"].astype(float)
    return {
        f"{prefix}_count": int(len(sel)),
        f"{prefix}_mean_return": float(r.mean()),
        f"{prefix}_median_return": float(r.median()),
        f"{prefix}_win_rate": float((r > 0).mean()),
        f"{prefix}_sharpe": _sharpe(r),
        f"{prefix}_alpha_vs_universe": float(r.mean() - universe_y.mean()),
        f"{prefix}_avg_mfe": float(sel["future_mfe_after_costs"].mean()) if "future_mfe_after_costs" in sel else None,
        f"{prefix}_avg_mae": float(sel["future_mae_after_costs"].mean()) if "future_mae_after_costs" in sel else None,
    }


def _calibration_bins(df: pd.DataFrame, proba_col: str, label_col: str, bins: int = 10) -> list[dict[str, Any]]:
    if df.empty or proba_col not in df or label_col not in df:
        return []
    tmp = df[[proba_col, label_col]].dropna().copy()
    if tmp.empty:
        return []
    tmp["bin"] = pd.qcut(tmp[proba_col].rank(method="first"), q=min(bins, len(tmp)), duplicates="drop")
    out: list[dict[str, Any]] = []
    for _, sub in tmp.groupby("bin", observed=True):
        out.append({
            "count": int(len(sub)),
            "avg_predicted_prob": float(sub[proba_col].mean()),
            "realized_win_rate": float(sub[label_col].mean()),
        })
    return out


def _feature_importance(model: Any, X: pd.DataFrame, y: pd.Series, features: list[str], cfg: ModelSuiteConfig) -> list[dict[str, Any]]:
    n = min(len(X), cfg.permutation_importance_rows)
    if n <= 100:
        return []
    sample = X.tail(n)
    target = y.loc[sample.index]
    result = permutation_importance(model, sample, target, n_repeats=3, random_state=cfg.random_state, scoring="neg_mean_absolute_error")
    imp = pd.DataFrame({"feature": features, "importance": result.importances_mean}).sort_values("importance", ascending=False)
    return imp.head(25).to_dict(orient="records")


def _sharpe(x: pd.Series) -> float:
    x = pd.to_numeric(x, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if len(x) < 2:
        return 0.0
    sd = x.std(ddof=0)
    if sd == 0 or not np.isfinite(sd):
        return 0.0
    return float(x.mean() / sd * np.sqrt(len(x)))


def _spearman(a: pd.Series, b: pd.Series) -> float:
    aa = pd.to_numeric(a, errors="coerce")
    bb = pd.to_numeric(b, errors="coerce")
    mask = aa.notna() & bb.notna()
    if mask.sum() < 3:
        return 0.0
    return float(aa[mask].rank().corr(bb[mask].rank()))


def _safe_div(a: Any, b: Any) -> pd.Series:
    aa = _num(a, np.nan)
    bb = _num(b, np.nan)
    with np.errstate(divide="ignore", invalid="ignore"):
        out = aa / bb.replace(0, np.nan)
    return out.replace([np.inf, -np.inf], np.nan)


def _num(x: Any, default: float = np.nan) -> pd.Series:
    if isinstance(x, pd.Series):
        return pd.to_numeric(x, errors="coerce")
    if isinstance(x, np.ndarray):
        return pd.Series(x).astype(float)
    if x is None:
        return pd.Series(dtype=float)
    try:
        return pd.Series(x).astype(float)
    except Exception:
        return pd.Series(default)


def _pct_rank(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").rank(pct=True).fillna(0.5)

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
from sklearn.isotonic import IsotonicRegression
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

from .config import AssetProfile, BotConfig
from .session import add_market_session_features
from .storage import Store

log = logging.getLogger(__name__)

MODEL_VERSION = "option_model_suite_v6_shadow_meta_policy"
GROWW_CHAIN_SOURCES = ("groww_option_chain", "groww_instrument_quote_chain")


def metrics_summary_for_log(metrics: dict[str, Any]) -> str:
    def fmt(value: Any, digits: int = 4) -> str:
        try:
            out = float(value)
        except (TypeError, ValueError):
            return "NA"
        if not np.isfinite(out):
            return "NA"
        return f"{out:.{digits}f}"

    if not metrics:
        return "metrics=unavailable"
    return (
        f"passed_live_gate={bool(metrics.get('passed_live_gate', False))} "
        f"quality_gate={bool(metrics.get('model_quality_gate_passed', False))} "
        f"labelled_rows={metrics.get('labelled_rows', 'NA')} "
        f"min_live_rows={(metrics.get('live_gate') or {}).get('min_live_rows', 'NA')} "
        f"train_rows={metrics.get('train_rows', 'NA')} "
        f"test_rows={metrics.get('test_rows', 'NA')} "
        f"snapshots={metrics.get('snapshots_evaluated', 'NA')} "
        f"median_width={fmt(metrics.get('median_options_per_snapshot'), 1)} "
        f"return_ic={fmt(metrics.get('return_spearman_ic'))} "
        f"rank_ic={fmt(metrics.get('rank_spearman_ic'))} "
        f"top_decile_count={metrics.get('top_decile_count', 'NA')} "
        f"top_decile_win_rate={fmt(metrics.get('top_decile_win_rate'))} "
        f"top_decile_sharpe={fmt(metrics.get('top_decile_sharpe'))} "
        f"top_decile_alpha={fmt(metrics.get('top_decile_alpha_vs_universe'))} "
        f"top1_count={metrics.get('top1_per_snapshot_count', 'NA')} "
        f"top1_win_rate={fmt(metrics.get('top1_per_snapshot_win_rate'))} "
        f"top1_sharpe={fmt(metrics.get('top1_per_snapshot_sharpe'))} "
        f"top1_alpha={fmt(metrics.get('top1_per_snapshot_alpha_vs_universe'))} "
        f"adaptive_top1_win_rate={fmt(metrics.get('adaptive_top1_per_snapshot_win_rate'))} "
        f"adaptive_top1_sharpe={fmt(metrics.get('adaptive_top1_per_snapshot_sharpe'))} "
        f"adaptive_top1_alpha={fmt(metrics.get('adaptive_top1_per_snapshot_alpha_vs_universe'))} "
        f"shadow_trades={metrics.get('shadow_policy_count', 'NA')} "
        f"shadow_win_rate={fmt(metrics.get('shadow_policy_win_rate'))} "
        f"shadow_sharpe={fmt(metrics.get('shadow_policy_sharpe'))} "
        f"strategy_trades={metrics.get('strategy_policy_count', 'NA')} "
        f"strategy_win_rate={fmt(metrics.get('strategy_policy_win_rate'))} "
        f"strategy_sharpe={fmt(metrics.get('strategy_policy_sharpe'))} "
        f"strategy_alpha={fmt(metrics.get('strategy_policy_alpha_vs_universe'))} "
        f"strategy_target_hit={fmt(metrics.get('strategy_policy_target_hit_rate'))} "
        f"strategy_stop_hit={fmt(metrics.get('strategy_policy_stop_hit_rate'))} "
        f"strategy_mode={metrics.get('strategy_policy_selection_mode', 'NA')} "
        f"strategy_meta_prob={fmt(metrics.get('strategy_policy_avg_meta_prob'))} "
        f"strategy_meta_ev={fmt(metrics.get('strategy_policy_avg_meta_ev'))}"
    )

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
    "local_market_minute",
    "session_elapsed_pct",
    "minutes_to_session_close",
    "minutes_since_session_open",
    "is_session_open",
    "is_opening_30m",
    "is_closing_30m",
    "is_after_equity_close",
    "is_late_session",
    "commodity_late_session",
    "weekday",
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
    "snapshot_option_count",
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

META_POLICY_FEATURES: list[str] = [
    "predicted_return",
    "prob_profit",
    "prob_iv_expansion",
    "rank_score",
    "return_q20",
    "return_q50",
    "return_q80",
    "estimated_cost",
    "edge_score",
    "model_score",
    "model_rank_at_ts",
    "adaptive_tp_pct",
    "adaptive_sl_pct",
    "adaptive_rr",
    "adaptive_breakeven_prob",
    "adaptive_exit_score",
    "spread_pct",
    "bid_quantity",
    "ask_quantity",
    "volume",
    "open_interest",
    "book_pressure_score",
    "depth_imbalance_5",
    "top_book_imbalance",
    "minutes_to_session_close",
    "session_elapsed_pct",
    "time_to_expiry_days",
    "moneyness",
    "abs_moneyness",
    "theta_to_premium",
    "iv",
    "iv_z",
    "premium_return_3",
    "premium_realized_vol_20",
    "snapshot_option_count",
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


def _strategy_score_weights(strategy_name: str) -> dict[str, float]:
    strategies = {
        "index_cross_sectional_premium_expansion": {
            "predicted_return": 0.40,
            "rank_score": 0.25,
            "prob_profit": 0.20,
            "prob_iv_expansion": 0.10,
            "upside_spread": 0.05,
            "estimated_cost": 0.15,
        },
        "energy_volatility_liquidity_breakout": {
            "predicted_return": 0.34,
            "rank_score": 0.18,
            "prob_profit": 0.16,
            "prob_iv_expansion": 0.22,
            "upside_spread": 0.10,
            "estimated_cost": 0.22,
        },
        "energy_trend_vol_premium_expansion": {
            "predicted_return": 0.44,
            "rank_score": 0.20,
            "prob_profit": 0.16,
            "prob_iv_expansion": 0.10,
            "upside_spread": 0.10,
            "estimated_cost": 0.20,
        },
    }
    return strategies.get(strategy_name, strategies["index_cross_sectional_premium_expansion"])


@dataclass(frozen=True)
class ModelSuiteConfig:
    asset_id: str = "nifty"
    underlying: str = "NIFTY"
    exchange: str = "NSE"
    segment: str = "FNO"
    strategy_name: str = "index_cross_sectional_premium_expansion"
    horizon_rows: int = 12
    cost_bps: float = 35.0
    min_rows: int = 5000
    min_live_rows: int = 5000
    test_fraction: float = 0.25
    top_quantile: float = 0.10
    random_state: int = 42
    permutation_importance_rows: int = 2500
    min_live_trades: int = 80
    min_live_win_rate: float = 0.54
    min_live_sharpe: float = 0.60
    require_groww_source: bool = True
    min_snapshot_width: int = 4
    uncertainty_buffer: float = 0.003
    min_edge_return: float = 0.006
    max_spread_pct: float = 0.025
    min_volume: float = 1.0
    min_oi: float = 1.0
    session_close_buffer_minutes: float = 12.0
    max_model_rank: int = 5
    calibration_fraction: float = 0.20
    min_meta_policy_prob: float = 0.55
    min_meta_policy_ev: float = 0.0
    min_meta_policy_candidates: int = 200
    min_meta_policy_class_count: int = 25
    allow_short_option_entries: bool = False
    short_tp_pct: float = 0.01
    short_sl_pct: float = 0.025

    @classmethod
    def from_bot_config(cls, cfg: BotConfig, asset: AssetProfile | None = None) -> "ModelSuiteConfig":
        profile = asset or cfg.get_asset_profile("nifty")
        return cls(
            asset_id=profile.asset_id,
            underlying=profile.underlying,
            exchange=profile.exchange,
            segment=profile.segment,
            strategy_name=profile.model_strategy,
            horizon_rows=cfg.label_horizon_rows,
            cost_bps=cfg.estimated_round_trip_cost_bps,
            min_rows=cfg.model_suite_shadow_min_rows,
            min_live_rows=cfg.live_train_min_rows,
            test_fraction=cfg.test_fraction,
            min_live_trades=profile.min_backtest_trades or cfg.min_backtest_trades,
            min_live_win_rate=cfg.min_model_win_rate,
            min_live_sharpe=cfg.min_model_sharpe,
            require_groww_source=cfg.require_groww_source,
            uncertainty_buffer=cfg.uncertainty_buffer,
            min_edge_return=profile.min_edge_return,
            max_spread_pct=profile.max_spread_pct,
            min_volume=profile.min_volume,
            min_oi=profile.min_oi,
            min_meta_policy_prob=cfg.min_meta_policy_prob,
            min_meta_policy_ev=cfg.min_meta_policy_ev,
            min_meta_policy_candidates=cfg.min_meta_policy_candidates,
            min_meta_policy_class_count=cfg.min_meta_policy_class_count,
            allow_short_option_entries=profile.allow_short_option_entries,
            short_tp_pct=profile.short_tp_pct,
            short_sl_pct=profile.short_sl_pct,
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
        spread_pct = _num(df.get("spread_pct"), 0.05).fillna(0.05).clip(lower=0.0, upper=0.50)
        depth_imb = _num(df.get("depth_imbalance_5"), 0.0).fillna(0.0).abs().clip(lower=0.0, upper=1.0)
        volume_rank = _num(df.get("volume_rank"), 0.0).fillna(0.0).clip(0, 1)
        low_liq_penalty = 0.0025 * (1.0 - volume_rank)
        imbalance_penalty = 0.0015 * depth_imb
        return (spread_pct + low_liq_penalty + imbalance_penalty).to_numpy(dtype=float)


@dataclass
class ConstantProbabilityModel:
    """Fallback for early shadow fits when a binary target has one observed class."""

    positive_probability: float

    def fit(self, X: pd.DataFrame, y: pd.Series | None = None) -> "ConstantProbabilityModel":
        return self

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        p = float(np.clip(self.positive_probability, 0.0, 1.0))
        return np.tile(np.array([[1.0 - p, p]], dtype=float), (len(X), 1))

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        return np.full(len(X), int(self.positive_probability >= 0.5), dtype=int)


@dataclass
class IdentityProbabilityCalibrator:
    """Fallback calibrator when a proper monotonic calibration set is unavailable."""

    reason: str = "identity"

    def predict(self, values: Any) -> np.ndarray:
        return np.asarray(pd.to_numeric(pd.Series(values), errors="coerce").fillna(0.0).clip(0.0, 1.0), dtype=float)


class OptionModelSuite:
    """Model-only layer for option assets.

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
        data = _prepare_training_frame(frame, self.feature_cols, min_snapshot_width=self.cfg.min_snapshot_width)
        if len(data) < self.cfg.min_rows:
            raise ValueError(
                "Not enough institutional labelled rows to train model suite: "
                f"{len(data)} < {self.cfg.min_rows} "
                f"(min_snapshot_width={self.cfg.min_snapshot_width})"
            )

        data = data.sort_values([c for c in ["ts", "asset_id", "expiry", "trading_symbol"] if c in data.columns]).reset_index(drop=True)
        train, test, split_meta = _chronological_snapshot_split(data, self.cfg.test_fraction)
        model_train, calibration, calibration_split_meta = _chronological_snapshot_split(train, self.cfg.calibration_fraction)
        if model_train.empty or calibration.empty:
            model_train = train.copy()
            calibration = train.iloc[0:0].copy()
            calibration_split_meta = {"policy": "no_calibration_split_fallback", "rows": 0}
        if train.empty or test.empty:
            raise ValueError("Train/test split produced an empty partition.")
        log.info(
            "model fit dataset | asset=%s strategy=%s labelled_rows=%s train_rows=%s calibration_rows=%s test_rows=%s features=%s horizon_rows=%s cost_bps=%.2f",
            self.cfg.asset_id,
            self.cfg.strategy_name,
            len(data),
            len(model_train),
            len(calibration),
            len(test),
            len(self.feature_cols),
            self.cfg.horizon_rows,
            self.cfg.cost_bps,
        )

        X_train = clean_matrix(model_train, self.feature_cols)
        X_test = clean_matrix(test, self.feature_cols)
        y_ret_train = model_train["future_return_after_costs"].astype(float)
        y_ret_test = test["future_return_after_costs"].astype(float)
        y_cls_train = model_train["future_positive"].astype(int)
        y_cls_test = test["future_positive"].astype(int)
        y_rank_train = model_train["future_rank_pct"].astype(float)
        y_rank_test = test["future_rank_pct"].astype(float)
        y_iv_train = model_train["future_iv_up"].astype(int)
        y_iv_test = test["future_iv_up"].astype(int)

        return_model, return_model_type = _make_return_regressor(self.cfg.random_state)
        log.info("model fit stage | asset=%s target=future_return model=%s", self.cfg.asset_id, return_model_type)
        return_model.fit(X_train, y_ret_train)
        self.models["return_regressor"] = return_model

        classifier, classifier_type = _fit_binary_classifier(X_train, y_cls_train, self.cfg.random_state)
        log.info(
            "model fit stage | asset=%s target=future_positive model=%s positive_rate=%.4f classes=%s",
            self.cfg.asset_id,
            classifier_type,
            float(y_cls_train.mean()) if len(y_cls_train) else 0.0,
            int(y_cls_train.nunique()),
        )
        self.models["profit_classifier"] = classifier

        ranker, ranker_type = _make_ranker(self.cfg.random_state)
        log.info("model fit stage | asset=%s target=future_rank_pct model=%s", self.cfg.asset_id, ranker_type)
        _fit_ranker(ranker, X_train, y_rank_train, model_train)
        self.models["cross_sectional_ranker"] = ranker

        q20, q20_type = _make_quantile_regressor(0.20, self.cfg.random_state)
        q50, q50_type = _make_quantile_regressor(0.50, self.cfg.random_state)
        q80, q80_type = _make_quantile_regressor(0.80, self.cfg.random_state)
        log.info("model fit stage | asset=%s target=return_quantiles models=%s,%s,%s", self.cfg.asset_id, q20_type, q50_type, q80_type)
        q20.fit(X_train, y_ret_train)
        q50.fit(X_train, y_ret_train)
        q80.fit(X_train, y_ret_train)
        self.models["return_q20"] = q20
        self.models["return_q50"] = q50
        self.models["return_q80"] = q80

        iv_model, iv_model_type = _fit_binary_classifier(X_train, y_iv_train, self.cfg.random_state + 1)
        log.info(
            "model fit stage | asset=%s target=future_iv_up model=%s positive_rate=%.4f classes=%s",
            self.cfg.asset_id,
            iv_model_type,
            float(y_iv_train.mean()) if len(y_iv_train) else 0.0,
            int(y_iv_train.nunique()),
        )
        self.models["iv_expansion_classifier"] = iv_model

        calibration_meta: dict[str, Any] = {"rows": int(len(calibration)), "policy": "identity_no_calibration_rows"}
        meta_model_type = "not_fitted_no_calibration_rows"
        if not calibration.empty:
            raw_calib = self._predict_base(calibration, calibrated=False)
            calibrator, calibrator_type = _fit_probability_calibrator(raw_calib["prob_profit"], calibration["future_positive"])
            self.models["profit_probability_calibrator"] = calibrator
            calibration_meta = _probability_calibration_meta(raw_calib["prob_profit"], calibration["future_positive"], calibrator_type)

            calibrated_calib = self.predict(calibration)
            meta_model, meta_model_type, meta_training = _fit_meta_policy_model(calibration, calibrated_calib, self.cfg)
            if meta_model is not None:
                self.models["policy_meta_classifier"] = meta_model
            calibration_meta["meta_policy_training"] = meta_training

        pred = self.predict(test)
        self.metrics = _evaluate_predictions(test, pred, self.cfg, labelled_rows=len(data))
        self.metrics["model_types"] = {
            "return_regressor": return_model_type,
            "profit_classifier": classifier_type,
            "cross_sectional_ranker": ranker_type,
            "return_q20": q20_type,
            "return_q50": q50_type,
            "return_q80": q80_type,
            "iv_expansion_classifier": iv_model_type,
            "profit_probability_calibrator": calibration_meta.get("calibrator_type", "identity"),
            "policy_meta_classifier": meta_model_type,
            "cost_model": "deterministic_spread_depth_cost_model",
        }
        self.metrics["created_at_utc"] = datetime.utcnow().isoformat()
        self.metrics["version"] = MODEL_VERSION
        self.metrics["config"] = asdict(self.cfg)
        self.metrics["feature_cols"] = self.feature_cols
        self.metrics["train_rows"] = int(len(model_train))
        self.metrics["calibration_rows"] = int(len(calibration))
        self.metrics["test_rows"] = int(len(test))
        self.metrics["split_policy"] = split_meta
        self.metrics["calibration_split_policy"] = calibration_split_meta
        self.metrics["calibration_policy"] = calibration_meta
        try:
            self.metrics["feature_importance"] = _feature_importance(return_model, X_test, y_ret_test, self.feature_cols, self.cfg)
        except Exception as exc:
            self.metrics["feature_importance_error"] = str(exc)
        log.info("model fit metrics | asset=%s %s", self.cfg.asset_id, metrics_summary_for_log(self.metrics))
        return self

    def _predict_base(self, frame: pd.DataFrame, calibrated: bool = True) -> pd.DataFrame:
        if not self.models:
            raise ValueError("Model suite is not fitted/loaded.")
        df = frame.copy()
        X = clean_matrix(df, self.feature_cols)
        out = df[[c for c in ["ts", "asset_id", "underlying", "exchange", "segment", "expiry", "strike", "option_type", "trading_symbol", "ltp"] if c in df.columns]].copy()
        out["predicted_return"] = self.models["return_regressor"].predict(X)
        out["rank_score"] = _predict_any(self.models["cross_sectional_ranker"], X)
        out["return_q20"] = self.models["return_q20"].predict(X)
        out["return_q50"] = self.models["return_q50"].predict(X)
        out["return_q80"] = self.models["return_q80"].predict(X)
        raw_profit = _predict_positive_proba(self.models["profit_classifier"], X)
        out["raw_prob_profit"] = raw_profit
        out["prob_profit"] = _apply_probability_calibrator(self.models.get("profit_probability_calibrator"), raw_profit) if calibrated else raw_profit
        out["prob_iv_expansion"] = _predict_positive_proba(self.models["iv_expansion_classifier"], X)
        out["estimated_cost"] = self.cost_model.predict_cost(df)
        _add_adaptive_exit_forecast(out)
        weights = _strategy_score_weights(self.cfg.strategy_name)
        rank_groups = _snapshot_group_cols(out)
        upside_spread = out["return_q80"] - out["return_q20"].abs()
        out["model_score"] = (
            weights["predicted_return"] * _pct_rank_by_group(out, out["predicted_return"], rank_groups)
            + weights["rank_score"] * _pct_rank_by_group(out, out["rank_score"], rank_groups)
            + weights["prob_profit"] * out["prob_profit"].clip(0, 1)
            + weights["prob_iv_expansion"] * out["prob_iv_expansion"].clip(0, 1)
            + weights["upside_spread"] * _pct_rank_by_group(out, upside_spread, rank_groups)
            - weights["estimated_cost"] * _pct_rank_by_group(out, out["estimated_cost"], rank_groups)
        )
        out["edge_score"] = out["predicted_return"] - self.cfg.uncertainty_buffer
        if "ts" in out.columns:
            out["model_rank_at_ts"] = out.groupby(rank_groups)["model_score"].rank(ascending=False, method="first")
        return out.sort_values(["ts", "model_score"] if "ts" in out.columns else ["model_score"], ascending=[True, False] if "ts" in out.columns else False)

    def predict(self, frame: pd.DataFrame) -> pd.DataFrame:
        out = self._predict_base(frame, calibrated=True)
        return _add_meta_policy_forecast(out, frame, self.models.get("policy_meta_classifier"), self.cfg)

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


def train_model_suite_from_store(bot_cfg: BotConfig, store: Store, asset: AssetProfile | str | None = None) -> ModelTrainingResult:
    profile = bot_cfg.get_asset_profile(asset) if isinstance(asset, str) else (asset or bot_cfg.get_asset_profile("nifty"))
    suite_cfg = ModelSuiteConfig.from_bot_config(bot_cfg, profile)
    readiness = training_readiness_from_store(bot_cfg, store, profile)
    log.info(
        "training readiness | asset=%s ready=%s labelled_rows=%s/%s live_rows=%s/%s mode=%s min_snapshot_width=%s raw_rows=%s symbols=%s max_symbol_snapshots=%s horizon_rows=%s reason=%s",
        profile.asset_id,
        bool(readiness.get("ready")),
        readiness.get("estimated_labelled_rows"),
        readiness.get("min_rows_required"),
        readiness.get("estimated_labelled_rows"),
        readiness.get("min_live_rows_required"),
        readiness.get("training_mode"),
        readiness.get("min_snapshot_width"),
        readiness.get("raw_rows"),
        readiness.get("symbols"),
        readiness.get("max_snapshots_per_symbol"),
        readiness.get("horizon_rows"),
        readiness.get("reason"),
    )
    if not readiness.get("ready"):
        raise ValueError(
            "training warm-up | "
            f"asset={profile.asset_id} "
            f"labelled_rows={readiness['estimated_labelled_rows']}/{readiness['min_rows_required']} "
            f"live_rows={readiness.get('estimated_labelled_rows')}/{readiness.get('min_live_rows_required')} "
            f"mode={readiness.get('training_mode')} "
            f"min_snapshot_width={readiness.get('min_snapshot_width')} "
            f"raw_rows={readiness['raw_rows']} "
            f"symbols={readiness['symbols']} "
            f"max_symbol_snapshots={readiness['max_snapshots_per_symbol']} "
            f"horizon_rows={readiness['horizon_rows']} "
            f"reason={readiness['reason']}"
        )
    raw = load_option_model_raw_data(bot_cfg, store, profile)
    frame = build_option_model_frame(raw, horizon_rows=suite_cfg.horizon_rows, cost_bps=suite_cfg.cost_bps)
    prepared_rows = len(_prepare_training_frame(frame, CORE_FEATURES, min_snapshot_width=suite_cfg.min_snapshot_width))
    log.info(
        "training frame built | asset=%s raw_rows=%s frame_rows=%s institutional_labelled_rows=%s min_snapshot_width=%s features=%s strategy=%s",
        profile.asset_id,
        len(raw),
        len(frame),
        prepared_rows,
        suite_cfg.min_snapshot_width,
        len(CORE_FEATURES),
        suite_cfg.strategy_name,
    )
    suite = OptionModelSuite(suite_cfg).fit(frame)
    model_path = bot_cfg.model_suite_path_for(profile.asset_id)
    meta_path = bot_cfg.model_suite_meta_path_for(profile.asset_id)
    suite.save(model_path, meta_path)
    log.info(
        "model artifacts saved | asset=%s model_path=%s meta_path=%s %s",
        profile.asset_id,
        model_path,
        meta_path,
        metrics_summary_for_log(suite.metrics),
    )

    metrics = suite.metrics
    # Persist a model-run row without changing the old table contract.
    store.append_df(
        "backtest_metrics",
        pd.DataFrame([
            {
                "ts": datetime.utcnow(),
                "asset_id": profile.asset_id,
                "underlying": profile.underlying,
                "exchange": profile.exchange,
                "segment": profile.segment,
                "model_path": str(model_path),
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
    return ModelTrainingResult(model_path, meta_path, metrics)


def training_readiness_from_store(bot_cfg: BotConfig, store: Store, asset: AssetProfile | str | None = None) -> dict[str, Any]:
    profile = bot_cfg.get_asset_profile(asset) if isinstance(asset, str) else (asset or bot_cfg.get_asset_profile("nifty"))
    suite_cfg = ModelSuiteConfig.from_bot_config(bot_cfg, profile)
    chain_sources = "', '".join(GROWW_CHAIN_SOURCES)
    chain_source_clause = f"AND source IN ('{chain_sources}')" if bot_cfg.require_groww_source else ""
    try:
        rows = store.query_df(
            f"""
            WITH base AS (
                SELECT trading_symbol,
                       ts,
                       expiry,
                       count(*) OVER (PARTITION BY asset_id, expiry, ts) AS snapshot_width
                FROM option_chain_snapshots
                WHERE trading_symbol IS NOT NULL
                  AND asset_id = ?
                  AND ltp IS NOT NULL
                  AND ltp BETWEEN ? AND ?
                  AND coalesce(volume, 0) >= ?
                  AND coalesce(open_interest, 0) >= ?
                  {chain_source_clause}
                  AND lower(coalesce(raw_json, '')) NOT LIKE '%synthetic%'
                  AND lower(coalesce(raw_json, '')) NOT LIKE '%dummy%'
                  AND lower(coalesce(raw_json, '')) NOT LIKE '%fake%'
            ),
            eligible AS (
                SELECT trading_symbol, ts
                FROM base
                WHERE snapshot_width >= ?
            ),
            by_symbol AS (
                SELECT trading_symbol,
                       count(*) AS snapshots,
                       min(ts) AS first_ts,
                       max(ts) AS last_ts
                FROM eligible
                GROUP BY trading_symbol
            )
            SELECT coalesce(sum(snapshots), 0) AS raw_rows,
                   count(*) AS symbols,
                   coalesce(sum(CASE WHEN snapshots > ? THEN snapshots - ? ELSE 0 END), 0) AS estimated_labelled_rows,
                   coalesce(max(snapshots), 0) AS max_snapshots_per_symbol,
                   coalesce(min(snapshots), 0) AS min_snapshots_per_symbol,
                   coalesce(avg(snapshots), 0) AS avg_snapshots_per_symbol,
                   min(first_ts) AS first_ts,
                   max(last_ts) AS last_ts
            FROM by_symbol
            """,
            (
                profile.asset_id,
                profile.min_ltp,
                profile.max_ltp,
                profile.min_volume,
                profile.min_oi,
                suite_cfg.min_snapshot_width,
                suite_cfg.horizon_rows,
                suite_cfg.horizon_rows,
            ),
        )
    except Exception as exc:
        return {
            "asset_id": profile.asset_id,
            "ready": False,
            "reason": f"readiness_query_failed:{exc}",
            "raw_rows": 0,
            "symbols": 0,
            "estimated_labelled_rows": 0,
            "min_rows_required": suite_cfg.min_rows,
            "min_live_rows_required": suite_cfg.min_live_rows,
            "min_snapshot_width": suite_cfg.min_snapshot_width,
            "horizon_rows": suite_cfg.horizon_rows,
            "max_snapshots_per_symbol": 0,
            "min_snapshots_per_symbol": 0,
            "avg_snapshots_per_symbol": 0.0,
            "first_ts": None,
            "last_ts": None,
        }

    row = rows.iloc[0].to_dict() if not rows.empty else {}
    raw_rows = int(row.get("raw_rows") or 0)
    symbols = int(row.get("symbols") or 0)
    labelled = int(row.get("estimated_labelled_rows") or 0)
    max_snapshots = int(row.get("max_snapshots_per_symbol") or 0)
    min_snapshots = int(row.get("min_snapshots_per_symbol") or 0)
    avg_snapshots = float(row.get("avg_snapshots_per_symbol") or 0.0)
    if raw_rows <= 0:
        reason = "no_eligible_groww_rows"
    elif max_snapshots <= suite_cfg.horizon_rows:
        reason = "waiting_for_future_horizon"
    elif labelled < suite_cfg.min_rows:
        reason = "need_more_labelled_rows"
    else:
        reason = "ready"
    return {
        "asset_id": profile.asset_id,
        "ready": labelled >= suite_cfg.min_rows,
        "reason": reason,
        "raw_rows": raw_rows,
        "symbols": symbols,
        "estimated_labelled_rows": labelled,
        "min_rows_required": int(suite_cfg.min_rows),
        "min_live_rows_required": int(suite_cfg.min_live_rows),
        "min_snapshot_width": int(suite_cfg.min_snapshot_width),
        "labelled_shortfall": max(0, int(suite_cfg.min_rows) - labelled),
        "live_labelled_shortfall": max(0, int(suite_cfg.min_live_rows) - labelled),
        "training_mode": "live_grade" if labelled >= suite_cfg.min_live_rows else "shadow_until_live_gate",
        "horizon_rows": int(suite_cfg.horizon_rows),
        "min_snapshots_for_first_label": int(suite_cfg.horizon_rows + 1),
        "max_snapshots_per_symbol": max_snapshots,
        "min_snapshots_per_symbol": min_snapshots,
        "avg_snapshots_per_symbol": avg_snapshots,
        "first_ts": row.get("first_ts"),
        "last_ts": row.get("last_ts"),
    }


def load_option_model_raw_data(cfg: BotConfig, store: Store, asset: AssetProfile | str | None = None) -> pd.DataFrame:
    """Load only real market snapshots collected by the Groww collector.

    Synthetic/sample rows are deliberately not supported. With require_groww_source=True in bot/config.py
    the trainer accepts only rows tagged by the live collector as groww_option_chain,
    groww_instrument_quote_chain, and groww_quote.
    """
    profile = cfg.get_asset_profile(asset) if isinstance(asset, str) else asset
    if profile is None:
        profile = cfg.get_asset_profile("nifty")
    chain_sources = "', '".join(GROWW_CHAIN_SOURCES)
    chain_source_clause = f"AND source IN ('{chain_sources}')" if cfg.require_groww_source else ""
    chain = store.query_df(
        f"""
        SELECT * FROM option_chain_snapshots
        WHERE trading_symbol IS NOT NULL
          AND asset_id = ?
          AND ltp IS NOT NULL
          AND ltp BETWEEN ? AND ?
          AND coalesce(volume, 0) >= ?
          AND coalesce(open_interest, 0) >= ?
          {chain_source_clause}
          AND lower(coalesce(raw_json, '')) NOT LIKE '%synthetic%'
          AND lower(coalesce(raw_json, '')) NOT LIKE '%dummy%'
          AND lower(coalesce(raw_json, '')) NOT LIKE '%fake%'
        ORDER BY trading_symbol, ts
        """,
        (profile.asset_id, profile.min_ltp, profile.max_ltp, profile.min_volume, profile.min_oi),
    )
    if chain.empty:
        return chain

    try:
        quote_source_clause = "AND source = 'groww_quote'" if cfg.require_groww_source else ""
        quotes = store.query_df(
            f"""
            SELECT * FROM quote_snapshots
            WHERE trading_symbol IS NOT NULL
              AND asset_id = ?
              {quote_source_clause}
              AND lower(coalesce(raw_json, '')) NOT LIKE '%synthetic%'
              AND lower(coalesce(raw_json, '')) NOT LIKE '%dummy%'
              AND lower(coalesce(raw_json, '')) NOT LIKE '%fake%'
            ORDER BY trading_symbol, ts
            """,
            (profile.asset_id,),
        )
    except Exception:
        quotes = pd.DataFrame()

    if quotes.empty:
        return chain
    return merge_chain_quotes(chain, quotes)


def merge_chain_quotes(chain: pd.DataFrame, quotes: pd.DataFrame, tolerance_seconds: int = 90) -> pd.DataFrame:
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
            direction="nearest",
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
    df = add_market_session_features(df)

    for col in [
        "strike", "underlying_ltp", "ltp", "open_interest", "volume", "delta", "gamma", "theta", "vega", "rho", "iv",
        "last_price", "bid_price", "bid_quantity", "offer_price", "offer_quantity", "spread", "spread_pct",
        "total_buy_quantity", "total_sell_quantity", "last_trade_quantity",
        "local_market_minute", "session_elapsed_pct", "minutes_to_session_close", "minutes_since_session_open",
        "is_session_open", "is_opening_30m", "is_closing_30m", "is_after_equity_close", "is_late_session",
        "commodity_late_session", "weekday",
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
    snapshot_cols = [col for col in ["asset_id", "ts", "expiry"] if col in df.columns]
    df["snapshot_option_count"] = df.groupby(snapshot_cols)["trading_symbol"].transform("nunique")
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

    # Cross-sectional label: rank options by future return inside each asset/expiry snapshot.
    rank_groups = [col for col in ["asset_id", "ts", "expiry"] if col in df.columns]
    df["future_rank_pct"] = df.groupby(rank_groups)["future_return_after_costs"].rank(pct=True)


def clean_matrix(df: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in feature_cols:
        if c not in out.columns:
            out[c] = np.nan
        out[c] = pd.to_numeric(out[c], errors="coerce")
    X = out[feature_cols].mask(lambda x: ~np.isfinite(x), np.nan)
    return X.fillna(0.0).astype(float)


def _snapshot_group_cols(df: pd.DataFrame) -> list[str]:
    return [col for col in ["asset_id", "ts", "expiry"] if col in df.columns]


def _chronological_snapshot_split(data: pd.DataFrame, test_fraction: float) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    group_cols = _snapshot_group_cols(data)
    if not group_cols:
        split_idx = int(len(data) * (1.0 - test_fraction))
        return (
            data.iloc[:split_idx].copy(),
            data.iloc[split_idx:].copy(),
            {"policy": "row_chronological_fallback", "group_cols": [], "test_fraction": test_fraction},
        )

    keys = data[group_cols].drop_duplicates().copy()
    sort_cols = [col for col in ["ts", "asset_id", "expiry"] if col in keys.columns]
    keys = keys.sort_values(sort_cols).reset_index(drop=True)
    if len(keys) < 3:
        split_idx = int(len(data) * (1.0 - test_fraction))
        return (
            data.iloc[:split_idx].copy(),
            data.iloc[split_idx:].copy(),
            {"policy": "row_chronological_small_group_fallback", "group_cols": group_cols, "groups": int(len(keys)), "test_fraction": test_fraction},
        )

    n_test = max(1, int(math.ceil(len(keys) * float(test_fraction))))
    n_train = max(1, len(keys) - n_test)
    test_keys = keys.iloc[n_train:].copy()
    marked = data.merge(test_keys.assign(_is_test_group=1), on=group_cols, how="left")
    train = marked[marked["_is_test_group"].isna()].drop(columns=["_is_test_group"]).copy()
    test = marked[marked["_is_test_group"].notna()].drop(columns=["_is_test_group"]).copy()
    return (
        train.reset_index(drop=True),
        test.reset_index(drop=True),
        {
            "policy": "whole_snapshot_chronological",
            "group_cols": group_cols,
            "train_groups": int(len(keys) - len(test_keys)),
            "test_groups": int(len(test_keys)),
            "test_fraction": test_fraction,
        },
    )


def _prepare_training_frame(frame: pd.DataFrame, feature_cols: list[str], min_snapshot_width: int = 1) -> pd.DataFrame:
    required = list(feature_cols) + ["future_return_after_costs", "future_positive", "future_rank_pct", "future_iv_up"]
    df = frame.copy()
    for c in required:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce")
        df[c] = df[c].mask(~np.isfinite(df[c]), np.nan)
    if "snapshot_option_count" in df.columns and min_snapshot_width > 1:
        df = df[df["snapshot_option_count"].fillna(0).ge(min_snapshot_width)].copy()
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


def _fit_binary_classifier(X: pd.DataFrame, y: pd.Series, seed: int) -> tuple[Any, str]:
    y_clean = pd.to_numeric(y, errors="coerce").fillna(0).astype(int)
    classes = int(y_clean.nunique())
    if classes < 2:
        p = float(y_clean.mean()) if len(y_clean) else 0.0
        model = ConstantProbabilityModel(p).fit(X, y_clean)
        return model, f"constant_probability_single_class_p={p:.4f}"
    model, model_type = _make_profit_classifier(seed)
    model.fit(X, y_clean)
    return model, model_type


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
        group_cols = [col for col in ["asset_id", "ts", "expiry"] if col in ordered.columns]
        ordered = ordered.sort_values(group_cols + ["_row"])
        X_ord = X.loc[ordered.index]
        y_ord = y.loc[ordered.index]
        # Convert percentile label to integer relevance grades 0..4.
        rel = np.floor(y_ord.clip(0, 1).to_numpy() * 5).clip(0, 4).astype(int)
        group = ordered.groupby(group_cols, sort=False).size().to_list()
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


def _apply_probability_calibrator(calibrator: Any, raw_probability: Any) -> pd.Series:
    raw = pd.to_numeric(pd.Series(raw_probability), errors="coerce").fillna(0.0).clip(0.0, 1.0)
    if calibrator is None:
        return raw
    try:
        calibrated = calibrator.predict(raw.to_numpy(dtype=float))
    except Exception:
        return raw
    return pd.Series(calibrated, index=raw.index).clip(0.0, 1.0).fillna(raw)


def _fit_probability_calibrator(raw_probability: Any, y: Any) -> tuple[Any, str]:
    raw = pd.to_numeric(pd.Series(raw_probability), errors="coerce").fillna(0.0).clip(0.0, 1.0)
    target = pd.to_numeric(pd.Series(y), errors="coerce").fillna(0).astype(int)
    mask = raw.notna() & target.notna()
    raw = raw[mask]
    target = target[mask]
    if len(raw) < 100 or target.nunique() < 2 or raw.nunique() < 5:
        return IdentityProbabilityCalibrator("insufficient_calibration_variation"), "identity_insufficient_calibration"
    model = IsotonicRegression(out_of_bounds="clip")
    model.fit(raw.to_numpy(dtype=float), target.to_numpy(dtype=int))
    return model, "isotonic_walk_forward_calibration"


def _probability_calibration_meta(raw_probability: Any, y: Any, calibrator_type: str) -> dict[str, Any]:
    raw = pd.to_numeric(pd.Series(raw_probability), errors="coerce").fillna(0.0).clip(0.0, 1.0)
    target = pd.to_numeric(pd.Series(y), errors="coerce").fillna(0).astype(int)
    return {
        "calibrator_type": calibrator_type,
        "rows": int(len(raw)),
        "positive_rate": float(target.mean()) if len(target) else 0.0,
        "raw_avg_probability": float(raw.mean()) if len(raw) else 0.0,
    }


def _copy_prediction_columns(frame: pd.DataFrame, pred: pd.DataFrame, cfg: ModelSuiteConfig) -> pd.DataFrame:
    joined = frame.copy().reset_index(drop=True)
    p = pred.sort_index().reset_index(drop=True)
    copy_cols = [
        "predicted_return",
        "raw_prob_profit",
        "prob_profit",
        "prob_iv_expansion",
        "rank_score",
        "return_q20",
        "return_q50",
        "return_q80",
        "model_score",
        "model_rank_at_ts",
        "edge_score",
        "estimated_cost",
        "adaptive_tp_pct",
        "adaptive_sl_pct",
        "adaptive_rr",
        "adaptive_breakeven_prob",
        "adaptive_exit_score",
        "meta_policy_prob",
        "meta_policy_ev",
        "meta_policy_edge",
        "meta_policy_active",
        "policy_calibrated_ev",
        "policy_raw_ev",
        "policy_score_rank_pct",
    ]
    for col in copy_cols:
        if col in p.columns:
            joined[col] = p[col]
    if "edge_score" not in joined.columns and "predicted_return" in joined.columns:
        joined["edge_score"] = joined["predicted_return"] - cfg.uncertainty_buffer
    return joined


def _meta_policy_matrix(scored: pd.DataFrame, source: pd.DataFrame | None = None) -> pd.DataFrame:
    data = scored.copy()
    if source is not None:
        source_aligned = source.reindex(data.index)
        for col in META_POLICY_FEATURES:
            if col not in data.columns and col in source_aligned.columns:
                data[col] = source_aligned[col]
    for col in META_POLICY_FEATURES:
        if col not in data.columns:
            data[col] = np.nan
    return clean_matrix(data, META_POLICY_FEATURES)


def _add_meta_policy_forecast(out: pd.DataFrame, source: pd.DataFrame, model: Any, cfg: ModelSuiteConfig) -> pd.DataFrame:
    scored = out.copy()
    normal = _normalise_policy_frame(scored, cfg)
    calibrated_ev, raw_ev = _policy_ev_columns(normal)
    scored["policy_calibrated_ev"] = calibrated_ev.reindex(scored.index)
    scored["policy_raw_ev"] = raw_ev.reindex(scored.index)
    scored["policy_score_rank_pct"] = _policy_rank_pct(normal).reindex(scored.index)
    tp = pd.to_numeric(scored.get("adaptive_tp_pct"), errors="coerce").fillna(0.0).clip(0.0, 0.65)
    sl = pd.to_numeric(scored.get("adaptive_sl_pct"), errors="coerce").fillna(0.0).clip(0.0, 0.40)
    cost = pd.to_numeric(scored.get("estimated_cost"), errors="coerce").fillna(0.0).clip(0.0, 0.30)
    net_tp = (tp - cost).clip(lower=0.001)
    net_sl = (sl + cost).clip(lower=0.001)
    if model is None:
        scored["meta_policy_active"] = 0.0
        scored["meta_policy_prob"] = np.nan
        scored["meta_policy_ev"] = np.nan
        scored["meta_policy_edge"] = np.nan
        return scored
    else:
        prob = _predict_positive_proba(model, _meta_policy_matrix(scored, source)).reindex(scored.index).fillna(0.0)
    scored["meta_policy_active"] = 1.0
    scored["meta_policy_prob"] = prob.clip(0.0, 1.0)
    scored["meta_policy_ev"] = (scored["meta_policy_prob"] * net_tp) - ((1.0 - scored["meta_policy_prob"]) * net_sl)
    scored["meta_policy_edge"] = scored["meta_policy_prob"] - pd.to_numeric(scored.get("adaptive_breakeven_prob"), errors="coerce").fillna(1.0)
    return scored


def _fit_meta_policy_model(calibration: pd.DataFrame, pred: pd.DataFrame, cfg: ModelSuiteConfig) -> tuple[Any | None, str, dict[str, Any]]:
    joined = _copy_prediction_columns(calibration, pred, cfg)
    candidates, gate_counts = _strategy_policy_candidate_frame(joined, cfg, mode="shadow")
    meta: dict[str, Any] = {
        "candidate_rows": int(len(candidates)),
        "gate_pass_counts": gate_counts,
        "target": "adaptive_stop_first_triple_barrier_positive",
        "candidate_policy": "candidate_for_meta_training_shadow_policy",
        "min_candidate_rows": cfg.min_meta_policy_candidates,
        "min_class_rows": cfg.min_meta_policy_class_count,
        "active": False,
    }
    ret, target_hit, stop_hit = _adaptive_exit_returns(candidates)
    y = (ret > 0).astype(int)
    positives = int(y.sum()) if len(y) else 0
    negatives = int(len(y) - positives) if len(y) else 0
    meta.update({
        "positive_rows": positives,
        "negative_rows": negatives,
        "positive_rate": float(y.mean()) if len(y) else 0.0,
        "target_hit_rate": float(target_hit.mean()) if len(target_hit) else 0.0,
        "stop_hit_rate": float(stop_hit.mean()) if len(stop_hit) else 0.0,
    })
    if len(candidates) < cfg.min_meta_policy_candidates:
        meta["reason"] = "insufficient_candidate_rows"
        return None, "inactive_insufficient_meta_candidates", meta

    if positives < cfg.min_meta_policy_class_count or negatives < cfg.min_meta_policy_class_count:
        meta["reason"] = "insufficient_outcome_diversity"
        return None, "inactive_insufficient_meta_outcome_diversity", meta

    X = _meta_policy_matrix(candidates)
    model, model_type = _fit_binary_classifier(X, y, cfg.random_state + 7)
    meta["active"] = True
    meta["reason"] = "trained"
    return model, model_type, meta


def _add_adaptive_exit_forecast(out: pd.DataFrame) -> None:
    pred = _num(out.get("predicted_return"), 0.0).fillna(0.0)
    q20 = _num(out.get("return_q20"), 0.0).fillna(0.0)
    q50 = _num(out.get("return_q50"), 0.0).fillna(0.0)
    q80 = _num(out.get("return_q80"), 0.0).fillna(0.0)
    prob = _num(out.get("prob_profit"), 0.0).fillna(0.0).clip(0.0, 1.0)
    cost = _num(out.get("estimated_cost"), 0.0).fillna(0.0).clip(lower=0.0, upper=0.30)

    upside_candidates = pd.concat(
        [
            pred,
            q80,
            q50 + 0.50 * (q80 - q50).clip(lower=0.0),
            2.0 * cost,
        ],
        axis=1,
    )
    downside_candidates = pd.concat(
        [
            (-q20).clip(lower=0.0),
            (-q50).clip(lower=0.0),
            1.25 * cost,
        ],
        axis=1,
    )
    tp = upside_candidates.max(axis=1).clip(lower=0.03, upper=0.65)
    sl = downside_candidates.max(axis=1).clip(lower=0.02, upper=0.40)

    # Keep adaptive exits convex: do not accept tiny upside against a wide stop.
    min_rr = 1.15
    tp = pd.concat([tp, sl * min_rr], axis=1).max(axis=1).clip(upper=0.65)
    rr = _safe_div(tp, sl).mask(lambda x: ~np.isfinite(x), np.nan).fillna(0.0)
    breakeven_prob = _safe_div(sl + cost, tp + sl).clip(0.0, 1.0).fillna(1.0)

    out["adaptive_tp_pct"] = tp.astype(float)
    out["adaptive_sl_pct"] = sl.astype(float)
    out["adaptive_rr"] = rr.astype(float)
    out["adaptive_breakeven_prob"] = breakeven_prob.astype(float)
    out["adaptive_exit_score"] = (prob - breakeven_prob + 0.10 * (rr - min_rr).clip(lower=0.0)).astype(float)


def _evaluate_predictions(test: pd.DataFrame, pred: pd.DataFrame, cfg: ModelSuiteConfig, labelled_rows: int) -> dict[str, Any]:
    joined = _copy_prediction_columns(test, pred, cfg)
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
        "labelled_rows": int(labelled_rows),
        "rows_evaluated": int(len(joined)),
    }
    group_cols = [col for col in ["asset_id", "ts", "expiry"] if col in joined.columns]
    snapshot_sizes = joined.groupby(group_cols).size() if group_cols else pd.Series([len(joined)])
    metrics["snapshots_evaluated"] = int(len(snapshot_sizes))
    metrics["avg_options_per_snapshot"] = float(snapshot_sizes.mean()) if len(snapshot_sizes) else 0.0
    metrics["median_options_per_snapshot"] = float(snapshot_sizes.median()) if len(snapshot_sizes) else 0.0
    metrics["min_options_per_snapshot"] = int(snapshot_sizes.min()) if len(snapshot_sizes) else 0

    # Model-quality view: top-decile predictions per timestamp, not a trade strategy.
    q = 1.0 - cfg.top_quantile
    joined["score_rank_pct"] = joined.groupby(group_cols)["model_score"].rank(pct=True)
    top = joined[joined["score_rank_pct"] >= q].copy()
    metrics.update(_selection_metrics(top, y, "top_decile"))
    metrics.update(_adaptive_selection_metrics(top, y, "adaptive_top_decile"))

    # Top-1 per timestamp: what the model says is the best option in each snapshot.
    top1 = (
        joined.sort_values(group_cols + ["model_score"], ascending=[True] * len(group_cols) + [False])
        .groupby(group_cols, as_index=False)
        .head(1)
    )
    metrics.update(_selection_metrics(top1, y, "top1_per_snapshot"))
    metrics.update(_adaptive_selection_metrics(top1, y, "adaptive_top1_per_snapshot"))
    metrics.update(_strategy_policy_metrics(joined, y, cfg))

    # Calibration bins for probability model.
    metrics["probability_calibration"] = _calibration_bins(joined, "prob_profit", "future_positive")
    top_decile_quality = bool(
        metrics.get("return_spearman_ic", 0) > 0
        and metrics.get("rank_spearman_ic", 0) > 0
        and metrics.get("median_options_per_snapshot", 0) >= 4
        and metrics.get("top_decile_mean_return", 0) > metrics.get("universe_mean_return", 0)
        and metrics.get("top_decile_win_rate", 0) > metrics.get("universe_win_rate", 0)
        and metrics.get("adaptive_top_decile_mean_return", 0) > metrics.get("universe_mean_return", 0)
        and metrics.get("adaptive_top_decile_win_rate", 0) > metrics.get("universe_win_rate", 0)
    )
    min_quality_trades = max(10, min(20, int(cfg.min_live_trades)))
    strategy_quality = bool(
        metrics.get("median_options_per_snapshot", 0) >= 4
        and metrics.get("strategy_policy_count", 0) >= min_quality_trades
        and metrics.get("strategy_policy_mean_return", 0.0) > 0
        and metrics.get("strategy_policy_win_rate", 0.0) > metrics.get("universe_win_rate", 0.0)
        and metrics.get("strategy_policy_sharpe", 0.0) > 0
        and metrics.get("strategy_policy_alpha_vs_universe", 0.0) > 0
        and metrics.get("strategy_policy_barrier_quality_passed", False)
    )
    metrics["model_quality_gate_passed"] = bool(top_decile_quality or strategy_quality)
    metrics["model_quality_gate_components"] = {
        "top_decile_quality": top_decile_quality,
        "strategy_policy_quality": strategy_quality,
        "min_strategy_quality_trades": min_quality_trades,
        "strategy_policy_barrier_quality": bool(metrics.get("strategy_policy_barrier_quality_passed", False)),
    }
    metrics["passed_live_gate"] = bool(
        metrics.get("model_quality_gate_passed", False)
        and int(metrics.get("labelled_rows", 0)) >= cfg.min_live_rows
        and metrics.get("strategy_policy_count", 0) >= cfg.min_live_trades
        and metrics.get("strategy_policy_win_rate", 0.0) >= cfg.min_live_win_rate
        and metrics.get("strategy_policy_sharpe", 0.0) >= cfg.min_live_sharpe
        and metrics.get("strategy_policy_mean_return", 0.0) > 0
        and metrics.get("strategy_policy_alpha_vs_universe", 0.0) > 0
        and metrics.get("strategy_policy_barrier_quality_passed", False)
    )
    metrics["live_gate"] = {
        "min_live_rows": cfg.min_live_rows,
        "labelled_rows": int(labelled_rows),
        "min_trades": cfg.min_live_trades,
        "min_win_rate": cfg.min_live_win_rate,
        "min_sharpe": cfg.min_live_sharpe,
        "selection_policy": metrics.get("strategy_policy_selection_mode", "candidate_for_live_trade_pre_meta"),
        "exit_policy": "adaptive_tp_sl_stop_first_barrier",
        "requires_positive_mean_return": True,
        "requires_positive_alpha_vs_universe": True,
        "requires_target_hit_rate_gt_stop_hit_rate": True,
        "requires_groww_source": cfg.require_groww_source,
    }
    metrics["adaptive_exit_policy"] = {
        "description": "TP/SL are predicted from q20/q50/q80, expected return, probability, and execution cost; tested with conservative stop-first MFE/MAE barriers.",
        "tp_bounds": [0.03, 0.65],
        "sl_bounds": [0.02, 0.40],
        "min_rr": 1.15,
        "ambiguous_target_and_stop_hit": "stop_first",
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
    r = _close_returns_for_entry_side(sel)
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


def _entry_side_mask(sel: pd.DataFrame) -> pd.Series:
    if "entry_side" not in sel.columns:
        return pd.Series(False, index=sel.index)
    side = sel["entry_side"].astype(str).str.upper()
    return side.eq("SELL") | side.eq("SHORT")


def _close_returns_for_entry_side(sel: pd.DataFrame) -> pd.Series:
    close_ret = pd.to_numeric(sel["future_return_after_costs"], errors="coerce").fillna(0.0)
    short_mask = _entry_side_mask(sel)
    if bool(short_mask.any()):
        close_ret = close_ret.copy()
        close_ret.loc[short_mask] = -close_ret.loc[short_mask]
    return close_ret.astype(float)


def _adaptive_exit_returns(sel: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    if sel.empty:
        empty = pd.Series(dtype=float)
        return empty, empty.astype(bool), empty.astype(bool)
    def col(name: str, default: float) -> pd.Series:
        if name in sel.columns:
            return pd.to_numeric(sel[name], errors="coerce").fillna(default)
        return pd.Series(default, index=sel.index, dtype=float)

    close_ret = pd.to_numeric(sel["future_return_after_costs"], errors="coerce").fillna(0.0)
    mfe = pd.to_numeric(sel["future_mfe_after_costs"], errors="coerce").fillna(close_ret) if "future_mfe_after_costs" in sel.columns else close_ret.copy()
    mae = pd.to_numeric(sel["future_mae_after_costs"], errors="coerce").fillna(close_ret) if "future_mae_after_costs" in sel.columns else close_ret.copy()
    tp = col("adaptive_tp_pct", 0.03).clip(0.01, 0.65)
    sl = col("adaptive_sl_pct", 0.02).clip(0.01, 0.40)
    cost = col("estimated_cost", 0.0).clip(0.0, 0.30)

    net_tp = (tp - cost).clip(lower=0.001)
    net_sl = (sl + cost).clip(lower=0.001)
    target_hit = mfe >= net_tp
    stop_hit = mae <= -net_sl

    # Conservative barrier approximation: if both barriers are touched inside the horizon,
    # count the stop first because we do not have tick-level sequencing.
    ret = close_ret.copy()
    ret[target_hit & ~stop_hit] = net_tp[target_hit & ~stop_hit]
    ret[stop_hit] = -net_sl[stop_hit]
    return ret.astype(float), target_hit.astype(bool), stop_hit.astype(bool)


def _short_exit_returns(sel: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    if sel.empty:
        empty = pd.Series(dtype=float)
        return empty, empty.astype(bool), empty.astype(bool)

    def col(name: str, default: float) -> pd.Series:
        if name in sel.columns:
            return pd.to_numeric(sel[name], errors="coerce").fillna(default)
        return pd.Series(default, index=sel.index, dtype=float)

    close_ret = -pd.to_numeric(sel["future_return_after_costs"], errors="coerce").fillna(0.0)
    long_mfe = pd.to_numeric(sel["future_mfe_after_costs"], errors="coerce").fillna(sel["future_return_after_costs"]) if "future_mfe_after_costs" in sel.columns else -close_ret
    long_mae = pd.to_numeric(sel["future_mae_after_costs"], errors="coerce").fillna(sel["future_return_after_costs"]) if "future_mae_after_costs" in sel.columns else -close_ret
    tp = col("adaptive_tp_pct", 0.01).clip(0.001, 0.20)
    sl = col("adaptive_sl_pct", 0.025).clip(0.002, 0.30)
    cost = col("estimated_cost", 0.0).clip(0.0, 0.30)

    net_tp = (tp - cost).clip(lower=0.001)
    net_sl = (sl + cost).clip(lower=0.001)
    target_hit = (-long_mae) >= net_tp
    stop_hit = long_mfe >= net_sl

    # Conservative sequencing: if premium both drops to target and spikes to stop,
    # count the stop first because the dataset is snapshot-level, not tick-level.
    ret = close_ret.copy()
    ret[target_hit & ~stop_hit] = net_tp[target_hit & ~stop_hit]
    ret[stop_hit] = -net_sl[stop_hit]
    return ret.astype(float), target_hit.astype(bool), stop_hit.astype(bool)


def _policy_exit_returns(sel: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    long_ret, long_target, long_stop = _adaptive_exit_returns(sel)
    short_mask = _entry_side_mask(sel)
    if not bool(short_mask.any()):
        return long_ret, long_target, long_stop
    short_ret, short_target, short_stop = _short_exit_returns(sel)
    ret = long_ret.copy()
    target = long_target.copy()
    stop = long_stop.copy()
    ret.loc[short_mask] = short_ret.loc[short_mask]
    target.loc[short_mask] = short_target.loc[short_mask]
    stop.loc[short_mask] = short_stop.loc[short_mask]
    return ret.astype(float), target.astype(bool), stop.astype(bool)


def _mean_col(df: pd.DataFrame, name: str) -> float:
    if name not in df.columns:
        return 0.0
    value = pd.to_numeric(df[name], errors="coerce").mean()
    return float(value) if pd.notna(value) and np.isfinite(value) else 0.0


def _adaptive_selection_metrics(sel: pd.DataFrame, universe_y: pd.Series, prefix: str) -> dict[str, Any]:
    if sel.empty:
        return {
            f"{prefix}_count": 0,
            f"{prefix}_mean_return": 0.0,
            f"{prefix}_win_rate": 0.0,
            f"{prefix}_sharpe": 0.0,
            f"{prefix}_alpha_vs_universe": 0.0,
            f"{prefix}_target_hit_rate": 0.0,
            f"{prefix}_stop_hit_rate": 0.0,
            f"{prefix}_avg_tp_pct": 0.0,
            f"{prefix}_avg_sl_pct": 0.0,
            f"{prefix}_avg_rr": 0.0,
        }
    ret, target_hit, stop_hit = _policy_exit_returns(sel)
    return {
        f"{prefix}_count": int(len(ret)),
        f"{prefix}_mean_return": float(ret.mean()),
        f"{prefix}_median_return": float(ret.median()),
        f"{prefix}_win_rate": float((ret > 0).mean()),
        f"{prefix}_sharpe": _sharpe(ret),
        f"{prefix}_alpha_vs_universe": float(ret.mean() - universe_y.mean()),
        f"{prefix}_target_hit_rate": float(target_hit.mean()),
        f"{prefix}_stop_hit_rate": float(stop_hit.mean()),
        f"{prefix}_avg_tp_pct": _mean_col(sel, "adaptive_tp_pct"),
        f"{prefix}_avg_sl_pct": _mean_col(sel, "adaptive_sl_pct"),
        f"{prefix}_avg_rr": _mean_col(sel, "adaptive_rr"),
    }


def _normalise_policy_frame(joined: pd.DataFrame, cfg: ModelSuiteConfig) -> pd.DataFrame:
    data = joined.copy()
    numeric_defaults = {
        "predicted_return": 0.0,
        "prob_profit": 0.0,
        "raw_prob_profit": 0.0,
        "prob_iv_expansion": 0.0,
        "return_q20": 0.0,
        "return_q50": 0.0,
        "return_q80": 0.0,
        "estimated_cost": 0.0,
        "edge_score": np.nan,
        "model_score": 0.0,
        "spread_pct": np.nan,
        "bid_quantity": 0.0,
        "ask_quantity": 0.0,
        "volume": 0.0,
        "open_interest": 0.0,
        "book_pressure_score": 0.0,
        "minutes_to_session_close": np.nan,
        "model_rank_at_ts": np.nan,
        "adaptive_rr": 0.0,
        "adaptive_exit_score": 0.0,
        "adaptive_tp_pct": 0.0,
        "adaptive_sl_pct": 0.0,
        "adaptive_breakeven_prob": 1.0,
        "meta_policy_prob": np.nan,
        "meta_policy_ev": np.nan,
        "meta_policy_edge": np.nan,
        "meta_policy_active": 0.0,
        "policy_calibrated_ev": 0.0,
        "policy_raw_ev": 0.0,
        "policy_score_rank_pct": 0.0,
    }
    for col, default in numeric_defaults.items():
        if col not in data.columns:
            data[col] = default
        data[col] = pd.to_numeric(data[col], errors="coerce")
    data["edge_score"] = data["edge_score"].where(data["edge_score"].notna(), data["predicted_return"] - cfg.uncertainty_buffer)
    return data


def _policy_ev_columns(data: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    tp = data["adaptive_tp_pct"].fillna(0.0).clip(0.0, 0.65)
    sl = data["adaptive_sl_pct"].fillna(0.0).clip(0.0, 0.40)
    cost = data["estimated_cost"].fillna(0.0).clip(0.0, 0.30)
    net_tp = (tp - cost).clip(lower=0.001)
    net_sl = (sl + cost).clip(lower=0.001)
    calibrated_prob = data["prob_profit"].fillna(0.0).clip(0.0, 1.0)
    raw_prob = data["raw_prob_profit"].where(data["raw_prob_profit"].notna(), calibrated_prob).fillna(0.0).clip(0.0, 1.0)
    calibrated_ev = (calibrated_prob * net_tp) - ((1.0 - calibrated_prob) * net_sl)
    raw_ev = (raw_prob * net_tp) - ((1.0 - raw_prob) * net_sl)
    return calibrated_ev.astype(float), raw_ev.astype(float)


def _policy_rank_pct(data: pd.DataFrame) -> pd.Series:
    group_cols = _snapshot_group_cols(data)
    if not group_cols:
        return data["model_score"].rank(pct=True).fillna(0.0)
    return data.groupby(group_cols)["model_score"].rank(pct=True).fillna(0.0)


def _strategy_policy_named_gates(joined: pd.DataFrame, cfg: ModelSuiteConfig, mode: str = "live") -> tuple[pd.DataFrame, dict[str, pd.Series]]:
    data = _normalise_policy_frame(joined, cfg)
    mode = str(mode or "live").lower()
    if mode not in {"shadow", "live", "live_meta"}:
        raise ValueError(f"Unknown strategy policy mode: {mode}")

    calibrated_ev, raw_ev = _policy_ev_columns(data)
    score_rank_pct = _policy_rank_pct(data)
    data["policy_calibrated_ev"] = calibrated_ev
    data["policy_raw_ev"] = raw_ev
    data["policy_score_rank_pct"] = score_rank_pct

    if mode == "shadow":
        rank_limit = max(cfg.max_model_rank * 3, 12)
        rank_pool = data["model_rank_at_ts"].fillna(999).le(rank_limit) | score_rank_pct.ge(0.70)
        probability_ev_or_rank = (
            calibrated_ev.gt(0)
            | raw_ev.gt(0)
            | (rank_pool & data["edge_score"].fillna(-999).gt(0))
        )
        named_gates = {
            "shadow_edge": data["edge_score"].fillna(-999).ge(max(0.0, cfg.min_edge_return * 0.25)),
            "shadow_net_return": data["predicted_return"].fillna(-999).ge(data["estimated_cost"].fillna(0) + cfg.min_edge_return * 0.25),
            "shadow_upside_positive": data["return_q80"].fillna(-999).gt(0),
            "shadow_adaptive_path": data["adaptive_exit_score"].fillna(-999).gt(-0.05) | data["model_rank_at_ts"].fillna(999).le(1),
            "adaptive_rr": data["adaptive_rr"].fillna(0).ge(1.15),
            "probability_ev_or_rank": probability_ev_or_rank,
            "spread": data["spread_pct"].fillna(999).le(cfg.max_spread_pct),
            "offer_depth": data["ask_quantity"].fillna(0).gt(0),
            "volume": data["volume"].fillna(0).ge(cfg.min_volume),
            "open_interest": data["open_interest"].fillna(0).ge(cfg.min_oi),
            "session_time": data["minutes_to_session_close"].fillna(999).gt(cfg.session_close_buffer_minutes),
            "shadow_rank_pool": rank_pool,
        }
    else:
        rank_pool = data["model_rank_at_ts"].fillna(999).le(cfg.max_model_rank) | score_rank_pct.ge(0.88)
        probability_ev_or_rank = (
            calibrated_ev.gt(0)
            | (raw_ev.gt(0) & score_rank_pct.ge(0.85))
            | (
                data["model_rank_at_ts"].fillna(999).le(1)
                & data["adaptive_exit_score"].fillna(-999).gt(0)
                & data["edge_score"].fillna(-999).ge(cfg.min_edge_return)
            )
        )
        named_gates = {
            "edge": data["edge_score"].fillna(-999).ge(cfg.min_edge_return),
            "net_return": data["predicted_return"].fillna(-999).ge(data["estimated_cost"].fillna(0) + cfg.min_edge_return),
            "probability_ev_or_rank": probability_ev_or_rank,
            "median_return": data["return_q50"].fillna(-999).gt(0),
            "quantile_asymmetry": data["return_q80"].fillna(-999).gt(data["return_q20"].fillna(999).abs() * 0.75),
            "adaptive_exit": data["adaptive_exit_score"].fillna(-999).gt(0),
            "adaptive_rr": data["adaptive_rr"].fillna(0).ge(1.15),
            "spread": data["spread_pct"].fillna(999).le(cfg.max_spread_pct),
            "offer_depth": data["ask_quantity"].fillna(0).gt(0),
            "volume": data["volume"].fillna(0).ge(cfg.min_volume),
            "open_interest": data["open_interest"].fillna(0).ge(cfg.min_oi),
            "session_time": data["minutes_to_session_close"].fillna(999).gt(cfg.session_close_buffer_minutes),
            "rank_pool": rank_pool,
        }
        meta_active = data["meta_policy_active"].fillna(0).gt(0).any()
        if mode == "live_meta" and meta_active:
            named_gates["meta_policy_prob"] = data["meta_policy_prob"].fillna(0).ge(cfg.min_meta_policy_prob)
            named_gates["meta_policy_ev"] = data["meta_policy_ev"].fillna(-999).gt(cfg.min_meta_policy_ev)

    return data, named_gates


def _strategy_policy_candidate_frame(joined: pd.DataFrame, cfg: ModelSuiteConfig, mode: str = "live") -> tuple[pd.DataFrame, dict[str, int]]:
    data, named_gates = _strategy_policy_named_gates(joined, cfg, mode=mode)
    gates = pd.Series(True, index=data.index)
    gate_counts: dict[str, int] = {}
    for name, mask in named_gates.items():
        valid = mask.fillna(False)
        gate_counts[name] = int(valid.sum())
        gates &= valid
    return data[gates].copy(), gate_counts


def strategy_policy_gate_diagnostics(joined: pd.DataFrame, cfg: ModelSuiteConfig, mode: str = "live", limit: int = 20) -> list[dict[str, Any]]:
    data, named_gates = _strategy_policy_named_gates(joined, cfg, mode=mode)
    gate_names = list(named_gates)
    if not gate_names:
        return []

    diagnostics: list[dict[str, Any]] = []
    ranked = data.sort_values(["model_score", "edge_score"], ascending=False).head(limit)
    for _, row in ranked.iterrows():
        failed = [name for name, mask in named_gates.items() if not bool(mask.reindex([row.name]).fillna(False).iloc[0])]
        diagnostics.append({
            "trading_symbol": row.get("trading_symbol"),
            "model_rank_at_ts": row.get("model_rank_at_ts"),
            "model_score": row.get("model_score"),
            "edge_score": row.get("edge_score"),
            "prob_profit": row.get("prob_profit"),
            "raw_prob_profit": row.get("raw_prob_profit"),
            "policy_calibrated_ev": row.get("policy_calibrated_ev"),
            "policy_raw_ev": row.get("policy_raw_ev"),
            "adaptive_exit_score": row.get("adaptive_exit_score"),
            "meta_policy_active": row.get("meta_policy_active"),
            "meta_policy_prob": row.get("meta_policy_prob"),
            "meta_policy_ev": row.get("meta_policy_ev"),
            "failed_gates": failed,
        })
    return diagnostics


def _research_policy_base_frame(joined: pd.DataFrame, cfg: ModelSuiteConfig) -> pd.DataFrame:
    data = _normalise_policy_frame(joined, cfg)
    data["policy_calibrated_ev"], data["policy_raw_ev"] = _policy_ev_columns(data)
    data["policy_score_rank_pct"] = _policy_rank_pct(data)
    return data


def _historical_execution_mask(data: pd.DataFrame, cfg: ModelSuiteConfig) -> pd.Series:
    return (
        data["spread_pct"].fillna(999).le(cfg.max_spread_pct)
        & data["ask_quantity"].fillna(0).gt(0)
        & data["volume"].fillna(0).ge(cfg.min_volume)
        & data["open_interest"].fillna(0).ge(cfg.min_oi)
    )


def _historical_short_execution_mask(data: pd.DataFrame, cfg: ModelSuiteConfig) -> pd.Series:
    return (
        data["spread_pct"].fillna(999).le(cfg.max_spread_pct)
        & data["bid_quantity"].fillna(0).gt(0)
        & data["ask_quantity"].fillna(0).gt(0)
        & data["volume"].fillna(0).ge(cfg.min_volume)
        & data["open_interest"].fillna(0).ge(cfg.min_oi)
    )


def _quote_guard_research_mask(data: pd.DataFrame, cfg: ModelSuiteConfig) -> pd.Series:
    return (
        data["edge_score"].fillna(-999).gt(0)
        & data["predicted_return"].fillna(-999).gt(0)
        & data["adaptive_rr"].fillna(0).ge(1.05)
        & data["minutes_to_session_close"].fillna(999).gt(cfg.session_close_buffer_minutes)
    )


def _naturalgas_short_decay_mask(data: pd.DataFrame, cfg: ModelSuiteConfig) -> pd.Series:
    group_cols = _snapshot_group_cols(data)
    low_model_rank = _pct_rank_by_group(data, data["model_score"], group_cols, ascending=True).le(0.35)
    return (
        low_model_rank
        & data["minutes_to_session_close"].fillna(999).gt(cfg.session_close_buffer_minutes)
        & data["volume"].fillna(0).ge(cfg.min_volume)
        & data["open_interest"].fillna(0).ge(cfg.min_oi)
    )


def _select_research_strategy_policy(joined: pd.DataFrame, cfg: ModelSuiteConfig, policy_id: str) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Select one candidate per snapshot for a named validation/live policy.

    Policies ending in quote_guard intentionally do not require historical bid/ask
    availability during backtest. The live execution path still checks the current
    Groww quote, spread, depth, margin, duplicate orders, and contract permissions
    immediately before order placement.
    """
    data = _research_policy_base_frame(joined, cfg)
    group_cols = _snapshot_group_cols(data)
    policy_id = str(policy_id or "legacy_live_gate")

    if policy_id == "legacy_live_gate":
        meta_active = bool(data["meta_policy_active"].fillna(0).gt(0).any())
        eligible, gate_counts = _strategy_policy_candidate_frame(data, cfg, mode="live_meta" if meta_active else "live")
        selected = _select_policy_per_snapshot(eligible, group_cols, use_meta=meta_active)
        if not selected.empty:
            selected = selected.copy()
            selected["entry_side"] = "BUY"
            selected["exit_side"] = "SELL"
        return eligible, selected, {
            "policy_id": policy_id,
            "selection": "legacy_conviction_gate",
            "entry_side": "BUY",
            "exit_side": "SELL",
            "historical_execution_filter": True,
            "live_execution_guard": True,
            "gate_counts": gate_counts,
        }

    if policy_id.startswith("naturalgas_short_decay"):
        if not cfg.allow_short_option_entries:
            empty = data.iloc[0:0].copy()
            return empty, empty, {
                "policy_id": policy_id,
                "selection": "short_premium_decay_disabled",
                "entry_side": "SELL",
                "exit_side": "BUY",
                "historical_execution_filter": False,
                "live_execution_guard": True,
            }
        mask = _naturalgas_short_decay_mask(data, cfg)
        historical_execution_filter = policy_id.endswith("_executable")
        if historical_execution_filter:
            mask &= _historical_short_execution_mask(data, cfg)
        candidates = data[mask].copy()
        if not candidates.empty:
            candidates["entry_side"] = "SELL"
            candidates["exit_side"] = "BUY"
            candidates["adaptive_tp_pct"] = float(cfg.short_tp_pct)
            candidates["adaptive_sl_pct"] = float(cfg.short_sl_pct)
            candidates["adaptive_rr"] = _safe_div(candidates["adaptive_tp_pct"], candidates["adaptive_sl_pct"]).fillna(0.0)
            candidates["short_decay_score"] = (
                -candidates["model_score"].fillna(0.0)
                - candidates["spread_pct"].fillna(cfg.max_spread_pct).clip(lower=0.0)
                + 0.05 * candidates["book_pressure_score"].fillna(0.0)
            )
            selected = (
                candidates.sort_values(
                    group_cols + ["model_score", "spread_pct", "book_pressure_score"],
                    ascending=[True] * len(group_cols) + [True, True, False],
                )
                .groupby(group_cols, as_index=False)
                .head(1)
            )
        else:
            selected = candidates.copy()
        return candidates, selected, {
            "policy_id": policy_id,
            "selection": "naturalgas_low_long_score_short_premium_decay",
            "entry_side": "SELL",
            "exit_side": "BUY",
            "sort_cols": ["model_score", "spread_pct", "book_pressure_score"],
            "short_tp_pct": float(cfg.short_tp_pct),
            "short_sl_pct": float(cfg.short_sl_pct),
            "historical_execution_filter": historical_execution_filter,
            "live_execution_guard": True,
            "base_mask_rows": int(mask.sum()),
        }

    mask = _quote_guard_research_mask(data, cfg)
    historical_execution_filter = policy_id.endswith("_executable")
    if historical_execution_filter:
        mask &= _historical_execution_mask(data, cfg)

    if policy_id.startswith("raw_ev_"):
        mask &= data["policy_raw_ev"].fillna(-999).gt(0)
    elif policy_id.startswith("calibrated_ev_"):
        mask &= data["policy_calibrated_ev"].fillna(-999).gt(0)
    elif policy_id.startswith("adaptive_score_"):
        mask &= data["adaptive_exit_score"].fillna(-999).gt(0)

    candidates = data[mask].copy()
    if policy_id.startswith("raw_ev_top1"):
        sort_cols = ["policy_raw_ev", "model_score", "edge_score"]
    elif policy_id.startswith("calibrated_ev_top1"):
        sort_cols = ["policy_calibrated_ev", "model_score", "edge_score"]
    elif policy_id.startswith("adaptive_score_top1"):
        sort_cols = ["adaptive_exit_score", "model_score", "edge_score"]
    else:
        sort_cols = ["model_score", "edge_score", "policy_raw_ev"]

    if candidates.empty:
        selected = candidates.copy()
    else:
        selected = (
            candidates.sort_values(group_cols + sort_cols, ascending=[True] * len(group_cols) + [False] * len(sort_cols))
            .groupby(group_cols, as_index=False)
            .head(1)
        )
    if not selected.empty:
        selected = selected.copy()
        selected["entry_side"] = "BUY"
        selected["exit_side"] = "SELL"
    return candidates, selected, {
        "policy_id": policy_id,
        "selection": "top1_per_snapshot",
        "entry_side": "BUY",
        "exit_side": "SELL",
        "sort_cols": sort_cols,
        "historical_execution_filter": historical_execution_filter,
        "live_execution_guard": True,
        "base_mask_rows": int(mask.sum()),
    }


def _selected_policy_summary(selected: pd.DataFrame, universe_y: pd.Series, prefix: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    out.update(_selection_metrics(selected, universe_y, f"{prefix}_close"))
    out.update(_adaptive_selection_metrics(selected, universe_y, prefix))
    return {
        "close_count": out.get(f"{prefix}_close_count", 0),
        "close_mean_return": out.get(f"{prefix}_close_mean_return", 0.0),
        "close_win_rate": out.get(f"{prefix}_close_win_rate", 0.0),
        "close_sharpe": out.get(f"{prefix}_close_sharpe", 0.0),
        "close_alpha_vs_universe": out.get(f"{prefix}_close_alpha_vs_universe", 0.0),
        "count": out.get(f"{prefix}_count", 0),
        "mean_return": out.get(f"{prefix}_mean_return", 0.0),
        "median_return": out.get(f"{prefix}_median_return", 0.0),
        "win_rate": out.get(f"{prefix}_win_rate", 0.0),
        "sharpe": out.get(f"{prefix}_sharpe", 0.0),
        "alpha_vs_universe": out.get(f"{prefix}_alpha_vs_universe", 0.0),
        "target_hit_rate": out.get(f"{prefix}_target_hit_rate", 0.0),
        "stop_hit_rate": out.get(f"{prefix}_stop_hit_rate", 0.0),
        "avg_tp_pct": out.get(f"{prefix}_avg_tp_pct", 0.0),
        "avg_sl_pct": out.get(f"{prefix}_avg_sl_pct", 0.0),
        "avg_rr": out.get(f"{prefix}_avg_rr", 0.0),
    }


def _research_policy_live_pass(summary: dict[str, Any], cfg: ModelSuiteConfig) -> bool:
    return bool(
        int(summary.get("count") or 0) >= cfg.min_live_trades
        and float(summary.get("win_rate") or 0.0) >= cfg.min_live_win_rate
        and float(summary.get("sharpe") or 0.0) >= cfg.min_live_sharpe
        and float(summary.get("mean_return") or 0.0) > 0
        and float(summary.get("alpha_vs_universe") or 0.0) > 0
        and float(summary.get("target_hit_rate") or 0.0) > float(summary.get("stop_hit_rate") or 0.0)
    )


def _research_policy_score(summary: dict[str, Any]) -> float:
    return float(
        3.0 * float(summary.get("sharpe") or 0.0)
        + 2.0 * float(summary.get("win_rate") or 0.0)
        + 5.0 * float(summary.get("mean_return") or 0.0)
        + 1.5 * float(summary.get("target_hit_rate") or 0.0)
        - 1.5 * float(summary.get("stop_hit_rate") or 0.0)
        + 0.01 * min(int(summary.get("count") or 0), 500)
    )


def _research_strategy_policy_candidates(joined: pd.DataFrame, universe_y: pd.Series, cfg: ModelSuiteConfig) -> tuple[list[dict[str, Any]], dict[str, Any], pd.DataFrame, pd.DataFrame]:
    policy_ids = [
        "legacy_live_gate",
        "model_top1_quote_guard",
        "raw_ev_top1_quote_guard",
        "raw_ev_model_top1_quote_guard",
        "calibrated_ev_top1_quote_guard",
        "adaptive_score_top1_quote_guard",
        "model_top1_executable",
        "raw_ev_top1_executable",
        "raw_ev_model_top1_executable",
    ]
    if cfg.asset_id == "naturalgas" and cfg.allow_short_option_entries:
        policy_ids = [
            "naturalgas_short_decay_executable",
            "naturalgas_short_decay_quote_guard",
            *policy_ids,
        ]
    research: list[dict[str, Any]] = []
    selected_by_id: dict[str, pd.DataFrame] = {}
    candidates_by_id: dict[str, pd.DataFrame] = {}
    for policy_id in policy_ids:
        candidates, selected, meta = _select_research_strategy_policy(joined, cfg, policy_id)
        summary = _selected_policy_summary(selected, universe_y, "candidate_policy")
        passed = _research_policy_live_pass(summary, cfg)
        record = {
            **meta,
            "candidate_rows": int(len(candidates)),
            "candidate_snapshots": int(len(selected)),
            "passed_live_thresholds": passed,
            "research_score": _research_policy_score(summary),
            **summary,
        }
        research.append(record)
        selected_by_id[policy_id] = selected
        candidates_by_id[policy_id] = candidates

    passing = [row for row in research if row["passed_live_thresholds"]]
    best = max(passing or research, key=lambda row: (bool(row["passed_live_thresholds"]), row["research_score"], row["count"]))
    return research, best, candidates_by_id[best["policy_id"]], selected_by_id[best["policy_id"]]


def _policy_conviction_scores(eligible: pd.DataFrame, group_cols: list[str], use_meta: bool = True) -> pd.Series:
    meta_component = 0.0
    if use_meta and "meta_policy_prob" in eligible and eligible["meta_policy_active"].fillna(0).gt(0).any():
        meta_component = (
            0.20 * eligible["meta_policy_prob"].fillna(0.0).clip(0, 1)
            + 0.15 * _pct_rank_by_group(eligible, eligible["meta_policy_ev"].fillna(0.0), group_cols)
        )
    return (
        0.25 * _pct_rank_by_group(eligible, eligible["edge_score"], group_cols)
        + 0.18 * eligible["prob_profit"].clip(0, 1)
        + meta_component
        + 0.10 * eligible["prob_iv_expansion"].clip(0, 1)
        + 0.10 * _pct_rank_by_group(eligible, eligible["return_q80"] - eligible["return_q20"].abs(), group_cols)
        + 0.08 * eligible["adaptive_exit_score"].clip(-1, 1)
        + 0.07 * _pct_rank_by_group(eligible, eligible["book_pressure_score"], group_cols)
        - 0.10 * _pct_rank_by_group(eligible, eligible["spread_pct"], group_cols)
    )


def _select_policy_per_snapshot(eligible: pd.DataFrame, group_cols: list[str], use_meta: bool) -> pd.DataFrame:
    if eligible.empty:
        return eligible.copy()
    ranked = eligible.copy()
    ranked["conviction_score"] = _policy_conviction_scores(ranked, group_cols, use_meta=use_meta)
    return (
        ranked.sort_values(
            group_cols + ["conviction_score", "model_score", "edge_score"],
            ascending=[True] * len(group_cols) + [False, False, False],
        )
        .groupby(group_cols, as_index=False)
        .head(1)
    )


def _policy_metrics_block(
    eligible: pd.DataFrame,
    universe_y: pd.Series,
    group_cols: list[str],
    prefix: str,
    *,
    use_meta: bool,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        f"{prefix}_candidate_rows": int(len(eligible)),
    }
    if eligible.empty:
        out.update(_selection_metrics(eligible, universe_y, f"{prefix}_close"))
        out.update(_adaptive_selection_metrics(eligible, universe_y, prefix))
        out[f"{prefix}_candidate_snapshots"] = 0
        out[f"{prefix}_avg_candidates_per_snapshot"] = 0.0
        out[f"{prefix}_avg_meta_prob"] = None
        out[f"{prefix}_avg_meta_ev"] = None
        return out

    selected = _select_policy_per_snapshot(eligible, group_cols, use_meta=use_meta)
    out[f"{prefix}_candidate_snapshots"] = int(len(selected))
    out[f"{prefix}_avg_candidates_per_snapshot"] = float(len(eligible) / max(len(selected), 1))
    out[f"{prefix}_avg_meta_prob"] = _mean_col(selected, "meta_policy_prob") if use_meta else None
    out[f"{prefix}_avg_meta_ev"] = _mean_col(selected, "meta_policy_ev") if use_meta else None
    out.update(_selection_metrics(selected, universe_y, f"{prefix}_close"))
    out.update(_adaptive_selection_metrics(selected, universe_y, prefix))
    return out


def _strategy_policy_metrics(joined: pd.DataFrame, universe_y: pd.Series, cfg: ModelSuiteConfig) -> dict[str, Any]:
    group_cols = _snapshot_group_cols(joined)
    normal = _normalise_policy_frame(joined, cfg)
    meta_active = bool(normal["meta_policy_active"].fillna(0).gt(0).any())
    live_mode = "live_meta" if meta_active else "live"

    shadow_eligible, shadow_counts = _strategy_policy_candidate_frame(normal, cfg, mode="shadow")
    live_eligible, live_counts = _strategy_policy_candidate_frame(normal, cfg, mode=live_mode)
    research, selected_research, selected_candidates, selected = _research_strategy_policy_candidates(normal, universe_y, cfg)
    out: dict[str, Any] = {
        "shadow_policy_gate_pass_counts": shadow_counts,
        "legacy_strategy_policy_gate_pass_counts": live_counts,
        "legacy_strategy_policy_selection_mode": "candidate_for_live_trade_meta_gated" if meta_active else "candidate_for_live_trade_pre_meta",
        "strategy_policy_meta_active": meta_active,
        "strategy_policy_meta_required_candidate_rows": cfg.min_meta_policy_candidates,
        "strategy_policy_research": research,
        "strategy_policy_selected": selected_research,
        "strategy_policy_selected_live_thresholds_passed": bool(selected_research.get("passed_live_thresholds", False)),
        "strategy_policy_selection_mode": str(selected_research.get("policy_id") or "unknown"),
        "strategy_policy_live_execution_guard": bool(selected_research.get("live_execution_guard", True)),
        "strategy_policy_historical_execution_filter": bool(selected_research.get("historical_execution_filter", False)),
    }
    out.update(_policy_metrics_block(shadow_eligible, universe_y, group_cols, "shadow_policy", use_meta=False))
    out.update(_policy_metrics_block(live_eligible, universe_y, group_cols, "legacy_strategy_policy", use_meta=meta_active))
    out["strategy_policy_candidate_rows"] = int(len(selected_candidates))
    out["strategy_policy_candidate_snapshots"] = int(len(selected))
    out["strategy_policy_avg_candidates_per_snapshot"] = float(len(selected_candidates) / max(len(selected), 1)) if len(selected) else 0.0
    out["strategy_policy_avg_meta_prob"] = _mean_col(selected, "meta_policy_prob") if meta_active else None
    out["strategy_policy_avg_meta_ev"] = _mean_col(selected, "meta_policy_ev") if meta_active else None
    out.update(_selection_metrics(selected, universe_y, "strategy_policy_close"))
    out.update(_adaptive_selection_metrics(selected, universe_y, "strategy_policy"))
    out["strategy_policy_barrier_quality_passed"] = bool(_research_policy_live_pass(selected_research, cfg))
    return out


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
    x = pd.to_numeric(x, errors="coerce")
    x = x.mask(~np.isfinite(x), np.nan).dropna()
    if len(x) < 2:
        return 0.0
    sd = x.std(ddof=0)
    if sd <= 1e-12 or not np.isfinite(sd):
        return 0.0
    return float(x.mean() / sd * np.sqrt(len(x)))


def _spearman(a: pd.Series, b: pd.Series) -> float:
    aa = pd.to_numeric(a, errors="coerce")
    bb = pd.to_numeric(b, errors="coerce")
    aa = aa.mask(~np.isfinite(aa), np.nan)
    bb = bb.mask(~np.isfinite(bb), np.nan)
    mask = aa.notna() & bb.notna()
    if mask.sum() < 3:
        return 0.0
    aa_rank = aa[mask].rank()
    bb_rank = bb[mask].rank()
    if aa_rank.nunique() < 2 or bb_rank.nunique() < 2:
        return 0.0
    corr = aa_rank.corr(bb_rank)
    return float(corr) if pd.notna(corr) and np.isfinite(corr) else 0.0


def _safe_div(a: Any, b: Any) -> pd.Series:
    aa = _num(a, np.nan)
    bb = _num(b, np.nan)
    with np.errstate(divide="ignore", invalid="ignore"):
        out = aa / bb.mask(bb == 0, np.nan)
    return out.mask(~np.isfinite(out), np.nan)


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


def _pct_rank_by_group(df: pd.DataFrame, values: pd.Series, group_cols: list[str] | None = None, ascending: bool = True) -> pd.Series:
    s = pd.to_numeric(values, errors="coerce").mask(lambda x: ~np.isfinite(x), np.nan)
    groups = [col for col in (group_cols or []) if col in df.columns]
    if not groups:
        return s.rank(pct=True, ascending=ascending).fillna(0.5)
    tmp = pd.DataFrame({"_value": s}, index=df.index)
    for col in groups:
        tmp[col] = df[col].to_numpy()
    return tmp.groupby(groups)["_value"].rank(pct=True, ascending=ascending).fillna(0.5)

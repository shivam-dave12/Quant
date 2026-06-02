from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from typing import Any

import pandas as pd

from .collector import OptionDataCollector, _is_rate_limit_error, flatten_quote
from .config import AssetProfile, BotConfig
from .groww_adapter import GrowwAdapter
from .groww_instruments import load_option_contracts_from_groww_instruments
from .nse_contracts import load_nifty_options_contracts
from .option_models import (
    OptionModelSuite,
    build_option_model_frame,
    load_option_model_raw_data,
    train_model_suite_from_store,
    training_readiness_from_store,
)
from .risk import limit_price_from_quote_for_side, normalize_quote, option_buy_quantity, quote_is_executable_for_side
from .session import is_session_open
from .storage import Store
from .strategy import InstitutionalOptionStrategy

log = logging.getLogger(__name__)


def _fmt_metric(value: Any, digits: int = 4) -> str:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return "NA"
    if pd.isna(out):
        return "NA"
    return f"{out:.{digits}f}"


def _model_metrics_summary(metrics: dict[str, Any]) -> str:
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
        f"median_width={_fmt_metric(metrics.get('median_options_per_snapshot'), 1)} "
        f"top1_trades={metrics.get('top1_per_snapshot_count', 'NA')} "
        f"top1_win_rate={_fmt_metric(metrics.get('top1_per_snapshot_win_rate'))} "
        f"top1_sharpe={_fmt_metric(metrics.get('top1_per_snapshot_sharpe'))} "
        f"top1_alpha={_fmt_metric(metrics.get('top1_per_snapshot_alpha_vs_universe'))} "
        f"adaptive_top1_win_rate={_fmt_metric(metrics.get('adaptive_top1_per_snapshot_win_rate'))} "
        f"adaptive_top1_sharpe={_fmt_metric(metrics.get('adaptive_top1_per_snapshot_sharpe'))} "
        f"adaptive_top1_alpha={_fmt_metric(metrics.get('adaptive_top1_per_snapshot_alpha_vs_universe'))} "
        f"shadow_trades={metrics.get('shadow_policy_count', 'NA')} "
        f"shadow_win_rate={_fmt_metric(metrics.get('shadow_policy_win_rate'))} "
        f"shadow_sharpe={_fmt_metric(metrics.get('shadow_policy_sharpe'))} "
        f"strategy_trades={metrics.get('strategy_policy_count', 'NA')} "
        f"strategy_win_rate={_fmt_metric(metrics.get('strategy_policy_win_rate'))} "
        f"strategy_sharpe={_fmt_metric(metrics.get('strategy_policy_sharpe'))} "
        f"strategy_alpha={_fmt_metric(metrics.get('strategy_policy_alpha_vs_universe'))} "
        f"strategy_target_hit={_fmt_metric(metrics.get('strategy_policy_target_hit_rate'))} "
        f"strategy_stop_hit={_fmt_metric(metrics.get('strategy_policy_stop_hit_rate'))} "
        f"strategy_mode={metrics.get('strategy_policy_selection_mode', 'NA')} "
        f"strategy_meta_prob={_fmt_metric(metrics.get('strategy_policy_avg_meta_prob'))} "
        f"strategy_meta_ev={_fmt_metric(metrics.get('strategy_policy_avg_meta_ev'))} "
        f"top_decile_win_rate={_fmt_metric(metrics.get('top_decile_win_rate'))} "
        f"top_decile_sharpe={_fmt_metric(metrics.get('top_decile_sharpe'))} "
        f"return_ic={_fmt_metric(metrics.get('return_spearman_ic'))} "
        f"rank_ic={_fmt_metric(metrics.get('rank_spearman_ic'))}"
    )


def _strategy_log_summary(decision: dict[str, Any]) -> str:
    diag = decision.get("strategy") or {}
    top = decision.get("top") or {}
    selected = diag.get("selected") or {}
    rejected = diag.get("top_rejected") or {}
    focus = dict(top)
    focus.update(rejected)
    focus.update(selected)
    gate_counts = diag.get("gate_pass_counts") or {}
    gate_text = ",".join(f"{k}:{v}" for k, v in sorted(gate_counts.items())) if gate_counts else "NA"
    return (
        f"strategy={diag.get('strategy', 'NA')} "
        f"mode={diag.get('gate_mode', 'NA')} "
        f"selected_policy={diag.get('selected_policy_id', 'NA')} "
        f"meta_active={diag.get('meta_policy_active', 'NA')} "
        f"input_rows={diag.get('input_rows', 'NA')} "
        f"shadow_passed_gate_rows={diag.get('shadow_passed_gate_rows', 'NA')} "
        f"passed_gate_rows={diag.get('passed_gate_rows', 'NA')} "
        f"gate_pass_counts={gate_text} "
        f"symbol={focus.get('trading_symbol', 'NA')} "
        f"entry_side={focus.get('entry_side', top.get('entry_side', 'BUY'))} "
        f"edge={_fmt_metric(focus.get('edge_score'))} "
        f"pred_return={_fmt_metric(focus.get('predicted_return'))} "
        f"prob_profit={_fmt_metric(focus.get('prob_profit'))} "
        f"raw_prob={_fmt_metric(focus.get('raw_prob_profit'))} "
        f"policy_ev={_fmt_metric(focus.get('policy_calibrated_ev'))} "
        f"raw_policy_ev={_fmt_metric(focus.get('policy_raw_ev'))} "
        f"spread_pct={_fmt_metric(focus.get('spread_pct'))} "
        f"q20={_fmt_metric(focus.get('return_q20'))} "
        f"q50={_fmt_metric(focus.get('return_q50'))} "
        f"q80={_fmt_metric(focus.get('return_q80'))} "
        f"tp={_fmt_metric(focus.get('adaptive_tp_pct'))} "
        f"sl={_fmt_metric(focus.get('adaptive_sl_pct'))} "
        f"rr={_fmt_metric(focus.get('adaptive_rr'))} "
        f"exit_score={_fmt_metric(focus.get('adaptive_exit_score'))} "
        f"meta_prob={_fmt_metric(focus.get('meta_policy_prob'))} "
        f"meta_ev={_fmt_metric(focus.get('meta_policy_ev'))} "
        f"meta_edge={_fmt_metric(focus.get('meta_policy_edge'))}"
    )


def _model_gate_failures(metrics: dict[str, Any], cfg: BotConfig) -> list[str]:
    failures: list[str] = []
    def finite_float(value: Any, default: float = 0.0) -> float:
        try:
            out = float(value)
        except (TypeError, ValueError):
            return default
        return out if pd.notna(out) else default

    def first_present(*keys: str) -> Any:
        for key in keys:
            if key in metrics and metrics.get(key) is not None:
                return metrics.get(key)
        return None

    live_gate = metrics.get("live_gate") if isinstance(metrics.get("live_gate"), dict) else {}
    policy = str(live_gate.get("selection_policy") or "adaptive_top1_per_snapshot")
    min_live_rows = int(live_gate.get("min_live_rows") or cfg.live_train_min_rows)
    min_trades = int(live_gate.get("min_trades") or cfg.min_backtest_trades)
    min_win_rate = finite_float(live_gate.get("min_win_rate"), cfg.min_model_win_rate)
    min_sharpe = finite_float(live_gate.get("min_sharpe"), cfg.min_model_sharpe)
    labelled_rows = int(metrics.get("labelled_rows") or 0)
    trades = int(first_present("strategy_policy_count", "adaptive_top1_per_snapshot_count", "top1_per_snapshot_count") or 0)
    win_rate = finite_float(first_present("strategy_policy_win_rate", "adaptive_top1_per_snapshot_win_rate", "top1_per_snapshot_win_rate"))
    sharpe = finite_float(first_present("strategy_policy_sharpe", "adaptive_top1_per_snapshot_sharpe", "top1_per_snapshot_sharpe"))
    mean_return = finite_float(first_present("strategy_policy_mean_return", "adaptive_top1_per_snapshot_mean_return", "top1_per_snapshot_mean_return"))
    alpha = finite_float(first_present("strategy_policy_alpha_vs_universe", "adaptive_top1_per_snapshot_alpha_vs_universe", "top1_per_snapshot_alpha_vs_universe"))
    target_hit = finite_float(first_present("strategy_policy_target_hit_rate", "adaptive_top1_per_snapshot_target_hit_rate"))
    stop_hit = finite_float(first_present("strategy_policy_stop_hit_rate", "adaptive_top1_per_snapshot_stop_hit_rate"))
    median_width = metrics.get("median_options_per_snapshot")
    min_width = float((metrics.get("config") or {}).get("min_snapshot_width") or 4)
    median_width_num = finite_float(median_width, -1.0)

    if not metrics.get("model_quality_gate_passed", False):
        failures.append("quality_gate_false")
    if median_width is None or median_width_num < min_width:
        failures.append(f"cross_section_width:{median_width}<{min_width}")
    if labelled_rows < min_live_rows:
        failures.append(f"labelled_rows:{labelled_rows}<{min_live_rows}")
    if trades < min_trades:
        failures.append(f"{policy}_trades:{trades}<{min_trades}")
    if win_rate < min_win_rate:
        failures.append(f"{policy}_win_rate:{win_rate:.4f}<{min_win_rate:.4f}")
    if sharpe < min_sharpe:
        failures.append(f"{policy}_sharpe:{sharpe:.4f}<{min_sharpe:.4f}")
    if mean_return <= 0:
        failures.append(f"{policy}_mean_return:{mean_return:.4f}<=0")
    if alpha <= 0:
        failures.append(f"{policy}_alpha:{alpha:.4f}<=0")
    if target_hit <= 0 or target_hit <= stop_hit:
        failures.append(f"{policy}_barrier_quality:target_hit={target_hit:.4f}<=stop_hit={stop_hit:.4f}")
    return failures


class LiveOptionBot:
    """Live option-only loop.

    The live loop is strict by design:
    - it records Groww option-chain/quote data first;
    - it trains only from rows tagged by the live Groww collector;
    - it refuses to trade if no real trained model suite exists;
    - it refuses live orders unless the model passes live gates and the two-key live switch is enabled.
    """

    def __init__(self, cfg: BotConfig, store: Store, adapter: GrowwAdapter, asset: AssetProfile | None = None):
        self.cfg = cfg
        self.store = store
        self.adapter = adapter
        self.asset = asset or cfg.get_asset_profile("nifty")
        self.suite: OptionModelSuite | None = self._load_suite_if_exists()
        self.last_train_status: dict[str, Any] = {}
        self.last_collect_status: dict[str, Any] = {}
        self._last_train_status_log_ts = 0.0
        self.strategy = InstitutionalOptionStrategy(cfg, self.asset, self.suite.metrics if self.suite is not None else None)
        self.contract_meta = None
        if self.suite is not None:
            log.info(
                "model loaded | asset=%s path=%s %s",
                self.asset.asset_id,
                self.cfg.model_suite_path_for(self.asset.asset_id),
                _model_metrics_summary(self.suite.metrics),
            )
        if self.asset.asset_id == "nifty" and cfg.nse_contract_file.exists():
            try:
                self.contract_meta = load_nifty_options_contracts(cfg.nse_contract_file, cfg.underlying).set_index("trading_symbol")
            except Exception:
                self.contract_meta = None
        if self.contract_meta is None and cfg.groww_instruments_csv.exists():
            try:
                self.contract_meta = load_option_contracts_from_groww_instruments(cfg.groww_instruments_csv, self.asset).set_index("trading_symbol")
            except Exception:
                log.exception("contract metadata load failed | asset=%s path=%s", self.asset.asset_id, cfg.groww_instruments_csv)
                self.contract_meta = None

    def _load_suite_if_exists(self) -> OptionModelSuite | None:
        model_path = self.cfg.model_suite_path_for(self.asset.asset_id)
        if not model_path.exists():
            return None
        try:
            suite = OptionModelSuite.load(model_path)
            cfg_payload = suite.metrics.get("config") if isinstance(suite.metrics, dict) else {}
            min_width = float((cfg_payload or {}).get("min_snapshot_width") or 4)
            median_width = suite.metrics.get("median_options_per_snapshot") if isinstance(suite.metrics, dict) else None
            if median_width is None:
                log.warning(
                    "model ignored: missing cross-section width metrics | asset=%s path=%s min_snapshot_width=%.1f",
                    self.asset.asset_id,
                    model_path,
                    min_width,
                )
                return None
            if float(median_width) < min_width:
                log.warning(
                    "model ignored: insufficient cross-section width | asset=%s path=%s median_width=%.1f min_snapshot_width=%.1f",
                    self.asset.asset_id,
                    model_path,
                    float(median_width),
                    min_width,
                )
                return None
            return suite
        except Exception:
            log.exception("model suite load failed | asset=%s path=%s", self.asset.asset_id, model_path)
            return None

    def model_gate_passed(self) -> bool:
        return bool(self.suite and self.suite.metrics.get("passed_live_gate", False))

    def _maybe_train_live_model(self, force: bool = False) -> dict[str, Any]:
        if not self.cfg.auto_train_enabled and not force:
            return {"trained": False, "reason": "auto_train_disabled"}
        if self.suite is not None and self.model_gate_passed() and not force:
            return {"trained": False, "reason": "existing_model_gate_passed"}
        readiness = training_readiness_from_store(self.cfg, self.store, self.asset)
        if not readiness.get("ready") and not force:
            status = {"trained": False, "reason": readiness.get("reason", "training_not_ready"), "readiness": readiness}
            self.last_train_status = status
            now = time.time()
            if now - self._last_train_status_log_ts >= self.cfg.train_status_log_interval_seconds:
                self._last_train_status_log_ts = now
                log.info(
                    "model warm-up | asset=%s labelled_rows=%s/%s live_rows=%s/%s mode=%s min_snapshot_width=%s raw_rows=%s symbols=%s max_symbol_snapshots=%s horizon_rows=%s reason=%s",
                    self.asset.asset_id,
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
            return status
        if self.suite is not None and not force:
            current_labelled = int(readiness.get("estimated_labelled_rows") or 0)
            trained_labelled = int(
                self.suite.metrics.get("labelled_rows")
                or (int(self.suite.metrics.get("train_rows") or 0) + int(self.suite.metrics.get("test_rows") or 0))
                or 0
            )
            min_new_labels = max(50, self.cfg.label_horizon_rows * 4)
            if current_labelled <= trained_labelled + min_new_labels:
                status = {
                    "trained": False,
                    "reason": "existing_model_waiting_for_new_labels",
                    "readiness": readiness,
                    "trained_labelled_rows": trained_labelled,
                    "current_labelled_rows": current_labelled,
                    "min_new_labels": min_new_labels,
                    "passed_live_gate": self.model_gate_passed(),
                }
                self.last_train_status = status
                now = time.time()
                if now - self._last_train_status_log_ts >= self.cfg.train_status_log_interval_seconds:
                    self._last_train_status_log_ts = now
                    log.info(
                        "model update skipped | asset=%s reason=%s trained_labelled_rows=%s current_labelled_rows=%s min_new_labels=%s passed_live_gate=%s",
                        self.asset.asset_id,
                        status["reason"],
                        trained_labelled,
                        current_labelled,
                        min_new_labels,
                        self.model_gate_passed(),
                    )
                return status
        try:
            log.info(
                "model training start | asset=%s labelled_rows=%s min_rows=%s live_min_rows=%s mode=%s min_snapshot_width=%s raw_rows=%s symbols=%s horizon_rows=%s strategy=%s",
                self.asset.asset_id,
                readiness.get("estimated_labelled_rows"),
                readiness.get("min_rows_required"),
                readiness.get("min_live_rows_required"),
                readiness.get("training_mode"),
                readiness.get("min_snapshot_width"),
                readiness.get("raw_rows"),
                readiness.get("symbols"),
                readiness.get("horizon_rows"),
                self.asset.model_strategy,
            )
            result = train_model_suite_from_store(self.cfg, self.store, self.asset)
            self.suite = OptionModelSuite.load(result.model_path)
            self.strategy.set_model_metrics(self.suite.metrics)
            status = {
                "trained": True,
                "asset_id": self.asset.asset_id,
                "model_path": str(result.model_path),
                "passed_live_gate": bool(result.metrics.get("passed_live_gate", False)),
                "top1_win_rate": result.metrics.get("top1_per_snapshot_win_rate"),
                "top1_sharpe": result.metrics.get("top1_per_snapshot_sharpe"),
                "top1_alpha": result.metrics.get("top1_per_snapshot_alpha_vs_universe"),
                "shadow_policy_count": result.metrics.get("shadow_policy_count"),
                "shadow_policy_win_rate": result.metrics.get("shadow_policy_win_rate"),
                "shadow_policy_sharpe": result.metrics.get("shadow_policy_sharpe"),
                "strategy_policy_count": result.metrics.get("strategy_policy_count"),
                "strategy_policy_win_rate": result.metrics.get("strategy_policy_win_rate"),
                "strategy_policy_sharpe": result.metrics.get("strategy_policy_sharpe"),
                "strategy_policy_alpha": result.metrics.get("strategy_policy_alpha_vs_universe"),
                "strategy_policy_target_hit_rate": result.metrics.get("strategy_policy_target_hit_rate"),
                "strategy_policy_stop_hit_rate": result.metrics.get("strategy_policy_stop_hit_rate"),
                "strategy_policy_selection_mode": result.metrics.get("strategy_policy_selection_mode"),
                "strategy_policy_meta_active": result.metrics.get("strategy_policy_meta_active"),
                "strategy_policy_avg_meta_prob": result.metrics.get("strategy_policy_avg_meta_prob"),
                "strategy_policy_avg_meta_ev": result.metrics.get("strategy_policy_avg_meta_ev"),
            }
            log.info(
                "model training complete | asset=%s model_path=%s %s",
                self.asset.asset_id,
                result.model_path,
                _model_metrics_summary(result.metrics),
            )
            self.last_train_status = status
            return status
        except ValueError as exc:
            status = {"trained": False, "reason": str(exc)}
            self.last_train_status = status
            log.info("model training skipped | asset=%s reason=%s", self.asset.asset_id, exc)
            return status
        except Exception as exc:
            status = {"trained": False, "reason": f"training_error: {exc}"}
            self.last_train_status = status
            log.exception("live model training failed")
            return status

    def _collect_quotes_for_snapshot(self, snap: pd.DataFrame) -> int:
        if snap.empty:
            return 0
        candidates = snap[
            snap["ltp"].between(self.asset.min_ltp, self.asset.max_ltp)
            & (snap["volume"].fillna(0) >= self.asset.min_volume)
            & (snap["open_interest"].fillna(0) >= self.asset.min_oi)
        ].copy()
        if candidates.empty:
            return 0
        candidates["liq_score"] = candidates["volume"].fillna(0) + candidates["open_interest"].fillna(0) * 0.05
        symbols = (
            candidates.sort_values("liq_score", ascending=False)["trading_symbol"]
            .dropna()
            .astype(str)
            .drop_duplicates()
            .head(self.asset.max_quote_symbols)
            .tolist()
        )
        total = 0
        for sym in symbols:
            try:
                q = self._get_quote_with_backoff(sym)
                total += self.store.append_df("quote_snapshots", flatten_quote(sym, q, self.asset))
            except Exception as exc:
                if _is_rate_limit_error(exc):
                    log.warning("quote snapshot rate-limited | asset=%s symbol=%s error=%s", self.asset.asset_id, sym, exc)
                else:
                    log.exception("quote snapshot failed | symbol=%s", sym)
        return total

    def _get_quote_with_backoff(self, symbol: str) -> dict[str, Any]:
        attempts = max(1, int(self.cfg.groww_quote_retry_attempts))
        last_exc: Exception | None = None
        for attempt in range(attempts):
            if attempt > 0:
                time.sleep(max(0.0, float(self.cfg.groww_quote_rate_limit_backoff_seconds)) * attempt)
            try:
                quote = self.adapter.get_quote(symbol, segment=self.asset.segment, exchange=self.asset.exchange)
                delay = max(0.0, float(self.cfg.groww_quote_delay_seconds))
                if delay:
                    time.sleep(delay)
                return quote
            except Exception as exc:
                last_exc = exc
                if not _is_rate_limit_error(exc) or attempt == attempts - 1:
                    raise
                log.warning(
                    "quote retry after rate limit | asset=%s symbol=%s attempt=%s/%s backoff=%.2fs",
                    self.asset.asset_id,
                    symbol,
                    attempt + 1,
                    attempts,
                    float(self.cfg.groww_quote_rate_limit_backoff_seconds) * (attempt + 1),
                )
        raise last_exc or RuntimeError(f"quote failed: {symbol}")

    @staticmethod
    def _normalize_expiries(expiries: str | list[str] | tuple[str, ...]) -> list[str]:
        if isinstance(expiries, str):
            raw = [expiries]
        else:
            raw = list(expiries)
        return list(dict.fromkeys(str(pd.to_datetime(expiry).date()) for expiry in raw if str(expiry).strip()))

    @staticmethod
    def _latest_rows_per_expiry(frame: pd.DataFrame, expiries: list[str], window_seconds: int = 90) -> pd.DataFrame:
        data = frame.copy()
        data["ts"] = pd.to_datetime(data["ts"])
        data["expiry"] = pd.to_datetime(data["expiry"])
        expiry_dates = {pd.to_datetime(expiry).date() for expiry in expiries}
        data = data[data["expiry"].dt.date.isin(expiry_dates)].copy()
        frames: list[pd.DataFrame] = []
        for _, sub in data.groupby(data["expiry"].dt.date, sort=True):
            latest_ts = sub["ts"].max()
            scan = sub[sub["ts"].ge(latest_ts - pd.Timedelta(seconds=window_seconds))].copy()
            if scan.empty:
                scan = sub[sub["ts"].eq(latest_ts)].copy()
            scan = scan.sort_values(["trading_symbol", "ts"]).groupby("trading_symbol", as_index=False).tail(1)
            scan["ts"] = latest_ts
            frames.append(scan.copy())
        return pd.concat(frames, ignore_index=True) if frames else data.iloc[0:0].copy()

    def collect_and_score(self, expiries: str | list[str] | tuple[str, ...]) -> pd.DataFrame:
        session = is_session_open(self.asset)
        if not session.is_open:
            log.info("live engine skipped | asset=%s reason=%s local_time=%s", self.asset.asset_id, session.reason, session.local_time)
            return pd.DataFrame()
        expiry_list = self._normalize_expiries(expiries)
        if not expiry_list:
            return pd.DataFrame()
        collector = OptionDataCollector(self.cfg, self.store, self.adapter, self.asset)
        rows_by_expiry = collector.collect_expiries_once(expiry_list, quote_top_symbols=True)
        self.last_collect_status = {**collector.last_status, "rows_by_expiry": rows_by_expiry}
        if self.last_collect_status.get("blocking"):
            log.warning("live collection blocked | asset=%s status=%s", self.asset.asset_id, json.dumps(self.last_collect_status, default=str))
            return pd.DataFrame()
        if self.suite is None or not self.model_gate_passed():
            self._maybe_train_live_model()
        if self.suite is None:
            return pd.DataFrame()

        raw = load_option_model_raw_data(self.cfg, self.store, self.asset)
        if raw.empty:
            return pd.DataFrame()
        frame = build_option_model_frame(raw, horizon_rows=self.cfg.label_horizon_rows, cost_bps=self.cfg.estimated_round_trip_cost_bps)
        if frame.empty:
            return pd.DataFrame()
        latest = self._latest_rows_per_expiry(frame, expiry_list)
        if latest.empty:
            return pd.DataFrame()
        eligible = latest[
            latest["ltp"].between(self.asset.min_ltp, self.asset.max_ltp)
            & (latest["volume"].fillna(0) >= self.asset.min_volume)
            & (latest["open_interest"].fillna(0) >= self.asset.min_oi)
        ].copy()
        if eligible.empty:
            return eligible

        scored = self.suite.predict(eligible)
        for col in [
            "open_interest", "volume", "mid_price", "quoted_entry_price", "quoted_exit_price",
            "spread", "spread_pct", "bid_price", "offer_price", "bid_quantity", "ask_quantity",
            "book_pressure_score", "depth_imbalance_5", "top_book_imbalance", "minutes_to_session_close",
            "session_elapsed_pct", "is_late_session", "commodity_late_session",
        ]:
            if col in eligible.columns:
                scored[col] = eligible.loc[scored.index, col]
        scored["edge_score"] = scored["predicted_return"] - self.cfg.uncertainty_buffer
        return scored.sort_values(["model_score", "edge_score"], ascending=False).reset_index(drop=True)

    def decide_once(self, expiries: str | list[str] | tuple[str, ...]) -> dict[str, Any]:
        scored = self.collect_and_score(expiries)
        if self.last_collect_status.get("blocking"):
            return {
                "decision": "NO_TRADE",
                "reason": self.last_collect_status.get("reason", "collection_blocked"),
                "collect_status": self.last_collect_status,
                "train_status": self.last_train_status,
            }
        if self.suite is None:
            return {
                "decision": "NO_TRADE",
                "reason": "no_real_groww_trained_model",
                "collect_status": self.last_collect_status,
                "train_status": self.last_train_status,
            }
        if scored.empty:
            return {
                "decision": "NO_TRADE",
                "reason": "no_eligible_options",
                "collect_status": self.last_collect_status,
                "train_status": self.last_train_status,
            }
        strategy_decision = self.strategy.choose(scored)
        top = strategy_decision.row or scored.iloc[0].to_dict()
        strategy_log_payload = {"strategy": strategy_decision.diagnostics, "top": top}
        log.info(
            "strategy decision | asset=%s action=%s reason=%s %s",
            self.asset.asset_id,
            strategy_decision.action,
            strategy_decision.reason,
            _strategy_log_summary(strategy_log_payload),
        )
        if strategy_decision.action != "TRADE_CANDIDATE":
            self._save_signal(top, "NO_TRADE", strategy_decision.reason)
            return {
                "decision": "NO_TRADE",
                "reason": strategy_decision.reason,
                "top": top,
                "strategy": strategy_decision.diagnostics,
                "collect_status": self.last_collect_status,
                "train_status": self.last_train_status,
            }
        self._save_signal(top, "TRADE_CANDIDATE", strategy_decision.reason)
        return {
            "decision": "TRADE_CANDIDATE",
            "top": top,
            "strategy": strategy_decision.diagnostics,
            "collect_status": self.last_collect_status,
            "train_status": self.last_train_status,
        }

    def _save_signal(self, row: dict[str, Any], decision: str, reason: str) -> None:
        feature_cols = self.suite.feature_cols if self.suite is not None else []
        decision_cols = [
            "predicted_return",
            "edge_score",
            "raw_prob_profit",
            "prob_profit",
            "prob_iv_expansion",
            "rank_score",
            "return_q20",
            "return_q50",
            "return_q80",
            "estimated_cost",
            "adaptive_tp_pct",
            "adaptive_sl_pct",
            "adaptive_rr",
            "adaptive_breakeven_prob",
            "adaptive_exit_score",
            "policy_calibrated_ev",
            "policy_raw_ev",
            "policy_score_rank_pct",
            "meta_policy_active",
            "meta_policy_prob",
            "meta_policy_ev",
            "meta_policy_edge",
            "model_score",
            "model_rank_at_ts",
            "spread_pct",
            "bid_quantity",
            "ask_quantity",
            "volume",
            "open_interest",
            "minutes_to_session_close",
        ]
        feature_payload = {k: row.get(k) for k in [*feature_cols, *decision_cols] if k in row}
        df = pd.DataFrame([{
            "ts": datetime.utcnow(),
            "asset_id": self.asset.asset_id,
            "underlying": self.asset.underlying,
            "exchange": self.asset.exchange,
            "segment": self.asset.segment,
            "trading_symbol": row.get("trading_symbol"),
            "expiry": pd.to_datetime(row.get("expiry")).date() if row.get("expiry") is not None else None,
            "strike": row.get("strike"),
            "option_type": row.get("option_type"),
            "ltp": row.get("ltp"),
            "predicted_return": row.get("predicted_return"),
            "edge_score": row.get("edge_score"),
            "decision": decision,
            "reason": reason,
            "features_json": json.dumps(feature_payload, default=str),
        }])
        self.store.append_df("model_signals", df)

    @staticmethod
    def _items(payload: Any, key: str) -> list[dict[str, Any]]:
        value = payload.get(key) if isinstance(payload, dict) else payload
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        return []

    @staticmethod
    def _is_terminal_order(status: Any) -> bool:
        return str(status or "").upper() in {
            "CANCELLED",
            "CANCELED",
            "COMPLETED",
            "COMPLETE",
            "EXECUTED",
            "FAILED",
            "REJECTED",
            "EXPIRED",
        }

    def _live_account_guard(self, trading_symbol: str) -> tuple[bool, str]:
        try:
            positions = self._items(self.adapter.get_positions_for_user(self.asset.segment), "positions")
        except Exception as exc:
            return False, f"position_check_failed:{exc}"

        open_positions = []
        for pos in positions:
            qty = pd.to_numeric(pos.get("quantity"), errors="coerce")
            if pd.notna(qty) and abs(float(qty)) > 0:
                open_positions.append(pos)
        if any(str(pos.get("trading_symbol")) == trading_symbol for pos in open_positions):
            return False, "duplicate_open_position"
        if len(open_positions) >= self.cfg.max_open_positions:
            return False, f"max_open_positions_reached:{len(open_positions)}"

        orders: list[dict[str, Any]] = []
        try:
            for page in range(4):
                page_orders = self._items(
                    self.adapter.get_order_list(segment=self.asset.segment, page=page, page_size=25),
                    "order_list",
                )
                orders.extend(page_orders)
                if len(page_orders) < 25:
                    break
        except Exception as exc:
            return False, f"open_order_check_failed:{exc}"
        active_orders = [order for order in orders if not self._is_terminal_order(order.get("order_status"))]
        if any(str(order.get("trading_symbol")) == trading_symbol for order in active_orders):
            return False, "duplicate_open_order"
        return True, "ok"

    @staticmethod
    def _nested_float(payload: dict[str, Any], *path: str) -> float | None:
        current: Any = payload
        for key in path:
            if not isinstance(current, dict):
                return None
            current = current.get(key)
        try:
            out = float(current)
        except (TypeError, ValueError):
            return None
        return out if pd.notna(out) else None

    def _live_margin_guard(self, trading_symbol: str, quantity: int, price: float, entry_side: str = "BUY") -> tuple[bool, str]:
        if not self.cfg.require_live_margin_check:
            return True, "ok"
        entry_side = str(entry_side or "BUY").upper()
        try:
            order_req = {
                "trading_symbol": trading_symbol,
                "transaction_type": self.adapter.TRANSACTION_TYPE_SELL if entry_side == "SELL" else self.adapter.TRANSACTION_TYPE_BUY,
                "quantity": int(quantity),
                "price": float(price),
                "order_type": self.adapter.ORDER_TYPE_LIMIT,
                "product": self.adapter.product_const(self.asset.product),
                "exchange": self.adapter.exchange_const(self.asset.exchange),
            }
            margin = self.adapter.get_order_margin_details(segment=self.asset.segment, orders=[order_req])
            available = self.adapter.get_available_margin_details()
        except Exception as exc:
            return False, f"margin_check_failed:{exc}"

        required = self._nested_float(margin, "total_requirement")
        if required is None:
            required = self._nested_float(margin, "option_buy_premium")
        available_cash = self._nested_float(available, "fno_margin_details", "option_buy_balance_available")
        if available_cash is None:
            available_cash = self._nested_float(available, "clear_cash")
        if required is None or available_cash is None:
            return False, "margin_check_incomplete"
        required_with_buffer = required * (1.0 + self.cfg.live_margin_buffer_pct)
        if available_cash < required_with_buffer:
            return False, f"insufficient_margin:{available_cash:.2f}<{required_with_buffer:.2f}"
        return True, "ok"

    def _adaptive_exit_pcts(self, row: dict[str, Any]) -> tuple[float, float]:
        entry_side = str(row.get("entry_side") or "BUY").upper()

        def pct(name: str, fallback: float, lo: float, hi: float) -> float:
            try:
                value = float(row.get(name))
            except (TypeError, ValueError):
                value = fallback
            if pd.isna(value) or value <= 0:
                value = fallback
            return float(min(max(value, lo), hi))

        if entry_side == "SELL":
            return (
                pct("adaptive_tp_pct", self.asset.short_tp_pct, 0.001, 0.20),
                pct("adaptive_sl_pct", self.asset.short_sl_pct, 0.002, 0.30),
            )
        return (
            pct("adaptive_tp_pct", self.asset.tp_pct, 0.03, 0.65),
            pct("adaptive_sl_pct", self.asset.sl_pct, 0.02, 0.40),
        )

    def trade_once(self, expiries: str | list[str] | tuple[str, ...]) -> dict[str, Any]:
        session = is_session_open(self.asset)
        if not session.is_open:
            return {
                "decision": "NO_TRADE",
                "asset_id": self.asset.asset_id,
                "reason": session.reason,
                "local_time": session.local_time,
            }
        decision = self.decide_once(expiries)
        if decision.get("decision") != "TRADE_CANDIDATE":
            log.info("no trade | asset=%s reason=%s", self.asset.asset_id, decision.get("reason"))
            return decision

        if not self.model_gate_passed():
            metrics = self.suite.metrics if self.suite is not None else {}
            failures = _model_gate_failures(metrics, self.cfg)
            log.warning(
                "live blocked: model has not passed real-data live gates | asset=%s failures=%s %s",
                self.asset.asset_id,
                ",".join(failures) if failures else "unknown",
                _model_metrics_summary(metrics),
            )
            decision["decision"] = "PAPER_ONLY_MODEL_GATE_FAILED"
            decision["reason"] = "model_live_gate_failed"
            decision["model_gate_failures"] = failures
            decision["model_metrics_summary"] = _model_metrics_summary(metrics)
            return decision

        top = decision["top"]
        sym = str(top["trading_symbol"])
        entry_side = str(top.get("entry_side") or "BUY").upper()
        if entry_side not in {"BUY", "SELL"}:
            entry_side = "BUY"
        decision["entry_side"] = entry_side
        lot_size = 50
        tick_size = 0.05
        if self.contract_meta is not None and sym in self.contract_meta.index:
            meta = self.contract_meta.loc[sym]
            if isinstance(meta, pd.DataFrame):
                meta = meta.iloc[0]
            if entry_side == "BUY" and pd.to_numeric(meta.get("buy_allowed"), errors="coerce") == 0:
                decision["decision"] = "NO_TRADE"
                decision["reason"] = "contract_buy_not_allowed"
                return decision
            sell_allowed = pd.to_numeric(meta.get("sell_allowed"), errors="coerce")
            if entry_side == "SELL" and pd.notna(sell_allowed) and float(sell_allowed) == 0:
                decision["decision"] = "NO_TRADE"
                decision["reason"] = "contract_sell_not_allowed"
                return decision
            lot_size = int(float(meta.get("lot_size") or lot_size))
            tick_size = float(meta.get("tick_size") or tick_size)

        quote = normalize_quote(self._get_quote_with_backoff(sym))
        ok, reason = quote_is_executable_for_side(quote, side=entry_side, max_spread_pct=self.asset.max_spread_pct)
        if not ok:
            log.warning("execution blocked | asset=%s symbol=%s reason=%s", self.asset.asset_id, sym, reason)
            decision["decision"] = "NO_TRADE"
            decision["reason"] = reason
            return decision

        price = limit_price_from_quote_for_side(quote, side=entry_side, tick_size=tick_size, max_cross_ticks=self.cfg.entry_tick_buffer)
        if price is None:
            decision["decision"] = "NO_TRADE"
            decision["reason"] = "no_limit_price"
            return decision

        tp_pct, sl_pct = self._adaptive_exit_pcts(top)
        decision["adaptive_tp_pct"] = tp_pct
        decision["adaptive_sl_pct"] = sl_pct
        qty = option_buy_quantity(
            price,
            lot_size,
            self.cfg.account_capital,
            self.asset.risk_per_trade_pct,
            self.asset.max_premium_value_per_trade,
            sl_pct=sl_pct,
        )
        if qty <= 0:
            decision["decision"] = "NO_TRADE"
            decision["reason"] = "qty_zero_risk_budget"
            return decision

        if self.cfg.paper_trading or not self.cfg.live_trading_enabled:
            log.info(
                "paper %s | asset=%s symbol=%s qty=%s limit=%.2f edge=%.4f adaptive_tp=%.4f adaptive_sl=%.4f",
                entry_side.lower(),
                self.asset.asset_id,
                sym,
                qty,
                price,
                float(top["edge_score"]),
                tp_pct,
                sl_pct,
            )
            self._save_order("PAPER", sym, entry_side, qty, price, None, None, "PAPER", {"signal": top, "quote": quote, "adaptive_tp_pct": tp_pct, "adaptive_sl_pct": sl_pct})
            decision["decision"] = f"PAPER_{entry_side}"
            decision["qty"] = qty
            decision["limit_price"] = price
            return decision

        ok, reason = self._live_account_guard(sym)
        if not ok:
            log.warning("live account guard blocked | symbol=%s reason=%s", sym, reason)
            decision["decision"] = "NO_TRADE"
            decision["reason"] = reason
            return decision

        ok, reason = self._live_margin_guard(sym, qty, price, entry_side=entry_side)
        if not ok:
            log.warning("live margin guard blocked | symbol=%s reason=%s", sym, reason)
            decision["decision"] = "NO_TRADE"
            decision["reason"] = reason
            return decision

        log.warning("live %s | asset=%s symbol=%s qty=%s limit=%.2f", entry_side.lower(), self.asset.asset_id, sym, qty, price)
        try:
            if entry_side == "SELL":
                order = self.adapter.place_sell_option_limit(sym, qty, price, product=self.asset.product, exchange=self.asset.exchange, segment=self.asset.segment)
            else:
                order = self.adapter.place_buy_option_limit(sym, qty, price, product=self.asset.product, exchange=self.asset.exchange, segment=self.asset.segment)
        except Exception as exc:
            log.exception("live order submit failed | asset=%s symbol=%s side=%s", self.asset.asset_id, sym, entry_side)
            decision["decision"] = "ORDER_SUBMIT_FAILED"
            decision["reason"] = str(exc)
            return decision
        oid = order.get("groww_order_id")
        self._save_order("LIVE", sym, entry_side, qty, price, oid, order.get("order_reference_id"), order.get("order_status"), order)
        if not oid:
            decision["decision"] = "ORDER_SUBMIT_FAILED"
            decision["order"] = order
            return decision

        fill = self.adapter.wait_for_fill(oid, timeout_seconds=10, segment=self.asset.segment)
        avg_fill = fill.get("average_fill_price") or fill.get("avgFillPrice") or price
        filled_qty = int(float(fill.get("filled_quantity") or fill.get("filledQty") or 0))
        if filled_qty <= 0:
            decision["decision"] = "ENTRY_NOT_FILLED"
            decision["order"] = fill
            return decision

        if entry_side == "SELL":
            oco = self.adapter.create_short_exit_oco(
                sym,
                filled_qty,
                float(avg_fill),
                tp_pct,
                sl_pct,
                self.asset.product,
                exchange=self.asset.exchange,
                segment=self.asset.segment,
                tick_size=tick_size,
            )
            decision["decision"] = "LIVE_SELL_WITH_OCO"
        else:
            oco = self.adapter.create_exit_oco(
                sym,
                filled_qty,
                float(avg_fill),
                tp_pct,
                sl_pct,
                self.asset.product,
                exchange=self.asset.exchange,
                segment=self.asset.segment,
                tick_size=tick_size,
            )
            decision["decision"] = "LIVE_BUY_WITH_OCO"
        decision["entry_order"] = fill
        decision["oco"] = oco
        return decision

    def _save_order(self, mode, sym, side, qty, price, oid, ref, status, raw) -> None:
        df = pd.DataFrame([{
            "ts": datetime.utcnow(),
            "mode": mode,
            "asset_id": self.asset.asset_id,
            "underlying": self.asset.underlying,
            "exchange": self.asset.exchange,
            "segment": self.asset.segment,
            "trading_symbol": sym,
            "transaction_type": side,
            "quantity": int(qty),
            "price": float(price),
            "groww_order_id": oid,
            "order_reference_id": ref,
            "order_status": status,
            "raw_json": json.dumps(raw, default=str),
        }])
        self.store.append_df("orders", df)

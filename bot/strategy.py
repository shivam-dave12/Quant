from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from .config import AssetProfile, BotConfig
from .option_models import (
    ModelSuiteConfig,
    _normalise_policy_frame,
    _policy_conviction_scores,
    _select_research_strategy_policy,
    _select_policy_per_snapshot,
    _snapshot_group_cols,
    _strategy_policy_candidate_frame,
    strategy_policy_gate_diagnostics,
)


@dataclass(frozen=True)
class StrategyDecision:
    action: str
    reason: str
    row: dict[str, Any] | None
    diagnostics: dict[str, Any]


class InstitutionalOptionStrategy:
    """Convert model scores into trade candidates with execution-aware gates.

    This layer is deliberately separate from model fitting. It treats the model
    output as forecasts, then applies portfolio/execution constraints that a
    live option desk would expect before sending an order candidate.
    """

    def __init__(self, cfg: BotConfig, asset: AssetProfile, model_metrics: dict[str, Any] | None = None):
        self.cfg = cfg
        self.asset = asset
        self.model_metrics = model_metrics or {}

    def set_model_metrics(self, model_metrics: dict[str, Any] | None) -> None:
        self.model_metrics = model_metrics or {}

    def choose(self, scored: pd.DataFrame) -> StrategyDecision:
        if scored.empty:
            return StrategyDecision("NO_TRADE", "no_scored_options", None, {})

        model_cfg = ModelSuiteConfig.from_bot_config(self.cfg, self.asset)
        data = _normalise_policy_frame(scored, model_cfg)
        for col in [
            "predicted_return",
            "prob_profit",
            "raw_prob_profit",
            "prob_iv_expansion",
            "return_q20",
            "return_q50",
            "return_q80",
            "estimated_cost",
            "edge_score",
            "model_score",
            "spread_pct",
            "bid_price",
            "offer_price",
            "bid_quantity",
            "ask_quantity",
            "volume",
            "open_interest",
            "book_pressure_score",
            "minutes_to_session_close",
            "model_rank_at_ts",
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
        ]:
            if col not in data.columns:
                data[col] = 0.0
            data[col] = pd.to_numeric(data[col], errors="coerce")

        meta_active = bool(data["meta_policy_active"].fillna(0).gt(0).any())
        gate_mode = "live_meta" if meta_active else "live"
        shadow_eligible, shadow_gate_counts = _strategy_policy_candidate_frame(data, model_cfg, mode="shadow")
        selected_policy_id = str(self.model_metrics.get("strategy_policy_selection_mode") or "legacy_live_gate")
        eligible, selected_rows, selected_policy_meta = _select_research_strategy_policy(data, model_cfg, selected_policy_id)
        legacy_eligible, gate_counts = _strategy_policy_candidate_frame(data, model_cfg, mode=gate_mode)
        diagnostics = {
            "input_rows": int(len(data)),
            "asset_id": self.asset.asset_id,
            "strategy": self.asset.model_strategy,
            "gate_mode": gate_mode,
            "selected_policy_id": selected_policy_id,
            "selected_policy_meta": selected_policy_meta,
            "meta_policy_active": meta_active,
            "min_edge_return": self.asset.min_edge_return,
            "max_spread_pct": self.asset.max_spread_pct,
            "min_meta_policy_prob": self.cfg.min_meta_policy_prob,
            "min_meta_policy_ev": self.cfg.min_meta_policy_ev,
            "shadow_passed_gate_rows": int(len(shadow_eligible)),
            "shadow_gate_pass_counts": shadow_gate_counts,
        }
        diagnostics["legacy_passed_gate_rows"] = int(len(legacy_eligible))
        diagnostics["passed_gate_rows"] = int(len(eligible))
        diagnostics["selected_policy_rows"] = int(len(selected_rows))
        diagnostics["gate_pass_counts"] = gate_counts
        execution_guard_blocked = False
        live_executable_candidates = _current_execution_candidates(eligible, self.asset, self.cfg)
        fallback_attempts: list[dict[str, Any]] = []
        if live_executable_candidates.empty:
            for fallback_policy_id in _validated_fallback_policy_ids(self.model_metrics, selected_policy_id):
                fallback_eligible, fallback_selected, fallback_meta = _select_research_strategy_policy(data, model_cfg, fallback_policy_id)
                fallback_executable = _current_execution_candidates(fallback_eligible, self.asset, self.cfg)
                fallback_attempts.append({
                    "policy_id": fallback_policy_id,
                    "passed_gate_rows": int(len(fallback_eligible)),
                    "selected_policy_rows": int(len(fallback_selected)),
                    "live_execution_prefilter_rows": int(len(fallback_executable)),
                    "quote_execution_prefilter_rows": int(fallback_executable.attrs.get("quote_execution_prefilter_rows", len(fallback_executable))),
                    "sizing_prefilter_rows": int(fallback_executable.attrs.get("sizing_prefilter_rows", len(fallback_executable))),
                })
                if fallback_executable.empty:
                    continue
                selected_policy_id = fallback_policy_id
                eligible = fallback_eligible
                selected_rows = fallback_selected
                selected_policy_meta = fallback_meta
                live_executable_candidates = fallback_executable
                diagnostics["selected_policy_id"] = selected_policy_id
                diagnostics["selected_policy_meta"] = selected_policy_meta
                diagnostics["passed_gate_rows"] = int(len(eligible))
                diagnostics["selected_policy_rows"] = int(len(selected_rows))
                break
        diagnostics["validated_policy_fallback_attempts"] = fallback_attempts
        diagnostics["quote_execution_prefilter_rows"] = int(live_executable_candidates.attrs.get("quote_execution_prefilter_rows", len(live_executable_candidates)))
        diagnostics["sizing_prefilter_rows"] = int(live_executable_candidates.attrs.get("sizing_prefilter_rows", len(live_executable_candidates)))
        diagnostics["sizing_rejected_preview"] = live_executable_candidates.attrs.get("sizing_rejected_preview")
        if not live_executable_candidates.empty:
            selected_rows = live_executable_candidates.copy()
            if "entry_side" not in selected_rows.columns:
                selected_rows["entry_side"] = selected_policy_meta.get("entry_side", "BUY")
            if "exit_side" not in selected_rows.columns:
                selected_rows["exit_side"] = selected_policy_meta.get("exit_side", "SELL")
            diagnostics["live_execution_prefilter_rows"] = int(len(live_executable_candidates))
        else:
            diagnostics["live_execution_prefilter_rows"] = 0
            if bool(selected_policy_meta.get("live_execution_guard", True)):
                selected_rows = selected_rows.iloc[0:0].copy()
                execution_guard_blocked = int(len(eligible)) > 0

        if selected_rows.empty:
            top_frame = shadow_eligible if not shadow_eligible.empty else data
            if not shadow_eligible.empty:
                group_cols = _snapshot_group_cols(shadow_eligible)
                top_frame = _select_policy_per_snapshot(shadow_eligible, group_cols, use_meta=False)
                diagnostics["shadow_selected"] = _compact_strategy_row(top_frame.sort_values(["conviction_score", "model_score", "edge_score"], ascending=False).iloc[0].to_dict())
            top = top_frame.sort_values(["model_score", "edge_score"], ascending=False).iloc[0].to_dict()
            diagnostics["top_rejected"] = {
                "trading_symbol": top.get("trading_symbol"),
                "edge_score": top.get("edge_score"),
                "predicted_return": top.get("predicted_return"),
                "prob_profit": top.get("prob_profit"),
                "raw_prob_profit": top.get("raw_prob_profit"),
                "policy_calibrated_ev": top.get("policy_calibrated_ev"),
                "policy_raw_ev": top.get("policy_raw_ev"),
                "spread_pct": top.get("spread_pct"),
                "return_q20": top.get("return_q20"),
                "return_q50": top.get("return_q50"),
                "return_q80": top.get("return_q80"),
                "adaptive_tp_pct": top.get("adaptive_tp_pct"),
                "adaptive_sl_pct": top.get("adaptive_sl_pct"),
                "adaptive_rr": top.get("adaptive_rr"),
                "adaptive_exit_score": top.get("adaptive_exit_score"),
                "meta_policy_prob": top.get("meta_policy_prob"),
                "meta_policy_ev": top.get("meta_policy_ev"),
                "meta_policy_edge": top.get("meta_policy_edge"),
            }
            diagnostics["top_failed_gates"] = strategy_policy_gate_diagnostics(data, model_cfg, mode=gate_mode, limit=20)
            sizing_blocked = int(diagnostics.get("quote_execution_prefilter_rows") or 0) > 0 and int(diagnostics.get("sizing_prefilter_rows") or 0) == 0
            preview = diagnostics.get("sizing_rejected_preview")
            if sizing_blocked and isinstance(preview, dict):
                top = {**top, **preview}
                diagnostics["top_rejected"] = {**diagnostics["top_rejected"], **preview}
            if sizing_blocked:
                reason = "no_live_candidate_fits_risk_budget"
            else:
                reason = "no_live_executable_quote_for_validated_policy" if execution_guard_blocked else "institutional_gates_failed"
            return StrategyDecision("NO_TRADE", reason, top, diagnostics)

        group_cols = _snapshot_group_cols(eligible)
        if "conviction_score" not in selected_rows.columns:
            selected_rows = selected_rows.copy()
            if selected_policy_id == "legacy_live_gate":
                selected_rows["conviction_score"] = _policy_conviction_scores(selected_rows, group_cols, use_meta=meta_active)
            else:
                selected_rows["conviction_score"] = (
                    selected_rows["model_score"].fillna(0)
                    + selected_rows["edge_score"].fillna(0)
                    + selected_rows["policy_raw_ev"].fillna(0)
        )
        best = selected_rows.sort_values(["conviction_score", "model_score", "edge_score"], ascending=False).iloc[0].to_dict()
        diagnostics["selected"] = _compact_strategy_row(best)
        return StrategyDecision("TRADE_CANDIDATE", "institutional_gates_passed", best, diagnostics)


def _validated_fallback_policy_ids(model_metrics: dict[str, Any], selected_policy_id: str) -> list[str]:
    research = model_metrics.get("strategy_policy_research") if isinstance(model_metrics, dict) else None
    if not isinstance(research, list):
        return []
    rows: list[dict[str, Any]] = [row for row in research if isinstance(row, dict)]
    rows = [
        row for row in rows
        if bool(row.get("passed_live_thresholds")) and str(row.get("policy_id") or "") != selected_policy_id
    ]
    rows.sort(
        key=lambda row: (
            float(row.get("research_score") or 0.0),
            float(row.get("sharpe") or 0.0),
            float(row.get("win_rate") or 0.0),
            int(row.get("count") or 0),
        ),
        reverse=True,
    )
    return [str(row.get("policy_id")) for row in rows if row.get("policy_id")]


def _pct_rank(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").rank(pct=True).fillna(0.5)


def _compact_strategy_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "trading_symbol": row.get("trading_symbol"),
        "entry_side": row.get("entry_side", "BUY"),
        "exit_side": row.get("exit_side", "SELL"),
        "lot_size": row.get("lot_size"),
        "premium_per_lot": row.get("premium_per_lot"),
        "loss_per_lot_at_sl": row.get("loss_per_lot_at_sl"),
        "conviction_score": row.get("conviction_score"),
        "edge_score": row.get("edge_score"),
        "predicted_return": row.get("predicted_return"),
        "prob_profit": row.get("prob_profit"),
        "raw_prob_profit": row.get("raw_prob_profit"),
        "policy_calibrated_ev": row.get("policy_calibrated_ev"),
        "policy_raw_ev": row.get("policy_raw_ev"),
        "bid_price": row.get("bid_price"),
        "offer_price": row.get("offer_price"),
        "bid_quantity": row.get("bid_quantity"),
        "ask_quantity": row.get("ask_quantity"),
        "spread_pct": row.get("spread_pct"),
        "adaptive_tp_pct": row.get("adaptive_tp_pct"),
        "adaptive_sl_pct": row.get("adaptive_sl_pct"),
        "adaptive_rr": row.get("adaptive_rr"),
        "adaptive_exit_score": row.get("adaptive_exit_score"),
        "meta_policy_active": row.get("meta_policy_active"),
        "meta_policy_prob": row.get("meta_policy_prob"),
        "meta_policy_ev": row.get("meta_policy_ev"),
        "meta_policy_edge": row.get("meta_policy_edge"),
    }


def _current_execution_candidates(rows: pd.DataFrame, asset: AssetProfile, cfg: BotConfig | None = None) -> pd.DataFrame:
    if rows.empty:
        return rows.copy()
    data = rows.copy()
    for col in [
        "bid_price", "offer_price", "bid_quantity", "ask_quantity", "spread_pct", "policy_raw_ev", "model_score", "edge_score",
        "lot_size", "adaptive_sl_pct", "book_pressure_score", "conviction_score",
    ]:
        if col not in data.columns:
            data[col] = pd.NA
        data[col] = pd.to_numeric(data[col], errors="coerce")
    default_lot_size = int(getattr(asset, "default_lot_size", 0) or 0)
    if default_lot_size > 0:
        data["lot_size"] = data["lot_size"].where(data["lot_size"].fillna(0).gt(0), float(default_lot_size))
    executable = data[
        data["bid_price"].fillna(0).gt(0)
        & data["offer_price"].fillna(0).gt(0)
        & data["bid_quantity"].fillna(0).gt(0)
        & data["ask_quantity"].fillna(0).gt(0)
        & data["spread_pct"].fillna(999).le(asset.max_spread_pct)
    ].copy()
    quote_executable_rows = int(len(executable))
    if executable.empty:
        executable.attrs["quote_execution_prefilter_rows"] = 0
        executable.attrs["sizing_prefilter_rows"] = 0
        return executable
    entry_side = executable["entry_side"].astype(str).str.upper() if "entry_side" in executable.columns else pd.Series("BUY", index=executable.index)
    entry_price = executable["offer_price"].where(~entry_side.eq("SELL"), executable["bid_price"])
    known_lot = executable["lot_size"].fillna(0).gt(0)
    if bool(known_lot.any()) and cfg is not None:
        sl = executable["adaptive_sl_pct"].copy()
        fallback_sl = pd.Series(float(asset.sl_pct), index=executable.index)
        fallback_sl.loc[entry_side.eq("SELL")] = float(asset.short_sl_pct)
        sl = sl.where(sl.gt(0), fallback_sl)
        executable["premium_per_lot"] = entry_price * executable["lot_size"]
        executable["loss_per_lot_at_sl"] = executable["premium_per_lot"] * sl
        preview_source = executable.copy()
        risk_budget = float(cfg.account_capital) * float(asset.risk_per_trade_pct)
        risk_limit = risk_budget
        if str(getattr(asset, "segment", "")).upper() == "COMMODITY":
            risk_limit *= max(1.0, float(getattr(cfg, "commodity_single_lot_risk_tolerance", 1.0) or 1.0))
        sized = (
            ~known_lot
            | (
                executable["premium_per_lot"].le(float(asset.max_premium_value_per_trade))
                & executable["loss_per_lot_at_sl"].le(risk_limit)
            )
        )
        executable = executable[sized.fillna(False)].copy()
        executable.attrs["quote_execution_prefilter_rows"] = quote_executable_rows
        executable.attrs["sizing_prefilter_rows"] = int(len(executable))
        if executable.empty and not preview_source.empty:
            if "entry_side" in preview_source.columns and preview_source["entry_side"].astype(str).str.upper().eq("SELL").any():
                preview_rows = preview_source.sort_values(["model_score", "spread_pct", "book_pressure_score"], ascending=[True, True, False])
            else:
                preview_rows = preview_source.sort_values(["policy_raw_ev", "model_score", "edge_score"], ascending=False)
            executable.attrs["sizing_rejected_preview"] = _compact_strategy_row(preview_rows.iloc[0].to_dict())
        if executable.empty:
            return executable
    else:
        executable.attrs["quote_execution_prefilter_rows"] = quote_executable_rows
        executable.attrs["sizing_prefilter_rows"] = int(len(executable))
    if "conviction_score" in executable.columns and executable["conviction_score"].notna().any():
        out = executable.sort_values(["conviction_score", "spread_pct", "model_score"], ascending=[False, True, False])
    elif "entry_side" in executable.columns and executable["entry_side"].astype(str).str.upper().eq("SELL").any():
        out = executable.sort_values(["model_score", "spread_pct", "book_pressure_score"], ascending=[True, True, False])
    else:
        out = executable.sort_values(["policy_raw_ev", "model_score", "edge_score"], ascending=False)
    out.attrs.update(executable.attrs)
    return out

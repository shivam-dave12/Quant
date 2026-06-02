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
        live_executable_candidates = _current_execution_candidates(eligible, self.asset)
        if not live_executable_candidates.empty:
            selected_rows = live_executable_candidates
            diagnostics["live_execution_prefilter_rows"] = int(len(live_executable_candidates))
        else:
            diagnostics["live_execution_prefilter_rows"] = 0

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
            return StrategyDecision("NO_TRADE", "institutional_gates_failed", top, diagnostics)

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


def _pct_rank(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").rank(pct=True).fillna(0.5)


def _compact_strategy_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "trading_symbol": row.get("trading_symbol"),
        "entry_side": row.get("entry_side", "BUY"),
        "exit_side": row.get("exit_side", "SELL"),
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


def _current_execution_candidates(rows: pd.DataFrame, asset: AssetProfile) -> pd.DataFrame:
    if rows.empty:
        return rows.copy()
    data = rows.copy()
    for col in ["bid_price", "offer_price", "bid_quantity", "ask_quantity", "spread_pct", "policy_raw_ev", "model_score", "edge_score"]:
        if col not in data.columns:
            data[col] = pd.NA
        data[col] = pd.to_numeric(data[col], errors="coerce")
    executable = data[
        data["bid_price"].fillna(0).gt(0)
        & data["offer_price"].fillna(0).gt(0)
        & data["bid_quantity"].fillna(0).gt(0)
        & data["ask_quantity"].fillna(0).gt(0)
        & data["spread_pct"].fillna(999).le(asset.max_spread_pct)
    ].copy()
    if executable.empty:
        return executable
    if "entry_side" in executable.columns and executable["entry_side"].astype(str).str.upper().eq("SELL").any():
        return executable.sort_values(["model_score", "spread_pct", "book_pressure_score"], ascending=[True, True, False])
    return executable.sort_values(["policy_raw_ev", "model_score", "edge_score"], ascending=False)

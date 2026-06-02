from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import AssetProfile, BotConfig
from .option_models import training_readiness_from_store
from .storage import Store


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_rundown(path: str | Path) -> dict[str, Any]:
    path = Path(path)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_rundown(path: str | Path, payload: dict[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


def _table_summary(store: Store, table: str, asset_id: str) -> dict[str, Any]:
    try:
        df = store.query_df(
            f"""
            SELECT count(*) AS rows,
                   count(distinct trading_symbol) AS symbols,
                   min(ts) AS first_ts,
                   max(ts) AS latest_ts
            FROM {table}
            WHERE asset_id = ?
            """,
            (asset_id,),
        )
    except Exception as exc:
        return {"error": str(exc), "rows": 0, "symbols": 0, "first_ts": None, "latest_ts": None}
    row = df.iloc[0].to_dict() if not df.empty else {}
    return {
        "rows": int(row.get("rows") or 0),
        "symbols": int(row.get("symbols") or 0),
        "first_ts": row.get("first_ts"),
        "latest_ts": row.get("latest_ts"),
    }


def _model_summary(cfg: BotConfig, asset: AssetProfile) -> dict[str, Any]:
    model_path = cfg.model_suite_path_for(asset.asset_id)
    meta_path = cfg.model_suite_meta_path_for(asset.asset_id)
    out: dict[str, Any] = {
        "model_path": str(model_path),
        "model_exists": model_path.exists(),
        "meta_path": str(meta_path),
        "meta_exists": meta_path.exists(),
    }
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            out["passed_live_gate"] = bool(meta.get("passed_live_gate", False))
            out["created_at_utc"] = meta.get("created_at_utc")
            out["top1_win_rate"] = meta.get("top1_per_snapshot_win_rate")
            out["top1_sharpe"] = meta.get("top1_per_snapshot_sharpe")
            out["adaptive_top1_win_rate"] = meta.get("adaptive_top1_per_snapshot_win_rate")
            out["adaptive_top1_sharpe"] = meta.get("adaptive_top1_per_snapshot_sharpe")
            out["adaptive_top1_alpha"] = meta.get("adaptive_top1_per_snapshot_alpha_vs_universe")
            out["shadow_policy_count"] = meta.get("shadow_policy_count")
            out["shadow_policy_win_rate"] = meta.get("shadow_policy_win_rate")
            out["shadow_policy_sharpe"] = meta.get("shadow_policy_sharpe")
            out["strategy_policy_count"] = meta.get("strategy_policy_count")
            out["strategy_policy_win_rate"] = meta.get("strategy_policy_win_rate")
            out["strategy_policy_sharpe"] = meta.get("strategy_policy_sharpe")
            out["strategy_policy_alpha"] = meta.get("strategy_policy_alpha_vs_universe")
            out["strategy_policy_target_hit_rate"] = meta.get("strategy_policy_target_hit_rate")
            out["strategy_policy_stop_hit_rate"] = meta.get("strategy_policy_stop_hit_rate")
            out["strategy_policy_selection_mode"] = meta.get("strategy_policy_selection_mode")
            out["strategy_policy_meta_active"] = meta.get("strategy_policy_meta_active")
            out["strategy_policy_avg_meta_prob"] = meta.get("strategy_policy_avg_meta_prob")
            out["strategy_policy_avg_meta_ev"] = meta.get("strategy_policy_avg_meta_ev")
            cfg_payload = meta.get("config") if isinstance(meta.get("config"), dict) else {}
            min_width = float((cfg_payload or {}).get("min_snapshot_width") or 4)
            median_width = meta.get("median_options_per_snapshot")
            out["median_options_per_snapshot"] = median_width
            out["min_snapshot_width"] = min_width
            out["model_usable_for_scoring"] = bool(
                median_width is not None
                and float(median_width) >= min_width
            )
        except Exception as exc:
            out["meta_error"] = str(exc)
    return out


def build_rundown(
    cfg: BotConfig,
    store: Store,
    assets: list[AssetProfile],
    *,
    started_at_utc: str,
    cycle: int = 0,
    last_results: dict[str, Any] | None = None,
    previous: dict[str, Any] | None = None,
) -> dict[str, Any]:
    asset_payload: dict[str, Any] = {}
    for asset in assets:
        asset_payload[asset.asset_id] = {
            "label": asset.label,
            "underlying": asset.underlying,
            "exchange": asset.exchange,
            "segment": asset.segment,
            "session": {
                "timezone": asset.session_timezone,
                "open": asset.session_open,
                "close": asset.session_close,
            },
            "expiry_dates": list(asset.expiry_dates),
            "strategy": asset.model_strategy,
            "option_chain_mode": asset.option_chain_mode,
            "chain": _table_summary(store, "option_chain_snapshots", asset.asset_id),
            "quotes": _table_summary(store, "quote_snapshots", asset.asset_id),
            "training_readiness": training_readiness_from_store(cfg, store, asset),
            "model": _model_summary(cfg, asset),
            "last_result": (last_results or {}).get(asset.asset_id),
        }
    previous_started_at = previous.get("started_at_utc") if previous else None
    previous_updated_at = previous.get("updated_at_utc") if previous else None
    return {
        "schema_version": 1,
        "started_at_utc": started_at_utc,
        "updated_at_utc": utc_now_iso(),
        "cycle": int(cycle),
        "db_path": str(cfg.db_path),
        "previous_started_at_utc": previous_started_at,
        "previous_updated_at_utc": previous_updated_at,
        "assets": asset_payload,
    }


def compact_startup_summary(rundown: dict[str, Any]) -> str:
    if not rundown:
        return "no_previous_rundown"
    parts = []
    for asset_id, asset in (rundown.get("assets") or {}).items():
        chain_rows = ((asset.get("chain") or {}).get("rows")) or 0
        labelled = ((asset.get("training_readiness") or {}).get("estimated_labelled_rows")) or 0
        model = asset.get("model") or {}
        model_exists = model.get("model_exists") or False
        usable = model.get("model_usable_for_scoring")
        parts.append(f"{asset_id}:rows={chain_rows},labels={labelled},model={int(bool(model_exists))},usable={int(bool(usable))}")
    return "; ".join(parts) if parts else "previous_rundown_empty"

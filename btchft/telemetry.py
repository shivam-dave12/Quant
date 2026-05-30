from __future__ import annotations

import gzip
import json
from collections import Counter, deque
from pathlib import Path
from typing import Any, Iterable


def _open_text(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    if not path.exists():
        return
    with _open_text(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def tail_jsonl(path: Path, n: int = 1000) -> list[dict[str, Any]]:
    q: deque[dict[str, Any]] = deque(maxlen=n)
    for row in iter_jsonl(path):
        q.append(row)
    return list(q)


def summarize_live_dir(live_dir: str | Path, *, tail_rows: int = 5000) -> dict[str, Any]:
    live = Path(live_dir)
    raw_path = live / "raw_delta_events.jsonl.gz"
    feat_path = live / "features.jsonl.gz"
    decision_path = live / "decisions.jsonl.gz"
    state_path = live / "runtime_state.json"
    telemetry_path = live / "ml_telemetry_snapshot.json"

    latest_state: dict[str, Any] | None = None
    for p in [telemetry_path, state_path]:
        if p.exists():
            try:
                latest_state = json.loads(p.read_text(encoding="utf-8"))
                break
            except Exception:
                pass

    raw_tail = tail_jsonl(raw_path, min(tail_rows, 10000)) if raw_path.exists() else []
    feat_tail = tail_jsonl(feat_path, tail_rows) if feat_path.exists() else []
    dec_tail = tail_jsonl(decision_path, tail_rows) if decision_path.exists() else []

    raw_types = Counter()
    streams = Counter()
    for r in raw_tail:
        streams[str(r.get("stream", "unknown"))] += 1
        raw = r.get("raw") if isinstance(r.get("raw"), dict) else {}
        raw_types[str(raw.get("type", r.get("type", "unknown")))] += 1

    action_counts = Counter(str(r.get("action", "UNKNOWN")) for r in dec_tail if r.get("type") != "metadata")
    gate_counts = Counter(str(r.get("gate_reason", "NONE")) for r in dec_tail if r.get("type") != "metadata")
    signals = [r for r in dec_tail if r.get("chosen_signal")]
    sig_by_side = Counter(str((r.get("chosen_signal") or {}).get("side", "unknown")) for r in signals)
    sig_by_horizon = Counter(str((r.get("chosen_signal") or {}).get("horizon_ms", "unknown")) for r in signals)

    last_feature = next((r for r in reversed(feat_tail) if r.get("type") != "metadata"), None)
    last_decision = next((r for r in reversed(dec_tail) if r.get("type") != "metadata"), None)

    models = (latest_state or {}).get("models", {}) if latest_state else {}
    model_tests = None
    if last_decision and isinstance(last_decision.get("model_tests"), dict):
        model_tests = last_decision["model_tests"]
    elif isinstance(models.get("tests_running"), dict):
        model_tests = models.get("tests_running")

    status = "UNKNOWN"
    if latest_state:
        book = latest_state.get("book_integrity", {})
        feed = latest_state.get("feed_health", {})
        if latest_state.get("sticky_halt_reason") or book.get("halted") or feed.get("stalled"):
            status = "DATA_BAD_OR_HALTED"
        elif models.get("promoted_horizon_ms"):
            status = "DATA_OK_MODEL_PROMOTED"
        elif models.get("matured_labels", 0) > 0:
            status = "DATA_OK_TRAINING_OK_NO_PROMOTION_YET"
        else:
            status = "DATA_OK_WAITING_FOR_LABELS"

    files = {}
    for p in [raw_path, feat_path, decision_path, state_path, telemetry_path]:
        files[p.name] = {"exists": p.exists(), "path": str(p), "size_bytes": p.stat().st_size if p.exists() else 0}

    return {
        "live_dir": str(live),
        "overall_status": status,
        "files": files,
        "raw_tail_counts": {"rows_sampled": len(raw_tail), "streams": dict(streams), "message_types": dict(raw_types)},
        "feature_tail_counts": {"rows_sampled": len(feat_tail), "last_book_seq": last_feature.get("book_seq") if last_feature else None},
        "decision_tail_counts": {
            "rows_sampled": len(dec_tail),
            "actions": dict(action_counts),
            "gate_reasons": dict(gate_counts),
            "signals_seen": len(signals),
            "signals_by_side": dict(sig_by_side),
            "signals_by_horizon": dict(sig_by_horizon),
            "last_decision": last_decision,
        },
        "latest_runtime_state": latest_state,
        "model_tests": model_tests,
        "how_to_read": {
            "pipeline_ok": "raw/features/decisions increase; feed not stalled; no checksum or sequence failures; matured_labels and samples increase",
            "edge_ok": "prequential_mean_return positive after costs, enough evaluations, promotion_test.passed true, promoted_horizon_ms not null",
            "live_ok": "edge_ok plus real_fill_count >= configured gate and live_gate_reason null in LIVE mode",
        },
    }


def print_human(summary: dict[str, Any]) -> str:
    state = summary.get("latest_runtime_state") or {}
    models = state.get("models") or {}
    feed = state.get("feed_health") or {}
    book = state.get("book_integrity") or {}
    costs = state.get("costs") or {}
    lines = []
    lines.append(f"ML CONTROL ROOM | status={summary.get('overall_status')}")
    lines.append(f"live_dir={summary.get('live_dir')}")
    lines.append("")
    lines.append("DATA FEED")
    lines.append(f"  public_events={feed.get('public_event_count')} book_events={feed.get('book_event_count')} trades={feed.get('trade_event_count')} stalled={feed.get('stalled')}")
    lines.append(f"  snapshots={book.get('snapshots')} updates={book.get('updates')} checksum_failures={book.get('checksum_failures')} sequence_gaps={book.get('sequence_gaps')}")
    lines.append("")
    lines.append("TRAINING")
    lines.append(f"  matured_labels={models.get('matured_labels')} pending_labels={models.get('pending_labels')} prequential_count={models.get('prequential_count')}")
    lines.append(f"  prequential_mean_return={models.get('prequential_mean_return')} promoted_horizon_ms={models.get('promoted_horizon_ms')}")
    lines.append(f"  round_trip_cost_bps={costs.get('round_trip_bps')} real_fill_count={costs.get('real_fill_count')}")
    lines.append("")
    lines.append("MODELS")
    regs = models.get("regressors") or {}
    clfs = models.get("classifiers") or {}
    for h in sorted(set(regs) | set(clfs), key=lambda x: int(x) if str(x).isdigit() else 10**9):
        r = regs.get(h, {})
        c = clfs.get(h, {})
        lines.append(f"  {h}ms: reg_samples={r.get('samples')} residual_std_bps={r.get('residual_std_bps')} clf_samples={c.get('samples')} clf_accuracy={c.get('accuracy')}")
    lines.append("")
    dec = summary.get("decision_tail_counts", {})
    lines.append("SIGNALS / DECISIONS IN TAIL")
    lines.append(f"  actions={dec.get('actions')}")
    lines.append(f"  signals_seen={dec.get('signals_seen')} by_side={dec.get('signals_by_side')} by_horizon={dec.get('signals_by_horizon')}")
    lines.append(f"  gate_reasons={dec.get('gate_reasons')}")
    return "\n".join(lines)

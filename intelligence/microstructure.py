"""Microstructure feature extraction and executable-quality calculations."""
from __future__ import annotations
from core.identifiers import VenueMicrostate

def local_flow_score(state: VenueMicrostate) -> float:
    scale = max(1.0, sum(state.bid_depth_usd_by_band.values()) + sum(state.ask_depth_usd_by_band.values()))
    raw = (state.ofi_usd_10s + state.tfi_usd_10s) / scale
    return max(-1.0, min(1.0, raw))

def execution_quality(state: VenueMicrostate) -> float:
    spread_quality = max(0.0, 1.0 - state.spread_bps / 30.0)
    near_depth = state.bid_depth_usd_by_band.get("0-1", 0.0) + state.ask_depth_usd_by_band.get("0-1", 0.0)
    depth_quality = min(1.0, near_depth / 100_000.0)
    latency_quality = 1.0 if state.update_latency_ms is None else max(0.0, 1.0 - state.update_latency_ms / 500.0)
    return state.feed_quality_score * (0.45 * spread_quality + 0.35 * depth_quality + 0.20 * latency_quality)

def venue_feature_row(state: VenueMicrostate) -> list[float]:
    return [state.spread_bps, state.microprice / state.mid - 1.0, state.obi_by_band.get("0-1", 0.0), state.ofi_usd_1s,
            state.ofi_usd_10s, state.tfi_usd_1s, state.tfi_usd_10s, state.feed_quality_score,
            0.0 if state.basis_bps is None else state.basis_bps, 0.0 if state.funding_rate is None else state.funding_rate]

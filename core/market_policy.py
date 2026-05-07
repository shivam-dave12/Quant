"""
core/market_policy.py — instrument-native runtime policy
=========================================================

Centralised, asset-class aware execution/risk/runtime policy.  This removes the
old failure mode where BTC-calibrated constants were reused for xStock, metal,
index-token and crypto contracts.

The policy is intentionally derived from the confirmed TradableInstrument plus
config base priors.  Config remains the default prior source; this module turns
those priors into per-contract runtime values.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict, replace
from typing import Any, Dict, Optional
import math

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore

from core.instruments import AssetClass, TradableInstrument, current_instrument


def _cfg(name: str, default: Any) -> Any:
    if config is None:
        return default
    return getattr(config, name, default)


def _f(name: str, default: float) -> float:
    try:
        v = float(_cfg(name, default))
        return v if math.isfinite(v) else float(default)
    except Exception:
        return float(default)


def _i(name: str, default: int) -> int:
    try:
        return int(_cfg(name, default))
    except Exception:
        return int(default)


def _b(name: str, default: bool) -> bool:
    try:
        return bool(_cfg(name, default))
    except Exception:
        return bool(default)


@dataclass(frozen=True)
class InstrumentPolicy:
    asset_id: str
    asset_class: str
    leverage: int
    margin_pct: float
    risk_multiplier: float
    min_margin_usd: float
    tick_eval_sec: float
    loop_interval_sec: float
    min_1m_bars: int
    min_5m_bars: int
    atr_min_pctile: float
    atr_max_pctile: float
    max_hold_sec: int
    cooldown_sec: int
    loss_lockout_sec: int
    min_rr: float
    max_rr: float
    sl_buffer_atr_mult: float
    trail_min_move_atr: float
    slippage_tolerance: float
    spread_soft_atr_ratio: float
    spread_max_atr_ratio: float
    spread_max_bps: float
    spread_max_ticks: float
    spread_min_size_mult: float
    spread_haircut_max: float
    ob_depth_levels: int
    tick_agg_window_sec: float
    vwap_window: int
    cvd_window: int
    notes: str = ""
    # v77 target/stop behavior profile.  These are not static trade triggers;
    # they calibrate how the pool selector prices objectives for each product's
    # auction behavior.  BTC can use a primary-liquidity funded runner, metals
    # remain slower/cleaner, and xStocks receive spread-aware target behavior.
    target_style: str = "single_full_position"
    tp_durable_rr_floor: float = 1.35
    tp_be_move_mult: float = 1.80
    tp_terminal_profile_floor: float = 0.55
    tp_terminal_reach_floor: float = 0.05
    tp_primary_objective_min_rr: float = 1.05
    tp_path_support_lift_max: float = 0.00
    tp_posterior_blend_weight: float = 0.50
    tp_min_delivery_prob_floor: float = 1e-6
    # v79 predictive institutional flow: do not wait for a perfect one-shot TP.
    # These calibrate whether a post-sweep move may use a live accepted shelf as
    # invalidation and a liquidity ladder/runner as the target path.  They do
    # not create synthetic fallback targets and they do not permit late chasing.
    predictive_flow_enabled: bool = True
    shelf_sl_enabled: bool = True
    shelf_sl_max_distance_atr: float = 3.20
    shelf_sl_min_significance: float = 1.20
    shelf_sl_min_delivery_atr: float = 0.55
    shelf_sl_min_entry_distance_atr: float = 0.18
    late_entry_max_chase_atr: float = 2.20
    late_entry_tp_compression_rr: float = 0.35

    @property
    def evaluation_interval_sec(self) -> float:
        """Backward-compatible alias used by orchestration/Telegram code.

        v9 renamed the per-instrument scan cadence to ``loop_interval_sec``.
        Some v10 notification paths still referenced the old field name.
        Keep this alias so command-center/startup messages cannot crash the bot.
        """
        return float(self.loop_interval_sec)

    def asdict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["evaluation_interval_sec"] = self.evaluation_interval_sec
        return d


def _cap_leverage(inst: Optional[TradableInstrument], default: int) -> int:
    base = max(1, int(default))
    if inst is None:
        return base
    try:
        mx = float(getattr(inst, 'max_leverage', 0.0) or 0.0)
        if mx > 0:
            return max(1, min(base, int(mx)))
    except Exception:
        pass
    if inst.asset_class in (AssetClass.EQUITY, AssetClass.COMMODITY, AssetClass.INDEX):
        return min(base, 25)
    return base


def build_instrument_policy(inst: Optional[TradableInstrument]) -> InstrumentPolicy:
    """Build the active policy for one contract.

    Design intent:
      • Crypto/BTC keeps the tight, fast settings.
      • xStock/RWA contracts use slower cadence, lower margin allocation, and
        spread impairment instead of BTC-style hard blocking.
      • Metals/tokens sit between crypto and equity.
      • All values are centralised here instead of scattered inside strategy.
    """
    ac = getattr(inst, 'asset_class', AssetClass.CRYPTO) if inst is not None else AssetClass.CRYPTO
    asset_id = getattr(inst, 'asset_id', 'GLOBAL') if inst is not None else 'GLOBAL'

    lev = _cap_leverage(inst, _i('LEVERAGE', 30))
    base_margin = _f('QUANT_MARGIN_PCT', 0.20)
    base_rr = _f('MIN_RISK_REWARD_RATIO', 2.0)
    base_cooldown = _i('QUANT_COOLDOWN_SEC', 300)

    if ac == AssetClass.EQUITY:
        return InstrumentPolicy(
            asset_id=asset_id, asset_class=ac.value, leverage=lev,
            margin_pct=min(base_margin, _f('POLICY_EQUITY_MARGIN_PCT', 0.12)),
            risk_multiplier=_f('POLICY_EQUITY_RISK_MULT', 0.55),
            min_margin_usd=_f('POLICY_EQUITY_MIN_MARGIN_USD', 0.5),
            tick_eval_sec=_f('POLICY_EQUITY_TICK_EVAL_SEC', 0.75),
            loop_interval_sec=_f('POLICY_EQUITY_LOOP_INTERVAL_SEC', 0.75),
            min_1m_bars=_i('POLICY_EQUITY_MIN_1M_BARS', 90),
            min_5m_bars=_i('POLICY_EQUITY_MIN_5M_BARS', 70),
            atr_min_pctile=_f('POLICY_EQUITY_ATR_MIN_PCTILE', 0.03),
            atr_max_pctile=_f('POLICY_EQUITY_ATR_MAX_PCTILE', 0.985),
            max_hold_sec=_i('POLICY_EQUITY_MAX_HOLD_SEC', 5400),
            cooldown_sec=min(base_cooldown, _i('POLICY_EQUITY_COOLDOWN_SEC', 180)),
            loss_lockout_sec=_i('POLICY_EQUITY_LOSS_LOCKOUT_SEC', 1800),
            min_rr=max(1.45, min(base_rr, _f('POLICY_EQUITY_MIN_RR', 1.75))),
            max_rr=_f('POLICY_EQUITY_MAX_RR', 5.0),
            sl_buffer_atr_mult=_f('POLICY_EQUITY_SL_BUFFER_ATR', 0.55),
            trail_min_move_atr=_f('POLICY_EQUITY_TRAIL_MIN_MOVE_ATR', 0.06),
            slippage_tolerance=_f('POLICY_EQUITY_SLIPPAGE_TOL', 0.0010),
            spread_soft_atr_ratio=_f('QUANT_SPREAD_SOFT_ATR_RATIO_EQUITY', 0.50),
            spread_max_atr_ratio=_f('QUANT_MAX_SPREAD_ATR_RATIO_EQUITY', 4.00),
            spread_max_bps=_f('QUANT_MAX_SPREAD_BPS_EQUITY', 35.0),
            spread_max_ticks=_f('QUANT_MAX_SPREAD_TICKS_EQUITY', 8.0),
            spread_min_size_mult=_f('QUANT_SPREAD_MIN_SIZE_MULT', 0.35),
            spread_haircut_max=_f('QUANT_SPREAD_SIZE_HAIRCUT_MAX', 0.55),
            ob_depth_levels=_i('POLICY_EQUITY_OB_DEPTH_LEVELS', 3),
            tick_agg_window_sec=_f('POLICY_EQUITY_TICK_AGG_WINDOW_SEC', 45.0),
            vwap_window=_i('POLICY_EQUITY_VWAP_WINDOW', 60),
            cvd_window=_i('POLICY_EQUITY_CVD_WINDOW', 30),
            notes='xStock/RWA equity policy; no BTC lot/risk assumptions',
        )

    if ac == AssetClass.COMMODITY:
        return InstrumentPolicy(
            asset_id=asset_id, asset_class=ac.value, leverage=lev,
            margin_pct=min(base_margin, _f('POLICY_COMMODITY_MARGIN_PCT', 0.14)),
            risk_multiplier=_f('POLICY_COMMODITY_RISK_MULT', 0.70),
            min_margin_usd=_f('POLICY_COMMODITY_MIN_MARGIN_USD', 0.5),
            tick_eval_sec=_f('POLICY_COMMODITY_TICK_EVAL_SEC', 0.50),
            loop_interval_sec=_f('POLICY_COMMODITY_LOOP_INTERVAL_SEC', 0.50),
            min_1m_bars=_i('POLICY_COMMODITY_MIN_1M_BARS', 85),
            min_5m_bars=_i('POLICY_COMMODITY_MIN_5M_BARS', 65),
            atr_min_pctile=_f('POLICY_COMMODITY_ATR_MIN_PCTILE', 0.04),
            atr_max_pctile=_f('POLICY_COMMODITY_ATR_MAX_PCTILE', 0.985),
            max_hold_sec=_i('POLICY_COMMODITY_MAX_HOLD_SEC', 4800),
            cooldown_sec=min(base_cooldown, _i('POLICY_COMMODITY_COOLDOWN_SEC', 210)),
            loss_lockout_sec=_i('POLICY_COMMODITY_LOSS_LOCKOUT_SEC', 1800),
            min_rr=max(1.55, min(base_rr, _f('POLICY_COMMODITY_MIN_RR', 1.85))),
            max_rr=_f('POLICY_COMMODITY_MAX_RR', 5.0),
            sl_buffer_atr_mult=_f('POLICY_COMMODITY_SL_BUFFER_ATR', 0.50),
            trail_min_move_atr=_f('POLICY_COMMODITY_TRAIL_MIN_MOVE_ATR', 0.07),
            slippage_tolerance=_f('POLICY_COMMODITY_SLIPPAGE_TOL', 0.0008),
            spread_soft_atr_ratio=_f('QUANT_SPREAD_SOFT_ATR_RATIO_COMMODITY', 0.50),
            spread_max_atr_ratio=_f('QUANT_MAX_SPREAD_ATR_RATIO_COMMODITY', 2.00),
            spread_max_bps=_f('QUANT_MAX_SPREAD_BPS_COMMODITY', 45.0),
            spread_max_ticks=_f('QUANT_MAX_SPREAD_TICKS_COMMODITY', 10.0),
            spread_min_size_mult=_f('QUANT_SPREAD_MIN_SIZE_MULT', 0.35),
            spread_haircut_max=_f('QUANT_SPREAD_SIZE_HAIRCUT_MAX', 0.55),
            ob_depth_levels=_i('POLICY_COMMODITY_OB_DEPTH_LEVELS', 4),
            tick_agg_window_sec=_f('POLICY_COMMODITY_TICK_AGG_WINDOW_SEC', 40.0),
            vwap_window=_i('POLICY_COMMODITY_VWAP_WINDOW', 55),
            cvd_window=_i('POLICY_COMMODITY_CVD_WINDOW', 25),
            notes='commodity-token policy; instrument-native tick/lot/ATR',
        )

    # Crypto/default policy keeps the original aggressive BTC runtime prior.
    return InstrumentPolicy(
        asset_id=asset_id, asset_class=getattr(ac, 'value', str(ac)), leverage=lev,
        margin_pct=base_margin,
        risk_multiplier=_f('POLICY_CRYPTO_RISK_MULT', 1.0),
        min_margin_usd=_f('MIN_MARGIN_PER_TRADE', 1.0),
        tick_eval_sec=_f('ENTRY_EVALUATION_INTERVAL_SECONDS', 1.0),
        loop_interval_sec=_f('POLICY_CRYPTO_LOOP_INTERVAL_SEC', float(_cfg('SCANNER_TICK_SLEEP_SEC', 0.25))),
        min_1m_bars=_i('MIN_CANDLES_1M', 80),
        min_5m_bars=_i('MIN_CANDLES_5M', 60),
        atr_min_pctile=_f('QUANT_ATR_MIN_PCTILE', 0.05),
        atr_max_pctile=_f('QUANT_ATR_MAX_PCTILE', 0.97),
        max_hold_sec=_i('QUANT_MAX_HOLD_SEC', 3600),
        cooldown_sec=base_cooldown,
        loss_lockout_sec=_i('QUANT_LOSS_LOCKOUT_SEC', 1800),
        min_rr=base_rr,
        max_rr=_f('QUANT_TP_MAX_RR', 3.5),
        sl_buffer_atr_mult=_f('QUANT_SL_BUFFER_ATR_MULT', 0.4),
        trail_min_move_atr=_f('SL_MIN_IMPROVEMENT_ATR_MULT', 0.08),
        slippage_tolerance=_f('QUANT_SLIPPAGE_TOLERANCE', 0.0005),
        spread_soft_atr_ratio=_f('QUANT_SPREAD_SOFT_ATR_RATIO_CRYPTO', 0.30),
        spread_max_atr_ratio=_f('QUANT_MAX_SPREAD_ATR_RATIO', 0.50),
        spread_max_bps=_f('QUANT_MAX_SPREAD_BPS_CRYPTO', 12.0),
        spread_max_ticks=_f('QUANT_MAX_SPREAD_TICKS_CRYPTO', 10.0),
        spread_min_size_mult=_f('QUANT_SPREAD_MIN_SIZE_MULT', 0.35),
        spread_haircut_max=_f('QUANT_SPREAD_SIZE_HAIRCUT_MAX', 0.55),
        ob_depth_levels=_i('QUANT_OB_DEPTH_LEVELS', 5),
        tick_agg_window_sec=_f('QUANT_TICK_AGG_WINDOW_SEC', 30.0),
        vwap_window=_i('QUANT_VWAP_WINDOW', 50),
        cvd_window=_i('QUANT_CVD_WINDOW', 20),
        notes='crypto policy',
    )


def _apply_behavior_overrides(pol: InstrumentPolicy) -> InstrumentPolicy:
    """Apply per-symbol behaviour profile without scattering ticker logic.

    Institutional execution is not one-size-fits-all: BTC has deep continuous
    liquidity and often trades best as a primary-liquidity objective plus runner;
    metals have cleaner but slower auction paths; xStock tokens are more
    spread-/venue-sensitive and should keep stricter target quality.
    """
    aid = str(pol.asset_id or "").upper()
    # BTC: deep liquidity, high regime velocity, frequent near-pool refuels.
    # Use staged liquidity objectives: TP can be a deeper runner if there is a
    # valid nearer pool to finance BE/trailing. This fixes the v76 BTC pathology
    # where 70 accepted sweeps died at a single full-position TP gate.
    if aid == "BTC":
        return replace(
            pol,
            min_rr=max(1.35, min(float(pol.min_rr), 1.65)),
            max_rr=max(float(pol.max_rr), 5.5),
            sl_buffer_atr_mult=max(float(pol.sl_buffer_atr_mult), 0.45),
            target_style="btc_staged_liquidity_runner",
            tp_durable_rr_floor=1.08,
            tp_be_move_mult=1.25,
            tp_terminal_profile_floor=0.68,
            tp_terminal_reach_floor=0.12,
            tp_primary_objective_min_rr=0.72,
            tp_path_support_lift_max=0.24,
            tp_posterior_blend_weight=0.64,
            shelf_sl_max_distance_atr=2.80,
            shelf_sl_min_significance=0.95,
            shelf_sl_min_delivery_atr=0.50,
            shelf_sl_min_entry_distance_atr=0.18,
            late_entry_max_chase_atr=1.85,
            late_entry_tp_compression_rr=0.32,
            notes=(pol.notes + "; BTC predictive staged-liquidity TP/SL profile").strip("; "),
        )

    # Metals/tokenised commodities: cleaner liquidity shelves but larger stop
    # sweep envelopes. Keep full-position targets stricter than BTC, allow only
    # mild path support from nearby objectives.
    if aid in {"GOLD", "SILVER"}:
        return replace(
            pol,
            min_rr=max(1.25, min(float(pol.min_rr), 1.65)),
            target_style="commodity_staged_displacement",
            tp_durable_rr_floor=0.98,
            tp_be_move_mult=1.25,
            tp_terminal_profile_floor=0.66,
            tp_terminal_reach_floor=0.12,
            tp_primary_objective_min_rr=0.30,
            tp_path_support_lift_max=0.22,
            tp_posterior_blend_weight=0.61,
            shelf_sl_max_distance_atr=3.80,
            shelf_sl_min_significance=0.85,
            shelf_sl_min_delivery_atr=0.45,
            shelf_sl_min_entry_distance_atr=0.16,
            late_entry_max_chase_atr=1.95,
            late_entry_tp_compression_rr=0.28,
            notes=(pol.notes + "; metal predictive staged-displacement TP/SL profile").strip("; "),
        )

    # High-beta xStocks / crypto-adjacent equities: allow some staged objective
    # support, but keep the spread/venue penalties materially stronger than BTC.
    if aid in {"NVDA", "TSLA", "COIN", "CRCL"}:
        return replace(
            pol,
            target_style="high_beta_xstock_staged_spread_aware",
            tp_durable_rr_floor=1.05,
            tp_be_move_mult=1.35,
            tp_terminal_profile_floor=0.62,
            tp_terminal_reach_floor=0.10,
            tp_primary_objective_min_rr=0.45,
            tp_path_support_lift_max=0.18,
            tp_posterior_blend_weight=0.56,
            shelf_sl_max_distance_atr=3.20,
            shelf_sl_min_significance=0.95,
            shelf_sl_min_delivery_atr=0.55,
            shelf_sl_min_entry_distance_atr=0.18,
            late_entry_max_chase_atr=1.75,
            late_entry_tp_compression_rr=0.34,
            notes=(pol.notes + "; high-beta xStock predictive staged TP/SL profile").strip("; "),
        )

    # Broad/large-cap xStocks: lower volatility, weaker 24/7 venue liquidity.
    # Prefer clean full-position objectives and only a small path support lift.
    if aid in {"SPY", "QQQ", "AAPL", "AMZN", "META", "GOOGL"}:
        return replace(
            pol,
            target_style="large_cap_xstock_staged_clean",
            tp_durable_rr_floor=1.12,
            tp_be_move_mult=1.45,
            tp_terminal_profile_floor=0.60,
            tp_terminal_reach_floor=0.08,
            tp_primary_objective_min_rr=0.55,
            tp_path_support_lift_max=0.14,
            tp_posterior_blend_weight=0.54,
            shelf_sl_max_distance_atr=2.90,
            shelf_sl_min_significance=1.00,
            shelf_sl_min_delivery_atr=0.60,
            shelf_sl_min_entry_distance_atr=0.20,
            late_entry_max_chase_atr=1.60,
            late_entry_tp_compression_rr=0.38,
            notes=(pol.notes + "; large-cap xStock predictive staged TP/SL profile").strip("; "),
        )
    return pol


def active_policy(inst: Optional[TradableInstrument] = None) -> InstrumentPolicy:
    return _apply_behavior_overrides(build_instrument_policy(inst if inst is not None else current_instrument()))


def policy_value(name: str, default: Any = None, inst: Optional[TradableInstrument] = None) -> Any:
    pol = active_policy(inst)
    return getattr(pol, name, default)

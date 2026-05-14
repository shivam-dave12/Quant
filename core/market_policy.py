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

from dataclasses import dataclass, asdict
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


def _desk_config_for(inst: Optional[TradableInstrument]) -> tuple[str, Dict[str, Any]]:
    desks = _cfg("TRADING_DESKS", {}) or {}
    asset_id = str(getattr(inst, "asset_id", "BTC") or "BTC").upper()
    ac = getattr(inst, "asset_class", AssetClass.CRYPTO) if inst is not None else AssetClass.CRYPTO
    ac_value = str(getattr(ac, "value", ac) or "crypto").lower()

    for desk_id, desk in desks.items():
        ids = {str(x).upper() for x in desk.get("asset_ids", ())}
        if asset_id in ids:
            return str(desk_id).upper(), dict(desk)
    for desk_id, desk in desks.items():
        classes = {str(x).lower() for x in desk.get("asset_classes", ())}
        if ac_value in classes:
            return str(desk_id).upper(), dict(desk)
    fallback = "BTC"
    return fallback, dict(desks.get(fallback, {}))


def _desk_section(desk: Dict[str, Any], section: str) -> Dict[str, Any]:
    val = desk.get(section, {})
    return val if isinstance(val, dict) else {}


def _desk_float(desk: Dict[str, Any], key: str, default: float, section: str = "") -> float:
    src = _desk_section(desk, section) if section else desk
    try:
        v = float(src.get(key, default))
        return v if math.isfinite(v) else float(default)
    except Exception:
        return float(default)


def _desk_int(desk: Dict[str, Any], key: str, default: int, section: str = "") -> int:
    src = _desk_section(desk, section) if section else desk
    try:
        return int(src.get(key, default))
    except Exception:
        return int(default)


def _desk_bool(desk: Dict[str, Any], key: str, default: bool, section: str = "") -> bool:
    src = _desk_section(desk, section) if section else desk
    raw = src.get(key, default)
    if isinstance(raw, str):
        return raw.strip().lower() in ("1", "true", "yes", "on", "enabled")
    return bool(raw)


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
    desk_id: str = "BTC"
    desk_name: str = "BTC Desk"
    strategy_key: str = "ICT_FVG_QUANT"
    entry_confirm_ticks: int = 2
    entry_fvg_proximity_atr: float = 0.8
    entry_require_ob_or_fvg: bool = False
    entry_min_pool_significance: float = 1.25
    entry_min_sweep_quality: float = 0.20
    entry_reversal_sl_buffer_atr: float = 0.35
    entry_continuation_sl_buffer_atr: float = 0.40
    exit_tp_min_rr_reversion: float = 1.8
    exit_tp_min_rr_trend: float = 2.5

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
    if not isinstance(ac, AssetClass):
        try:
            ac = AssetClass(str(ac).lower())
        except Exception:
            ac = AssetClass.CRYPTO
    asset_id = getattr(inst, 'asset_id', 'GLOBAL') if inst is not None else 'GLOBAL'

    lev = _cap_leverage(inst, _i('LEVERAGE', 30))
    base_margin = _f('QUANT_MARGIN_PCT', 0.20)
    base_rr = _f('MIN_RISK_REWARD_RATIO', 2.0)
    base_cooldown = _i('QUANT_COOLDOWN_SEC', 300)
    desk_id, desk = _desk_config_for(inst)
    desk_name = str(desk.get("display_name", f"{desk_id.title()} Desk"))
    strategy_key = str(desk.get("strategy", _cfg("STRATEGY_CORE_NAME", "ICT_FVG_QUANT")))
    desk_fields = {
        "desk_id": desk_id,
        "desk_name": desk_name,
        "strategy_key": strategy_key,
        "entry_confirm_ticks": _desk_int(desk, "confirm_ticks", _i('QUANT_CONFIRM_TICKS', 2), "entry"),
        "entry_fvg_proximity_atr": _desk_float(desk, "fvg_proximity_atr", _f('ICT_FVG_PROXIMITY_ATR', 0.8), "entry"),
        "entry_require_ob_or_fvg": _desk_bool(desk, "require_ob_or_fvg", _b('ICT_REQUIRE_OB_OR_FVG', False), "entry"),
        "entry_min_pool_significance": _desk_float(desk, "min_pool_significance", _f('ENTRY_MIN_POOL_SIGNIFICANCE', 1.25), "entry"),
        "entry_min_sweep_quality": _desk_float(desk, "min_sweep_quality", _f('ENTRY_MIN_SWEEP_QUALITY', 0.20), "entry"),
        "entry_reversal_sl_buffer_atr": _desk_float(desk, "reversal_sl_buffer_atr", 0.35, "entry"),
        "entry_continuation_sl_buffer_atr": _desk_float(desk, "continuation_sl_buffer_atr", 0.40, "entry"),
        "exit_tp_min_rr_reversion": _desk_float(desk, "tp_min_rr_reversion", _desk_float(desk, "min_rr", base_rr), "exit"),
        "exit_tp_min_rr_trend": _desk_float(desk, "tp_min_rr_trend", _desk_float(desk, "min_rr", base_rr), "exit"),
    }

    if ac == AssetClass.EQUITY:
        return InstrumentPolicy(
            asset_id=asset_id, asset_class=ac.value, leverage=lev,
            margin_pct=min(base_margin, _desk_float(desk, "margin_pct", _f('POLICY_EQUITY_MARGIN_PCT', 0.12))),
            risk_multiplier=_desk_float(desk, "risk_multiplier", _f('POLICY_EQUITY_RISK_MULT', 0.55)),
            min_margin_usd=_f('POLICY_EQUITY_MIN_MARGIN_USD', 0.5),
            tick_eval_sec=_desk_float(desk, "tick_eval_sec", _f('POLICY_EQUITY_TICK_EVAL_SEC', 0.75)),
            loop_interval_sec=_desk_float(desk, "loop_interval_sec", _f('POLICY_EQUITY_LOOP_INTERVAL_SEC', 0.75)),
            min_1m_bars=_desk_int(desk, "min_1m_bars", _i('POLICY_EQUITY_MIN_1M_BARS', 90)),
            min_5m_bars=_desk_int(desk, "min_5m_bars", _i('POLICY_EQUITY_MIN_5M_BARS', 70)),
            atr_min_pctile=_f('POLICY_EQUITY_ATR_MIN_PCTILE', 0.03),
            atr_max_pctile=_f('POLICY_EQUITY_ATR_MAX_PCTILE', 0.985),
            max_hold_sec=_desk_int(desk, "max_hold_sec", _i('POLICY_EQUITY_MAX_HOLD_SEC', 5400)),
            cooldown_sec=_desk_int(desk, "cooldown_sec", min(base_cooldown, _i('POLICY_EQUITY_COOLDOWN_SEC', 180))),
            loss_lockout_sec=_i('POLICY_EQUITY_LOSS_LOCKOUT_SEC', 1800),
            min_rr=_desk_float(desk, "min_rr", max(1.45, min(base_rr, _f('POLICY_EQUITY_MIN_RR', 1.75)))),
            max_rr=_desk_float(desk, "max_rr", _f('POLICY_EQUITY_MAX_RR', 5.0)),
            sl_buffer_atr_mult=_desk_float(desk, "sl_buffer_atr", _f('POLICY_EQUITY_SL_BUFFER_ATR', 0.55)),
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
            **desk_fields,
        )

    if ac == AssetClass.COMMODITY:
        return InstrumentPolicy(
            asset_id=asset_id, asset_class=ac.value, leverage=lev,
            margin_pct=min(base_margin, _desk_float(desk, "margin_pct", _f('POLICY_COMMODITY_MARGIN_PCT', 0.14))),
            risk_multiplier=_desk_float(desk, "risk_multiplier", _f('POLICY_COMMODITY_RISK_MULT', 0.70)),
            min_margin_usd=_f('POLICY_COMMODITY_MIN_MARGIN_USD', 0.5),
            tick_eval_sec=_desk_float(desk, "tick_eval_sec", _f('POLICY_COMMODITY_TICK_EVAL_SEC', 0.50)),
            loop_interval_sec=_desk_float(desk, "loop_interval_sec", _f('POLICY_COMMODITY_LOOP_INTERVAL_SEC', 0.50)),
            min_1m_bars=_desk_int(desk, "min_1m_bars", _i('POLICY_COMMODITY_MIN_1M_BARS', 85)),
            min_5m_bars=_desk_int(desk, "min_5m_bars", _i('POLICY_COMMODITY_MIN_5M_BARS', 65)),
            atr_min_pctile=_f('POLICY_COMMODITY_ATR_MIN_PCTILE', 0.04),
            atr_max_pctile=_f('POLICY_COMMODITY_ATR_MAX_PCTILE', 0.985),
            max_hold_sec=_desk_int(desk, "max_hold_sec", _i('POLICY_COMMODITY_MAX_HOLD_SEC', 4800)),
            cooldown_sec=_desk_int(desk, "cooldown_sec", min(base_cooldown, _i('POLICY_COMMODITY_COOLDOWN_SEC', 210))),
            loss_lockout_sec=_i('POLICY_COMMODITY_LOSS_LOCKOUT_SEC', 1800),
            min_rr=_desk_float(desk, "min_rr", max(1.55, min(base_rr, _f('POLICY_COMMODITY_MIN_RR', 1.85)))),
            max_rr=_desk_float(desk, "max_rr", _f('POLICY_COMMODITY_MAX_RR', 5.0)),
            sl_buffer_atr_mult=_desk_float(desk, "sl_buffer_atr", _f('POLICY_COMMODITY_SL_BUFFER_ATR', 0.50)),
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
            **desk_fields,
        )

    # Crypto/default policy keeps the original aggressive BTC runtime prior.
    return InstrumentPolicy(
        asset_id=asset_id, asset_class=getattr(ac, 'value', str(ac)), leverage=lev,
        margin_pct=_desk_float(desk, "margin_pct", base_margin),
        risk_multiplier=_desk_float(desk, "risk_multiplier", _f('POLICY_CRYPTO_RISK_MULT', 1.0)),
        min_margin_usd=_f('MIN_MARGIN_PER_TRADE', 1.0),
        tick_eval_sec=_desk_float(desk, "tick_eval_sec", _f('ENTRY_EVALUATION_INTERVAL_SECONDS', 1.0)),
        loop_interval_sec=_desk_float(desk, "loop_interval_sec", _f('POLICY_CRYPTO_LOOP_INTERVAL_SEC', float(_cfg('SCANNER_TICK_SLEEP_SEC', 0.25)))),
        min_1m_bars=_desk_int(desk, "min_1m_bars", _i('MIN_CANDLES_1M', 80)),
        min_5m_bars=_desk_int(desk, "min_5m_bars", _i('MIN_CANDLES_5M', 60)),
        atr_min_pctile=_f('QUANT_ATR_MIN_PCTILE', 0.05),
        atr_max_pctile=_f('QUANT_ATR_MAX_PCTILE', 0.97),
        max_hold_sec=_desk_int(desk, "max_hold_sec", _i('QUANT_MAX_HOLD_SEC', 3600)),
        cooldown_sec=_desk_int(desk, "cooldown_sec", base_cooldown),
        loss_lockout_sec=_i('QUANT_LOSS_LOCKOUT_SEC', 1800),
        min_rr=_desk_float(desk, "min_rr", base_rr),
        max_rr=_desk_float(desk, "max_rr", _f('QUANT_TP_MAX_RR', 3.5)),
        sl_buffer_atr_mult=_desk_float(desk, "sl_buffer_atr", _f('QUANT_SL_BUFFER_ATR_MULT', 0.4)),
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
        **desk_fields,
    )


def active_policy(inst: Optional[TradableInstrument] = None) -> InstrumentPolicy:
    return build_instrument_policy(inst if inst is not None else current_instrument())


def policy_value(name: str, default: Any = None, inst: Optional[TradableInstrument] = None) -> Any:
    pol = active_policy(inst)
    return getattr(pol, name, default)

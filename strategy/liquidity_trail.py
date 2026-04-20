"""
liquidity_trail.py — Redirected to sl_tp_engine.StructuralTrailEngine
=======================================================================
This module previously contained the Fibonacci SL trailing engine (v5.0).
That engine has been superseded by StructuralTrailEngine in sl_tp_engine.py,
which uses pool-to-pool / OB / swing structural trailing instead of Fibonacci.

All public symbols are re-exported from sl_tp_engine for backward compatibility.
Any code importing from this module continues to work without changes.
"""
from __future__ import annotations
from typing import List, Optional

# ── Primary re-exports ────────────────────────────────────────────────────────
try:
    from strategy.sl_tp_engine import (
        StructuralTrailEngine   as LiquidityTrailEngine,
        TrailResult,
        SLResult,
        TPResult,
    )
except ImportError:
    from sl_tp_engine import (
        StructuralTrailEngine   as LiquidityTrailEngine,
        TrailResult,
        SLResult,
        TPResult,
    )


# ── LiquidityTrailResult — backward-compat wrapper ───────────────────────────
# Callers that construct or destructure LiquidityTrailResult objects by field
# name continue to work. TrailResult fields map 1-to-1 except:
#   TrailResult.anchor_price / .anchor_label  → exposed via .anchor shim
#   TrailResult.blocked  → exposed as .trail_blocked
class LiquidityTrailResult:
    """
    Thin compatibility wrapper over TrailResult.

    Build from a raw TrailResult (returned by StructuralTrailEngine.compute())
    or construct directly with keyword arguments for test compatibility.
    """
    __slots__ = (
        'new_sl', 'reason', 'phase', 'r_multiple',
        'trail_blocked', 'block_reason', 'anchor',
        # Extra fields some callers read
        'swing_low', 'swing_high', 'momentum_gate', 'htf_aligned',
    )

    def __init__(self, raw=None, **kwargs):
        if raw is not None:
            self.new_sl        = raw.new_sl
            self.reason        = raw.reason
            self.phase         = raw.phase
            self.r_multiple    = raw.r_multiple
            self.trail_blocked = getattr(raw, 'blocked', False)
            self.block_reason  = ""
            self.swing_low     = None
            self.swing_high    = None
            self.momentum_gate = ""
            self.htf_aligned   = None

            class _AnchorShim:
                price     = raw.anchor_price or 0.0
                sig       = 0.0
                quality   = 0.0
                timeframe = ""
            self.anchor = _AnchorShim()
        else:
            # Direct construction from kwargs (tests / legacy callers)
            self.new_sl        = kwargs.get('new_sl')
            self.reason        = kwargs.get('reason', '')
            self.phase         = kwargs.get('phase', 'HOLD')
            self.r_multiple    = kwargs.get('r_multiple', 0.0)
            self.trail_blocked = kwargs.get('trail_blocked', False)
            self.block_reason  = kwargs.get('block_reason', '')
            self.swing_low     = kwargs.get('swing_low')
            self.swing_high    = kwargs.get('swing_high')
            self.momentum_gate = kwargs.get('momentum_gate', '')
            self.htf_aligned   = kwargs.get('htf_aligned')

            _ap = kwargs.get('anchor_price')
            class _AnchorShim:
                price     = _ap or 0.0
                sig       = 0.0
                quality   = 0.0
                timeframe = ""
            self.anchor = _AnchorShim()


# ── PoolAnchor — backward-compat stub ────────────────────────────────────────
# Some callers import PoolAnchor from liquidity_trail for type hints.
class PoolAnchor:
    """Backward-compat stub. New code uses TrailResult.anchor_price/label."""
    __slots__ = ('price','side','timeframe','sig','buffer_atr','is_swept',
                 'distance_atr','quality','fib_ratio','swing_low','swing_high',
                 'pool_boost','is_cluster','n_cluster_tfs','pool_between_expand')

    def __init__(self, price=0.0, side='', timeframe='', sig=0.0,
                 buffer_atr=0.0, is_swept=False, distance_atr=0.0,
                 quality=0.0, fib_ratio=None, swing_low=None,
                 swing_high=None, pool_boost=False, is_cluster=False,
                 n_cluster_tfs=1, pool_between_expand=False):
        self.price             = price
        self.side              = side
        self.timeframe         = timeframe
        self.sig               = sig
        self.buffer_atr        = buffer_atr
        self.is_swept          = is_swept
        self.distance_atr      = distance_atr
        self.quality           = quality
        self.fib_ratio         = fib_ratio
        self.swing_low         = swing_low
        self.swing_high        = swing_high
        self.pool_boost        = pool_boost
        self.is_cluster        = is_cluster
        self.n_cluster_tfs     = n_cluster_tfs
        self.pool_between_expand = pool_between_expand


# ── Module-level phase constants ─────────────────────────────────────────────
# Kept for any code that reads them directly from this module.
PHASE_0_MAX_R        = 0.50
PHASE_1_MAX_R        = 1.00
PHASE_2_MAX_R        = 2.50
PHASE_GATE_DENOM_ATR = 1.5

__all__ = [
    'LiquidityTrailEngine',
    'LiquidityTrailResult',
    'PoolAnchor',
    'PHASE_0_MAX_R',
    'PHASE_1_MAX_R',
    'PHASE_2_MAX_R',
    'PHASE_GATE_DENOM_ATR',
]

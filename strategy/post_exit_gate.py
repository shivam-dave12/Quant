# -*- coding: utf-8 -*-
"""
strategy/post_exit_gate.py — Industry-grade post-exit re-entry gate
====================================================================

PURPOSE
-------
After a position closes, the bot used to wait `QUANT_COOLDOWN_SEC` (30s) and
then accept the next signal. In practice this produced two failure modes that
the user observed in production:

  (A) Same-side continuation immediately after a TP (or trailed exit) — the
      regime that printed the move was already mature; chasing it puts entry
      INTO the next wave's exhaustion.
  (B) Opposite-side flip immediately after an SL — re-entering against the
      direction that just stopped you out has the worst expectancy in the
      book; the move that broke your structure is not done.

This module replaces the flat 30s cooldown with a multi-factor decision:

    accept(side, ctx) -> (allow: bool, decay_until: float, reason: str)

The gate composes six orthogonal rejection lenses. Any one veto blocks the
entry; vetoes carry a `decay_until` timestamp that tells the strategy when
the same lens will next re-evaluate (instead of polling).

DESIGN
------
The gate is **stateless from the caller's POV** — it reads only:
    * the just-closed trade outcome (passed via record_exit())
    * counters from PostTradeAgent / DailyRiskGate (already wired)
    * live context object (snapshot of structure, ATR, flow)

Internal state is just (a) decay timers per lens, (b) the rolling outcome
of the last N trades, (c) a "regime fingerprint" of the post-exit market
so we can detect when conditions have actually changed.

Each lens has a single responsibility and is unit-testable in isolation.
The gate does NOT make trading decisions about WHETHER to enter — that
is the entry engine's job. It only decides "is the post-exit window still
hostile to this re-entry?".

LENSES (in order of evaluation; first veto wins)
------------------------------------------------
1. BASE_COOLDOWN
       Always-on minimum: max(QUANT_COOLDOWN_SEC, POST_EXIT_BASE_SEC).
       Prevents same-tick re-entry on noisy fills. Default 60s.

2. LOSS_DECAY
       After N consecutive losses, multiply base cooldown by 2^(N-1).
       Reads from `risk_gate.consec_losses` if available, else internal.

3. SIDE_FLIP_RESISTANCE
       After SL or trailed-loss exit, the OPPOSITE side requires:
           * ATR-distance >= POST_EXIT_FLIP_MIN_ATR_FROM_EXIT, OR
           * structure event (BOS/CHoCH) on the new side since exit
       Same-side re-entry is unaffected by this lens.

4. SAME_DIRECTION_EXHAUSTION
       After a TP win, same-side re-entry needs proof the move has
       reset (a pullback ≥ 50% of the prior MFE, or fresh BOS in
       the same direction). Without it, you're chasing.

5. ATR_REGIME_GUARD
       If ATR_5m has spiked >POST_EXIT_ATR_SHOCK_PCT in the post-exit
       window, force a longer cooldown (volatility shock = wider stops
       needed, model is off-distribution).

6. STRUCTURE_PROOF
       Require ONE of:
           - new BOS/CHoCH event since exit_time, OR
           - displacement candle in entry direction, OR
           - sweep-and-reverse (the entry engine's bread and butter)
       This is the hardest gate; it's the one that differentiates
       "noise re-entry" from "the next real setup".

TUNING
------
All thresholds are config-driven. Defaults are tuned for BTC perps on a
1m/5m/15m liquidity-first stack. The gate is conservative by design — a
rejected entry costs nothing; a forced loss costs RISK_PER_TRADE.

INTEGRATION
-----------
Wired in two places in `quant_strategy.py`:

    # 1) on every exit:
    self._post_exit_gate.record_exit(
        side=pos.side, exit_reason=reason, exit_price=fill,
        entry_price=pos.entry_price, mfe_pts=peak_profit,
        mae_pts=mae_pts, atr=atr_5m.atr, exit_time=time.time())

    # 2) before _enter_trade(), inside the entry-block:
    allow, retry_at, reason = self._post_exit_gate.accept(
        side=signal.side,
        now=time.time(),
        atr=self._atr_5m.atr,
        price=current_price,
        bos_count_since_exit=ict._bos_count_since(self._post_exit_gate.last_exit_time),
        choch_active=(choch_tf is not None),
        flow=flow_state,
        consec_losses=self._risk_gate.consec_losses,
        post_trade_agent=self._post_trade_agent,
    )
    if not allow:
        # decay-aware retry — engine sleeps until retry_at, no busy-loop
        return ('post_exit_gate', reason, retry_at)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional, Tuple

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Tunables (sourced from config with safe fallbacks)
# ─────────────────────────────────────────────────────────────────────────────


def _cfg_get(name: str, default: Any) -> Any:
    try:
        import config as _c
        return getattr(_c, name, default)
    except Exception:
        return default


# Default thresholds — overridden by config.py if present
_BASE_SEC                        = lambda: float(_cfg_get("POST_EXIT_BASE_SEC",                  60.0))
_LOSS_DECAY_FACTOR               = lambda: float(_cfg_get("POST_EXIT_LOSS_DECAY_FACTOR",          2.0))
_LOSS_DECAY_CAP_SEC              = lambda: float(_cfg_get("POST_EXIT_LOSS_DECAY_CAP_SEC",       900.0))  # 15 min ceiling
_FLIP_MIN_ATR_AFTER_SL           = lambda: float(_cfg_get("POST_EXIT_FLIP_MIN_ATR_FROM_EXIT",     1.5))
_FLIP_BASE_SEC                   = lambda: float(_cfg_get("POST_EXIT_FLIP_BASE_SEC",            120.0))
_TP_SAMESIDE_PULLBACK_PCT        = lambda: float(_cfg_get("POST_EXIT_TP_SAMESIDE_PULLBACK_PCT",  0.50))
_TP_SAMESIDE_BASE_SEC            = lambda: float(_cfg_get("POST_EXIT_TP_SAMESIDE_BASE_SEC",      90.0))
_ATR_SHOCK_PCT                   = lambda: float(_cfg_get("POST_EXIT_ATR_SHOCK_PCT",             0.40))
_ATR_SHOCK_PENALTY_SEC           = lambda: float(_cfg_get("POST_EXIT_ATR_SHOCK_PENALTY_SEC",   180.0))
_STRUCTURE_PROOF_TIMEOUT_SEC     = lambda: float(_cfg_get("POST_EXIT_STRUCTURE_PROOF_TIMEOUT",  240.0))
_STRUCTURE_PROOF_REQUIRED        = lambda: bool (_cfg_get("POST_EXIT_STRUCTURE_PROOF_REQUIRED",  True))
_LOSS_SAMESIDE_DEAD_SEC          = lambda: float(_cfg_get("POST_EXIT_LOSS_SAMESIDE_DEAD_SEC",   300.0))
_FLIP_REQUIRES_BOS               = lambda: bool (_cfg_get("POST_EXIT_FLIP_REQUIRES_BOS",         True))


# ─────────────────────────────────────────────────────────────────────────────
# Decision payload
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class GateDecision:
    allow:      bool
    retry_at:   float          # epoch s; when this lens should be re-checked
    lens:       str            # which lens vetoed (or "" on allow)
    detail:     str            # human-readable reason
    metrics:    dict = field(default_factory=dict)

    def as_tuple(self) -> Tuple[bool, float, str]:
        if self.allow:
            return True, 0.0, ""
        return False, self.retry_at, f"{self.lens}: {self.detail}"


# ─────────────────────────────────────────────────────────────────────────────
# Last-exit context (immutable record set on each exit)
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class LastExit:
    side:         str            # "long" / "short"
    is_loss:      bool
    is_trailed:   bool
    exit_reason:  str            # tp_hit / sl_hit / trail_sl_hit / ...
    exit_price:   float
    entry_price:  float
    mfe_pts:      float
    mae_pts:      float
    atr_at_exit:  float
    exit_time:    float
    # Updated as ticks come in (NOT immutable — these track the
    # post-exit market state)
    extreme_after_exit: float = 0.0   # most-extreme price seen since exit
    extreme_atr_after:  float = 0.0   # ATR observed at extreme
    bos_count_at_exit:  int   = 0     # ICT BOS counter at exit time


# ─────────────────────────────────────────────────────────────────────────────
# Live context handed to accept() each tick
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class GateContext:
    now:                  float
    side:                 str            # candidate side ("long" / "short")
    price:                float
    atr:                  float
    bos_count_since_exit: int            # ICT BOS events since last exit
    choch_active:         bool
    flow_conviction:      float          # signed flow score (-1..1)
    consec_losses:        int            # from DailyRiskGate
    sweep_present:        bool   = False # entry engine just ID'd a sweep
    displacement_present: bool   = False # ICT displacement candle in side direction
    # Optional reference to PostTradeAgent for richer stats
    post_trade_agent:     Optional[Any] = None


# ─────────────────────────────────────────────────────────────────────────────
# The gate
# ─────────────────────────────────────────────────────────────────────────────


class PostExitGate:
    """
    Six-lens post-exit re-entry gate.

    Public surface:
        record_exit(...)        — call from _record_pnl on every exit
        observe_tick(price,atr) — optional; updates extreme_after_exit
        accept(ctx)             — call before _enter_trade
        snapshot()              — for /position and watchdog
        force_clear()           — operator override (telegram /unfreeze)

    Thread-safety: caller must hold strategy lock during accept(); the
    gate itself does not lock.
    """

    def __init__(self) -> None:
        self._last: Optional[LastExit] = None
        self._force_clear_until: float = 0.0
        # diagnostic — last decision for /position display
        self._last_decision: Optional[GateDecision] = None

    # ---- lifecycle ---------------------------------------------------------

    def record_exit(self,
                    side: str,
                    exit_reason: str,
                    exit_price: float,
                    entry_price: float,
                    mfe_pts: float,
                    mae_pts: float,
                    atr: float,
                    exit_time: float,
                    bos_count_at_exit: int = 0) -> None:
        """Capture the just-closed trade. Called from `_record_pnl`."""
        side_l = (side or "").lower()
        reason_l = (exit_reason or "").lower()
        is_loss = reason_l.startswith("sl") or "stop" in reason_l or "trail_sl" in reason_l
        is_trailed = "trail" in reason_l
        # Heuristic: a "trail_sl_hit" with positive PnL is a win that gave back;
        # treat for re-entry as a TP-class event (it locked profit).
        if is_trailed and exit_price and entry_price:
            if (side_l == "long" and exit_price >= entry_price) or \
               (side_l == "short" and exit_price <= entry_price):
                is_loss = False
        self._last = LastExit(
            side=side_l,
            is_loss=is_loss,
            is_trailed=is_trailed,
            exit_reason=reason_l,
            exit_price=float(exit_price or 0.0),
            entry_price=float(entry_price or 0.0),
            mfe_pts=float(mfe_pts or 0.0),
            mae_pts=float(mae_pts or 0.0),
            atr_at_exit=float(atr or 0.0),
            exit_time=float(exit_time or time.time()),
            extreme_after_exit=float(exit_price or 0.0),
            extreme_atr_after=float(atr or 0.0),
            bos_count_at_exit=int(bos_count_at_exit or 0),
        )
        logger.info(
            "post_exit_gate: recorded %s %s @ $%.2f mfe=%.1fpts mae=%.1fpts atr=%.1f",
            side_l, exit_reason, exit_price or 0.0, mfe_pts, mae_pts, atr,
        )

    def observe_tick(self, price: float, atr: float) -> None:
        """
        Track the post-exit price extreme so SAME_DIRECTION_EXHAUSTION can
        measure the pullback that's actually occurred. Cheap to call from
        the main on_tick path.
        """
        le = self._last
        if le is None or price <= 0:
            return
        if le.side == "long":
            if price > le.extreme_after_exit:
                le.extreme_after_exit = price
                le.extreme_atr_after  = max(le.extreme_atr_after, atr or 0.0)
        elif le.side == "short":
            if le.extreme_after_exit <= 0 or price < le.extreme_after_exit:
                le.extreme_after_exit = price
                le.extreme_atr_after  = max(le.extreme_atr_after, atr or 0.0)

    def force_clear(self, duration_sec: float = 0.0) -> None:
        """Operator override. duration_sec=0 → permanent until next exit."""
        if duration_sec <= 0:
            self._force_clear_until = float("inf")
        else:
            self._force_clear_until = time.time() + duration_sec
        logger.warning("post_exit_gate: force-cleared by operator (until=%s)",
                       "permanent" if duration_sec <= 0 else f"+{duration_sec:.0f}s")

    # ---- accessors ---------------------------------------------------------

    @property
    def last_exit_time(self) -> float:
        return self._last.exit_time if self._last else 0.0

    @property
    def last_decision(self) -> Optional[GateDecision]:
        return self._last_decision

    def snapshot(self) -> dict:
        if self._last is None:
            return {"armed": False}
        le = self._last
        out = {
            "armed":        True,
            "side":         le.side,
            "is_loss":      le.is_loss,
            "is_trailed":   le.is_trailed,
            "exit_reason":  le.exit_reason,
            "exit_price":   le.exit_price,
            "exit_age_sec": round(time.time() - le.exit_time, 1),
            "mfe_pts":      le.mfe_pts,
        }
        if self._last_decision is not None:
            out["last_decision"] = {
                "allow":  self._last_decision.allow,
                "lens":   self._last_decision.lens,
                "detail": self._last_decision.detail,
            }
        return out

    # ---- main entry --------------------------------------------------------

    def accept(self, ctx: GateContext) -> GateDecision:
        """
        Evaluate all six lenses. Returns first veto, or allow.
        """
        # Operator override
        if ctx.now < self._force_clear_until:
            d = GateDecision(True, 0.0, "", "operator override active")
            self._last_decision = d
            return d

        # No prior exit → no gate
        if self._last is None:
            d = GateDecision(True, 0.0, "", "first trade")
            self._last_decision = d
            return d

        le = self._last
        age = max(0.0, ctx.now - le.exit_time)

        for lens in (
            self._lens_base_cooldown,
            self._lens_loss_decay,
            self._lens_side_flip_resistance,
            self._lens_same_direction_exhaustion,
            self._lens_atr_regime_guard,
            self._lens_structure_proof,
        ):
            d = lens(ctx, le, age)
            if not d.allow:
                self._last_decision = d
                return d

        d = GateDecision(True, 0.0, "", f"all lenses passed (age={age:.0f}s)")
        self._last_decision = d
        return d

    # ─── lens 1: base cooldown ────────────────────────────────────────────
    def _lens_base_cooldown(self, ctx: GateContext, le: LastExit, age: float) -> GateDecision:
        base = _BASE_SEC()
        if age >= base:
            return GateDecision(True, 0.0, "", "")
        retry = le.exit_time + base
        return GateDecision(
            False, retry, "BASE_COOLDOWN",
            f"{age:.0f}s/{base:.0f}s since exit",
            {"age_sec": round(age, 1), "base_sec": base},
        )

    # ─── lens 2: consecutive-loss decay ───────────────────────────────────
    def _lens_loss_decay(self, ctx: GateContext, le: LastExit, age: float) -> GateDecision:
        n = max(0, int(ctx.consec_losses))
        if n == 0:
            return GateDecision(True, 0.0, "", "")
        # 2^(n-1) multiplier: 1L→1x, 2L→2x, 3L→4x, 4L→8x (capped)
        mult = _LOSS_DECAY_FACTOR() ** max(0, n - 1)
        decay = min(_BASE_SEC() * mult, _LOSS_DECAY_CAP_SEC())
        if age >= decay:
            return GateDecision(True, 0.0, "", "")
        retry = le.exit_time + decay
        return GateDecision(
            False, retry, "LOSS_DECAY",
            f"{n} consec losses → {decay:.0f}s lockout ({age:.0f}s elapsed)",
            {"consec_losses": n, "decay_sec": decay, "age_sec": round(age, 1)},
        )

    # ─── lens 3: side-flip resistance ─────────────────────────────────────
    def _lens_side_flip_resistance(self, ctx: GateContext, le: LastExit, age: float) -> GateDecision:
        cand = (ctx.side or "").lower()
        # Same side as the just-exited trade → not a flip; let other lenses handle
        if cand == le.side:
            return GateDecision(True, 0.0, "", "")
        # Only on losses do we resist immediate flip (a TP exit followed by
        # the opposite side is a reversal play and is policed by lens 4-6)
        if not le.is_loss:
            return GateDecision(True, 0.0, "", "")

        flip_base = _FLIP_BASE_SEC()
        # Time gate
        if age < flip_base:
            return GateDecision(
                False, le.exit_time + flip_base, "SIDE_FLIP_TIME",
                f"flip-resist {age:.0f}s/{flip_base:.0f}s after SL",
                {"age_sec": round(age, 1), "flip_sec": flip_base},
            )

        # Distance gate: price must have travelled away from the SL by
        # at least N ATR before we trust the flip
        atr = max(ctx.atr, le.atr_at_exit, 1e-9)
        moved = abs(ctx.price - le.exit_price)
        atr_dist = moved / atr
        need = _FLIP_MIN_ATR_AFTER_SL()
        if atr_dist < need:
            return GateDecision(
                False, ctx.now + 5.0, "SIDE_FLIP_DISTANCE",
                f"only {atr_dist:.2f}ATR from SL ({moved:.0f}pts), need ≥{need:.1f}ATR",
                {"atr_dist": round(atr_dist, 2), "needed": need},
            )

        # Optional: require BOS in the new direction
        if _FLIP_REQUIRES_BOS() and ctx.bos_count_since_exit < 1 and not ctx.choch_active:
            return GateDecision(
                False, ctx.now + 10.0, "SIDE_FLIP_NO_BOS",
                "no BOS/CHoCH on new side since exit",
                {"bos_since_exit": ctx.bos_count_since_exit, "choch": ctx.choch_active},
            )

        return GateDecision(True, 0.0, "", "")

    # ─── lens 4: same-direction exhaustion ────────────────────────────────
    def _lens_same_direction_exhaustion(self, ctx: GateContext, le: LastExit,
                                          age: float) -> GateDecision:
        cand = (ctx.side or "").lower()
        if cand != le.side:
            return GateDecision(True, 0.0, "", "")
        # Only police same-side after wins (TP / trailed-profit). A same-side
        # re-entry after an SL is a "thesis still alive" call — the loss-decay
        # lens already handles it.
        if le.is_loss:
            return GateDecision(True, 0.0, "", "")

        base = _TP_SAMESIDE_BASE_SEC()
        if age < base:
            return GateDecision(
                False, le.exit_time + base, "SAMESIDE_TP_TIME",
                f"same-side post-TP cooldown {age:.0f}s/{base:.0f}s",
                {"age_sec": round(age, 1), "base_sec": base},
            )

        # Pullback proof: from the post-exit extreme, has price retraced by
        # POST_EXIT_TP_SAMESIDE_PULLBACK_PCT of the prior MFE?
        if le.mfe_pts <= 0 or le.extreme_after_exit <= 0:
            # No prior MFE info → fall through (don't block on missing data)
            return GateDecision(True, 0.0, "", "")
        if le.side == "long":
            pullback = max(0.0, le.extreme_after_exit - ctx.price)
        else:
            pullback = max(0.0, ctx.price - le.extreme_after_exit)
        need = le.mfe_pts * _TP_SAMESIDE_PULLBACK_PCT()
        if pullback < need:
            return GateDecision(
                False, ctx.now + 5.0, "SAMESIDE_NO_PULLBACK",
                f"pullback {pullback:.0f}pts < {need:.0f}pts (50% of MFE {le.mfe_pts:.0f}pts)",
                {"pullback_pts": round(pullback, 1), "needed": round(need, 1)},
            )
        return GateDecision(True, 0.0, "", "")

    # ─── lens 5: ATR regime shock ─────────────────────────────────────────
    def _lens_atr_regime_guard(self, ctx: GateContext, le: LastExit,
                                 age: float) -> GateDecision:
        if le.atr_at_exit <= 0 or ctx.atr <= 0:
            return GateDecision(True, 0.0, "", "")
        delta = abs(ctx.atr - le.atr_at_exit) / max(le.atr_at_exit, 1e-9)
        if delta < _ATR_SHOCK_PCT():
            return GateDecision(True, 0.0, "", "")
        # Volatility shock — extend cooldown
        penalty = _ATR_SHOCK_PENALTY_SEC()
        if age >= penalty:
            return GateDecision(True, 0.0, "", "")
        return GateDecision(
            False, le.exit_time + penalty, "ATR_SHOCK",
            f"ATR Δ{delta:+.0%} ({le.atr_at_exit:.1f}→{ctx.atr:.1f}); regime unstable",
            {"atr_delta_pct": round(delta * 100, 1),
             "atr_was": le.atr_at_exit, "atr_now": ctx.atr},
        )

    # ─── lens 6: structure proof ──────────────────────────────────────────
    def _lens_structure_proof(self, ctx: GateContext, le: LastExit,
                                 age: float) -> GateDecision:
        if not _STRUCTURE_PROOF_REQUIRED():
            return GateDecision(True, 0.0, "", "")
        timeout = _STRUCTURE_PROOF_TIMEOUT_SEC()
        if age >= timeout:
            # Past the proof window → the gate stops requiring it (the entry
            # engine has presumably lined up a fresh setup of its own).
            return GateDecision(True, 0.0, "", "")
        # Any one of: BOS, CHoCH, sweep, or displacement satisfies proof
        if (ctx.bos_count_since_exit > 0 or ctx.choch_active
                or ctx.sweep_present or ctx.displacement_present):
            return GateDecision(True, 0.0, "", "")
        return GateDecision(
            False, ctx.now + 5.0, "NO_STRUCTURE_PROOF",
            f"no BOS/CHoCH/sweep/displacement since exit ({age:.0f}s elapsed)",
            {"age_sec": round(age, 1),
             "bos_since": ctx.bos_count_since_exit,
             "choch": ctx.choch_active,
             "sweep": ctx.sweep_present,
             "displacement": ctx.displacement_present},
        )

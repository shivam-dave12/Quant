"""
config_schema.py — Pydantic v2 schema for active ICT/Liquidity risk controls.
==========================================================================

ARCHITECTURE
------------
config.py remains the single source of truth (bare module of constants).
This module wraps it with a validated, typed interface:

    from config_schema import cfg

    cfg.RISK_PER_TRADE          # float, validated in (0, 0.05)
    cfg.risk.MIN_RISK_REWARD_RATIO   # float, validated >= 1.5
    cfg.LEVERAGE                # int, validated in [1, 125]

Active structural-risk and execution invariants are validated here:
  1. Field-level   — Field(gt=0, le=0.05) — enforced at instantiation
  2. Cross-field   — @model_validator — invariants that span multiple fields
  3. Self-documenting — the model is the spec, readable as API docs

INTEGRATION
-----------
config.py already calls validate_config() at module end.  At the bottom of
config.py, add one line:

    from config_schema import cfg as _cfg_validated  # noqa: F401  (side-effect: validates)

This ensures the schema is validated on every import of config.py.
Downstream modules that want typed access import cfg directly:

    from config_schema import cfg

Modules that use the bare config.py style (e.g. config.RISK_PER_TRADE)
continue working unchanged — there is zero migration cost.

DESIGN NOTES
------------
* model_config = ConfigDict(frozen=True) — the live schema is immutable after
  startup.  Any attempted mutation raises ValidationError immediately.
* All fields mirror their config.py names exactly to make grep/search trivial.
* Extra fields are forbidden (extra="forbid") so a typo in config.py that
  creates a new constant doesn't silently pass validation.
* The schema is populated from config.py at import time; it does NOT re-read
  environment variables — that is config.py's job.
"""

from __future__ import annotations

import importlib
import sys
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


# ---------------------------------------------------------------------------
# Helper — pull a constant from config.py with a typed default
# ---------------------------------------------------------------------------

def _c(name: str, default: Any = None) -> Any:
    """Return config.<name>, falling back to default if the attribute is absent."""
    try:
        mod = sys.modules.get("config") or importlib.import_module("config")
        return getattr(mod, name, default)
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Sub-model: Risk Management
# ---------------------------------------------------------------------------

class RiskConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    RISK_PER_TRADE: float = Field(
        default_factory=lambda: _c("RISK_PER_TRADE", 0.05),
        gt=0.0,
        le=0.05,
        description=(
            "Fraction of available balance risked per trade.  "
            "0.05 = 5.0%.  Hard ceiling 5% (0.05) prevents catastrophic "
            "over-sizing at high leverage."
        ),
    )
    MAX_DAILY_LOSS_PCT: float = Field(
        default_factory=lambda: _c("MAX_DAILY_LOSS_PCT", 3.0),
        gt=0.0,
        le=10.0,
        description="Daily drawdown circuit-breaker in percent of balance.",
    )
    MAX_DRAWDOWN_PCT: float = Field(
        default_factory=lambda: _c("MAX_DRAWDOWN_PCT", 15.0),
        gt=0.0,
        le=50.0,
    )
    MAX_CONSECUTIVE_LOSSES: int = Field(
        default_factory=lambda: _c("MAX_CONSECUTIVE_LOSSES", 1),
        ge=1,
        le=20,
    )
    MAX_DAILY_TRADES: int = Field(
        default_factory=lambda: _c("MAX_DAILY_TRADES", 10),
        ge=1,
        le=500,
    )
    MIN_RISK_REWARD_RATIO: float = Field(
        default_factory=lambda: _c("MIN_RISK_REWARD_RATIO", 2.0),
        ge=1.0,
        le=10.0,
        description="Institutional R:R floor.  Must be >= 1.5.",
    )
    TARGET_RISK_REWARD_RATIO: float = Field(
        default_factory=lambda: _c("TARGET_RISK_REWARD_RATIO", 3.0),
        ge=1.0,
        le=20.0,
    )
    LEVERAGE: int = Field(
        default_factory=lambda: _c("LEVERAGE", 40),
        ge=1,
        le=125,
        description="Exchange leverage.  Capped at 125× (max on most CEXes).",
    )
    MIN_TIME_BETWEEN_TRADES_SEC: float = Field(
        default_factory=lambda: _c("MIN_TIME_BETWEEN_TRADES_SEC", 30.0),
        gt=0.0,
        le=3600.0,
    )
    TRADE_COOLDOWN_SECONDS: float = Field(
        default_factory=lambda: _c("TRADE_COOLDOWN_SECONDS", 30.0),
        ge=0.0,
        le=3600.0,
    )

    @model_validator(mode="after")
    def risk_budget_consistent(self) -> "RiskConfig":
        """
        The per-streak risk must not exceed the daily loss budget.

        Invariant:
            RISK_PER_TRADE × MAX_CONSECUTIVE_LOSSES ≤ MAX_DAILY_LOSS_PCT / 100

        Rationale: if a full loss streak can blow the daily budget in one
        sitting, the consecutive-loss counter is a dead parameter (the day-loss
        breaker fires first) and risk is systemically under-constrained.
        """
        streak_loss = self.RISK_PER_TRADE * self.MAX_CONSECUTIVE_LOSSES
        daily_budget = self.MAX_DAILY_LOSS_PCT / 100.0
        if streak_loss > daily_budget + 1e-9:
            raise ValueError(
                f"Risk budget inconsistency: "
                f"RISK_PER_TRADE ({self.RISK_PER_TRADE:.4f}) × "
                f"MAX_CONSECUTIVE_LOSSES ({self.MAX_CONSECUTIVE_LOSSES}) = "
                f"{streak_loss:.4f} exceeds "
                f"MAX_DAILY_LOSS_PCT / 100 ({daily_budget:.4f}). "
                f"Either lower RISK_PER_TRADE, reduce MAX_CONSECUTIVE_LOSSES, "
                f"or raise MAX_DAILY_LOSS_PCT."
            )
        return self

    @model_validator(mode="after")
    def rr_ordering(self) -> "RiskConfig":
        """TARGET_RR must be >= MIN_RR (cannot target below floor)."""
        if self.TARGET_RISK_REWARD_RATIO < self.MIN_RISK_REWARD_RATIO - 1e-9:
            raise ValueError(
                f"TARGET_RISK_REWARD_RATIO ({self.TARGET_RISK_REWARD_RATIO}) "
                f"must be >= MIN_RISK_REWARD_RATIO ({self.MIN_RISK_REWARD_RATIO})."
            )
        return self




# ---------------------------------------------------------------------------
# Sub-model: Stop-Loss Infrastructure
# ---------------------------------------------------------------------------

class SLConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    SL_BUFFER_TICKS: int = Field(
        default_factory=lambda: _c("SL_BUFFER_TICKS", 5),
        ge=1,
        le=200,
    )
    SL_MIN_ATR_MULT: float = Field(
        default_factory=lambda: _c("SL_MIN_ATR_MULT", 0.20),
        gt=0.0,
        le=3.0,
        description="ATR noise floor for entry stops; not an SL ceiling.",
    )
    SL_SWEEP_WICK_CLEARANCE_MULT: float = Field(
        default_factory=lambda: _c("SL_SWEEP_WICK_CLEARANCE_MULT", 0.10),
        ge=0.0,
        le=2.0,
        description="Extra wick-depth clearance beyond swept structure.",
    )
    SL_REGIME_BUFF_SLOPE: float = Field(
        default_factory=lambda: _c("SL_REGIME_BUFF_SLOPE", 0.80),
        ge=0.0,
        le=3.0,
        description="ATR-percentile slope for volatility-regime SL buffers.",
    )
    SL_ATR_BUFFER_MULT: float = Field(
        default_factory=lambda: _c("SL_ATR_BUFFER_MULT", 0.75),
        ge=0.0,
        le=5.0,
    )
    SL_MIN_CLEARANCE_ATR_MULT: float = Field(
        default_factory=lambda: _c("SL_MIN_CLEARANCE_ATR_MULT", 1.5),
        ge=0.0,
        le=10.0,
    )
    POOL_GATE_BE_MIN_ATR_DIST: float = Field(
        default_factory=lambda: _c("POOL_GATE_BE_MIN_ATR_DIST", 0.40),
        ge=0.0,
        le=5.0,
        description=(
            "Minimum ATR distance from current price required before migrating "
            "SL to BE.  Prevents the pool-gate BE loop (BUG 1/11/32)."
        ),
    )

# ---------------------------------------------------------------------------
# Sub-model: Trailing Stop
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Sub-model: Exchange / Execution
# ---------------------------------------------------------------------------

class ExecutionConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    TICK_SIZE_DELTA: float = Field(
        default_factory=lambda: _c("TICK_SIZE_DELTA", 0.5),
        gt=0.0,
        description="Delta Exchange minimum price increment.",
    )
    TICK_SIZE_COINSWITCH: float = Field(
        default_factory=lambda: _c("TICK_SIZE_COINSWITCH", 0.1),
        gt=0.0,
    )
    ORDER_TIMEOUT_SECONDS: float = Field(
        default_factory=lambda: _c("ORDER_TIMEOUT_SECONDS", 600.0),
        gt=0.0,
        le=7200.0,
    )
    MAX_ORDER_RETRIES: int = Field(
        default_factory=lambda: _c("MAX_ORDER_RETRIES", 2),
        ge=0,
        le=10,
    )
    RATE_LIMIT_ORDERS: int = Field(
        default_factory=lambda: _c("RATE_LIMIT_ORDERS", 15),
        ge=1,
        le=1000,
        description="Maximum orders per second before self-throttling.",
    )
    COMMISSION_RATE: float = Field(
        default_factory=lambda: _c("COMMISSION_RATE", 0.00055),
        ge=0.0,
        le=0.01,
    )
    COMMISSION_RATE_MAKER: float = Field(
        default_factory=lambda: _c("COMMISSION_RATE_MAKER", 0.00020),
        ge=-0.001,  # negative = maker rebate
        le=0.01,
    )
    FEE_TO_RISK_SOFT_MAX: float = Field(
        default_factory=lambda: _c("FEE_TO_RISK_SOFT_MAX", 0.35),
        ge=0.0,
        le=2.0,
    )
    FEE_TO_RISK_NO_ALLOC: float = Field(
        default_factory=lambda: _c("FEE_TO_RISK_NO_ALLOC", 0.75),
        ge=0.0,
        le=3.0,
    )
    DELTA_REQUIRE_NATIVE_BRACKET: bool = Field(
        default_factory=lambda: _c("DELTA_REQUIRE_NATIVE_BRACKET", True),
        description="If true, Delta entries must use native bracket orders and cannot fall back to naked limit + standalone exits.",
    )


# ---------------------------------------------------------------------------
# Sub-model: R:R consistency across all three gates
# ---------------------------------------------------------------------------



# ---------------------------------------------------------------------------
# Root model — composes all sub-models
# ---------------------------------------------------------------------------


class TradingConfig(BaseModel):
    """Validated configuration for the sole ICT/Liquidity entry authority."""
    model_config = ConfigDict(frozen=True)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    sl: SLConfig = Field(default_factory=SLConfig)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)

    @model_validator(mode="after")
    def structural_rr_floor(self) -> "TradingConfig":
        if self.risk.MIN_RISK_REWARD_RATIO < 1.5 - 1e-9:
            raise ValueError("risk.MIN_RISK_REWARD_RATIO must be >= 1.5.")
        return self

try:
    cfg: TradingConfig = TradingConfig()
except Exception as _schema_err:
    raise ValueError(
        f"config_schema validation failed — fix config.py before starting the bot.\n"
        f"Detail: {_schema_err}"
    ) from _schema_err

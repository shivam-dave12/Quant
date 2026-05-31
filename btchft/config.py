from __future__ import annotations

"""Single source of truth for every non-secret runtime setting.

Only API credentials are allowed to come from the environment/.env file:
    - DELTA_API_KEY
    - DELTA_SECRET_KEY

Do not add non-secret knobs to .env. Change them here, commit the change, and
rebuild/redeploy so every live run is reproducible from versioned code.
"""

from dataclasses import dataclass
from pathlib import Path
import os
from typing import Final


# ─────────────────────────────────────────────────────────────────────────────
# Secret names only. The secret values remain outside code in .env / secret store.
# ─────────────────────────────────────────────────────────────────────────────
DELTA_API_KEY_ENV: Final[str] = "DELTA_API_KEY"
DELTA_SECRET_KEY_ENV: Final[str] = "DELTA_SECRET_KEY"


def load_delta_credentials() -> tuple[str, str]:
    """Load only secrets from the environment.

    Every other value used by the bot is declared in Settings below.
    """

    return os.getenv(DELTA_API_KEY_ENV, ""), os.getenv(DELTA_SECRET_KEY_ENV, "")


# ─────────────────────────────────────────────────────────────────────────────
# Runtime / strategy / execution configuration.
# Edit these values directly; do not move them to .env.
# ─────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Settings:
    # Mode: SHADOW = capture/train/score only; PAPER = simulated brackets; LIVE = real orders.
    trading_mode: str = "SHADOW"
    allow_live: bool = False
    allow_unvalidated_bootstrap_live: bool = False
    auto_promote_model: bool = True

    # Venue / instrument.
    delta_symbol: str = "BTCUSD"
    delta_testnet: bool = False  # production public feed by default; SHADOW mode still prevents orders

    # Execution-cost profile used for model scoring/promotion.
    # DELTA_TAKER is the conservative current default. HYPERLIQUID_* is for
    # shadow comparison only unless a real Hyperliquid execution/fill adapter is added.
    execution_venue: str = "DELTA"  # DELTA or HYPERLIQUID
    execution_cost_profile: str = "DELTA_TAKER"  # DELTA_TAKER, DELTA_MAKER, HYPERLIQUID_TAKER, HYPERLIQUID_MAKER
    hyperliquid_taker_fee_bps: float = 4.5
    hyperliquid_maker_fee_bps: float = 1.5
    hyperliquid_impact_floor_bps: float = 2.0

    # Journals, state and model artefacts.
    raw_event_journal: Path = Path("artifacts/live/raw_delta_events.jsonl.gz")
    feature_journal: Path = Path("artifacts/live/features.jsonl.gz")
    decision_journal: Path = Path("artifacts/live/decisions.jsonl.gz")
    telemetry_snapshot: Path = Path("artifacts/live/ml_telemetry_snapshot.json")
    model_dir: Path = Path("artifacts/live/models")
    execution_ledger: Path = Path("artifacts/live/rest_fills.jsonl")
    state_path: Path = Path("artifacts/live/runtime_state.json")
    bootstrap_tradeflow_model: Path = Path("artifacts/bootstrap_tradeflow_model.joblib")
    bootstrap_tradeflow_manifest: Path = Path("artifacts/bootstrap_tradeflow_manifest.json")

    # Default output paths for offline utility commands.
    tradeflow_inspection_out: Path = Path("artifacts/tradeflow_inspection.json")
    bootstrap_out_model: Path = Path("artifacts/bootstrap_tradeflow_model.joblib")
    bootstrap_out_manifest: Path = Path("artifacts/bootstrap_tradeflow_manifest.json")
    bootstrap_horizon_seconds: int = 5

    # Costs. Exact realised commissions/slippage are reconciled from REST fills when available.
    taker_fee_bps_pre_gst: float = 5.0
    maker_fee_bps_pre_gst: float = 2.0
    gst_rate: float = 0.18
    impact_floor_bps: float = 2.0
    min_real_fill_count_for_cost_model: int = 30

    # Live learning / promotion gates.
    min_labels_to_score: int = 2_000
    min_labels_to_trade: int = 10_000
    min_promotion_evals: int = 5_000
    promotion_rolling_window: int = 25_000
    min_eligible_predictions_for_promotion: int = 100
    min_eligible_rate_for_promotion: float = 0.001
    min_real_fills_to_live_trade: int = 50
    label_horizons_ms: tuple[int, ...] = (1_000, 3_000, 5_000, 15_000)
    max_feed_latency_ms: float = 250.0
    max_decision_latency_ms: float = 10.0
    feed_stall_seconds: float = 30.0

    # Risk.
    starting_equity_usd: float = 10_000.0
    max_risk_per_trade: float = 0.001
    max_gross_leverage: float = 1.0
    daily_drawdown_halt: float = 0.015
    min_net_edge_bps: float = 1.5
    max_open_position_contracts: int = 1

    # CLI/status ergonomics.
    status_every_seconds: int = 30

    @property
    def scheduled_one_way_taker_fee_bps_with_gst(self) -> float:
        return self.taker_fee_bps_pre_gst * (1.0 + self.gst_rate)

    @property
    def scheduled_round_trip_cost_floor_bps(self) -> float:
        return 2.0 * (self.scheduled_one_way_taker_fee_bps_with_gst + self.impact_floor_bps)

    def validate(self) -> None:
        if self.delta_symbol != "BTCUSD":
            raise ValueError("This package is intentionally bound to Delta BTCUSD until separately validated.")
        if self.trading_mode not in {"SHADOW", "PAPER", "LIVE"}:
            raise ValueError("Settings.trading_mode must be SHADOW, PAPER or LIVE")
        if self.execution_venue not in {"DELTA", "HYPERLIQUID"}:
            raise ValueError("Settings.execution_venue must be DELTA or HYPERLIQUID")
        if self.execution_cost_profile not in {"DELTA_TAKER", "DELTA_MAKER", "HYPERLIQUID_TAKER", "HYPERLIQUID_MAKER"}:
            raise ValueError("Unsupported execution_cost_profile")
        if self.trading_mode == "LIVE" and self.execution_venue != "DELTA":
            raise ValueError("LIVE Hyperliquid execution is blocked until a real Hyperliquid execution/fill adapter is implemented.")
        if self.trading_mode == "LIVE" and not self.allow_live:
            raise ValueError("LIVE rejected: set Settings.allow_live=True after operational approval.")
        if self.min_labels_to_trade < self.min_labels_to_score:
            raise ValueError("min_labels_to_trade must be >= min_labels_to_score")
        if self.max_risk_per_trade <= 0 or self.max_risk_per_trade > 0.05:
            raise ValueError("max_risk_per_trade must be in (0, 0.05]")

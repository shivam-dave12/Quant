"""Fund mandate and institutional risk constants.

The mandate is configuration, not alpha. It defines what agents are allowed to
consider and what the risk committee may approve.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore


def _cfg(name: str, default: Any) -> Any:
    if config is None:
        return default
    return getattr(config, name, default)


@dataclass(frozen=True)
class FundMandate:
    enabled: bool = True
    paper_mode: bool = True
    live_ordering_enabled: bool = False
    top_n_execution_desks: int = 3
    top_n_depth_scan: int = 6
    min_ticker_score: float = 0.52
    min_execution_score: float = 0.58
    min_setup_score: float = 0.50
    max_spread_bps_crypto: float = 18.0
    max_spread_bps_equity: float = 45.0
    max_spread_bps_commodity: float = 55.0
    max_data_age_sec: float = 90.0
    min_warmup_ratio: float = 0.88
    audit_log_path: str = "data/fund_audit.jsonl"
    icici_enabled: bool = False
    coindcx_enabled: bool = False

    @classmethod
    def from_config(cls) -> "FundMandate":
        return cls(
            enabled=bool(_cfg("AGENTIC_FUND_ENABLED", True)),
            paper_mode=bool(_cfg("FUND_PAPER_MODE", True)),
            live_ordering_enabled=bool(_cfg("FUND_LIVE_ORDERING_ENABLED", False)),
            top_n_execution_desks=max(1, int(_cfg("FUND_TOP_N_EXECUTION_DESKS", 3))),
            top_n_depth_scan=max(1, int(_cfg("FUND_TOP_N_DEPTH_SCAN", 6))),
            min_ticker_score=float(_cfg("FUND_MIN_TICKER_SCORE", 0.52)),
            min_execution_score=float(_cfg("FUND_MIN_EXECUTION_SCORE", 0.58)),
            min_setup_score=float(_cfg("FUND_MIN_SETUP_SCORE", 0.50)),
            max_spread_bps_crypto=float(_cfg("FUND_MAX_SPREAD_BPS_CRYPTO", 18.0)),
            max_spread_bps_equity=float(_cfg("FUND_MAX_SPREAD_BPS_EQUITY", 45.0)),
            max_spread_bps_commodity=float(_cfg("FUND_MAX_SPREAD_BPS_COMMODITY", 55.0)),
            max_data_age_sec=float(_cfg("FUND_MAX_DATA_AGE_SEC", 90.0)),
            min_warmup_ratio=float(_cfg("FUND_MIN_WARMUP_RATIO", 0.88)),
            audit_log_path=str(_cfg("FUND_AUDIT_LOG_PATH", "data/fund_audit.jsonl")),
            icici_enabled=bool(_cfg("ICICI_ENABLED", False)),
            coindcx_enabled=bool(_cfg("COINDCX_ENABLED", False)),
        )

    def max_spread_for_class(self, asset_class: str) -> float:
        ac = str(asset_class or "").lower()
        if "equity" in ac or "index" in ac:
            return self.max_spread_bps_equity
        if "commodity" in ac:
            return self.max_spread_bps_commodity
        return self.max_spread_bps_crypto

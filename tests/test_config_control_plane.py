from __future__ import annotations

import importlib
from pathlib import Path

import config
import strategy.institutional_strategy as institutional_strategy


def test_env_cannot_override_non_secret_runtime_policy(monkeypatch):
    monkeypatch.setenv("INSTITUTIONAL_ENABLE_LIVE_ENTRIES", "true")
    monkeypatch.setenv("EXECUTION_EXCHANGE", "groww")
    monkeypatch.setenv("UNIVERSE_INCLUDE_EXCHANGES", "groww")
    monkeypatch.setenv("GROWW_SESSION_MODEL_AUDIT_FULL_INFO", "true")
    reloaded = importlib.reload(config)
    assert reloaded.LIVE_TRADING_ENABLED is True
    assert reloaded.INSTITUTIONAL_ENABLE_LIVE_ENTRIES is True
    assert reloaded.LIVE_EXECUTION_VENUES == ("delta", "coinswitch", "hyperliquid")
    assert reloaded.EXECUTION_EXCHANGE == "delta"
    assert reloaded.UNIVERSE_INCLUDE_EXCHANGES == "delta,coinswitch,hyperliquid,groww"
    assert reloaded.GROWW_SESSION_MODEL_AUDIT_FULL_INFO is False


def test_env_example_contains_secrets_only():
    text = Path(__file__).resolve().parents[1].joinpath(".env.example").read_text()
    forbidden = {
        "LIVE_TRADING_ENABLED", "INSTITUTIONAL_ENABLE_LIVE_ENTRIES", "EXECUTION_EXCHANGE",
        "UNIVERSE_INCLUDE_EXCHANGES", "GROWW_APPROVED_STATIC_IPS",
        "GROWW_SEBI_ALGO_REGISTRATION_CONFIRMED", "INSTITUTIONAL_DECISION_TELEMETRY_ENABLED",
    }
    assert not [key for key in sorted(forbidden) if f"{key}=" in text]
    for required in ("TELEGRAM_BOT_TOKEN=", "GROWW_TOTP_TOKEN=", "GROWW_TOTP_SECRET="):
        assert required in text


def test_live_order_permission_is_venue_allowlisted(monkeypatch):
    values = {"LIVE_TRADING_ENABLED": True, "LIVE_EXECUTION_VENUES": ("groww",)}
    monkeypatch.setattr(institutional_strategy, "_cfg", lambda name, default: values.get(name, default))
    assert institutional_strategy._live_routing_permission("groww") == (True, "live_execution_authorised")
    assert institutional_strategy._live_routing_permission("delta") == (False, "live_execution_venue_not_authorised:delta")


def test_groww_live_policy_fails_closed_without_ip_or_broker_confirmation(monkeypatch):
    monkeypatch.setattr(config, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(config, "LIVE_EXECUTION_VENUES", ("groww",))
    monkeypatch.setattr(config, "GROWW_APPROVED_STATIC_IPS", ())
    monkeypatch.setattr(config, "GROWW_SEBI_ALGO_REGISTRATION_CONFIRMED", False)
    try:
        config.validate_live_control_plane()
    except ValueError as exc:
        message = str(exc)
        assert "GROWW_APPROVED_STATIC_IPS" in message
        assert "GROWW_SEBI_ALGO_REGISTRATION_CONFIRMED" in message
    else:
        raise AssertionError("Groww live policy must fail closed without IP and broker confirmation")


def test_delta_only_live_policy_does_not_require_groww_prerequisites(monkeypatch):
    monkeypatch.setattr(config, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(config, "LIVE_EXECUTION_VENUES", ("delta",))
    monkeypatch.setattr(config, "GROWW_APPROVED_STATIC_IPS", ())
    monkeypatch.setattr(config, "GROWW_SEBI_ALGO_REGISTRATION_CONFIRMED", False)
    monkeypatch.setattr(config, "DELTA_API_KEY", "test")
    monkeypatch.setattr(config, "DELTA_SECRET_KEY", "test")
    config.validate_live_control_plane()


def test_three_venue_live_policy_fails_closed_without_coinswitch_credentials(monkeypatch):
    monkeypatch.setattr(config, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(config, "LIVE_EXECUTION_VENUES", ("delta", "coinswitch", "hyperliquid"))
    monkeypatch.setattr(config, "DELTA_API_KEY", "test")
    monkeypatch.setattr(config, "DELTA_SECRET_KEY", "test")
    monkeypatch.setattr(config, "COINSWITCH_EXECUTION_ENABLED", True)
    monkeypatch.setattr(config, "COINSWITCH_API_KEY", "")
    monkeypatch.setattr(config, "COINSWITCH_SECRET_KEY", "")
    monkeypatch.setattr(config, "HYPERLIQUID_EXECUTION_ENABLED", True)
    monkeypatch.setattr(config, "HYPERLIQUID_PRIVATE_KEY", "test")
    monkeypatch.setattr(config, "HYPERLIQUID_MAIN_API_KEY", "test")
    try:
        config.validate_live_control_plane()
    except ValueError as exc:
        assert "COINSWITCH_API_KEY" in str(exc)
    else:
        raise AssertionError("Three-venue live policy must fail closed without CoinSwitch credentials")

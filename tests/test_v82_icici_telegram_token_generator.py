import threading

import config
from telegram.controller import TelegramBotController


def _controller_stub():
    c = object.__new__(TelegramBotController)
    c._icici_otp_cv = threading.Condition()
    c._icici_pending_otp = None
    c._icici_waiting_for_otp = False
    c._icici_refresh_thread = None
    c._icici_refresh_result = ""
    c.sent = []
    c.send_message = lambda msg, parse_mode="HTML": c.sent.append(msg) or True
    return c


def test_plain_six_digit_otp_is_accepted_when_icici_waiting():
    c = _controller_stub()
    with c._icici_otp_cv:
        c._icici_waiting_for_otp = True
    assert "OTP received" in c.handle_command("123456")
    assert c._icici_pending_otp == "123456"


def test_startup_auto_runs_icici_token_generator_with_telegram_otp(monkeypatch):
    c = _controller_stub()
    monkeypatch.setattr(config, "ICICI_AUTO_TOKEN_GENERATOR_ON_STARTUP", True, raising=False)
    monkeypatch.setattr(config, "DYNAMIC_DESK_ICICI_DETAILS_ENABLED", True, raising=False)
    monkeypatch.setattr(config, "ICICI_AUTH_REQUIRED_FOR_DETAILS", False, raising=False)
    monkeypatch.setattr(config, "ICICI_ENABLED", False, raising=False)
    c._telegram_icici_otp_getter = lambda: "654321"

    calls = []

    class FakeSession:
        def age_sec(self):
            return 1.2

        def masked(self):
            return {"session_token": "abc***1234"}

    class FakeService:
        def require_configured(self, *, for_login=False):
            calls.append(("require", for_login))

        def get_session(self, *, force_refresh=False):
            calls.append(("get", force_refresh))
            raise RuntimeError("missing api session")

        def refresh(self, *, otp_getter=None, otp_code=None):
            calls.append(("refresh", otp_getter()))
            return FakeSession()

    import exchanges.icici.breeze_auth as breeze_auth

    monkeypatch.setattr(breeze_auth, "BreezeTokenService", FakeService)
    c._ensure_icici_session_before_bot_start()

    assert ("refresh", "654321") in calls
    assert any("ICICI Breeze token ready" in msg for msg in c.sent)

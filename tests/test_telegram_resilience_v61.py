import os
import socket
import unittest

os.environ.setdefault("DELTA_API_KEY", "dummy")
os.environ.setdefault("DELTA_API_SECRET", "dummy")
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi")
os.environ.setdefault("TELEGRAM_CHAT_ID", "111")

from telegram.controller import TelegramBotController, _redact_telegram_secret


class TelegramResilienceTests(unittest.TestCase):
    def test_token_redaction_masks_url_and_raw_token(self):
        raw = "https://api.telegram.org/bot123456:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi/getUpdates token=123456:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi"
        safe = _redact_telegram_secret(raw)
        self.assertNotIn("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi", safe)
        self.assertIn("/bot<redacted>/", safe)

    def test_dns_failure_enters_backoff_without_raising(self):
        c = TelegramBotController()
        original = socket.getaddrinfo
        def fail(*args, **kwargs):
            raise OSError("temporary dns failure 123456:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi")
        socket.getaddrinfo = fail
        try:
            out = c.get_updates(timeout=1)
            self.assertEqual(out, [])
            self.assertTrue(c._tg_degraded)
            self.assertEqual(c._tg_failures, 1)
            self.assertGreaterEqual(c._tg_backoff_s, 2.0)
            c._tg_next_poll_ts = 0
            c.get_updates(timeout=1)
            self.assertGreaterEqual(c._tg_backoff_s, 4.0)
        finally:
            socket.getaddrinfo = original


if __name__ == "__main__":
    unittest.main()

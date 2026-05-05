import unittest
from unittest.mock import patch

from exchanges.delta.websocket import DeltaWebSocket
import exchanges.delta.websocket as delta_ws_mod


class _FakeWebSocketApp:
    def __init__(self, url, on_open=None, on_message=None, on_error=None, on_close=None):
        self.url = url
        self.on_open = on_open
        self.on_message = on_message
        self.on_error = on_error
        self.on_close = on_close
        self.sent = []

    def run_forever(self, *args, **kwargs):
        # Regression guard for v15: connect() used to execute a mis-indented
        # logger.warning(_msg) outside _on_error and crash before the thread.
        if self.on_open:
            self.on_open(self)
        if self.on_error:
            self.on_error(self, RuntimeError("synthetic websocket error"))
        return None

    def send(self, payload):
        self.sent.append(payload)

    def close(self):
        return None


class DeltaWebSocketRuntimeRegressionTest(unittest.TestCase):
    def test_connect_does_not_crash_on_ws_error_callback(self):
        with patch.object(delta_ws_mod.websocket, "WebSocketApp", _FakeWebSocketApp), \
             patch.object(delta_ws_mod.websocket, "enableTrace", lambda *_a, **_k: None):
            ws = DeltaWebSocket(api_key="", secret_key="", testnet=True)
            self.assertTrue(ws.connect(timeout=2))
            self.assertTrue(ws.is_connected)
            ws.disconnect()


if __name__ == "__main__":
    unittest.main()

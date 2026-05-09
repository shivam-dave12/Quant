import hashlib
import json
from types import SimpleNamespace

import pytest

from agents.tradable_ticker_desk import TradableTickerDesk
from core.instruments import AssetClass, ExchangeInstrument, ExchangeName, TradableInstrument
from exchanges.icici.api import BreezeRestClient
from exchanges.icici.breeze_auth import BreezeSession, BreezeTokenService
from exchanges.icici.token_generator import login_url


class FakeAuth:
    api_key = "app+key=with special"
    secret_key = "secret"

    def __init__(self):
        self.force_flags = []

    def get_session(self, force_refresh=False):
        self.force_flags.append(force_refresh)
        return BreezeSession(api_session="api-session", session_token="session-token", created_at=1.0, raw_customer_details={})

    def can_refresh_without_operator(self):
        return True


def test_breeze_login_url_encodes_app_key():
    assert login_url("abc+def=ghi!").endswith("abc%2Bdef%3Dghi%21")


def test_breeze_headers_follow_checksum_contract(monkeypatch):
    client = BreezeRestClient(auth=FakeAuth())
    monkeypatch.setattr(client, "_timestamp", lambda: "2026-05-09T10:00:00.000Z")
    payload = json.dumps({"exchange_code": "NSE"}, separators=(",", ":"))
    headers = client._headers(payload)
    expected = hashlib.sha256(("2026-05-09T10:00:00.000Z" + payload + "secret").encode("utf-8")).hexdigest()
    assert headers["X-Checksum"] == "token " + expected
    assert headers["X-AppKey"] == "app+key=with special"
    assert headers["X-SessionToken"] == "session-token"


def test_portfolio_positions_uses_official_path(monkeypatch):
    client = BreezeRestClient(auth=FakeAuth())
    seen = {}

    def fake_request(method, path, body=None, **kwargs):
        seen.update(method=method, path=path, body=body)
        return {"Success": []}

    monkeypatch.setattr(client, "request", fake_request)
    assert client.get_portfolio_positions() == {"Success": []}
    assert seen == {"method": "GET", "path": "/portfoliopositions", "body": {}}


def test_token_service_prefers_api_session_file(monkeypatch, tmp_path):
    p = tmp_path / "api_session.txt"
    p.write_text("login-session\n", encoding="utf-8")
    svc = BreezeTokenService(api_key="app", secret_key="sec", api_session_path=p, cache_path=tmp_path / "cache.json")

    def fake_exchange(api_session):
        assert api_session == "login-session"
        return BreezeSession(api_session=api_session, session_token="header-session", created_at=1.0, raw_customer_details={})

    monkeypatch.setattr(svc, "exchange_api_session", fake_exchange)
    got = svc.get_session(force_refresh=True)
    assert got.session_token == "header-session"


def _icici_inst(asset_id="NIFTY"):
    ex = ExchangeInstrument(
        exchange=ExchangeName.ICICI,
        symbol=asset_id,
        ws_symbol=asset_id,
        display_symbol=asset_id,
        asset_id=asset_id,
        asset_class=AssetClass.OPTION,
        raw={"ExchangeCode": "NSE", "ShortName": asset_id, "Series": "EQ"},
    )
    return TradableInstrument(
        asset_id=asset_id,
        display_name=asset_id,
        asset_class=AssetClass.OPTION,
        primary_exchange=ExchangeName.ICICI,
        by_exchange={ExchangeName.ICICI: ex},
        priority=1,
    )


class FakeICICIQuotes:
    def __init__(self):
        self.calls = 0

    def get_quote_for_instrument(self, instrument):
        self.calls += 1
        return {"Success": {"ltp": "100", "bPrice": "99.95", "sPrice": "100.05", "ttv": "10000000"}}


def test_tradable_desk_icici_quote_probes_are_bounded(monkeypatch):
    monkeypatch.setattr("agents.tradable_ticker_desk._cfg", lambda name, default: 1 if name == "DYNAMIC_DESK_ICICI_QUOTE_PROBES" else default)
    api = FakeICICIQuotes()
    sel = TradableTickerDesk().select([_icici_inst("NIFTY"), _icici_inst("BANKNIFTY")], icici_api=api)
    assert api.calls == 1
    assert any("icici authenticated quote probes=1" in note for note in sel.notes)

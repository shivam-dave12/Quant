"""Breeze authentication service.

ICICI Breeze has two tokens:
1. API_Session from the browser login redirect.
2. session_token returned by CustomerDetails, used in signed API headers.

This service owns both steps, caches only what is needed, and never logs token
contents.
"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Optional

import requests

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore

from .token_generator import generate_api_session, generate_api_session_from_env


def _cfg(name: str, default: Any) -> Any:
    if config is None:
        return default
    return getattr(config, name, default)


@dataclass(frozen=True)
class BreezeSession:
    api_session: str
    session_token: str
    created_at: float
    raw_customer_details: dict

    def age_sec(self) -> float:
        return max(0.0, time.time() - self.created_at)

    def masked(self) -> dict:
        d = asdict(self)
        d["api_session"] = self._mask(self.api_session)
        d["session_token"] = self._mask(self.session_token)
        return d

    @staticmethod
    def _mask(value: str) -> str:
        value = str(value or "")
        if len(value) <= 8:
            return "***"
        return value[:3] + "***" + value[-4:]


class BreezeTokenService:
    CUSTOMER_DETAILS_URL = "https://api.icicidirect.com/breezeapi/api/v1/customerdetails"

    def __init__(
        self,
        *,
        api_key: str | None = None,
        secret_key: str | None = None,
        client_id: str | None = None,
        password: str | None = None,
        cache_path: str | Path | None = None,
        ttl_sec: float | None = None,
    ) -> None:
        self.api_key = api_key if api_key is not None else str(_cfg("BREEZE_API_KEY", os.getenv("BREEZE_API_KEY", "")))
        self.secret_key = secret_key if secret_key is not None else str(_cfg("BREEZE_SECRET_KEY", os.getenv("BREEZE_SECRET_KEY", "")))
        self.client_id = client_id if client_id is not None else str(_cfg("ICICI_CLIENT_ID", os.getenv("ICICI_CLIENT_ID", "")))
        self.password = password if password is not None else str(_cfg("ICICI_PASSWORD", os.getenv("ICICI_PASSWORD", "")))
        self.cache_path = Path(cache_path or _cfg("ICICI_SESSION_CACHE_PATH", "data/icici_breeze_session.json"))
        self.ttl_sec = float(ttl_sec if ttl_sec is not None else _cfg("ICICI_SESSION_TTL_SEC", 6 * 60 * 60))
        self._lock = threading.RLock()
        self._memory: Optional[BreezeSession] = None

    def require_configured(self) -> None:
        missing = []
        if not self.api_key:
            missing.append("BREEZE_API_KEY")
        if not self.secret_key:
            missing.append("BREEZE_SECRET_KEY")
        if not self.client_id:
            missing.append("ICICI_CLIENT_ID")
        if not self.password:
            missing.append("ICICI_PASSWORD")
        if missing:
            raise RuntimeError("Missing ICICI Breeze configuration: " + ", ".join(missing))

    def get_session(self, *, force_refresh: bool = False, otp_getter: Callable[[], str] | None = None) -> BreezeSession:
        with self._lock:
            self.require_configured()
            if not force_refresh and self._memory and self._is_fresh(self._memory):
                return self._memory
            cached = None if force_refresh else self._load_cache()
            if cached and self._is_fresh(cached) and self.validate_session(cached):
                self._memory = cached
                return cached
            return self.refresh(otp_getter=otp_getter)

    def refresh(self, *, otp_getter: Callable[[], str] | None = None, otp_code: str | None = None) -> BreezeSession:
        with self._lock:
            self.require_configured()
            if otp_getter is not None or otp_code is not None:
                api_session = generate_api_session(
                    api_key=self.api_key,
                    client_id=self.client_id,
                    password=self.password,
                    otp_getter=otp_getter,
                    otp_code=otp_code,
                    headless=True,
                    debug_dir=str(_cfg("ICICI_DEBUG_DIR", "data/icici_debug")),
                )
            else:
                api_session = generate_api_session_from_env(headless=False)
            session = self.exchange_api_session(api_session)
            self._memory = session
            self._save_cache(session)
            return session

    def exchange_api_session(self, api_session: str) -> BreezeSession:
        payload = json.dumps({"SessionToken": api_session, "AppKey": self.api_key}, separators=(",", ":"))
        resp = requests.request(
            "GET",
            self.CUSTOMER_DETAILS_URL,
            headers={"Content-Type": "application/json"},
            data=payload,
            timeout=20,
        )
        try:
            data = resp.json()
        except ValueError as exc:
            raise RuntimeError(f"Breeze CustomerDetails returned non-JSON HTTP {resp.status_code}") from exc
        if resp.status_code >= 400 or data.get("Error"):
            raise RuntimeError(f"Breeze CustomerDetails failed HTTP {resp.status_code}: {data.get('Error') or data}")
        session_token = str((data.get("Success") or {}).get("session_token") or "")
        if not session_token:
            raise RuntimeError("Breeze CustomerDetails did not return session_token")
        return BreezeSession(api_session=api_session, session_token=session_token, created_at=time.time(), raw_customer_details=data)

    def validate_session(self, session: BreezeSession) -> bool:
        if not session.session_token:
            return False
        if session.age_sec() > self.ttl_sec:
            return False
        return True

    def _is_fresh(self, session: BreezeSession) -> bool:
        return bool(session.session_token and session.age_sec() <= self.ttl_sec)

    def _load_cache(self) -> Optional[BreezeSession]:
        try:
            data = json.loads(self.cache_path.read_text(encoding="utf-8"))
            return BreezeSession(
                api_session=str(data.get("api_session", "")),
                session_token=str(data.get("session_token", "")),
                created_at=float(data.get("created_at", 0.0)),
                raw_customer_details=dict(data.get("raw_customer_details") or {}),
            )
        except Exception:
            return None

    def _save_cache(self, session: BreezeSession) -> None:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.cache_path.with_suffix(self.cache_path.suffix + ".tmp")
        tmp.write_text(json.dumps(asdict(session), sort_keys=True, default=str), encoding="utf-8")
        os.replace(tmp, self.cache_path)

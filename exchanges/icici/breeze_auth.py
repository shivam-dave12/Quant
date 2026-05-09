"""Breeze authentication service.

ICICI Breeze uses a two-step session flow:
1. API_Session from the browser/app login redirect.
2. session_token returned by CustomerDetails, used in signed API headers.

This service owns both steps, caches only what is needed, and never logs token
contents.  Runtime API clients must call this before protected Breeze endpoints;
Security Master discovery remains public and does not require auth.
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

from .token_generator import generate_api_session


def _cfg(name: str, default: Any) -> Any:
    if config is None:
        return default
    return getattr(config, name, default)


def _env_first(*names: str) -> str:
    for name in names:
        value = _cfg(name, "")
        if str(value or "").strip():
            return str(value).strip()
    return ""


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
    """Session manager compliant with the Breeze CustomerDetails auth flow."""

    CUSTOMER_DETAILS_URL = "https://api.icicidirect.com/breezeapi/api/v1/customerdetails"

    def __init__(
        self,
        *,
        api_key: str | None = None,
        secret_key: str | None = None,
        client_id: str | None = None,
        password: str | None = None,
        api_session: str | None = None,
        session_token: str | None = None,
        api_session_path: str | Path | None = None,
        cache_path: str | Path | None = None,
        ttl_sec: float | None = None,
    ) -> None:
        self.api_key = api_key if api_key is not None else str(_cfg("BREEZE_API_KEY", ""))
        self.secret_key = secret_key if secret_key is not None else str(_cfg("BREEZE_SECRET_KEY", ""))
        self.client_id = client_id if client_id is not None else str(_cfg("ICICI_CLIENT_ID", ""))
        self.password = password if password is not None else str(_cfg("ICICI_PASSWORD", ""))
        self._api_session_override = str(api_session or "").strip()
        self._session_token_override = str(session_token or "").strip()
        self.api_session_path = Path(api_session_path or _cfg("ICICI_API_SESSION_PATH", "data/icici_api_session.txt"))
        self.cache_path = Path(cache_path or _cfg("ICICI_SESSION_CACHE_PATH", "data/icici_breeze_session.json"))
        self.ttl_sec = float(ttl_sec if ttl_sec is not None else _cfg("ICICI_SESSION_TTL_SEC", 6 * 60 * 60))
        self._lock = threading.RLock()
        self._memory: Optional[BreezeSession] = None

    def has_minimum_config(self) -> bool:
        return bool(self.api_key and self.secret_key)

    def require_configured(self, *, for_login: bool = False) -> None:
        missing = []
        if not self.api_key:
            missing.append("BREEZE_API_KEY")
        if not self.secret_key:
            missing.append("BREEZE_SECRET_KEY")
        if for_login:
            if not self.client_id:
                missing.append("ICICI_CLIENT_ID")
            if not self.password:
                missing.append("ICICI_PASSWORD")
        if missing:
            raise RuntimeError("Missing ICICI Breeze configuration: " + ", ".join(missing))

    def get_session(self, *, force_refresh: bool = False, otp_getter: Callable[[], str] | None = None, otp_code: str | None = None) -> BreezeSession:
        with self._lock:
            self.require_configured(for_login=False)
            if not force_refresh and self._memory and self._is_fresh(self._memory):
                return self._memory
            cached = None if force_refresh else self._load_cache()
            if cached and self._is_fresh(cached) and self.validate_session(cached):
                self._memory = cached
                return cached
            return self.refresh(otp_getter=otp_getter, otp_code=otp_code)

    def refresh(self, *, otp_getter: Callable[[], str] | None = None, otp_code: str | None = None) -> BreezeSession:
        """Create a fresh Breeze session.

        Preferred production path:
        - generate API_Session once using the login flow or script;
        - place it in BREEZE_API_SESSION / ICICI_API_SESSION or the configured
          ICICI_API_SESSION_PATH;
        - this method exchanges it through CustomerDetails for session_token.

        Browser automation is kept as an operator-controlled fallback only.  It
        is never attempted silently without an OTP source.
        """
        with self._lock:
            self.require_configured(for_login=False)

            # Accept an already exchanged session_token only when deliberately
            # supplied.  This is useful for emergency manual preflight but the
            # normal compliant path remains API_Session -> CustomerDetails.
            direct_session_token = self._configured_session_token()
            if direct_session_token:
                session = BreezeSession(
                    api_session="",
                    session_token=direct_session_token,
                    created_at=time.time(),
                    raw_customer_details={"source": "manual_session_token"},
                )
                self._memory = session
                self._save_cache(session)
                return session

            api_session = self._configured_api_session()
            if api_session:
                session = self.exchange_api_session(api_session)
                self._memory = session
                self._save_cache(session)
                return session

            if otp_getter is not None or otp_code is not None:
                self.require_configured(for_login=True)
                api_session = generate_api_session(
                    api_key=self.api_key,
                    client_id=self.client_id,
                    password=self.password,
                    otp_getter=otp_getter,
                    otp_code=otp_code,
                    headless=bool(_cfg("ICICI_TOKEN_GENERATOR_HEADLESS", True)),
                    debug_dir=str(_cfg("ICICI_DEBUG_DIR", "data/icici_debug")),
                )
                self._save_api_session(api_session)
                session = self.exchange_api_session(api_session)
                self._memory = session
                self._save_cache(session)
                return session

            raise RuntimeError(
                "ICICI Breeze API_Session is missing. Run the token generator first, "
                "then set BREEZE_API_SESSION/ICICI_API_SESSION or write it to "
                f"{self.api_session_path}. Protected Breeze endpoints cannot be used before CustomerDetails session generation."
            )

    def exchange_api_session(self, api_session: str) -> BreezeSession:
        api_session = str(api_session or "").strip()
        if not api_session:
            raise RuntimeError("Breeze API_Session is empty")
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

    def can_refresh_without_operator(self) -> bool:
        return bool(self._configured_session_token() or self._configured_api_session())

    def _is_fresh(self, session: BreezeSession) -> bool:
        return bool(session.session_token and session.age_sec() <= self.ttl_sec)

    def _configured_session_token(self) -> str:
        if self._session_token_override:
            return self._session_token_override
        return _env_first("BREEZE_SESSION_TOKEN", "ICICI_SESSION_TOKEN")

    def _configured_api_session(self) -> str:
        if self._api_session_override:
            return self._api_session_override
        env_token = _env_first("BREEZE_API_SESSION", "ICICI_API_SESSION", "BREEZE_SESSION_KEY", "ICICI_SESSION_KEY")
        if env_token:
            return env_token
        try:
            if self.api_session_path.exists():
                return self.api_session_path.read_text(encoding="utf-8").strip()
        except Exception:
            return ""
        return ""

    def _save_api_session(self, api_session: str) -> None:
        try:
            self.api_session_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.api_session_path.with_suffix(self.api_session_path.suffix + ".tmp")
            tmp.write_text(str(api_session).strip() + "\n", encoding="utf-8")
            os.replace(tmp, self.api_session_path)
        except Exception:
            # Never fail auth just because a token cache cannot be written.
            pass

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

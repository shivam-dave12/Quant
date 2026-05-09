"""Minimal institutional ICICI Breeze REST client."""

from __future__ import annotations

import hashlib
import json
import csv
import io
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import requests

from .breeze_auth import BreezeTokenService


class BreezeRestClient:
    BASE_URL = "https://api.icicidirect.com/breezeapi/api/v1"
    SECURITY_MASTER_URL = "http://directlink.icicidirect.com/NewSecurityMaster/SecurityMaster.zip"

    def __init__(self, auth: BreezeTokenService | None = None) -> None:
        self.auth = auth or BreezeTokenService()
        self.http = requests.Session()

    def _timestamp(self) -> str:
        return datetime.now(timezone.utc).isoformat()[:19] + ".000Z"

    def _payload(self, body: Optional[Dict[str, Any]]) -> str:
        return json.dumps(body or {}, separators=(",", ":"))

    def _headers(self, payload: str) -> Dict[str, str]:
        session = self.auth.get_session()
        ts = self._timestamp()
        checksum = hashlib.sha256((ts + payload + self.auth.secret_key).encode("utf-8")).hexdigest()
        return {
            "Content-Type": "application/json",
            "X-Checksum": "token " + checksum,
            "X-Timestamp": ts,
            "X-AppKey": self.auth.api_key,
            "X-SessionToken": session.session_token,
        }

    def request(self, method: str, path: str, body: Optional[Dict[str, Any]] = None, *, timeout: float = 20.0) -> Dict[str, Any]:
        payload = self._payload(body)
        resp = self.http.request(
            method.upper(),
            self.BASE_URL + path,
            headers=self._headers(payload),
            data=payload,
            timeout=timeout,
        )
        try:
            data = resp.json()
        except ValueError as exc:
            raise RuntimeError(f"Breeze {path} returned non-JSON HTTP {resp.status_code}") from exc
        if resp.status_code >= 400 or data.get("Error"):
            raise RuntimeError(f"Breeze {path} failed HTTP {resp.status_code}: {data.get('Error') or data}")
        return data

    def get_funds(self) -> Dict[str, Any]:
        return self.request("GET", "/funds", {})

    def get_margin(self, exchange_code: str = "NFO") -> Dict[str, Any]:
        return self.request("GET", "/margin", {"exchange_code": exchange_code})

    def get_quotes(self, **kwargs) -> Dict[str, Any]:
        return self.request("GET", "/quotes", kwargs)

    def get_option_chain_quotes(self, **kwargs) -> Dict[str, Any]:
        return self.request("GET", "/OptionChain", kwargs)

    def get_historical_charts(self, **kwargs) -> Dict[str, Any]:
        return self.request("GET", "/historicalcharts", kwargs)

    def get_security_master_rows(self, *, url: str | None = None, cache_path: str | Path | None = None, timeout: float = 30.0) -> list[dict[str, str]]:
        """Download and parse ICICI's daily Security Master file.

        This is the catalog source for NSE/NFO stock codes, tokens, futures and
        options. It is public, so discovery can run without consuming authenticated
        Breeze API quota; order placement still requires a valid Breeze session.
        """
        source = url or self.SECURITY_MASTER_URL
        data: bytes
        if cache_path:
            path = Path(cache_path)
            if path.exists():
                data = path.read_bytes()
            else:
                data = self._download_security_master(source, timeout)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(data)
        else:
            data = self._download_security_master(source, timeout)
        return self._parse_security_master_zip(data)

    def get_portfolio_positions(self) -> Dict[str, Any]:
        return self.request("GET", "/portfolio", {})

    def place_order(self, **kwargs) -> Dict[str, Any]:
        order_type = str(kwargs.get("order_type", "")).lower()
        if order_type != "limit":
            raise RuntimeError("ICICI Breeze institutional guard: market orders are not permitted; use order_type='limit'")
        required = ("stock_code", "exchange_code", "product", "action", "quantity", "price", "validity")
        missing = [k for k in required if kwargs.get(k) in (None, "")]
        if missing:
            raise RuntimeError("ICICI Breeze order missing required fields: " + ", ".join(missing))
        return self.request("POST", "/order", kwargs)

    def cancel_order(self, **kwargs) -> Dict[str, Any]:
        return self.request("DELETE", "/order", kwargs)

    def modify_order(self, **kwargs) -> Dict[str, Any]:
        return self.request("PUT", "/order", kwargs)

    def square_off(self, **kwargs) -> Dict[str, Any]:
        return self.request("POST", "/squareoff", kwargs)

    @staticmethod
    def _download_security_master(url: str, timeout: float) -> bytes:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.content

    @staticmethod
    def _parse_security_master_zip(data: bytes) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            for name in zf.namelist():
                if not name.lower().endswith((".csv", ".txt")):
                    continue
                raw = zf.read(name)
                text = raw.decode("utf-8", errors="ignore")
                sample = text[:4096]
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=",|\t")
                except Exception:
                    dialect = csv.excel
                reader = csv.DictReader(io.StringIO(text), dialect=dialect)
                for row in reader:
                    cleaned = {str(k or "").strip(): str(v or "").strip() for k, v in row.items()}
                    if cleaned:
                        cleaned["_source_file"] = name
                        rows.append(cleaned)
        return rows

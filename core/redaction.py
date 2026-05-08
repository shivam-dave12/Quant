"""Central sensitive-data redaction for Telegram and dashboard telemetry."""
from __future__ import annotations

import re
from typing import Any

_SENSITIVE_KEY_RE = re.compile(
    r"(?i)\b(api[_-]?key|secret[_-]?key|private[_-]?key|passphrase|password|token|authorization|bearer|wallet[_-]?private[_-]?key)\b\s*[:=]\s*(['\"]?)[^\s,'\"}]+\2"
)
_BEARER_RE = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._\-=/+]{12,}")
_SK_RE = re.compile(r"\bsk-[A-Za-z0-9_\-]{8,}\b")
_HEX_PRIV_RE = re.compile(r"\b0x[a-fA-F0-9]{64}\b")
_LONG_TOKEN_RE = re.compile(r"\b[A-Za-z0-9_\-]{48,}\b")

_ALLOW_LONG_PREFIXES = ("ORDER", "POSITION")


def redact_sensitive(value: Any) -> Any:
    """Redact secrets recursively while keeping market/order numbers intact."""
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            ks = str(k).lower()
            if any(x in ks for x in ("api_key", "apikey", "secret", "private_key", "password", "token", "authorization", "passphrase")):
                out[k] = "<REDACTED>"
            else:
                out[k] = redact_sensitive(v)
        return out
    if isinstance(value, list):
        return [redact_sensitive(v) for v in value]
    if isinstance(value, tuple):
        return tuple(redact_sensitive(v) for v in value)
    if not isinstance(value, str):
        return value
    s = value
    s = _SENSITIVE_KEY_RE.sub(lambda m: f"{m.group(1)}=<REDACTED>", s)
    s = _BEARER_RE.sub("Bearer <REDACTED>", s)
    s = _SK_RE.sub("sk-<REDACTED>", s)
    s = _HEX_PRIV_RE.sub("0x<REDACTED_PRIVATE_KEY>", s)
    # Avoid redacting normal numeric prices/order IDs. This last rule catches
    # very long JWT-like tokens only when they contain letters and symbols.
    def repl(m: re.Match[str]) -> str:
        token = m.group(0)
        if token.isdigit():
            return token
        if any(token.upper().startswith(p) for p in _ALLOW_LONG_PREFIXES):
            return token
        if any(c.isalpha() for c in token) and ("_" in token or "-" in token):
            return "<REDACTED_TOKEN>"
        return token
    return _LONG_TOKEN_RE.sub(repl, s)

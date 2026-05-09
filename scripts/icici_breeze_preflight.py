#!/usr/bin/env python3
"""Generate and validate ICICI Breeze session state.

Usage examples:
  # Print the encoded login URL for manual API_Session generation
  python scripts/icici_breeze_preflight.py --login-url

  # Exchange a manually obtained API_Session through CustomerDetails
  BREEZE_API_SESSION='...' python scripts/icici_breeze_preflight.py --exchange

  # Headless OTP flow, then exchange via CustomerDetails
  ICICI_OTP='123456' python scripts/icici_breeze_preflight.py --generate --exchange

No token contents are printed.  The API_Session is written to
ICICI_API_SESSION_PATH and the exchanged session_token cache is written to
ICICI_SESSION_CACHE_PATH.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config
from exchanges.icici.breeze_auth import BreezeTokenService
from exchanges.icici.token_generator import generate_api_session_from_env, login_url


def main() -> int:
    parser = argparse.ArgumentParser(description="ICICI Breeze token generator/session preflight")
    parser.add_argument("--login-url", action="store_true", help="Print encoded Breeze login URL and exit")
    parser.add_argument("--generate", action="store_true", help="Generate API_Session using ICICI login automation")
    parser.add_argument("--exchange", action="store_true", help="Exchange API_Session through CustomerDetails")
    parser.add_argument("--otp", default="", help="6 digit OTP/TOTP for headless login")
    parser.add_argument("--headed", action="store_true", help="Use a headed browser for login automation")
    parser.add_argument("--api-session-path", default=config.ICICI_API_SESSION_PATH)
    args = parser.parse_args()

    api_key = config.BREEZE_API_KEY
    if args.login_url:
        print(login_url(api_key))
        return 0

    api_session_path = Path(args.api_session_path)
    if args.generate:
        token = generate_api_session_from_env(otp_code=args.otp or None, headless=not args.headed)
        api_session_path.parent.mkdir(parents=True, exist_ok=True)
        api_session_path.write_text(token.strip() + "\n", encoding="utf-8")
        print(f"API_Session generated and saved to {api_session_path}")

    if args.exchange or args.generate:
        svc = BreezeTokenService(api_session_path=api_session_path)
        session = svc.get_session(force_refresh=True)
        masked = session.masked()
        print("CustomerDetails exchange OK")
        print(f"api_session={masked['api_session']} session_token={masked['session_token']}")
        return 0

    parser.error("choose --login-url, --generate, or --exchange")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

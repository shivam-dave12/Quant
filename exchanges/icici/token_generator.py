"""Headless Breeze API_Session generator.

This is the hardened, importable version of the standalone generator. It keeps
the OTP step under operator control: the bot can request an OTP through
Telegram, but the OTP itself must still be supplied by the account owner.
"""

from __future__ import annotations

import logging
import os
import platform
import re
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

log = logging.getLogger(__name__)

LOGIN_URL_TMPL = "https://api.icicidirect.com/apiuser/login?api_key={api_key}"

PAGE_LOAD_MS = 30_000
ELEM_WAIT_MS = 20_000
OTP_WAIT_MS = 75_000

SELS_USER = [
    "input#loginid",
    "input[name='loginid']",
    "input[placeholder*='Client']",
    "input[type='text']:first-of-type",
]
SELS_PASS = [
    "input#password",
    "input[name='password']",
    "input[placeholder*='assword']",
    "input[type='password']",
]
SELS_LOGIN = [
    "input#btnSubmit",
    "input[value='Login']",
    "input#handleLoginbtn",
    "button#handleLoginbtn",
    "input[type='submit']",
    "button[type='submit']",
    "button:has-text('Login')",
]
SELS_TNC = [
    "input#tc",
    "input#aggr",
    "input#termsconditions",
    "input[name='tc']",
    "input[name='aggr']",
    "input[type='checkbox']",
]
SELS_OTP = [
    "input[tg-nm='otp'][tg-ord='first']",
    "input[tg-nm='otp']",
    "input#totp",
    "input[name='totp']",
    "input[placeholder*='OTP']",
    "input[placeholder*='otp']",
    "input[maxlength='6']",
]
SELS_OTPOK = [
    "input#Button1",
    "input[value='Submit']",
    "input#handleOtpbtn",
    "button#handleOtpbtn",
    "button:has-text('Submit')",
    "button:has-text('Verify')",
    "input[type='submit']",
    "button[type='submit']",
]


def launch_args() -> list[str]:
    if platform.system() == "Linux":
        return [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--single-process",
        ]
    return ["--disable-gpu"]


def extract_api_session(url_or_text: str) -> str | None:
    parsed = urlparse(str(url_or_text or ""))
    for source in (parsed.query, parsed.fragment):
        params = parse_qs(source)
        for key in ("apisession", "ApiSession", "session_token"):
            if key in params and params[key]:
                return params[key][0].strip()
        if source and "=" not in source and len(source) > 10:
            return source.strip()
    for part in str(url_or_text or "").replace("?", "&").replace("#", "&").split("&"):
        if "apisession" in part.lower() and "=" in part:
            return part.split("=", 1)[-1].strip()
    return None


def _find(page, selectors: list[str], timeout: int = ELEM_WAIT_MS):
    combined = ", ".join(selectors)
    loc = page.locator(combined).first
    loc.wait_for(state="visible", timeout=timeout)
    return loc


def _dump_html(page, debug_dir: Path, label: str) -> None:
    try:
        debug_dir.mkdir(parents=True, exist_ok=True)
        (debug_dir / f"icici_{label}.html").write_text(page.content(), encoding="utf-8")
    except Exception:
        pass


def _is_split_otp(page) -> bool:
    try:
        return page.locator("input[tg-nm='otp']").count() >= 6
    except Exception:
        return False


def _fill_otp(page, code: str) -> None:
    code = str(code or "").strip()
    if not (len(code) == 6 and code.isdigit()):
        raise RuntimeError("ICICI OTP must be a 6 digit code")
    if _is_split_otp(page):
        page.locator("input[tg-nm='otp']").first.click()
        for digit in code:
            page.keyboard.type(digit, delay=80)
            time.sleep(0.05)
    else:
        loc = _find(page, SELS_OTP, timeout=OTP_WAIT_MS)
        loc.click()
        loc.fill(code)


def _intercept_redirects(page) -> None:
    def _handler(route) -> None:
        req = route.request
        if req.resource_type == "document" and req.url.startswith("http") and "icicidirect.com" not in req.url:
            route.fulfill(status=200, content_type="text/html", body="<html><body>Breeze redirect captured.</body></html>")
        else:
            route.continue_()

    page.route("**", _handler)


def _poll_api_session(page, seconds: float) -> str | None:
    deadline = time.time() + seconds
    full_url = page.url
    while time.time() < deadline:
        try:
            full_url = page.evaluate("window.location.href")
        except Exception:
            full_url = page.url
        token = extract_api_session(full_url)
        if token:
            return token
        time.sleep(0.35)
    try:
        html = page.content()
        match = re.search(r'apisession["\s:=]+([A-Za-z0-9_\-]{10,})', html, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    except Exception:
        pass
    return None


def generate_api_session(
    *,
    api_key: str,
    client_id: str,
    password: str,
    otp_getter=None,
    otp_code: str | None = None,
    headless: bool = True,
    debug_dir: str | Path = "data/icici_debug",
) -> str:
    if not api_key or not client_id or not password:
        raise RuntimeError("BREEZE_API_KEY, ICICI_CLIENT_ID, and ICICI_PASSWORD are required")
    if otp_getter is None and not otp_code and headless:
        raise RuntimeError("Headless ICICI login requires otp_getter or otp_code")
    try:
        from playwright.sync_api import TimeoutError as PWTimeout
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError("Playwright is required: pip install playwright && python -m playwright install chromium") from exc

    debug_path = Path(debug_dir)
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless, args=launch_args() if headless else ["--disable-gpu"])
        context = browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()
        page.set_default_timeout(ELEM_WAIT_MS)
        try:
            page.goto(LOGIN_URL_TMPL.format(api_key=api_key), wait_until="domcontentloaded", timeout=PAGE_LOAD_MS)
            page.wait_for_load_state("networkidle", timeout=PAGE_LOAD_MS)

            _find(page, SELS_USER).fill("")
            _find(page, SELS_USER).type(client_id, delay=80)
            page.keyboard.press("Tab")
            _find(page, SELS_PASS).fill("")
            _find(page, SELS_PASS).type(password, delay=60)
            for selector in SELS_TNC:
                try:
                    cb = page.locator(selector).first
                    cb.wait_for(state="visible", timeout=1200)
                    if not cb.is_checked():
                        cb.click()
                    break
                except Exception:
                    continue

            _intercept_redirects(page)
            try:
                _find(page, SELS_LOGIN).click()
            except PWTimeout:
                page.keyboard.press("Enter")

            api_session = _poll_api_session(page, 10)
            if api_session:
                return api_session

            try:
                _find(page, SELS_OTP, timeout=OTP_WAIT_MS)
            except PWTimeout as exc:
                api_session = _poll_api_session(page, 8)
                if api_session:
                    return api_session
                _dump_html(page, debug_path, "otp_missing")
                raise RuntimeError("ICICI OTP field not found and no API_Session was captured") from exc

            code = otp_code or str(otp_getter()).strip()
            _fill_otp(page, code)
            try:
                _find(page, SELS_OTPOK, timeout=5_000).click()
            except PWTimeout:
                page.keyboard.press("Enter")

            api_session = _poll_api_session(page, 30)
            if not api_session:
                _dump_html(page, debug_path, "no_api_session")
                raise RuntimeError("ICICI login completed but API_Session was not found")
            return api_session
        finally:
            try:
                browser.close()
            except Exception:
                pass


def generate_api_session_from_env(otp_getter=None, otp_code: str | None = None, headless: bool = True) -> str:
    return generate_api_session(
        api_key=os.getenv("BREEZE_API_KEY", ""),
        client_id=os.getenv("ICICI_CLIENT_ID", ""),
        password=os.getenv("ICICI_PASSWORD", ""),
        otp_getter=otp_getter,
        otp_code=otp_code,
        headless=headless,
        debug_dir=os.getenv("ICICI_DEBUG_DIR", "data/icici_debug"),
    )

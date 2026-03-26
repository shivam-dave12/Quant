"""
telegram/config.py — Telegram Bot Configuration
=================================================
Single source of truth for all Telegram credentials and settings.

TELEGRAM_REPORT_INTERVAL_SEC is imported from root config (which reads
TELEGRAM_REPORT_INTERVAL_SEC env-var with a 900s default) to avoid
duplication between this sub-config and the root config.
"""

import sys, os as _os; sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
import os
from dotenv import load_dotenv

load_dotenv()

# Bot credentials
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

# Enable/disable Telegram notifications
TELEGRAM_ENABLED = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)

# Periodic report interval — single source of truth is root config.py.
# The root config reads env-var TELEGRAM_REPORT_INTERVAL_SEC (default 900s).
try:
    import config as _root_config
    TELEGRAM_REPORT_INTERVAL_SEC = _root_config.TELEGRAM_REPORT_INTERVAL_SEC
except (ImportError, AttributeError):
    try:
        TELEGRAM_REPORT_INTERVAL_SEC = int(
            os.getenv("TELEGRAM_REPORT_INTERVAL_SEC", "900")
        )
    except Exception:
        TELEGRAM_REPORT_INTERVAL_SEC = 900

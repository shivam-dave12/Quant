import sys, os as _os; sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
# telegram/config.py

import os
from dotenv import load_dotenv

# Reuse the existing .env setup
load_dotenv()

# Bot credentials from .env
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Enable/disable Telegram notifications
TELEGRAM_ENABLED = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)

# BUG-11 FIX: TELEGRAM_REPORT_INTERVAL_SEC was defined independently here
# (hardcoded 900 or env-var) AND in the root config.py (hardcoded 900).
# notifier.py imports from root config, so this sub-config value was never
# read — dead code and a maintenance trap (two sources of truth for the
# same constant).  Import the single source-of-truth from root config.
# Root config reads the env-var TELEGRAM_REPORT_INTERVAL_SEC with a 900s
# default, so the behaviour is identical but the duplication is eliminated.
try:
    import config as _root_config
    TELEGRAM_REPORT_INTERVAL_SEC = _root_config.TELEGRAM_REPORT_INTERVAL_SEC
except (ImportError, AttributeError):
    # Graceful fallback if root config is not on sys.path for some reason
    try:
        TELEGRAM_REPORT_INTERVAL_SEC = int(
            os.getenv("TELEGRAM_REPORT_INTERVAL_SEC", "900")
        )
    except Exception:
        TELEGRAM_REPORT_INTERVAL_SEC = 900

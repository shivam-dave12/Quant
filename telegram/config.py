"""
telegram/config.py — Telegram Bot Configuration
=================================================
Single source of truth is root config.py. This module re-exports
the Telegram-specific values for backward-compatible imports.
"""
import sys, os as _os
sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))

import config as _root

TELEGRAM_BOT_TOKEN = _root.TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID   = _root.TELEGRAM_CHAT_ID
TELEGRAM_ENABLED   = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)
TELEGRAM_REPORT_INTERVAL_SEC = _root.TELEGRAM_REPORT_INTERVAL_SEC

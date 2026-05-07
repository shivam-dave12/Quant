"""
main.py — Delta unified EntryEngine bot entrypoint
==================================================
Single production entrypoint. The old single-asset / dual-exchange bootstrap was
removed; runtime now starts the portfolio scanner in Delta-only native-bracket
mode.
"""
from __future__ import annotations

from orchestration.multi_asset_bot import main

if __name__ == "__main__":
    main()

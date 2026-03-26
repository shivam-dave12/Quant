#!/usr/bin/env python3
"""
patch_display.py -- Patch display/logging/telegram for v9 strategy
====================================================================
Patches main.py, controller.py to use the new display engine.

USAGE:
  python patch_display.py <project_root>

  where project_root contains:
    main.py
    telegram/controller.py
    strategy/v9_display.py  (must be copied there first)

Creates .bak backups of every file touched.
"""

import sys
import os
import shutil


def read_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()

def write_file(path, content):
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)

def backup(path):
    bak = path + '.bak'
    if os.path.exists(bak):
        bak_size = os.path.getsize(bak)
        cur_size = os.path.getsize(path)
        if bak_size > cur_size:
            print(f"  Keeping larger backup for {path}")
            return
    shutil.copy2(path, bak)
    print(f"  Backup: {bak}")


# ==================================================================
# MAIN.PY PATCHES
# ==================================================================

MAIN_IMPORT_BLOCK = '''
# -- v9.0: Display engine --
try:
    from strategy.v9_display import format_heartbeat as _fmt_hb
    _V9_DISPLAY = True
except ImportError:
    _V9_DISPLAY = False
'''

MAIN_HEARTBEAT_REPLACEMENT = '''
    def maybe_log_heartbeat(self) -> None:
        now = time.time()
        if now - self.last_heartbeat_sec < 60.0:
            return
        self.last_heartbeat_sec = now

        price = self.data_manager.get_last_price() if self.data_manager else 0.0
        pos   = self.strategy.get_position()       if self.strategy   else None
        agg   = self.data_manager.get_secondary_status() if self.data_manager else {}
        feed  = "dual" if agg.get("alive") else "single"
        exch  = self.execution_router.active_exchange.upper() if self.execution_router else "?"

        # v9.0: Use new display engine if available
        if _V9_DISPLAY and self.strategy:
            strat = self.strategy
            engine_state = "SCANNING"
            tracking_info = None
            primary_target = None
            n_bsl = 999.0
            n_ssl = 999.0
            sweep_count = 0
            flow_conv = 0.0
            flow_dir = ""

            if hasattr(strat, '_entry_engine') and strat._entry_engine is not None:
                engine_state = strat._entry_engine.state
                tracking_info = strat._entry_engine.tracking_info

            if hasattr(strat, '_liq_map') and strat._liq_map is not None:
                try:
                    snap = strat._liq_map.get_snapshot(price, strat._atr_5m.atr)
                    primary_target = snap.primary_target
                    n_bsl = snap.nearest_bsl_atr
                    n_ssl = snap.nearest_ssl_atr
                    sweep_count = len(snap.recent_sweeps)
                except Exception:
                    pass

            stats = strat.get_stats() if strat else {}

            msg = _fmt_hb(
                price=price, feed=feed, exchange=exch,
                position=pos, engine_state=engine_state,
                tracking_info=tracking_info,
                primary_target=primary_target,
                nearest_bsl_atr=n_bsl, nearest_ssl_atr=n_ssl,
                recent_sweep_count=sweep_count,
                total_trades=stats.get("total_trades", 0),
                total_pnl=stats.get("total_pnl", 0.0),
                flow_conviction=flow_conv,
                flow_direction=flow_dir,
            )
            logger.info(msg)
            return

        # Legacy heartbeat fallback
        if pos:
            side  = pos.get("side", "?").upper()
            entry = pos.get("entry_price", 0.0)
            sl    = pos.get("sl_price", 0.0)
            tp    = pos.get("tp_price", 0.0)
            if entry <= 0 or side not in ("LONG", "SHORT"):
                logger.info(f"${price:,.2f} [{feed}] | PENDING FILL")
            else:
                pnl = (price - entry) if side == "LONG" else (entry - price)
                logger.info(
                    f"${price:,.2f} [{feed}] | IN {side} @ ${entry:,.2f} | "
                    f"SL ${sl:,.2f}  TP ${tp:,.2f} | unrealised {pnl:+.2f} pts")
        else:
            stats  = self.strategy.get_stats() if self.strategy else {}
            phase  = stats.get("current_phase", "FLAT")
            trades = stats.get("daily_trades", 0)
            pnl    = stats.get("total_pnl", 0.0)
            logger.info(
                f"${price:,.2f} [{feed}|exec={exch}] | {phase} | "
                f"trades today: {trades} | session PnL: ${pnl:+.2f}")
'''


# ==================================================================
# CONTROLLER.PY PATCHES
# ==================================================================

CTRL_IMPORT_BLOCK = '''
# -- v9.0: Display engine --
try:
    from strategy.v9_display import (
        format_thinking_telegram, format_pools_telegram,
        format_flow_telegram, format_status_report_v9,
        format_periodic_report_v9, HELP_TEXT as V9_HELP,
    )
    _V9_DISPLAY = True
except ImportError:
    _V9_DISPLAY = False
'''

CTRL_NEW_POOLS_CMD = '''
    def _cmd_pools(self) -> str:
        """Show full liquidity pool map."""
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."
        try:
            strat = bot_instance.strategy
            dm = bot_instance.data_manager
            if not strat or not dm:
                return "Components not ready."

            price = dm.get_last_price()
            atr = strat._atr_5m.atr
            if not hasattr(strat, '_liq_map') or strat._liq_map is None:
                return "Liquidity map not available (v9 engine not active)."

            snap = strat._liq_map.get_snapshot(price, atr)
            summary = strat._liq_map.get_status_summary(price, atr)

            msg = format_pools_telegram(
                price=price, atr=atr,
                bsl_pools=snap.bsl_pools, ssl_pools=snap.ssl_pools,
                primary_target=snap.primary_target,
                recent_sweeps=snap.recent_sweeps,
                tf_coverage=summary.get("tf_coverage", {}),
            )
            self.send_message(msg)
            return None
        except Exception as e:
            logger.error(f"Pools error: {e}", exc_info=True)
            return f"Error: {e}"
'''

CTRL_NEW_FLOW_CMD = '''
    def _cmd_flow(self) -> str:
        """Show detailed orderflow state."""
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."
        try:
            strat = bot_instance.strategy
            dm = bot_instance.data_manager
            if not strat or not dm:
                return "Components not ready."

            price = dm.get_last_price()

            tick_flow = strat._tick_eng.get_signal() if strat._tick_eng else 0.0
            cvd_trend = strat._cvd.get_trend_signal() if strat._cvd else 0.0
            cvd_div = 0.0
            try:
                cvd_div = strat._cvd.get_divergence_signal(
                    dm.get_candles("1m", limit=60))
            except Exception:
                pass

            ob_imbalance = 0.0
            try:
                ob = dm.get_orderbook()
                if ob and ob.get("bids") and ob.get("asks"):
                    bv = sum(float(b[1]) for b in ob["bids"][:10])
                    av = sum(float(a[1]) for a in ob["asks"][:10])
                    total = bv + av
                    if total > 0:
                        ob_imbalance = (bv - av) / total
            except Exception:
                pass

            streak = getattr(strat, '_flow_streak_count_v2', 0)
            streak_dir = getattr(strat, '_flow_streak_dir_v2', "")

            # Compute conviction
            signals = [tick_flow, cvd_trend]
            if abs(ob_imbalance) > 0.1:
                signals.append(ob_imbalance * 0.5)
            conviction = sum(signals) / len(signals)
            direction = "long" if conviction > 0.25 else ("short" if conviction < -0.25 else "")

            msg = format_flow_telegram(
                price=price, tick_flow=tick_flow,
                cvd_trend=cvd_trend, cvd_divergence=cvd_div,
                ob_imbalance=ob_imbalance,
                tick_streak=streak, streak_direction=streak_dir,
                flow_conviction=conviction, flow_direction=direction,
            )
            self.send_message(msg)
            return None
        except Exception as e:
            logger.error(f"Flow error: {e}", exc_info=True)
            return f"Error: {e}"
'''


def patch_main(project_root):
    """Patch main.py with new heartbeat."""
    path = os.path.join(project_root, "main.py")
    if not os.path.exists(path):
        print(f"  SKIP: {path} not found")
        return

    backup(path)
    content = read_file(path)
    changes = []

    # Add import
    if '_V9_DISPLAY' not in content:
        # Insert after existing imports
        idx = content.find('logger = logging.getLogger(__name__)')
        if idx >= 0:
            end = content.index('\n', idx) + 1
            content = content[:end] + MAIN_IMPORT_BLOCK + content[end:]
            changes.append("Added v9 display import")

    # Replace heartbeat method
    old_hb_start = '    def maybe_log_heartbeat(self) -> None:'
    old_hb_end_marker = '    # ========================================================================='
    if old_hb_start in content and '_V9_DISPLAY' not in content.split(old_hb_start)[1].split(old_hb_end_marker)[0]:
        # Find the full method
        start = content.index(old_hb_start)
        # Find the next method (stream supervisor section)
        after_start = content[start:]
        next_section = after_start.find(old_hb_end_marker, 10)
        if next_section > 0:
            old_method = after_start[:next_section]
            content = content.replace(old_method, MAIN_HEARTBEAT_REPLACEMENT + '\n')
            changes.append("Replaced heartbeat with v9 version")
    elif '_V9_DISPLAY' in content:
        changes.append("Heartbeat already patched")

    write_file(path, content)
    for c in changes:
        print(f"  [OK] main.py: {c}")


def patch_controller(project_root):
    """Patch controller.py with new commands."""
    path = os.path.join(project_root, "telegram", "controller.py")
    if not os.path.exists(path):
        print(f"  SKIP: {path} not found")
        return

    backup(path)
    content = read_file(path)
    changes = []

    # Add import
    if '_V9_DISPLAY' not in content:
        idx = content.find('logger = logging.getLogger(__name__)')
        if idx >= 0:
            end = content.index('\n', idx) + 1
            content = content[:end] + CTRL_IMPORT_BLOCK + content[end:]
            changes.append("Added v9 display import")

    # Add /pools routing
    if '"/pools"' not in content and 'cmd == "/pools"' not in content:
        # Add before the 'else: Unknown command' line
        content = content.replace(
            '            elif cmd == "/huntstatus":',
            '            elif cmd == "/pools":\n'
            '                return self._cmd_pools()\n'
            '            elif cmd == "/flow":\n'
            '                return self._cmd_flow()\n'
            '            elif cmd == "/huntstatus":',
            1
        )
        changes.append("Added /pools and /flow routing")

    # Add _cmd_pools method
    if 'def _cmd_pools(' not in content:
        # Insert before _cmd_help or at the end of command implementations
        insert_marker = '    def _cmd_help(self) -> str:'
        if insert_marker in content:
            content = content.replace(
                insert_marker,
                CTRL_NEW_POOLS_CMD + '\n' + CTRL_NEW_FLOW_CMD + '\n' + insert_marker,
                1
            )
            changes.append("Added _cmd_pools and _cmd_flow methods")

    # Update help text
    if '_V9_DISPLAY' in content and 'V9_HELP' in content:
        # Already has v9 help ref
        pass
    elif '_V9_DISPLAY' in content:
        # Add v9 help override
        content = content.replace(
            'def _cmd_help(self) -> str:\n        return (',
            'def _cmd_help(self) -> str:\n'
            '        if _V9_DISPLAY:\n'
            '            return V9_HELP\n'
            '        return (',
            1
        )
        changes.append("Added V9 help text override")

    # Update /huntstatus to redirect to /pools
    if 'def _cmd_huntstatus(self)' in content:
        # Add redirect at top of huntstatus
        old = 'def _cmd_huntstatus(self)'
        idx = content.find(old)
        if idx >= 0:
            # Find the method body start
            body_start = content.index(':', idx) + 1
            next_line_end = content.index('\n', body_start) + 1
            # Check if already redirected
            if '_cmd_pools' not in content[body_start:body_start+200]:
                # Insert redirect
                indent = '        '
                redirect = f'\n{indent}if _V9_DISPLAY and hasattr(self, "_cmd_pools"):\n{indent}    return self._cmd_pools()\n'
                # Find the first actual code line after docstring
                remaining = content[next_line_end:]
                # Skip docstring
                in_doc = False
                insert_point = next_line_end
                for j, line in enumerate(remaining.split('\n')):
                    insert_point += len(line) + 1
                    stripped = line.strip()
                    if stripped.startswith('"""') or stripped.startswith("'''"):
                        if in_doc:
                            in_doc = False
                            continue
                        if stripped.count('"""') >= 2 or stripped.count("'''") >= 2:
                            continue
                        in_doc = True
                    elif not in_doc and stripped:
                        # First real code line
                        insert_point -= len(line) + 1
                        break

                content = content[:insert_point] + redirect + content[insert_point:]
                changes.append("Added /huntstatus -> /pools redirect")

    write_file(path, content)
    for c in changes:
        print(f"  [OK] controller.py: {c}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python patch_display.py <project_root>")
        print("\nPatches main.py and telegram/controller.py for v9 display engine.")
        print("Requires strategy/v9_display.py to already be in place.")
        sys.exit(1)

    root = sys.argv[0] if len(sys.argv) < 2 else sys.argv[1]
    if not os.path.isdir(root):
        print(f"Error: {root} is not a directory")
        sys.exit(1)

    print(f"Patching display layer in {root}...")
    print()

    # Check v9_display exists
    v9_path = os.path.join(root, "strategy", "v9_display.py")
    if not os.path.exists(v9_path):
        print(f"WARNING: {v9_path} not found. Copy it there first!")
        print("Patches will add imports but they'll fail at runtime without the file.")
        print()

    patch_main(root)
    print()
    patch_controller(root)
    print()
    print("Done. Backups saved as .bak files.")
    print("Test: python -c \"from strategy.v9_display import format_heartbeat\"")


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
migrate_strategy.py - Fixed Migration Script (Windows-safe)
============================================================
Patches quant_strategy.py to use the new liquidity-first entry engine.

FIXES vs previous version:
  - Explicit UTF-8 encoding for read/write (Windows cp1252 fix)
  - No Unicode arrows/emoji in injected code blocks
  - Safer insertion logic with line-by-line validation
  - Dry-run mode to preview changes without writing
  - Size safety check prevents writing empty/truncated files

USAGE:
  python migrate_strategy.py quant_strategy.py
  python migrate_strategy.py quant_strategy.py --dry-run

ROLLBACK:
  copy quant_strategy.py.bak quant_strategy.py
"""

import sys
import os
import shutil


def read_file(path: str) -> str:
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()


def write_file(path: str, content: str) -> None:
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)


# =====================================================================
# CODE BLOCKS TO INJECT (ASCII-safe - no Unicode arrows or emoji)
# =====================================================================

NEW_IMPORTS = '''
# -- v9.0: Liquidity-First Entry Engine ------------------------------------
try:
    from strategy.liquidity_map import LiquidityMap
    _LIQ_MAP_AVAILABLE = True
except ImportError:
    try:
        from liquidity_map import LiquidityMap
        _LIQ_MAP_AVAILABLE = True
    except ImportError:
        _LIQ_MAP_AVAILABLE = False

try:
    from strategy.entry_engine import (
        EntryEngine, ICTTrailManager, OrderFlowState, ICTContext,
        EntryType,
    )
    _ENTRY_ENGINE_AVAILABLE = True
except ImportError:
    try:
        from entry_engine import (
            EntryEngine, ICTTrailManager, OrderFlowState, ICTContext,
            EntryType,
        )
        _ENTRY_ENGINE_AVAILABLE = True
    except ImportError:
        _ENTRY_ENGINE_AVAILABLE = False
'''

INIT_BLOCK = '''
        # -- v9.0: New liquidity-first engines --
        self._liq_map = LiquidityMap() if _LIQ_MAP_AVAILABLE else None
        self._entry_engine = EntryEngine() if _ENTRY_ENGINE_AVAILABLE else None
        self._ict_trail = ICTTrailManager() if _ENTRY_ENGINE_AVAILABLE else None
        self._flow_streak_dir_v2 = ""
        self._flow_streak_count_v2 = 0
        self._last_think_log_v2 = 0.0
        self._force_sl = None
        self._force_tp = None
'''

NEW_EVALUATE_ENTRY = '''
    def _evaluate_entry(self, data_manager, order_manager, risk_manager, now):
        """
        v9.0 -- Liquidity-First Entry Engine.
        Single decision flow. Falls back to legacy if new engine unavailable.
        """
        if not _ENTRY_ENGINE_AVAILABLE or self._entry_engine is None or self._liq_map is None:
            return self._evaluate_entry_legacy(data_manager, order_manager, risk_manager, now)

        # Step 1: Spread gate
        spread_ok, spread_ratio = self._spread_atr_gate(data_manager)
        if not spread_ok:
            return

        price = data_manager.get_last_price()
        atr = self._atr_5m.atr
        if atr < 1e-10 or price < 1.0:
            return

        now_ms = int(now * 1000) if now < 1e12 else int(now)

        # Step 2: Gather candles (all timeframes)
        candles_by_tf = {}
        for tf, limit in [("1m", 200), ("5m", 300), ("15m", 200),
                          ("1h", 100), ("4h", 50), ("1d", 30)]:
            try:
                candles_by_tf[tf] = data_manager.get_candles(tf, limit=limit)
            except Exception:
                candles_by_tf[tf] = []

        # Step 3: Update ICT engine (preserved -- provides structural context)
        if self._ict is not None:
            try:
                self._ict.update(
                    candles_by_tf.get("5m", []),
                    candles_by_tf.get("15m", []),
                    price, now_ms,
                    candles_1m=candles_by_tf.get("1m"),
                    candles_1h=candles_by_tf.get("1h"),
                    candles_4h=candles_by_tf.get("4h"),
                    candles_1d=candles_by_tf.get("1d"),
                )
                if hasattr(self._ict, 'set_order_flow_data'):
                    tf_now = self._tick_eng.get_signal() if self._tick_eng else 0.0
                    cvd_now = self._cvd.get_trend_signal() if self._cvd else 0.0
                    self._ict.set_order_flow_data(tf_now, cvd_now)
            except Exception as e:
                logger.debug(f"ICT update error: {e}")

        # Step 4: Update liquidity map
        self._liq_map.update(
            candles_by_tf=candles_by_tf,
            price=price, atr=atr, now=now,
            ict_engine=self._ict,
        )
        liq_snapshot = self._liq_map.get_snapshot(price, atr)

        # Step 5: Build orderflow state
        tick_flow = self._tick_eng.get_signal() if self._tick_eng else 0.0
        cvd_trend = self._cvd.get_trend_signal() if self._cvd else 0.0
        cvd_div = 0.0
        try:
            cvd_div = self._cvd.get_divergence_signal(
                candles_by_tf.get("1m", []))
        except Exception:
            pass

        if tick_flow > 0.4:
            if self._flow_streak_dir_v2 == "long":
                self._flow_streak_count_v2 += 1
            else:
                self._flow_streak_dir_v2 = "long"
                self._flow_streak_count_v2 = 1
        elif tick_flow < -0.4:
            if self._flow_streak_dir_v2 == "short":
                self._flow_streak_count_v2 += 1
            else:
                self._flow_streak_dir_v2 = "short"
                self._flow_streak_count_v2 = 1
        else:
            self._flow_streak_count_v2 = max(0, self._flow_streak_count_v2 - 1)
            if self._flow_streak_count_v2 == 0:
                self._flow_streak_dir_v2 = ""

        ob_imbalance = 0.0
        try:
            ob = data_manager.get_orderbook()
            if ob and ob.get("bids") and ob.get("asks"):
                bid_vol = sum(float(b[1]) for b in ob["bids"][:10])
                ask_vol = sum(float(a[1]) for a in ob["asks"][:10])
                total = bid_vol + ask_vol
                if total > 0:
                    ob_imbalance = (bid_vol - ask_vol) / total
        except Exception:
            pass

        flow_state = OrderFlowState(
            tick_flow=tick_flow,
            cvd_trend=cvd_trend,
            cvd_divergence=cvd_div,
            ob_imbalance=ob_imbalance,
            tick_streak=self._flow_streak_count_v2,
            streak_direction=self._flow_streak_dir_v2,
        )

        # Step 6: Build ICT context
        ict_ctx = ICTContext()
        if self._ict is not None and getattr(self._ict, '_initialized', False):
            try:
                amd = self._ict.get_amd_state()
                ict_ctx.amd_phase = getattr(amd, 'phase', "")
                ict_ctx.amd_bias = getattr(amd, 'bias', "")
                ict_ctx.amd_confidence = getattr(amd, 'confidence', 0.0)
                mb = self._ict.get_market_bias()
                ict_ctx.in_premium = getattr(mb, 'in_premium', False)
                ict_ctx.in_discount = getattr(mb, 'in_discount', False)
                tf_5m = self._ict._tf.get("5m")
                if tf_5m:
                    ict_ctx.structure_5m = getattr(tf_5m, 'structure', "neutral")
                    ict_ctx.bos_5m = getattr(tf_5m, 'bos_direction', "")
                    ict_ctx.choch_5m = getattr(tf_5m, 'choch_direction', "")
                tf_15m = self._ict._tf.get("15m")
                if tf_15m:
                    ict_ctx.structure_15m = getattr(tf_15m, 'structure', "neutral")
                try:
                    ob_sl = self._ict.get_ob_sl_level("long", price, atr, now_ms)
                    if ob_sl:
                        ict_ctx.nearest_ob_price = ob_sl
                except Exception:
                    pass
                try:
                    sess = self._ict.get_amd_session_context(now_ms)
                    ict_ctx.kill_zone = sess.get("session", "")
                except Exception:
                    pass
            except Exception as e:
                logger.debug(f"ICT context build error: {e}")

        # Step 7: Feed to entry engine
        self._entry_engine.update(
            liq_snapshot=liq_snapshot,
            flow_state=flow_state,
            ict_ctx=ict_ctx,
            price=price, atr=atr, now=now,
        )

        # Step 8: Check for signal and execute
        signal = self._entry_engine.get_signal()
        if signal is not None:
            bal_info = risk_manager.get_available_balance()
            total_bal = float((bal_info or {}).get("total", 0))
            allowed, reason = self._risk_gate.can_trade(total_bal)
            if not allowed:
                logger.info(f"Signal blocked by risk manager: {reason}")
                self._entry_engine.consume_signal()
                return

            logger.info(
                f"SIGNAL: {signal.entry_type.value} {signal.side.upper()} "
                f"@ ${signal.entry_price:,.1f} | "
                f"SL=${signal.sl_price:,.1f} TP=${signal.tp_price:,.1f} "
                f"R:R={signal.rr_ratio:.1f} | {signal.reason}")

            _min_sig = self._last_sig if self._last_sig is not None else SignalBreakdown()
            _min_sig.atr = atr

            self._force_sl = signal.sl_price
            self._force_tp = signal.tp_price

            _tier_map = {
                EntryType.SWEEP_REVERSAL: "S",
                EntryType.PRE_SWEEP_APPROACH: "A",
                EntryType.SWEEP_CONTINUATION: "B",
            }
            _tier = _tier_map.get(signal.entry_type, "A")

            self._launch_entry_async(
                data_manager, order_manager, risk_manager,
                side=signal.side, sig=_min_sig,
                mode=signal.entry_type.value.lower(),
                ict_tier=_tier,
            )
            self._entry_engine.consume_signal()
            self._entry_engine.on_entry_placed()

        # Step 9: Periodic thinking log
        if now - self._last_think_log_v2 >= 30.0:
            self._last_think_log_v2 = now
            state = self._entry_engine.state
            flow_dir = flow_state.direction or "neutral"
            conv = flow_state.conviction
            parts = [f"State={state}", f"Flow={flow_dir}({conv:+.2f})",
                     f"CVD={cvd_trend:+.2f}"]
            if liq_snapshot.primary_target:
                t = liq_snapshot.primary_target
                parts.append(f"Target={t.direction}->${t.pool.price:,.0f}"
                             f"({t.distance_atr:.1f}ATR)")
            if ict_ctx.amd_phase:
                parts.append(f"AMD={ict_ctx.amd_phase[:4]}")
            parts.append(f"BSL={liq_snapshot.nearest_bsl_atr:.1f}ATR")
            parts.append(f"SSL={liq_snapshot.nearest_ssl_atr:.1f}ATR")
            tracking = self._entry_engine.tracking_info
            if tracking:
                parts.append(f"Track={tracking['direction']}->{tracking['target']}")
            logger.info(f"[THINK] {' | '.join(parts)}")

'''

FORCE_SL_TP_BLOCK = '''
        # -- v9.0: Use force SL/TP from entry engine if available --
        _force_sl = getattr(self, '_force_sl', None)
        _force_tp = getattr(self, '_force_tp', None)
        if _force_sl is not None and _force_tp is not None and _force_sl > 0 and _force_tp > 0:
            _fsl = _round_to_tick(_force_sl)
            _ftp = _round_to_tick(_force_tp)
            _dir_ok = False
            if side == "long" and _fsl < price and _ftp > price:
                _dir_ok = True
            elif side == "short" and _fsl > price and _ftp < price:
                _dir_ok = True
            if _dir_ok:
                sl_price = _fsl
                tp_price = _ftp
                logger.info(f"v9.0 force SL/TP: SL=${sl_price:,.1f} TP=${tp_price:,.1f}")
                self._force_sl = None
                self._force_tp = None
'''


def patch_file(filepath: str, dry_run: bool = False) -> bool:
    """Main patching function."""

    print(f"Reading {filepath} ...")
    content = read_file(filepath)
    original_len = len(content)
    lines = content.split('\n')
    print(f"  {len(lines)} lines, {original_len:,} bytes")

    if original_len < 1000:
        print("ERROR: File appears empty or corrupted. Restore from .bak first:")
        print(f"  copy {filepath}.bak {filepath}")
        return False

    # -- Backup (only if no existing larger backup) --
    backup_path = filepath + '.bak'
    if not dry_run:
        if os.path.exists(backup_path):
            bak_size = os.path.getsize(backup_path)
            cur_size = os.path.getsize(filepath)
            if bak_size > cur_size:
                print(f"  Keeping existing backup ({bak_size:,}b > current {cur_size:,}b)")
            else:
                shutil.copy2(filepath, backup_path)
                print(f"  Backup: {backup_path}")
        else:
            shutil.copy2(filepath, backup_path)
            print(f"  Backup: {backup_path}")

    changes = []

    # =================================================================
    # PATCH 1: Add new imports
    # =================================================================
    insert_after = None
    for i, line in enumerate(lines):
        if 'logger = logging.getLogger(__name__)' in line:
            insert_after = i

    if insert_after is not None:
        nearby = '\n'.join(lines[insert_after:min(insert_after+20, len(lines))])
        if '_LIQ_MAP_AVAILABLE' not in nearby:
            lines.insert(insert_after + 1, NEW_IMPORTS)
            changes.append(f"PATCH 1: Added imports after line {insert_after+1}")
        else:
            changes.append("PATCH 1: Already present (skip)")
    else:
        changes.append("PATCH 1: MANUAL -- add NEW_IMPORTS near top")

    content = '\n'.join(lines)
    lines = content.split('\n')

    # =================================================================
    # PATCH 2: Add engine init in __init__
    # =================================================================
    patched_init = False
    for i, line in enumerate(lines):
        if 'self._log_init()' in line and line.strip() == 'self._log_init()':
            nearby = '\n'.join(lines[max(0, i-10):i])
            if '_liq_map' not in nearby:
                lines.insert(i, INIT_BLOCK)
                changes.append(f"PATCH 2: Added engine init at line {i}")
            else:
                changes.append("PATCH 2: Already present (skip)")
            patched_init = True
            break
    if not patched_init:
        changes.append("PATCH 2: MANUAL -- add INIT_BLOCK before self._log_init()")

    content = '\n'.join(lines)
    lines = content.split('\n')

    # =================================================================
    # PATCH 3: Rename old _evaluate_entry
    # =================================================================
    old_def = 'def _evaluate_entry(self, data_manager, order_manager, risk_manager, now):'
    new_def = 'def _evaluate_entry_legacy(self, data_manager, order_manager, risk_manager, now):'

    if old_def in content and new_def not in content:
        content = content.replace(old_def, new_def, 1)
        changes.append("PATCH 3: Renamed _evaluate_entry -> _evaluate_entry_legacy")
    elif new_def in content:
        changes.append("PATCH 3: Already renamed (skip)")
    else:
        changes.append("PATCH 3: WARNING -- def not found")

    lines = content.split('\n')

    # =================================================================
    # PATCH 4: Insert new _evaluate_entry
    # =================================================================
    patched_eval = False
    for i, line in enumerate(lines):
        if 'def _evaluate_entry_legacy(self' in line:
            before = '\n'.join(lines[:i])
            if 'def _evaluate_entry(self, data_manager, order_manager, risk_manager, now):' not in before:
                lines.insert(i, NEW_EVALUATE_ENTRY)
                changes.append(f"PATCH 4: Inserted new _evaluate_entry at line {i}")
            else:
                changes.append("PATCH 4: Already present (skip)")
            patched_eval = True
            break
    if not patched_eval:
        changes.append("PATCH 4: MANUAL -- insert NEW_EVALUATE_ENTRY before _evaluate_entry_legacy")

    content = '\n'.join(lines)
    lines = content.split('\n')

    # =================================================================
    # PATCH 5: Force SL/TP in _enter_trade
    # =================================================================
    patched_enter = False
    for i, line in enumerate(lines):
        if 'sl_price, tp_price = self._compute_sl_tp(' in line:
            context = '\n'.join(lines[max(0, i-8):i])
            if ('Bug-B STEP 1' in context or 'Compute SL/TP FIRST' in context):
                if '_force_sl' not in context:
                    lines.insert(i, FORCE_SL_TP_BLOCK)
                    changes.append(f"PATCH 5: Added force SL/TP at line {i}")
                else:
                    changes.append("PATCH 5: Already present (skip)")
                patched_enter = True
                break
    if not patched_enter:
        changes.append("PATCH 5: MANUAL -- add force SL/TP before _compute_sl_tp in _enter_trade")

    content = '\n'.join(lines)

    # =================================================================
    # PATCH 6: Lifecycle hooks
    # =================================================================
    if 'self._entry_engine.on_position_opened()' not in content:
        content = content.replace(
            "self._risk_gate.record_trade_start()",
            "self._risk_gate.record_trade_start()\n"
            "        if hasattr(self, '_entry_engine') and self._entry_engine is not None:\n"
            "            self._entry_engine.on_position_opened()",
            1
        )
        changes.append("PATCH 6a: Added on_position_opened hook")
    else:
        changes.append("PATCH 6a: Already present (skip)")

    if 'self._entry_engine.on_position_closed()' not in content:
        content = content.replace(
            "def _finalise_exit(self):",
            "def _finalise_exit(self):\n"
            "        if hasattr(self, '_entry_engine') and self._entry_engine is not None:\n"
            "            self._entry_engine.on_position_closed()",
            1
        )
        changes.append("PATCH 6b: Added on_position_closed hook")
    else:
        changes.append("PATCH 6b: Already present (skip)")

    # =================================================================
    # Safety check
    # =================================================================
    final_len = len(content)
    if final_len < original_len * 0.5:
        print(f"\nERROR: Output ({final_len:,}b) < 50% of input ({original_len:,}b)")
        print("Aborting to prevent data loss.")
        return False

    # Print summary
    print(f"\n{'='*60}")
    print(f"MIGRATION SUMMARY {'(DRY RUN)' if dry_run else ''}")
    print(f"{'='*60}")
    for c in changes:
        tag = "OK" if "MANUAL" not in c and "WARNING" not in c else "!!"
        print(f"  [{tag}] {c}")
    print(f"\n  Input:  {original_len:,} bytes")
    print(f"  Output: {final_len:,} bytes (+{final_len - original_len:,})")

    if dry_run:
        print(f"\n  DRY RUN -- no files written. Remove --dry-run to apply.")
        return True

    write_file(filepath, content)
    print(f"\n  Written: {filepath}")
    print(f"  Backup:  {backup_path}")
    print(f"\n  NEXT:")
    print(f"  1. Ensure liquidity_map.py + entry_engine.py in strategy/")
    print(f"  2. python -c \"from strategy.quant_strategy import QuantStrategy\"")
    print(f"  3. Rollback: copy {backup_path} {filepath}")

    return True


if __name__ == '__main__':
    dry_run = '--dry-run' in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith('--')]

    if not args:
        print("Usage: python migrate_strategy.py <quant_strategy.py> [--dry-run]")
        print("\n  --dry-run   Preview changes without writing")
        sys.exit(1)

    filepath = args[0]
    if not os.path.exists(filepath):
        print(f"Error: {filepath} not found")
        sys.exit(1)

    success = patch_file(filepath, dry_run=dry_run)
    sys.exit(0 if success else 1)

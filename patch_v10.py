from pathlib import Path
root=Path('/mnt/data/v10work')
notifier=root/'telegram/notifier.py'
text=notifier.read_text()
# Insert helper after get_queue_stats or before send? Better before send_telegram_message
insert = r'''

# ======================================================================
# MULTI-ASSET TELEGRAM CONTEXT ENRICHMENT
# ======================================================================

def _tg_current_instrument():
    try:
        from core.instruments import current_instrument
        return current_instrument()
    except Exception:
        return None


def _tg_asset_policy(inst):
    try:
        from core.market_policy import active_policy
        return active_policy(inst)
    except Exception:
        return None


def _tg_asset_header(inst=None, event_type: str = "", context: Optional[Dict[str, Any]] = None) -> str:
    """Build an institutional asset-specific Telegram header.

    This is intentionally centralised so legacy BTC-era messages can still be
    sent by strategy code while Telegram always receives the correct contract,
    venue, policy, phase and portfolio context.
    """
    inst = inst or _tg_current_instrument()
    if inst is None:
        return ""
    context = context or {}
    try:
        asset = _esc(getattr(inst, "asset_id", "ASSET"))
        name = _esc(getattr(inst, "display_name", asset))
        primary = getattr(inst, "primary_exchange", None)
        primary_name = _esc(getattr(primary, "value", str(primary or "-")).upper())
        symbol = _esc(getattr(inst, "display_symbol", getattr(inst, "execution_symbol", "-")))
        asset_class = _esc(getattr(getattr(inst, "asset_class", ""), "value", str(getattr(inst, "asset_class", ""))).upper())
        venues = []
        for ex, ei in getattr(inst, "by_exchange", {}).items():
            try:
                venues.append(f"{getattr(ex,'value',str(ex)).upper()}:{getattr(ei,'display_symbol',getattr(ei,'symbol','-'))}")
            except Exception:
                continue
        venue_txt = _esc(", ".join(venues) if venues else f"{primary_name}:{symbol}")
        pol = _tg_asset_policy(inst)
        lev = getattr(pol, "leverage", None) or context.get("leverage") or getattr(inst, "max_leverage", 0) or "-"
        margin = getattr(pol, "margin_pct", None)
        risk_mult = getattr(pol, "risk_multiplier", None)
        cadence = getattr(pol, "evaluation_interval_sec", None)
        state = _esc(str(context.get("state") or context.get("phase") or "-").upper())
        price = context.get("price")
        slots = context.get("slots") or context.get("portfolio_slots") or ""
        event = _esc(str(event_type or context.get("event_type") or "STRATEGY").upper().replace("_", " "))
        line1 = f"🏛 <b>{event}</b>  <code>{asset}</code> <i>{name}</i>"
        line2 = f"<code>{primary_name}:{symbol}</code> · {asset_class} · venues <code>{venue_txt}</code>"
        bits = []
        if lev != "-":
            try: bits.append(f"lev {float(lev):g}x")
            except Exception: bits.append(f"lev {lev}")
        if margin is not None:
            try: bits.append(f"margin {float(margin):.0%}")
            except Exception: pass
        if risk_mult is not None:
            try: bits.append(f"risk×{float(risk_mult):.2f}")
            except Exception: pass
        if cadence is not None:
            try: bits.append(f"cadence {float(cadence):.2f}s")
            except Exception: pass
        if state and state != "-": bits.append(f"state {state}")
        if price is not None:
            try: bits.append(f"px {_tg_price(float(price))}")
            except Exception: pass
        if slots: bits.append(f"slots {slots}")
        line3 = "<code>" + _esc(" | ".join(bits)) + "</code>" if bits else ""
        return "\n".join([x for x in (line1, line2, line3, _TG_RULE) if x])
    except Exception:
        return ""


def _tg_message_already_asset_scoped(message: str) -> bool:
    m = str(message or "")[:240]
    return ("<b>ASSET" in m or "🏛 <b>" in m or "MULTI-ASSET" in m or "EXECUTION UNIVERSE" in m)


def _tg_infer_event_type(message: str) -> str:
    m = str(message or "").upper()
    if "BRACKET" in m or "ENTRY" in m or "POSITION OPEN" in m:
        return "EXECUTION"
    if "TRAIL" in m or "SL" in m or "STOP" in m:
        return "RISK / TRAIL"
    if "EXIT" in m or "PNL" in m or "TP HIT" in m:
        return "EXIT"
    if "POSTERIOR" in m or "P(EDGE)" in m or "EV=" in m:
        return "POSTERIOR"
    if "LIQUIDITY" in m or "SWEEP" in m or "POOL" in m:
        return "LIQUIDITY"
    if "STATUS" in m or "THINK" in m:
        return "STATUS"
    return "ASSET EVENT"


def _tg_enrich_asset_message(message: str, *, instrument=None, event_type: Optional[str] = None, context: Optional[Dict[str, Any]] = None) -> str:
    inst = instrument or _tg_current_instrument()
    if inst is None:
        return message
    if _tg_message_already_asset_scoped(message):
        return message
    header = _tg_asset_header(inst, event_type or _tg_infer_event_type(message), context=context)
    if not header:
        return message
    return f"{header}\n{message}"
'''
# Add after _ensure_worker_started before send_telegram_message
marker='def send_telegram_message(message: str, parse_mode: str = "HTML") -> bool:'
if marker in text:
    text=text.replace(marker, insert+'\n'+marker)
# Modify signature
text=text.replace('def send_telegram_message(message: str, parse_mode: str = "HTML") -> bool:',
                  'def send_telegram_message(message: str, parse_mode: str = "HTML", *, instrument=None, event_type: Optional[str] = None, context: Optional[Dict[str, Any]] = None, enrich: bool = True) -> bool:')
# Insert enrich after repair
old='''    message = _repair_mojibake(str(message))\n    _ensure_worker_started()'''
new='''    message = _repair_mojibake(str(message))\n    if enrich:\n        try:\n            message = _tg_enrich_asset_message(message, instrument=instrument, event_type=event_type, context=context)\n        except Exception:\n            pass\n    _ensure_worker_started()'''
text=text.replace(old,new)
# Patch format_periodic_report BTC line
text=text.replace('''    lines = [\n        f"\\U0001f4ca <b>INSTITUTIONAL STATUS</b>  <code>{_esc(now_ist)}</code>",\n        _TG_RULE,\n        f"<code>BTC     {_tg_price(current_price):>14}   ATR {_tg_num(atr, 1):>7}</code>",''', '''    _inst = _kw.get("instrument", None) or _tg_current_instrument()\n    _asset = getattr(_inst, "asset_id", "BTC") if _inst is not None else "BTC"\n    _symbol = getattr(_inst, "display_symbol", _asset) if _inst is not None else _asset\n    lines = [\n        f"\\U0001f4ca <b>INSTITUTIONAL STATUS</b>  <code>{_esc(now_ist)}</code>",\n        _TG_RULE,\n        f"<code>{_esc(str(_asset)):<7} {_tg_price(current_price):>14}   ATR {_tg_num(atr, 1):>7}</code>",\n        f"<code>SYMBOL  {_esc(str(_symbol)):<14}</code>",''')
notifier.write_text(text)

# Patch quant_strategy send calls
p=root/'strategy/quant_strategy.py'
qt=p.read_text()
# Replace post_trade_agent notifier set
qt=qt.replace('self._post_trade_agent.set_ic_gate_notifier(send_telegram_message)',
              'self._post_trade_agent.set_ic_gate_notifier(lambda m, **kw: self._send_telegram(m, event_type="post_trade", **kw))')
# Insert methods before _log_init
marker='''    def _log_init(self):\n'''
method=r'''    def _telegram_context(self) -> dict:
        """Runtime context attached to every asset-specific Telegram alert."""
        try:
            p = getattr(self, "_pos", None)
            phase = getattr(getattr(p, "phase", None), "name", "UNKNOWN")
            price = float(getattr(self, "_last_known_price", 0.0) or 0.0)
            ctx = {
                "state": phase,
                "price": price if price > 0 else None,
                "leverage": QCfg.LEVERAGE(),
            }
            if p is not None and not getattr(p, "is_flat", lambda: True)():
                ctx.update({
                    "position_side": getattr(p, "side", ""),
                    "entry": getattr(p, "entry_price", 0.0),
                    "sl": getattr(p, "sl_price", 0.0),
                    "tp": getattr(p, "tp_price", 0.0),
                    "qty": getattr(p, "quantity", 0.0),
                })
            return ctx
        except Exception:
            return {}

    def _send_telegram(self, message: str, parse_mode: str = "HTML", *, event_type: str = None, **kwargs) -> bool:
        """Asset-aware Telegram send wrapper for this strategy instance."""
        try:
            ctx = self._telegram_context()
            extra_ctx = kwargs.pop("context", None)
            if isinstance(extra_ctx, dict):
                ctx.update(extra_ctx)
            return send_telegram_message(
                message,
                parse_mode=parse_mode,
                instrument=getattr(self, "_instrument", None),
                event_type=event_type,
                context=ctx,
                **kwargs,
            )
        except Exception:
            return send_telegram_message(message, parse_mode=parse_mode)

'''
if marker in qt:
    qt=qt.replace(marker, method+marker)
# Replace send_telegram_message calls inside QuantStrategy body (not import)
qt_lines=[]
for line in qt.splitlines():
    if 'from telegram.notifier import send_telegram_message as _stm' in line:
        # remove alias import
        continue
    if 'send_telegram_message(' in line and 'from telegram.notifier import send_telegram_message' not in line:
        line=line.replace('send_telegram_message(', 'self._send_telegram(')
    if '_stm(' in line:
        line=line.replace('_stm(', 'self._send_telegram(')
    qt_lines.append(line)
qt='\n'.join(qt_lines)+'\n'
# pass instrument to format_periodic_report
qt=qt.replace('''            direction_ps_analysis=direction_ps_analysis,\n        )''', '''            direction_ps_analysis=direction_ps_analysis,\n            instrument=getattr(self, "_instrument", None),\n        )''')
p.write_text(qt)

# Patch multi_asset_bot startup and assets report maybe more Telegram clean.
p=root/'orchestration/multi_asset_bot.py'
mt=p.read_text()
# Add portfolio summary method if absent? Simpler enhance startup_message lines.
mt=mt.replace('''        lines = ["⚡ <b>MULTI-ASSET INSTITUTIONAL BOT STARTED</b>", ""]\n        lines.append("<b>Execution universe:</b>")''', '''        lines = ["🏛 <b>PORTFOLIO COMMAND CENTER ONLINE</b>", ""]\n        lines.append("<b>Execution universe — asset-scoped strategy desks:</b>")''')
mt=mt.replace('''            lines.append(f"• <b>{inst.asset_id}</b> — primary {inst.primary_exchange.value.upper()} {inst.display_symbol}; venues: {venues}; leverage target {lev}x; policy {pol.asset_class} risk×{pol.risk_multiplier:.2f} margin {pol.margin_pct:.0%}")''', '''            lines.append(f"• <b>{inst.asset_id}</b> — {inst.primary_exchange.value.upper()}:{inst.display_symbol} | venues {venues} | lev {lev}x | {pol.asset_class} | risk×{pol.risk_multiplier:.2f} | margin {pol.margin_pct:.0%} | cadence {pol.evaluation_interval_sec:.2f}s")''')
p.write_text(mt)

# Add tests
pt=root/'tests/test_asset_notifications_v10.py'
pt.write_text(r'''
import unittest
from core.instruments import AssetClass, ExchangeName, ExchangeInstrument, TradableInstrument, instrument_scope
from telegram.notifier import _tg_enrich_asset_message, format_periodic_report


def _inst(asset="AAPL", sym="AAPLXUSD", cls=AssetClass.EQUITY):
    ei = ExchangeInstrument(
        exchange=ExchangeName.DELTA,
        symbol=sym,
        ws_symbol=sym,
        display_symbol=sym,
        asset_id=asset,
        asset_class=cls,
        max_leverage=25,
    )
    return TradableInstrument(
        asset_id=asset,
        display_name=f"{asset} xStock",
        asset_class=cls,
        primary_exchange=ExchangeName.DELTA,
        by_exchange={ExchangeName.DELTA: ei},
    )


class AssetNotificationTests(unittest.TestCase):
    def test_asset_header_is_added(self):
        inst = _inst()
        msg = _tg_enrich_asset_message("🧠 <b>POSTERIOR ACCEPTED</b>", instrument=inst, event_type="posterior", context={"state":"SCANNING", "price":279.0})
        self.assertIn("AAPL", msg)
        self.assertIn("DELTA:AAPLXUSD", msg)
        self.assertIn("POSTERIOR", msg)
        self.assertIn("SCANNING", msg)

    def test_periodic_report_uses_asset_not_btc(self):
        inst = _inst("NVDA", "NVDAXUSD")
        with instrument_scope(inst):
            msg = format_periodic_report(current_price=198.0, atr=1.2, instrument=inst)
        self.assertIn("NVDA", msg)
        self.assertIn("NVDAXUSD", msg)
        self.assertNotIn("<code>BTC", msg)

    def test_already_scoped_message_not_double_wrapped(self):
        inst = _inst()
        raw = "🏛 <b>POSTERIOR</b>  <code>AAPL</code>\nbody"
        msg = _tg_enrich_asset_message(raw, instrument=inst, event_type="posterior")
        self.assertEqual(raw, msg)

if __name__ == "__main__":
    unittest.main()
''')

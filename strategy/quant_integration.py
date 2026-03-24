"""
quant_integration.py — Drop-in QuantStrategy replacement v1.1
==============================================================
ZERO references to old quant_strategy.py. Fully standalone.

BUG FIXES: QI1-QI12 (see MIGRATION_GUIDE.md)
ADDED: _trade_history, close_position, set_trail_override,
       format_status_report, 4-arg on_tick compat
"""

from __future__ import annotations
import logging, math, time, threading
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from enum import Enum, auto

import sys, os as _os
sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
import config

from telegram.notifier import send_telegram_message
from execution.order_manager import CancelResult

try:
    from strategy.quant_engine import QuantEngine, Prediction
except ImportError:
    from quant_engine import QuantEngine, Prediction
try:
    from strategy.structural_trail import StructuralTrail
except ImportError:
    from structural_trail import StructuralTrail
try:
    from strategy.ict_engine import ICTEngine
    _ICT = True
except ImportError:
    _ICT = False
try:
    from strategy.fee_engine import ExecutionCostEngine
except ImportError:
    ExecutionCostEngine = None

logger = logging.getLogger(__name__)

def _cfg(n, d):
    v = getattr(config, n, None); return d if v is None else v

class QCfg:
    @staticmethod
    def SYMBOL(): return str(config.SYMBOL)
    @staticmethod
    def EXCHANGE(): return str(config.EXCHANGE)
    @staticmethod
    def LEVERAGE(): return int(_cfg("LEVERAGE",30))
    @staticmethod
    def MARGIN_PCT(): return float(_cfg("QUANT_MARGIN_PCT",0.20))
    @staticmethod
    def LOT_STEP(): return float(_cfg("LOT_STEP_SIZE",0.001))
    @staticmethod
    def MIN_QTY(): return float(_cfg("MIN_POSITION_SIZE",0.001))
    @staticmethod
    def MAX_QTY(): return float(_cfg("MAX_POSITION_SIZE",1.0))
    @staticmethod
    def MIN_MARGIN_USDT(): return float(_cfg("MIN_MARGIN_PER_TRADE",4.0))
    @staticmethod
    def COMMISSION_RATE(): return float(_cfg("COMMISSION_RATE",0.00055))
    @staticmethod
    def TICK_SIZE(): return float(_cfg("TICK_SIZE",0.1))
    @staticmethod
    def ATR_PERIOD(): return int(_cfg("SL_ATR_PERIOD",14))
    @staticmethod
    def COOLDOWN_SEC(): return int(_cfg("QUANT_COOLDOWN_SEC",180))
    @staticmethod
    def LOSS_LOCKOUT_SEC(): return int(_cfg("QUANT_LOSS_LOCKOUT_SEC",3600))
    @staticmethod
    def MAX_DAILY_TRADES(): return int(_cfg("MAX_DAILY_TRADES",8))
    @staticmethod
    def MAX_CONSEC_LOSSES(): return int(_cfg("MAX_CONSECUTIVE_LOSSES",3))
    @staticmethod
    def TRAIL_INTERVAL_S(): return int(_cfg("TRAILING_SL_CHECK_INTERVAL",10))
    @staticmethod
    def MIN_SL_PCT(): return float(_cfg("MIN_SL_DISTANCE_PCT",0.003))
    @staticmethod
    def MAX_SL_PCT(): return float(_cfg("MAX_SL_DISTANCE_PCT",0.035))
    @staticmethod
    def MIN_RR_RATIO(): return float(_cfg("MIN_RISK_REWARD_RATIO",1.5))
    @staticmethod
    def POS_SYNC_SEC(): return float(_cfg("QUANT_POS_SYNC_SEC",30))
    @staticmethod
    def SPREAD_ATR_MAX(): return float(_cfg("QUANT_SPREAD_ATR_MAX",0.50))
    @staticmethod
    def MIN_CONFIDENCE(): return float(_cfg("QUANT_MIN_CONFIDENCE",0.55))
    @staticmethod
    def CONFIRM_TICKS(): return int(_cfg("QUANT_CONFIRM_TICKS",3))
    @staticmethod
    def HIGH_CONF_THRESH(): return float(_cfg("QUANT_HIGH_CONF_THRESH",0.72))
    @staticmethod
    def ADX_PERIOD(): return int(_cfg("QUANT_ADX_PERIOD",14))

class ATREngine:
    def __init__(self, period=14):
        self._p=period; self.atr=0.0; self._hist=deque(maxlen=200); self._seeded=False; self._last_ts=-1
    def compute(self, candles):
        if len(candles)<self._p+1: return self.atr
        lt=int(candles[-2].get("t",0)) if len(candles)>=2 else 0
        if lt==self._last_ts and self._seeded: return self.atr
        trs=[]
        for i in range(1,len(candles)):
            h=float(candles[i]["h"]); l=float(candles[i]["l"]); pc=float(candles[i-1]["c"])
            trs.append(max(h-l,abs(h-pc),abs(l-pc)))
        if not self._seeded and len(trs)>=self._p:
            self.atr=sum(trs[:self._p])/self._p
            for tr in trs[self._p:]: self.atr=(self.atr*(self._p-1)+tr)/self._p
            self._seeded=True
        elif self._seeded and trs: self.atr=(self.atr*(self._p-1)+trs[-1])/self._p
        if self._seeded: self._hist.append(self.atr); self._last_ts=lt
        return self.atr
    def get_percentile(self):
        if len(self._hist)<15: return 0.5
        return sum(1 for v in self._hist if v<=self.atr)/len(self._hist)
    def reset_state(self): self._seeded=False; self._last_ts=-1

class ADXEngine:
    def __init__(self, period=14):
        self._p=period; self.adx=0.0; self._seeded=False; self._last_ts=-1
        self._sp=self._sm=self._st=0.0; self._pdi=self._mdi=0.0
    def compute(self, candles):
        if len(candles)<self._p*2+2: return self.adx
        lt=int(candles[-2].get("t",0)) if len(candles)>=2 else 0
        if lt==self._last_ts and self._seeded: return self.adx
        if not self._seeded:
            closed=candles[:-1]; pdms=[]; mdms=[]; trs=[]
            for i in range(1,len(closed)):
                h=float(closed[i]["h"]); l=float(closed[i]["l"])
                ph=float(closed[i-1]["h"]); pl=float(closed[i-1]["l"]); pc=float(closed[i-1]["c"])
                up=h-ph; dn=pl-l
                pdms.append(up if up>dn and up>0 else 0); mdms.append(dn if dn>up and dn>0 else 0)
                trs.append(max(h-l,abs(h-pc),abs(l-pc)))
            if len(pdms)<self._p*2: return self.adx
            sp=sum(pdms[:self._p]); sm=sum(mdms[:self._p]); st=sum(trs[:self._p]); dxs=[]
            for i in range(self._p,len(pdms)):
                sp=sp-sp/self._p+pdms[i]; sm=sm-sm/self._p+mdms[i]; st=st-st/self._p+trs[i]
                if st<1e-10: continue
                pdi=100*sp/st; mdi=100*sm/st; den=pdi+mdi
                dxs.append(100*abs(pdi-mdi)/den if den>1e-10 else 0)
                self._pdi=pdi; self._mdi=mdi
            self._sp=sp; self._sm=sm; self._st=st
            if dxs:
                n=min(self._p,len(dxs)); a=sum(dxs[:n])/n
                for dx in dxs[n:]: a=(a*(self._p-1)+dx)/self._p
                self.adx=a; self._seeded=True
        elif len(candles)>=3:
            h=float(candles[-2]["h"]); l=float(candles[-2]["l"])
            ph=float(candles[-3]["h"]); pl=float(candles[-3]["l"]); pc=float(candles[-3]["c"])
            up=h-ph; dn=pl-l
            pdm=up if up>dn and up>0 else 0; mdm=dn if dn>up and dn>0 else 0
            tr=max(h-l,abs(h-pc),abs(l-pc))
            self._sp=self._sp-self._sp/self._p+pdm; self._sm=self._sm-self._sm/self._p+mdm
            self._st=self._st-self._st/self._p+tr
            if self._st>1e-10:
                self._pdi=100*self._sp/self._st; self._mdi=100*self._sm/self._st
                den=self._pdi+self._mdi
                dx=100*abs(self._pdi-self._mdi)/den if den>1e-10 else 0
                self.adx=(self.adx*(self._p-1)+dx)/self._p
        self._last_ts=lt; return self.adx

class PositionPhase(Enum):
    FLAT=auto(); ENTERING=auto(); ACTIVE=auto(); EXITING=auto()

@dataclass
class PositionState:
    phase:PositionPhase=PositionPhase.FLAT; side:str=""; quantity:float=0.0
    entry_price:float=0.0; sl_price:float=0.0; tp_price:float=0.0
    sl_order_id:Optional[str]=None; tp_order_id:Optional[str]=None
    entry_time:float=0.0; initial_sl_dist:float=0.0
    trail_active:bool=False; last_trail_time:float=0.0
    peak_profit:float=0.0; entry_atr:float=0.0; entry_vol:float=0.0
    peak_price_abs:float=0.0; trade_mode:str="quant"
    entry_fill_type:str="taker"; entry_fee_paid:float=0.0
    def is_active(self): return self.phase==PositionPhase.ACTIVE
    def is_flat(self): return self.phase==PositionPhase.FLAT
    def to_dict(self):
        return {"side":self.side,"quantity":self.quantity,"entry_price":self.entry_price,
                "sl_price":self.sl_price,"tp_price":self.tp_price}

class RiskGate:
    def __init__(self):
        self.daily_trades=0; self.consec_losses=0; self.daily_pnl=0.0
        self._last_reset=None; self._last_loss_time=0.0; self._reset_daily()
    def _reset_daily(self):
        from datetime import date
        today=date.today()
        if self._last_reset!=today: self.daily_trades=0; self.daily_pnl=0.0; self._last_reset=today
    def allows_entry(self):
        self._reset_daily()
        if self.daily_trades>=QCfg.MAX_DAILY_TRADES(): return False,"daily_limit"
        if self.consec_losses>=QCfg.MAX_CONSEC_LOSSES():
            el=time.time()-self._last_loss_time
            if el<QCfg.LOSS_LOCKOUT_SEC(): return False,f"loss_lockout({int(QCfg.LOSS_LOCKOUT_SEC()-el)}s)"
            else: self.consec_losses=0
        return True,"ok"
    def record_trade_start(self): self.daily_trades+=1
    def record_trade_result(self, pnl):
        self.daily_pnl+=pnl
        if pnl<0: self.consec_losses+=1; self._last_loss_time=time.time()
        else: self.consec_losses=0

# ═══════════════ MAIN STRATEGY ═══════════════
class QuantStrategy:
    def __init__(self, order_manager_or_router):
        self._lock=threading.RLock()
        self._quant=QuantEngine(); self._atr_5m=ATREngine(QCfg.ATR_PERIOD())
        self._atr_1m=ATREngine(QCfg.ATR_PERIOD()); self._adx=ADXEngine(QCfg.ADX_PERIOD())
        self._ict=None
        if _ICT:
            try: self._ict=ICTEngine(); logger.info("✅ ICT loaded (SL/TP only)")
            except Exception as e: logger.warning(f"ICT failed: {e}")
        self._fee_engine=None
        if ExecutionCostEngine:
            try: self._fee_engine=ExecutionCostEngine()
            except: pass
        self._pos=PositionState(); self._risk_gate=RiskGate()
        self.current_sl_price=self.current_tp_price=0.0
        self._confirm_long=self._confirm_short=0
        self._last_exit_time=self._last_known_price=0.0
        self._last_data_warn=self._last_thinking_log=self._last_fed_trade_ts=0.0
        self._last_candle_ts={"1m":0,"5m":0,"15m":0}
        self._total_trades=self._total_wins=0; self._total_pnl=0.0
        self._trade_history: List[Dict] = []  # for /trades command
        self._trail_in_progress=False; self._pos_sync_in_progress=False
        self._entering_since=self._exiting_since=0.0
        self._last_pos_sync=self._last_exit_sync=0.0
        self._reconcile_pending=False; self._reconcile_data=None
        self._last_reconcile_time=0.0; self._RECONCILE_SEC=60.0
        self._exit_sync_in_progress=False; self._last_tp_gate_rejection=0.0
        self._last_exit_side=""
        logger.info("✅ QuantStrategy v2.0 (Predictive) initialized")

    # ═══════ PUBLIC API ═══════
    def get_position(self):
        with self._lock:
            if self._pos.is_flat(): return None
            return self._pos.to_dict()
    def get_stats(self):
        with self._lock:
            return {"total_trades":self._total_trades,"total_wins":self._total_wins,
                "total_pnl":self._total_pnl,"win_rate":self._win_rate(),
                "daily_trades":self._risk_gate.daily_trades,"current_phase":self._pos.phase.name,
                "consec_losses":self._risk_gate.consec_losses}
    def _win_rate(self): return self._total_wins/self._total_trades if self._total_trades>0 else 0.0
    def get_trail_enabled(self): return True
    def set_trail_override(self, val): pass  # trail always on
    def format_status_report(self): return self.get_status_text()  # alias for main.py
    def close_position(self, om, reason="manual_close"):
        with self._lock:
            if self._pos.phase!=PositionPhase.ACTIVE: return False,"No active position"
        price=self._last_known_price; self._exit_trade(om,price,reason)
        return True,f"Closed at ~${price:,.2f}"

    # ═══════ MAIN TICK (accepts 3 or 4 args for backward compat) ═══════
    def on_tick(self, data_manager, order_manager, risk_manager, now_ms=None):
        now=time.time()
        with self._lock:
            self._feed_microstructure(data_manager)
            try:
                p=data_manager.get_last_price()
                if p>1: self._last_known_price=p
            except: pass
            if self._reconcile_data is not None:
                d=self._reconcile_data; self._reconcile_data=None
                self._reconcile_apply(order_manager,d)
            if not self._reconcile_pending and now-self._last_reconcile_time>=self._RECONCILE_SEC:
                self._last_reconcile_time=now; self._reconcile_pending=True
                threading.Thread(target=self._reconcile_query,args=(order_manager,),daemon=True).start()
            phase=self._pos.phase; cd_ok=now-self._last_exit_time>=QCfg.COOLDOWN_SEC()

        if phase==PositionPhase.ACTIVE:
            if not self._pos_sync_in_progress and now-self._last_pos_sync>QCfg.POS_SYNC_SEC():
                self._pos_sync_in_progress=True; self._last_pos_sync=now
                def _s():
                    try: self._sync_position(order_manager)
                    except: pass
                    finally: self._pos_sync_in_progress=False
                threading.Thread(target=_s,daemon=True,name="pos-sync").start()
            self._manage_active(data_manager,order_manager,now)
        elif phase==PositionPhase.ENTERING:
            if now-self._entering_since>75:
                with self._lock:
                    if self._pos.phase==PositionPhase.ENTERING:
                        self._pos.phase=PositionPhase.FLAT; self._last_exit_time=now
                        send_telegram_message("⚠️ <b>ENTERING TIMEOUT</b>\nCheck exchange!")
        elif phase==PositionPhase.EXITING:
            if not self._exit_sync_in_progress and now-self._last_exit_sync>QCfg.POS_SYNC_SEC():
                self._exit_sync_in_progress=True; self._last_exit_sync=now
                def _es():
                    try: self._sync_position(order_manager)
                    except: pass
                    finally: self._exit_sync_in_progress=False
                threading.Thread(target=_es,daemon=True).start()
            if now-self._exiting_since>120:
                with self._lock: self._finalise_exit()
        elif phase==PositionPhase.FLAT and cd_ok:
            self._evaluate_entry(data_manager,order_manager,risk_manager,now)

    def _feed_microstructure(self, dm):
        try:
            ob=dm.get_orderbook(); price=dm.get_last_price()
            if ob and price>1:
                self._quant.on_orderbook(ob.get("bids",[]),ob.get("asks",[]),price)
                if self._fee_engine:
                    try: self._fee_engine.on_orderbook(ob,price)
                    except: pass
        except: pass
        try:
            trades=dm.get_recent_trades_raw(); co=self._last_fed_trade_ts; mx=co
            for t in trades:
                ts=t.get("timestamp",0)
                if ts>co:
                    _p=t.get("price",0); _q=t.get("quantity",0); _b=t.get("side")=="buy"
                    if _p>0 and _q>0: self._quant.on_trade(_p,_q,_b,ts)
                    if ts>mx: mx=ts
            if mx>co: self._last_fed_trade_ts=mx
        except: pass
        for tf in ("1m","5m","15m"):
            try:
                cs=dm.get_candles(tf,limit=5)
                if cs and len(cs)>=2:
                    c=cs[-2]; ts=int(c.get("t",0))
                    if ts>self._last_candle_ts.get(tf,0):
                        self._last_candle_ts[tf]=ts; self._quant.on_candle(tf,c)
            except: pass

    def _evaluate_entry(self, dm, om, rm, now):
        c5m=dm.get_candles("5m",limit=100); c1m=dm.get_candles("1m",limit=200)
        if len(c5m)<30 or len(c1m)<60:
            if now-self._last_data_warn>=30: self._last_data_warn=now
            self._confirm_long=self._confirm_short=0; return
        atr=self._atr_5m.compute(c5m); self._atr_1m.compute(c1m); self._adx.compute(c5m)
        if atr<1e-10: return
        price=dm.get_last_price()
        if price<1: return
        if not self._quant.is_warmed:
            if now-self._last_data_warn>=30: self._last_data_warn=now; logger.info(f"⏳ Warming: {self._quant.warmup_status}")
            return
        # Spread gate
        try:
            ob=dm.get_orderbook()
            if ob:
                bids=ob.get("bids",[]); asks=ob.get("asks",[])
                if bids and asks:
                    bb=float(bids[0][0]) if isinstance(bids[0],(list,tuple)) else float(bids[0].get("limit_price",0))
                    ba=float(asks[0][0]) if isinstance(asks[0],(list,tuple)) else float(asks[0].get("limit_price",0))
                    if atr>0 and (ba-bb)/atr>QCfg.SPREAD_ATR_MAX():
                        self._confirm_long=self._confirm_short=0; return
        except: pass
        ok,reason=self._risk_gate.allows_entry()
        if not ok: self._confirm_long=self._confirm_short=0; return
        pred=self._quant.predict(price,atr)
        if self._ict:
            try:
                c15m=dm.get_candles("15m",limit=60); c1h=dm.get_candles("1h",limit=100)
                c4h=dm.get_candles("4h",limit=50); c1d=dm.get_candles("1d",limit=30)
                self._ict.update(c5m,c15m,price,int(now*1000),candles_1m=c1m,candles_1h=c1h,candles_4h=c4h,candles_1d=c1d)
            except: pass
        if now-self._last_thinking_log>=8:
            self._last_thinking_log=now; self._log_thinking(pred,price,atr)
        if not pred.is_actionable(QCfg.MIN_CONFIDENCE()):
            self._confirm_long=self._confirm_short=0; return
        side=pred.direction; req=QCfg.CONFIRM_TICKS()
        if pred.confidence>=QCfg.HIGH_CONF_THRESH(): req=max(1,req-1)
        if side=="long":
            self._confirm_long+=1; self._confirm_short=0
            if self._confirm_long<req: return
        else:
            self._confirm_short+=1; self._confirm_long=0
            if self._confirm_short<req: return
        self._confirm_long=self._confirm_short=0
        self._launch_entry(dm,om,rm,side,pred,price,atr,now)

    def _log_thinking(self, pred, price, atr):
        s=pred.signals
        lines=[f"┌── 🧠 PREDICT ${price:,.2f} ATR={atr:.1f} ADX={self._adx.adx:.1f}",
            f"  {pred.direction.upper()} conf={pred.confidence:.3f} α={pred.alpha_bps:+.1f}bps {pred.regime}",
            f"  VPIN={pred.vpin:.3f} OB={s.get('ob_comp',0):+.3f} Kyle={s.get('kyle_sig',0):+.3f}",
            f"  Mom: align={s.get('mom_align',0):+.3f} lead={s.get('mom_lead',0):+.3f} raw={s.get('raw',0):+.4f}",
            f"└{'─'*50}"]
        logger.info("\n"+"\n".join(lines))

    def _compute_sl_tp(self, dm, price, side, atr):
        sl=tp=None; src="fb"; now_ms=int(time.time()*1000)
        min_d=max(price*QCfg.MIN_SL_PCT(),0.5*atr); max_d=min(price*QCfg.MAX_SL_PCT(),3.5*atr)
        if self._ict and getattr(self._ict,'_initialized',False):
            try:
                osl=self._ict.get_ob_sl_level(side,price,atr,now_ms,htf_only=True)
                if osl:
                    d=abs(price-osl)
                    if min_d<=d<=max_d: sl=osl; src="ICT_OB"
            except: pass
        if sl is None:
            try:
                c5=dm.get_candles("5m",limit=20)
                if len(c5)>=8:
                    closed=c5[:-1]; buf=0.3*atr; hs=[]; ls=[]
                    for i in range(2,len(closed)-2):
                        h=float(closed[i]["h"]); l=float(closed[i]["l"])
                        if all(h>=float(closed[j]["h"]) for j in range(i-2,i+3) if j!=i): hs.append(h)
                        if all(l<=float(closed[j]["l"]) for j in range(i-2,i+3) if j!=i): ls.append(l)
                    if side=="long" and ls:
                        v=[l for l in ls if min_d<=(price-l+buf)<=max_d]
                        if v: sl=max(v)-buf; src="swing"
                    elif side=="short" and hs:
                        v=[h for h in hs if min_d<=(h+buf-price)<=max_d]
                        if v: sl=min(v)+buf; src="swing"
            except: pass
        if sl is None:
            sl=price-1.5*atr if side=="long" else price+1.5*atr; src="ATR"
        sd=abs(price-sl)
        if self._ict and getattr(self._ict,'_initialized',False):
            try:
                tgts=self._ict.get_structural_tp_targets(side,price,atr,now_ms,
                    min_dist=sd*QCfg.MIN_RR_RATIO(),max_dist=sd*4)
                if tgts: best=max(tgts,key=lambda t:t[1]); tp=best[0]; src+=f"+ICT({best[2]})"
            except: pass
        if tp is None:
            rr=max(QCfg.MIN_RR_RATIO(),2.0)
            tp=price+sd*rr if side=="long" else price-sd*rr; src+="+RR"
        if sd>0 and abs(tp-price)/sd<QCfg.MIN_RR_RATIO(): return None,None,"rr_fail"
        return sl,tp,src

    def _launch_entry(self, dm, om, rm, side, pred, price, atr, now):
        with self._lock: self._pos.phase=PositionPhase.ENTERING; self._entering_since=now
        def _bg():
            try: self._enter_trade(dm,om,rm,side,pred,price,atr)
            except Exception as e: logger.error(f"Entry error: {e}",exc_info=True)
            finally:
                with self._lock:
                    if self._pos.phase==PositionPhase.ENTERING:
                        gr=time.time()-self._last_tp_gate_rejection<5
                        if not gr: self._last_exit_time=time.time()
                        self._pos.phase=PositionPhase.FLAT
        threading.Thread(target=_bg,daemon=True,name=f"enter-{side}").start()

    def _enter_trade(self, dm, om, rm, side, pred, price, atr):
        sl,tp,src=self._compute_sl_tp(dm,price,side,atr)
        if sl is None: self._last_tp_gate_rejection=time.time(); return
        bal=rm.get_available_balance()
        if not bal or bal.get("error"): return
        avail=float(bal.get("available",0)); margin=avail*QCfg.MARGIN_PCT()
        if margin<QCfg.MIN_MARGIN_USDT(): return
        qty=margin*QCfg.LEVERAGE()/price if price>0 else 0
        ls=QCfg.LOT_STEP(); qty=math.floor(qty/ls)*ls; qty=max(QCfg.MIN_QTY(),min(qty,QCfg.MAX_QTY()))
        if qty<QCfg.MIN_QTY(): return
        logger.info(f"🎯 ENTERING {side.upper()} conf={pred.confidence:.3f} SL=${sl:,.2f} TP=${tp:,.2f}")
        # Try bracket, fallback to separate
        entry_data=None; is_bracket=False
        try:
            logger.info(f"[BRACKET] Attempting {('buy' if side=='long' else 'sell').upper()} "
                        f"qty={qty} @ ${price:,.2f} SL=${sl:,.2f} TP=${tp:,.2f}")
            entry_data=om.place_bracket_limit_entry(side="buy" if side=="long" else "sell",
                quantity=qty,limit_price=price,sl_price=sl,tp_price=tp)
            if entry_data and entry_data.get("order_id"):
                is_bracket=True
                logger.info(f"✅ Bracket accepted: {entry_data.get('order_id')}")
            else:
                logger.warning("⚠️ Bracket returned no order_id — fallback to market+SL/TP")
        except (AttributeError,TypeError) as e:
            logger.warning(f"⚠️ Bracket unsupported ({e}) — fallback to market+SL/TP")
        if not is_bracket:
            entry_data=om.place_market_order(side="buy" if side=="long" else "sell",quantity=qty)
        if not entry_data or not entry_data.get("order_id"): return
        order_id = entry_data["order_id"]
        logger.info(f"⏳ Polling fill for order {order_id} (is_bracket={is_bracket})...")

        # ── Fill price extraction ──────────────────────────────────────────────
        # Bracket: fill_price comes back from place_bracket_limit_entry directly.
        # Market: poll get_fill_details up to 8s; guard against stale/wrong fields.
        fill_price = price  # fallback to signal price
        PRICE_SANITY = 0.05  # reject fill if >5% away from signal price (bad field)
        if is_bracket:
            fp = entry_data.get("fill_price", 0)
            if fp and fp > 1 and abs(fp - price) / price < PRICE_SANITY:
                fill_price = fp
                logger.info(f"✅ Bracket fill price: ${fill_price:,.2f}")
            else:
                logger.warning(f"⚠️ Bracket fill_price={fp} looks wrong vs signal={price:.2f} — "
                                f"querying exchange directly")
                try:
                    details = om.get_fill_details(order_id)
                    if details:
                        fp2 = float(details.get("fill_price") or details.get("average_fill_price") or 0)
                        if fp2 > 1 and abs(fp2 - price) / price < PRICE_SANITY:
                            fill_price = fp2
                            logger.info(f"✅ Fill price from exchange query: ${fill_price:,.2f}")
                        else:
                            logger.warning(f"⚠️ Exchange fill_price={fp2} also suspect — "
                                           f"using signal price ${price:,.2f}")
                except Exception as e:
                    logger.warning(f"Fill query failed: {e} — using signal price")
        else:
            try:
                for attempt in range(8):
                    time.sleep(1.0)
                    details = om.get_fill_details(order_id)
                    if details:
                        status = details.get("status","")
                        fp = float(details.get("fill_price") or details.get("average_fill_price") or 0)
                        logger.info(f"  Fill poll {attempt+1}/8: status={status} fill_price={fp}")
                        if fp > 1 and abs(fp - price) / price < PRICE_SANITY:
                            fill_price = fp; break
                        elif fp > 1:
                            logger.warning(f"  fill_price={fp} is >{PRICE_SANITY*100:.0f}% from "
                                           f"signal {price:.2f} — ignoring (bad field)")
            except Exception as e:
                logger.warning(f"Fill poll error: {e}")
        logger.info(f"✅ Using fill_price=${fill_price:,.2f} "
                    f"(signal=${price:,.2f} diff={fill_price-price:+.2f})")

        # ── SL/TP order IDs ───────────────────────────────────────────────────
        exit_side="sell" if side=="long" else "buy"
        sl_oid=tp_oid=None
        if is_bracket:
            sl_oid=entry_data.get("bracket_sl_order_id",""); tp_oid=entry_data.get("bracket_tp_order_id","")
            bsl=entry_data.get("bracket_sl_price",0); btp=entry_data.get("bracket_tp_price",0)
            if bsl>0: sl=bsl
            if btp>0: tp=btp
            logger.info(f"  Bracket SL order: {sl_oid or 'pending'} @ ${sl:,.2f}")
            logger.info(f"  Bracket TP order: {tp_oid or 'pending'} @ ${tp:,.2f}")
        else:
            logger.info(f"  Placing separate SL @ ${sl:,.2f}...")
            sld=om.place_stop_loss(side=exit_side,quantity=qty,trigger_price=sl)
            if not sld:
                logger.error("❌ SL placement failed — emergency flat")
                om.place_market_order(side=exit_side,quantity=qty,reduce_only=True)
                self._last_exit_time=time.time(); return
            sl_oid=sld.get("order_id")
            logger.info(f"  ✅ SL placed: {sl_oid} @ ${sl:,.2f}")
            logger.info(f"  Placing separate TP @ ${tp:,.2f}...")
            tpd=om.place_take_profit(side=exit_side,quantity=qty,trigger_price=tp)
            if not tpd:
                logger.error("❌ TP placement failed — cancelling SL and emergency flat")
                om.cancel_order(sl_oid)
                om.place_market_order(side=exit_side,quantity=qty,reduce_only=True)
                self._last_exit_time=time.time(); return
            tp_oid=tpd.get("order_id")
            logger.info(f"  ✅ TP placed: {tp_oid} @ ${tp:,.2f}")

        sdf=abs(fill_price-sl)
        with self._lock:
            self._pos=PositionState(phase=PositionPhase.ACTIVE,side=side,quantity=qty,entry_price=fill_price,
                sl_price=sl,tp_price=tp,sl_order_id=sl_oid,tp_order_id=tp_oid,entry_time=time.time(),
                initial_sl_dist=sdf,entry_atr=self._atr_5m.atr,peak_price_abs=fill_price)
            self.current_sl_price=sl; self.current_tp_price=tp
            self._confirm_long=self._confirm_short=0; self._risk_gate.record_trade_start()
        rr=abs(tp-fill_price)/sdf if sdf>0 else 0
        logger.info(f"🟢 POSITION OPEN {side.upper()}  fill=${fill_price:,.2f}  "
                    f"SL=${sl:,.2f}  TP=${tp:,.2f}  qty={qty}  R:R=1:{rr:.2f}  [{src}]")
        send_telegram_message(f"🎯 <b>ENTRY {side.upper()}</b>\nConf={pred.confidence:.3f} {pred.regime}\n"
            f"${fill_price:,.2f}  qty={qty}\nSL=${sl:,.2f}  TP=${tp:,.2f}\nR:R=1:{rr:.2f}  [{src}]")

    def _manage_active(self, dm, om, now):
        pos=self._pos; price=self._last_known_price
        if price<1 or not pos.is_active(): return
        profit=(price-pos.entry_price) if pos.side=="long" else (pos.entry_price-price)
        if profit>pos.peak_profit: pos.peak_profit=profit
        if pos.side=="long":
            if price>pos.peak_price_abs: pos.peak_price_abs=price
        else:
            if pos.peak_price_abs<1e-10 or price<pos.peak_price_abs: pos.peak_price_abs=price

        # ── Per-tick position status log (every 30s) ──────────────────────────
        if not hasattr(self,"_last_pos_log"): self._last_pos_log=0.0
        if now-self._last_pos_log>=30.0:
            self._last_pos_log=now
            id_=pos.initial_sl_dist if pos.initial_sl_dist>1e-10 else abs(pos.entry_price-pos.sl_price)
            r_now=profit/id_ if id_>0 else 0
            r_peak=pos.peak_profit/id_ if id_>0 else 0
            hm=(now-pos.entry_time)/60
            upnl=self._estimate_pnl(pos,price)
            sl_dist=abs(price-pos.sl_price); tp_dist=abs(pos.tp_price-price)
            trail_next=max(0,QCfg.TRAIL_INTERVAL_S()-(now-pos.last_trail_time))
            pred=self._quant.last_prediction
            logger.info(
                f"📍 {pos.side.upper()} {hm:.1f}m  "
                f"entry=${pos.entry_price:,.2f} now=${price:,.2f}  "
                f"uPnL=${upnl:+.2f}  R={r_now:+.2f} (peak={r_peak:.2f}R)\n"
                f"   SL=${pos.sl_price:,.2f} (dist=${sl_dist:.1f})  "
                f"TP=${pos.tp_price:,.2f} (dist=${tp_dist:.1f})  "
                f"trail={'✅ active' if pos.trail_active else '⏳ waiting'}  "
                f"next_check={trail_next:.0f}s\n"
                f"   pred={pred.direction.upper()} conf={pred.confidence:.3f} "
                f"{pred.regime}  VPIN={pred.vpin:.3f} ADX={self._adx.adx:.1f}"
            )

        # ── Trail check ───────────────────────────────────────────────────────
        if now-pos.last_trail_time>=QCfg.TRAIL_INTERVAL_S():
            self._pos.last_trail_time=now
            with self._lock:
                if self._trail_in_progress: return
                self._trail_in_progress=True
            def _bg():
                try:
                    lp=dm.get_last_price(); self._update_trail(om,dm,lp if lp>1 else price,time.time())
                except Exception as e: logger.error(f"Trail error: {e}",exc_info=True)
                finally: self._trail_in_progress=False
            threading.Thread(target=_bg,daemon=True,name="trail").start()

    def _update_trail(self, om, dm, price, now):
        pos=self._pos; atr=self._atr_5m.atr
        id_=pos.initial_sl_dist if pos.initial_sl_dist>1e-10 else abs(pos.entry_price-pos.sl_price)
        profit=(price-pos.entry_price) if pos.side=="long" else (pos.entry_price-price)
        r_now=profit/id_ if id_>0 else 0

        if atr<1e-10 or pos.entry_price<1:
            logger.debug("Trail skip: no ATR or entry"); return
        if not pos.sl_order_id:
            logger.warning("⚠️ Trail skip: no sl_order_id — bracket SL pending resolution"); return

        if profit>pos.peak_profit: pos.peak_profit=profit
        try: c1m=dm.get_candles("1m",limit=60)
        except: c1m=[]
        try: c5m=dm.get_candles("5m",limit=30)
        except: c5m=[]
        try: ob=dm.get_orderbook()
        except: ob={"bids":[],"asks":[]}
        if self._ict:
            try: self._ict.update(c5m,dm.get_candles("15m",limit=30),price,int(now*1000),candles_1m=c1m)
            except: pass
        hr=[]
        new_sl=StructuralTrail.compute(pos_side=pos.side,price=price,entry_price=pos.entry_price,
            current_sl=pos.sl_price,atr=atr,initial_sl_dist=pos.initial_sl_dist,
            peak_profit=pos.peak_profit,peak_price_abs=pos.peak_price_abs,hold_seconds=now-pos.entry_time,
            candles_1m=c1m,candles_5m=c5m,orderbook=ob,ict_engine=self._ict,
            tick_size=QCfg.TICK_SIZE(),atr_percentile=self._atr_5m.get_percentile(),
            adx=self._adx.adx,hold_reason=hr)
        rs=" | ".join(hr) if hr else "structural"
        if new_sl is None:
            logger.info(f"🔒 Trail HOLD [{rs}]  R={r_now:+.2f} (activation >0.40R)  "
                        f"SL=${pos.sl_price:,.2f}  price=${price:,.2f}  ATR={atr:.1f}")
            return
        nst=_rt(new_sl,QCfg.TICK_SIZE())
        if abs(nst-pos.sl_price)<1e-6:
            logger.info(f"🔒 Trail NO-MOVE [{rs}]  proposed=${nst:,.2f} == current  R={r_now:+.2f}")
            return
        old_sl=pos.sl_price
        rlvl=max(profit,pos.peak_profit)/id_ if id_>0 else 0
        move_dir="▲" if nst>old_sl else "▼"
        logger.info(f"🔒 Trail MOVE [{rs}]  ${old_sl:,.2f} {move_dir} ${nst:,.2f}  "
                    f"(Δ${abs(nst-old_sl):.1f})  R={rlvl:.2f}")
        es="sell" if pos.side=="long" else "buy"
        result=om.replace_stop_loss(existing_sl_order_id=pos.sl_order_id,side=es,
                                    quantity=pos.quantity,new_trigger_price=nst)
        if result is None:
            logger.warning("🔒 Trail: replace_stop_loss→None — SL likely fired, recording exit")
            self._record_exchange_exit(None); return
        if isinstance(result,dict) and "error" not in result:
            with self._lock:
                self._pos.sl_price=nst
                self._pos.sl_order_id=result.get("order_id",pos.sl_order_id)
                self.current_sl_price=nst
                if not pos.trail_active: self._pos.trail_active=True
            logger.info(f"✅ Trail confirmed on exchange: ${old_sl:,.2f} {move_dir} ${nst:,.2f}")
            send_telegram_message(f"🔒 <b>TRAIL</b> [{rs}]\n${old_sl:,.2f} {move_dir} ${nst:,.2f}  R={rlvl:.2f}R")
        else:
            err=(result or {}).get("error","unknown")
            logger.warning(f"⚠️ Trail replace failed ({err}) — SL stays at ${pos.sl_price:,.2f}")

    # ═══════ EXIT / PNL ═══════
    def _exit_trade(self, om, price, reason):
        pos=self._pos
        if pos.phase!=PositionPhase.ACTIVE: return
        self._pos.phase=PositionPhase.EXITING; self._exiting_since=time.time()
        om.cancel_all_exit_orders(sl_order_id=pos.sl_order_id,tp_order_id=pos.tp_order_id)
        es="sell" if pos.side=="long" else "buy"
        om.place_market_order(side=es,quantity=pos.quantity,reduce_only=True)
        pnl=self._estimate_pnl(pos,price)
        self._record_pnl(pnl,reason,price,pos)
        send_telegram_message(f"{'✅' if pnl>0 else '❌'} <b>EXIT</b> {reason}\n${pos.entry_price:,.2f}→${price:,.2f} PnL=${pnl:+.2f}")

    def _estimate_pnl(self, pos, price, **kw):
        raw=(price-pos.entry_price) if pos.side=="long" else (pos.entry_price-price)
        gross=raw*pos.quantity; fr=QCfg.COMMISSION_RATE()
        return gross - (pos.entry_price*pos.quantity*fr + price*pos.quantity*fr)

    def _record_pnl(self, pnl, reason, exit_price, pos=None):
        self._total_trades+=1; self._total_pnl+=pnl
        is_win = pnl>0
        if is_win: self._total_wins+=1
        self._risk_gate.record_trade_result(pnl)
        if pos is None: pos = self._pos
        id_ = pos.initial_sl_dist if pos.initial_sl_dist>1e-10 else abs(pos.entry_price-pos.sl_price)
        raw = (exit_price-pos.entry_price) if pos.side=="long" else (pos.entry_price-exit_price)
        ar = raw/id_ if id_>0 else 0
        self._trade_history.append({
            "side":pos.side,"entry":pos.entry_price,"exit":exit_price,
            "pnl":pnl,"reason":reason,"is_win":is_win,
            "hold_min":(time.time()-pos.entry_time)/60 if pos.entry_time>0 else 0,
            "achieved_r":ar,"regime":self._quant.last_prediction.regime,
            "timestamp":time.time()})

    def _finalise_exit(self):
        self._pos=PositionState(); self._last_exit_time=time.time()
        self.current_sl_price=self.current_tp_price=0.0

    def _record_exchange_exit(self, ex_pos):
        if self._pos.phase not in (PositionPhase.ACTIVE,PositionPhase.EXITING): return
        price=self._last_known_price; pnl=self._estimate_pnl(self._pos,price)
        self._record_pnl(pnl,"exchange_exit",price,self._pos); self._finalise_exit()

    # ═══════ RECONCILE ═══════
    def _reconcile_query(self, om):
        try:
            ex=om.get_open_position()
            if ex is None: return
            sz=float(ex.get("size",0)); oo=None
            if sz>=QCfg.MIN_QTY():
                try: oo=om.get_open_orders()
                except: pass
            with self._lock: self._reconcile_data={"ex_pos":ex,"open_orders":oo}
        except Exception as e: logger.warning(f"Reconcile: {e}")
        finally: self._reconcile_pending=False

    def _reconcile_apply(self, om, data):
        ex=data["ex_pos"]; oo=data.get("open_orders"); sz=float(ex.get("size",0)); ph=self._pos.phase
        if ph==PositionPhase.FLAT and sz>=QCfg.MIN_QTY():
            entry=float(ex.get("entry_price",0))
            if entry<1: return
            es=str(ex.get("side","")).upper(); iside="long" if es=="LONG" else "short"
            sl_p=tp_p=0.0; sl_oid=tp_oid=None
            if oo:
                for o in oo:
                    ot=(o.get("type") or (o.get("raw") or {}).get("order_type") or "").upper().replace(" ","_")
                    trig=float(o.get("trigger_price") or (o.get("raw") or {}).get("stop_price") or 0)
                    if "STOP" in ot and "PROFIT" not in ot and "TAKE" not in ot: sl_oid=o["order_id"]; sl_p=trig
                    elif "PROFIT" in ot or "TAKE_PROFIT" in ot: tp_oid=o["order_id"]; tp_p=trig
            self._pos=PositionState(phase=PositionPhase.ACTIVE,side=iside,quantity=sz,entry_price=entry,
                sl_price=sl_p,tp_price=tp_p,sl_order_id=sl_oid,tp_order_id=tp_oid,
                entry_time=time.time(),initial_sl_dist=abs(entry-sl_p) if sl_p>0 else 0,
                entry_atr=self._atr_5m.atr,peak_price_abs=entry)
            self.current_sl_price=sl_p; self.current_tp_price=tp_p
            logger.warning(f"⚡ ADOPTED {es} @ ${entry:,.2f}")
            send_telegram_message(f"⚡ <b>ADOPTED</b> {es} @ ${entry:,.2f}")
            return
        if ph==PositionPhase.ACTIVE and sz<QCfg.MIN_QTY():
            self._record_exchange_exit(ex)
        if ph==PositionPhase.EXITING and sz<QCfg.MIN_QTY():
            self._finalise_exit()

    def _sync_position(self, om):
        try: ex=om.get_open_position()
        except: return
        if ex is None: return
        sz=float(ex.get("size",0))
        if self._pos.phase in (PositionPhase.ACTIVE,PositionPhase.EXITING) and sz<QCfg.MIN_QTY():
            self._record_exchange_exit(ex)

    # ═══════ STATUS ═══════
    def get_status_text(self):
        pred=self._quant.last_prediction; vs=self._quant._vol.state; p=self._pos
        lines=[f"<b>🧠 Predictive Quant v2</b>",
            f"  {pred.direction.upper()} conf={pred.confidence:.3f} {pred.regime}",
            f"  VPIN={pred.vpin:.3f} α={pred.alpha_bps:+.1f}bps ADX={self._adx.adx:.1f}",
            f"  Vol={vs.regime} ({vs.atr_percentile:.0%})",
            "",f"<b>📊 Session</b>",
            f"  {self._total_trades}T WR={self._win_rate():.0%} PnL=${self._total_pnl:+.2f}",
            f"  Daily={self._risk_gate.daily_trades}/{QCfg.MAX_DAILY_TRADES()} ConsecL={self._risk_gate.consec_losses}"]
        if p.is_active():
            pr=self._last_known_price; raw=(pr-p.entry_price) if p.side=="long" else (p.entry_price-pr)
            id_=p.initial_sl_dist if p.initial_sl_dist>1e-10 else abs(p.entry_price-p.sl_price)
            cr=raw/id_ if id_>0 else 0; upnl=self._estimate_pnl(p,pr); hm=(time.time()-p.entry_time)/60
            lines+=[f"",f"<b>🟢 {p.side.upper()}</b> ${p.entry_price:,.2f}→${pr:,.2f}",
                f"  SL=${p.sl_price:,.2f} TP=${p.tp_price:,.2f}",
                f"  uPnL=<b>${upnl:+.2f}</b> R={cr:+.2f}R Trail={'✅' if p.trail_active else '⏳'} {hm:.0f}m"]
        return "\n".join(lines)

def _rt(price, tick=0.1):
    if tick<=0: return price
    return round(round(price/tick)*tick, 10)

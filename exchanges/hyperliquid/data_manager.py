"""Hyperliquid read-only data manager for secondary crypto-perp market data."""
from __future__ import annotations
import logging, threading, time
from collections import deque
from datetime import datetime, timezone
from typing import Dict, List, Optional
import sys, os; sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import config
from core.instruments import ExchangeName
from core.candle import Candle
from exchanges.hyperliquid.api import HyperliquidAPI
from exchanges.hyperliquid.websocket import HyperliquidWebSocket
logger=logging.getLogger(__name__)
class StreamStats:
    def __init__(self): self._last_update=None; self._ob_count=self._trade_count=self._candle_count=0; self._lock=threading.RLock()
    def record_orderbook(self):
        with self._lock: self._ob_count+=1; self._last_update=datetime.now(timezone.utc)
    def record_trade(self):
        with self._lock: self._trade_count+=1; self._last_update=datetime.now(timezone.utc)
    def record_candle(self):
        with self._lock: self._candle_count+=1; self._last_update=datetime.now(timezone.utc)
    def get_last_update(self):
        with self._lock: return self._last_update
class HyperliquidDataManager:
    _WARMUP_CONFIG={"1m":("1m",1,200,"_candles_1m"),"5m":("5m",5,200,"_candles_5m"),"15m":("15m",15,200,"_candles_15m"),"1h":("1h",60,100,"_candles_1h"),"4h":("4h",240,50,"_candles_4h"),"1d":("1d",1440,30,"_candles_1d")}
    _WARMUP_SLEEP=0.10
    def __init__(self,instrument=None):
        self.instrument=instrument; self.exchange_instrument=(instrument.by_exchange.get(ExchangeName.HYPERLIQUID) if instrument is not None and hasattr(instrument,"by_exchange") else None)
        self.symbol=(self.exchange_instrument.symbol if self.exchange_instrument is not None else getattr(config,"HYPERLIQUID_SYMBOL","BTC")); self.ws_symbol=(self.exchange_instrument.ws_symbol if self.exchange_instrument is not None else self.symbol)
        self.dex=(self.exchange_instrument.raw or {}).get("dex") if self.exchange_instrument is not None else None
        self.api=HyperliquidAPI(); self.ws=None; self.stats=StreamStats(); self._lock=threading.RLock(); self._recent_trades=deque(maxlen=500); self._orderbook={"bids":[],"asks":[]}; self._last_price=0.0; self._last_price_update_time=0.0; self._forming_ts={}; self._warmup_complete=False; self._strategy_ref=None; self.is_ready=False; self.is_streaming=False
        self._candles_1m=deque(maxlen=2000); self._candles_5m=deque(maxlen=1200); self._candles_15m=deque(maxlen=800); self._candles_1h=deque(maxlen=500); self._candles_4h=deque(maxlen=400); self._candles_1d=deque(maxlen=100)
        logger.info("HyperliquidDataManager initialised (%s)", self.symbol)
    def start(self):
        try:
            self.is_ready=self.is_streaming=False; coin=self.ws_symbol; logger.info("Hyperliquid DM[%s]: starting WebSocket...",coin); self.ws=HyperliquidWebSocket()
            if self.ws.connect(timeout=20):
                self.ws.subscribe_orderbook(coin,self._on_orderbook); self.ws.subscribe_trades(coin,self._on_trade)
                for label,(interval,_,_,_) in self._WARMUP_CONFIG.items(): self.ws.subscribe_candlestick(coin, interval=interval, callback=self._make_candle_cb(label))
                self.is_streaming=True; logger.info("✅ Hyperliquid WS streams subscribed for %s",coin)
            else: logger.warning("Hyperliquid WS failed for %s; REST warmup only",coin)
            for tf in ("1m","5m","15m","1h","4h","1d"): self._warmup_klines(tf); time.sleep(self._WARMUP_SLEEP)
            self._warmup_complete=True; self._seed_orderbook_from_rest(); self.is_ready=self._check_minimum_data(); logger.info("Hyperliquid DM[%s] ready=%s (1m=%d 5m=%d 15m=%d 4h=%d)",self.symbol,self.is_ready,len(self._candles_1m),len(self._candles_5m),len(self._candles_15m),len(self._candles_4h)); return bool(self.is_ready)
        except Exception as e: logger.error("Hyperliquid DM start error: %s",e,exc_info=True); self.is_ready=self.is_streaming=False; return False
    def stop(self):
        self.is_ready=self.is_streaming=False
        try:
            if self.ws: self.ws.disconnect()
        except Exception: pass
        logger.info("Hyperliquid DM stopped")
    def restart_streams(self): self.stop(); time.sleep(1.0); return self.start()
    def register_strategy(self,strategy): self._strategy_ref=strategy
    def wait_until_ready(self,timeout_sec=120.0):
        end=time.time()+float(timeout_sec)
        while time.time()<end and not self.is_ready: self.is_ready=self._check_minimum_data(); time.sleep(0.5 if not self.is_ready else 0)
        return bool(self.is_ready)
    def _warmup_klines(self,label,limit=0,retries=1):
        interval,minutes,default_limit,attr=self._WARMUP_CONFIG[label]; target=getattr(self,attr); limit=limit or default_limit; end_ms=int(time.time()*1000); start_ms=end_ms-limit*minutes*60*1000
        rows=self.api.get_candles(self.symbol,interval,start_ms,end_ms,dex=self.dex); seeded=0
        for k in sorted(rows,key=lambda x:int(x.get("t") or 0)):
            try:
                c=Candle(timestamp=float(k.get("T") or k.get("t") or 0)/1000.0, open=float(k.get("o") or 0), high=float(k.get("h") or 0), low=float(k.get("l") or 0), close=float(k.get("c") or 0), volume=float(k.get("v") or 0))
                if c.close>0: target.append(c); seeded+=1; self._last_price=c.close; self._last_price_update_time=time.time()
            except Exception: pass
        if seeded: logger.info("Hyperliquid warmup %s %s: %d candles", self.symbol,label,seeded)
    def _seed_orderbook_from_rest(self):
        try: self._on_orderbook(self.api.get_l2_book(self.symbol,dex=self.dex))
        except Exception: pass
    def _make_candle_cb(self,label):
        interval,_,_,attr=self._WARMUP_CONFIG[label]; target=getattr(self,attr)
        def cb(data):
            try:
                if str(data.get("i") or "")!=interval: return
                c=Candle(timestamp=float(data.get("t") or 0)/1000.0, open=float(data.get("o") or 0), high=float(data.get("h") or 0), low=float(data.get("l") or 0), close=float(data.get("c") or 0), volume=float(data.get("v") or 0))
                if c.close<=0: return
                with self._lock:
                    self._last_price=c.close; self._last_price_update_time=time.time(); target.append(c); self.stats.record_candle()
            except Exception as e: logger.debug("Hyperliquid %s candle callback error: %s",label,e)
        return cb
    def _on_orderbook(self,data):
        try:
            levels=data.get("levels") or [[],[]]; bids=[[float(x.get("px")),float(x.get("sz"))] for x in (levels[0] if len(levels)>0 else []) if float(x.get("px",0) or 0)>0]; asks=[[float(x.get("px")),float(x.get("sz"))] for x in (levels[1] if len(levels)>1 else []) if float(x.get("px",0) or 0)>0]
            with self._lock:
                self._orderbook={"bids":bids,"asks":asks}
                if bids and asks: self._last_price=(bids[0][0]+asks[0][0])/2.0; self._last_price_update_time=time.time()
                self.stats.record_orderbook()
        except Exception as e: logger.debug("Hyperliquid OB callback error: %s",e)
    def _on_trade(self,data):
        try:
            px=float(data.get("px") or data.get("price") or 0); qty=float(data.get("sz") or data.get("size") or data.get("quantity") or 0); side="buy" if str(data.get("side") or "").upper() in {"B","BUY"} else "sell"; ts=float(data.get("time") or int(time.time()*1000))/1000.0; cb=None
            with self._lock:
                if px>0: self._last_price=px; self._last_price_update_time=time.time(); self._recent_trades.append({"price":px,"quantity":qty,"side":side,"timestamp":ts,"source":"hyperliquid","venue":"hyperliquid","symbol":self.symbol}); cb=getattr(self._strategy_ref,"_on_realtime_trade",None) if self._strategy_ref is not None else None
                self.stats.record_trade()
            if cb and px>0:
                try: cb(px,qty,side)
                except Exception: pass
        except Exception as e: logger.debug("Hyperliquid trade callback error: %s",e)
    def _check_minimum_data(self):
        counts={"1m":len(self._candles_1m),"5m":len(self._candles_5m),"15m":len(self._candles_15m),"1h":len(self._candles_1h),"4h":len(self._candles_4h),"1d":len(self._candles_1d)}; mins={"1m":getattr(config,"MIN_CANDLES_1M",100),"5m":getattr(config,"MIN_CANDLES_5M",100),"15m":getattr(config,"MIN_CANDLES_15M",100),"1h":getattr(config,"MIN_CANDLES_1H",20),"4h":max(getattr(config,"MIN_CANDLES_4H",40),29),"1d":getattr(config,"MIN_CANDLES_1D",7)}; missing=[f"{tf}({counts[tf]}<{mins[tf]})" for tf in mins if counts[tf]<mins[tf]]
        if missing: logger.debug("Hyperliquid DM not ready: %s",", ".join(missing)); return False
        return True
    def get_last_price(self):
        with self._lock: return self._last_price
    def get_orderbook(self):
        with self._lock: return {"bids":list(self._orderbook.get("bids",[])),"asks":list(self._orderbook.get("asks",[])),"timestamp":time.time()}
    def get_recent_trades_raw(self):
        with self._lock: return list(self._recent_trades)[-200:]
    def is_price_fresh(self,max_stale_seconds=90.0): return self._last_price_update_time==0 or (time.time()-self._last_price_update_time)<max_stale_seconds
    def get_candles(self,timeframe="5m",limit=100):
        tf_map={"1m":self._candles_1m,"5m":self._candles_5m,"15m":self._candles_15m,"1h":self._candles_1h,"4h":self._candles_4h,"1d":self._candles_1d}
        with self._lock: candles=list(tf_map.get(timeframe,self._candles_5m))
        return [{"t":int(c.timestamp*1000),"o":c.open,"h":c.high,"l":c.low,"c":c.close,"v":c.volume} for c in candles[-limit:]]
    def get_volume_delta(self,lookback_seconds=60.0):
        with self._lock:
            cutoff=time.time()-lookback_seconds; buy=sum(t["quantity"] for t in self._recent_trades if t["timestamp"]>=cutoff and t["side"]=="buy"); sell=sum(t["quantity"] for t in self._recent_trades if t["timestamp"]>=cutoff and t["side"]=="sell")
        total=buy+sell; return {"buy_volume":buy,"sell_volume":sell,"delta":buy-sell,"delta_pct":(buy-sell)/total if total>0 else 0.0}

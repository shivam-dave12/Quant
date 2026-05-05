"""Hyperliquid public WebSocket adapter."""
from __future__ import annotations
import json, logging, os, threading
from typing import Callable, Dict, Optional, Tuple
try:
    import websocket
except Exception:
    websocket=None
logger=logging.getLogger(__name__)
class HyperliquidWebSocket:
    def __init__(self, *, testnet:Optional[bool]=None, url:Optional[str]=None)->None:
        self.testnet=bool(testnet if testnet is not None else str(os.getenv("HYPERLIQUID_TESTNET","false")).lower()=="true")
        self.url=url or os.getenv("HYPERLIQUID_WS_URL") or ("wss://api.hyperliquid-testnet.xyz/ws" if self.testnet else "wss://api.hyperliquid.xyz/ws")
        self.ws=None; self._thread=None; self._connected=threading.Event(); self._stop=threading.Event(); self._callbacks={}; self._subscriptions=[]; self._lock=threading.RLock()
        logger.info("HyperliquidWebSocket initialized — endpoint: %s", self.url)
    def connect(self, timeout:float=20.0)->bool:
        if websocket is None: logger.error("websocket-client package is not installed"); return False
        self._stop.clear(); self._connected.clear(); self.ws=websocket.WebSocketApp(self.url,on_open=self._on_open,on_message=self._on_message,on_error=self._on_error,on_close=self._on_close)
        self._thread=threading.Thread(target=self.ws.run_forever, kwargs={"ping_interval":20,"ping_timeout":10}, daemon=True); self._thread.start(); return self._connected.wait(timeout=float(timeout))
    def disconnect(self):
        self._stop.set();
        try:
            if self.ws: self.ws.close()
        except Exception: pass
    def _on_open(self,_ws):
        self._connected.set(); logger.info("Hyperliquid WS connected")
        with self._lock: subs=list(self._subscriptions)
        for sub in subs: self._send_sub(sub)
    def _on_close(self,*_a):
        self._connected.clear();
        if not self._stop.is_set(): logger.warning("Hyperliquid WS disconnected")
    def _on_error(self,_ws,error): logger.warning("Hyperliquid WS error: %s", error)
    def _send_sub(self,sub):
        try:
            if self.ws: self.ws.send(json.dumps({"method":"subscribe","subscription":sub}))
        except Exception as e: logger.warning("Hyperliquid WS subscribe send failed %s: %s", sub, e)
    def _subscribe(self,sub,callback):
        key=(str(sub.get("type","")),str(sub.get("coin","")),str(sub.get("interval","")))
        with self._lock:
            self._callbacks[key]=callback
            if sub not in self._subscriptions: self._subscriptions.append(sub)
        if self._connected.is_set(): self._send_sub(sub)
    def subscribe_orderbook(self,coin,callback): self._subscribe({"type":"l2Book","coin":str(coin).upper()},callback)
    def subscribe_trades(self,coin,callback): self._subscribe({"type":"trades","coin":str(coin).upper()},callback)
    def subscribe_candlestick(self,coin,interval,callback): self._subscribe({"type":"candle","coin":str(coin).upper(),"interval":interval},callback)
    def _on_message(self,_ws,message):
        try:
            msg=json.loads(message); ch=msg.get("channel"); data=msg.get("data")
            if ch=="subscriptionResponse" or data is None: return
            if ch=="l2Book":
                cb=self._callbacks.get(("l2Book",str(data.get("coin","")),""));
                if cb: cb(data)
            elif ch=="trades":
                for tr in (data if isinstance(data,list) else [data]):
                    cb=self._callbacks.get(("trades",str(tr.get("coin","")),""));
                    if cb: cb(tr)
            elif ch=="candle":
                cb=self._callbacks.get(("candle",str(data.get("s") or data.get("coin") or ""),str(data.get("i") or "")))
                if cb: cb(data)
        except Exception as e: logger.debug("Hyperliquid WS message parse error: %s", e)

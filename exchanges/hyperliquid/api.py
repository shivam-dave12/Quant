"""
exchanges/hyperliquid/api.py — Hyperliquid public market-data client
=====================================================================
Public Info API wrapper for Hyperliquid native perps and HIP-3 builder-deployed
perps. This module is read-only unless a signed execution adapter is explicitly
implemented and enabled; market discovery/data access must never imply safe live
execution.
"""
from __future__ import annotations
import logging, os
from typing import Any, Dict, List, Optional
import requests
logger=logging.getLogger(__name__)
class HyperliquidAPI:
    def __init__(self, api_key:str="", secret_key:str="", *, testnet:Optional[bool]=None, base_url:Optional[str]=None)->None:
        self.api_key=api_key or os.getenv("HYPERLIQUID_API_KEY",""); self.secret_key=secret_key or os.getenv("HYPERLIQUID_SECRET_KEY",""); self.wallet_address=os.getenv("HYPERLIQUID_WALLET_ADDRESS","")
        self.testnet=bool(testnet if testnet is not None else str(os.getenv("HYPERLIQUID_TESTNET","false")).lower()=="true")
        self.base_url=(base_url or os.getenv("HYPERLIQUID_BASE_URL") or ("https://api.hyperliquid-testnet.xyz" if self.testnet else "https://api.hyperliquid.xyz")).rstrip("/")
        self.timeout=float(os.getenv("HYPERLIQUID_TIMEOUT","15")); self.session=requests.Session(); self.session.headers.update({"Content-Type":"application/json"})
        logger.info("HyperliquidAPI initialized — endpoint: %s (testnet=%s)", self.base_url, self.testnet)
    def info(self,payload:Dict[str,Any])->Any:
        try:
            r=self.session.post(f"{self.base_url}/info", json=payload, timeout=self.timeout)
            if r.status_code==429: return {"error":"rate_limited","status_code":429,"raw":r.text[:500]}
            if r.status_code>=400: return {"error":f"http_{r.status_code}","status_code":r.status_code,"raw":r.text[:500],"payload":payload}
            return r.json()
        except Exception as e: return {"error":"request_failed","message":str(e),"payload":payload}
    @staticmethod
    def _with_dex(payload:Dict[str,Any], dex:Optional[str])->Dict[str,Any]:
        p=dict(payload)
        if dex: p["dex"]=str(dex)
        return p
    def get_meta(self,dex:Optional[str]=None)->Dict[str,Any]:
        payloads=[self._with_dex({"type":"meta"},dex)]
        if dex: payloads += [{"type":"meta","perpDex":str(dex)},{"type":"meta","dexName":str(dex)}]
        last={}
        for p in payloads:
            d=self.info(p); last=d
            if isinstance(d,dict) and isinstance(d.get("universe"),list):
                if dex: d.setdefault("_dex",str(dex))
                return d
        return last if isinstance(last,dict) else {"error":"unexpected_meta_response","data":last}
    def get_perp_dexs(self)->Dict[str,Any]:
        d=self.info({"type":"perpDexs"}); return d if isinstance(d,dict) else {"data":d}
    def get_all_mids(self,dex:Optional[str]=None)->Dict[str,Any]:
        d=self.info(self._with_dex({"type":"allMids"},dex)); return d if isinstance(d,dict) else {}
    def get_l2_book(self,coin:str,dex:Optional[str]=None)->Dict[str,Any]:
        d=self.info({"type":"l2Book","coin":self.normalize_coin(coin,dex=dex)}); return d if isinstance(d,dict) else {"error":"unexpected_l2_response","data":d}
    def get_candles(self,coin:str,interval:str,start_ms:int,end_ms:int,dex:Optional[str]=None)->List[Dict[str,Any]]:
        d=self.info({"type":"candleSnapshot","req":{"coin":self.normalize_coin(coin,dex=dex),"interval":interval,"startTime":int(start_ms),"endTime":int(end_ms)}})
        if isinstance(d,list): return d
        logger.warning("Hyperliquid candleSnapshot %s %s failed: %s", coin, interval, d); return []
    def get_futures_ticker(self,symbol:str,dex:Optional[str]=None,**_)->Dict[str,Any]:
        coin=self.normalize_coin(symbol,dex=dex); mids=self.get_all_mids(dex=dex)
        return {"symbol":coin,"coin":coin,"mark_price":mids[coin],"lastPrice":mids[coin]} if coin in mids else {"error":"symbol_not_found","symbol":coin}
    @staticmethod
    def normalize_coin(symbol:str,dex:Optional[str]=None)->str:
        s=str(symbol or "").upper().replace("/","").replace("-","")
        if ":" in s:
            d,c=s.split(":",1)
            for q in ("USDT","USDC","USD"):
                if c.endswith(q) and len(c)>len(q): c=c[:-len(q)]; break
            return f"{d.lower()}:{c.upper()}"
        for q in ("USDT","USDC","USD"):
            if s.endswith(q) and len(s)>len(q): s=s[:-len(q)]; break
        return f"{str(dex).lower()}:{s}" if dex else s
    def execution_enabled(self)->bool: return False
    def _execution_disabled(self,*_a,**_kw):
        return {"error":"hyperliquid_execution_not_enabled","message":"Hyperliquid is enabled for discovery/data only. Live execution requires signed actions, asset-id mapping, reduce-only/bracket-equivalent protection and explicit enablement."}
    place_order=cancel_order=get_order=set_leverage=get_balance=get_positions=_execution_disabled
FuturesAPI=HyperliquidAPI

let state = null;
let selectedAsset = null;
let lastAssetList = [];

const $ = (id) => document.getElementById(id);
const fmt = {
  money(v){ const n=Number(v||0); return `${n>=0?'+':''}$${Math.abs(n).toFixed(2)}`; },
  price(v){ const n=Number(v||0); if(!Number.isFinite(n) || n===0) return '--'; return `$${n>=1000?n.toLocaleString(undefined,{maximumFractionDigits:2}):n.toFixed(n>=10?3:5)}`; },
  pct(v){ return `${(Number(v||0)*100).toFixed(1)}%`; },
  n(v,d=2){ const n=Number(v||0); return Number.isFinite(n)?n.toFixed(d):'--'; },
  ts(v){ return v?new Date(v*1000).toLocaleTimeString():'--'; },
  age(v){ if(!v) return '--'; const s=Math.max(0,Date.now()/1000-v); if(s<60)return `${s.toFixed(0)}s`; if(s<3600)return `${(s/60).toFixed(0)}m`; return `${(s/3600).toFixed(1)}h`; }
};
function sevClass(x){ x=String(x||'').toLowerCase(); if(x.includes('crit')||x.includes('error')||x==='bad')return'crit'; if(x.includes('warn'))return'warn'; return'ok'; }
function htmlEscape(s){ return String(s??'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])); }
function kpi(label,value,sub='',cls=''){return `<div class="kpi"><div class="label">${label}</div><div class="value ${cls}">${value}</div><div class="sub">${sub}</div></div>`}

function drawLine(canvasId, series, opts={}){
  const c=$(canvasId); if(!c) return; const ctx=c.getContext('2d');
  const rect=c.getBoundingClientRect(); const dpr=window.devicePixelRatio||1; c.width=rect.width*dpr; c.height=rect.height*dpr; ctx.scale(dpr,dpr);
  const w=rect.width,h=rect.height; ctx.clearRect(0,0,w,h);
  ctx.strokeStyle=getComputedStyle(document.body).getPropertyValue('--line'); ctx.lineWidth=1;
  for(let i=0;i<5;i++){ const y=15+(h-30)*i/4; ctx.beginPath();ctx.moveTo(35,y);ctx.lineTo(w-10,y);ctx.stroke(); }
  const flat=[];
  for(const s of series){ for(const p of s.points||[]) flat.push({x:p.ts||0,y:Number(p.value ?? p.bps ?? p.atr ?? p.r ?? p.mfe_r ?? p.p ?? p.ev ?? 0)}); }
  if(flat.length<2){ ctx.fillStyle='#9eb0d2'; ctx.font='12px system-ui'; ctx.fillText('waiting for data...',40,h/2); return; }
  const minX=Math.min(...flat.map(p=>p.x)), maxX=Math.max(...flat.map(p=>p.x));
  let minY=Math.min(...flat.map(p=>p.y)), maxY=Math.max(...flat.map(p=>p.y));
  if(opts.zero){ minY=Math.min(minY,0); maxY=Math.max(maxY,0); }
  if(minY===maxY){ minY-=1; maxY+=1; }
  const sx=x=>35+(x-minX)/(maxX-minX||1)*(w-50); const sy=y=>h-18-(y-minY)/(maxY-minY||1)*(h-38);
  ctx.fillStyle='#9eb0d2'; ctx.font='11px system-ui'; ctx.fillText(maxY.toFixed(2),4,18); ctx.fillText(minY.toFixed(2),4,h-20);
  const colors=['#62a8ff','#25d28a','#f6c65c','#ff6174','#a58bff','#7ee7ff'];
  series.forEach((s,i)=>{ const pts=(s.points||[]).filter(p=>Number.isFinite(Number(p.value ?? p.bps ?? p.atr ?? p.r ?? p.mfe_r ?? p.p ?? p.ev ?? 0))); if(pts.length<2)return; ctx.strokeStyle=s.color||colors[i%colors.length]; ctx.lineWidth=2; ctx.beginPath(); pts.forEach((p,j)=>{ const yv=Number(p.value ?? p.bps ?? p.atr ?? p.r ?? p.mfe_r ?? p.p ?? p.ev ?? 0); const x=sx(p.ts||0), y=sy(yv); if(j===0)ctx.moveTo(x,y); else ctx.lineTo(x,y); }); ctx.stroke(); ctx.fillStyle=s.color||colors[i%colors.length]; ctx.fillText(s.name||'', w-130, 16+i*14); });
}

function renderCharts(){
  if(!state) return;
  const charts=state.charts||{};
  drawLine('pnlChart',[{name:'equity',points:(charts.pnl||[]).map(p=>({ts:p.ts,value:p.value}))},{name:'upnl',points:(charts.pnl||[]).map(p=>({ts:p.ts,value:p.upnl})),color:'#f6c65c'}],{zero:true});
  const asset=selectedAsset || (lastAssetList[0]?.asset);
  if(asset){
    $('assetChartTitle').textContent=`${asset} Price`;
    drawLine('priceChart',[{name:asset,points:(charts.price?.[asset]||[]).map(p=>({ts:p.ts,value:p.value}))}]);
    drawLine('spreadChart',[{name:'bps',points:(charts.spread?.[asset]||[]).map(p=>({ts:p.ts,value:p.bps}))},{name:'ATR ratio',points:(charts.spread?.[asset]||[]).map(p=>({ts:p.ts,value:p.atr})),color:'#f6c65c'}],{zero:true});
    drawLine('posteriorChart',[{name:'posterior',points:(charts.posterior?.[asset]||[]).map(p=>({ts:p.ts,value:p.p}))},{name:'EV',points:(charts.posterior?.[asset]||[]).map(p=>({ts:p.ts,value:p.ev})),color:'#25d28a'}],{zero:true});
  }
}

function renderKPIs(){ const sys=state.system||{}; $('botStatus').textContent=sys.bot_online?'BOT ONLINE':'BOT OFFLINE'; $('botStatus').className=`pill ${sys.bot_online?'ok':'crit'}`; $('kpiGrid').innerHTML=[
  kpi('Open Positions',`${sys.open_positions||0}`,`${sys.assets||0} assets tracked`),
  kpi('Live UPNL',fmt.money(sys.total_upnl||0),'unrealized',Number(sys.total_upnl||0)>=0?'ok':'crit'),
  kpi('Realized PNL',fmt.money(sys.total_realized||0),'closed trades',Number(sys.total_realized||0)>=0?'ok':'crit'),
  kpi('Parsed Events',sys.parsed_events||0,`${sys.ingested_lines||0} log lines`),
  kpi('Source',sys.source||'--',`mode ${sys.mode||'--'}`),
  kpi('Heartbeat',fmt.age(sys.last_heartbeat),fmt.ts(sys.last_heartbeat),sys.bot_online?'ok':'warn'),
].join(''); }

function renderHeatmap(){ const heat=state.heatmap||[]; $('heatmap').innerHTML=heat.slice(0,12).map(h=>{ const cls=h.score>70?'ok':h.score>35?'warn':'crit'; return `<div class="heat" data-asset="${h.asset}"><div class="asset-row"><b>${h.asset}</b><span class="score ${cls}">${fmt.n(h.score,0)}</span></div><div class="mini">${h.state||''}</div><div class="mini">p ${fmt.n(h.posterior,2)} · EV ${fmt.n(h.ev,2)}</div></div>`}).join(''); document.querySelectorAll('.heat').forEach(x=>x.onclick=()=>selectAsset(x.dataset.asset,true)); }
function renderAssets(){ const q=($('assetSearch').value||'').trim().toUpperCase(); const assets=(state.assets||[]).filter(a=>!q||a.asset.includes(q)||String(a.symbol).includes(q)); lastAssetList=assets; const sel=$('assetSelect'); const old=sel.value; sel.innerHTML=assets.map(a=>`<option value="${a.asset}">${a.asset} · ${a.symbol}</option>`).join(''); if(assets.some(a=>a.asset===old))sel.value=old; else if(selectedAsset)sel.value=selectedAsset; if(!selectedAsset && assets[0])selectedAsset=assets[0].asset;
  $('assetList').innerHTML=assets.map(a=>{ const cls=sevClass(a.health); const active=a.asset===selectedAsset?'active':''; return `<div class="asset-card ${active}" data-asset="${a.asset}"><div class="asset-row"><div class="asset-name">${a.asset}</div><span class="badge ${cls}">${a.health||'OK'}</span></div><div class="asset-row mini"><span>${a.venue||''}:${a.symbol||''}</span><span>${fmt.price(a.price)}</span></div><div class="asset-row mini"><span>${a.phase||a.state}</span><span>p ${fmt.n(Math.max(a.posterior||0,a.confidence||0),2)} EV ${fmt.n(a.ev,2)}</span></div><div class="asset-row mini"><span>spread ${fmt.n(a.spread_bps,1)}bps</span><span>risk×${fmt.n(a.risk_mult,2)}</span></div></div>`}).join('') || '<div class="empty">No assets parsed yet.</div>'; document.querySelectorAll('.asset-card').forEach(x=>x.onclick=()=>selectAsset(x.dataset.asset,true)); }
function renderPositions(){ const arr=state.positions||[]; $('positionsCount').textContent=arr.length; $('positionsList').innerHTML=arr.length?arr.map(p=>`<div class="item"><div class="item-row"><b>${p.asset}</b><span class="badge">${p.venue}:${p.symbol}</span></div><div class="item-row"><span>${p.side} · ${p.state} · trail ${p.trailing}</span><b class="${Number(p.upnl)>=0?'ok':'crit'}">${fmt.money(p.upnl)}</b></div><div class="item-row mini"><span>entry ${fmt.price(p.entry)} · px ${fmt.price(p.price)}</span><span>qty ${fmt.n(p.qty,4)}</span></div><div class="item-row mini"><span>SL ${fmt.price(p.sl)} · TP ${fmt.price(p.tp)}</span><span>R ${fmt.n(p.r,2)} MFE ${fmt.n(p.mfe_r,2)}</span></div></div>`).join(''):'<div class="empty">No live positions.</div>'; }
function renderDecisions(){ const arr=state.decisions||[]; $('decisionsCount').textContent=arr.length; $('decisionFeed').innerHTML=arr.slice(0,80).map(d=>`<div class="item" data-asset="${d.asset}"><div class="item-row"><b>${d.asset}</b><span class="badge">${d.kind}</span></div><div class="item-row mini"><span>${d.side||''} p ${fmt.n(d.p,2)} EV ${fmt.n(d.ev,2)} RR ${fmt.n(d.rr,2)}</span><span>${fmt.ts(d.ts)}</span></div><div class="raw">${htmlEscape(d.reason||d.raw||'')}</div></div>`).join('')||'<div class="empty">No decisions yet.</div>'; document.querySelectorAll('#decisionFeed .item').forEach(x=>x.onclick=()=>selectAsset(x.dataset.asset,true)); }
function renderAlerts(){ const arr=state.alerts||[]; $('alertsCount').textContent=arr.length; $('alertsList').innerHTML=arr.slice(0,80).map(a=>`<div class="item"><div class="item-row"><b class="${sevClass(a.severity)}">${htmlEscape(a.title)}</b><span class="badge ${sevClass(a.severity)}">${a.severity}</span></div><div class="item-row mini"><span>${a.asset||''} ${a.symbol||''}</span><span>${fmt.ts(a.ts)}</span></div><div class="raw">${htmlEscape(a.message)}</div></div>`).join('')||'<div class="empty">No alerts.</div>'; }
function renderEvents(){ const arr=state.events||[]; $('eventsCount').textContent=arr.length; $('eventsList').innerHTML=arr.slice(0,120).map(e=>`<div class="item"><div class="item-row"><b class="blue">${e.type}</b><span class="mini">${fmt.ts(e.ts)}</span></div><div class="mini">${e.asset||''} ${e.venue||''}:${e.symbol||''}</div><div class="raw">${htmlEscape(e.last_reason||e.reason||e.message||e.last_decision||'')}</div></div>`).join('')||'<div class="empty">No events yet.</div>'; }
function renderAll(){ if(!state)return; renderKPIs(); renderAssets(); renderHeatmap(); renderPositions(); renderDecisions(); renderAlerts(); renderEvents(); renderCharts(); }
async function refresh(asset){ const qs=asset?`?asset=${encodeURIComponent(asset)}`:''; const r=await fetch('/api/state'+qs); state=await r.json(); renderAll(); }
async function selectAsset(asset,openDrawer=false){ selectedAsset=asset; $('assetSelect').value=asset; await refresh(); if(openDrawer) await openDrawerFor(asset); }
async function openDrawerFor(asset){ const r=await fetch(`/api/assets/${encodeURIComponent(asset)}`); const data=await r.json(); const a=(data.assets||[])[0]||{}; $('drawerTitle').textContent=`${asset} · ${a.symbol||''}`; const dec=(data.decisions||[]).slice(0,25); const alerts=(data.alerts||[]).slice(0,15); const pos=(data.positions||[]); $('drawerBody').innerHTML=`<div class="detail-grid">${[
  ['Price',fmt.price(a.price)],['State',a.state||'--'],['Phase',a.phase||'--'],['Direction',a.direction||'--'],['Posterior',fmt.n(Math.max(a.posterior||0,a.confidence||0),3)],['EV',fmt.n(a.ev,3)],['Spread',`${fmt.n(a.spread_bps,2)} bps / ${fmt.n(a.spread_atr,2)} ATR`],['ATR',fmt.n(a.atr,4)],['Policy',`${a.policy||''} ${a.leverage||''} margin ${a.margin||''}`]
].map(([l,v])=>`<div class="detail-box"><div class="label">${l}</div><div class="value">${v}</div></div>`).join('')}</div>
  <div class="card"><div class="section-title"><h2>Current Position</h2></div>${pos.length?pos.map(p=>`<div class="item"><b>${p.side}</b> entry ${fmt.price(p.entry)} SL ${fmt.price(p.sl)} TP ${fmt.price(p.tp)} R ${fmt.n(p.r,2)}</div>`).join(''):'<div class="empty">No live position.</div>'}</div>
  <div class="card"><div class="section-title"><h2>Latest Decisions</h2></div>${dec.length?dec.map(d=>`<div class="item"><div class="item-row"><b>${d.kind}</b><span>${fmt.ts(d.ts)}</span></div><div class="raw">${htmlEscape(d.raw||d.reason)}</div></div>`).join(''):'<div class="empty">No decisions.</div>'}</div>
  <div class="card"><div class="section-title"><h2>Asset Alerts</h2></div>${alerts.length?alerts.map(alertCard).join(''):'<div class="empty">No alerts.</div>'}</div>`;
  $('drawerBackdrop').classList.remove('hidden'); $('assetDrawer').classList.remove('hidden'); }
function alertCard(a){ return `<div class="item"><div class="item-row"><b class="${sevClass(a.severity)}">${htmlEscape(a.title)}</b><span>${fmt.ts(a.ts)}</span></div><div class="raw">${htmlEscape(a.message)}</div></div>` }
$('refreshBtn').onclick=()=>refresh(); $('toggleThemeBtn').onclick=()=>document.body.classList.toggle('light'); $('assetSearch').oninput=()=>renderAssets(); $('assetSelect').onchange=(e)=>selectAsset(e.target.value,false); $('drawerClose').onclick=()=>{$('drawerBackdrop').classList.add('hidden');$('assetDrawer').classList.add('hidden')}; $('drawerBackdrop').onclick=$('drawerClose').onclick;
refresh(); setInterval(()=>refresh(selectedAsset),2500);

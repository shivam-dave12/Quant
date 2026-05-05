const fmt = {
  money(v) { return `${v >= 0 ? '+' : ''}$${Number(v || 0).toFixed(2)}`; },
  price(v) { const n = Number(v || 0); return `$${n >= 100 ? n.toFixed(2) : n.toFixed(4)}`; },
  ts(v) { return v ? new Date(v * 1000).toLocaleTimeString() : '--'; }
};

function clsSeverity(sev) {
  if (sev === 'critical') return 'crit';
  if (sev === 'warning') return 'warn';
  return 'ok';
}

function renderMetric(label, value, klass='') {
  return `<div class="metric"><div class="label">${label}</div><div class="value ${klass}">${value}</div></div>`;
}

function positionCard(p) {
  const upnlClass = Number(p.upnl || 0) >= 0 ? 'ok' : 'crit';
  return `
    <div class="item">
      <div class="row"><div class="asset">${p.asset}</div><div class="badge">${p.venue}:${p.symbol}</div></div>
      <div class="row"><div class="small">${p.side} · ${p.state} · trail ${p.trailing}</div><div class="strong ${upnlClass}">${fmt.money(p.upnl)}</div></div>
      <div class="row small"><div>entry ${fmt.price(p.entry)} · px ${fmt.price(p.price)}</div><div>qty ${Number(p.qty || 0).toFixed(4)}</div></div>
      <div class="row small"><div>SL ${fmt.price(p.sl)} · TP ${fmt.price(p.tp)}</div><div>R ${Number(p.achieved_r || 0).toFixed(2)} · MFE ${Number(p.mfe_r || 0).toFixed(2)}</div></div>
    </div>`;
}

function scannerCard(s) {
  return `
    <div class="item">
      <div class="row"><div class="asset">${s.asset}</div><div class="badge">${s.venue}:${s.symbol}</div></div>
      <div class="row"><div class="small">${s.phase}</div><div class="small">px ${fmt.price(s.price)}</div></div>
      <div class="row small"><div>spread ${Number(s.spread_bps || 0).toFixed(2)} bps</div><div>ATR ${Number(s.atr || 0).toFixed(4)}</div></div>
      <div class="row small"><div>posterior ${Number(s.posterior || 0).toFixed(2)}</div><div>quality ${Number(s.setup_quality || 0).toFixed(2)}</div></div>
      <div class="row small"><div>${s.last_reason || ''}</div><div>${fmt.ts(s.updated_at)}</div></div>
    </div>`;
}

function alertCard(a) {
  return `
    <div class="item">
      <div class="row"><div class="strong ${clsSeverity(a.severity)}">${a.title}</div><div class="badge ${clsSeverity(a.severity)}">${a.severity}</div></div>
      <div class="row small"><div>${[a.asset, a.venue && `${a.venue}:${a.symbol}`].filter(Boolean).join(' · ')}</div><div>${fmt.ts(a.ts)}</div></div>
      <div class="row small"><div>${a.message}</div></div>
    </div>`;
}

function tradeCard(t) {
  const cls = Number(t.pnl || 0) >= 0 ? 'ok' : 'crit';
  return `
    <div class="item">
      <div class="row"><div class="asset">${t.asset}</div><div class="badge">${t.venue}:${t.symbol}</div></div>
      <div class="row"><div class="small">${t.side} · ${t.reason || 'closed'}</div><div class="strong ${cls}">${fmt.money(t.pnl)}</div></div>
      <div class="row small"><div>${fmt.price(t.entry)} → ${fmt.price(t.exit)}</div><div>R ${Number(t.achieved_r || 0).toFixed(2)}</div></div>
    </div>`;
}

function eventCard(e) {
  return `
    <div class="item">
      <div class="row"><div class="strong blue">${e.type}</div><div class="small">${fmt.ts(e.ts)}</div></div>
      <div class="row small"><div>${[e.asset, e.venue && `${e.venue}:${e.symbol}`].filter(Boolean).join(' · ')}</div></div>
      <div class="row small"><div>${e.title || e.reason || e.message || ''}</div></div>
    </div>`;
}

function updateDom(data) {
  const system = data.system || {};
  document.getElementById('botStatus').textContent = system.bot_online ? 'BOT ONLINE' : 'BOT OFFLINE';
  document.getElementById('botStatus').className = `pill ${system.bot_online ? 'ok' : 'crit'}`;
  document.getElementById('clock').textContent = new Date().toLocaleTimeString();

  document.getElementById('summaryGrid').innerHTML = [
    renderMetric('Mode', system.mode || '--'),
    renderMetric('Environment', system.environment || '--'),
    renderMetric('Open Positions', `${system.open_positions || 0}/${system.max_positions || 0}`),
    renderMetric('Live UPNL', fmt.money(system.total_upnl || 0), Number(system.total_upnl || 0) >= 0 ? 'ok' : 'crit'),
    renderMetric('Realized PNL', fmt.money(system.total_realized || 0), Number(system.total_realized || 0) >= 0 ? 'ok' : 'crit'),
    renderMetric('Last Heartbeat', fmt.ts(system.last_heartbeat || 0)),
  ].join('');

  document.getElementById('positionsCount').textContent = (data.positions || []).length;
  document.getElementById('positionsList').innerHTML = (data.positions || []).length ? data.positions.map(positionCard).join('') : '<div class="item small">No live positions.</div>';
  document.getElementById('scannersCount').textContent = (data.scanners || []).length;
  document.getElementById('scannersList').innerHTML = (data.scanners || []).length ? data.scanners.map(scannerCard).join('') : '<div class="item small">No scanners yet.</div>';
  document.getElementById('alertsCount').textContent = (data.alerts || []).length;
  document.getElementById('alertsList').innerHTML = (data.alerts || []).length ? data.alerts.map(alertCard).join('') : '<div class="item small">No alerts.</div>';
  document.getElementById('tradesCount').textContent = (data.trades || []).length;
  document.getElementById('tradesList').innerHTML = (data.trades || []).length ? data.trades.map(tradeCard).join('') : '<div class="item small">No closed trades yet.</div>';
  document.getElementById('eventsCount').textContent = (data.events || []).length;
  document.getElementById('eventsList').innerHTML = (data.events || []).slice(0, 100).map(eventCard).join('');
}

async function refresh() {
  const res = await fetch('/api/state');
  const data = await res.json();
  updateDom(data);
}

refresh();
setInterval(refresh, 2000);

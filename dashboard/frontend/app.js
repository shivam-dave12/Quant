let state = null;
let diagnostics = null;
let selectedAsset = "";
let selectedAgent = "PortfolioCIO";
let activeView = "overview";

const $ = (id) => document.getElementById(id);
const clamp = (v, lo = 0, hi = 1) => Math.max(lo, Math.min(hi, Number(v || 0)));
const esc = (s) => String(s ?? "").replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));

const fmt = {
  money(v) {
    const n = Number(v || 0);
    const sign = n >= 0 ? "+" : "-";
    return `${sign}$${Math.abs(n).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
  },
  price(v) {
    const n = Number(v || 0);
    if (!Number.isFinite(n) || n === 0) return "--";
    if (Math.abs(n) >= 1000) return `$${n.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
    if (Math.abs(n) >= 10) return `$${n.toFixed(3)}`;
    return `$${n.toFixed(5)}`;
  },
  pct(v) { return `${(Number(v || 0) * 100).toFixed(1)}%`; },
  n(v, d = 2) {
    const n = Number(v || 0);
    return Number.isFinite(n) ? n.toFixed(d) : "--";
  },
  ts(v) { return v ? new Date(v * 1000).toLocaleTimeString() : "--"; },
  ageFromTs(v) {
    if (!v) return "--";
    const s = Math.max(0, Date.now() / 1000 - Number(v));
    if (s < 60) return `${s.toFixed(0)}s`;
    if (s < 3600) return `${(s / 60).toFixed(0)}m`;
    return `${(s / 3600).toFixed(1)}h`;
  }
};

const agentCatalog = [
  ["PortfolioCIO", "Governor", "Capital allocation and desk selection"],
  ["UniverseAgent", "Discovery", "Live instruments, data readiness, spread checks"],
  ["TickerSelectionAgent", "Ranking", "Ranks desks by tradability and opportunity"],
  ["SetupSelectionAgent", "Alpha triage", "Reads cached strategy edge, EV, and setup maturity"],
  ["RiskCommitteeAgent", "Risk", "Non-bypassable deterministic approval layer"],
  ["ExecutionDeskAgent", "Execution", "Venue routing and order safety authority"],
  ["PostTradeLearningAgent", "Learning", "Closed-trade attribution and adaptive priors"]
];

function severityClass(x) {
  const s = String(x || "").toLowerCase();
  if (s.includes("crit") || s.includes("error") || s.includes("bad") || s.includes("block")) return "crit";
  if (s.includes("warn") || s.includes("paper") || s.includes("watch")) return "warn";
  return "ok";
}

function panelMetric(label, value, sub = "", cls = "") {
  return `<button class="summary-card ${cls}" data-summary="${esc(label)}">
    <span>${esc(label)}</span><b>${value}</b><small>${esc(sub)}</small>
  </button>`;
}

function metricBox(label, value, sub = "", cls = "") {
  return `<div class="metric-box"><span>${esc(label)}</span><b class="${cls}">${value}</b><small>${esc(sub)}</small></div>`;
}

function getAssets() {
  return (state?.assets || []).slice().sort((a, b) => String(a.asset).localeCompare(String(b.asset)));
}

function renderAssetSelect() {
  const select = $("assetSelect");
  if (!select) return;
  const assets = getAssets();
  if (!assets.length) {
    selectedAsset = "";
    select.innerHTML = `<option value="">No assets</option>`;
    return;
  }
  if (!selectedAsset || !assets.some((a) => a.asset === selectedAsset)) selectedAsset = assets[0].asset;
  select.innerHTML = assets.map((a) => `<option value="${esc(a.asset)}">${esc(a.asset)} | ${esc(a.symbol || "")}</option>`).join("");
  select.value = selectedAsset;
}

function getAsset(asset = selectedAsset) {
  return getAssets().find((a) => a.asset === asset) || getAssets()[0] || {};
}

function getAgents() {
  const live = new Map((state?.agents || []).map((a) => [a.agent, a]));
  return agentCatalog.map(([agent, role, fallback]) => {
    const row = live.get(agent);
    if (row) return row;
    return {
      agent, role, status: "WAITING", score: 0, selected: 0, rejected: 0,
      approved: 0, blocked: 0, last_reason: fallback, detail: {}, last_update: 0
    };
  });
}

function getAgent(name = selectedAgent) {
  return getAgents().find((a) => a.agent === name) || getAgents()[0];
}

function heatScore(a) {
  const edge = Math.max(Number(a.posterior || 0), Number(a.confidence || 0));
  const ev = Math.max(0, Number(a.ev || 0));
  const spread = Math.max(0, 1 - Math.min(Number(a.spread_atr || 0), 3) / 3);
  const stateBoost = ["POST_SWEEP", "DIRECTION", "READY", "ACTIVE"].includes(String(a.phase || a.state)) ? 0.12 : 0.04;
  return clamp(edge * 0.45 + Math.min(ev, 2) * 0.20 + spread * 0.23 + stateBoost);
}

function fitCanvas(canvas) {
  const rect = canvas.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  canvas.width = Math.max(1, rect.width * dpr);
  canvas.height = Math.max(1, rect.height * dpr);
  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  return { ctx, w: rect.width, h: rect.height };
}

function drawLine(canvasId, series, opts = {}) {
  const canvas = $(canvasId);
  if (!canvas) return;
  const { ctx, w, h } = fitCanvas(canvas);
  ctx.clearRect(0, 0, w, h);
  const css = getComputedStyle(document.body);
  const grid = css.getPropertyValue("--line");
  const text = css.getPropertyValue("--muted");
  const colors = opts.colors || ["#3bc8ff", "#2ed47a", "#f3b64b", "#ff5d73", "#b783ff", "#44d7b6"];

  ctx.strokeStyle = grid;
  ctx.lineWidth = 1;
  for (let i = 0; i < 5; i++) {
    const y = 18 + (h - 42) * i / 4;
    ctx.beginPath();
    ctx.moveTo(40, y);
    ctx.lineTo(w - 12, y);
    ctx.stroke();
  }

  const points = [];
  series.forEach((s) => (s.points || []).forEach((p) => {
    const y = Number(p.value ?? p.score ?? p.selected ?? p.blocked ?? p.bps ?? p.atr ?? p.r ?? p.mfe_r ?? p.p ?? p.ev ?? 0);
    if (Number.isFinite(y)) points.push({ x: Number(p.ts || 0), y });
  }));
  if (points.length < 2) {
    ctx.fillStyle = text;
    ctx.font = "12px Inter, system-ui";
    ctx.fillText("waiting for data", 44, h / 2);
    return;
  }

  const minX = Math.min(...points.map((p) => p.x));
  const maxX = Math.max(...points.map((p) => p.x));
  let minY = Math.min(...points.map((p) => p.y));
  let maxY = Math.max(...points.map((p) => p.y));
  if (opts.zero) {
    minY = Math.min(minY, 0);
    maxY = Math.max(maxY, 0);
  }
  if (opts.unit) {
    minY = Math.min(minY, 0);
    maxY = Math.max(maxY, 1);
  }
  if (minY === maxY) {
    minY -= 1;
    maxY += 1;
  }
  const sx = (x) => 40 + (x - minX) / (maxX - minX || 1) * (w - 56);
  const sy = (y) => h - 20 - (y - minY) / (maxY - minY || 1) * (h - 44);

  ctx.fillStyle = text;
  ctx.font = "11px Inter, system-ui";
  ctx.fillText(maxY.toFixed(2), 4, 20);
  ctx.fillText(minY.toFixed(2), 4, h - 20);

  series.forEach((s, i) => {
    const pts = (s.points || []).filter((p) => Number.isFinite(Number(p.value ?? p.score ?? p.selected ?? p.blocked ?? p.bps ?? p.atr ?? p.r ?? p.mfe_r ?? p.p ?? p.ev ?? 0)));
    if (pts.length < 2) return;
    ctx.strokeStyle = s.color || colors[i % colors.length];
    ctx.lineWidth = 2.2;
    ctx.beginPath();
    pts.forEach((p, j) => {
      const yv = Number(p.value ?? p.score ?? p.selected ?? p.blocked ?? p.bps ?? p.atr ?? p.r ?? p.mfe_r ?? p.p ?? p.ev ?? 0);
      const x = sx(Number(p.ts || 0));
      const y = sy(yv);
      if (j === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.stroke();
    ctx.fillStyle = s.color || colors[i % colors.length];
    ctx.fillText(s.name || "", w - 138, 18 + i * 15);
  });
}

function drawBars(canvasId, rows, opts = {}) {
  const canvas = $(canvasId);
  if (!canvas) return;
  const { ctx, w, h } = fitCanvas(canvas);
  ctx.clearRect(0, 0, w, h);
  const css = getComputedStyle(document.body);
  const text = css.getPropertyValue("--muted");
  const rows2 = rows.slice(0, opts.limit || 10);
  if (!rows2.length) {
    ctx.fillStyle = text;
    ctx.font = "12px Inter, system-ui";
    ctx.fillText("waiting for data", 24, h / 2);
    return;
  }
  const max = Math.max(...rows2.map((r) => Math.abs(Number(r.value || 0))), 1);
  const barH = Math.max(10, (h - 28) / rows2.length - 8);
  rows2.forEach((r, i) => {
    const y = 18 + i * (barH + 8);
    const val = Number(r.value || 0);
    const bw = Math.abs(val) / max * (w - 128);
    ctx.fillStyle = r.color || "#3bc8ff";
    ctx.fillRect(96, y, bw, barH);
    ctx.fillStyle = text;
    ctx.font = "11px Inter, system-ui";
    ctx.fillText(String(r.label).slice(0, 12), 8, y + barH - 1);
    ctx.fillText(val.toFixed(opts.digits ?? 2), Math.min(w - 38, 104 + bw), y + barH - 1);
  });
}

function setView(view) {
  activeView = view;
  document.querySelectorAll(".view").forEach((el) => el.classList.toggle("active", el.id === `view-${view}`));
  document.querySelectorAll(".nav-btn").forEach((el) => el.classList.toggle("active", el.dataset.view === view));
  const titles = { overview: "Portfolio Overview", agents: "Agent Pages", assets: "Asset Desk Detail", risk: "Risk Command", execution: "Execution Desk", telemetry: "Telemetry" };
  $("viewTitle").textContent = titles[view] || "Command Center";
  location.hash = view;
  renderAll();
}

function renderHealth() {
  const sys = state?.system || {};
  const metrics = state?.metrics || {};
  const online = !!sys.bot_online;
  $("railStatusDot").className = `status-dot ${online ? "ok" : "crit"}`;
  $("railStatusText").textContent = online ? "Online" : "Offline";
  $("railStatusSub").textContent = `source ${sys.source || "--"}`;
  const cls = online ? "ok" : ((sys.parsed_events || 0) ? "warn" : "crit");
  const title = online ? "Dashboard receiving live heartbeat" : ((sys.parsed_events || 0) ? "Telemetry parsed, heartbeat stale" : "No telemetry received");
  const body = online
    ? `${sys.assets || 0} assets, ${state?.agents?.length || 0} agents, ${sys.event_count || 0} events`
    : (diagnostics?.probable_root_cause || "Start the dashboard emitter or log-tail sidecar.");
  $("healthBanner").className = `health-banner ${cls}`;
  $("healthBanner").innerHTML = `<b>${esc(title)}</b><span>${esc(body)}</span><small>event age ${metrics.last_event_age_sec == null ? "--" : fmt.n(metrics.last_event_age_sec, 0) + "s"}</small>`;
}

function renderSummary() {
  const sys = state?.system || {};
  const m = state?.metrics || {};
  const agents = getAgents();
  const selected = agents.find((a) => a.agent === "PortfolioCIO")?.selected || 0;
  $("summaryStrip").innerHTML = [
    panelMetric("Open Positions", sys.open_positions || 0, `${sys.assets || 0} assets tracked`, "link", "execution"),
    panelMetric("Live UPNL", fmt.money(sys.total_upnl || 0), "unrealized", Number(sys.total_upnl || 0) >= 0 ? "ok" : "crit"),
    panelMetric("Realized PNL", fmt.money(sys.total_realized || 0), "closed trades", Number(sys.total_realized || 0) >= 0 ? "ok" : "crit"),
    panelMetric("Win Rate", fmt.pct(m.win_rate || 0), `${m.trade_count || 0} trades`, "link"),
    panelMetric("CIO Selected", selected, `${agents.length} agents`, "link"),
    panelMetric("Best Desk", m.best_asset || "--", `score ${fmt.n((m.best_score || 0) / 100, 2)}`, "link"),
    panelMetric("Coverage", fmt.pct(m.coverage || 0), `${m.fresh_assets || 0} fresh`, "link"),
    panelMetric("Events", sys.parsed_events || 0, `${sys.ingested_lines || 0} lines`, "link")
  ].join("");
  document.querySelectorAll(".summary-card").forEach((el, idx) => {
    el.onclick = () => setView(["execution", "risk", "risk", "risk", "agents", "assets", "assets", "telemetry"][idx] || "overview");
  });
}

function renderOverview() {
  const assets = getAssets().sort((a, b) => heatScore(b) - heatScore(a));
  $("opportunityMeta").textContent = `${assets.length} desks`;
  $("opportunityGrid").innerHTML = assets.slice(0, 18).map((a) => {
    const score = heatScore(a);
    return `<button class="opportunity-card ${a.asset === selectedAsset ? "active" : ""}" data-asset="${esc(a.asset)}">
      <span class="asset-code">${esc(a.asset)}</span>
      <b>${fmt.n(score * 100, 0)}</b>
      <small>${esc(a.venue || "")}:${esc(a.symbol || "")}</small>
      <div class="bar"><i style="width:${score * 100}%"></i></div>
      <small>p ${fmt.n(Math.max(a.posterior || 0, a.confidence || 0), 2)} EV ${fmt.n(a.ev, 2)} spread ${fmt.n(a.spread_bps, 1)}bps</small>
    </button>`;
  }).join("") || `<div class="empty">No asset telemetry yet.</div>`;
  document.querySelectorAll(".opportunity-card").forEach((el) => {
    el.onclick = () => { selectedAsset = el.dataset.asset; setView("assets"); renderAll(); };
  });

  const agents = getAgents();
  $("agentMeta").textContent = `${agents.filter((a) => a.status !== "WAITING").length} reporting`;
  $("agentStackMini").innerHTML = agents.map((a) => `<button class="agent-mini" data-agent="${esc(a.agent)}">
    <span><b>${esc(a.agent)}</b><small>${esc(a.role)}</small></span>
    <i class="${severityClass(a.status)}">${esc(a.status)}</i>
    <div class="bar"><i style="width:${clamp(a.score) * 100}%"></i></div>
  </button>`).join("");
  document.querySelectorAll(".agent-mini").forEach((el) => {
    el.onclick = () => { selectedAgent = el.dataset.agent; setView("agents"); renderAll(); };
  });
}

function renderAgents() {
  const agents = getAgents();
  $("agentDirectory").innerHTML = agents.map((a) => `<button class="agent-card ${a.agent === selectedAgent ? "active" : ""}" data-agent="${esc(a.agent)}">
    <span class="agent-head"><b>${esc(a.agent)}</b><i class="${severityClass(a.status)}">${esc(a.status)}</i></span>
    <small>${esc(a.role)}</small>
    <div class="bar"><i style="width:${clamp(a.score) * 100}%"></i></div>
    <span class="agent-stats"><small>selected ${a.selected || 0}</small><small>blocked ${a.blocked || 0}</small></span>
  </button>`).join("");
  document.querySelectorAll(".agent-card").forEach((el) => {
    el.onclick = () => { selectedAgent = el.dataset.agent; renderAgents(); };
  });

  const a = getAgent();
  $("agentPageTitle").textContent = a.agent;
  $("agentPageRole").textContent = `${a.role || "--"} | ${a.status || "--"} | updated ${fmt.ageFromTs(a.last_update)}`;
  const detail = a.detail || {};
  const selectedRows = detail.selected || [];
  const rejectedRows = detail.rejected || [];
  $("agentDetail").innerHTML = `
    <div class="metric-grid">
      ${metricBox("Score", fmt.pct(a.score || 0), "current confidence", severityClass(a.status))}
      ${metricBox("Selected", a.selected || 0, "current cycle")}
      ${metricBox("Rejected", a.rejected || 0, "parked desks")}
      ${metricBox("Approved", a.approved || 0, "risk approvals")}
      ${metricBox("Blocked", a.blocked || 0, "risk blocks", a.blocked ? "warn" : "ok")}
      ${metricBox("Latency", `${fmt.n(a.latency_ms || 0, 0)}ms`, "agent runtime")}
    </div>
    <div class="agent-narrative">${esc(a.last_reason || "Waiting for first fund cycle.")}</div>
    <div class="dual-list">
      <div><h3>Selected Desks</h3>${selectedRows.length ? selectedRows.map(selectionRow).join("") : `<div class="empty">No selected desks.</div>`}</div>
      <div><h3>Parked Desks</h3>${rejectedRows.length ? rejectedRows.slice(0, 10).map(selectionRow).join("") : `<div class="empty">No parked desks.</div>`}</div>
    </div>`;
  const hist = state?.charts?.agents?.[a.agent] || [];
  drawLine("agentScoreChart", [{ name: a.agent, points: hist.map((p) => ({ ts: p.ts, score: p.score })) }], { unit: true });
  drawLine("agentActionChart", [
    { name: "selected", points: hist.map((p) => ({ ts: p.ts, selected: p.selected })), color: "#2ed47a" },
    { name: "blocked", points: hist.map((p) => ({ ts: p.ts, blocked: p.blocked })), color: "#ff5d73" }
  ], { zero: true });
}

function selectionRow(row) {
  const d = row.diagnostics || {};
  const asset = row.asset_id || row.asset || row.ticker || row.symbol || d.asset_id || d.asset || "";
  return `<button class="selection-row" data-asset="${esc(asset)}">
    <b>${esc(asset || "--")}</b>
    <span>score ${fmt.n(row.score, 2)}</span>
    <small>${esc(row.reason || "")}</small>
  </button>`;
}

function renderAssets() {
  const assets = getAssets();
  renderAssetSelect();
  const q = ($("assetSearch")?.value || "").trim().toUpperCase();
  const filtered = assets.filter((a) => !q || String(a.asset).includes(q) || String(a.symbol).toUpperCase().includes(q));
  $("assetList").innerHTML = filtered.map((a) => assetCard(a)).join("") || `<div class="empty">No assets parsed yet.</div>`;
  document.querySelectorAll(".asset-card").forEach((el) => {
    el.onclick = () => { selectedAsset = el.dataset.asset; renderAll(); };
  });
  renderAssetDetail();
}

function assetCard(a) {
  const score = heatScore(a);
  const active = a.asset === selectedAsset ? "active" : "";
  return `<button class="asset-card ${active}" data-asset="${esc(a.asset)}">
    <span class="asset-top"><b>${esc(a.asset)}</b><i class="${severityClass(a.health)}">${esc(a.health || "OK")}</i></span>
    <small>${esc(a.venue || "")}:${esc(a.symbol || "")} | ${fmt.price(a.price)}</small>
    <div class="bar"><i style="width:${score * 100}%"></i></div>
    <span class="asset-meta"><small>${esc(a.phase || a.state || "")}</small><small>p ${fmt.n(Math.max(a.posterior || 0, a.confidence || 0), 2)}</small></span>
  </button>`;
}

function renderAssetDetail() {
  const a = getAsset();
  $("assetPageTitle").textContent = `${a.asset || "--"} | ${a.symbol || ""}`;
  $("assetPageSub").textContent = `${a.venue || "--"} | ${a.phase || a.state || "--"} | updated ${fmt.ageFromTs(a.last_update)}`;
  $("assetDetailGrid").innerHTML = [
    ["Price", fmt.price(a.price), ""],
    ["Posterior", fmt.n(Math.max(a.posterior || 0, a.confidence || 0), 3), ""],
    ["EV", `${fmt.n(a.ev, 3)}R`, Number(a.ev || 0) >= 0 ? "ok" : "crit"],
    ["Spread", `${fmt.n(a.spread_bps, 2)} bps`, ""],
    ["ATR Ratio", fmt.n(a.spread_atr, 2), ""],
    ["ATR", fmt.n(a.atr, 4), ""],
    ["Policy", a.policy || "--", ""],
    ["Leverage", a.leverage || "--", ""],
    ["Risk Mult", fmt.n(a.risk_mult, 2), ""],
    ["State", a.state || "--", severityClass(a.health)],
    ["Direction", a.direction || "--", ""],
    ["Margin", a.margin || "--", ""]
  ].map(([l, v, c]) => metricBox(l, v, "", c)).join("");

  const charts = state?.charts || {};
  drawLine("assetPriceChart", [{ name: a.asset || "", points: charts.price?.[a.asset] || [] }]);
  drawLine("assetPosteriorChart", [
    { name: "posterior", points: (charts.posterior?.[a.asset] || []).map((p) => ({ ts: p.ts, p: p.p })) },
    { name: "EV", points: (charts.posterior?.[a.asset] || []).map((p) => ({ ts: p.ts, ev: p.ev })), color: "#2ed47a" }
  ], { zero: true });
  drawLine("assetSpreadChart", [
    { name: "bps", points: (charts.spread?.[a.asset] || []).map((p) => ({ ts: p.ts, bps: p.bps })) },
    { name: "ATR", points: (charts.spread?.[a.asset] || []).map((p) => ({ ts: p.ts, atr: p.atr })), color: "#f3b64b" }
  ], { zero: true });
  drawLine("assetRChart", [
    { name: "R", points: (charts.r?.[a.asset] || []).map((p) => ({ ts: p.ts, r: p.r })) },
    { name: "MFE", points: (charts.r?.[a.asset] || []).map((p) => ({ ts: p.ts, mfe_r: p.mfe_r })), color: "#2ed47a" }
  ], { zero: true });

  const decisions = (state?.decisions || []).filter((d) => d.asset === a.asset).slice(0, 25);
  $("assetDecisionTrail").innerHTML = decisions.length ? decisions.map(decisionItem).join("") : `<div class="empty">No asset decisions yet.</div>`;
}

function renderRisk() {
  const sys = state?.system || {};
  const m = state?.metrics || {};
  $("riskCards").innerHTML = [
    metricBox("Open Slots", `${sys.open_positions || 0}`, "live reservations"),
    metricBox("Risk Heat", fmt.money(m.risk_heat || 0), "realized + unrealized", Number(m.risk_heat || 0) >= 0 ? "ok" : "crit"),
    metricBox("Profit Factor", fmt.n(m.profit_factor || 0, 2), "closed trade quality"),
    metricBox("Average R", fmt.n(m.avg_r || 0, 2), "closed trades"),
    metricBox("Fresh Assets", `${m.fresh_assets || 0}`, `${m.stale_assets || 0} stale`),
    metricBox("Risk Agent", fmt.pct(getAgent("RiskCommitteeAgent")?.score || 0), "approval ratio")
  ].join("");
  const trades = state?.trades || [];
  drawBars("riskRChart", trades.slice(0, 18).map((t) => ({ label: t.asset, value: t.r, color: Number(t.r) >= 0 ? "#2ed47a" : "#ff5d73" })), { digits: 2 });
  drawLine("riskHeatChart", [{ name: "portfolio", points: (state?.charts?.pnl || []).map((p) => ({ ts: p.ts, value: p.value })) }], { zero: true });
  const riskEvents = (state?.events || []).filter((e) => String(e.type || "").includes("fund") || String(e.type || "").includes("candidate") || String(e.message || "").includes("Risk")).slice(0, 50);
  $("riskTape").innerHTML = riskEvents.length ? riskEvents.map(eventItem).join("") : `<div class="empty">No risk events yet.</div>`;
}

function renderExecution() {
  const positions = state?.positions || [];
  $("positionsCount").textContent = positions.length;
  $("positionsList").innerHTML = positions.length ? positions.map(positionItem).join("") : `<div class="empty">No live positions.</div>`;
  const execAgent = getAgent("ExecutionDeskAgent");
  $("executionCards").innerHTML = [
    metricBox("Execution Mode", execAgent.status || "--", execAgent.last_reason || ""),
    metricBox("Open Positions", positions.length, "managed every cycle"),
    metricBox("Alerts", (state?.alerts || []).length, "operator tape"),
    metricBox("Selected Asset", selectedAsset || "--", "chart focus"),
    metricBox("Spread", `${fmt.n(getAsset().spread_bps, 2)} bps`, "selected desk"),
    metricBox("Bracket Health", positions.every((p) => String(p.bracket).includes("SL") || String(p.bracket).includes("TP")) ? "OK" : "WATCH", "protective orders")
  ].join("");
  const a = getAsset();
  drawLine("executionCostChart", [
    { name: "bps", points: (state?.charts?.spread?.[a.asset] || []).map((p) => ({ ts: p.ts, bps: p.bps })) },
    { name: "ATR ratio", points: (state?.charts?.spread?.[a.asset] || []).map((p) => ({ ts: p.ts, atr: p.atr })), color: "#f3b64b" }
  ], { zero: true });
  const alerts = state?.alerts || [];
  $("alertsCount").textContent = alerts.length;
  $("alertsList").innerHTML = alerts.length ? alerts.slice(0, 80).map(alertItem).join("") : `<div class="empty">No alerts.</div>`;
}

function renderTelemetry() {
  const sys = state?.system || {};
  $("diagnosticsGrid").innerHTML = [
    metricBox("Source", sys.source || "--", "direct or log-tail"),
    metricBox("Heartbeat", fmt.ageFromTs(sys.last_heartbeat), fmt.ts(sys.last_heartbeat), sys.bot_online ? "ok" : "warn"),
    metricBox("Parsed Events", sys.parsed_events || 0, `${sys.ingested_lines || 0} lines`),
    metricBox("Assets", sys.assets || 0, "state records"),
    metricBox("Agents", state?.agents?.length || 0, "agent records"),
    metricBox("Backend", diagnostics?.status || "--", diagnostics?.probable_root_cause || "")
  ].join("");
  const decisions = state?.decisions || [];
  $("decisionsCount").textContent = decisions.length;
  $("decisionFeed").innerHTML = decisions.length ? decisions.slice(0, 120).map(decisionItem).join("") : `<div class="empty">No decisions yet.</div>`;
  const events = state?.events || [];
  $("eventsCount").textContent = events.length;
  $("eventsList").innerHTML = events.length ? events.slice(0, 160).map(eventItem).join("") : `<div class="empty">No events yet.</div>`;
}

function positionItem(p) {
  return `<button class="tape-item" data-asset="${esc(p.asset)}">
    <span><b>${esc(p.asset)}</b><i>${esc(p.side)} | ${esc(p.state)}</i></span>
    <span><b class="${Number(p.upnl || 0) >= 0 ? "ok" : "crit"}">${fmt.money(p.upnl)}</b><i>R ${fmt.n(p.r, 2)} MFE ${fmt.n(p.mfe_r, 2)}</i></span>
    <small>entry ${fmt.price(p.entry)} | px ${fmt.price(p.price)} | SL ${fmt.price(p.sl)} | TP ${fmt.price(p.tp)}</small>
  </button>`;
}

function decisionItem(d) {
  return `<button class="tape-item" data-asset="${esc(d.asset)}">
    <span><b>${esc(d.asset || "SYSTEM")}</b><i>${esc(d.kind)}</i></span>
    <span><b>${fmt.n(d.p, 2)}</b><i>EV ${fmt.n(d.ev, 2)} RR ${fmt.n(d.rr, 2)}</i></span>
    <small>${esc(d.reason || d.raw || "")}</small>
  </button>`;
}

function eventItem(e) {
  return `<button class="tape-item" data-asset="${esc(String(e.asset || "").toUpperCase())}">
    <span><b>${esc(e.type || "event")}</b><i>${fmt.ts(e.ts)}</i></span>
    <small>${esc(e.asset || "")} ${esc(e.venue || "")}:${esc(e.symbol || "")}</small>
    <small>${esc(e.last_reason || e.reason || e.message || e.last_decision || "")}</small>
  </button>`;
}

function alertItem(a) {
  return `<button class="tape-item" data-asset="${esc(a.asset || "")}">
    <span><b class="${severityClass(a.severity)}">${esc(a.title)}</b><i>${fmt.ts(a.ts)}</i></span>
    <small>${esc(a.asset || "")} ${esc(a.symbol || "")}</small>
    <small>${esc(a.message || "")}</small>
  </button>`;
}

function renderCharts() {
  const charts = state?.charts || {};
  const asset = getAsset();
  $("priceChartTitle").textContent = `${asset.asset || "Asset"} Price`;
  drawLine("pnlChart", [
    { name: "equity", points: (charts.pnl || []).map((p) => ({ ts: p.ts, value: p.value })) },
    { name: "upnl", points: (charts.pnl || []).map((p) => ({ ts: p.ts, value: p.upnl })), color: "#f3b64b" }
  ], { zero: true });
  drawLine("priceChart", [{ name: asset.asset || "", points: charts.price?.[asset.asset] || [] }]);
  drawLine("posteriorChart", [
    { name: "posterior", points: (charts.posterior?.[asset.asset] || []).map((p) => ({ ts: p.ts, p: p.p })) },
    { name: "EV", points: (charts.posterior?.[asset.asset] || []).map((p) => ({ ts: p.ts, ev: p.ev })), color: "#2ed47a" }
  ], { zero: true });
  drawLine("spreadChart", [
    { name: "bps", points: (charts.spread?.[asset.asset] || []).map((p) => ({ ts: p.ts, bps: p.bps })) },
    { name: "ATR", points: (charts.spread?.[asset.asset] || []).map((p) => ({ ts: p.ts, atr: p.atr })), color: "#f3b64b" }
  ], { zero: true });
}

function openDrawer(kind, id) {
  $("drawerEyebrow").textContent = kind.toUpperCase();
  $("drawerTitle").textContent = id || "Detail";
  if (kind === "asset") {
    const a = getAsset(id);
    $("drawerBody").innerHTML = `<div class="detail-grid">${[
      ["Price", fmt.price(a.price)], ["State", a.state || "--"], ["Phase", a.phase || "--"], ["Posterior", fmt.n(Math.max(a.posterior || 0, a.confidence || 0), 3)],
      ["EV", fmt.n(a.ev, 3)], ["Spread", `${fmt.n(a.spread_bps, 2)}bps`], ["ATR", fmt.n(a.atr, 4)], ["Policy", a.policy || "--"]
    ].map(([l, v]) => metricBox(l, v)).join("")}</div>
    <div class="timeline">${(state?.decisions || []).filter((d) => d.asset === a.asset).slice(0, 20).map(decisionItem).join("") || `<div class="empty">No decisions.</div>`}</div>`;
  } else {
    const a = getAgent(id);
    $("drawerBody").innerHTML = `<div class="detail-grid">${[
      ["Role", a.role || "--"], ["Status", a.status || "--"], ["Score", fmt.pct(a.score || 0)], ["Selected", a.selected || 0],
      ["Rejected", a.rejected || 0], ["Approved", a.approved || 0], ["Blocked", a.blocked || 0], ["Updated", fmt.ageFromTs(a.last_update)]
    ].map(([l, v]) => metricBox(l, v)).join("")}</div><pre class="json-block">${esc(JSON.stringify(a.detail || {}, null, 2))}</pre>`;
  }
  $("drawerBackdrop").classList.remove("hidden");
  $("drillDrawer").classList.remove("hidden");
}

function wireDynamicClicks() {
  document.querySelectorAll(".selection-row, .tape-item").forEach((el) => {
    el.onclick = () => {
      const asset = el.dataset.asset;
      if (asset) {
        selectedAsset = asset;
        openDrawer("asset", asset);
      }
    };
  });
}

function renderAll() {
  if (!state) return;
  renderHealth();
  renderAssetSelect();
  renderSummary();
  renderCharts();
  renderOverview();
  if (activeView === "agents") renderAgents();
  if (activeView === "assets") renderAssets();
  if (activeView === "risk") renderRisk();
  if (activeView === "execution") renderExecution();
  if (activeView === "telemetry") renderTelemetry();
  wireDynamicClicks();
}

async function refresh() {
  const res = await fetch("/api/state");
  state = await res.json();
  try {
    const d = await fetch("/api/diagnostics");
    diagnostics = await d.json();
  } catch (err) {
    diagnostics = null;
  }
  renderAll();
}

function init() {
  document.querySelectorAll(".nav-btn").forEach((btn) => btn.onclick = () => setView(btn.dataset.view));
  $("refreshBtn").onclick = () => refresh();
  $("themeBtn").onclick = () => document.body.classList.toggle("light");
  $("assetSelect").onchange = (e) => { selectedAsset = e.target.value; renderAll(); };
  $("assetSearch").oninput = () => renderAssets();
  $("drawerClose").onclick = () => {
    $("drawerBackdrop").classList.add("hidden");
    $("drillDrawer").classList.add("hidden");
  };
  $("drawerBackdrop").onclick = $("drawerClose").onclick;
  const initial = (location.hash || "#overview").replace("#", "");
  setView(["overview", "agents", "assets", "risk", "execution", "telemetry"].includes(initial) ? initial : "overview");
  window.addEventListener("hashchange", () => {
    const next = (location.hash || "#overview").replace("#", "");
    if (["overview", "agents", "assets", "risk", "execution", "telemetry"].includes(next) && next !== activeView) {
      setView(next);
    }
  });
  refresh();
  setInterval(refresh, 2500);
}

init();

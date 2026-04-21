/* ── App State ───────────────────────────────────────────────────────────── */
let activeStream = null;

function apiBase() {
  return document.getElementById("apiBase").value.trim().replace(/\/$/, "");
}

/* ── Utilities ───────────────────────────────────────────────────────────── */
function fmt(n) {
  if (n === null || n === undefined || n === "—") return "—";
  return Number(n).toLocaleString();
}

function timeAgo(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  const diff = Math.floor((Date.now() - d) / 1000);
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  return `${Math.floor(diff / 3600)}h ago`;
}

function setFlowStep(step, state) {
  const ids = { upload: "flow-upload", spark: "flow-spark", dbt: "flow-dbt", done: "flow-done" };
  const el = document.getElementById(ids[step] || step);
  if (!el) return;
  el.classList.remove("active", "done", "error");
  if (state) el.classList.add(state);
}

function resetFlow() {
  ["upload", "spark", "dbt", "done"].forEach(s => setFlowStep(s, null));
}

/* ── Log Output ──────────────────────────────────────────────────────────── */
function appendLog(text, color) {
  const box = document.getElementById("logBox");
  const placeholder = box.querySelector(".log-placeholder");
  if (placeholder) placeholder.remove();

  const line = document.createElement("div");
  const t = text.trim();

  // Auto color-code log lines
  if (color) {
    line.className = `log-line-${color}`;
  } else if (/error|exception|traceback|failed/i.test(t)) {
    line.className = "log-line-error";
  } else if (/warning|warn/i.test(t)) {
    line.className = "log-line-warn";
  } else if (/info|success|done|completed/i.test(t)) {
    line.className = "log-line-info";
  } else if (/\[EXIT:0\]/.test(t)) {
    line.className = "log-line-success";
    line.textContent = "✓ Process exited successfully.";
    box.appendChild(line);
    box.scrollTop = box.scrollHeight;
    return;
  } else if (/\[EXIT:\d+\]/.test(t)) {
    line.className = "log-line-error";
    line.textContent = `✗ Process exited with error: ${t}`;
    box.appendChild(line);
    box.scrollTop = box.scrollHeight;
    return;
  }

  line.textContent = text || " ";
  box.appendChild(line);
  box.scrollTop = box.scrollHeight;
}

function clearLog() {
  document.getElementById("logBox").innerHTML =
    '<div class="log-placeholder">Log cleared.</div>';
}

/* ── Service Status ──────────────────────────────────────────────────────── */
async function refreshStatus() {
  const vm = apiBase();
  document.getElementById("link-metabase").href = vm.replace("8000", "3000");

  const globalEl = document.getElementById("globalStatus");
  let allOk = true;

  try {
    const res = await fetch(`${vm}/status`, { signal: AbortSignal.timeout(6000) });
    if (!res.ok) throw new Error("non-200");
    const data = await res.json();

    const mapping = {
      api:      "svc-api",
      airflow:  "svc-airflow",
      minio:    "svc-minio",
      postgres: "svc-postgres",
    };

    for (const [key, elId] of Object.entries(mapping)) {
      const card = document.getElementById(elId);
      const badge = card.querySelector(".svc-badge");
      const status = data[key] || "offline";
      card.className = `service-card status-${status}`;
      badge.textContent = status;
      if (status !== "ok") allOk = false;
    }

    globalEl.className = `global-status status-${allOk ? "ok" : "degraded"}`;
    globalEl.querySelector("span").textContent = allOk ? "All Systems OK" : "Degraded";
    document.getElementById("lastRefresh").textContent =
      `Refreshed ${new Date().toLocaleTimeString()}`;

  } catch (e) {
    // API unreachable
    ["svc-api", "svc-airflow", "svc-minio", "svc-postgres"].forEach(id => {
      const card = document.getElementById(id);
      card.className = "service-card status-offline";
      card.querySelector(".svc-badge").textContent = "offline";
    });
    globalEl.className = "global-status status-offline";
    globalEl.querySelector("span").textContent = "API Offline";
  }
}

/* ── Metrics ─────────────────────────────────────────────────────────────── */
async function refreshMetrics() {
  const vm = apiBase();
  try {
    const [pgRes, s3Res] = await Promise.all([
      fetch(`${vm}/postgres/metrics`, { signal: AbortSignal.timeout(8000) }),
      fetch(`${vm}/minio/buckets`,    { signal: AbortSignal.timeout(8000) }),
    ]);
    const pg = await pgRes.json();
    const s3 = await s3Res.json();

    document.getElementById("m-total").textContent    = fmt(pg.staging_total_rows);
    document.getElementById("m-positive").textContent = fmt((pg.sentiment_breakdown || {}).positive);
    document.getElementById("m-negative").textContent = fmt((pg.sentiment_breakdown || {}).negative);
    document.getElementById("m-lastingest").textContent = timeAgo(pg.last_ingest);

    // MinIO total files
    const totalFiles = (s3.buckets || []).reduce((a, b) => a + (b.file_count > 0 ? b.file_count : 0), 0);
    document.getElementById("m-buckets").textContent = fmt(totalFiles);

    // Platform breakdown
    const pb = pg.platform_breakdown || {};
    document.getElementById("p-shopee").textContent = fmt(pb.shopee);
    document.getElementById("p-lazada").textContent = fmt(pb.lazada);

  } catch (e) {
    // Silently fail — status bar already shows API offline
  }
}

/* ── DAG Status ──────────────────────────────────────────────────────────── */
async function refreshDagStatus() {
  const vm = apiBase();
  try {
    const res = await fetch(`${vm}/airflow/dag-status`, { signal: AbortSignal.timeout(6000) });
    const data = await res.json();

    const badge = document.getElementById("dag-status-badge");
    const runId = document.getElementById("dag-run-id");
    const start = document.getElementById("dag-start");

    const status = data.status || "unknown";
    badge.textContent = status.replace("_", " ");
    badge.className = `dag-badge dag-badge--${status === "success" ? "success" : status === "running" ? "running" : status === "failed" ? "failed" : "unknown"}`;
    runId.textContent = data.run_id || "No runs yet";
    start.textContent = data.start_date ? `Started: ${new Date(data.start_date).toLocaleString()}` : "";
  } catch (e) {
    const badge = document.getElementById("dag-status-badge");
    badge.textContent = "unreachable";
    badge.className = "dag-badge dag-badge--unknown";
  }
}

async function triggerDag() {
  const vm = apiBase();
  try {
    appendLog("Triggering Airflow DAG...", "info");
    const res = await fetch(`${vm}/airflow/trigger`, { method: "POST" });
    const data = await res.json();
    appendLog(`DAG triggered: ${JSON.stringify(data)}`, "success");
    setTimeout(refreshDagStatus, 2000);
  } catch (e) {
    appendLog(`Failed to trigger DAG: ${e.message}`, "error");
  }
}

/* ── Pipeline Step Runner ────────────────────────────────────────────────── */
const stepConfig = {
  upload:   { btn: "btn-upload",  flow: "upload", label: "Upload to MinIO" },
  spark:    { btn: "btn-spark",   flow: "spark",  label: "Spark Processing" },
  dbt:      { btn: "btn-dbt",     flow: "dbt",    label: "dbt Transform" },
  "run-all":{ btn: "btn-all",     flow: null,     label: "Full Pipeline" },
};

async function runStep(step) {
  const vm = apiBase();
  const cfg = stepConfig[step];
  if (!cfg) return;

  const btn = document.getElementById(cfg.btn);
  btn.classList.add("running");
  btn.disabled = true;
  resetFlow();
  if (cfg.flow) setFlowStep(cfg.flow, "active");

  clearLog();
  appendLog(`▶ Starting ${cfg.label}...`, "info");

  try {
    const res = await fetch(`${vm}/pipeline/${step}`, { method: "POST" });
    const data = await res.json();

    if (step === "run-all") {
      for (const [stage, result] of Object.entries(data)) {
        if (["upload", "spark", "dbt"].includes(stage)) {
          appendLog(`\n── ${stage.toUpperCase()} ──`, "info");
          if (result.stdout) appendLog(result.stdout);
          if (result.stderr) appendLog(result.stderr, "warn");
          setFlowStep(stage, result.success ? "done" : "error");
          if (!result.success) {
            appendLog(`✗ Pipeline stopped at: ${stage}`, "error");
            break;
          }
        }
      }
      if (!data.stopped_at) setFlowStep("done", "done");
    } else {
      if (data.stdout) appendLog(data.stdout);
      if (data.stderr) appendLog(data.stderr, "warn");
      if (data.success) {
        appendLog(`✓ ${cfg.label} completed successfully.`, "success");
        if (cfg.flow) setFlowStep(cfg.flow, "done");
      } else {
        appendLog(`✗ ${cfg.label} failed (exit ${data.returncode}).`, "error");
        if (cfg.flow) setFlowStep(cfg.flow, "error");
      }
    }

    // Refresh metrics after run
    setTimeout(refreshMetrics, 1500);
    setTimeout(refreshDagStatus, 2000);

  } catch (e) {
    appendLog(`Network error calling API: ${e.message}`, "error");
    if (cfg.flow) setFlowStep(cfg.flow, "error");
  } finally {
    btn.classList.remove("running");
    btn.disabled = false;
  }
}

/* ── Live Log Streaming (SSE) ────────────────────────────────────────────── */
function startStream(type) {
  if (activeStream) {
    activeStream.close();
    activeStream = null;
  }

  clearLog();
  appendLog(`↺ Connecting to ${type} live stream...`, "info");

  const url = `${apiBase()}/logs/stream/${type}`;
  const es = new EventSource(url);
  activeStream = es;

  es.onmessage = (e) => {
    appendLog(e.data);
    if (e.data.startsWith("[EXIT:")) {
      es.close();
      activeStream = null;
    }
  };
  es.onerror = () => {
    appendLog("Stream disconnected.", "warn");
    es.close();
    activeStream = null;
  };
}

/* ── Init & Auto-Refresh ─────────────────────────────────────────────────── */
async function fullRefresh() {
  await refreshStatus();
  await refreshMetrics();
  await refreshDagStatus();
}

// Boot
fullRefresh();

// Auto-refresh every 30 seconds
setInterval(fullRefresh, 30_000);

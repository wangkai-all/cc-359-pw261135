/* Refract UI — no framework, no build step.
   All data from the API is untrusted (filenames, receipts, client labels):
   inserted with textContent only. */

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

const state = {
  snapshot: null,
  filter: "all",
  autoRefresh: true,
  pollTimer: null,
  demoRunning: false,
  inflight: new Set(),
};

const SOURCE_VARS = ["--series-1", "--series-2", "--series-3", "--series-4"];

/* ---------------------------------------------------------------- utils */

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined && text !== null) node.textContent = String(text);
  return node;
}

function clear(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
}

function bytes(n) {
  if (!Number.isFinite(n)) return "—";
  if (n < 1024) return `${n} B`;
  const units = ["KiB", "MiB", "GiB"];
  let value = n / 1024;
  let i = 0;
  while (value >= 1024 && i < units.length - 1) {
    value /= 1024;
    i += 1;
  }
  return `${value >= 10 ? value.toFixed(0) : value.toFixed(1)} ${units[i]}`;
}

function clockTime(ts) {
  if (!ts) return "—";
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function dateTime(ts) {
  if (!ts) return "—";
  const d = new Date(ts * 1000);
  return d.toLocaleString([], {
    month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit",
  });
}

function duration(seconds) {
  seconds = Math.max(0, Math.round(seconds));
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
  const h = Math.floor(seconds / 3600);
  return `${h}h ${Math.floor((seconds % 3600) / 60)}m`;
}

function rel(ts, now) {
  const delta = ts - now;
  if (Math.abs(delta) < 2) return "now";
  return delta >= 0 ? `in ${duration(delta)}` : `${duration(-delta)} ago`;
}

function sourceColor(slot) {
  return `var(${SOURCE_VARS[(Math.max(1, slot) - 1) % SOURCE_VARS.length]})`;
}

let toastTimer = null;
function toast(message, kind = "") {
  const node = $("#toast");
  node.textContent = message;
  node.className = `toast show ${kind}`.trim();
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => {
    node.className = "toast";
  }, 4200);
}

async function api(path, options = {}) {
  const response = await fetch(path, { cache: "no-store", ...options });
  const contentType = response.headers.get("Content-Type") || "";
  let body = null;
  if (contentType.includes("application/json")) {
    body = await response.json();
  }
  if (!response.ok) {
    const error = new Error((body && body.error && body.error.message) || `HTTP ${response.status}`);
    error.code = body && body.error && body.error.code;
    error.status = response.status;
    throw error;
  }
  return body;
}

function jsonPost(path, payload) {
  return api(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
  });
}

async function sha256Hex(buffer) {
  const digest = await crypto.subtle.digest("SHA-256", buffer);
  return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, "0")).join("");
}

/* ------------------------------------------------------------ downloading */

async function readChunk(leaseId, offset, size) {
  const response = await fetch(
    `/api/leases/${leaseId}/chunk?offset=${offset}&limit=${size}`,
    { cache: "no-store" },
  );
  if (!response.ok) {
    let message = `HTTP ${response.status}`;
    let code = "http_error";
    try {
      const body = await response.json();
      message = body.error.message;
      code = body.error.code;
    } catch (_) { /* binary or empty */ }
    const error = new Error(message);
    error.code = code;
    error.status = response.status;
    throw error;
  }
  return {
    data: new Uint8Array(await response.arrayBuffer()),
    next: Number(response.headers.get("X-Refract-Next") || offset),
    total: Number(response.headers.get("X-Refract-Total") || 0),
    sha256: response.headers.get("X-Refract-Sha256") || "",
  };
}

/**
 * Source-scoped download: acquire a delivery lease, then pull verified chunks.
 * A recall, expiry or purge turns the next chunk into 410 and we fail closed.
 */
async function downloadVia(deliveryId, options = {}) {
  const {
    chunkSize = 64 * 1024,
    delayMs = 0,
    client = "browser",
    onProgress = () => {},
    halfWay = null,
    save = true,
  } = options;

  const lease = await jsonPost("/api/leases", {
    delivery_id: deliveryId,
    client,
    ttl: Math.max(15, Math.ceil(((state.snapshot ? state.snapshot.lease_ttl : 30) || 30))),
  });

  const parts = [];
  let offset = 0;
  let chunkCount = 0;
  const total = lease.size;
  onProgress({ offset, total, lease });

  try {
    while (offset < total) {
      if (delayMs) await new Promise((r) => setTimeout(r, delayMs));
      const chunk = await readChunk(lease.lease_id, offset, chunkSize);
      parts.push(chunk.data);
      offset = chunk.next;
      chunkCount += 1;
      onProgress({ offset, total, lease });
      if (halfWay && chunkCount === 1) {
        await halfWay({ lease, offset, total });
      }
      if (chunk.data.length === 0) break;
    }
  } catch (error) {
    // A fenced/expired/purged lease is already recorded; mark the reader
    // aborted so it stops counting as an active download at the next tick.
    await jsonPost(`/api/leases/${lease.lease_id}/finish`, {
      state: "aborted",
      bytes_sent: offset,
      detail: error.code || "client aborted",
    }).catch(() => {});
    throw error;
  }

  await jsonPost(`/api/leases/${lease.lease_id}/finish`, {
    state: "released",
    bytes_sent: offset,
  }).catch(() => {});

  const blob = new Blob(parts, { type: lease.media_type || "application/octet-stream" });
  const assembled = await sha256Hex(await blob.arrayBuffer());
  const verified = assembled === lease.sha256;
  if (save) {
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = lease.filename || "package.bin";
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    setTimeout(() => URL.revokeObjectURL(url), 10000);
  }
  return { lease, blob, bytes: offset, verified, sha256: assembled };
}

/* -------------------------------------------------------------- rendering */

function renderKpis(snap) {
  const k = snap.kpis;
  $("#kpi-blobs").textContent = k.physical_blobs;
  $("#kpi-blobs-sub").textContent = `${k.deliveries_total} deliveries · content-addressed`;
  $("#kpi-bytes").textContent = bytes(k.physical_bytes);
  $("#kpi-bytes-sub").textContent = k.purged_packages
    ? `${k.purged_packages} purged tombstone${k.purged_packages === 1 ? "" : "s"}`
    : "content-addressed";
  $("#kpi-refs").textContent = k.valid_references;
  $("#kpi-refs-sub").textContent = `across ${k.deliveries_total} deliveries`;
  $("#kpi-blocked").textContent = k.blocked_cleanups;
  $("#kpi-blocked-sub").textContent = `${k.active_leases} active download${k.active_leases === 1 ? "" : "s"}`;
  $("#kpi-ready").textContent = k.cleanup_ready;
  $("#kpi-ready-sub").textContent = "no refs · no readers";
  $(".stat-tile:nth-child(4)").classList.toggle("is-blocked", k.blocked_cleanups > 0);
  $(".stat-tile:nth-child(5)").classList.toggle("is-ready", k.cleanup_ready > 0);
}

function renderSources(snap) {
  const list = $("#sources");
  clear(list);
  for (const source of snap.sources) {
    const li = el("li");
    const dot = el("span", "source-dot");
    dot.style.setProperty("--dot", sourceColor(source.color_slot));
    const body = el("div");
    body.appendChild(el("div", "name", source.display_name));
    body.appendChild(el("div", "note", source.note));
    const count = el("div", "count");
    count.appendChild(document.createTextNode(String(source.valid_references)));
    count.appendChild(el("span", "", "valid refs"));
    li.append(dot, body, count);
    list.appendChild(li);
  }
}

const EVENT_TONES = {
  purged: "bad",
  recalled: "bad",
  lease_abandoned: "bad",
  lease_expired: "bad",
  blob_missing: "bad",
  retention_elapsed: "bad",
  received: "good",
  stream_released: "good",
  server_started: "",
};

function renderEvents(snap) {
  const list = $("#events");
  clear(list);
  for (const event of snap.events) {
    const li = el("li");
    li.appendChild(el("div", "event-time", clockTime(event.at)));
    const body = el("div", "event-body");
    const action = el("div", "action", event.action.replace(/_/g, " "));
    if (EVENT_TONES[event.action]) action.dataset.tone = EVENT_TONES[event.action];
    body.appendChild(action);
    body.appendChild(el("div", "entity", `${event.entity} ${String(event.entity_id).slice(0, 10)}`));
    if (event.detail) body.appendChild(el("div", "detail", event.detail));
    li.appendChild(body);
    list.appendChild(li);
  }
}

function deliveryNode(delivery, pkg, snap, now) {
  const node = $("#delivery-template").content.firstElementChild.cloneNode(true);
  const dot = $(".source-dot", node);
  dot.style.setProperty("--dot", sourceColor(delivery.color_slot));
  $(".source-name", node).textContent = delivery.source_name;

  const badge = $(".delivery-status", node);
  badge.textContent = delivery.status;
  badge.dataset.state = delivery.status;
  if (delivery.status === "recalled") node.classList.add("is-recalled");

  const line = $(".delivery-line", node);
  line.textContent =
    `receipt ${delivery.receipt_no} · received ${dateTime(delivery.received_at)} · ` +
    (delivery.retain_until > now
      ? `retain ${rel(delivery.retain_until, now)}`
      : `retention ended ${duration(now - delivery.retain_until)} ago`);

  const sub = $(".delivery-sub", node);
  if (delivery.recalled_at) {
    sub.appendChild(document.createTextNode("recalled "));
    sub.appendChild(document.createTextNode(dateTime(delivery.recalled_at)));
    sub.appendChild(document.createTextNode(" · "));
    const why = el("span", "why", delivery.recall_reason || "source recall");
    sub.appendChild(why);
  } else if (delivery.status === "expired") {
    sub.textContent =
      `${delivery.filename} · attested ${delivery.observed_sha256.slice(0, 16)}… · no longer a reference`;
  } else {
    sub.textContent = `${delivery.filename} · attested ${delivery.observed_sha256.slice(0, 16)}… · ${delivery.verification}`;
  }

  const download = $(".act-download-source", node);
  download.disabled = !delivery.valid || pkg.tombstone;
  download.addEventListener("click", async () => {
    if (state.inflight.has(delivery.id)) return;
    state.inflight.add(delivery.id);
    download.disabled = true;
    download.textContent = "Downloading…";
    try {
      const result = await downloadVia(delivery.id, { client: "browser", onProgress: () => {} });
      toast(
        result.verified
          ? `Verified ${bytes(result.bytes)} via ${delivery.source_name}`
          : "Download finished but the digest did not match",
        result.verified ? "" : "error",
      );
    } catch (error) {
      toast(`${delivery.source_name}: ${error.message}`, "error");
    } finally {
      state.inflight.delete(delivery.id);
      download.textContent = "Download via this source";
      download.disabled = !delivery.valid;
      refresh();
    }
  });

  const recall = $(".act-recall", node);
  recall.classList.add("danger");
  recall.disabled = !!delivery.recalled_at;
  recall.addEventListener("click", async () => {
    if (!confirm(`Recall this delivery from ${delivery.source_name}? Its downloads stop immediately; other sources keep their access.`)) return;
    try {
      await jsonPost(`/api/deliveries/${delivery.id}/recall`, { reason: "source takedown (demo)" });
      toast(`Recalled ${delivery.source_name} · ${delivery.receipt_no}`);
      refresh();
    } catch (error) {
      toast(error.message, "error");
    }
  });

  const end = $(".act-end-retention", node);
  end.disabled = !delivery.valid;
  end.title = "Demo shortcut: move this source's retention deadline to now";
  end.addEventListener("click", async () => {
    try {
      await jsonPost(`/api/deliveries/${delivery.id}/end-retention`, {
        note: "retention ended from dashboard",
      });
      toast(`Retention ended for ${delivery.source_name}`);
      refresh();
    } catch (error) {
      toast(error.message, "error");
    }
  });

  return node;
}

function packageNode(pkg, snap) {
  const now = snap.now;
  const node = $("#package-template").content.firstElementChild.cloneNode(true);
  const title = pkg.filenames.length === 1 ? pkg.filenames[0] : (pkg.filenames[0] || "package") +
    (pkg.filenames.length > 1 ? ` +${pkg.filenames.length - 1}` : "");
  $(".package-title", node).textContent = title;

  const badge = $(".disposition", node);
  const label = {
    blocked: "blocked",
    cleanup_ready: "cleanup-ready",
    purged: "purged",
    inconsistent: "inconsistent",
  }[pkg.cleanup.disposition] || pkg.cleanup.disposition;
  badge.textContent = label;
  badge.dataset.state = pkg.cleanup.disposition;

  $(".digest", node).textContent = `sha256:${pkg.short_digest}…`;
  $(".digest", node).title = pkg.digest;
  $(".size", node).textContent = bytes(pkg.size);
  $(".blob-state", node).textContent = pkg.tombstone
    ? "bytes purged"
    : pkg.physical
      ? `physical blob · ${pkg.reference_count} valid ref${pkg.reference_count === 1 ? "" : "s"}`
      : "bytes missing";

  const list = $(".delivery-list", node);
  for (const delivery of pkg.deliveries) {
    list.appendChild(deliveryNode(delivery, pkg, snap, now));
  }

  const topDownload = $(".act-download", node);
  const preferred = pkg.deliveries.find((d) => d.valid);
  topDownload.disabled = !preferred || pkg.tombstone;
  topDownload.addEventListener("click", () => {
    if (preferred) $(".act-download-source", list.children[pkg.deliveries.indexOf(preferred)]).click();
  });

  const cleanupBtn = $(".act-cleanup", node);
  cleanupBtn.disabled = !pkg.physical;
  cleanupBtn.title = "Run the global cleanup; this package is purged only if every gate is open";
  cleanupBtn.addEventListener("click", () => runCleanup());

  $(".cleanup-reason", node).textContent = pkg.cleanup.reason;

  const blockers = $(".blocker-list", node);
  for (const blocker of pkg.cleanup.blockers) {
    const item = el("li", "blocker");
    item.dataset.kind = blocker.kind;
    const strong = el("strong", "", {
      reference: "valid reference",
      download: "active reader",
      missing: "missing bytes",
    }[blocker.kind] || blocker.kind);
    item.appendChild(strong);
    item.appendChild(document.createTextNode(` — ${blocker.detail}`));
    blockers.appendChild(item);
  }
  if (!pkg.cleanup.blockers.length) {
    blockers.appendChild(el("li", "empty-line", "No blockers."));
  }

  const leases = $(".lease-list", node);
  if (!pkg.leases.length) {
    leases.appendChild(el("li", "empty-line", "No download grants yet."));
  }
  for (const lease of pkg.leases) {
    const item = el("li", "lease-item");
    item.dataset.streaming = String(lease.streaming && lease.state === "active");
    const left = el("div");
    const who = el("div", "who");
    who.textContent = `${lease.client || "unnamed"} · ${lease.source_name || "?"}`;
    left.appendChild(who);
    const meta = el("div", "muted");
    const pct = pkg.size ? Math.min(100, Math.round((lease.bytes_sent / pkg.size) * 100)) : 0;
    meta.textContent = lease.state === "active"
      ? `${lease.streaming ? "streaming" : "open"} · ${bytes(lease.bytes_sent)} · renews ${rel(lease.expires_at, now)}`
      : `${lease.state} · ${bytes(lease.bytes_sent)}${lease.fence_reason ? ` · ${lease.fence_reason}` : ""}`;
    left.appendChild(meta);
    const meter = el("div", "meter");
    const fill = el("i");
    fill.style.width = `${pct}%`;
    meter.appendChild(fill);
    left.appendChild(meter);
    const badge = el("span", "badge", lease.state);
    badge.dataset.state = lease.state;
    item.append(left, badge);
    leases.appendChild(item);
  }

  return node;
}

function renderPackages(snap) {
  const host = $("#packages");
  clear(host);
  const filter = state.filter;
  const visible = snap.packages.filter((pkg) => {
    if (filter === "all") return true;
    if (filter === "blocked") return pkg.cleanup.disposition === "blocked";
    if (filter === "ready") return pkg.cleanup.disposition === "cleanup_ready";
    if (filter === "purged") return pkg.tombstone;
    return true;
  });
  for (const pkg of visible) host.appendChild(packageNode(pkg, snap));

  $("#packages-empty").hidden = visible.length > 0;
  if (visible.length !== snap.packages.length) {
    $("#packages-empty").querySelector("p").textContent = "No packages match this filter.";
  } else {
    $("#packages-empty").querySelector("p").textContent =
      "Nothing received yet. Upload a file above, or seed a local fixture into two different sources to watch duplicate bytes share one blob.";
  }
  const total = snap.packages.length;
  const refs = snap.kpis.valid_references;
  $("#packages-count").textContent = total
    ? `${total} package${total === 1 ? "" : "s"} · ${snap.kpis.deliveries_total} deliveries · ` +
      `${refs} valid reference${refs === 1 ? "" : "s"}`
    : "No packages yet.";
}

function renderChrome(snap) {
  $("#epoch").textContent = snap.server.epoch_short;
  $("#ttl").textContent = `${snap.server.lease_ttl}s`;
  const select = $("#source-select");
  const current = select.value;
  clear(select);
  for (const source of snap.sources) {
    const option = document.createElement("option");
    option.value = source.id;
    option.textContent = source.display_name;
    select.appendChild(option);
  }
  if (current) select.value = current;

  const fixtures = snap.fixtures || [];
  const demoSelect = $("#demo-fixture");
  const demoCurrent = demoSelect.value;
  clear(demoSelect);
  for (const name of fixtures) {
    const option = document.createElement("option");
    option.value = name;
    option.textContent = name;
    demoSelect.appendChild(option);
  }
  if (demoCurrent && fixtures.includes(demoCurrent)) demoSelect.value = demoCurrent;
  else if (fixtures.includes("array-capture.bin")) demoSelect.value = "array-capture.bin";

  const host = $("#fixture-buttons");
  clear(host);
  for (const name of fixtures) {
    const button = el("button", "", name);
    button.type = "button";
    button.title = "Seed this local fixture into the selected source";
    button.addEventListener("click", () => seedFixture(name, select.value));
    host.appendChild(button);
  }
}

function render(snap) {
  state.snapshot = snap;
  renderChrome(snap);
  renderKpis(snap);
  renderSources(snap);
  renderEvents(snap);
  renderPackages(snap);
}

/* ---------------------------------------------------------------- actions */

async function refresh() {
  try {
    const snap = await api("/api/state");
    render(snap);
  } catch (error) {
    console.error(error);
  }
}

async function runCleanup() {
  try {
    const out = await api("/api/cleanup", { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
    if (out.purged.length) {
      toast(`Purged ${out.purged.length} package(s) · ${bytes(out.purged_bytes)} freed`);
    } else if (out.blocked.length) {
      const blockers = out.blocked.reduce((n, b) => n + b.blockers.length, 0);
      toast(`Cleanup ran: nothing purged · ${blockers} blocker${blockers === 1 ? "" : "s"}`);
    } else {
      toast("Cleanup ran: no packages to purge");
    }
    await refresh();
    return out;
  } catch (error) {
    toast(error.message, "error");
    throw error;
  }
}

function generateReceipt(prefix) {
  return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
}

async function seedFixture(name, source, options = {}) {
  const receipt = options.receipt || generateReceipt("seed");
  const status = $("#upload-status");
  try {
    const out = await jsonPost("/api/seed-fixtures", {
      fixture: name,
      source,
      receipt,
      retain_seconds: options.retain ?? 300,
      recall_after_seconds: options.recallAfter ?? null,
      attestation: options.attestation || `local fixture ${name}`,
    });
    status.className = "panel-status ok";
    status.textContent = out.duplicate_bytes
      ? `Received ${name} from ${source} · identical bytes reused the existing physical blob · recipe ${receipt}`
      : out.resurrected
        ? `Received ${name} from ${source} · purged bytes were resurrected · receipt ${receipt}`
        : `Received ${name} from ${source} · new physical blob stored · receipt ${receipt}`;
    await refresh();
    return out;
  } catch (error) {
    status.className = "panel-status error";
    status.textContent = error.message;
    throw error;
  }
}

async function uploadFile(event) {
  event.preventDefault();
  const form = $("#upload-form");
  const button = $("#upload-btn");
  const status = $("#upload-status");
  const file = $("#file-input").files[0];
  if (!file) return;
  button.disabled = true;
  status.className = "panel-status";
  status.textContent = `Reading and hashing ${file.name} locally…`;
  try {
    const buffer = await file.arrayBuffer();
    const digest = await sha256Hex(buffer);
    status.textContent = `Uploading ${file.name} (${bytes(file.size)}) · sha256 ${digest.slice(0, 16)}…`;
    const params = new URLSearchParams({
      source: form.source.value,
      receipt: form.receipt.value || generateReceipt("web"),
      retain: form.retain.value || "300",
      filename: file.name,
      sha256: digest,
      attestation: `browser upload of ${file.name}`,
    });
    if (form.recall_after.value) params.set("recall_after", form.recall_after.value);
    const out = await api(`/api/uploads?${params.toString()}`, {
      method: "POST",
      headers: {
        "Content-Type": file.type || "application/octet-stream",
        "Content-Length": String(buffer.byteLength),
      },
      body: buffer,
    });
    status.className = "panel-status ok";
    status.textContent = out.duplicate_bytes
      ? `Stored delivery ${out.delivery.receipt_no} · verified sha256 · bytes shared with an existing blob`
      : out.resurrected
        ? `Stored delivery ${out.delivery.receipt_no} · verified sha256 · purged bytes resurrected`
        : `Stored delivery ${out.delivery.receipt_no} · verified sha256 · new physical blob`;
    form.receipt.value = "";
    form.file.value = "";
    await refresh();
  } catch (error) {
    status.className = "panel-status error";
    status.textContent = error.message;
  } finally {
    button.disabled = false;
  }
}

async function restartServer() {
  const out = await jsonPost("/api/simulate/restart", {});
  const abandoned = out.recovery.abandoned_leases.length;
  toast(
    `Server restarted · epoch ${out.epoch_short} · ${abandoned} abandoned lease${abandoned === 1 ? "" : "s"}`,
    abandoned ? "error" : "",
  );
  await refresh();
  return out;
}

async function advanceClock(seconds) {
  try {
    await jsonPost("/api/clock/advance", { seconds });
    toast(`Ledger clock advanced ${duration(seconds)}`);
    await refresh();
  } catch (error) {
    toast(error.message, "error");
  }
}

/* ------------------------------------------------------------------- demo */

function setStep(step, text, tone = "") {
  const node = $(`#demo-steps [data-step="${step}"]`);
  const result = $(".step-result", node);
  result.textContent = text;
  result.dataset.tone = tone;
  node.dataset.done = "1";
}

function resetSteps() {
  for (const node of $$("#demo-steps li")) {
    node.dataset.done = "";
    const result = $(".step-result", node);
    result.textContent = "";
    result.dataset.tone = "";
  }
}

async function runDemo() {
  if (state.demoRunning) return;
  state.demoRunning = true;
  const button = $("#btn-demo");
  button.disabled = true;
  const runState = $("#demo-runstate");
  const fixture = $("#demo-fixture").value;
  if (!fixture) {
    toast("No fixture available", "error");
    state.demoRunning = false;
    button.disabled = false;
    return;
  }
  resetSteps();
  runState.textContent = `running · ${fixture}`;
  const stamp = Date.now().toString(36);

  try {
    /* 1 — duplicate delivery */
    runState.textContent = "1/4 duplicate delivery";
    const atlas = await seedFixture(fixture, "atlas", {
      receipt: `demo-atlas-${stamp}`,
      retain: 600,
      attestation: "guided demo · atlas receipt",
    });
    const harbor = await seedFixture(fixture, "harbor", {
      receipt: `demo-harbor-${stamp}`,
      retain: 600,
      recallAfter: 3600,
      attestation: "guided demo · harbor receipt",
    });
    const sameBytes = atlas.package_digest === harbor.package_digest;
    setStep(
      "duplicate",
      `${sameBytes ? "shared" : "!! different"} blob ${atlas.package_digest.slice(0, 12)}… · ` +
        `atlas ${atlas.duplicate_bytes ? "reused" : "stored"} · harbor ${harbor.duplicate_bytes ? "reused" : "stored"} · ` +
        `2 deliveries, 1 physical file`,
      sameBytes ? "good" : "bad",
    );
    const atlasId = atlas.delivery.id;
    const harborId = harbor.delivery.id;

    /* 2 — recall one source only */
    runState.textContent = "2/4 recall one source";
    await jsonPost(`/api/deliveries/${harborId}/recall`, {
      reason: "guided demo: harbor takedown",
    });
    let harborBlocked = false;
    try {
      await jsonPost("/api/leases", { delivery_id: harborId, client: "probe" });
    } catch (error) {
      harborBlocked = error.code === "delivery_recalled";
    }
    const atlasLease = await jsonPost("/api/leases", {
      delivery_id: atlasId,
      client: "probe",
      ttl: 20,
    });
    let probeBytes = 0;
    try {
      const probe = await readChunk(atlasLease.lease_id, 0, 4096);
      probeBytes = probe.data.length;
    } finally {
      await jsonPost(`/api/leases/${atlasLease.lease_id}/finish`, {
        state: "released",
        bytes_sent: probeBytes,
      }).catch(() => {});
    }
    await refresh();
    const atlasPkg = state.snapshot.packages.find((p) => p.digest === atlas.package_digest);
    const atlasStillValid = atlasPkg && atlasPkg.reference_count >= 1;
    const blockedNames = atlasPkg.cleanup.blockers
      .filter((b) => b.kind === "reference")
      .map((b) => b.source_name)
      .join(", ");
    setStep(
      "recall",
      `harbor access ${harborBlocked ? "refused (410)" : "!! still open"} · ` +
        `atlas still serves ${probeBytes} bytes · cleanup blocked by ${blockedNames || "nothing"} · ` +
        `harbor recall does not delete atlas's reference`,
      harborBlocked && atlasStillValid ? "good" : "bad",
    );

    /* 3 — end retention during a slow, live download, then restart */
    runState.textContent = "3/4 slow download + restart";
    const slowChunks = 8;
    const slowChunkSize = Math.max(32, Math.ceil((atlasPkg ? atlasPkg.size : 4096) / slowChunks));
    let slowFailure = null;
    let leaseId = null;
    let midCleanup = null;
    let midBlockers = null;
    const slow = downloadVia(atlasId, {
      chunkSize: slowChunkSize,
      delayMs: 90,
      client: "slow-reader",
      save: false,
      halfWay: async ({ lease }) => {
        leaseId = lease.lease_id;
        await jsonPost(`/api/deliveries/${atlasId}/end-retention`, {
          note: "guided demo: atlas retention ends mid-download",
        });
        await refresh();
        const midPkg = state.snapshot.packages.find((p) => p.digest === atlas.package_digest);
        midBlockers = midPkg
          ? {
              download: !!midPkg.cleanup.blockers.find((b) => b.kind === "download"),
              reference: !!midPkg.cleanup.blockers.find((b) => b.kind === "reference"),
            }
          : { download: false, reference: false };
        midCleanup = await jsonPost("/api/cleanup", {});
        if (midCleanup.purged.some((p) => p.digest === atlas.package_digest)) {
          throw new Error("cleanup purged a blob with a live reader");
        }
        await restartServer();
      },
    });
    try {
      await slow;
      slowFailure = new Error("download completed although the server restarted");
    } catch (error) {
      slowFailure = error;
    }
    await refresh();
    const afterRestartPkg = state.snapshot.packages.find((p) => p.digest === atlas.package_digest);
    const abandoned = afterRestartPkg
      ? afterRestartPkg.leases.find((l) => l.id === leaseId)
      : null;
    const failedClosed = !!slowFailure && slowFailure.status === 410;
    const abandonedLease = !!abandoned && abandoned.state === "abandoned";
    const midSafe = !!midBlockers && midBlockers.download && !midBlockers.reference &&
      !!midCleanup && !midCleanup.purged.some((p) => p.digest === atlas.package_digest);
    setStep(
      "restart",
      `after retention ended · cleanup blocked by ${midBlockers && midBlockers.download ? "active reader" : "!! no reader"} · ` +
        `reader failed closed: ${slowFailure ? `${slowFailure.status || "!"} ${slowFailure.code || slowFailure.message}` : "!! no error"} · ` +
        `lease ${abandoned ? abandoned.state : "missing"} · ` +
        `blob ${afterRestartPkg && afterRestartPkg.physical ? "still present" : "gone"}`,
      midSafe && failedClosed && abandonedLease ? "good" : "bad",
    );

    /* 4 — purge, then resurrect identical bytes */
    runState.textContent = "4/4 purge + resurrect";
    const cleanup = await jsonPost("/api/cleanup", {});
    await refresh();
    const purgedPkg = state.snapshot.packages.find((p) => p.digest === atlas.package_digest);
    const wasPurged = !!purgedPkg && purgedPkg.tombstone;
    const again = await seedFixture(fixture, "orchard", {
      receipt: `demo-orchard-${stamp}`,
      retain: 300,
      attestation: "guided demo · re-delivery after purge",
    });
    const afterPackage = state.snapshot.packages.find((p) => p.digest === atlas.package_digest);
    setStep(
      "release",
      `cleanup purged ${cleanup.purged.length} package(s) · tombstone ${wasPurged ? "recorded" : "!! missing"} · ` +
        `re-delivery by orchard ${again.resurrected ? "resurrected the bytes" : again.duplicate_bytes ? "reused a blob" : "stored a new blob"} · ` +
        `new delivery ${again.delivery.receipt_no} · ${afterPackage ? afterPackage.reference_count : "?"} valid reference`,
      wasPurged && (again.resurrected || again.duplicate_bytes) ? "good" : "bad",
    );
    runState.textContent = "complete";
    toast("Guided demo complete");
  } catch (error) {
    console.error(error);
    runState.textContent = `stopped · ${error.message}`;
    toast(`Demo stopped: ${error.message}`, "error");
  } finally {
    state.demoRunning = false;
    button.disabled = false;
    await refresh();
  }
}

/* ------------------------------------------------------------------- boot */

function wire() {
  $("#upload-form").addEventListener("submit", uploadFile);
  $("#btn-cleanup").addEventListener("click", () => runCleanup().catch(() => {}));
  $("#btn-advance").addEventListener("click", () => advanceClock(3600));
  $("#btn-restart").addEventListener("click", () => restartServer().catch((e) => toast(e.message, "error")));
  $("#btn-demo").addEventListener("click", runDemo);

  for (const button of $$(".seg[data-filter]")) {
    button.addEventListener("click", () => {
      state.filter = button.dataset.filter;
      for (const other of $$(".seg[data-filter]")) {
        other.setAttribute("aria-pressed", String(other === button));
      }
      if (state.snapshot) renderPackages(state.snapshot);
    });
  }

  const toggle = $("#toggle-auto");
  toggle.checked = state.autoRefresh;
  toggle.addEventListener("change", () => {
    state.autoRefresh = toggle.checked;
    schedulePoll();
  });

  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) refresh();
  });
}

function schedulePoll() {
  clearTimeout(state.pollTimer);
  if (!state.autoRefresh) return;
  state.pollTimer = setTimeout(async () => {
    if (!document.hidden && !state.demoRunning) await refresh();
    schedulePoll();
  }, 2500);
}

wire();
refresh().then(() => schedulePoll());

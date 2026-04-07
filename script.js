(() => {
  const FIREBASE_ROOMS_URL =
    "https://group4-project-73093-default-rtdb.firebaseio.com/envi-guard/sessions/rpi5-group4/rooms.json";

  const AUTH_KEY = "enviGuard_auth";
  const AUTH_USER_KEY = "enviGuard_user";
  const AUTH_ROLE_KEY = "enviGuard_role";

  const EMAIL_ENABLED_KEY = "enviGuard_email_enabled";
  const EMAIL_PUBLIC_KEY = "enviGuard_emailjs_public_key";
  const EMAIL_SERVICE_ID_KEY = "enviGuard_emailjs_service_id";
  const EMAIL_TEMPLATE_ID_KEY = "enviGuard_emailjs_template_id";
  const EMAIL_TO_KEY = "enviGuard_email_to";

  const EMAILJS_CDN =
    "https://cdn.jsdelivr.net/npm/@emailjs/browser@4.4.1/dist/email.min.js";

  const DEFAULT_ADMIN_USER = "Admin";
  const DEFAULT_ADMIN_PASS = "Admin123";
  const DEFAULT_OPERATOR_USER = "Operator";
  const DEFAULT_OPERATOR_PASS = "1234";

  const DB_NAME = "enviGuard_db";
  const DB_VERSION = 2;
  const STORE_NAME = "readings";
  const ALARM_EVENTS_STORE = "alarm_events";
  const ALARM_STATE_STORE = "alarm_state";

  const MAX_DISPLAY_POINTS = 1800;
  const MAX_ALARM_ROWS = 5000;

  const TREND_CACHE_KEY = "enviGuard_trend_cache_v3";
  const TREND_CACHE_MAX_MS = 30 * 24 * 60 * 60 * 1000;
  const MAX_LOCAL_POINTS = 8000;

  const GATEWAY_STALE_MS = 6 * 60 * 1000;
  const READING_RETENTION_MS = 90 * 24 * 60 * 60 * 1000;

  const BAD_CONNECTIVITY_STATUSES = new Set([
    "offline",
    "error",
    "down",
    "failed",
    "gateway_down",
    "lost",
    "disconnected"
  ]);

  const TIME_WINDOWS = {
    "1h": { label: "Last 1 hour", ms: 1 * 60 * 60 * 1000 },
    "7h": { label: "Last 7 hours", ms: 7 * 60 * 60 * 1000 },
    "12h": { label: "Last 12 hours", ms: 12 * 60 * 60 * 1000 },
    "24h": { label: "Last 24 hours", ms: 24 * 60 * 60 * 1000 },
    "7d": { label: "Last week", ms: 7 * 24 * 60 * 60 * 1000 },
    "30d": { label: "Last month", ms: 30 * 24 * 60 * 60 * 1000 }
  };

  const $ = (id) => document.getElementById(id);

  const state = {
    auth: {
      loggedIn: false,
      user: null,
      role: null
    },
    devices: {}, // device_id -> room
    latest: {}, // device_id -> { temp_c, humidity_rh, tsMs, timestamp, room }
    selectedDeviceId: null,
    trendWindowKey: "24h",
    alarmWindowKey: "7d",
    thresholds: {
      temp: 35,
      hum: 70
    },
    refreshing: false,
    refreshLockedAt: 0,
    refreshTimer: null,
    dbPromise: null,
    alarmBackfilled: false,
    fetchCount: 0
  };

  function clamp(n, min, max) {
    return Math.max(min, Math.min(max, n));
  }

  function toISO(d) {
    try {
      return d.toISOString();
    } catch {
      return new Date().toISOString();
    }
  }

  function safeNumber(n) {
    const v = Number(n);
    return Number.isFinite(v) ? v : null;
  }

  function normalizeRoomsPayload(json) {
    if (json == null || typeof json !== "object" || Array.isArray(json)) return {};
    const keys = Object.keys(json);
    if (
      keys.length === 1 &&
      json.rooms != null &&
      typeof json.rooms === "object" &&
      !Array.isArray(json.rooms)
    ) {
      return json.rooms;
    }
    if (
      keys.length === 1 &&
      json.data != null &&
      typeof json.data === "object" &&
      !Array.isArray(json.data)
    ) {
      return json.data;
    }
    return json;
  }

  function isGlobalGatewaysDown(latestByDevice) {
    const nums = Object.values(latestByDevice || {})
      .map((d) => d?.gateways_online)
      .filter((x) => typeof x === "number");
    if (!nums.length) return false;
    return nums.every((x) => x <= 0);
  }

  function isDeviceCommOffline(latest, nowMs = Date.now()) {
    if (!latest) return true;
    if (typeof latest.gateways_online === "number" && latest.gateways_online <= 0) return true;
    const st = String(latest.status || "normal").toLowerCase().trim();
    if (BAD_CONNECTIVITY_STATUSES.has(st)) return true;
    const ts = typeof latest.tsMs === "number" ? latest.tsMs : null;
    if (ts == null || !Number.isFinite(ts)) return true;
    return nowMs - ts > GATEWAY_STALE_MS;
  }

  function connectivityAlarmDetail(latest, nowMs = Date.now()) {
    if (!latest) return "No telemetry for this device.";
    if (typeof latest.gateways_online === "number" && latest.gateways_online <= 0) {
      return `Gateways offline (gateways_online=${latest.gateways_online}).`;
    }
    const st = String(latest.status || "normal").toLowerCase().trim();
    if (BAD_CONNECTIVITY_STATUSES.has(st)) return `Device status: ${latest.status}.`;
    const ts = typeof latest.tsMs === "number" ? latest.tsMs : null;
    if (ts == null || !Number.isFinite(ts)) return "Invalid or missing timestamp.";
    const ageMin = Math.floor((nowMs - ts) / 60000);
    if (nowMs - ts > GATEWAY_STALE_MS) {
      return `Stale data (${ageMin} min since last sample) — gateway or radio path may be down.`;
    }
    return "Connectivity issue.";
  }

  function pickFirstNumericField(raw, keys) {
    for (const k of keys) {
      const v = safeNumber(raw?.[k]);
      if (v !== null) return v;
    }
    return null;
  }

  function pickFirstStringField(raw, keys) {
    for (const k of keys) {
      const v = raw?.[k];
      if (v != null && String(v).trim() !== "") return String(v).trim();
    }
    return "";
  }

  /** Optional fields from Firebase if present (RSSI, role, etc.). */
  function extractExtraTelemetry(raw) {
    const extra = {};
    const rssi = pickFirstNumericField(raw, [
      "rssi",
      "lora_rssi",
      "signal_rssi",
      "gw_rssi",
      "radio_rssi",
      "RSSI"
    ]);
    if (rssi !== null) extra.rssi = rssi;
    const snr = pickFirstNumericField(raw, ["snr", "lora_snr", "signal_snr", "gw_snr", "SNR"]);
    if (snr !== null) extra.snr = snr;
    const signalQuality = pickFirstNumericField(raw, [
      "signal_strength",
      "signal_quality",
      "link_quality",
      "lqi"
    ]);
    if (signalQuality !== null) extra.signal_quality = signalQuality;
    const role = pickFirstStringField(raw, [
      "gateway_role",
      "gw_role",
      "redundancy_role",
      "link_role",
      "gateway_type"
    ]);
    if (role) extra.gateway_role_reported = role;
    const gwName = pickFirstStringField(raw, ["gateway_name", "gw_name", "lora_gateway_name", "gateway_label"]);
    if (gwName) extra.gateway_display_name = gwName;
    const redundancy = pickFirstStringField(raw, ["redundancy", "redundancy_mode", "failover_mode", "mesh_role"]);
    if (redundancy) extra.redundancy_mode = redundancy;
    return extra;
  }

  function formatRssiDbm(v) {
    if (typeof v !== "number" || !Number.isFinite(v)) return "—";
    return `${Math.round(v)} dBm`;
  }

  function formatSnrDb(v) {
    if (typeof v !== "number" || !Number.isFinite(v)) return "—";
    return `${v >= 10 ? v.toFixed(0) : v.toFixed(1)} dB`;
  }

  function formatSignalQuality(v) {
    if (typeof v !== "number" || !Number.isFinite(v)) return "";
    if (v >= 0 && v <= 100) return `${Math.round(v)}% link`;
    return String(Math.round(v));
  }

  function normalizeRoleLabel(reported, inferred) {
    const r = String(reported || "").trim();
    if (r) {
      const low = r.toLowerCase();
      if (low.includes("primary") || low === "main" || low === "p") return `Primary (${r})`;
      if (low.includes("redundant") || low.includes("backup") || low.includes("secondary") || low === "r")
        return `Redundant (${r})`;
      return r;
    }
    return inferred || "—";
  }

  function renderDashboardInsights() {
    const hotRoot = $("hotRoomInsights");
    const gwBody = $("gatewaysDetailTableBody");
    const gwNote = $("gatewaysDetailNote");
    if (!hotRoot || !gwBody) return;

    const nowMs = Date.now();
    const deviceIds = Object.keys(state.devices || {});

    const clearHot = (msg) => {
      hotRoot.innerHTML = "";
      const p = document.createElement("p");
      p.className = "insight-empty";
      p.textContent = msg;
      hotRoot.appendChild(p);
    };

    if (!deviceIds.length) {
      clearHot("No sensors yet.");
      gwBody.innerHTML = "";
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = 8;
      td.className = "insight-empty";
      td.textContent = "No gateway data yet.";
      tr.appendChild(td);
      gwBody.appendChild(tr);
      if (gwNote) gwNote.textContent = "";
      return;
    }

    const withTemp = deviceIds
      .map((id) => ({ id, latest: state.latest[id], room: state.devices[id] || "" }))
      .filter((x) => x.latest && typeof x.latest.temp_c === "number");

    hotRoot.innerHTML = "";
    if (!withTemp.length) {
      clearHot("No temperature readings yet.");
    } else {
      let maxT = -Infinity;
      for (const x of withTemp) maxT = Math.max(maxT, x.latest.temp_c);
      const hottest = withTemp.filter((x) => x.latest.temp_c === maxT);
      const wrap = document.createElement("div");
      wrap.className = "hot-room-layout";
      for (const x of hottest) {
        const block = document.createElement("div");
        block.className = "hot-room-block";
        const tempEl = document.createElement("div");
        tempEl.className = "hot-room-temp";
        tempEl.textContent = `${x.latest.temp_c.toFixed(1)} °C`;
        const meta = document.createElement("div");
        meta.className = "hot-room-meta";
        const line1 = document.createElement("div");
        line1.className = "hot-room-line";
        const strong = document.createElement("strong");
        strong.textContent = x.room || "Room";
        line1.appendChild(strong);
        const span = document.createElement("span");
        span.className = "hot-room-device";
        span.textContent = x.id;
        line1.appendChild(span);
        const line2 = document.createElement("div");
        line2.className = "hot-room-sub";
        const hum =
          typeof x.latest.humidity_rh === "number" ? `${x.latest.humidity_rh.toFixed(0)} %RH` : "Humidity —";
        line2.textContent = `${hum} · Last ${formatDateTime(x.latest.tsMs)}`;
        meta.appendChild(line1);
        meta.appendChild(line2);
        block.appendChild(tempEl);
        block.appendChild(meta);
        wrap.appendChild(block);
      }
      if (hottest.length > 1) {
        const tie = document.createElement("p");
        tie.className = "hot-room-tie";
        tie.textContent = `${hottest.length} rooms are tied at this temperature.`;
        wrap.appendChild(tie);
      }
      hotRoot.appendChild(wrap);
    }

    const gatewayKeys = new Set();
    for (const id of deviceIds) {
      const l = state.latest[id];
      const gid = l?.gateway_id != null && String(l.gateway_id).trim() !== "" ? String(l.gateway_id).trim() : "";
      gatewayKeys.add(gid || "__none__");
    }
    const sortedRealIds = Array.from(gatewayKeys)
      .filter((k) => k !== "__none__")
      .sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }));

    gwBody.innerHTML = "";
    const rows = [];

    for (const key of Array.from(gatewayKeys).sort((a, b) => {
      if (a === "__none__") return 1;
      if (b === "__none__") return -1;
      return a.localeCompare(b, undefined, { sensitivity: "base" });
    })) {
      const samples = deviceIds
        .map((id) => ({ id, latest: state.latest[id], room: state.devices[id] || "" }))
        .filter((x) => {
          const gid = x.latest?.gateway_id != null && String(x.latest.gateway_id).trim() !== "" ? String(x.latest.gateway_id).trim() : "";
          const k = gid || "__none__";
          return k === key;
        });

      const gidDisplay = key === "__none__" ? "—" : key;
      const rolesReported = samples.map((s) => s.latest?.gateway_role_reported).filter(Boolean);
      const roleVote = rolesReported.length ? rolesReported[0] : "";
      const idxInMesh = sortedRealIds.indexOf(key);
      let inferred = "";
      if (key === "__none__") inferred = "—";
      else if (sortedRealIds.length <= 1) inferred = "Primary (single gateway)";
      else if (idxInMesh === 0) inferred = "Primary (inferred)";
      else inferred = "Redundant (inferred)";

      const rssiVals = samples.map((s) => s.latest?.rssi).filter((v) => typeof v === "number");
      const bestRssi = rssiVals.length ? Math.max(...rssiVals) : null;
      const snrVals = samples.map((s) => s.latest?.snr).filter((v) => typeof v === "number");
      const bestSnr = snrVals.length ? Math.max(...snrVals) : null;
      const sqVals = samples.map((s) => s.latest?.signal_quality).filter((v) => typeof v === "number");
      const bestSq = sqVals.length ? Math.max(...sqVals) : null;

      const names = samples.map((s) => s.latest?.gateway_display_name).filter(Boolean);
      const nameDisplay = names.length ? names[0] : "—";

      const onlineSet = new Set();
      for (const s of samples) {
        const g = s.latest?.gateways_online;
        if (typeof g === "number") onlineSet.add(g);
      }
      const onlineStr =
        onlineSet.size === 0 ? "—" : onlineSet.size === 1 ? String([...onlineSet][0]) : [...onlineSet].sort((a, b) => a - b).join(" / ");

      const roomList = [...new Set(samples.map((s) => s.room || s.id).filter(Boolean))].join(", ") || "—";

      let pathOk = true;
      for (const s of samples) {
        if (isDeviceCommOffline(s.latest, nowMs)) {
          pathOk = false;
          break;
        }
      }
      const stText = pathOk ? "OK" : "Check path";

      const snrLink = [formatSnrDb(bestSnr), formatSignalQuality(bestSq)].filter(Boolean).join(" · ") || "—";

      rows.push({
        gidDisplay,
        nameDisplay,
        role: normalizeRoleLabel(roleVote, inferred),
        rssi: formatRssiDbm(bestRssi),
        snrLink,
        onlineStr,
        roomList,
        stText,
        pathOk
      });
    }

    for (const r of rows) {
      const tr = document.createElement("tr");
      const cells = [
        r.gidDisplay,
        r.nameDisplay,
        r.role,
        r.rssi,
        r.snrLink,
        r.onlineStr,
        r.roomList,
        r.stText
      ];
      cells.forEach((text, i) => {
        const td = document.createElement("td");
        td.textContent = text;
        if (i === 7) {
          td.classList.add("gw-path-cell");
          td.classList.add(r.pathOk ? "gw-path-ok" : "gw-path-warn");
        }
        tr.appendChild(td);
      });
      gwBody.appendChild(tr);
    }

    if (gwNote) {
      gwNote.textContent =
        sortedRealIds.length > 1
          ? "Multiple gateway IDs appear in telemetry. Primary/redundant is inferred by sorted gateway ID when the payload has no gateway_role field."
          : "Signal columns fill in when the backend adds fields such as rssi, snr, or signal_strength to each room document.";
    }
  }

  function formatDateTime(tsMs) {
    try {
      return new Date(tsMs).toLocaleString();
    } catch {
      return String(tsMs);
    }
  }

  function formatXAxisLabel(tsMs, windowKey) {
    try {
      const d = new Date(tsMs);
      if (["1h", "7h", "12h", "24h"].includes(windowKey)) {
        return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
      }
      return d.toLocaleDateString([], { month: "short", day: "2-digit" });
    } catch {
      return String(tsMs);
    }
  }

  function setVisibility() {
    const loggedIn = state.auth.loggedIn;
    const loginWrap = $("screenLoginWrap");
    if (loginWrap) loginWrap.classList.toggle("hidden", loggedIn);
    $("screenLogin").classList.toggle("hidden", loggedIn);
    $("screenMain").classList.toggle("hidden", !loggedIn);
    $("btnLogout").classList.toggle("hidden", !loggedIn);

    const chipUser = $("chipUser");
    if (chipUser) chipUser.classList.toggle("hidden", !loggedIn);
    const userName = $("userName");
    if (userName) userName.textContent = loggedIn ? String(state.auth.user || "") : "";

    const chipGateway = $("chipGateway");
    if (chipGateway) chipGateway.classList.toggle("hidden", !loggedIn);
    if (loggedIn) updateGatewayHeader();
  }

  function setThresholdControlsEnabled(enabled) {
    const ids = [
      "tempThresholdRange",
      "tempThresholdNumber",
      "tempThresholdRange2",
      "tempThresholdNumber2",
      "humThresholdRange",
      "humThresholdNumber",
      "humThresholdRange2",
      "humThresholdNumber2"
    ];
    for (const id of ids) {
      const el = $(id);
      if (el) el.disabled = !enabled;
    }
  }

  function applyRolePermissions() {
    const role = state.auth.role;
    const canEdit = role === "admin";
    const loggedIn = state.auth.loggedIn;
    setThresholdControlsEnabled(canEdit);
    const emailPanel = $("emailAlertsPanel");
    if (emailPanel) {
      emailPanel.classList.toggle("hidden", !loggedIn);
      emailPanel.classList.toggle("email-alerts-panel--readonly", loggedIn && !canEdit);
      const readonlyNote = $("emailAlertsReadonlyNote");
      if (readonlyNote) readonlyNote.classList.toggle("hidden", !loggedIn || canEdit);
      const emailLocked = loggedIn && !canEdit;
      for (const id of [
        "emailAlertsEnabled",
        "emailJsPublicKey",
        "emailJsServiceId",
        "emailJsTemplateId",
        "emailNotifyTo",
        "btnEmailAlertsSave",
        "btnEmailAlertsTest"
      ]) {
        const el = $(id);
        if (el) el.disabled = emailLocked;
      }
    }
  }

  function syncThresholdUI() {
    const tempVal = state.thresholds.temp;
    const humVal = state.thresholds.hum;

    const tempRange = $("tempThresholdRange");
    const tempNum = $("tempThresholdNumber");
    const humRange = $("humThresholdRange");
    const humNum = $("humThresholdNumber");
    const tempRange2 = $("tempThresholdRange2");
    const tempNum2 = $("tempThresholdNumber2");
    const humRange2 = $("humThresholdRange2");
    const humNum2 = $("humThresholdNumber2");

    if (tempRange) tempRange.value = String(tempVal);
    if (tempNum) tempNum.value = String(tempVal);
    if (humRange) humRange.value = String(humVal);
    if (humNum) humNum.value = String(humVal);

    if (tempRange2) tempRange2.value = String(tempVal);
    if (tempNum2) tempNum2.value = String(tempVal);
    if (humRange2) humRange2.value = String(humVal);
    if (humNum2) humNum2.value = String(humVal);

    $("tempThrPill").textContent = `High: ${tempVal} °C`;
    $("humThrPill").textContent = `High: ${humVal} %RH`;
    $("tempThrPill2").textContent = `High: ${tempVal} °C`;
    $("humThrPill2").textContent = `High: ${humVal} %RH`;
  }

  function setActiveNav(active) {
    const dashBtn = $("btnNavDashboard");
    const alarmsBtn = $("btnNavAlarms");
    const dashScreen = $("screenDashboard");
    const alarmsScreen = $("screenAlarms");

    if (active === "dashboard") {
      dashBtn.classList.add("active-page");
      alarmsBtn.classList.remove("active-page");
      dashScreen.classList.remove("hidden");
      alarmsScreen.classList.add("hidden");
    } else {
      dashBtn.classList.remove("active-page");
      alarmsBtn.classList.add("active-page");
      dashScreen.classList.add("hidden");
      alarmsScreen.classList.remove("hidden");
    }
  }

  function updateGatewayHeader() {
    const chip = $("chipGateway");
    const iconWrap = $("gatewayIconWrap");
    const titleEl = $("gatewayTitle");
    const subEl = $("gatewaySub");
    if (!chip || !iconWrap || !titleEl || !subEl) return;
    if (!state.auth.loggedIn) return;

    iconWrap.classList.remove("gateway-icon-wrap--ok", "gateway-icon-wrap--warn", "gateway-icon-wrap--danger");

    const latestList = Object.values(state.latest || {}).filter(Boolean);
    if (!latestList.length) {
      titleEl.textContent = "Gateways";
      subEl.textContent = "Awaiting sensor data…";
      iconWrap.classList.add("gateway-icon-wrap--warn");
      return;
    }

    const globalDown = isGlobalGatewaysDown(state.latest);
    const nums = latestList.map((d) => d.gateways_online).filter((x) => typeof x === "number");
    const onlineCount = nums.length ? Math.min(...nums) : null;
    const idSet = new Set();
    for (const d of latestList) {
      if (d.gateway_id) idSet.add(String(d.gateway_id));
    }
    const idsArr = Array.from(idSet);
    const idsStr = idsArr.slice(0, 2).join(" · ");
    const idsExtra = idsArr.length > 2 ? ` (+${idsArr.length - 2})` : "";

    const nowMs = Date.now();
    let anyStale = false;
    for (const d of latestList) {
      if (isDeviceCommOffline(d, nowMs)) {
        anyStale = true;
        break;
      }
    }

    if (globalDown) {
      iconWrap.classList.add("gateway-icon-wrap--danger");
      titleEl.textContent = "Gateways offline";
      subEl.textContent = idsStr
        ? `${idsStr}${idsExtra} · gateways_online≤0`
        : "All sensors report gateways offline.";
      return;
    }

    if (anyStale) {
      iconWrap.classList.add("gateway-icon-wrap--warn");
      titleEl.textContent = "Gateway / link";
      subEl.textContent = "Stale sample on a sensor — check gateway or LoRa path.";
      return;
    }

    iconWrap.classList.add("gateway-icon-wrap--ok");
    titleEl.textContent = "Gateways OK";
    const onlinePart = onlineCount != null ? `${onlineCount} online` : "Reporting";
    const idPart = idsStr ? ` · ${idsStr}${idsExtra}` : "";
    subEl.textContent = onlinePart + idPart;
  }

  function setConnectivity(status, text) {
    const chip = $("chipConnectivity");
    if (!chip) return;
    const dot = chip.querySelector(".status-dot");
    const connText = $("connectivityText");

    if (connText) connText.textContent = text;
    if (dot) dot.style.background = "rgba(148,163,184,0.7)";
    if (status === "ok" && dot) dot.style.background = "rgba(52,211,153,1)";
    if (status === "error" && dot) dot.style.background = "rgba(248,113,113,1)";
  }

  function scrollToTop() {
    try {
      window.scrollTo(0, 0);
    } catch {}
    try {
      document.documentElement.scrollTop = 0;
      document.body.scrollTop = 0;
    } catch {}
  }

  function dropdownInit(menuEl, buttonEl, onSelect) {
    if (!menuEl || !buttonEl) return;
    const toggle = (open) => {
      menuEl.classList.toggle("hidden", !open);
      menuEl.setAttribute("aria-hidden", open ? "false" : "true");
    };

    buttonEl.addEventListener("click", (e) => {
      e.stopPropagation();
      const isOpen = menuEl.getAttribute("aria-hidden") === "false";
      toggle(!isOpen);
    });

    for (const item of menuEl.querySelectorAll(".dropdown-item")) {
      item.addEventListener("click", () => {
        const key = item.getAttribute("data-window");
        if (!key) return;
        onSelect(key);
        toggle(false);
      });
    }

    // Close on outside click
    document.addEventListener("click", () => toggle(false));
  }

  function setTrendWindowUI(key) {
    state.trendWindowKey = key;
    const label = TIME_WINDOWS[key]?.label || TIME_WINDOWS["24h"].label;

    const dashLabelEl = $("timeWindowLabelDash");
    if (dashLabelEl) dashLabelEl.textContent = label;

    for (const item of $("timeWindowMenuDash")?.querySelectorAll(".dropdown-item") || []) {
      item.classList.toggle("active", item.getAttribute("data-window") === key);
    }
  }

  function setAlarmWindowUI(key) {
    state.alarmWindowKey = key;
    const label = TIME_WINDOWS[key]?.label || TIME_WINDOWS["24h"].label;

    const alarmsLabelEl = $("timeWindowLabelAlarms");
    if (alarmsLabelEl) alarmsLabelEl.textContent = label;

    for (const item of $("timeWindowMenuAlarms")?.querySelectorAll(".dropdown-item") || []) {
      item.classList.toggle("active", item.getAttribute("data-window") === key);
    }
  }

  function openDB() {
    if (state.dbPromise) return state.dbPromise;

    state.dbPromise = new Promise((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, DB_VERSION);
      req.onupgradeneeded = () => {
        const db = req.result;

        if (!db.objectStoreNames.contains(STORE_NAME)) {
          const store = db.createObjectStore(STORE_NAME, { keyPath: "id" });
          store.createIndex("by_device_ts", ["device_id", "tsMs"], { unique: true });
          store.createIndex("by_device", "device_id", { unique: false });
        }

        if (!db.objectStoreNames.contains(ALARM_EVENTS_STORE)) {
          const alarmEvents = db.createObjectStore(ALARM_EVENTS_STORE, { keyPath: "id" });
          alarmEvents.createIndex("by_device_ts", ["device_id", "tsMs"], { unique: false });
          alarmEvents.createIndex("by_variable_ts", ["variable", "tsMs"], { unique: false });
        }

        if (!db.objectStoreNames.contains(ALARM_STATE_STORE)) {
          db.createObjectStore(ALARM_STATE_STORE, { keyPath: "id" });
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });

    return state.dbPromise;
  }

  async function upsertReadings(readings) {
    if (!readings?.length) return;
    const db = await openDB();

    return new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_NAME, "readwrite");
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
      const store = tx.objectStore(STORE_NAME);
      for (const r of readings) store.put(r);
    });
  }

  async function queryReadings(deviceId, fromMs, toMs) {
    const db = await openDB();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_NAME, "readonly");
      const store = tx.objectStore(STORE_NAME);
      const idx = store.index("by_device_ts");
      const range = IDBKeyRange.bound([deviceId, fromMs], [deviceId, toMs]);
      const req = idx.openCursor(range, "next");
      const out = [];

      req.onsuccess = () => {
        const cur = req.result;
        if (cur) {
          out.push(cur.value);
          cur.continue();
        }
      };
      req.onerror = () => reject(req.error);
      tx.oncomplete = () => {
        out.sort((a, b) => a.tsMs - b.tsMs);
        resolve(out);
      };
    });
  }

  function loadTrendCache() {
    try {
      const raw = localStorage.getItem(TREND_CACHE_KEY);
      if (!raw) return { version: 1, byDevice: {} };
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== "object") return { version: 1, byDevice: {} };
      return parsed;
    } catch {
      return { version: 1, byDevice: {} };
    }
  }

  function saveTrendCache(cache) {
    try {
      localStorage.setItem(TREND_CACHE_KEY, JSON.stringify(cache));
    } catch {
      /* quota / private mode */
    }
  }

  function downsampleSeries(series, maxPoints) {
    if (series.length <= maxPoints) return series;
    const stride = Math.ceil(series.length / maxPoints);
    const out = [];
    for (let i = 0; i < series.length; i += stride) out.push(series[i]);
    if (out.length && out[out.length - 1].tMs !== series[series.length - 1].tMs) {
      out.push(series[series.length - 1]);
    }
    return out;
  }

  function addSampleToCacheArray(arr, sample) {
    const out = arr.slice();
    const idx = out.findIndex((p) => p.tMs === sample.tMs);
    if (idx >= 0) out[idx] = sample;
    else out.push(sample);
    out.sort((a, b) => a.tMs - b.tMs);
    return out;
  }

  function persistTrendCache(readingsEntries) {
    const now = Date.now();
    const fromMin = now - TREND_CACHE_MAX_MS;
    const cache = loadTrendCache();
    cache.version = 1;
    cache.byDevice = cache.byDevice || {};

    for (const e of readingsEntries || []) {
      const deviceId = e.device_id;
      if (!deviceId) continue;
      cache.byDevice[deviceId] = cache.byDevice[deviceId] || { temp: [], hum: [] };

      if (typeof e.temp_c === "number" && e.tsMs >= fromMin) {
        cache.byDevice[deviceId].temp = addSampleToCacheArray(cache.byDevice[deviceId].temp, {
          tMs: e.tsMs,
          v: e.temp_c,
          room: e.room || ""
        });
      }
      if (typeof e.humidity_rh === "number" && e.tsMs >= fromMin) {
        cache.byDevice[deviceId].hum = addSampleToCacheArray(cache.byDevice[deviceId].hum, {
          tMs: e.tsMs,
          v: e.humidity_rh,
          room: e.room || ""
        });
      }
    }

    for (const deviceId of Object.keys(cache.byDevice || {})) {
      const d = cache.byDevice[deviceId];
      if (Array.isArray(d.temp)) {
        d.temp = downsampleSeries(
          d.temp.filter((p) => p.tMs >= fromMin),
          MAX_LOCAL_POINTS
        );
      } else d.temp = [];
      if (Array.isArray(d.hum)) {
        d.hum = downsampleSeries(
          d.hum.filter((p) => p.tMs >= fromMin),
          MAX_LOCAL_POINTS
        );
      } else d.hum = [];
    }

    saveTrendCache(cache);
  }

  function getCachedSeries(deviceId, variable, fromMs, toMs) {
    const cache = loadTrendCache();
    const d = cache.byDevice?.[deviceId];
    if (!d) return [];
    const series = variable === "temp" ? d.temp || [] : d.hum || [];
    return series.filter((p) => p.tMs >= fromMs && p.tMs <= toMs).sort((a, b) => a.tMs - b.tMs);
  }

  function makeAlarmEventId(deviceId, variable, tsMs, threshold) {
    const thrScaled = typeof threshold === "number" ? Math.round(threshold * 10) : 0;
    return `${deviceId}_${variable}_${tsMs}_${thrScaled}`;
  }

  async function queryAlarmEvents(deviceId, fromMs, toMs) {
    const db = await openDB();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(ALARM_EVENTS_STORE, "readonly");
      const store = tx.objectStore(ALARM_EVENTS_STORE);
      const idx = store.index("by_device_ts");
      const range = IDBKeyRange.bound([deviceId, fromMs], [deviceId, toMs]);
      const req = idx.openCursor(range, "next");
      const out = [];

      req.onsuccess = () => {
        const cur = req.result;
        if (cur) {
          out.push(cur.value);
          cur.continue();
        }
      };
      req.onerror = () => reject(req.error);
      tx.oncomplete = () => {
        out.sort((a, b) => a.tsMs - b.tsMs);
        resolve(out);
      };
    });
  }

  function downsamplePoints(points, maxPoints) {
    if (points.length <= maxPoints) return points;
    const stride = Math.ceil(points.length / maxPoints);
    const out = [];
    for (let i = 0; i < points.length; i += stride) out.push(points[i]);
    if (out.length && out[out.length - 1].tMs !== points[points.length - 1].tMs) {
      out.push(points[points.length - 1]);
    }
    return out;
  }

  function getTrendWindow() {
    const toMs = Date.now();
    const fromMs = toMs - (TIME_WINDOWS[state.trendWindowKey]?.ms || TIME_WINDOWS["24h"].ms);
    return { fromMs, toMs };
  }

  function getAlarmWindow() {
    const toMs = Date.now();
    const fromMs = toMs - (TIME_WINDOWS[state.alarmWindowKey]?.ms || TIME_WINDOWS["24h"].ms);
    return { fromMs, toMs };
  }

  function getEmailAlarmConfig() {
    return {
      enabled: localStorage.getItem(EMAIL_ENABLED_KEY) === "1",
      publicKey: (localStorage.getItem(EMAIL_PUBLIC_KEY) || "").trim(),
      serviceId: (localStorage.getItem(EMAIL_SERVICE_ID_KEY) || "").trim(),
      templateId: (localStorage.getItem(EMAIL_TEMPLATE_ID_KEY) || "").trim(),
      toEmail: (localStorage.getItem(EMAIL_TO_KEY) || "").trim()
    };
  }

  function saveEmailAlarmConfig(cfg) {
    localStorage.setItem(EMAIL_ENABLED_KEY, cfg.enabled ? "1" : "0");
    localStorage.setItem(EMAIL_PUBLIC_KEY, cfg.publicKey);
    localStorage.setItem(EMAIL_SERVICE_ID_KEY, cfg.serviceId);
    localStorage.setItem(EMAIL_TEMPLATE_ID_KEY, cfg.templateId);
    localStorage.setItem(EMAIL_TO_KEY, cfg.toEmail);
  }

  function loadEmailConfigIntoForm() {
    const c = getEmailAlarmConfig();
    const en = $("emailAlertsEnabled");
    const pk = $("emailJsPublicKey");
    const sid = $("emailJsServiceId");
    const tid = $("emailJsTemplateId");
    const to = $("emailNotifyTo");
    if (en) en.checked = c.enabled;
    if (pk) pk.value = c.publicKey;
    if (sid) sid.value = c.serviceId;
    if (tid) tid.value = c.templateId;
    if (to) to.value = c.toEmail;
  }

  function loadEmailJsSdk() {
    if (typeof window.emailjs !== "undefined" && window.emailjs?.init) {
      return Promise.resolve();
    }
    return new Promise((resolve, reject) => {
      const existing = document.querySelector('script[data-envi-emailjs="1"]');
      if (existing) {
        existing.addEventListener("load", () => resolve());
        existing.addEventListener("error", () => reject(new Error("EmailJS script load failed")));
        return;
      }
      const s = document.createElement("script");
      s.src = EMAILJS_CDN;
      s.async = true;
      s.dataset.enviEmailjs = "1";
      s.onload = () => resolve();
      s.onerror = () => reject(new Error("EmailJS script load failed"));
      document.head.appendChild(s);
    });
  }

  function formatAlarmEmailBody(ev) {
    const timeStr = formatDateTime(ev.tsMs);
    let body = `Envi-Guard alarm\n\nTime: ${timeStr}\nVariable: ${ev.variable}\nDevice: ${ev.device_id}\nRoom: ${
      ev.room || "—"
    }\n`;
    if (ev.variable === "Connectivity") {
      body += `Detail: ${ev.detail || "—"}\n`;
    } else if (ev.variable === "Temperature") {
      body += `Value: ${ev.value} °C\nThreshold: ${ev.threshold} °C\n`;
    } else if (ev.variable === "Humidity") {
      body += `Value: ${ev.value} %RH\nThreshold: ${ev.threshold} %RH\n`;
    }
    return body;
  }

  function buildAlarmEmailSubject(ev) {
    if (ev.variable === "Connectivity" && ev.device_id === "SYSTEM") {
      return "[Envi-Guard] All gateways offline";
    }
    if (ev.variable === "Connectivity") {
      return `[Envi-Guard] Gateway / link — ${ev.device_id}`;
    }
    if (ev.variable === "Temperature") {
      return `[Envi-Guard] Temperature high — ${ev.device_id}`;
    }
    if (ev.variable === "Humidity") {
      return `[Envi-Guard] Humidity high — ${ev.device_id}`;
    }
    return `[Envi-Guard] ${ev.variable} — ${ev.device_id}`;
  }

  async function sendAlarmEmailsForEvents(events) {
    if (!events?.length) return;
    const cfg = getEmailAlarmConfig();
    if (!cfg.enabled) return;
    if (!cfg.publicKey || !cfg.serviceId || !cfg.templateId || !cfg.toEmail) {
      console.warn("Envi-Guard: email alerts enabled but EmailJS configuration incomplete.");
      return;
    }
    try {
      await loadEmailJsSdk();
      const emailjs = window.emailjs;
      if (!emailjs?.init || !emailjs?.send) {
        throw new Error("EmailJS not available on window");
      }
      emailjs.init({ publicKey: cfg.publicKey });
      for (const ev of events) {
        const subject = buildAlarmEmailSubject(ev);
        const message = formatAlarmEmailBody(ev);
        const templateParams = {
          to_email: cfg.toEmail,
          subject,
          message,
          reply_to: cfg.toEmail,
          alarm_variable: ev.variable,
          device_id: ev.device_id,
          room: ev.room || "",
          alarm_time: formatDateTime(ev.tsMs)
        };
        await emailjs.send(cfg.serviceId, cfg.templateId, templateParams);
      }
    } catch (err) {
      console.error("Envi-Guard: failed to send alarm email(s)", err);
    }
  }

  async function readAlarmStateKeys(keys) {
    const db = await openDB();
    const uniq = Array.from(new Set(keys.filter(Boolean)));
    if (!uniq.length) return new Map();

    return new Promise((resolve, reject) => {
      const out = new Map();
      const tx = db.transaction(ALARM_STATE_STORE, "readonly");
      const store = tx.objectStore(ALARM_STATE_STORE);
      tx.oncomplete = () => resolve(out);
      tx.onerror = () => reject(tx.error);
      tx.onabort = () => reject(tx.error || new Error("IndexedDB transaction aborted"));

      for (const key of uniq) {
        const k = key;
        const req = store.get(k);
        req.onsuccess = () => {
          if (req.result) out.set(k, req.result);
        };
        req.onerror = () => reject(req.error);
      }
    });
  }

  async function updateAlarmEventsFromLatest() {
    if (!state.devices || !Object.keys(state.devices).length) return;

    const db = await openDB();
    const deviceIds = Object.keys(state.devices);
    const nowMs = Date.now();
    const keysToRead = ["GLOBAL_gateways"];
    for (const deviceId of deviceIds) {
      const latest = state.latest[deviceId];
      if (!latest) continue;
      if (typeof latest.temp_c === "number") keysToRead.push(`${deviceId}_Temperature`);
      if (typeof latest.humidity_rh === "number") keysToRead.push(`${deviceId}_Humidity`);
      keysToRead.push(`${deviceId}_Connectivity`);
    }

    const prevMap = await readAlarmStateKeys(keysToRead);
    const globalDown = isGlobalGatewaysDown(state.latest);
    const risingEdgeForEmail = [];

    await new Promise((resolve, reject) => {
      const tx = db.transaction([ALARM_STATE_STORE, ALARM_EVENTS_STORE], "readwrite");
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
      const stateStore = tx.objectStore(ALARM_STATE_STORE);
      const eventsStore = tx.objectStore(ALARM_EVENTS_STORE);

      {
        const key = "GLOBAL_gateways";
        const prev = prevMap.get(key);
        const prevBad = prev ? !!prev.above : false;
        if (!prevBad && globalDown) {
          const ev = {
            id: makeAlarmEventId("SYSTEM", "Connectivity", nowMs, 0),
            device_id: "SYSTEM",
            room: "—",
            tsMs: nowMs,
            variable: "Connectivity",
            value: 0,
            threshold: 0,
            detail: "All gateways offline (gateways_online≤0 on all sensors)."
          };
          eventsStore.put(ev);
          risingEdgeForEmail.push({ ...ev });
        }
        stateStore.put({
          id: key,
          device_id: "SYSTEM",
          variable: "Connectivity",
          above: globalDown,
          updatedTsMs: nowMs
        });
      }

      for (const deviceId of deviceIds) {
        const latest = state.latest[deviceId];
        if (!latest) continue;

        if (typeof latest.temp_c === "number") {
          const variable = "Temperature";
          const skey = `${deviceId}_${variable}`;
          const thr = state.thresholds.temp;
          const aboveNow = latest.temp_c >= thr;
          const prev = prevMap.get(skey);
          const prevAbove = !!prev?.above;
          const prevLoggedThr =
            typeof prev?.lastLoggedThreshold === "number" && Number.isFinite(prev.lastLoggedThreshold)
              ? prev.lastLoggedThreshold
              : null;
          const prevReadingTs =
            typeof prev?.lastAlarmReadingTs === "number" && Number.isFinite(prev.lastAlarmReadingTs)
              ? prev.lastAlarmReadingTs
              : null;

          let shouldLog = false;
          if (aboveNow) {
            const rising = !prevAbove;
            const thrChanged = prevLoggedThr !== null && prevLoggedThr !== thr;
            const newReadingWhileHot =
              prevReadingTs !== null && Number.isFinite(prevReadingTs) && latest.tsMs !== prevReadingTs;
            const backlogViolation =
              prevAbove && prevLoggedThr === null && prevReadingTs === null;
            shouldLog = rising || thrChanged || newReadingWhileHot || backlogViolation;
          }

          if (shouldLog) {
            const ev = {
              id: makeAlarmEventId(deviceId, variable, latest.tsMs, thr),
              device_id: deviceId,
              room: latest.room,
              tsMs: latest.tsMs,
              variable,
              value: latest.temp_c,
              threshold: thr
            };
            eventsStore.put(ev);
            risingEdgeForEmail.push({ ...ev });
          }

          const nextLastReadingTs = aboveNow
            ? shouldLog
              ? latest.tsMs
              : prevReadingTs !== null
                ? prevReadingTs
                : latest.tsMs
            : null;
          const nextLastLoggedThr = aboveNow ? (shouldLog ? thr : prevLoggedThr !== null ? prevLoggedThr : thr) : null;

          stateStore.put({
            id: skey,
            device_id: deviceId,
            variable,
            above: aboveNow,
            updatedTsMs: latest.tsMs,
            lastLoggedThreshold: nextLastLoggedThr,
            lastAlarmReadingTs: nextLastReadingTs
          });
        }

        if (typeof latest.humidity_rh === "number") {
          const variable = "Humidity";
          const skey = `${deviceId}_${variable}`;
          const thr = state.thresholds.hum;
          const aboveNow = latest.humidity_rh >= thr;
          const prev = prevMap.get(skey);
          const prevAbove = !!prev?.above;
          const prevLoggedThr =
            typeof prev?.lastLoggedThreshold === "number" && Number.isFinite(prev.lastLoggedThreshold)
              ? prev.lastLoggedThreshold
              : null;
          const prevReadingTs =
            typeof prev?.lastAlarmReadingTs === "number" && Number.isFinite(prev.lastAlarmReadingTs)
              ? prev.lastAlarmReadingTs
              : null;

          let shouldLog = false;
          if (aboveNow) {
            const rising = !prevAbove;
            const thrChanged = prevLoggedThr !== null && prevLoggedThr !== thr;
            const newReadingWhileHot =
              prevReadingTs !== null && Number.isFinite(prevReadingTs) && latest.tsMs !== prevReadingTs;
            const backlogViolation =
              prevAbove && prevLoggedThr === null && prevReadingTs === null;
            shouldLog = rising || thrChanged || newReadingWhileHot || backlogViolation;
          }

          if (shouldLog) {
            const ev = {
              id: makeAlarmEventId(deviceId, variable, latest.tsMs, thr),
              device_id: deviceId,
              room: latest.room,
              tsMs: latest.tsMs,
              variable,
              value: latest.humidity_rh,
              threshold: thr
            };
            eventsStore.put(ev);
            risingEdgeForEmail.push({ ...ev });
          }

          const nextLastReadingTs = aboveNow
            ? shouldLog
              ? latest.tsMs
              : prevReadingTs !== null
                ? prevReadingTs
                : latest.tsMs
            : null;
          const nextLastLoggedThr = aboveNow ? (shouldLog ? thr : prevLoggedThr !== null ? prevLoggedThr : thr) : null;

          stateStore.put({
            id: skey,
            device_id: deviceId,
            variable,
            above: aboveNow,
            updatedTsMs: latest.tsMs,
            lastLoggedThreshold: nextLastLoggedThr,
            lastAlarmReadingTs: nextLastReadingTs
          });
        }

        const variable = "Connectivity";
        const skey = `${deviceId}_${variable}`;
        const offlineNow = !globalDown && isDeviceCommOffline(latest, nowMs);
        const prev = prevMap.get(skey);
        const prevOffline = prev ? !!prev.above : null;
        if (prevOffline !== true && offlineNow === true) {
          const ageMin = Math.max(0, Math.floor((nowMs - (latest.tsMs || nowMs)) / 60000));
          const ev = {
            id: makeAlarmEventId(deviceId, variable, nowMs, ageMin),
            device_id: deviceId,
            room: latest.room,
            tsMs: nowMs,
            variable,
            value: ageMin,
            threshold: Math.round(GATEWAY_STALE_MS / 60000),
            detail: connectivityAlarmDetail(latest, nowMs)
          };
          eventsStore.put(ev);
          risingEdgeForEmail.push({ ...ev });
        }
        stateStore.put({
          id: skey,
          device_id: deviceId,
          variable,
          above: offlineNow,
          updatedTsMs: nowMs
        });
      }
    });

    void sendAlarmEmailsForEvents(risingEdgeForEmail);
  }

  async function backfillAlarmEvents(fromMs) {
    if (state.alarmBackfilled) return;
    if (!state.devices || !Object.keys(state.devices).length) return;

    const deviceIds = Object.keys(state.devices);
    const nowMs = Date.now();

    const db = await openDB();
    for (const deviceId of deviceIds) {
      // Read a short history before fromMs to determine whether we were already above.
      const lookbackMs = 2 * 60 * 60 * 1000; // 2 hours
      const prevReadings = await queryReadings(deviceId, Math.max(0, fromMs - lookbackMs), fromMs);

      let prevTempAbove = false;
      let prevHumAbove = false;

      for (let i = prevReadings.length - 1; i >= 0; i--) {
        const r = prevReadings[i];
        if (typeof r.temp_c === "number") {
          prevTempAbove = r.temp_c >= state.thresholds.temp;
          break;
        }
      }
      for (let i = prevReadings.length - 1; i >= 0; i--) {
        const r = prevReadings[i];
        if (typeof r.humidity_rh === "number") {
          prevHumAbove = r.humidity_rh >= state.thresholds.hum;
          break;
        }
      }

      const readings = await queryReadings(deviceId, fromMs, nowMs);

      const eventsToInsert = [];
      for (const r of readings) {
        if (typeof r.temp_c === "number") {
          const aboveNow = r.temp_c >= state.thresholds.temp;
          if (aboveNow === true && prevTempAbove === false) {
            eventsToInsert.push({
              id: makeAlarmEventId(deviceId, "Temperature", r.tsMs, state.thresholds.temp),
              device_id: deviceId,
              room: r.room,
              tsMs: r.tsMs,
              variable: "Temperature",
              value: r.temp_c,
              threshold: state.thresholds.temp
            });
          }
          prevTempAbove = aboveNow;
        }
        if (typeof r.humidity_rh === "number") {
          const aboveNow = r.humidity_rh >= state.thresholds.hum;
          if (aboveNow === true && prevHumAbove === false) {
            eventsToInsert.push({
              id: makeAlarmEventId(deviceId, "Humidity", r.tsMs, state.thresholds.hum),
              device_id: deviceId,
              room: r.room,
              tsMs: r.tsMs,
              variable: "Humidity",
              value: r.humidity_rh,
              threshold: state.thresholds.hum
            });
          }
          prevHumAbove = aboveNow;
        }
      }

      // Insert in a single transaction per device.
      if (eventsToInsert.length) {
        await new Promise((resolve, reject) => {
          const tx = db.transaction([ALARM_EVENTS_STORE], "readwrite");
          tx.oncomplete = () => resolve();
          tx.onerror = () => reject(tx.error);
          const eventsStore = tx.objectStore(ALARM_EVENTS_STORE);
          for (const ev of eventsToInsert) eventsStore.put(ev);
        });
      }

      const last = readings.length ? readings[readings.length - 1] : prevReadings[prevReadings.length - 1];
      const hasLastMetrics =
        last && (typeof last.temp_c === "number" || typeof last.humidity_rh === "number");

      if (hasLastMetrics) {
        await new Promise((resolve, reject) => {
          const tx2 = db.transaction([ALARM_STATE_STORE], "readwrite");
          tx2.oncomplete = () => resolve();
          tx2.onerror = () => reject(tx2.error);
          const stateStore = tx2.objectStore(ALARM_STATE_STORE);
          if (typeof last.temp_c === "number") {
            stateStore.put({
              id: `${deviceId}_Temperature`,
              device_id: deviceId,
              variable: "Temperature",
              above: last.temp_c >= state.thresholds.temp,
              updatedTsMs: last.tsMs,
              lastLoggedThreshold: null,
              lastAlarmReadingTs: null
            });
          }
          if (typeof last.humidity_rh === "number") {
            stateStore.put({
              id: `${deviceId}_Humidity`,
              device_id: deviceId,
              variable: "Humidity",
              above: last.humidity_rh >= state.thresholds.hum,
              updatedTsMs: last.tsMs,
              lastLoggedThreshold: null,
              lastAlarmReadingTs: null
            });
          }
        });
      }
    }

    state.alarmBackfilled = true;
  }

  async function pruneOldReadings() {
    const cutoff = Date.now() - READING_RETENTION_MS;
    const db = await openDB();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_NAME, "readwrite");
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
      const store = tx.objectStore(STORE_NAME);
      const req = store.openCursor();
      req.onsuccess = (e) => {
        const cur = e.target.result;
        if (cur) {
          const ts = cur.value?.tsMs;
          if (typeof ts === "number" && ts < cutoff) cur.delete();
          cur.continue();
        }
      };
    });
  }

  async function requestStoragePersistence() {
    try {
      if (navigator.storage?.persist) await navigator.storage.persist();
    } catch {
      /* ignore */
    }
  }

  let alarmHistoryRebuildTimer = null;
  /** Re-evaluate alarms vs thresholds and refresh UI — does not delete stored alarm events. */
  function scheduleAlarmHistoryRebuild() {
    if (alarmHistoryRebuildTimer) clearTimeout(alarmHistoryRebuildTimer);
    alarmHistoryRebuildTimer = setTimeout(async () => {
      alarmHistoryRebuildTimer = null;
      try {
        await updateAlarmEventsFromLatest();
        if (getVisiblePage() === "alarms") await renderAlarmsTable();
        if (getVisiblePage() === "dashboard") renderDashboardInsights();
      } catch (e) {
        console.error(e);
      }
    }, 400);
  }

  function drawTrend(canvas, series, opts) {
    const ctx = canvas.getContext("2d");
    const W = canvas.width;
    const H = canvas.height;

    const paddingLeft = 64;
    const paddingRight = 18;
    const paddingTop = 18;
    const paddingBottom = 50;

    ctx.clearRect(0, 0, W, H);
    ctx.fillStyle = "rgba(0, 0, 0, 0.98)";
    ctx.fillRect(0, 0, W, H);

    const plotW = W - paddingLeft - paddingRight;
    const plotH = H - paddingTop - paddingBottom;

    if (!series?.length) {
      ctx.fillStyle = "rgba(148,163,184,0.95)";
      ctx.font = "14px ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial";
      ctx.fillText("No cached readings for this window.", paddingLeft, paddingTop + 22);
      ctx.fillStyle = "rgba(148,163,184,0.7)";
      ctx.font = "12px ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial";
      ctx.fillText("Waiting for the first samples…", paddingLeft, paddingTop + 42);
      return;
    }

    const seriesMin = series[0].tMs;
    const seriesMax = series[series.length - 1].tMs;
    const axisMin = typeof opts.axisFromMs === "number" ? opts.axisFromMs : seriesMin;
    const axisMax = typeof opts.axisToMs === "number" ? opts.axisToMs : seriesMax;
    const threshold = typeof opts.threshold === "number" ? opts.threshold : null;

    let yMin = series[0].v;
    let yMax = series[0].v;
    for (const p of series) {
      if (p.v < yMin) yMin = p.v;
      if (p.v > yMax) yMax = p.v;
    }
    if (threshold !== null) {
      if (threshold < yMin) yMin = threshold;
      if (threshold > yMax) yMax = threshold;
    }
    if (yMin === yMax) {
      yMin -= 1;
      yMax += 1;
    }

    const pad = (yMax - yMin) * 0.08;
    yMin -= pad;
    yMax += pad;

    const xForTime = (t) => paddingLeft + ((t - axisMin) / (axisMax - axisMin || 1)) * plotW;
    const yForValue = (v) => paddingTop + (1 - (v - yMin) / (yMax - yMin || 1)) * plotH;

    // Grid
    ctx.strokeStyle = "rgba(148,163,184,0.18)";
    ctx.lineWidth = 1;
    const gridLines = 4;
    for (let i = 0; i <= gridLines; i++) {
      const y = paddingTop + (plotH * i) / gridLines;
      ctx.beginPath();
      ctx.moveTo(paddingLeft, y);
      ctx.lineTo(W - paddingRight, y);
      ctx.stroke();
    }

    // Y labels
    ctx.fillStyle = "rgba(203, 213, 225, 0.95)";
    ctx.font = "700 15px ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial";
    ctx.textBaseline = "middle";
    for (let i = 0; i <= gridLines; i++) {
      const v = yMax - ((yMax - yMin) * i) / gridLines;
      const y = paddingTop + (plotH * i) / gridLines;
      ctx.fillText(String(Math.round(v * 10) / 10), 12, y);
    }

    // X axis (time) ticks and labels
    const tickCount = 5;
    const tMinLocal = axisMin;
    const tMaxLocal = axisMax;
    ctx.strokeStyle = "rgba(148,163,184,0.18)";
    ctx.lineWidth = 1;
    const xAxisY = paddingTop + plotH;
    ctx.beginPath();
    ctx.moveTo(paddingLeft, xAxisY);
    ctx.lineTo(W - paddingRight, xAxisY);
    ctx.stroke();

    ctx.fillStyle = "rgba(203, 213, 225, 0.9)";
    ctx.font = "650 14px ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial";
    ctx.textBaseline = "top";
    ctx.textAlign = "center";
    for (let i = 0; i <= tickCount; i++) {
      const ratio = tickCount ? i / tickCount : 0;
      const x = paddingLeft + ratio * plotW;
      const t = tMinLocal + ratio * (tMaxLocal - tMinLocal);
      const label = formatXAxisLabel(t, opts.timeWindowKey);

      // tick mark
      ctx.strokeStyle = "rgba(148,163,184,0.22)";
      ctx.beginPath();
      ctx.moveTo(x, xAxisY);
      ctx.lineTo(x, xAxisY + 6);
      ctx.stroke();

      ctx.fillText(label, x, xAxisY + 10);
    }
    ctx.textAlign = "start";

    // Threshold line
    if (threshold !== null) {
      const yT = yForValue(threshold);
      ctx.save();
      ctx.strokeStyle = opts.thresholdColor || "rgba(251, 113, 133, 0.9)";
      ctx.lineWidth = 2;
      ctx.setLineDash([6, 6]);
      ctx.beginPath();
      ctx.moveTo(paddingLeft, yT);
      ctx.lineTo(W - paddingRight, yT);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = "rgba(251, 113, 133, 0.95)";
      ctx.font = "12px ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial";
      ctx.fillText(opts.thresholdLabel ? opts.thresholdLabel : `Thr: ${threshold}`, paddingLeft + 6, yT - 8);
      ctx.restore();
    }

    // Series
    ctx.strokeStyle = opts.color;
    ctx.lineWidth = 2.2;
    ctx.beginPath();
    for (let i = 0; i < series.length; i++) {
      const x = xForTime(series[i].tMs);
      const y = yForValue(series[i].v);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.stroke();

    // Latest marker
    const latest = series[series.length - 1];
    const xL = xForTime(latest.tMs);
    const yL = yForValue(latest.v);
    ctx.save();
    ctx.fillStyle = "#f9fafb";
    ctx.beginPath();
    ctx.arc(xL, yL, 4.2, 0, Math.PI * 2);
    ctx.fill();
    ctx.strokeStyle = opts.color;
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.arc(xL, yL, 7, 0, Math.PI * 2);
    ctx.stroke();
    ctx.restore();
  }

  function getActiveAlarmText(latestReading, deviceId) {
    const events = [];
    const nowMs = Date.now();

    if (isGlobalGatewaysDown(state.latest)) {
      events.push("All gateways offline — check LoRaWAN gateways and backhaul.");
    } else if (deviceId && latestReading && isDeviceCommOffline(latestReading, nowMs)) {
      events.push(`Link / gateway: ${connectivityAlarmDetail(latestReading, nowMs)}`);
    }

    if (latestReading && typeof latestReading.temp_c === "number") {
      if (latestReading.temp_c >= state.thresholds.temp) {
        events.push(`Temp ${latestReading.temp_c.toFixed(1)} °C ≥ ${state.thresholds.temp} °C`);
      }
    }
    if (latestReading && typeof latestReading.humidity_rh === "number") {
      if (latestReading.humidity_rh >= state.thresholds.hum) {
        events.push(`Hum ${latestReading.humidity_rh.toFixed(0)} %RH ≥ ${state.thresholds.hum} %RH`);
      }
    }
    if (!events.length) return { active: false, text: "No active alarms for the selected sensor." };
    return { active: true, text: `Alarm active: ${events.join(" · ")}` };
  }

  function getVisiblePage() {
    if (!$("screenDashboard").classList.contains("hidden")) return "dashboard";
    if (!$("screenAlarms").classList.contains("hidden")) return "alarms";
    return "dashboard";
  }

  let pendingRedrawTimer = null;
  function scheduleRedraw() {
    if (pendingRedrawTimer) clearTimeout(pendingRedrawTimer);
    pendingRedrawTimer = setTimeout(async () => {
      pendingRedrawTimer = null;
      await redrawVisible();
    }, 250);
  }

  function mergeTrendSamplesFromReadingsAndCache(readings, deviceId, variable, fromMs, toMs) {
    const map = new Map();
    for (const p of getCachedSeries(deviceId, variable, fromMs, toMs)) {
      map.set(p.tMs, { tMs: p.tMs, v: p.v });
    }
    for (const r of readings) {
      if (variable === "temp" && typeof r.temp_c === "number") {
        map.set(r.tsMs, { tMs: r.tsMs, v: r.temp_c });
      }
      if (variable === "hum" && typeof r.humidity_rh === "number") {
        map.set(r.tsMs, { tMs: r.tsMs, v: r.humidity_rh });
      }
    }
    return Array.from(map.values())
      .filter((p) => p.tMs >= fromMs && p.tMs <= toMs)
      .sort((a, b) => a.tMs - b.tMs);
  }

  async function redrawVisible() {
    const page = getVisiblePage();
    const deviceIds = Object.keys(state.devices || {});

    if (!deviceIds.length) {
      if (page === "dashboard") renderDashboardInsights();
      return;
    }

    if (!state.selectedDeviceId || !state.devices[state.selectedDeviceId]) {
      state.selectedDeviceId = deviceIds[0];
    }

    const { fromMs, toMs } = getTrendWindow();
    const deviceId = state.selectedDeviceId;

    if (page === "dashboard") {
      $("dashboardDataHint").textContent = "Loading trend data from cached readings…";
    } else {
      $("alarmsDataHint").textContent = "Loading alarm history from cached readings…";
    }

    const readings = await queryReadings(deviceId, fromMs, toMs);
    const tempSeries = mergeTrendSamplesFromReadingsAndCache(readings, deviceId, "temp", fromMs, toMs);
    const humSeries = mergeTrendSamplesFromReadingsAndCache(readings, deviceId, "hum", fromMs, toMs);

    const tempDS = downsamplePoints(tempSeries, MAX_DISPLAY_POINTS);
    const humDS = downsamplePoints(humSeries, MAX_DISPLAY_POINTS);

    if (page === "dashboard") {
      $("tempMeta").textContent = `${state.thresholds.temp} °C threshold`;
      drawTrend($("tempCanvas"), tempDS, {
        color: "rgba(56, 189, 248, 0.95)",
        threshold: state.thresholds.temp,
        thresholdColor: "rgba(251, 113, 133, 0.95)",
        thresholdLabel: `${state.thresholds.temp}°C`,
        timeWindowKey: state.trendWindowKey,
        axisFromMs: fromMs,
        axisToMs: toMs
      });

      $("humMeta").textContent = `${state.thresholds.hum} %RH threshold`;
      drawTrend($("humCanvas"), humDS, {
        color: "rgba(168, 85, 247, 0.95)",
        threshold: state.thresholds.hum,
        thresholdColor: "rgba(251, 113, 133, 0.95)",
        thresholdLabel: `${state.thresholds.hum}%RH`,
        timeWindowKey: state.trendWindowKey,
        axisFromMs: fromMs,
        axisToMs: toMs
      });

      const latestReading = readings.length ? readings[readings.length - 1] : state.latest[deviceId];
      const active = getActiveAlarmText(latestReading, deviceId);
      const banner = $("activeAlarmBanner");
      banner.classList.remove("error", "ok");
      if (active.active) {
        banner.classList.add("error");
      }
      $("activeAlarmText").textContent = active.text;

      $("dashboardDataHint").textContent = `Range: ${TIME_WINDOWS[state.trendWindowKey].label} (${formatDateTime(
        fromMs
      )} - ${formatDateTime(toMs)}). Points: temp ${tempSeries.length}, hum ${humSeries.length} (IndexedDB + local backup).`;

      renderDashboardInsights();
    }
  }

  async function renderAlarmsTable() {
    const deviceIds = Object.keys(state.devices || {});
    if (!deviceIds.length) return;
    if (!state.selectedDeviceId || !state.devices[state.selectedDeviceId]) {
      state.selectedDeviceId = deviceIds[0];
    }

    const { fromMs, toMs } = getAlarmWindow();
    const deviceId = state.selectedDeviceId;

    const backfillFromMs = Math.max(
      0,
      Date.now() - (TIME_WINDOWS["30d"]?.ms || 30 * 24 * 60 * 60 * 1000)
    );
    if (!state.alarmBackfilled) {
      await backfillAlarmEvents(backfillFromMs);
    }

    const eventsSensor = await queryAlarmEvents(deviceId, fromMs, toMs);
    const eventsSystem = await queryAlarmEvents("SYSTEM", fromMs, toMs);
    const evMap = new Map();
    for (const ev of eventsSystem) evMap.set(ev.id, ev);
    for (const ev of eventsSensor) evMap.set(ev.id, ev);
    const events = Array.from(evMap.values()).sort((a, b) => a.tsMs - b.tsMs);

    const body = $("alarmsTableBody");
    body.innerHTML = "";

    const summaryLeft = $("alarmsSummaryLeft");
    const summaryRight = $("alarmsSummaryRight");
    const emptyHint = $("alarmsEmptyHint");

    const windowLabel = TIME_WINDOWS[state.alarmWindowKey]?.label || TIME_WINDOWS["24h"].label;
    $("alarmsDataHint").textContent = `Range: ${windowLabel} (${formatDateTime(fromMs)} - ${formatDateTime(
      toMs
    )}). Events: ${events.length}.`;

    if (!events.length) {
      emptyHint.style.display = "block";
      summaryLeft.textContent = `Sensor: ${state.devices[deviceId] || "—"} · No events`;
      summaryRight.style.display = "none";
      return;
    }

    emptyHint.style.display = "none";
    summaryLeft.textContent = `Sensor: ${state.devices[deviceId] || "—"} · Showing ${Math.min(
      events.length,
      MAX_ALARM_ROWS
    )} of ${events.length} events`;
    summaryRight.style.display = "inline-flex";

    const toRender = events.slice(0, MAX_ALARM_ROWS);
    const frag = document.createDocumentFragment();

    for (const ev of toRender) {
      const tr = document.createElement("tr");

      const tdTime = document.createElement("td");
      tdTime.textContent = formatDateTime(ev.tsMs);

      const tdRoom = document.createElement("td");
      tdRoom.textContent = ev.room || "—";

      const tdDevice = document.createElement("td");
      tdDevice.textContent = ev.device_id || "—";

      const tdVar = document.createElement("td");
      if (ev.variable === "Connectivity") {
        tdVar.innerHTML = `<span class="var-badge conn">${ev.variable}</span>`;
      } else {
        tdVar.innerHTML =
          ev.variable === "Temperature"
            ? `<span class="var-badge temp">${ev.variable}</span>`
            : `<span class="var-badge hum">${ev.variable}</span>`;
      }

      const tdVal = document.createElement("td");
      if (ev.variable === "Connectivity") {
        tdVal.textContent = ev.detail || "—";
      } else {
        tdVal.textContent =
          ev.variable === "Temperature" ? `${ev.value.toFixed(1)} °C` : `${ev.value.toFixed(0)} %RH`;
      }

      const tdThr = document.createElement("td");
      if (ev.variable === "Connectivity") {
        tdThr.textContent =
          ev.device_id === "SYSTEM" ? "All gateways" : `${ev.threshold} min (no-data limit)`;
      } else {
        tdThr.textContent =
          ev.variable === "Temperature" ? `${ev.threshold.toFixed(1)} °C` : `${ev.threshold.toFixed(0)} %RH`;
      }

      tr.appendChild(tdTime);
      tr.appendChild(tdRoom);
      tr.appendChild(tdDevice);
      tr.appendChild(tdVar);
      tr.appendChild(tdVal);
      tr.appendChild(tdThr);
      frag.appendChild(tr);
    }

    body.appendChild(frag);
  }

  function renderSensorCards() {
    const container = $("sensorCards");
    if (!container) return;
    const deviceIds = Object.keys(state.devices || {});
    container.innerHTML = "";

    if (!deviceIds.length) {
      const div = document.createElement("div");
      div.className = "status-banner error";
      div.innerHTML =
        '<span class="status-dot" aria-hidden="true"></span><span>No sensor devices found yet. Waiting for the first Firebase snapshot…</span>';
      container.appendChild(div);
      updateGatewayHeader();
      renderDashboardInsights();
      return;
    }

    if (!state.selectedDeviceId || !state.devices[state.selectedDeviceId]) {
      state.selectedDeviceId = deviceIds[0];
    }

    for (const deviceId of deviceIds) {
      const room = state.devices[deviceId];
      const latest = state.latest[deviceId];

      const card = document.createElement("div");
      card.className = `sensor-card${deviceId === state.selectedDeviceId ? " selected" : ""}`;
      card.setAttribute("role", "button");
      card.setAttribute("tabindex", "0");
      card.dataset.deviceId = deviceId;

      const tempText = latest && typeof latest.temp_c === "number" ? `${latest.temp_c.toFixed(1)} °C` : "—";
      const humText = latest && typeof latest.humidity_rh === "number" ? `${latest.humidity_rh.toFixed(0)} %RH` : "—";

      card.innerHTML = `
        <div class="room-name">
          <span>${room || "Room"}</span>
          <span class="pill" style="padding:6px 10px; background: rgba(0, 0, 0, 0.25); color: var(--muted)">${deviceId}</span>
        </div>
        <div class="device-id">${latest?.timestamp ? `Last seen: ${formatDateTime(latest.tsMs)}` : "Awaiting data…"}</div>
        <div class="values">
          <div class="mini-metric">
            <div class="k">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" style="vertical-align:-2px; margin-right:6px">
                <path d="M14 14.76V5a2 2 0 1 0-4 0v9.76a4 4 0 1 0 4 0Z" stroke="rgba(56,189,248,1)" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
                <path d="M12 15a1 1 0 1 0 0-2 1 1 0 0 0 0 2Z" fill="rgba(56,189,248,1)"/>
              </svg>
              Temperature
            </div>
            <div class="v temp">${tempText}</div>
          </div>
          <div class="mini-metric">
            <div class="k">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" style="vertical-align:-2px; margin-right:6px">
                <path d="M12 2s7 7 7 13a7 7 0 0 1-14 0c0-6 7-13 7-13Z" stroke="rgba(168,85,247,1)" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
                <path d="M10 15c.5 2 3 2 4 0" stroke="rgba(168,85,247,0.9)" stroke-width="2" stroke-linecap="round"/>
              </svg>
              Humidity
            </div>
            <div class="v hum">${humText}</div>
          </div>
        </div>
      `;

      const select = () => {
        state.selectedDeviceId = deviceId;
        localStorage.setItem("enviGuard_selectedDeviceId", deviceId);
        renderSensorCards();
        scheduleRedraw();
        if (getVisiblePage() === "alarms") renderAlarmsTable().catch(() => {});
      };

      card.addEventListener("click", select);
      card.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          select();
        }
      });

      container.appendChild(card);
    }

    updateGatewayHeader();
    renderDashboardInsights();
  }

  async function fetchAndStoreOnce({ initial = false } = {}) {
    if (state.refreshing) {
      const age = state.refreshLockedAt ? Date.now() - state.refreshLockedAt : 0;
      if (age < 90_000) return;
      console.warn("Envi-Guard: clearing stuck refresh lock");
    }
    state.refreshing = true;
    state.refreshLockedAt = Date.now();

    try {
      if (initial) setConnectivity("pending", "Fetching data…");

      const res = await fetch(FIREBASE_ROOMS_URL, {
        cache: "no-store",
        credentials: "omit"
      });
      if (!res.ok) throw new Error(`Firebase fetch failed: ${res.status} ${res.statusText}`);
      const json = await res.json();
      const roomsRoot = normalizeRoomsPayload(json);

      const entries = [];
      const devices = {};

      for (const [deviceId, raw] of Object.entries(roomsRoot || {})) {
        const room = raw?.room || "";
        const temp = safeNumber(raw?.temp_c);
        const hum = safeNumber(raw?.humidity_rh);
        const timestamp = raw?.timestamp || null;
        const tsMsParsed = timestamp ? Date.parse(timestamp) : NaN;
        const tsMs = Number.isFinite(tsMsParsed) ? tsMsParsed : Date.now();
        const gatewayId = raw?.gateway_id != null ? String(raw.gateway_id) : "";
        const gatewaysOnline = safeNumber(raw?.gateways_online);
        const status = raw?.status != null ? String(raw.status) : "normal";
        const extraTel = extractExtraTelemetry(raw);

        devices[deviceId] = room;

        // Store only if at least one numeric variable exists.
        if (temp === null && hum === null) continue;

        const readingRow = {
          id: `${deviceId}_${tsMs}`,
          device_id: deviceId,
          room,
          ts: timestamp || toISO(new Date(tsMs)),
          tsMs,
          temp_c: temp,
          humidity_rh: hum,
          gateway_id: gatewayId,
          gateways_online: gatewaysOnline,
          status,
          ...extraTel
        };
        entries.push(readingRow);

        state.latest[deviceId] = {
          device_id: deviceId,
          room,
          temp_c: temp,
          humidity_rh: hum,
          timestamp: timestamp || toISO(new Date(tsMs)),
          tsMs,
          gateway_id: gatewayId,
          gateways_online: gatewaysOnline,
          status,
          ...extraTel
        };
      }

      state.devices = devices;
      localStorage.setItem("enviGuard_devices", JSON.stringify(devices));

      try {
        await upsertReadings(entries);
      } catch (idbErr) {
        console.error("IndexedDB upsert failed:", idbErr);
      }

      state.fetchCount += 1;
      if (state.fetchCount % 25 === 0) {
        pruneOldReadings().catch(() => {});
      }

      persistTrendCache(entries);

      setConnectivity("ok", "Connected");
      renderSensorCards();
      syncThresholdUI();
      await redrawVisible();
      if (getVisiblePage() === "alarms") {
        try {
          await renderAlarmsTable();
        } catch (t) {
          console.error(t);
        }
      }

      void (async () => {
        try {
          if (initial) {
            const backfillFromMs = Math.max(
              0,
              Date.now() - (TIME_WINDOWS["30d"]?.ms || 30 * 24 * 60 * 60 * 1000)
            );
            await backfillAlarmEvents(backfillFromMs);
          }
          // Always evaluate live alarms + email (backfill does not update connectivity state or send mail).
          await updateAlarmEventsFromLatest();
          if (getVisiblePage() === "alarms") await renderAlarmsTable();
        } catch (alarmErr) {
          console.error("Alarm update failed:", alarmErr);
        }
      })();
    } catch (e) {
      console.error(e);
      setConnectivity("error", "Offline / fetch error");
      const hint = $("dashboardDataHint");
      const hint2 = $("alarmsDataHint");
      if (hint) hint.textContent = "Firebase fetch failed. Displaying cached readings only.";
      if (hint2) hint2.textContent = "Firebase fetch failed. Alarm history is from cached readings only.";
      renderSensorCards();
      await redrawVisible();
      if (initial) {
        void (async () => {
          try {
            const backfillFromMs = Math.max(
              0,
              Date.now() - (TIME_WINDOWS["30d"]?.ms || 30 * 24 * 60 * 60 * 1000)
            );
            await backfillAlarmEvents(backfillFromMs);
            await updateAlarmEventsFromLatest();
            if (getVisiblePage() === "alarms") await renderAlarmsTable();
          } catch (alarmErr) {
            console.error(alarmErr);
          }
        })();
      }
      if (getVisiblePage() === "alarms") {
        try {
          await renderAlarmsTable();
        } catch (t) {
          console.error(t);
        }
      }
    } finally {
      state.refreshing = false;
      state.refreshLockedAt = 0;
    }
  }

  function startRefreshLoop() {
    if (state.refreshTimer) clearInterval(state.refreshTimer);
    state.refreshTimer = setInterval(() => fetchAndStoreOnce(), 60 * 1000);
  }

  function stopRefreshLoop() {
    if (state.refreshTimer) clearInterval(state.refreshTimer);
    state.refreshTimer = null;
  }

  function wireThresholdControl(rangeEl, numberEl, onChange) {
    const applyRange = () => {
      const raw = Number(rangeEl.value);
      if (!Number.isFinite(raw)) return;
      const rounded = Math.round(raw * 10) / 10;
      onChange(rounded);
    };
    const applyNumber = () => {
      const raw = Number(numberEl.value);
      if (!Number.isFinite(raw)) return;
      const rounded = Math.round(raw * 10) / 10;
      onChange(rounded);
    };

    rangeEl.addEventListener("input", () => {
      numberEl.value = rangeEl.value;
      applyRange();
    });
    numberEl.addEventListener("change", applyNumber);
    numberEl.addEventListener("input", () => {
      const n = Number(numberEl.value);
      if (Number.isFinite(n)) rangeEl.value = String(n);
    });
  }

  function wireTimeWindow(buttonId, menuId, target) {
    dropdownInit($(menuId), $(buttonId), (key) => {
      if (target === "trend") setTrendWindowUI(key);
      if (target === "alarms") setAlarmWindowUI(key);

      if (target === "trend") {
        if (getVisiblePage() === "dashboard") scheduleRedraw();
      } else if (target === "alarms") {
        if (getVisiblePage() === "alarms") renderAlarmsTable().catch(() => {});
      }
    });
  }

  async function initUI() {
    $("btnLogout").addEventListener("click", () => {
      localStorage.removeItem(AUTH_KEY);
      localStorage.removeItem(AUTH_USER_KEY);
      localStorage.removeItem(AUTH_ROLE_KEY);
      localStorage.removeItem("enviGuard_selectedDeviceId");
      state.auth.loggedIn = false;
      state.auth.user = null;
      state.auth.role = null;

      // Clear login form to avoid leaving credentials visible.
      $("loginUsername").value = "";
      $("loginPassword").value = "";
      $("loginError").classList.add("hidden");

      stopRefreshLoop();
      setVisibility();
      applyRolePermissions();
    });

    $("btnNavDashboard").addEventListener("click", () => {
      setActiveNav("dashboard");
      redrawVisible().catch(() => {});
      scrollToTop();
    });
    $("btnNavAlarms").addEventListener("click", () => {
      setActiveNav("alarms");
      renderAlarmsTable().catch(() => {});
      scrollToTop();
    });

    wireTimeWindow("timeWindowButtonDash", "timeWindowMenuDash", "trend");
    wireTimeWindow("timeWindowButtonAlarms", "timeWindowMenuAlarms", "alarms");

    wireThresholdControl($("tempThresholdRange"), $("tempThresholdNumber"), (val) => {
      if (state.auth.role !== "admin") return;
      state.thresholds.temp = clamp(val, 0, 100);
      syncThresholdUI();
      scheduleRedraw();
      scheduleAlarmHistoryRebuild();
    });
    wireThresholdControl($("tempThresholdRange2"), $("tempThresholdNumber2"), (val) => {
      if (state.auth.role !== "admin") return;
      state.thresholds.temp = clamp(val, 0, 100);
      syncThresholdUI();
      scheduleRedraw();
      scheduleAlarmHistoryRebuild();
    });

    wireThresholdControl($("humThresholdRange"), $("humThresholdNumber"), (val) => {
      if (state.auth.role !== "admin") return;
      state.thresholds.hum = clamp(val, 0, 100);
      syncThresholdUI();
      scheduleRedraw();
      scheduleAlarmHistoryRebuild();
    });
    wireThresholdControl($("humThresholdRange2"), $("humThresholdNumber2"), (val) => {
      if (state.auth.role !== "admin") return;
      state.thresholds.hum = clamp(val, 0, 100);
      syncThresholdUI();
      scheduleRedraw();
      scheduleAlarmHistoryRebuild();
    });

    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape") {
        $("timeWindowMenuDash")?.classList.add("hidden");
        $("timeWindowMenuAlarms")?.classList.add("hidden");
      }
    });

    loadEmailConfigIntoForm();
    const btnEmailSave = $("btnEmailAlertsSave");
    const btnEmailTest = $("btnEmailAlertsTest");
    const emailStatusEl = $("emailAlertsStatus");
    if (btnEmailSave) {
      btnEmailSave.addEventListener("click", () => {
        saveEmailAlarmConfig({
          enabled: !!$("emailAlertsEnabled")?.checked,
          publicKey: ($("emailJsPublicKey")?.value || "").trim(),
          serviceId: ($("emailJsServiceId")?.value || "").trim(),
          templateId: ($("emailJsTemplateId")?.value || "").trim(),
          toEmail: ($("emailNotifyTo")?.value || "").trim()
        });
        if (emailStatusEl) {
          emailStatusEl.textContent = "Settings saved.";
          emailStatusEl.classList.remove("hidden");
        }
      });
    }
    if (btnEmailTest) {
      btnEmailTest.addEventListener("click", async () => {
        if (emailStatusEl) {
          emailStatusEl.textContent = "Sending test…";
          emailStatusEl.classList.remove("hidden");
        }
        const testEv = {
          id: "TEST_email",
          device_id: "TEST_DEVICE",
          room: "Demo room",
          tsMs: Date.now(),
          variable: "Temperature",
          value: 37.5,
          threshold: state.thresholds.temp
        };
        await sendAlarmEmailsForEvents([testEv]);
        if (emailStatusEl) {
          const cfg = getEmailAlarmConfig();
          if (!cfg.enabled) {
            emailStatusEl.textContent = "Turn on “Enable email alerts”, save, then try again.";
          } else if (!cfg.publicKey || !cfg.serviceId || !cfg.templateId || !cfg.toEmail) {
            emailStatusEl.textContent = "Complete all EmailJS fields and the recipient email, then save.";
          } else {
            emailStatusEl.textContent =
              "Test send finished. Check the inbox (and the browser console if nothing arrived).";
          }
        }
      });
    }

    $("loginForm").addEventListener("submit", async (e) => {
      e.preventDefault();

      const u = $("loginUsername").value;
      const p = $("loginPassword").value;

      let role = null;
      if (u === DEFAULT_ADMIN_USER && p === DEFAULT_ADMIN_PASS) role = "admin";
      if (u === DEFAULT_OPERATOR_USER && p === DEFAULT_OPERATOR_PASS) role = "operator";

      if (!role) {
        const err = $("loginError");
        err.textContent = "Invalid credentials.";
        err.classList.remove("hidden");
        return;
      }

      state.auth.loggedIn = true;
      state.auth.user = u;
      state.auth.role = role;
      localStorage.setItem(AUTH_KEY, "1");
      localStorage.setItem(AUTH_USER_KEY, u);
      localStorage.setItem(AUTH_ROLE_KEY, role);

      state.devices = JSON.parse(localStorage.getItem("enviGuard_devices") || "{}");
      state.selectedDeviceId = localStorage.getItem("enviGuard_selectedDeviceId") || null;

      $("loginError").classList.add("hidden");
      setVisibility();
      applyRolePermissions();
      loadEmailConfigIntoForm();
      scrollToTop();

      await openDB();
      requestStoragePersistence();

      renderSensorCards();
      syncThresholdUI();
      setTrendWindowUI(state.trendWindowKey);
      setAlarmWindowUI(state.alarmWindowKey);
      await redrawVisible();
      if (getVisiblePage() === "alarms") await renderAlarmsTable();

      await fetchAndStoreOnce({ initial: true });
      startRefreshLoop();
    });
  }

  async function initFromAuthState() {
    state.auth.loggedIn = localStorage.getItem(AUTH_KEY) === "1";
    state.auth.user = localStorage.getItem(AUTH_USER_KEY);
    state.auth.role = localStorage.getItem(AUTH_ROLE_KEY);
    setVisibility();
    if (!state.auth.loggedIn) return;

    await openDB();
    requestStoragePersistence();
    applyRolePermissions();
    state.devices = JSON.parse(localStorage.getItem("enviGuard_devices") || "{}");
    state.selectedDeviceId = localStorage.getItem("enviGuard_selectedDeviceId") || null;

    setActiveNav("dashboard");
    renderSensorCards();
    syncThresholdUI();
    setTrendWindowUI(state.trendWindowKey);
    setAlarmWindowUI(state.alarmWindowKey);
    await redrawVisible();

    await fetchAndStoreOnce({ initial: true });
    startRefreshLoop();
  }

  function warnIfOpenedAsFile() {
    if (window.location.protocol !== "file:") return;
    const shell = document.querySelector(".app-shell");
    if (!shell) return;
    const bar = document.createElement("div");
    bar.className = "file-protocol-banner";
    bar.setAttribute("role", "alert");
    bar.innerHTML =
      "<strong>Opened as a local file (file://)</strong> — browsers usually block loading data from the internet this way, " +
      "so Firebase may not work. <strong>Use a local web server</strong> instead: open a terminal in this folder and run " +
      "<code>python -m http.server 8080</code> (or <code>py -m http.server 8080</code>), then visit " +
      "<code>http://localhost:8080</code>. GitHub Pages works because the site is served over <code>https://</code>.";
    shell.insertBefore(bar, shell.firstChild);
  }

  async function initApp() {
    warnIfOpenedAsFile();

    setTrendWindowUI(state.trendWindowKey);
    setAlarmWindowUI(state.alarmWindowKey);
    syncThresholdUI();
    setActiveNav("dashboard");

    $("btnLogout").classList.add("hidden");
    setVisibility();

    await initUI();
    await initFromAuthState();

    let resizeTimer = null;
    window.addEventListener("resize", () => {
      if (resizeTimer) clearTimeout(resizeTimer);
      resizeTimer = setTimeout(scheduleRedraw, 250);
    });
  }

  document.addEventListener("DOMContentLoaded", () => {
    initApp().catch((e) => {
      console.error(e);
      setConnectivity("error", "Initialization error");
    });
  });
})();


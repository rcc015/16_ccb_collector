const http = require("http");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const { URL } = require("url");
const { OAuth2Client } = require("google-auth-library");
require("dotenv").config();

const PORT = process.env.PORT || 3000;
const BASE_PATH = normalizeBasePath(process.env.BASE_PATH || "");
const PUBLIC_DIR = path.join(__dirname, "public");
const DATA_DIR = path.join(__dirname, "data");
const STATE_FILE = path.join(DATA_DIR, "state.json");
const SEED_FILE = path.join(DATA_DIR, "seed.json");
const AREA_DIRECTORY_FILE = path.join(DATA_DIR, "area-directory.json");
const GOOGLE_SNAPSHOT_FILE = path.join(DATA_DIR, "google-open-items-snapshot.json");
const LIVE_SYNC_CONFIG_FILE = path.join(DATA_DIR, "live-sync-config.json");
const AUTH_CONFIG_FILE = path.join(DATA_DIR, "auth-config.json");
const SESSION_COOKIE_NAME = "ccb_session";

const MIME_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".txt": "text/plain; charset=utf-8",
};

let liveSyncStatus = {
  enabled: false,
  sourceType: "csv",
  sourceUrl: "",
  intervalMs: 300000,
  lastCheckedAt: null,
  lastSyncedAt: null,
  lastFingerprint: "",
  lastChangeDetectedAt: null,
  lastError: "",
  isRunning: false,
};
let liveSyncTimer = null;
const sessionSecret = process.env.SESSION_SECRET || "local-dev-secret-change-me";
const googleClient = new OAuth2Client();

function normalizeBasePath(value) {
  const raw = String(value || "").trim();
  if (!raw || raw === "/") {
    return "";
  }

  const withLeadingSlash = raw.startsWith("/") ? raw : `/${raw}`;
  return withLeadingSlash.replace(/\/+$/, "");
}

function stripBasePath(pathname) {
  if (!BASE_PATH) {
    return pathname;
  }

  if (pathname === BASE_PATH) {
    return "/";
  }

  if (pathname.startsWith(`${BASE_PATH}/`)) {
    return pathname.slice(BASE_PATH.length) || "/";
  }

  return null;
}

function withBasePath(pathname) {
  if (!BASE_PATH) {
    return pathname;
  }

  return pathname === "/" ? `${BASE_PATH}/` : `${BASE_PATH}${pathname}`;
}

function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function writeJson(filePath, payload) {
  fs.writeFileSync(filePath, JSON.stringify(payload, null, 2));
}

function createDefaultState() {
  if (fs.existsSync(SEED_FILE)) {
    return readJson(SEED_FILE);
  }

  return {
    areas: [],
    openItems: [],
    lastSavedAt: new Date().toISOString(),
  };
}

function readOptionalJson(filePath, fallback) {
  if (!fs.existsSync(filePath)) {
    return fallback;
  }

  return readJson(filePath);
}

function ensureStateFile() {
  ensureDataDir();

  if (!fs.existsSync(STATE_FILE)) {
    writeJson(STATE_FILE, createDefaultState());
  }
}

function loadState() {
  ensureStateFile();
  return readJson(STATE_FILE);
}

function persistState(nextState) {
  nextState.lastSavedAt = new Date().toISOString();
  writeJson(STATE_FILE, nextState);
  return nextState;
}

function getAreaDirectory() {
  const payload = readOptionalJson(AREA_DIRECTORY_FILE, { areas: [] });
  return Array.isArray(payload.areas) ? payload.areas : [];
}

function normalizeStatus(value) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();

  if (normalized === "closed") {
    return "closed";
  }

  if (normalized === "deferred" || normalized === "on hold") {
    return "deferred";
  }

  return "open";
}

function parseScore(value) {
  const numeric = Number.parseFloat(String(value || "").trim());
  return Number.isFinite(numeric) ? numeric : null;
}

function inferSubstantialChange(row) {
  const score = parseScore(row.ccbScore);
  const ccbStatus = String(row.ccbStatus || "").trim().toUpperCase();

  return Boolean(
    score !== null ||
      (ccbStatus && ccbStatus !== "NO LOCALIZED" && ccbStatus !== "N/A")
  );
}

function rowHasUsefulContent(row) {
  return Boolean(String(row.id || "").trim() && String(row.description || "").trim());
}

function loadGoogleSnapshot() {
  return readOptionalJson(GOOGLE_SNAPSHOT_FILE, {
    spreadsheetId: "",
    spreadsheetTitle: "",
    sheetName: "Open Item List",
    importedAt: "",
    headers: [],
    rows: [],
  });
}

function loadLiveSyncConfig() {
  return readOptionalJson(LIVE_SYNC_CONFIG_FILE, {
    enabled: false,
    sourceType: "csv",
    sourceUrl: "",
    intervalMs: 300000,
  });
}

function loadAuthConfig() {
  const fileConfig = readOptionalJson(AUTH_CONFIG_FILE, {});
  return {
    enabled: process.env.AUTH_ENABLED === "false" ? false : fileConfig.enabled !== false,
    googleClientId: process.env.GOOGLE_CLIENT_ID || fileConfig.googleClientId || "",
    allowedDomain: process.env.ALLOWED_GOOGLE_DOMAIN || fileConfig.allowedDomain || "conceivable.life",
    appBaseUrl: process.env.APP_BASE_URL || fileConfig.appBaseUrl || "",
  };
}

function persistLiveSyncConfig(config) {
  writeJson(LIVE_SYNC_CONFIG_FILE, config);
  return config;
}

function computeFingerprint(value) {
  return JSON.stringify(value);
}

function signValue(value) {
  return crypto.createHmac("sha256", sessionSecret).update(value).digest("hex");
}

function encodeSessionCookie(payload) {
  const body = Buffer.from(JSON.stringify(payload), "utf8").toString("base64url");
  const signature = signValue(body);
  return `${body}.${signature}`;
}

function decodeSessionCookie(cookieValue) {
  if (!cookieValue || !cookieValue.includes(".")) {
    return null;
  }

  const [body, signature] = cookieValue.split(".");
  if (!body || !signature) {
    return null;
  }

  const expected = signValue(body);
  const signatureBuffer = Buffer.from(signature);
  const expectedBuffer = Buffer.from(expected);
  if (signatureBuffer.length !== expectedBuffer.length) {
    return null;
  }

  if (!crypto.timingSafeEqual(signatureBuffer, expectedBuffer)) {
    return null;
  }

  const payload = JSON.parse(Buffer.from(body, "base64url").toString("utf8"));
  if (!payload.exp || Date.now() > payload.exp) {
    return null;
  }

  return payload;
}

function parseCookies(request) {
  const raw = request.headers.cookie || "";
  const cookies = {};
  raw.split(";").forEach((entry) => {
    const trimmed = entry.trim();
    if (!trimmed) {
      return;
    }

    const separatorIndex = trimmed.indexOf("=");
    if (separatorIndex === -1) {
      return;
    }

    const key = trimmed.slice(0, separatorIndex);
    const value = trimmed.slice(separatorIndex + 1);
    cookies[key] = decodeURIComponent(value);
  });
  return cookies;
}

function getCurrentUser(request) {
  const authConfig = loadAuthConfig();
  if (!authConfig.enabled) {
    return {
      email: "local@conceivable.life",
      name: "Local Access",
      hd: authConfig.allowedDomain,
    };
  }

  const cookies = parseCookies(request);
  const session = decodeSessionCookie(cookies[SESSION_COOKIE_NAME]);
  return session ? session.user : null;
}

function requiresAuth(pathname) {
  if (pathname.startsWith("/api/auth/")) {
    return false;
  }

  if (!pathname.startsWith("/api/")) {
    return false;
  }

  return true;
}

function setCookie(response, cookieValue, maxAgeSeconds = 28800) {
  const authConfig = loadAuthConfig();
  const secureFlag = authConfig.appBaseUrl.startsWith("https://") ? "; Secure" : "";
  response.setHeader("Set-Cookie", [
    `${SESSION_COOKIE_NAME}=${encodeURIComponent(cookieValue)}; HttpOnly; Path=${BASE_PATH || "/"}; Max-Age=${maxAgeSeconds}; SameSite=Lax${secureFlag}`,
  ]);
}

function clearCookie(response) {
  const authConfig = loadAuthConfig();
  const secureFlag = authConfig.appBaseUrl.startsWith("https://") ? "; Secure" : "";
  response.setHeader("Set-Cookie", [
    `${SESSION_COOKIE_NAME}=; HttpOnly; Path=${BASE_PATH || "/"}; Max-Age=0; SameSite=Lax${secureFlag}`,
  ]);
}

async function verifyGoogleCredential(credential) {
  const authConfig = loadAuthConfig();
  if (!authConfig.googleClientId) {
    throw new Error("GOOGLE_CLIENT_ID no configurado");
  }

  const ticket = await googleClient.verifyIdToken({
    idToken: credential,
    audience: authConfig.googleClientId,
  });
  const payload = ticket.getPayload();
  if (!payload) {
    throw new Error("No se pudo leer el payload del token");
  }

  if (!payload.iss || !["accounts.google.com", "https://accounts.google.com"].includes(payload.iss)) {
    throw new Error("Issuer de Google invalido");
  }

  if (payload.email_verified !== true) {
    throw new Error("Google email no verificado");
  }

  if ((payload.hd || "").toLowerCase() !== authConfig.allowedDomain.toLowerCase()) {
    throw new Error(`Dominio no permitido: ${payload.hd || "unknown"}`);
  }

  return {
    email: payload.email,
    name: payload.name || payload.given_name || payload.email,
    picture: payload.picture || "",
    hd: payload.hd,
  };
}

function mapSnapshotToState(snapshot, existingState = null) {
  const areas = getAreaDirectory();
  const areaColumns = areas.filter((area) => area.sourceColumn);
  const previousVotes = new Map(
    (existingState?.openItems || []).map((item) => [item.id, item.votes || []]),
  );

  const openItems = (snapshot.rows || [])
    .filter(rowHasUsefulContent)
    .map((row) => {
      const impactedAreaIds = areaColumns
        .filter((area) => String(row[area.sourceColumn] || "").trim().toUpperCase() === "TRUE")
        .map((area) => area.id);

      const fallbackOwnerArea = impactedAreaIds[0] || "";
      return {
        id: row.id,
        title: row.description,
        description: row.comments || "",
        sourceRef: `${snapshot.spreadsheetTitle || "Google Sheet"} / ${snapshot.sheetName || "Open Item List"}`,
        ownerAreaId: fallbackOwnerArea,
        ownerName: row.owner || "",
        ownerAreaHint: "",
        impactedAreaIds,
        isSubstantial: inferSubstantialChange(row),
        status: normalizeStatus(row.status),
        createdAt: row.dateCreated || new Date().toISOString(),
        dueDate: row.dueDate || "",
        externalStatus: row.ccbStatus || "",
        ccbScore: row.ccbScore || "",
        rawSheetRow: row,
        votes: previousVotes.get(row.id) || [],
      };
    });

  return {
    areas,
    openItems,
    source: {
      type: "google-sheet-snapshot",
      spreadsheetId: snapshot.spreadsheetId || "",
      spreadsheetTitle: snapshot.spreadsheetTitle || "",
      importedAt: snapshot.importedAt || new Date().toISOString(),
    },
    lastSavedAt: new Date().toISOString(),
  };
}

function todayStamp(date = new Date()) {
  return date.toISOString().slice(0, 10);
}

function derivePrerequisiteStatus(state) {
  const openItems = state.openItems.filter((item) => item.status !== "closed");
  const substantialOpenItems = openItems.filter((item) => item.isSubstantial);
  const blockedItems = substantialOpenItems.filter((item) => {
    const approvals = item.votes.filter((vote) => vote.decision === "approve");
    return approvals.length < item.impactedAreaIds.length;
  });

  if (blockedItems.length > 0) {
    return {
      status: "blocked",
      label: "No cumple prerequisito",
      detail: "Hay cambios sustanciales abiertos sin aprobacion completa de las areas impactadas.",
      blockedItems: blockedItems.map((item) => item.id),
    };
  }

  if (substantialOpenItems.length > 0) {
    return {
      status: "review",
      label: "Requiere revision",
      detail: "Hay cambios sustanciales abiertos, pero ya cuentan con aprobaciones suficientes.",
      blockedItems: [],
    };
  }

  return {
    status: "clear",
    label: "Cumple prerequisito",
    detail: "No hay cambios sustanciales abiertos en el open item list.",
    blockedItems: [],
  };
}

function buildAreaMap(state) {
  return new Map(state.areas.map((area) => [area.id, area]));
}

function buildOpenReport(state) {
  const areaMap = buildAreaMap(state);
  return state.openItems
    .filter((item) => item.status !== "closed")
    .map((item) => ({
      id: item.id,
      title: item.title,
      status: item.status,
      isSubstantial: item.isSubstantial,
      ownerArea: areaMap.get(item.ownerAreaId)?.name || "Unassigned",
      impactedAreas: item.impactedAreaIds.map((areaId) => areaMap.get(areaId)?.name || areaId),
      approvals: item.votes.filter((vote) => vote.decision === "approve").length,
      rejections: item.votes.filter((vote) => vote.decision === "reject").length,
      needsInfo: item.votes.filter((vote) => vote.decision === "needs-info").length,
    }));
}

function buildDailyReport(state) {
  const areaMap = buildAreaMap(state);
  const stamp = todayStamp();
  const newVotes = [];

  state.openItems.forEach((item) => {
    item.votes.forEach((vote) => {
      if ((vote.createdAt || "").slice(0, 10) === stamp) {
        newVotes.push({
          openItemId: item.id,
          title: item.title,
          area: areaMap.get(vote.areaId)?.name || vote.areaId,
          owner: areaMap.get(vote.areaId)?.owner || "Unknown",
          decision: vote.decision,
          comment: vote.comment,
          createdAt: vote.createdAt,
        });
      }
    });
  });

  const ccbCandidates = state.openItems
    .filter((item) => item.status !== "closed")
    .filter((item) => item.isSubstantial || item.votes.some((vote) => vote.decision !== "approve"))
    .map((item) => ({
      id: item.id,
      title: item.title,
      impactedAreas: item.impactedAreaIds.map((areaId) => areaMap.get(areaId)?.name || areaId),
      pendingVotes: item.impactedAreaIds.filter(
        (areaId) => !item.votes.some((vote) => vote.areaId === areaId && vote.decision === "approve"),
      ),
    }))
    .map((item) => ({
      ...item,
      pendingVotes: item.pendingVotes.map((areaId) => areaMap.get(areaId)?.name || areaId),
    }));

  return {
    date: stamp,
    prerequisite: derivePrerequisiteStatus(state),
    newVotes,
    openSummary: buildOpenReport(state),
    ccbCandidates,
  };
}

function sendJson(response, statusCode, payload) {
  response.writeHead(statusCode, { "Content-Type": "application/json; charset=utf-8" });
  response.end(JSON.stringify(payload, null, 2));
}

function parseBody(request) {
  return new Promise((resolve, reject) => {
    let raw = "";

    request.on("data", (chunk) => {
      raw += chunk;
    });

    request.on("end", () => {
      if (!raw) {
        resolve({});
        return;
      }

      try {
        resolve(JSON.parse(raw));
      } catch (error) {
        reject(error);
      }
    });

    request.on("error", reject);
  });
}

function parseCsvLine(line) {
  const values = [];
  let current = "";
  let inQuotes = false;

  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    const next = line[index + 1];

    if (char === "\"") {
      if (inQuotes && next === "\"") {
        current += "\"";
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === "," && !inQuotes) {
      values.push(current);
      current = "";
      continue;
    }

    current += char;
  }

  values.push(current);
  return values.map((value) => value.trim());
}

function parseCsv(text) {
  const normalized = String(text || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  const lines = normalized
    .split("\n")
    .map((line) => line.trimEnd())
    .filter((line) => line.length > 0);

  if (!lines.length) {
    return [];
  }

  return lines.map(parseCsvLine);
}

function buildSnapshotFromCsv(csvText, spreadsheetTitle = "Imported Open Item List CSV") {
  const rows = parseCsv(csvText);
  if (!rows.length) {
    throw new Error("CSV vacio");
  }

  const headerRow = rows[0];
  const bodyRows = rows.slice(1);
  const columnIndexes = new Map(headerRow.map((header, index) => [header, index]));

  const getValue = (row, header) => row[columnIndexes.get(header)] || "";

  const snapshot = {
    spreadsheetId: "",
    spreadsheetTitle,
    sheetName: "Open Item List",
    importedAt: new Date().toISOString(),
    headers: headerRow,
    rows: bodyRows.map((row) => ({
      id: getValue(row, "ID"),
      description: getValue(row, "Open Item Description"),
      owner: getValue(row, "Owner"),
      dateCreated: getValue(row, "Date Created"),
      dueDate: getValue(row, "Due Date"),
      status: getValue(row, "Status"),
      comments: getValue(row, "Comments/Updates"),
      Software: getValue(row, "Software"),
      Product: getValue(row, "Product"),
      Quality: getValue(row, "Quality"),
      Machine: getValue(row, "Machine"),
      Testing: getValue(row, "Testing"),
      Infra: getValue(row, "Infra"),
      Optics: getValue(row, "Optics"),
      Data: getValue(row, "Data"),
      Research: getValue(row, "Research"),
      Exploration: getValue(row, "Exploration"),
      Mecha: getValue(row, "Mecha"),
      minutesRelated: getValue(row, "Minutes Related"),
      gitRepository: getValue(row, "Git Repository"),
      ccbScore: getValue(row, "CCB Score"),
      ccbStatus: getValue(row, "CCB Status"),
      jiraTicketsRelated: getValue(row, "Jira Tickets Related"),
    })),
  };

  writeJson(GOOGLE_SNAPSHOT_FILE, snapshot);
  return snapshot;
}

function buildSnapshotFromJson(payload) {
  if (!payload || !Array.isArray(payload.rows)) {
    throw new Error("JSON invalido para live sync");
  }

  const snapshot = {
    spreadsheetId: payload.spreadsheetId || "",
    spreadsheetTitle: payload.spreadsheetTitle || "Live Sync JSON",
    sheetName: payload.sheetName || "Open Item List",
    importedAt: new Date().toISOString(),
    headers: Array.isArray(payload.headers) ? payload.headers : [],
    rows: payload.rows,
  };

  writeJson(GOOGLE_SNAPSHOT_FILE, snapshot);
  return snapshot;
}

async function importSnapshotIntoState(snapshot) {
  const currentState = loadState();
  const nextState = persistState(mapSnapshotToState(snapshot, currentState));
  return nextState;
}

async function fetchLiveSyncSnapshot(config) {
  if (!config.sourceUrl) {
    throw new Error("No live sync source URL configured");
  }

  const response = await fetch(config.sourceUrl, {
    headers: {
      "Cache-Control": "no-cache",
    },
  });

  if (!response.ok) {
    throw new Error(`Live sync fetch failed with status ${response.status}`);
  }

  if (config.sourceType === "json") {
    const payload = await response.json();
    return buildSnapshotFromJson(payload);
  }

  const csvText = await response.text();
  return buildSnapshotFromCsv(csvText, "Live Sync CSV");
}

async function runLiveSyncCheck() {
  const config = loadLiveSyncConfig();
  liveSyncStatus = {
    ...liveSyncStatus,
    enabled: Boolean(config.enabled),
    sourceType: config.sourceType || "csv",
    sourceUrl: config.sourceUrl || "",
    intervalMs: Number(config.intervalMs) || 300000,
    isRunning: true,
    lastCheckedAt: new Date().toISOString(),
    lastError: "",
  };

  if (!config.enabled || !config.sourceUrl) {
    liveSyncStatus.isRunning = false;
    return;
  }

  try {
    const snapshot = await fetchLiveSyncSnapshot(config);
    const nextFingerprint = computeFingerprint(snapshot.rows || []);

    if (nextFingerprint !== liveSyncStatus.lastFingerprint) {
      await importSnapshotIntoState(snapshot);
      liveSyncStatus.lastFingerprint = nextFingerprint;
      liveSyncStatus.lastSyncedAt = new Date().toISOString();
      liveSyncStatus.lastChangeDetectedAt = liveSyncStatus.lastSyncedAt;
    }
  } catch (error) {
    liveSyncStatus.lastError = error.message;
  } finally {
    liveSyncStatus.isRunning = false;
  }
}

function restartLiveSyncScheduler() {
  if (liveSyncTimer) {
    clearInterval(liveSyncTimer);
    liveSyncTimer = null;
  }

  const config = loadLiveSyncConfig();
  liveSyncStatus = {
    ...liveSyncStatus,
    enabled: Boolean(config.enabled),
    sourceType: config.sourceType || "csv",
    sourceUrl: config.sourceUrl || "",
    intervalMs: Number(config.intervalMs) || 300000,
  };

  if (!config.enabled || !config.sourceUrl) {
    return;
  }

  liveSyncTimer = setInterval(() => {
    runLiveSyncCheck().catch((error) => {
      liveSyncStatus.lastError = error.message;
      liveSyncStatus.isRunning = false;
    });
  }, liveSyncStatus.intervalMs);
}

function sanitizeState(input) {
  return {
    areas: Array.isArray(input.areas) ? input.areas : [],
    openItems: Array.isArray(input.openItems)
      ? input.openItems.map((item) => ({
          id: item.id,
          title: item.title,
          description: item.description || "",
          sourceRef: item.sourceRef || "",
          ownerAreaId: item.ownerAreaId || "",
          ownerName: item.ownerName || "",
          ownerAreaHint: item.ownerAreaHint || "",
          impactedAreaIds: Array.isArray(item.impactedAreaIds) ? item.impactedAreaIds : [],
          isSubstantial: Boolean(item.isSubstantial),
          status: item.status || "open",
          createdAt: item.createdAt || new Date().toISOString(),
          dueDate: item.dueDate || "",
          externalStatus: item.externalStatus || "",
          ccbScore: item.ccbScore || "",
          rawSheetRow: item.rawSheetRow || null,
          votes: Array.isArray(item.votes)
            ? item.votes.map((vote) => ({
                areaId: vote.areaId,
                decision: vote.decision,
                comment: vote.comment || "",
                createdAt: vote.createdAt || new Date().toISOString(),
              }))
            : [],
        }))
      : [],
    source: input.source || null,
    lastSavedAt: new Date().toISOString(),
  };
}

function serveStatic(requestPath, response) {
  const targetPath = requestPath === "/" ? "/index.html" : requestPath;
  const filePath = path.join(PUBLIC_DIR, path.normalize(targetPath));

  if (!filePath.startsWith(PUBLIC_DIR)) {
    sendJson(response, 403, { error: "Forbidden" });
    return;
  }

  if (!fs.existsSync(filePath) || fs.statSync(filePath).isDirectory()) {
    sendJson(response, 404, { error: "Not found" });
    return;
  }

  if (targetPath === "/index.html") {
    const html = fs
      .readFileSync(filePath, "utf8")
      .replace(/__APP_BASE_PATH__/g, BASE_PATH);
    response.writeHead(200, {
      "Content-Type": "text/html; charset=utf-8",
    });
    response.end(html);
    return;
  }

  const ext = path.extname(filePath);
  response.writeHead(200, {
    "Content-Type": MIME_TYPES[ext] || "application/octet-stream",
  });
  fs.createReadStream(filePath).pipe(response);
}

const server = http.createServer(async (request, response) => {
  const url = new URL(request.url, `http://${request.headers.host}`);
  const pathname = stripBasePath(url.pathname);

  try {
    if (BASE_PATH && url.pathname === BASE_PATH) {
      response.writeHead(302, { Location: withBasePath("/") });
      response.end();
      return;
    }

    if (pathname === null) {
      sendJson(response, 404, { error: "Not found" });
      return;
    }

    if (requiresAuth(pathname) && !getCurrentUser(request)) {
      sendJson(response, 401, {
        error: "Unauthorized",
        detail: "Google authentication required",
      });
      return;
    }

    if (request.method === "GET" && pathname === "/api/auth/config") {
      const authConfig = loadAuthConfig();
      sendJson(response, 200, {
        enabled: authConfig.enabled,
        googleClientId: authConfig.googleClientId,
        allowedDomain: authConfig.allowedDomain,
        appBaseUrl: authConfig.appBaseUrl || withBasePath(""),
        user: getCurrentUser(request),
      });
      return;
    }

    if (request.method === "POST" && pathname === "/api/auth/google") {
      const body = await parseBody(request);
      const user = await verifyGoogleCredential(String(body.credential || ""));
      const sessionValue = encodeSessionCookie({
        user,
        exp: Date.now() + (8 * 60 * 60 * 1000),
      });
      setCookie(response, sessionValue);
      sendJson(response, 200, {
        ok: true,
        user,
      });
      return;
    }

    if (request.method === "POST" && pathname === "/api/auth/logout") {
      clearCookie(response);
      sendJson(response, 200, { ok: true });
      return;
    }

    if (request.method === "GET" && pathname === "/api/state") {
      const state = loadState();
      sendJson(response, 200, {
        state,
        prerequisite: derivePrerequisiteStatus(state),
        dailyReport: buildDailyReport(state),
      });
      return;
    }

    if (request.method === "POST" && pathname === "/api/state") {
      const body = await parseBody(request);
      const nextState = persistState(sanitizeState(body));
      sendJson(response, 200, {
        state: nextState,
        prerequisite: derivePrerequisiteStatus(nextState),
        dailyReport: buildDailyReport(nextState),
      });
      return;
    }

    if (request.method === "GET" && pathname === "/api/reports/daily") {
      sendJson(response, 200, buildDailyReport(loadState()));
      return;
    }

    if (request.method === "POST" && pathname === "/api/reset") {
      const nextState = persistState(createDefaultState());
      sendJson(response, 200, {
        state: nextState,
        prerequisite: derivePrerequisiteStatus(nextState),
        dailyReport: buildDailyReport(nextState),
      });
      return;
    }

    if (request.method === "POST" && pathname === "/api/import/google-sheet-snapshot") {
      const currentState = loadState();
      const nextState = persistState(mapSnapshotToState(loadGoogleSnapshot(), currentState));
      sendJson(response, 200, {
        state: nextState,
        prerequisite: derivePrerequisiteStatus(nextState),
        dailyReport: buildDailyReport(nextState),
      });
      return;
    }

    if (request.method === "POST" && pathname === "/api/import/google-sheet-csv") {
      const body = await parseBody(request);
      const snapshot = buildSnapshotFromCsv(body.csvText, body.spreadsheetTitle);
      const nextState = await importSnapshotIntoState(snapshot);
      sendJson(response, 200, {
        state: nextState,
        prerequisite: derivePrerequisiteStatus(nextState),
        dailyReport: buildDailyReport(nextState),
      });
      return;
    }

    if (request.method === "GET" && pathname === "/api/live-sync") {
      sendJson(response, 200, {
        config: loadLiveSyncConfig(),
        status: liveSyncStatus,
      });
      return;
    }

    if (request.method === "POST" && pathname === "/api/live-sync") {
      const body = await parseBody(request);
      const nextConfig = persistLiveSyncConfig({
        enabled: Boolean(body.enabled),
        sourceType: body.sourceType === "json" ? "json" : "csv",
        sourceUrl: String(body.sourceUrl || "").trim(),
        intervalMs: Math.max(30000, Number(body.intervalMs) || 300000),
      });
      restartLiveSyncScheduler();
      sendJson(response, 200, {
        config: nextConfig,
        status: liveSyncStatus,
      });
      return;
    }

    if (request.method === "POST" && pathname === "/api/live-sync/run") {
      await runLiveSyncCheck();
      const state = loadState();
      sendJson(response, 200, {
        config: loadLiveSyncConfig(),
        status: liveSyncStatus,
        state,
        prerequisite: derivePrerequisiteStatus(state),
        dailyReport: buildDailyReport(state),
      });
      return;
    }

    serveStatic(pathname, response);
  } catch (error) {
    sendJson(response, 500, {
      error: "Internal server error",
      detail: error.message,
    });
  }
});

server.listen(PORT, () => {
  ensureStateFile();
  restartLiveSyncScheduler();
  console.log(`CCB Collector running on http://localhost:${PORT}${withBasePath("/")}`);
});

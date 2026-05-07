const http = require("http");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const { URL } = require("url");
const { JWT, OAuth2Client } = require("google-auth-library");
require("dotenv").config();

const PORT = process.env.PORT || 3000;
const BASE_PATH = normalizeBasePath(process.env.BASE_PATH || "");
const PUBLIC_DIR = path.join(__dirname, "public");
const DATA_DIR = path.join(__dirname, "data");
const SOURCE_SPREADSHEET_ID = process.env.SOURCE_SPREADSHEET_ID || "1G6YNnnIrqEH_oIgq95bXmrER2lUjPYtkCeV5zwaXRms";
const SOURCE_SHEET_NAME = process.env.SOURCE_SHEET_NAME || "Open Item List";
const SOURCE_SHEET_HEADER_ROW = Number(process.env.SOURCE_SHEET_HEADER_ROW || 0);
const AREAS_SHEET_NAME = process.env.AREAS_SHEET_NAME || "CCB Areas";
const VOTES_SHEET_NAME = process.env.VOTES_SHEET_NAME || "CCB Votes";
const PRESESSION_SHEET_NAME = process.env.PRESESSION_SHEET_NAME || "CCB PreSession";
const CCB_DECISION_SHEET_NAME = process.env.CCB_DECISION_SHEET_NAME || "CCB_Decision";
const GOOGLE_SHEETS_AUTH_MODE = process.env.GOOGLE_SHEETS_AUTH_MODE || "";
const GOOGLE_SERVICE_ACCOUNT_KEY_PATH = process.env.GOOGLE_SERVICE_ACCOUNT_KEY_PATH || "";
const GOOGLE_SERVICE_ACCOUNT_JSON = process.env.GOOGLE_SERVICE_ACCOUNT_JSON || "";
const GOOGLE_OAUTH_CLIENT_ID = process.env.GOOGLE_OAUTH_CLIENT_ID || process.env.GOOGLE_CLIENT_ID || "";
const GOOGLE_OAUTH_CLIENT_SECRET = process.env.GOOGLE_OAUTH_CLIENT_SECRET || "";
const GOOGLE_OAUTH_REDIRECT_URI = process.env.GOOGLE_OAUTH_REDIRECT_URI || "";
const STATE_FILE = path.join(DATA_DIR, "state.json");
const SEED_FILE = path.join(DATA_DIR, "seed.json");
const AREA_DIRECTORY_FILE = path.join(DATA_DIR, "area-directory.json");
const EMPLOYEE_DIRECTORY_FILE = path.join(DATA_DIR, "employee-directory.json");
const GOOGLE_SNAPSHOT_FILE = path.join(DATA_DIR, "google-open-items-snapshot.json");
const LIVE_SYNC_CONFIG_FILE = path.join(DATA_DIR, "live-sync-config.json");
const AUTH_CONFIG_FILE = path.join(DATA_DIR, "auth-config.json");
const USER_GOOGLE_OAUTH_TOKEN_FILE = path.join(DATA_DIR, "google-user-oauth-token.json");
const SESSION_COOKIE_NAME = "ccb_session";
const OPEN_ITEM_HEADERS = [
  "ID",
  "Open Item Description",
  "Owner",
  "Date Created",
  "Due Date",
  "Status",
  "Comments/Updates",
  "Software",
  "Product",
  "Quality",
  "Machine",
  "Testing",
  "Infra",
  "Optics",
  "Data",
  "Research",
  "Exploration",
  "Mecha",
  "Minutes Related",
  "Git Repository",
  "CCB Score",
  "CCB Status",
  "Jira Tickets Related",
];
const AREA_HEADERS = ["id", "name", "owner", "email", "sourceColumn"];
const VOTE_HEADERS = ["openItemId", "areaId", "decision", "comment", "createdAt"];
const PRESESSION_HEADERS = [
  "openItemId",
  "areaId",
  "decision",
  "comment",
  "requestedAt",
  "lastNotifiedAt",
  "respondedAt",
  "responderEmail",
];
const CCB_DECISION_HEADERS = [
  "OI_ID",
  "Open Item Description",
  "Strategic Alignment Rationale",
  "Strategic Alignment Score",
  "Risk Assessment Rationale",
  "Risk Assessment Score",
  "Resource Impact Rationale",
  "Resource Impact Score",
  "Customer Value Rationale",
  "Customer Value Score",
  "Operational Feasibility Rationale",
  "Operational Feasibility Score",
  "Total",
  "Status",
];
const GOOGLE_SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"];
const GOOGLE_USER_OAUTH_SCOPES = [
  ...GOOGLE_SHEETS_SCOPES,
  "https://www.googleapis.com/auth/gmail.send",
];

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

function deleteIfExists(filePath) {
  if (fs.existsSync(filePath)) {
    fs.unlinkSync(filePath);
  }
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

async function loadStateStore() {
  if (isGoogleSheetsStorageEnabled()) {
    return loadStateFromGoogleSheets();
  }

  return loadState();
}

async function persistStateStore(nextState) {
  if (isGoogleSheetsStorageEnabled()) {
    return persistStateToGoogleSheets(nextState);
  }

  return persistState(nextState);
}

function getAreaDirectory() {
  const payload = readOptionalJson(AREA_DIRECTORY_FILE, { areas: [] });
  return Array.isArray(payload.areas) ? payload.areas : [];
}

function resolveAreas(areasOverride = null, existingState = null) {
  if (Array.isArray(areasOverride) && areasOverride.length) {
    return areasOverride;
  }

  if (Array.isArray(existingState?.areas) && existingState.areas.length) {
    return existingState.areas;
  }

  return getAreaDirectory();
}

function getEmployeeDirectory() {
  const payload = readOptionalJson(EMPLOYEE_DIRECTORY_FILE, {
    importedAt: "",
    sourceName: "",
    contacts: [],
  });

  return {
    importedAt: payload.importedAt || "",
    sourceName: payload.sourceName || "",
    contacts: Array.isArray(payload.contacts) ? payload.contacts : [],
  };
}

function persistEmployeeDirectory(directory) {
  const nextDirectory = {
    importedAt: directory.importedAt || new Date().toISOString(),
    sourceName: directory.sourceName || "",
    contacts: Array.isArray(directory.contacts) ? directory.contacts : [],
  };
  writeJson(EMPLOYEE_DIRECTORY_FILE, nextDirectory);
  return nextDirectory;
}

function buildKnownContactsDirectory(areas = []) {
  const contactsByEmail = new Map();
  const contactsByName = new Map();
  const contacts = [];

  const pushContact = (name, email) => {
    const normalizedName = String(name || "").trim();
    const normalizedEmail = normalizeEmail(email);
    if (!normalizedName && !normalizedEmail) {
      return;
    }

    const contact = {
      name: normalizedName,
      email: normalizedEmail,
    };

    if (normalizedEmail && !contactsByEmail.has(normalizedEmail)) {
      contactsByEmail.set(normalizedEmail, contact);
    }

    if (normalizedName) {
      const key = normalizedName.toLowerCase();
      if (!contactsByName.has(key)) {
        contactsByName.set(key, contact);
      }
    }

    contacts.push(contact);
  };

  getEmployeeDirectory().contacts.forEach((contact) => pushContact(contact.name, contact.email));
  areas.forEach((area) => pushContact(area.owner, area.email));

  return {
    contacts,
    byEmail: contactsByEmail,
    byName: contactsByName,
  };
}

function resolveContactByValue(value, contactsDirectory) {
  const normalizedValue = String(value || "").trim();
  if (!normalizedValue) {
    return null;
  }

  const normalizedEmail = normalizeEmail(normalizedValue);
  if (contactsDirectory.byEmail.has(normalizedEmail)) {
    return contactsDirectory.byEmail.get(normalizedEmail);
  }

  const exactNameMatch = contactsDirectory.byName.get(normalizedValue.toLowerCase());
  if (exactNameMatch) {
    return exactNameMatch;
  }

  const normalizedSearch = normalizeContactName(normalizedValue);
  if (!normalizedSearch) {
    return null;
  }

  const rankedMatches = (contactsDirectory.contacts || [])
    .map((contact) => {
      const normalizedContactName = normalizeContactName(contact.name);
      if (!normalizedContactName) {
        return null;
      }

      if (normalizedContactName === normalizedSearch) {
        return { score: 4, contact };
      }
      if (normalizedContactName.startsWith(`${normalizedSearch} `) || normalizedContactName.endsWith(` ${normalizedSearch}`)) {
        return { score: 3, contact };
      }
      if (normalizedContactName.includes(` ${normalizedSearch} `) || normalizedContactName.startsWith(normalizedSearch) || normalizedContactName.endsWith(normalizedSearch)) {
        return { score: 2, contact };
      }
      if (normalizedSearch.length >= 4 && normalizedContactName.includes(normalizedSearch)) {
        return { score: 1, contact };
      }
      return null;
    })
    .filter(Boolean)
    .sort((left, right) => right.score - left.score);

  if (!rankedMatches.length) {
    return null;
  }

  if (rankedMatches.length === 1 || rankedMatches[0].score > rankedMatches[1].score) {
    return rankedMatches[0].contact;
  }

  return null;
}

function clearEmployeeDirectory() {
  deleteIfExists(EMPLOYEE_DIRECTORY_FILE);
  return getEmployeeDirectory();
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
    spreadsheetId: SOURCE_SPREADSHEET_ID,
    spreadsheetTitle: "",
    sheetName: SOURCE_SHEET_NAME,
    importedAt: "",
    headers: [],
    rows: [],
  });
}

function loadLiveSyncConfig() {
  const fileConfig = readOptionalJson(LIVE_SYNC_CONFIG_FILE, {});
  return {
    enabled: process.env.LIVE_SYNC_ENABLED === "true" ? true : Boolean(fileConfig.enabled),
    sourceType: normalizeLiveSyncSourceType(process.env.LIVE_SYNC_SOURCE_TYPE || fileConfig.sourceType || "csv"),
    sourceUrl: process.env.LIVE_SYNC_SOURCE_URL || fileConfig.sourceUrl || "",
    intervalMs: Math.max(30000, Number(process.env.LIVE_SYNC_INTERVAL_MS || fileConfig.intervalMs) || 300000),
  };
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

function loadGoogleSheetsOAuthConfig() {
  return {
    mode: GOOGLE_SHEETS_AUTH_MODE || "",
    clientId: GOOGLE_OAUTH_CLIENT_ID,
    clientSecret: GOOGLE_OAUTH_CLIENT_SECRET,
    redirectUri: GOOGLE_OAUTH_REDIRECT_URI,
  };
}

function loadUserGoogleOAuthToken() {
  return readOptionalJson(USER_GOOGLE_OAUTH_TOKEN_FILE, null);
}

function persistUserGoogleOAuthToken(tokens) {
  writeJson(USER_GOOGLE_OAUTH_TOKEN_FILE, tokens);
}

function isUserGoogleOAuthConfigured() {
  const config = loadGoogleSheetsOAuthConfig();
  return Boolean(config.clientId && config.clientSecret && config.redirectUri);
}

function isUserGoogleOAuthConnected() {
  const token = loadUserGoogleOAuthToken();
  return Boolean(token?.refresh_token || token?.access_token);
}

function getGoogleSheetsStorageMode() {
  const explicitMode = String(GOOGLE_SHEETS_AUTH_MODE || "").trim().toLowerCase();
  if (explicitMode === "snapshot" || explicitMode === "none" || explicitMode === "local") {
    return "snapshot";
  }
  if (explicitMode === "user-oauth") {
    return isUserGoogleOAuthConnected() ? "user-oauth" : "snapshot";
  }
  if (explicitMode === "service-account") {
    return loadServiceAccountCredentials() ? "service-account" : "snapshot";
  }
  if (loadServiceAccountCredentials()) {
    return "service-account";
  }
  if (isUserGoogleOAuthConnected()) {
    return "user-oauth";
  }
  return "snapshot";
}

function createUserGoogleOAuthClient() {
  const config = loadGoogleSheetsOAuthConfig();
  if (!config.clientId || !config.clientSecret || !config.redirectUri) {
    throw new Error("Google OAuth de usuario no configurado");
  }

  const client = new OAuth2Client(config.clientId, config.clientSecret, config.redirectUri);
  const tokens = loadUserGoogleOAuthToken();
  if (tokens) {
    client.setCredentials(tokens);
  }
  client.on("tokens", (nextTokens) => {
    const mergedTokens = {
      ...(loadUserGoogleOAuthToken() || {}),
      ...nextTokens,
    };
    persistUserGoogleOAuthToken(mergedTokens);
  });
  return client;
}

function persistLiveSyncConfig(config) {
  writeJson(LIVE_SYNC_CONFIG_FILE, config);
  return config;
}

function normalizeLiveSyncSourceType(value) {
  return value === "json" || value === "google-sheets" ? value : "csv";
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

  if (pathname === "/api/google-sheets-auth/callback") {
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

function getGoogleSheetsAuthStatus() {
  const oauthConfig = loadGoogleSheetsOAuthConfig();
  return {
    mode: getGoogleSheetsStorageMode(),
    configuredMode: oauthConfig.mode || "auto",
    userOAuthConfigured: isUserGoogleOAuthConfigured(),
    userOAuthConnected: isUserGoogleOAuthConnected(),
    userOAuthRedirectUri: oauthConfig.redirectUri || "",
  };
}

function createGoogleSheetsOAuthState(request) {
  const user = getCurrentUser(request);
  const payload = {
    nonce: crypto.randomBytes(12).toString("hex"),
    exp: Date.now() + (10 * 60 * 1000),
    userEmail: user?.email || "",
  };
  return encodeSessionCookie(payload);
}

function decodeGoogleSheetsOAuthState(value) {
  return decodeSessionCookie(value);
}

function redirectToApp(response, status, message = "") {
  const location = `${withBasePath("/")}${message ? `?googleSheetsAuth=${encodeURIComponent(status)}&message=${encodeURIComponent(message)}` : `?googleSheetsAuth=${encodeURIComponent(status)}`}`;
  response.writeHead(302, { Location: location });
  response.end();
}

function normalizePreSessionDecision(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "impact" || normalized === "impacts" || normalized === "impacta" || normalized === "yes") {
    return "impact";
  }
  if (normalized === "no-impact" || normalized === "no impact" || normalized === "no-impacta" || normalized === "no") {
    return "no-impact";
  }
  return "";
}

function buildExistingPreSessionCheckMap(state) {
  return new Map(
    (state?.openItems || []).map((item) => [item.id, Array.isArray(item.preSessionChecks) ? item.preSessionChecks : []]),
  );
}

function mapSnapshotToState(snapshot, existingState = null, areasOverride = null, votesOverride = null, preSessionOverride = null) {
  const areas = resolveAreas(areasOverride, existingState);
  const areaColumns = areas.filter((area) => area.sourceColumn);
  const previousVotes = votesOverride instanceof Map
    ? votesOverride
    : new Map((existingState?.openItems || []).map((item) => [item.id, item.votes || []]));
  const previousPreSessionChecks = preSessionOverride instanceof Map
    ? preSessionOverride
    : buildExistingPreSessionCheckMap(existingState);
  const sourceType = snapshot.sourceType || existingState?.source?.type || "google-sheet-snapshot";

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
        preSessionChecks: previousPreSessionChecks.get(row.id) || [],
      };
    });

  return {
    areas,
    openItems,
    source: {
      type: sourceType,
      spreadsheetId: snapshot.spreadsheetId || "",
      spreadsheetTitle: snapshot.spreadsheetTitle || "",
      sheetName: snapshot.sheetName || SOURCE_SHEET_NAME,
      sheetHeaders: Array.isArray(snapshot.headers) ? snapshot.headers : OPEN_ITEM_HEADERS,
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

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

function normalizeContactName(value) {
  return String(value || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function isPreSessionCandidate(item) {
  if (!item || item.status === "closed") {
    return false;
  }

  const normalizedStatus = String(item.externalStatus || "").trim().toUpperCase();
  return normalizedStatus === "NEW";
}

function getPreSessionCheck(item, areaId) {
  return (item.preSessionChecks || []).find((check) => check.areaId === areaId) || null;
}

function upsertPreSessionCheck(item, areaId, patch) {
  if (!Array.isArray(item.preSessionChecks)) {
    item.preSessionChecks = [];
  }

  let check = item.preSessionChecks.find((candidate) => candidate.areaId === areaId);
  if (!check) {
    check = {
      areaId,
      decision: "",
      comment: "",
      requestedAt: "",
      lastNotifiedAt: "",
      respondedAt: "",
      responderEmail: "",
    };
    item.preSessionChecks.push(check);
  }

  Object.assign(check, patch);
  check.decision = normalizePreSessionDecision(check.decision);
  check.comment = String(check.comment || "").trim();
  check.requestedAt = String(check.requestedAt || "").trim();
  check.lastNotifiedAt = String(check.lastNotifiedAt || "").trim();
  check.respondedAt = String(check.respondedAt || "").trim();
  check.responderEmail = normalizeEmail(check.responderEmail);
  return check;
}

function applyPreSessionImpactDecision(item, areaId, decision) {
  const impactedAreaIds = new Set(Array.isArray(item.impactedAreaIds) ? item.impactedAreaIds : []);
  if (decision === "impact") {
    impactedAreaIds.add(areaId);
  }
  if (decision === "no-impact") {
    impactedAreaIds.delete(areaId);
  }
  item.impactedAreaIds = Array.from(impactedAreaIds);

  if (!item.ownerAreaId || !item.impactedAreaIds.includes(item.ownerAreaId)) {
    item.ownerAreaId = item.impactedAreaIds[0] || "";
  }
}

function getOwnedAreasForUser(state, user) {
  const userEmail = normalizeEmail(user?.email);
  if (!userEmail) {
    return [];
  }

  return state.areas.filter((area) => normalizeEmail(area.email) === userEmail);
}

function getPreSessionTargetAreaIds(state, item) {
  if (!item || item.status === "closed") {
    return [];
  }

  return (state.areas || []).map((area) => area.id).filter(Boolean);
}

function buildPreSessionOwnerView(state, user) {
  const ownedAreas = getOwnedAreasForUser(state, user);
  const areaMap = new Map(ownedAreas.map((area) => [area.id, area]));
  const pendingItems = [];
  const answeredItems = [];

  state.openItems
    .filter((item) => isPreSessionCandidate(item))
    .forEach((item) => {
      const targetAreaIds = new Set(getPreSessionTargetAreaIds(state, item));
      const relevantAreaIds = new Set([
        ...Array.from(targetAreaIds),
        ...((item.preSessionChecks || []).map((check) => check.areaId)),
      ]);

      Array.from(relevantAreaIds).forEach((areaId) => {
        const area = areaMap.get(areaId);
        if (!area) {
          return;
        }

        const check = getPreSessionCheck(item, areaId);
        const entry = {
          openItemId: item.id,
          title: item.title,
          description: item.description || "",
          itemStatus: item.status || "open",
          externalStatus: item.externalStatus || "",
          isSubstantial: Boolean(item.isSubstantial),
          ownerAreaId: item.ownerAreaId || "",
          areaId,
          areaName: area.name,
          requestedAt: check?.requestedAt || "",
          lastNotifiedAt: check?.lastNotifiedAt || "",
          respondedAt: check?.respondedAt || "",
          decision: check?.decision || "",
          comment: check?.comment || "",
          sourceRef: item.sourceRef || "",
          currentlyImpacted: Array.isArray(item.impactedAreaIds) && item.impactedAreaIds.includes(areaId),
        };

        if (entry.decision) {
          answeredItems.push(entry);
        } else if (targetAreaIds.has(areaId)) {
          pendingItems.push(entry);
        }
      });
    });

  const sortEntries = (left, right) =>
    Number(Boolean(right.externalStatus)) - Number(Boolean(left.externalStatus)) ||
    Number(right.isSubstantial) - Number(left.isSubstantial) ||
    left.openItemId.localeCompare(right.openItemId);

  pendingItems.sort(sortEntries);
  answeredItems.sort((left, right) => (right.respondedAt || "").localeCompare(left.respondedAt || "") || sortEntries(left, right));

  return {
    ownedAreas: ownedAreas.map((area) => ({
      id: area.id,
      name: area.name,
      email: area.email,
    })),
    pendingItems,
    answeredItems,
  };
}

function buildPreSessionOwnerQueue(state) {
  const areaMap = buildAreaMap(state);
  const groups = new Map();

  state.openItems
    .filter((item) => isPreSessionCandidate(item))
    .forEach((item) => {
      const targetAreaIds = getPreSessionTargetAreaIds(state, item);
      targetAreaIds.forEach((areaId) => {
        const area = areaMap.get(areaId);
        const ownerEmail = normalizeEmail(area?.email);
        if (!area || !ownerEmail) {
          return;
        }

        const check = getPreSessionCheck(item, areaId);
        if (check?.decision) {
          return;
        }

        if (!groups.has(ownerEmail)) {
          groups.set(ownerEmail, {
            ownerEmail,
            ownerName: area.owner || area.name,
            areaNames: new Set(),
            pendingItems: [],
            lastNotifiedAt: "",
          });
        }

        const group = groups.get(ownerEmail);
        group.areaNames.add(area.name);
        group.lastNotifiedAt = [group.lastNotifiedAt, check?.lastNotifiedAt || ""].sort().reverse()[0] || group.lastNotifiedAt;
        group.pendingItems.push({
          openItemId: item.id,
          title: item.title,
          areaId,
          areaName: area.name,
          itemStatus: item.status || "open",
          externalStatus: item.externalStatus || "",
          requestedAt: check?.requestedAt || "",
          lastNotifiedAt: check?.lastNotifiedAt || "",
          isSubstantial: Boolean(item.isSubstantial),
        });
      });
    });

  return Array.from(groups.values())
    .map((group) => ({
      ownerEmail: group.ownerEmail,
      ownerName: group.ownerName,
      areaNames: Array.from(group.areaNames).sort(),
      pendingCount: group.pendingItems.length,
      lastNotifiedAt: group.lastNotifiedAt || "",
      pendingItems: group.pendingItems.sort((left, right) => left.openItemId.localeCompare(right.openItemId)),
    }))
    .sort((left, right) => right.pendingCount - left.pendingCount || left.ownerEmail.localeCompare(right.ownerEmail));
}

function buildPreSessionOpenItemQueue(state) {
  const areaMap = buildAreaMap(state);

  return state.openItems
    .filter((item) => isPreSessionCandidate(item))
    .map((item) => {
      const pendingOwners = getPreSessionTargetAreaIds(state, item)
        .map((areaId) => {
          const area = areaMap.get(areaId);
          const check = getPreSessionCheck(item, areaId);
          if (!area || check?.decision) {
            return null;
          }

          return {
            areaId,
            areaName: area.name,
            ownerName: area.owner || area.name,
            ownerEmail: area.email || "",
          };
        })
        .filter(Boolean)
        .sort((left, right) => left.areaName.localeCompare(right.areaName));

      return {
        openItemId: item.id,
        title: item.title,
        itemStatus: item.status || "open",
        externalStatus: item.externalStatus || "",
        pendingOwners,
      };
    })
    .filter((item) => item.pendingOwners.length > 0)
    .sort((left, right) => left.openItemId.localeCompare(right.openItemId));
}

function buildPreSessionDashboard(state, user) {
  return {
    ownerView: buildPreSessionOwnerView(state, user),
    ownerQueue: buildPreSessionOwnerQueue(state),
    openItemQueue: buildPreSessionOpenItemQueue(state),
    gmailDeliveryAvailable: getGoogleSheetsStorageMode() === "user-oauth" && isUserGoogleOAuthConnected(),
  };
}

function buildPreSessionEmailSubject(group) {
  return `CCB pre-sesion: revision pendiente para ${group.pendingCount} open item${group.pendingCount === 1 ? "" : "s"}`;
}

function buildPreSessionEmailBody(group) {
  const appUrl = loadAuthConfig().appBaseUrl || withBasePath("/");
  const lines = [
    `Hola ${group.ownerName || group.ownerEmail},`,
    "",
    "Necesitamos tu revision de pre-sesion para los siguientes open items donde tu area aparece impactada.",
    "Por favor entra a la app y marca si impacta o no impacta para tu area.",
    "",
    ...group.pendingItems.map((item) => `- ${item.openItemId} | ${item.title} | Area: ${item.areaName}${item.externalStatus ? ` | Estado: ${item.externalStatus}` : ""}`),
    "",
    `App: ${appUrl}`,
  ];

  return lines.join("\n");
}

function toBase64Url(value) {
  return Buffer.from(value, "utf8")
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

async function sendGmailMessage({ to, subject, text }) {
  if (getGoogleSheetsStorageMode() !== "user-oauth") {
    throw new Error("El envio de correo requiere Google OAuth de usuario conectado.");
  }

  const url = "https://gmail.googleapis.com/gmail/v1/users/me/messages/send";
  const headers = await getGoogleRequestHeaders(url);
  const rawMessage = [
    "Content-Type: text/plain; charset=UTF-8",
    "MIME-Version: 1.0",
    `To: ${to}`,
    `Subject: ${subject}`,
    "",
    text,
  ].join("\r\n");

  const response = await fetch(url, {
    method: "POST",
    headers: {
      ...headers,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      raw: toBase64Url(rawMessage),
    }),
  });

  if (!response.ok) {
    const detail = await response.text().catch(() => "");
    throw new Error(`Gmail send error ${response.status}${detail ? `: ${detail}` : ""}`);
  }

  return response.json();
}

async function runPreSessionRequestJob(user) {
  const state = await loadStateStore();
  const queue = buildPreSessionOwnerQueue(state);
  const now = new Date().toISOString();
  const results = [];

  for (const group of queue) {
    group.pendingItems.forEach((entry) => {
      const item = state.openItems.find((candidate) => candidate.id === entry.openItemId);
      if (!item) {
        return;
      }

      const existing = getPreSessionCheck(item, entry.areaId);
      upsertPreSessionCheck(item, entry.areaId, {
        requestedAt: existing?.requestedAt || now,
      });
    });
  }

  for (const group of queue) {
    const subject = buildPreSessionEmailSubject(group);
    const body = buildPreSessionEmailBody(group);
    let deliveryMode = "preview";
    let sent = false;
    let errorMessage = "";

    try {
      if (getGoogleSheetsStorageMode() === "user-oauth" && isUserGoogleOAuthConnected()) {
        await sendGmailMessage({
          to: group.ownerEmail,
          subject,
          text: body,
        });
        deliveryMode = "gmail";
        sent = true;
      }
    } catch (error) {
      errorMessage = error.message;
    }

    if (sent) {
      group.pendingItems.forEach((entry) => {
        const item = state.openItems.find((candidate) => candidate.id === entry.openItemId);
        if (!item) {
          return;
        }

        const existing = getPreSessionCheck(item, entry.areaId);
        upsertPreSessionCheck(item, entry.areaId, {
          requestedAt: existing?.requestedAt || now,
          lastNotifiedAt: now,
        });
      });
    }

    results.push({
      ownerEmail: group.ownerEmail,
      ownerName: group.ownerName,
      pendingCount: group.pendingCount,
      deliveryMode,
      sent,
      error: errorMessage,
      subject,
      body,
    });
  }

  const nextState = await persistStateStore(state);
  return {
    ranAt: now,
    ranBy: user?.email || "",
    notifications: results,
    state: nextState,
    dashboard: buildPreSessionDashboard(nextState, user),
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
  const buildSnapshotRow = (row) => {
    const record = Object.fromEntries(
      headerRow.map((header) => [header, getValue(row, header)]),
    );

    return {
      ...record,
      id: getValue(row, "ID"),
      description: getValue(row, "Open Item Description"),
      owner: getValue(row, "Owner"),
      dateCreated: getValue(row, "Date Created"),
      dueDate: getValue(row, "Due Date"),
      status: getValue(row, "Status"),
      comments: getValue(row, "Comments/Updates"),
      minutesRelated: getValue(row, "Minutes Related"),
      gitRepository: getValue(row, "Git Repository"),
      ccbScore: getValue(row, "CCB Score"),
      ccbStatus: getValue(row, "CCB Status"),
      jiraTicketsRelated: getValue(row, "Jira Tickets Related"),
    };
  };

  const snapshot = {
    spreadsheetId: SOURCE_SPREADSHEET_ID,
    spreadsheetTitle: spreadsheetTitle || "",
    sheetName: SOURCE_SHEET_NAME,
    importedAt: new Date().toISOString(),
    headers: headerRow,
    rows: bodyRows.map(buildSnapshotRow),
  };

  writeJson(GOOGLE_SNAPSHOT_FILE, snapshot);
  return snapshot;
}

function toDisplayName(value) {
  const normalized = String(value || "").trim();
  if (!normalized) {
    return "";
  }

  if (normalized.includes("...")) {
    return normalized;
  }

  return normalized
    .split(/[._-]+/)
    .filter(Boolean)
    .map((chunk) => chunk.charAt(0).toUpperCase() + chunk.slice(1))
    .join(" ");
}

function findEmployeeEmailColumnIndex(headerRow) {
  const normalizedHeaders = headerRow.map((header) => String(header || "").trim().toLowerCase());
  const preferredHeaders = ["email", "email address", "primary email", "correo", "correo electronico"];

  for (const headerName of preferredHeaders) {
    const index = normalizedHeaders.findIndex((header) => header === headerName);
    if (index >= 0) {
      return index;
    }
  }

  return normalizedHeaders.findIndex((header) =>
    header.includes("email") &&
      header !== "email status" &&
      header !== "email preference",
  );
}

function isEmployeeDirectoryHeaderRow(row) {
  const normalizedHeaders = row.map((header) => String(header || "").trim().toLowerCase());
  return normalizedHeaders.includes("email address") || normalizedHeaders.includes("nickname");
}

function buildEmployeeDirectoryFromCsv(csvText, sourceName = "Imported employee CSV") {
  const rows = parseCsv(csvText);
  if (rows.length < 2) {
    throw new Error("CSV de empleados vacio o incompleto");
  }

  const headerIndex = rows.findIndex((row) => isEmployeeDirectoryHeaderRow(row));

  if (headerIndex === -1) {
    throw new Error("No encontre la fila de encabezados del CSV de empleados.");
  }

  const headerRow = rows[headerIndex];
  const bodyRows = rows.slice(headerIndex + 1);
  const emailIndex = findEmployeeEmailColumnIndex(headerRow);
  if (emailIndex === -1) {
    throw new Error("No encontre una columna de correo real en el CSV. Debe existir una columna como Email o Email Address.");
  }

  const contacts = [];
  const seen = new Set();

  bodyRows.forEach((row) => {
    const email = String(row[emailIndex] || "").trim().toLowerCase();
    if (!email) {
      return;
    }

    if (seen.has(email)) {
      return;
    }

    seen.add(email);
    contacts.push({
      name: toDisplayName(email.split("@")[0] || email),
      email,
    });
  });

  if (!contacts.length) {
    throw new Error("El CSV no contiene correos utilizables en la columna seleccionada.");
  }

  return persistEmployeeDirectory({
    importedAt: new Date().toISOString(),
    sourceName,
    contacts,
  });
}

function buildSnapshotFromJson(payload) {
  if (!payload || !Array.isArray(payload.rows)) {
    throw new Error("JSON invalido para live sync");
  }

  const snapshot = {
    spreadsheetId: payload.spreadsheetId || SOURCE_SPREADSHEET_ID,
    spreadsheetTitle: payload.spreadsheetTitle || "",
    sheetName: payload.sheetName || SOURCE_SHEET_NAME,
    importedAt: new Date().toISOString(),
    headers: Array.isArray(payload.headers) ? payload.headers : [],
    rows: payload.rows,
  };

  writeJson(GOOGLE_SNAPSHOT_FILE, snapshot);
  return snapshot;
}

async function importSnapshotIntoState(snapshot) {
  const currentState = await loadStateStore();
  const nextState = await persistStateStore(mapSnapshotToState(snapshot, currentState));
  return nextState;
}

function isGoogleSheetsStorageEnabled() {
  return getGoogleSheetsStorageMode() !== "snapshot";
}

function chunkRows(rows, size = 500) {
  const chunks = [];
  for (let index = 0; index < rows.length; index += size) {
    chunks.push(rows.slice(index, index + size));
  }
  return chunks;
}

function toA1Column(columnIndex) {
  let index = columnIndex + 1;
  let label = "";
  while (index > 0) {
    const remainder = (index - 1) % 26;
    label = String.fromCharCode(65 + remainder) + label;
    index = Math.floor((index - 1) / 26);
  }
  return label;
}

function buildSheetValuesMap(values) {
  if (!Array.isArray(values) || !values.length) {
    return [];
  }

  const [headers, ...rows] = values;
  const normalizedHeaders = headers.map((header) => String(header || "").trim());
  return rows.map((row) => {
    const record = {};
    normalizedHeaders.forEach((header, index) => {
      record[header] = row[index] || "";
    });
    return record;
  });
}

function normalizeSheetRangeName(rangeValue) {
  return String(rangeValue || "")
    .split("!")[0]
    .replace(/^'/, "")
    .replace(/'$/, "");
}

function findOpenItemHeaderRowIndex(values) {
  const hasRequiredHeaders = (row) => {
    const normalized = row.map((cell) => String(cell || "").trim());
    return normalized.includes("ID") && normalized.includes("Open Item Description");
  };

  if (SOURCE_SHEET_HEADER_ROW > 0) {
    const configuredIndex = Math.max(0, SOURCE_SHEET_HEADER_ROW - 1);
    if (values[configuredIndex] && hasRequiredHeaders(values[configuredIndex])) {
      return configuredIndex;
    }
  }

  return values.findIndex((row) => hasRequiredHeaders(row));
}

function getOpenItemSheetContext(values) {
  const headerRowIndex = findOpenItemHeaderRowIndex(values);
  if (headerRowIndex < 0 || !values[headerRowIndex]) {
    throw new Error(`No se pudo detectar la fila de encabezados de ${SOURCE_SHEET_NAME}. Restaura la hoja para que vuelva a contener una fila con 'ID' y 'Open Item Description'.`);
  }

  const headerRow = values[headerRowIndex].map((cell) => String(cell || "").trim());
  return {
    headerRowIndex,
    headerRowNumber: headerRowIndex + 1,
    headerRow,
    bodyRows: values.slice(headerRowIndex + 1),
  };
}

function findDecisionHeaderRowIndex(values) {
  return values.findIndex((row) => {
    const normalized = row.map((cell) => String(cell || "").trim());
    return normalized.includes("OI_ID") && normalized.includes("Open Item Description");
  });
}

function getDecisionSheetContext(values) {
  const headerRowIndex = findDecisionHeaderRowIndex(values);
  if (headerRowIndex < 0 || !values[headerRowIndex]) {
    throw new Error(`No se pudo detectar la tabla de ${CCB_DECISION_SHEET_NAME}. Debe existir una fila con 'OI_ID' y 'Open Item Description'.`);
  }

  const headerRow = values[headerRowIndex].map((cell) => String(cell || "").trim());
  return {
    headerRowIndex,
    headerRowNumber: headerRowIndex + 1,
    headerRow,
    bodyRows: values.slice(headerRowIndex + 1),
  };
}

function formatMexicoCityDateTime(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }

  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) {
    return raw;
  }

  const formatter = new Intl.DateTimeFormat("sv-SE", {
    timeZone: "America/Mexico_City",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });

  const parts = Object.fromEntries(
    formatter
      .formatToParts(parsed)
      .filter((part) => part.type !== "literal")
      .map((part) => [part.type, part.value]),
  );

  return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}`;
}

function buildOpenItemHeaders(state, areas) {
  const headerSet = new Set(
    Array.isArray(state.source?.sheetHeaders) && state.source.sheetHeaders.length
      ? state.source.sheetHeaders
      : OPEN_ITEM_HEADERS,
  );

  OPEN_ITEM_HEADERS.forEach((header) => headerSet.add(header));
  areas
    .filter((area) => area.sourceColumn)
    .forEach((area) => headerSet.add(area.sourceColumn));

  return Array.from(headerSet);
}

function buildManagedOpenItemHeaders(areas) {
  const managedHeaders = new Set([
    "ID",
    "Open Item Description",
    "Owner",
    "Date Created",
    "Due Date",
    "Status",
    "Comments/Updates",
  ]);

  areas
    .filter((area) => area.sourceColumn)
    .forEach((area) => managedHeaders.add(area.sourceColumn));

  return managedHeaders;
}

function buildManagedDecisionHeaders() {
  return new Set([
    "OI_ID",
    "Open Item Description",
  ]);
}

function parseAreasSheetRows(values) {
  const records = buildSheetValuesMap(values);
  return records
    .filter((record) => String(record.id || "").trim() && String(record.name || "").trim())
    .map((record) => ({
      id: String(record.id || "").trim(),
      name: String(record.name || "").trim(),
      owner: String(record.owner || "").trim(),
      email: String(record.email || "").trim(),
      sourceColumn: String(record.sourceColumn || "").trim(),
    }));
}

function parseVotesSheetRows(values) {
  const records = buildSheetValuesMap(values);
  const voteMap = new Map();

  records.forEach((record) => {
    const openItemId = String(record.openItemId || "").trim();
    const areaId = String(record.areaId || "").trim();
    if (!openItemId || !areaId) {
      return;
    }

    if (!voteMap.has(openItemId)) {
      voteMap.set(openItemId, []);
    }

    voteMap.get(openItemId).push({
      areaId,
      decision: String(record.decision || "").trim() || "needs-info",
      comment: String(record.comment || "").trim(),
      createdAt: String(record.createdAt || "").trim() || new Date().toISOString(),
    });
  });

  return voteMap;
}

function parsePreSessionSheetRows(values) {
  const records = buildSheetValuesMap(values);
  const checksMap = new Map();

  records.forEach((record) => {
    const openItemId = String(record.openItemId || "").trim();
    const areaId = String(record.areaId || "").trim();
    if (!openItemId || !areaId) {
      return;
    }

    if (!checksMap.has(openItemId)) {
      checksMap.set(openItemId, []);
    }

    checksMap.get(openItemId).push({
      areaId,
      decision: normalizePreSessionDecision(record.decision),
      comment: String(record.comment || "").trim(),
      requestedAt: String(record.requestedAt || "").trim(),
      lastNotifiedAt: String(record.lastNotifiedAt || "").trim(),
      respondedAt: String(record.respondedAt || "").trim(),
      responderEmail: String(record.responderEmail || "").trim().toLowerCase(),
    });
  });

  return checksMap;
}

function buildOpenItemsRowsFromState(state, areas) {
  const areaColumns = areas.filter((area) => area.sourceColumn);
  const headers = buildOpenItemHeaders(state, areas);
  const checkboxHeaders = new Set(areaColumns.map((area) => area.sourceColumn));

  return state.openItems.map((item) => {
    const baseRow = item.rawSheetRow && typeof item.rawSheetRow === "object"
      ? { ...item.rawSheetRow }
      : {};
    const impactedAreaIds = new Set(Array.isArray(item.impactedAreaIds) ? item.impactedAreaIds : []);
    const preservedOwner = String(baseRow.Owner || baseRow.owner || "").trim();

    baseRow.ID = item.id || "";
    baseRow["Open Item Description"] = item.title || "";
    baseRow.Owner = String(item.ownerName || preservedOwner).trim();
    baseRow["Date Created"] = formatMexicoCityDateTime(item.createdAt || baseRow["Date Created"] || "");
    baseRow["Due Date"] = formatMexicoCityDateTime(item.dueDate || baseRow["Due Date"] || "");
    baseRow.Status = item.status || "open";
    baseRow["Comments/Updates"] = item.description || "";
    baseRow["Minutes Related"] = baseRow["Minutes Related"] || "";
    baseRow["Git Repository"] = baseRow["Git Repository"] || "";
    baseRow["Jira Tickets Related"] = baseRow["Jira Tickets Related"] || "";
    baseRow["CCB Score"] = item.ccbScore || "";
    baseRow["CCB Status"] = item.externalStatus || (item.isSubstantial ? "REVIEW" : "No Localized");

    areaColumns.forEach((area) => {
      const preSessionCheck = getPreSessionCheck(item, area.id);
      if (preSessionCheck?.decision === "impact") {
        baseRow[area.sourceColumn] = true;
        return;
      }
      if (preSessionCheck?.decision === "no-impact") {
        baseRow[area.sourceColumn] = false;
        return;
      }
      if (isPreSessionCandidate(item)) {
        baseRow[area.sourceColumn] = "";
        return;
      }

      baseRow[area.sourceColumn] = impactedAreaIds.has(area.id);
    });

    return headers.map((header) => {
      if (checkboxHeaders.has(header)) {
        if (baseRow[header] === "") {
          return "";
        }
        return Boolean(baseRow[header]);
      }
      return String(baseRow[header] || "").trim();
    });
  });
}

function buildAreasSheetRows(areas) {
  return [
    AREA_HEADERS,
    ...areas.map((area) => AREA_HEADERS.map((header) => String(area[header] || "").trim())),
  ];
}

function buildVotesSheetRows(state) {
  const rows = [];
  state.openItems.forEach((item) => {
    (item.votes || []).forEach((vote) => {
      rows.push([
        item.id || "",
        vote.areaId || "",
        vote.decision || "",
        vote.comment || "",
        vote.createdAt || "",
      ]);
    });
  });

  return [VOTE_HEADERS, ...rows];
}

function buildPreSessionSheetRows(state) {
  const rows = [];
  state.openItems.forEach((item) => {
    (item.preSessionChecks || []).forEach((check) => {
      rows.push([
        item.id || "",
        check.areaId || "",
        normalizePreSessionDecision(check.decision),
        check.comment || "",
        check.requestedAt || "",
        check.lastNotifiedAt || "",
        check.respondedAt || "",
        check.responderEmail || "",
      ]);
    });
  });

  return [PRESESSION_HEADERS, ...rows];
}

function parseDecisionSheetRows(values) {
  const context = getDecisionSheetContext(values);
  const records = buildSheetValuesMap([context.headerRow, ...context.bodyRows]);
  const rowsByOpenItemId = new Map();

  records.forEach((record) => {
    const openItemId = String(record.OI_ID || "").trim();
    if (!openItemId) {
      return;
    }
    rowsByOpenItemId.set(openItemId, record);
  });

  return {
    context,
    rowsByOpenItemId,
  };
}

function buildDecisionSheetRows(state, existingRowsByOpenItemId = new Map()) {
  const rows = state.openItems.map((item) => {
    const existingRecord = existingRowsByOpenItemId.get(item.id) || {};
    const baseRecord = {};

    CCB_DECISION_HEADERS.forEach((header) => {
      baseRecord[header] = String(existingRecord[header] || "").trim();
    });

    baseRecord.OI_ID = item.id || "";
    baseRecord["Open Item Description"] = item.title || "";
    if (!baseRecord.Status && item.externalStatus) {
      baseRecord.Status = item.externalStatus;
    }
    if (!baseRecord.Total && item.ccbScore) {
      baseRecord.Total = item.ccbScore;
    }

    return CCB_DECISION_HEADERS.map((header) => baseRecord[header] || "");
  });

  return [CCB_DECISION_HEADERS, ...rows];
}

function loadServiceAccountCredentials() {
  if (GOOGLE_SERVICE_ACCOUNT_JSON) {
    return JSON.parse(GOOGLE_SERVICE_ACCOUNT_JSON);
  }

  if (GOOGLE_SERVICE_ACCOUNT_KEY_PATH) {
    return readJson(path.resolve(GOOGLE_SERVICE_ACCOUNT_KEY_PATH));
  }

  return null;
}

function getGoogleSheetsJwtClient() {
  const storageMode = getGoogleSheetsStorageMode();
  if (storageMode === "user-oauth") {
    return createUserGoogleOAuthClient();
  }

  const credentials = loadServiceAccountCredentials();
  if (!credentials?.client_email || !credentials?.private_key) {
    throw new Error("Google Sheets API no configurada");
  }

  return new JWT({
    email: credentials.client_email,
    key: credentials.private_key,
    scopes: GOOGLE_SHEETS_SCOPES,
  });
}

async function getGoogleRequestHeaders(url) {
  const client = getGoogleSheetsJwtClient();
  if (getGoogleSheetsStorageMode() === "user-oauth") {
    const accessTokenResponse = await client.getAccessToken();
    const accessToken = typeof accessTokenResponse === "string"
      ? accessTokenResponse
      : accessTokenResponse?.token;
    if (!accessToken) {
      throw new Error("No se pudo obtener access token de Google OAuth.");
    }
    return {
      Authorization: `Bearer ${accessToken}`,
    };
  }

  return client.getRequestHeaders(url);
}

async function fetchGoogleSheetsJson(url) {
  const headers = await getGoogleRequestHeaders(url);
  const response = await fetch(url, { headers });

  if (!response.ok) {
    const detail = await response.text().catch(() => "");
    throw new Error(`Google Sheets API error ${response.status}${detail ? `: ${detail}` : ""}`);
  }

  return response.json();
}

async function fetchSnapshotFromGoogleSheetsApi() {
  const spreadsheetId = SOURCE_SPREADSHEET_ID;
  const encodedSheetName = encodeURIComponent(SOURCE_SHEET_NAME);
  const metadataUrl =
    `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}` +
    `?fields=properties(title),sheets(properties(sheetId,title))`;
  const valuesUrl =
    `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}/values/${encodedSheetName}`;

  const [metadata, valuesPayload] = await Promise.all([
    fetchGoogleSheetsJson(metadataUrl),
    fetchGoogleSheetsJson(valuesUrl),
  ]);

  const values = Array.isArray(valuesPayload.values) ? valuesPayload.values : [];
  if (!values.length) {
    throw new Error(`La hoja ${SOURCE_SHEET_NAME} no tiene datos`);
  }

  const context = getOpenItemSheetContext(values);
  const headerRow = context.headerRow;
  const bodyRows = context.bodyRows;
  const columnIndexes = new Map(headerRow.map((header, index) => [header, index]));
  const getValue = (row, header) => row[columnIndexes.get(header)] || "";
  const buildSnapshotRow = (row) => {
    const record = Object.fromEntries(
      headerRow.map((header) => [header, getValue(row, header)]),
    );

    return {
      ...record,
      id: getValue(row, "ID"),
      description: getValue(row, "Open Item Description"),
      owner: getValue(row, "Owner"),
      dateCreated: getValue(row, "Date Created"),
      dueDate: getValue(row, "Due Date"),
      status: getValue(row, "Status"),
      comments: getValue(row, "Comments/Updates"),
      minutesRelated: getValue(row, "Minutes Related"),
      gitRepository: getValue(row, "Git Repository"),
      ccbScore: getValue(row, "CCB Score"),
      ccbStatus: getValue(row, "CCB Status"),
      jiraTicketsRelated: getValue(row, "Jira Tickets Related"),
    };
  };
  const matchedSheet = Array.isArray(metadata.sheets)
    ? metadata.sheets.find((sheet) => sheet.properties?.title === SOURCE_SHEET_NAME)
    : null;

  const snapshot = {
    spreadsheetId,
    spreadsheetTitle: metadata.properties?.title || "",
    sheetName: SOURCE_SHEET_NAME,
    sourceType: "google-sheets",
    sheetGid: matchedSheet?.properties?.sheetId ?? null,
    headerRowNumber: context.headerRowNumber,
    importedAt: new Date().toISOString(),
    headers: headerRow,
    rows: bodyRows.map(buildSnapshotRow),
  };

  writeJson(GOOGLE_SNAPSHOT_FILE, snapshot);
  return snapshot;
}

async function batchGetSpreadsheetValues(ranges, valueRenderOption = "") {
  const spreadsheetId = SOURCE_SPREADSHEET_ID;
  const query = ranges
    .map((range) => `ranges=${encodeURIComponent(range)}`)
    .join("&");
  const renderOptionQuery = valueRenderOption
    ? `&valueRenderOption=${encodeURIComponent(valueRenderOption)}`
    : "";
  const url = `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}/values:batchGet?${query}${renderOptionQuery}`;
  const payload = await fetchGoogleSheetsJson(url);
  const valueRanges = Array.isArray(payload.valueRanges) ? payload.valueRanges : [];
  const results = new Map();

  valueRanges.forEach((entry) => {
    results.set(normalizeSheetRangeName(entry.range), entry.values || []);
  });

  return results;
}

function preserveOpenItemFormulaCells(rows, existingDisplayValues, existingFormulaValues, areas) {
  if (!rows.length) {
    return rows;
  }

  const displayContext = getOpenItemSheetContext(existingDisplayValues);
  const formulaContext = getOpenItemSheetContext(existingFormulaValues);
  const managedHeaders = buildManagedOpenItemHeaders(areas);

  return rows.map((row, rowIndex) => {
    const formulaRow = formulaContext.bodyRows[rowIndex] || [];
    return row.map((cellValue, columnIndex) => {
      const header = displayContext.headerRow[columnIndex] || "";
      const formulaValue = formulaRow[columnIndex];
      if (
        !managedHeaders.has(header) &&
        typeof formulaValue === "string" &&
        formulaValue.trim().startsWith("=")
      ) {
        return formulaValue;
      }
      return cellValue;
    });
  });
}

function preserveDecisionFormulaCells(rows, existingDisplayValues, existingFormulaValues) {
  if (!rows.length) {
    return rows;
  }

  const displayContext = getDecisionSheetContext(existingDisplayValues);
  const formulaContext = getDecisionSheetContext(existingFormulaValues);
  const managedHeaders = buildManagedDecisionHeaders();

  return rows.map((row, rowIndex) => {
    const formulaRow = formulaContext.bodyRows[rowIndex] || [];
    return row.map((cellValue, columnIndex) => {
      const header = displayContext.headerRow[columnIndex] || "";
      const formulaValue = formulaRow[columnIndex];
      if (
        !managedHeaders.has(header) &&
        typeof formulaValue === "string" &&
        formulaValue.trim().startsWith("=")
      ) {
        return formulaValue;
      }
      return cellValue;
    });
  });
}

async function fetchSpreadsheetMetadata() {
  const url =
    `https://sheets.googleapis.com/v4/spreadsheets/${SOURCE_SPREADSHEET_ID}` +
    "?fields=properties(title),sheets(properties(sheetId,title))";
  return fetchGoogleSheetsJson(url);
}

async function ensureSpreadsheetSheets(sheetNames) {
  const metadata = await fetchSpreadsheetMetadata();
  const existingNames = new Set(
    (metadata.sheets || []).map((sheet) => sheet.properties?.title).filter(Boolean),
  );
  const requests = sheetNames
    .filter((sheetName) => !existingNames.has(sheetName))
    .map((sheetName) => ({
      addSheet: {
        properties: {
          title: sheetName,
        },
      },
    }));

  if (!requests.length) {
    return metadata;
  }

  const url = `https://sheets.googleapis.com/v4/spreadsheets/${SOURCE_SPREADSHEET_ID}:batchUpdate`;
  const headers = await getGoogleRequestHeaders(url);
  const response = await fetch(url, {
    method: "POST",
    headers: {
      ...headers,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ requests }),
  });

  if (!response.ok) {
    const detail = await response.text().catch(() => "");
    throw new Error(`Google Sheets batchUpdate error ${response.status}${detail ? `: ${detail}` : ""}`);
  }

  return fetchSpreadsheetMetadata();
}

async function clearAndWriteSheet(sheetName, rows) {
  const clearUrl =
    `https://sheets.googleapis.com/v4/spreadsheets/${SOURCE_SPREADSHEET_ID}` +
    `/values/${encodeURIComponent(sheetName)}:clear`;
  const clearHeaders = await getGoogleRequestHeaders(clearUrl);
  const clearResponse = await fetch(clearUrl, {
    method: "POST",
    headers: {
      ...clearHeaders,
      "Content-Type": "application/json",
    },
    body: "{}",
  });

  if (!clearResponse.ok) {
    const detail = await clearResponse.text().catch(() => "");
    throw new Error(`Google Sheets clear error ${clearResponse.status}${detail ? `: ${detail}` : ""}`);
  }

  if (!rows.length) {
    return;
  }

  const maxColumns = rows.reduce((max, row) => Math.max(max, row.length), 0);
  const targetRange = `${sheetName}!A1:${toA1Column(Math.max(0, maxColumns - 1))}${rows.length}`;
  const updateUrl =
    `https://sheets.googleapis.com/v4/spreadsheets/${SOURCE_SPREADSHEET_ID}` +
    `/values/${encodeURIComponent(targetRange)}?valueInputOption=RAW`;
  const updateHeaders = await getGoogleRequestHeaders(updateUrl);
  const updateResponse = await fetch(updateUrl, {
    method: "PUT",
    headers: {
      ...updateHeaders,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      range: targetRange,
      majorDimension: "ROWS",
      values: rows,
    }),
  });

  if (!updateResponse.ok) {
    const detail = await updateResponse.text().catch(() => "");
    throw new Error(`Google Sheets update error ${updateResponse.status}${detail ? `: ${detail}` : ""}`);
  }
}

async function clearSheetRange(sheetRange) {
  const clearUrl =
    `https://sheets.googleapis.com/v4/spreadsheets/${SOURCE_SPREADSHEET_ID}` +
    `/values/${encodeURIComponent(sheetRange)}:clear`;
  const clearHeaders = await getGoogleRequestHeaders(clearUrl);
  const clearResponse = await fetch(clearUrl, {
    method: "POST",
    headers: {
      ...clearHeaders,
      "Content-Type": "application/json",
    },
    body: "{}",
  });

  if (!clearResponse.ok) {
    const detail = await clearResponse.text().catch(() => "");
    throw new Error(`Google Sheets clear error ${clearResponse.status}${detail ? `: ${detail}` : ""}`);
  }
}

async function batchUpdateSpreadsheet(requests) {
  if (!requests.length) {
    return;
  }

  const url = `https://sheets.googleapis.com/v4/spreadsheets/${SOURCE_SPREADSHEET_ID}:batchUpdate`;
  const headers = await getGoogleRequestHeaders(url);
  const response = await fetch(url, {
    method: "POST",
    headers: {
      ...headers,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ requests }),
  });

  if (!response.ok) {
    const detail = await response.text().catch(() => "");
    throw new Error(`Google Sheets batchUpdate error ${response.status}${detail ? `: ${detail}` : ""}`);
  }
}

async function writeSheetRange(sheetRange, rows, valueInputOption = "RAW") {
  const updateUrl =
    `https://sheets.googleapis.com/v4/spreadsheets/${SOURCE_SPREADSHEET_ID}` +
    `/values/${encodeURIComponent(sheetRange)}?valueInputOption=${encodeURIComponent(valueInputOption)}`;
  const updateHeaders = await getGoogleRequestHeaders(updateUrl);
  const updateResponse = await fetch(updateUrl, {
    method: "PUT",
    headers: {
      ...updateHeaders,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      range: sheetRange,
      majorDimension: "ROWS",
      values: rows,
    }),
  });

  if (!updateResponse.ok) {
    const detail = await updateResponse.text().catch(() => "");
    throw new Error(`Google Sheets update error ${updateResponse.status}${detail ? `: ${detail}` : ""}`);
  }
}

function buildPeopleChipCellValue(contact, displayText = "") {
  const resolvedEmail = normalizeEmail(contact?.email);
  if (!resolvedEmail) {
    return null;
  }

  return {
    userEnteredValue: {
      stringValue: displayText || "@",
    },
    chipRuns: [
      {
        chip: {
          personProperties: {
            email: resolvedEmail,
            displayFormat: "DEFAULT",
          },
        },
      },
    ],
  };
}

async function applyPeopleChipUpdates(sheetId, chipUpdates) {
  const requests = chipUpdates
    .filter((entry) => entry && entry.cellValue)
    .map((entry) => ({
      updateCells: {
        rows: [
          {
            values: [entry.cellValue],
          },
        ],
        fields: "userEnteredValue,chipRuns",
        range: {
          sheetId,
          startRowIndex: entry.rowIndex,
          endRowIndex: entry.rowIndex + 1,
          startColumnIndex: entry.columnIndex,
          endColumnIndex: entry.columnIndex + 1,
        },
      },
    }));

  await batchUpdateSpreadsheet(requests);
}

async function applyOpenItemsCheckboxValidation(sheetId, headerRow, startRow, endRow, checkboxColumns = []) {
  if (!sheetId || endRow < startRow) {
    return;
  }

  const requests = checkboxColumns
    .map((header) => ({
      header,
      index: headerRow.findIndex((value) => String(value || "").trim() === header),
    }))
    .filter((entry) => entry.index >= 0)
    .map((entry) => ({
      setDataValidation: {
        range: {
          sheetId,
          startRowIndex: startRow - 1,
          endRowIndex: endRow,
          startColumnIndex: entry.index,
          endColumnIndex: entry.index + 1,
        },
        rule: {
          condition: {
            type: "BOOLEAN",
          },
          strict: true,
          showCustomUi: true,
        },
      },
    }));

  if (!requests.length) {
    return;
  }

  const url = `https://sheets.googleapis.com/v4/spreadsheets/${SOURCE_SPREADSHEET_ID}:batchUpdate`;
  const headers = await getGoogleRequestHeaders(url);
  const response = await fetch(url, {
    method: "POST",
    headers: {
      ...headers,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ requests }),
  });

  if (!response.ok) {
    const detail = await response.text().catch(() => "");
    if (response.status === 400 && detail.includes("not allowed on cells in typed columns")) {
      return;
    }
    throw new Error(`Google Sheets batchUpdate error ${response.status}${detail ? `: ${detail}` : ""}`);
  }
}

function getSheetMetadataByName(metadata, sheetName) {
  return Array.isArray(metadata?.sheets)
    ? metadata.sheets.find((sheet) => sheet.properties?.title === sheetName) || null
    : null;
}

async function applyOpenItemOwnerPeopleChips(sheetId, headerRow, startRow, state) {
  if (sheetId == null) {
    return;
  }

  const ownerColumnIndex = headerRow.findIndex((value) => String(value || "").trim() === "Owner");
  if (ownerColumnIndex < 0) {
    return;
  }

  const contactsDirectory = buildKnownContactsDirectory(state.areas || []);
  const chipUpdates = state.openItems.map((item, index) => {
    const contact = resolveContactByValue(item.ownerName, contactsDirectory);
    const cellValue = buildPeopleChipCellValue(contact, "@");
    if (!cellValue) {
      return null;
    }

    return {
      rowIndex: startRow - 1 + index,
      columnIndex: ownerColumnIndex,
      cellValue,
    };
  });

  await applyPeopleChipUpdates(sheetId, chipUpdates);
}

async function applyAreaOwnerPeopleChips(metadata, areas) {
  const matchedSheet = getSheetMetadataByName(metadata, AREAS_SHEET_NAME);
  const sheetId = matchedSheet?.properties?.sheetId ?? null;
  if (sheetId == null) {
    return;
  }

  const ownerColumnIndex = AREA_HEADERS.indexOf("owner");
  const contactsDirectory = buildKnownContactsDirectory(areas);
  const chipUpdates = areas.map((area, index) => {
    const contact = resolveContactByValue(area.email || area.owner, contactsDirectory) || {
      name: area.owner,
      email: area.email,
    };
    const cellValue = buildPeopleChipCellValue(contact, "@");
    if (!cellValue) {
      return null;
    }

    return {
      rowIndex: 1 + index,
      columnIndex: ownerColumnIndex,
      cellValue,
    };
  });

  await applyPeopleChipUpdates(sheetId, chipUpdates);
}

async function writeOpenItemsSheetPreservingTemplate(rows, state) {
  const [valuesMap, formulaMap, metadata] = await Promise.all([
    batchGetSpreadsheetValues([SOURCE_SHEET_NAME]),
    batchGetSpreadsheetValues([SOURCE_SHEET_NAME], "FORMULA"),
    fetchSpreadsheetMetadata(),
  ]);
  const existingValues = valuesMap.get(SOURCE_SHEET_NAME) || [];
  const existingFormulaValues = formulaMap.get(SOURCE_SHEET_NAME) || [];
  const context = getOpenItemSheetContext(existingValues);
  const matchedSheet = getSheetMetadataByName(metadata, SOURCE_SHEET_NAME);
  const sheetId = matchedSheet?.properties?.sheetId ?? null;
  const checkboxColumns = Array.from(
    new Set(
      (state.areas || [])
        .map((area) => String(area?.sourceColumn || "").trim())
        .filter(Boolean),
    ),
  );
  const headerWidth = Math.max(context.headerRow.length, rows.reduce((max, row) => Math.max(max, row.length), 0));
  const existingDataRowCount = Math.max(0, existingValues.length - context.headerRowNumber);
  const targetRowCount = Math.max(existingDataRowCount, rows.length, 1);
  const startRow = context.headerRowNumber + 1;
  const endRow = startRow + targetRowCount - 1;
  const endColumn = toA1Column(Math.max(0, headerWidth - 1));
  const clearRange = `${SOURCE_SHEET_NAME}!A${startRow}:${endColumn}${endRow}`;
  const nextRows = preserveOpenItemFormulaCells(rows, existingValues, existingFormulaValues, state.areas || []);

  await clearSheetRange(clearRange);

  if (nextRows.length) {
    const writeRange = `${SOURCE_SHEET_NAME}!A${startRow}:${endColumn}${startRow + nextRows.length - 1}`;
    await writeSheetRange(writeRange, nextRows, "USER_ENTERED");
  }

  await applyOpenItemsCheckboxValidation(sheetId, context.headerRow, startRow, endRow, checkboxColumns);
  await applyOpenItemOwnerPeopleChips(sheetId, context.headerRow, startRow, state);
}

async function writeDecisionSheetPreservingTemplate(state) {
  const [valuesMap, formulaMap, metadata] = await Promise.all([
    batchGetSpreadsheetValues([CCB_DECISION_SHEET_NAME]),
    batchGetSpreadsheetValues([CCB_DECISION_SHEET_NAME], "FORMULA"),
    fetchSpreadsheetMetadata(),
  ]);
  const existingValues = valuesMap.get(CCB_DECISION_SHEET_NAME) || [];
  const existingFormulaValues = formulaMap.get(CCB_DECISION_SHEET_NAME) || [];
  if (!existingValues.length) {
    return;
  }

  const { context, rowsByOpenItemId } = parseDecisionSheetRows(existingValues);
  const matchedSheet = getSheetMetadataByName(metadata, CCB_DECISION_SHEET_NAME);
  const sheetId = matchedSheet?.properties?.sheetId ?? null;
  if (sheetId == null) {
    return;
  }

  const decisionRows = buildDecisionSheetRows(state, rowsByOpenItemId);
  const nextDecisionRows = preserveDecisionFormulaCells(decisionRows.slice(1), existingValues, existingFormulaValues);
  const headerWidth = Math.max(
    context.headerRow.length,
    decisionRows.reduce((max, row) => Math.max(max, row.length), 0),
  );
  const existingDataRowCount = Math.max(0, existingValues.length - context.headerRowNumber);
  const targetRowCount = Math.max(existingDataRowCount, nextDecisionRows.length, 1);
  const startRow = context.headerRowNumber + 1;
  const endRow = startRow + targetRowCount - 1;
  const endColumn = toA1Column(Math.max(0, headerWidth - 1));
  const clearRange = `${CCB_DECISION_SHEET_NAME}!A${startRow}:${endColumn}${endRow}`;

  await clearSheetRange(clearRange);

  if (nextDecisionRows.length) {
    const writeRange = `${CCB_DECISION_SHEET_NAME}!A${startRow}:${endColumn}${startRow + nextDecisionRows.length - 1}`;
    await writeSheetRange(writeRange, nextDecisionRows, "USER_ENTERED");
  }
}

async function loadStateFromGoogleSheets() {
  const metadata = await fetchSpreadsheetMetadata();
  const existingSheets = new Set(
    (metadata.sheets || []).map((sheet) => sheet.properties?.title).filter(Boolean),
  );
  const ranges = [SOURCE_SHEET_NAME];
  if (existingSheets.has(AREAS_SHEET_NAME)) {
    ranges.push(AREAS_SHEET_NAME);
  }
  if (existingSheets.has(VOTES_SHEET_NAME)) {
    ranges.push(VOTES_SHEET_NAME);
  }
  if (existingSheets.has(PRESESSION_SHEET_NAME)) {
    ranges.push(PRESESSION_SHEET_NAME);
  }
  const valueMap = await batchGetSpreadsheetValues(ranges);
  const openItemValues = valueMap.get(SOURCE_SHEET_NAME) || [];
  const areaValues = valueMap.get(AREAS_SHEET_NAME) || [];
  const voteValues = valueMap.get(VOTES_SHEET_NAME) || [];
  const preSessionValues = valueMap.get(PRESESSION_SHEET_NAME) || [];
  const openItemContext = getOpenItemSheetContext(openItemValues);
  const areas = parseAreasSheetRows(areaValues);
  const votesMap = parseVotesSheetRows(voteValues);
  const preSessionChecksMap = parsePreSessionSheetRows(preSessionValues);
  const effectiveAreas = areas.length ? areas : getAreaDirectory();
  const snapshot = {
    spreadsheetId: SOURCE_SPREADSHEET_ID,
    spreadsheetTitle: metadata.properties?.title || "",
    sheetName: SOURCE_SHEET_NAME,
    sourceType: "google-sheets",
    importedAt: new Date().toISOString(),
    headers: openItemContext.headerRow || OPEN_ITEM_HEADERS,
    headerRowNumber: openItemContext.headerRowNumber,
    rows: buildSheetValuesMap([openItemContext.headerRow, ...openItemContext.bodyRows]).map((record) => ({
      ...record,
      id: record.ID || "",
      description: record["Open Item Description"] || "",
      owner: record.Owner || "",
      dateCreated: record["Date Created"] || "",
      dueDate: record["Due Date"] || "",
      status: record.Status || "",
      comments: record["Comments/Updates"] || "",
      minutesRelated: record["Minutes Related"] || "",
      gitRepository: record["Git Repository"] || "",
      ccbScore: record["CCB Score"] || "",
      ccbStatus: record["CCB Status"] || "",
      jiraTicketsRelated: record["Jira Tickets Related"] || "",
    })),
  };

  const state = mapSnapshotToState(snapshot, null, effectiveAreas, votesMap, preSessionChecksMap);
  state.source = {
    ...state.source,
    type: "google-sheets",
    sheetName: SOURCE_SHEET_NAME,
    sheetHeaders: snapshot.headers,
  };
  return state;
}

async function persistStateToGoogleSheets(nextState) {
  const normalizedState = sanitizeState(nextState);
  const existingState = await loadStateFromGoogleSheets();
  const areas = resolveAreas(normalizedState.areas, existingState);
  normalizedState.areas = areas;
  const headers = buildOpenItemHeaders(normalizedState, areas);
  normalizedState.source = {
    ...(normalizedState.source || {}),
    type: "google-sheets",
    spreadsheetId: SOURCE_SPREADSHEET_ID,
    sheetName: SOURCE_SHEET_NAME,
    sheetHeaders: headers,
  };
  const openItemRows = buildOpenItemsRowsFromState(normalizedState, areas);
  const areaRows = buildAreasSheetRows(areas);
  const voteRows = buildVotesSheetRows(normalizedState);
  const preSessionRows = buildPreSessionSheetRows(normalizedState);

  await ensureSpreadsheetSheets([AREAS_SHEET_NAME, VOTES_SHEET_NAME, PRESESSION_SHEET_NAME, CCB_DECISION_SHEET_NAME]);
  await writeOpenItemsSheetPreservingTemplate(openItemRows, normalizedState);
  await Promise.all([
    clearAndWriteSheet(AREAS_SHEET_NAME, areaRows),
    clearAndWriteSheet(VOTES_SHEET_NAME, voteRows),
    clearAndWriteSheet(PRESESSION_SHEET_NAME, preSessionRows),
  ]);
  await writeDecisionSheetPreservingTemplate(normalizedState);
  await applyAreaOwnerPeopleChips(await fetchSpreadsheetMetadata(), areas);

  return loadStateFromGoogleSheets();
}

async function fetchLiveSyncSnapshot(config) {
  if (config.sourceType === "google-sheets") {
    return fetchSnapshotFromGoogleSheetsApi();
  }

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

  if (!config.enabled || (config.sourceType !== "google-sheets" && !config.sourceUrl)) {
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

  if (!config.enabled || (config.sourceType !== "google-sheets" && !config.sourceUrl)) {
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
          preSessionChecks: Array.isArray(item.preSessionChecks)
            ? item.preSessionChecks.map((check) => ({
                areaId: check.areaId,
                decision: normalizePreSessionDecision(check.decision),
                comment: check.comment || "",
                requestedAt: check.requestedAt || "",
                lastNotifiedAt: check.lastNotifiedAt || "",
                respondedAt: check.respondedAt || "",
                responderEmail: String(check.responderEmail || "").trim().toLowerCase(),
              }))
            : [],
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

    if (request.method === "GET" && pathname === "/api/google-sheets-auth/callback") {
      const oauthState = decodeGoogleSheetsOAuthState(String(url.searchParams.get("state") || ""));
      if (!oauthState || !oauthState.exp || Date.now() > oauthState.exp) {
        redirectToApp(response, "error", "Estado OAuth invalido o expirado.");
        return;
      }

      if (url.searchParams.get("error")) {
        redirectToApp(response, "error", String(url.searchParams.get("error_description") || url.searchParams.get("error") || "Google rechazo la autorizacion."));
        return;
      }

      const code = String(url.searchParams.get("code") || "");
      if (!code) {
        redirectToApp(response, "error", "Google no devolvio codigo de autorizacion.");
        return;
      }

      const client = createUserGoogleOAuthClient();
      const { tokens } = await client.getToken(code);
      const existingTokens = loadUserGoogleOAuthToken() || {};
      persistUserGoogleOAuthToken({
        ...existingTokens,
        ...tokens,
      });
      redirectToApp(response, "success", "Conexion con Google Sheets lista.");
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
        googleSheetsAuth: getGoogleSheetsAuthStatus(),
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

    if (request.method === "GET" && pathname === "/api/google-sheets-auth/status") {
      sendJson(response, 200, getGoogleSheetsAuthStatus());
      return;
    }

    if (request.method === "GET" && pathname === "/api/google-sheets-auth/start") {
      if (!isUserGoogleOAuthConfigured()) {
        sendJson(response, 400, {
          error: "Google user OAuth not configured",
          detail: "Faltan GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET o GOOGLE_OAUTH_REDIRECT_URI.",
        });
        return;
      }

      const client = createUserGoogleOAuthClient();
      const authUrl = client.generateAuthUrl({
        access_type: "offline",
        prompt: "consent",
        include_granted_scopes: true,
        scope: GOOGLE_USER_OAUTH_SCOPES,
        state: createGoogleSheetsOAuthState(request),
      });
      response.writeHead(302, { Location: authUrl });
      response.end();
      return;
    }

    if (request.method === "POST" && pathname === "/api/google-sheets-auth/disconnect") {
      deleteIfExists(USER_GOOGLE_OAUTH_TOKEN_FILE);
      sendJson(response, 200, {
        ok: true,
        googleSheetsAuth: getGoogleSheetsAuthStatus(),
      });
      return;
    }

    if (request.method === "GET" && pathname === "/api/state") {
      const state = await loadStateStore();
      sendJson(response, 200, {
        state,
        prerequisite: derivePrerequisiteStatus(state),
        dailyReport: buildDailyReport(state),
      });
      return;
    }

    if (request.method === "GET" && pathname === "/api/employees") {
      sendJson(response, 200, getEmployeeDirectory());
      return;
    }

    if (request.method === "POST" && pathname === "/api/employees/clear") {
      sendJson(response, 200, {
        ok: true,
        ...clearEmployeeDirectory(),
      });
      return;
    }

    if (request.method === "GET" && pathname === "/api/pre-session/dashboard") {
      const state = await loadStateStore();
      sendJson(response, 200, buildPreSessionDashboard(state, getCurrentUser(request)));
      return;
    }

    if (request.method === "POST" && pathname === "/api/pre-session/respond") {
      const body = await parseBody(request);
      const user = getCurrentUser(request);
      const state = await loadStateStore();
      const openItemId = String(body.openItemId || "").trim();
      const areaId = String(body.areaId || "").trim();
      const decision = normalizePreSessionDecision(body.decision);
      const comment = String(body.comment || "").trim();
      const ownedAreas = new Set(getOwnedAreasForUser(state, user).map((area) => area.id));

      if (!openItemId || !areaId || !decision) {
        sendJson(response, 400, {
          error: "Bad request",
          detail: "Faltan openItemId, areaId o decision valida.",
        });
        return;
      }

      if (!ownedAreas.has(areaId)) {
        sendJson(response, 403, {
          error: "Forbidden",
          detail: "Solo el owner del area puede responder esta revision.",
        });
        return;
      }

      const item = state.openItems.find((candidate) => candidate.id === openItemId);
      const hasExistingCheck = Boolean(item && getPreSessionCheck(item, areaId));
      const targetAreaIds = item ? new Set(getPreSessionTargetAreaIds(state, item)) : new Set();
      if (!item || (!targetAreaIds.has(areaId) && !hasExistingCheck)) {
        sendJson(response, 404, {
          error: "Not found",
          detail: "No encontre ese open item impactando el area indicada.",
        });
        return;
      }

      const existing = getPreSessionCheck(item, areaId);
      upsertPreSessionCheck(item, areaId, {
        requestedAt: existing?.requestedAt || "",
        lastNotifiedAt: existing?.lastNotifiedAt || "",
        decision,
        comment,
        respondedAt: new Date().toISOString(),
        responderEmail: user?.email || "",
      });
      applyPreSessionImpactDecision(item, areaId, decision);

      const nextState = await persistStateStore(state);
      sendJson(response, 200, {
        ok: true,
        state: nextState,
        dashboard: buildPreSessionDashboard(nextState, user),
      });
      return;
    }

    if (request.method === "POST" && pathname === "/api/pre-session/job") {
      const result = await runPreSessionRequestJob(getCurrentUser(request));
      sendJson(response, 200, result);
      return;
    }

    if (request.method === "POST" && pathname === "/api/state") {
      const body = await parseBody(request);
      const nextState = await persistStateStore(sanitizeState(body));
      sendJson(response, 200, {
        state: nextState,
        prerequisite: derivePrerequisiteStatus(nextState),
        dailyReport: buildDailyReport(nextState),
      });
      return;
    }

    if (request.method === "GET" && pathname === "/api/reports/daily") {
      sendJson(response, 200, buildDailyReport(await loadStateStore()));
      return;
    }

    if (request.method === "POST" && pathname === "/api/reset") {
      const nextState = await persistStateStore(createDefaultState());
      sendJson(response, 200, {
        state: nextState,
        prerequisite: derivePrerequisiteStatus(nextState),
        dailyReport: buildDailyReport(nextState),
      });
      return;
    }

    if (request.method === "POST" && pathname === "/api/import/google-sheet-snapshot") {
      const snapshot = isGoogleSheetsStorageEnabled()
        ? await fetchSnapshotFromGoogleSheetsApi()
        : loadGoogleSnapshot();
      const currentState = await loadStateStore();
      const nextState = await persistStateStore(mapSnapshotToState(snapshot, currentState));
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

    if (request.method === "POST" && pathname === "/api/import/employees-csv") {
      const body = await parseBody(request);
      const directory = buildEmployeeDirectoryFromCsv(body.csvText, body.fileName);
      sendJson(response, 200, {
        ok: true,
        importedAt: directory.importedAt,
        sourceName: directory.sourceName,
        contactCount: directory.contacts.length,
        contacts: directory.contacts,
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
        sourceType: normalizeLiveSyncSourceType(body.sourceType),
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
      const state = await loadStateStore();
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

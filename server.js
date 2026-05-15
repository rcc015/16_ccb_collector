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
const LOGO_DIR = path.join(__dirname, "logo");
const DATA_DIR = path.join(__dirname, "data");
const PUBLIC_APP_ORIGIN = String(process.env.PUBLIC_APP_ORIGIN || "https://repo-validation.conceivable.life").trim().replace(/\/+$/, "");
const SOURCE_SPREADSHEET_ID = process.env.SOURCE_SPREADSHEET_ID || "1G6YNnnIrqEH_oIgq95bXmrER2lUjPYtkCeV5zwaXRms";
const SOURCE_SHEET_NAME = process.env.SOURCE_SHEET_NAME || "Open Item List";
const SOURCE_SHEET_HEADER_ROW = Number(process.env.SOURCE_SHEET_HEADER_ROW || 0);
const AREAS_SHEET_NAME = process.env.AREAS_SHEET_NAME || "CCB Areas";
const VOTES_SHEET_NAME = process.env.VOTES_SHEET_NAME || "CCB Votes";
const PRESESSION_SHEET_NAME = process.env.PRESESSION_SHEET_NAME || "CCB PreSession";
const CCB_DECISION_SHEET_NAME = process.env.CCB_DECISION_SHEET_NAME || "CCB_Decision";
const CCB_EVALUATIONS_SHEET_NAME = process.env.CCB_EVALUATIONS_SHEET_NAME || "CCB Evaluations";
const GOOGLE_SHEETS_AUTH_MODE = process.env.GOOGLE_SHEETS_AUTH_MODE || "";
const GOOGLE_SERVICE_ACCOUNT_KEY_PATH = process.env.GOOGLE_SERVICE_ACCOUNT_KEY_PATH || "";
const GOOGLE_SERVICE_ACCOUNT_JSON = process.env.GOOGLE_SERVICE_ACCOUNT_JSON || "";
const GOOGLE_OAUTH_CLIENT_ID = process.env.GOOGLE_OAUTH_CLIENT_ID || process.env.GOOGLE_CLIENT_ID || "";
const GOOGLE_OAUTH_CLIENT_SECRET = process.env.GOOGLE_OAUTH_CLIENT_SECRET || "";
const GOOGLE_OAUTH_REDIRECT_URI = process.env.GOOGLE_OAUTH_REDIRECT_URI || "";
const PRESESSION_EMAIL_OVERRIDE = normalizeEmail(process.env.PRESESSION_EMAIL_OVERRIDE || "");
const ADMIN_OVERRIDE_EMAIL = "rodrigo@conceivable.life";
const STATE_FILE = path.join(DATA_DIR, "state.json");
const SEED_FILE = path.join(DATA_DIR, "seed.json");
const AREA_DIRECTORY_FILE = path.join(DATA_DIR, "area-directory.json");
const EMPLOYEE_DIRECTORY_FILE = path.join(DATA_DIR, "employee-directory.json");
const GOOGLE_SNAPSHOT_FILE = path.join(DATA_DIR, "google-open-items-snapshot.json");
const LIVE_SYNC_CONFIG_FILE = path.join(DATA_DIR, "live-sync-config.json");
const AUTH_CONFIG_FILE = path.join(DATA_DIR, "auth-config.json");
const EMAIL_MODE_CONFIG_FILE = path.join(DATA_DIR, "email-mode.json");
const USER_GOOGLE_OAUTH_TOKEN_FILE = path.join(DATA_DIR, "google-user-oauth-token.json");
const USER_GOOGLE_OAUTH_TOKEN_RUNTIME_FILE = path.join(DATA_DIR, "google-user-oauth-token.runtime.json");
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
  "impactedUsers",
  "impactedAreas",
  "evaluationParticipants",
  "voteParticipants",
  "pendingEvaluators",
  "lastEvaluationDate",
  "Minutes Related",
  "Git Repository",
  "CCB Score",
  "CCB Status",
  "Jira Tickets Related",
];
const ITL_AREA_HEADERS = ["ITL ICSI", "ITL EGG", "ITL VIT", "ITL All"];
const AREA_HEADERS = ["id", "name", "owner", "email", "sourceColumn"];
const VOTE_HEADERS = ["openItemId", "areaId", "decision", "comment", "voterEmail", "voterName", "voterArea", "createdAt"];
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
const CCB_EVALUATION_HEADERS = [
  "openItemId",
  "evaluatorEmail",
  "evaluatorName",
  "evaluatorArea",
  "criterionId",
  "criterionName",
  "score",
  "weight",
  "rationale",
  "supportingReference",
  "createdAt",
  "updatedAt",
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
const CCB_DECISION_AGGREGATE_HEADERS = [
  "CCB Decision Average Score",
  "CCB Decision Recommendation",
  "CCB Decision Evaluator Count",
  "CCB Decision Strategic Alignment Avg",
  "CCB Decision Risk Assessment Avg",
  "CCB Decision Resource Impact Avg",
  "CCB Decision Customer Value Avg",
  "CCB Decision Operational Feasibility Avg",
  "CCB Decision Last Updated",
  "CCB Decision Evaluators",
];
const IMPLEMENTATION_TRACKING_COLUMN_ALIASES = {
  dateCreated: ["Date Created"],
  dueDate: ["Due Date"],
  status: ["Status"],
  comments: ["Comments/Updates"],
  minutesRelated: ["Minutes Related"],
  gitRepository: ["Git Repository"],
  jiraTicketsRelated: ["Jira Tickets Related"],
  branchLocation: [
    "Branch / Stream / Release Location",
    "Branch / Stream / Release",
    "Release Location",
    "Branch Location",
    "Branch",
  ],
  implementationNotes: [
    "Implementation Notes",
    "Implementation Tracking Notes",
    "Release Notes",
  ],
};

function stripOpenItemDecisionHeaders(headers = []) {
  return (Array.isArray(headers) ? headers : [])
    .map((header) => String(header || "").trim())
    .filter((header) => (
      header &&
      !CCB_DECISION_AGGREGATE_HEADERS.includes(header) &&
      !/^Column\s+\d+$/i.test(header)
    ));
}
const DEFAULT_CCB_DECISION_CRITERIA = [
  {
    id: "strategic-alignment",
    name: "Strategic Alignment",
    weight: 0.3,
    note: "Must score >=4 for approval",
    factors: [
      "Supports product roadmap",
      "Advances business goals",
      "Enhances competitive position",
    ],
    scoringGuide: [
      { score: 1, label: "Misaligned" },
      { score: 3, label: "Neutral" },
      { score: 5, label: "Fully aligned" },
    ],
  },
  {
    id: "risk-assessment",
    name: "Risk Assessment",
    weight: 0.25,
    note: "Reject if safety/compliance risk >3",
    factors: [
      "Technical complexity",
      "Safety/compliance risks",
      "Customer impact if failed",
    ],
    scoringGuide: [
      { score: 1, label: "High risk" },
      { score: 3, label: "Moderate" },
      { score: 5, label: "Low risk" },
    ],
  },
  {
    id: "resource-impact",
    name: "Resource Impact",
    weight: 0.2,
    note: "Flag if score <=2",
    factors: [
      "Engineering hours",
      "Cost: development, testing, rollout",
      "Timeline disruption",
    ],
    scoringGuide: [
      { score: 1, label: "Major impact, more than 20% budget/timeline" },
      { score: 5, label: "Minimal impact, less than 5%" },
    ],
  },
  {
    id: "customer-value",
    name: "Customer Value",
    weight: 0.15,
    note: "Requires supporting data",
    factors: [
      "Solves critical pain points",
      "Expected adoption/upsell",
      "CSAT/NPS impact",
    ],
    scoringGuide: [
      { score: 1, label: "Low value" },
      { score: 5, label: "High value, e.g. churn reduction" },
    ],
  },
  {
    id: "operational-feasibility",
    name: "Operational Feasibility",
    weight: 0.1,
    note: "Ease of execution and supportability",
    factors: [
      "Ease of implementation",
      "Maintenance burden",
      "Supplier/partner readiness",
    ],
    scoringGuide: [
      { score: 1, label: "Not feasible" },
      { score: 5, label: "Easily executable" },
    ],
  },
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
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
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

function getBrandLogoFileName() {
  try {
    const files = fs.readdirSync(LOGO_DIR, { withFileTypes: true })
      .filter((entry) => entry.isFile() && /\.(png|jpg|jpeg)$/i.test(entry.name))
      .map((entry) => entry.name)
      .sort((left, right) => left.localeCompare(right));
    return files[0] || "";
  } catch (error) {
    return "";
  }
}

function getBrandLogoPublicPath() {
  const fileName = getBrandLogoFileName();
  return fileName ? `/logo/${encodeURIComponent(fileName)}` : "";
}

function getBrandLogoAppPath() {
  const publicPath = getBrandLogoPublicPath();
  return publicPath ? withBasePath(publicPath) : "";
}

function getBrandLogoDocumentSrc() {
  return getBrandLogoAppPath() || "data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw==";
}

function getBrandLogoAbsoluteUrl() {
  const publicPath = getBrandLogoPublicPath();
  return publicPath ? `${PUBLIC_APP_ORIGIN}${publicPath}` : "";
}

const EMAIL_LOGO_CID = "conceivable-logo";

function getBrandLogoFilePath() {
  const fileName = getBrandLogoFileName();
  return fileName ? path.join(LOGO_DIR, fileName) : "";
}

function getEmailLogoAttachment() {
  const fileName = getBrandLogoFileName();
  const filePath = getBrandLogoFilePath();
  const exists = Boolean(filePath) && fs.existsSync(filePath);
  if (!fileName || !exists) {
    return null;
  }
  return {
    filename: fileName,
    path: filePath,
    cid: EMAIL_LOGO_CID,
    contentType: "image/png",
  };
}

function hasEmailLogoAsset() {
  return Boolean(getEmailLogoAttachment());
}

function isGlobalLogoPath(pathname) {
  return typeof pathname === "string" && pathname.startsWith("/logo/");
}

function renderEmailBrandHeader(title, eyebrow = "Conceivable") {
  const sansStack = "Inter, 'Avenir Next', 'Helvetica Neue', Arial, sans-serif";
  const hasEmbeddedLogo = hasEmailLogoAsset();
  return `
    <tr>
      <td style="padding:28px 22px 18px;border-bottom:1px solid #EEF2F6;background:#FFFFFF;">
        ${hasEmbeddedLogo ? `
          <img
            src="cid:${EMAIL_LOGO_CID}"
            width="220"
            alt="Conceivable Life Sciences"
            style="display:block;width:220px;max-width:220px;height:auto;border:0;outline:none;text-decoration:none;margin:0 0 10px 0;"
          />
        ` : `
          <div style="font-family:${sansStack};font-size:18px;line-height:1.25;color:#111827;font-weight:700;letter-spacing:-0.02em;margin-bottom:10px;">
            Conceivable Life Sciences
          </div>
        `}
        <div style="font-family:${sansStack};font-size:14px;line-height:1.4;color:#475569;font-weight:700;letter-spacing:.02em;margin-bottom:8px;">Change Control Board</div>
        <div style="font-family:${sansStack};font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:#C88A2D;font-weight:700;margin-bottom:4px;">${escapeHtmlEmail(eyebrow)}</div>
        <div style="font-family:${sansStack};font-size:24px;line-height:1.2;color:#0F172A;font-weight:700;letter-spacing:-0.02em;">${escapeHtmlEmail(title)}</div>
      </td>
    </tr>
  `;
}

function renderEmailShell({ title, eyebrow, introHtml, bodyHtml, ctaHref, ctaLabel = "Review Open Item", footerHtml = "" }) {
  const sansStack = "Inter, 'Avenir Next', 'Helvetica Neue', Arial, sans-serif";
  return `
<!doctype html>
<html lang="en">
  <body style="margin:0;padding:20px;background:#F8F6F1;">
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width:640px;margin:0 auto;border:1px solid #E8ECF1;border-radius:18px;background:#FFFFFF;overflow:hidden;">
      ${renderEmailBrandHeader(title, eyebrow)}
      <tr>
        <td style="padding:20px 22px 22px;">
          ${introHtml ? `<div style="font-family:${sansStack};font-size:14px;line-height:1.65;color:#334155;margin-bottom:16px;">${introHtml}</div>` : ""}
          ${bodyHtml}
          ${ctaHref ? `
            <div style="padding-top:16px;">
              <a href="${escapeHtmlEmail(ctaHref)}" style="display:inline-block;background:#0F2836;color:#FFFFFF;text-decoration:none;font-family:${sansStack};font-size:14px;font-weight:600;padding:10px 16px;border-radius:999px;">${escapeHtmlEmail(ctaLabel)}</a>
            </div>
            <div style="font-family:${sansStack};font-size:12px;line-height:1.6;color:#6B7280;margin-top:10px;">
              Fallback link: <a href="${escapeHtmlEmail(ctaHref)}" style="color:#0F2836;text-decoration:underline;">${escapeHtmlEmail(ctaHref)}</a>
            </div>
          ` : ""}
          ${footerHtml ? `<div style="font-family:${sansStack};font-size:12px;line-height:1.6;color:#7C8797;margin-top:14px;">${footerHtml}</div>` : ""}
        </td>
      </tr>
    </table>
  </body>
</html>`;
}

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

function normalizeEmailMode(value) {
  return String(value || "").trim().toLowerCase() === "live" ? "live" : "test";
}

function loadEmailModeConfig() {
  const fileConfig = readOptionalJson(EMAIL_MODE_CONFIG_FILE, {});
  return {
    mode: normalizeEmailMode(fileConfig.mode),
    updatedAt: String(fileConfig.updatedAt || "").trim(),
    updatedBy: normalizeEmail(String(fileConfig.updatedBy || "")),
  };
}

function persistEmailModeConfig(nextConfig = {}, user = null) {
  const payload = {
    mode: normalizeEmailMode(nextConfig.mode),
    updatedAt: new Date().toISOString(),
    updatedBy: normalizeEmail(user?.email || ""),
  };
  writeJson(EMAIL_MODE_CONFIG_FILE, payload);
  return payload;
}

function getEmailModeStatus() {
  const config = loadEmailModeConfig();
  const mode = normalizeEmailMode(config.mode);
  return {
    mode,
    overrideEmail: PRESESSION_EMAIL_OVERRIDE,
    effectiveRecipientBehavior: mode === "live" ? "actual" : "override",
    updatedAt: config.updatedAt || "",
    updatedBy: config.updatedBy || "",
  };
}

function resolveNotificationRecipient(intendedRecipient) {
  const normalizedIntendedRecipient = normalizeEmail(intendedRecipient || "");
  const emailMode = getEmailModeStatus();

  if (emailMode.mode === "live") {
    if (!normalizedIntendedRecipient) {
      return {
        ok: false,
        mode: "live",
        status: "missing-owner-email",
        intendedRecipient: "",
        deliveryTarget: "",
        footerText: "",
        footerHtml: "",
      };
    }
    return {
      ok: true,
      mode: "live",
      status: "live-sent",
      intendedRecipient: normalizedIntendedRecipient,
      deliveryTarget: normalizedIntendedRecipient,
      footerText: "",
      footerHtml: "",
    };
  }

  if (!PRESESSION_EMAIL_OVERRIDE) {
    return {
      ok: false,
      mode: "test",
      status: "test-mode-missing-override",
      intendedRecipient: normalizedIntendedRecipient,
      deliveryTarget: "",
      footerText: "",
      footerHtml: "",
      error: "Test email mode is active but PRESESSION_EMAIL_OVERRIDE is not configured.",
    };
  }

  const footerText = normalizedIntendedRecipient
    ? `Test mode: this message was sent to the override address. Original intended recipient: ${normalizedIntendedRecipient}.`
    : "Test mode: original intended recipient could not be resolved.";
  return {
    ok: true,
    mode: "test",
    status: "test-sent",
    intendedRecipient: normalizedIntendedRecipient,
    deliveryTarget: PRESESSION_EMAIL_OVERRIDE,
    footerText,
    footerHtml: escapeHtmlEmail(footerText),
  };
}

function createDefaultState() {
  if (fs.existsSync(SEED_FILE)) {
    return readJson(SEED_FILE);
  }

  return {
    areas: [],
    openItems: [],
    ccbDecisionCriteria: DEFAULT_CCB_DECISION_CRITERIA,
    ccbEvaluations: [],
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

async function persistEvaluationStateStore(nextState) {
  if (!isGoogleSheetsStorageEnabled()) {
    return persistState(nextState);
  }

  const normalizedState = sanitizeState(nextState);
  const evaluationRows = buildCcbEvaluationRows(normalizedState);

  await ensureSpreadsheetSheets([CCB_DECISION_SHEET_NAME, CCB_EVALUATIONS_SHEET_NAME]);
  await clearAndWriteSheet(CCB_EVALUATIONS_SHEET_NAME, evaluationRows);
  await writeDecisionSheetPreservingTemplate(normalizedState);

  return loadStateFromGoogleSheets();
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
  const contactsByAlias = new Map();
  const contacts = [];

  const pushAlias = (alias, contact) => {
    const key = String(alias || "").trim().toLowerCase();
    if (!key) {
      return;
    }
    const existing = contactsByAlias.get(key) || [];
    if (!existing.some((entry) => entry.email === contact.email && entry.name === contact.name)) {
      existing.push(contact);
      contactsByAlias.set(key, existing);
    }
  };

  const pushContact = (name, email, metadata = {}) => {
    const normalizedName = String(name || "").trim();
    const embeddedEmail = extractEmailAddress(normalizedName);
    const normalizedEmail = normalizeEmail(email || embeddedEmail);
    if (!normalizedName && !normalizedEmail) {
      return;
    }

    const contact = {
      name: normalizedName,
      email: normalizedEmail,
      displayName: String(metadata.displayName || "").trim(),
      area: String(metadata.area || "").trim(),
      primaryArea: String(metadata.primaryArea || "").trim(),
      source: String(metadata.source || "").trim(),
    };

    if (normalizedEmail && !contactsByEmail.has(normalizedEmail)) {
      contactsByEmail.set(normalizedEmail, contact);
    }

    const personKeys = new Set([
      normalizedName ? normalizedName.toLowerCase() : "",
      String(contact.displayName || "").trim().toLowerCase(),
      ...buildPersonAliases(normalizedName, normalizedEmail),
      ...buildPersonAliases(contact.displayName, normalizedEmail),
    ].filter(Boolean));

    personKeys.forEach((key) => {
      if (!contactsByName.has(key)) {
        contactsByName.set(key, contact);
      }
      pushAlias(key, contact);
    });

    contacts.push(contact);
  };

  getEmployeeDirectory().contacts.forEach((contact) => pushContact(contact.name || contact.displayName, contact.email, {
    displayName: contact.displayName || contact.name || "",
    area: contact.area || "",
    primaryArea: contact.primaryArea || "",
    source: "employee-directory",
  }));
  areas.forEach((area) => pushContact(area.owner, area.email, {
    displayName: area.owner || area.name || "",
    area: area.id || area.name || "",
    primaryArea: area.id || area.name || "",
    source: "ccb-areas",
  }));

  return {
    contacts,
    byEmail: contactsByEmail,
    byName: contactsByName,
    byAlias: contactsByAlias,
  };
}

function resolveContactByValue(value, contactsDirectory) {
  const normalizedValue = String(value || "").trim();
  if (!normalizedValue) {
    return null;
  }

  const normalizedEmail = extractEmailAddress(normalizedValue) || normalizeEmail(normalizedValue);
  if (contactsDirectory.byEmail.has(normalizedEmail)) {
    return contactsDirectory.byEmail.get(normalizedEmail);
  }

  const exactNameMatch = contactsDirectory.byName.get(normalizedValue.toLowerCase())
    || contactsDirectory.byName.get(normalizePersonName(normalizedValue));
  if (exactNameMatch) {
    return exactNameMatch;
  }

  const normalizedSearch = normalizeContactName(normalizedValue);
  if (!normalizedSearch) {
    return null;
  }

  const searchAliases = Array.from(new Set([
    normalizedSearch,
    ...buildPersonAliases(normalizedValue, normalizedEmail),
  ].filter(Boolean)));
  const aliasMatches = searchAliases
    .flatMap((alias) => contactsDirectory.byAlias.get(alias) || [])
    .filter((contact, index, collection) => collection.findIndex((entry) => entry.email === contact.email && entry.name === contact.name) === index);
  if (aliasMatches.length === 1) {
    return aliasMatches[0];
  }

  const rankedMatches = (contactsDirectory.contacts || [])
    .map((contact) => {
      const normalizedContactNames = new Set([
        normalizeContactName(contact.name),
        normalizeContactName(contact.displayName),
        ...buildPersonAliases(contact.name, contact.email),
        ...buildPersonAliases(contact.displayName, contact.email),
      ].filter(Boolean));
      if (!normalizedContactNames.size) {
        return null;
      }

      let bestScore = 0;
      normalizedContactNames.forEach((normalizedContactName) => {
        if (normalizedContactName === normalizedSearch) {
          bestScore = Math.max(bestScore, 5);
          return;
        }
        if (normalizedContactName.startsWith(`${normalizedSearch} `) || normalizedContactName.endsWith(` ${normalizedSearch}`)) {
          bestScore = Math.max(bestScore, 4);
          return;
        }
        if (normalizedContactName.includes(` ${normalizedSearch} `) || normalizedContactName.startsWith(normalizedSearch) || normalizedContactName.endsWith(normalizedSearch)) {
          bestScore = Math.max(bestScore, 3);
          return;
        }
        if (normalizedSearch.length >= 4 && normalizedContactName.includes(normalizedSearch)) {
          bestScore = Math.max(bestScore, 2);
        }
      });
      if (bestScore > 0) {
        return { score: bestScore, contact };
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

function normalizeImplementationStatus(value) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ");

  if (!normalized) {
    return "";
  }

  if (normalized === "open") {
    return "OPEN";
  }

  if (normalized === "in progress") {
    return "IN PROGRESS";
  }

  if (normalized === "closed") {
    return "CLOSED";
  }

  return "";
}

function slugifyCriterion(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function getDefaultCcbDecisionCriteria() {
  return DEFAULT_CCB_DECISION_CRITERIA.map((criterion) => ({
    ...criterion,
    factors: Array.isArray(criterion.factors) ? [...criterion.factors] : [],
    scoringGuide: Array.isArray(criterion.scoringGuide) ? criterion.scoringGuide.map((entry) => ({ ...entry })) : [],
  }));
}

function normalizeCriterionScoringGuide(scoringGuide = [], fallbackGuide = []) {
  const normalized = (Array.isArray(scoringGuide) ? scoringGuide : [])
    .map((entry) => {
      if (entry && typeof entry === "object") {
        const score = Number(entry.score);
        const label = String(entry.label || "").trim();
        return Number.isInteger(score) && label ? { score, label } : null;
      }

      if (typeof entry === "string") {
        const match = entry.match(/^\s*(\d+)\s*=\s*(.+)\s*$/);
        if (match) {
          return { score: Number(match[1]), label: String(match[2] || "").trim() };
        }
      }

      return null;
    })
    .filter(Boolean);

  return normalized.length
    ? normalized
    : (Array.isArray(fallbackGuide) ? fallbackGuide.map((entry) => ({ ...entry })) : []);
}

function normalizeDecisionCriterion(criterion, fallbackCriterion = {}) {
  const fallback = fallbackCriterion || {};
  const criterionId = String(criterion?.id || fallback.id || slugifyCriterion(criterion?.name || fallback.name || "")).trim();
  return {
    id: criterionId,
    name: String(criterion?.name || fallback.name || criterionId).trim(),
    weight: Number.isFinite(Number(criterion?.weight)) && Number(criterion.weight) > 0
      ? Number(criterion.weight)
      : Number(fallback.weight || 0),
    note: String(criterion?.note || fallback.note || "").trim(),
    factors: Array.isArray(criterion?.factors) && criterion.factors.length
      ? criterion.factors.map((factor) => String(factor || "").trim()).filter(Boolean)
      : Array.isArray(fallback.factors) ? fallback.factors.map((factor) => String(factor || "").trim()).filter(Boolean) : [],
    scoringGuide: normalizeCriterionScoringGuide(criterion?.scoringGuide, fallback.scoringGuide),
  };
}

function loadCcbDecisionCriteria(decisionSheetValues = []) {
  const defaults = getDefaultCcbDecisionCriteria();
  let context = null;
  try {
    context = Array.isArray(decisionSheetValues) && decisionSheetValues.length
      ? getDecisionSheetContext(decisionSheetValues)
      : null;
  } catch (error) {
    return defaults;
  }
  const headers = context?.headerRow || [];
  const availableCriteria = headers
    .filter((header) => String(header || "").trim().endsWith(" Score"))
    .map((header) => String(header || "").trim().replace(/ Score$/, ""));

  if (!availableCriteria.length) {
    return defaults;
  }

  return defaults.filter((criterion) => {
    const normalizedName = String(criterion.name || "").trim().toLowerCase();
    return availableCriteria.some((entry) => entry.trim().toLowerCase() === normalizedName);
  }).map((criterion) => normalizeDecisionCriterion(criterion, criterion));
}

function calculateWeightedScore(evaluationEntries = [], criteria = getDefaultCcbDecisionCriteria()) {
  const entryMap = new Map(
    (evaluationEntries || []).map((entry) => [String(entry.criterionId || "").trim(), entry]),
  );

  const allCriteriaScored = criteria.every((criterion) => {
    const score = Number(entryMap.get(criterion.id)?.score);
    return Number.isInteger(score) && score >= 1 && score <= 5;
  });

  if (!allCriteriaScored) {
    return null;
  }

  const total = criteria.reduce((sum, criterion) => {
    const score = Number(entryMap.get(criterion.id)?.score || 0);
    return sum + (score * Number(criterion.weight || 0));
  }, 0);

  return Number(total.toFixed(2));
}

function getDecisionRecommendation(weightedScore) {
  const numericScore = Number(weightedScore);
  if (!Number.isFinite(numericScore)) {
    return "INCOMPLETE";
  }
  if (numericScore >= 4) {
    return "APPROVE";
  }
  if (numericScore >= 3) {
    return "DEFER";
  }
  return "REJECT";
}

function calculateEvaluatorWeightedScore(evaluatorEvaluation = [], criteria = getDefaultCcbDecisionCriteria()) {
  return calculateWeightedScore(evaluatorEvaluation, criteria);
}

function getSubmittedEvaluationsForOpenItem(openItemId, state, criteria = null) {
  const effectiveCriteria = Array.isArray(criteria) && criteria.length ? criteria : (
    Array.isArray(state?.ccbDecisionCriteria) && state.ccbDecisionCriteria.length
      ? state.ccbDecisionCriteria
      : getDefaultCcbDecisionCriteria()
  );
  const grouped = new Map();

  (state?.ccbEvaluations || [])
    .filter((entry) => entry.openItemId === openItemId)
    .forEach((entry) => {
      const key = normalizeEmail(entry.evaluatorEmail);
      if (!key) {
        return;
      }
      if (!grouped.has(key)) {
        grouped.set(key, []);
      }
      grouped.get(key).push(entry);
    });

  return Array.from(grouped.entries())
    .map(([evaluatorEmail, entries]) => ({
      evaluatorEmail,
      evaluatorName: entries[0]?.evaluatorName || "",
      evaluatorArea: entries[0]?.evaluatorArea || "",
      entries,
      weightedScore: calculateEvaluatorWeightedScore(entries, effectiveCriteria),
      updatedAt: entries
        .map((entry) => entry.updatedAt || entry.createdAt || "")
        .filter(Boolean)
        .sort()
        .reverse()[0] || "",
    }))
    .filter((group) => Number.isFinite(group.weightedScore));
}

function calculateOpenItemCcbDecisionAverages(openItemId, state, criteria = null) {
  const effectiveCriteria = Array.isArray(criteria) && criteria.length ? criteria : (
    Array.isArray(state?.ccbDecisionCriteria) && state.ccbDecisionCriteria.length
      ? state.ccbDecisionCriteria
      : getDefaultCcbDecisionCriteria()
  );
  const submittedEvaluations = getSubmittedEvaluationsForOpenItem(openItemId, state, effectiveCriteria);
  if (!submittedEvaluations.length) {
    return {
      averageWeightedScore: null,
      recommendation: "INCOMPLETE",
      evaluatorCount: 0,
      criterionAverages: Object.fromEntries(effectiveCriteria.map((criterion) => [criterion.id, null])),
      lastUpdated: "",
      evaluators: [],
    };
  }

  const averageWeightedScore = Number((
    submittedEvaluations.reduce((sum, entry) => sum + Number(entry.weightedScore || 0), 0) / submittedEvaluations.length
  ).toFixed(2));

  const criterionAverages = Object.fromEntries(effectiveCriteria.map((criterion) => {
    const scores = submittedEvaluations
      .map((entry) => Number(entry.entries.find((candidate) => candidate.criterionId === criterion.id)?.score || 0))
      .filter((score) => Number.isInteger(score) && score >= 1 && score <= 5);
    const average = scores.length
      ? Number((scores.reduce((sum, score) => sum + score, 0) / scores.length).toFixed(2))
      : null;
    return [criterion.id, average];
  }));

  const lastUpdated = submittedEvaluations
    .map((entry) => entry.updatedAt)
    .filter(Boolean)
    .sort()
    .reverse()[0] || "";

  return {
    averageWeightedScore,
    recommendation: getDecisionRecommendation(averageWeightedScore),
    evaluatorCount: submittedEvaluations.length,
    criterionAverages,
    lastUpdated,
    evaluators: submittedEvaluations.map((entry) => entry.evaluatorName || entry.evaluatorEmail),
  };
}

function updateOpenItemCcbDecision(openItemId, aggregatedDecision, state) {
  const item = (state?.openItems || []).find((candidate) => candidate.id === openItemId);
  if (!item) {
    return null;
  }

  const summary = aggregatedDecision || calculateOpenItemCcbDecisionAverages(
    openItemId,
    state,
    Array.isArray(state?.ccbDecisionCriteria) && state.ccbDecisionCriteria.length
      ? state.ccbDecisionCriteria
      : getDefaultCcbDecisionCriteria(),
  );

  item.ccbDecisionAverageScore = summary.averageWeightedScore;
  item.ccbDecisionRecommendation = summary.recommendation;
  item.ccbDecisionEvaluatorCount = summary.evaluatorCount;
  item.ccbDecisionCriterionAverages = summary.criterionAverages;
  item.ccbDecisionLastUpdated = summary.lastUpdated;
  item.ccbDecisionEvaluators = summary.evaluators;
  return item;
}

function getOpenItemCcbDecisionSummary(openItemId, state) {
  const item = (state?.openItems || []).find((candidate) => candidate.id === openItemId);
  if (!item) {
    return null;
  }

  if (
    Object.prototype.hasOwnProperty.call(item, "ccbDecisionAverageScore") &&
    Object.prototype.hasOwnProperty.call(item, "ccbDecisionRecommendation")
  ) {
    return {
      averageWeightedScore: item.ccbDecisionAverageScore ?? null,
      recommendation: item.ccbDecisionRecommendation || "INCOMPLETE",
      evaluatorCount: Number(item.ccbDecisionEvaluatorCount || 0),
      criterionAverages: item.ccbDecisionCriterionAverages || {},
      lastUpdated: item.ccbDecisionLastUpdated || "",
      evaluators: Array.isArray(item.ccbDecisionEvaluators) ? item.ccbDecisionEvaluators : [],
    };
  }

  return calculateOpenItemCcbDecisionAverages(openItemId, state);
}

function isFastTrackCandidate(item, evaluationEntries = []) {
  const byCriterion = new Map((evaluationEntries || []).map((entry) => [entry.criterionId, entry]));
  const riskScore = Number(byCriterion.get("risk-assessment")?.score || 0);
  const resourceScore = Number(byCriterion.get("resource-impact")?.score || 0);
  const searchableText = [
    item?.title,
    item?.description,
    ...(evaluationEntries || []).map((entry) => `${entry.rationale || ""} ${entry.supportingReference || ""}`),
  ].join(" ").toLowerCase();
  const criticalHint = /(critical|security|legal|compliance|regulatory|privacy|gdpr|safety)/i.test(searchableText);

  return riskScore === 5 && resourceScore === 5 && criticalHint;
}

function getEvaluationWarnings(item, evaluationEntries = [], criteria = getDefaultCcbDecisionCriteria()) {
  const warnings = [];
  const byCriterion = new Map((evaluationEntries || []).map((entry) => [entry.criterionId, entry]));

  criteria.forEach((criterion) => {
    const entry = byCriterion.get(criterion.id);
    const score = Number(entry?.score);
    if (!Number.isInteger(score) || score < 1 || score > 5) {
      warnings.push(`Please select a score for ${criterion.name}.`);
    }
    if (!String(entry?.rationale || "").trim()) {
      warnings.push(`Please provide a decision justification for ${criterion.name}.`);
    }
  });

  const strategicScore = Number(byCriterion.get("strategic-alignment")?.score || 0);
  if (strategicScore > 0 && strategicScore < 4) {
    warnings.push("Strategic Alignment must score >=4 for approval.");
  }

  const resourceScore = Number(byCriterion.get("resource-impact")?.score || 0);
  if (resourceScore > 0 && resourceScore <= 2) {
    warnings.push("Resource impact is high and should be flagged.");
  }

  const customerValueEntry = byCriterion.get("customer-value");
  if (customerValueEntry?.score && !String(customerValueEntry.supportingReference || "").trim()) {
    warnings.push("Customer Value should include supporting data/reference.");
  }

  if (isFastTrackCandidate(item, evaluationEntries)) {
    warnings.push("Fast-track candidate.");
  }

  return warnings;
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
  const tokenFiles = [USER_GOOGLE_OAUTH_TOKEN_FILE, USER_GOOGLE_OAUTH_TOKEN_RUNTIME_FILE];
  for (const filePath of tokenFiles) {
    if (fs.existsSync(filePath)) {
      return readOptionalJson(filePath, null);
    }
  }
  return null;
}

function persistUserGoogleOAuthToken(tokens) {
  const tokenFiles = [USER_GOOGLE_OAUTH_TOKEN_FILE, USER_GOOGLE_OAUTH_TOKEN_RUNTIME_FILE];
  let lastError = null;

  for (const filePath of tokenFiles) {
    try {
      writeJson(filePath, tokens);
      return filePath;
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError || new Error("Failed to store the Google OAuth token.");
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

function isAdminUser(user) {
  return normalizeEmail(user?.email) === ADMIN_OVERRIDE_EMAIL;
}

function isChangeManagerUser(user) {
  return isAdminUser(user);
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
    throw new Error("Failed to read the token payload.");
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
  const contactsDirectory = buildKnownContactsDirectory(areas);
  const previousItems = new Map((existingState?.openItems || []).map((item) => [item.id, item]));
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

      const previousItem = previousItems.get(row.id) || null;
      const resolvedOwnerContact = resolveContactByValue(row.owner, contactsDirectory);
      const fallbackOwnerArea = impactedAreaIds[0] || "";
      return {
        id: row.id,
        title: row.description,
        description: row.comments || "",
        sourceRef: `${snapshot.spreadsheetTitle || "Google Sheet"} / ${snapshot.sheetName || "Open Item List"}`,
        ownerAreaId: fallbackOwnerArea,
        ownerName: row.owner || "",
        ownerEmail: resolvedOwnerContact?.email || previousItem?.ownerEmail || "",
        ownerAreaHint: "",
        impactedAreaIds,
        isSubstantial: inferSubstantialChange(row),
        status: normalizeStatus(row.status),
        implementationStatus: normalizeImplementationStatus(row.status || previousItem?.implementationStatus || ""),
        createdAt: row.dateCreated || new Date().toISOString(),
        dueDate: row.dueDate || "",
        minutesRelated: row.minutesRelated || "",
        gitRepository: row.gitRepository || "",
        jiraTicketsRelated: row.jiraTicketsRelated || "",
        branchLocation: String(
          row["Branch / Stream / Release Location"] ||
          row["Branch / Stream / Release"] ||
          row["Release Location"] ||
          row.Branch ||
          previousItem?.branchLocation ||
          ""
        ).trim(),
        implementationNotes: String(
          row["Implementation Notes"] ||
          row["Implementation Tracking Notes"] ||
          row["Release Notes"] ||
          previousItem?.implementationNotes ||
          ""
        ).trim(),
        implementationTrackingCreatedAt: String(previousItem?.implementationTrackingCreatedAt || "").trim(),
        implementationApprovalDate: String(previousItem?.implementationApprovalDate || "").trim(),
        externalStatus: row.ccbStatus || "",
        ccbScore: row.ccbScore || "",
        rawSheetRow: row,
        votes: previousVotes.get(row.id) || [],
        preSessionChecks: previousPreSessionChecks.get(row.id) || [],
        impactedUsers: String(row.impactedUsers || "").trim() ? String(row.impactedUsers || "").split(/\s*,\s*/).filter(Boolean) : [],
        impactedAreas: String(row.impactedAreas || "").trim() ? String(row.impactedAreas || "").split(/\s*,\s*/).filter(Boolean) : [],
        evaluationParticipants: String(row.evaluationParticipants || "").trim() ? String(row.evaluationParticipants || "").split(/\s*,\s*/).filter(Boolean) : [],
        voteParticipants: String(row.voteParticipants || "").trim() ? String(row.voteParticipants || "").split(/\s*,\s*/).filter(Boolean) : [],
        pendingEvaluators: String(row.pendingEvaluators || "").trim() ? String(row.pendingEvaluators || "").split(/\s*,\s*/).filter(Boolean) : [],
        lastEvaluationDate: row.lastEvaluationDate || "",
        ccbDecisionCriterionAverages: {
          "strategic-alignment": null,
          "risk-assessment": null,
          "resource-impact": null,
          "customer-value": null,
          "operational-feasibility": null,
        },
        ccbDecisionAverageScore: null,
        ccbDecisionRecommendation: "",
        ccbDecisionEvaluatorCount: 0,
        ccbDecisionLastUpdated: "",
        ccbDecisionEvaluators: [],
      };
    });

  openItems.forEach((item) => {
    const derived = deriveOpenItemTracking({
      ...existingState,
      areas,
      openItems,
      ccbEvaluations: Array.isArray(existingState?.ccbEvaluations) ? existingState.ccbEvaluations : [],
    }, item);
    item.impactedAreaIds = derived.impactedAreaIds;
    item.impactedAreas = derived.impactedAreas;
    item.impactedUsers = derived.impactedUsers;
    item.evaluationParticipants = derived.evaluationParticipants;
    item.voteParticipants = derived.voteParticipants;
    item.pendingEvaluators = derived.pendingEvaluators;
    item.lastEvaluationDate = derived.lastEvaluationDate;
  });

  return {
    areas,
    openItems,
    ccbDecisionCriteria: Array.isArray(existingState?.ccbDecisionCriteria) && existingState.ccbDecisionCriteria.length
      ? existingState.ccbDecisionCriteria
      : getDefaultCcbDecisionCriteria(),
    ccbEvaluations: Array.isArray(existingState?.ccbEvaluations) ? existingState.ccbEvaluations : [],
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
      label: "PREREQUISITE NOT MET",
      detail: "There are substantial open changes without complete impacted-area approval.",
      blockedItems: blockedItems.map((item) => item.id),
    };
  }

  if (substantialOpenItems.length > 0) {
    return {
      status: "review",
      label: "PENDING REVIEW",
      detail: "There are substantial open changes, but they already have sufficient approvals.",
      blockedItems: [],
    };
  }

  return {
    status: "clear",
    label: "PREREQUISITE MET",
    detail: "There are no substantial open changes in the Open Item List.",
    blockedItems: [],
  };
}

function buildAreaMap(state) {
  return new Map(state.areas.map((area) => [area.id, area]));
}

function uniqueStrings(values = []) {
  return Array.from(new Set(
    values
      .map((value) => String(value || "").trim())
      .filter(Boolean),
  ));
}

function deriveOpenItemTracking(state, item) {
  const areaMap = buildAreaMap(state);
  const impactedAreaIds = uniqueStrings([
    ...(Array.isArray(item.impactedAreaIds) ? item.impactedAreaIds : []),
    ...((item.votes || []).map((vote) => vote.areaId)),
    ...((item.preSessionChecks || [])
      .filter((check) => check.decision === "impact")
      .map((check) => check.areaId)),
    ...((state.ccbEvaluations || [])
      .filter((evaluation) => evaluation.openItemId === item.id)
      .map((evaluation) => evaluation.evaluatorArea)),
  ]);

  const autoAssignedUsers = impactedAreaIds
    .map((areaId) => normalizeEmail(areaMap.get(areaId)?.email))
    .filter(Boolean);
  const voteParticipants = uniqueStrings((item.votes || []).map((vote) => normalizeEmail(vote.voterEmail)));
  const evaluationParticipants = uniqueStrings(
    (state.ccbEvaluations || [])
      .filter((evaluation) => evaluation.openItemId === item.id)
      .map((evaluation) => normalizeEmail(evaluation.evaluatorEmail)),
  );
  const impactedUsers = uniqueStrings([
    ...autoAssignedUsers,
    ...voteParticipants,
    ...evaluationParticipants,
  ]);
  const respondedUsers = new Set([...voteParticipants, ...evaluationParticipants]);
  const pendingEvaluators = impactedUsers.filter((email) => !respondedUsers.has(email));
  const evaluationDates = (state.ccbEvaluations || [])
    .filter((evaluation) => evaluation.openItemId === item.id)
    .map((evaluation) => evaluation.updatedAt || evaluation.createdAt)
    .filter(Boolean)
    .sort()
    .reverse();

  return {
    impactedAreaIds,
    impactedAreas: uniqueStrings(impactedAreaIds.map((areaId) => areaMap.get(areaId)?.name || areaId)),
    impactedUsers,
    evaluationParticipants,
    voteParticipants,
    pendingEvaluators,
    lastEvaluationDate: evaluationDates[0] || "",
  };
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

function extractEmailAddress(value) {
  const match = String(value || "").match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  return normalizeEmail(match ? match[0] : "");
}

function normalizePersonName(value) {
  const raw = String(value || "")
    .replace(/[“”]/g, "\"")
    .replace(/[‘’]/g, "'")
    .trim();
  if (!raw) {
    return "";
  }

  const withoutEmail = raw
    .replace(/<[^>]*@[A-Z0-9.-]+\.[A-Z]{2,}[^>]*>/gi, " ")
    .replace(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi, " ")
    .replace(/\([^)]*\)/g, " ")
    .replace(/\[[^\]]*\]/g, " ")
    .replace(/\s+-\s+.*/g, " ");

  return withoutEmail
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function normalizeContactName(value) {
  return normalizePersonName(value);
}

function buildPersonAliases(name, email = "") {
  const aliases = new Set();
  const normalizedName = normalizePersonName(name);
  if (normalizedName) {
    aliases.add(normalizedName);
    const parts = normalizedName.split(" ").filter(Boolean);
    if (parts.length >= 2) {
      aliases.add(`${parts[0][0]}${parts[parts.length - 1]}`);
      aliases.add(parts[parts.length - 1]);
      aliases.add(parts[0]);
    }
  }

  const extractedEmail = extractEmailAddress(email || name);
  if (extractedEmail) {
    const localPart = extractedEmail.split("@")[0] || "";
    const normalizedLocalPart = normalizePersonName(localPart.replace(/[._-]+/g, " "));
    if (normalizedLocalPart) {
      aliases.add(normalizedLocalPart);
    }
    if (localPart) {
      aliases.add(localPart.toLowerCase());
    }
  }

  return Array.from(aliases).filter(Boolean);
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
  return `CCB Pre-Session Review Pending: ${group.pendingCount} Open Item${group.pendingCount === 1 ? "" : "s"}`;
}

function buildAppDeepLink(params = {}) {
  const authConfig = loadAuthConfig();
  const baseUrl = String(authConfig.appBaseUrl || withBasePath("/") || "").trim();

  let url;
  try {
    url = new URL(baseUrl);
  } catch (error) {
    url = new URL(baseUrl || "/", "http://localhost");
  }

  Object.entries(params).forEach(([key, value]) => {
    if (value == null || value === "") {
      return;
    }
    url.searchParams.set(key, String(value));
  });

  if (!/^https?:/i.test(baseUrl)) {
    return `${url.pathname}${url.search}${url.hash}`;
  }

  return url.toString();
}

function buildTicketDeepLink(openItemId) {
  return buildAppDeepLink({
    tab: "openItems",
    openItemId,
  });
}

function resolveOwnerEmailForOpenItem(item, state) {
  const areas = Array.isArray(state?.areas) ? state.areas : [];
  const contactsDirectory = buildKnownContactsDirectory(areas);
  const ownerRawValue = String(
    item?.ownerEmail
    || item?.ownerName
    || item?.rawSheetRow?.Owner
    || item?.rawSheetRow?.owner
    || "",
  ).trim();
  const normalizedOwner = normalizePersonName(ownerRawValue || item?.ownerName || "");
  const ownerAreaId = String(item?.ownerAreaId || item?.primaryArea || item?.rawSheetRow?.primaryArea || "").trim();
  const impactedAreaIds = Array.isArray(item?.impactedAreaIds) ? item.impactedAreaIds.filter(Boolean) : [];
  const debugBase = {
    openItemId: String(item?.id || "").trim(),
    ownerRawValue,
    normalizedOwner,
    primaryArea: ownerAreaId,
    impactedAreas: impactedAreaIds,
    emailMode: getEmailModeStatus().mode,
    overrideConfigured: Boolean(PRESESSION_EMAIL_OVERRIDE),
    availableOwnerMappingKeys: Array.from(contactsDirectory.byAlias.keys()).slice(0, 40),
  };
  const buildResult = (email, source, extra = {}) => ({
    email: normalizeEmail(email),
    source,
    ownerName: String(extra.ownerName || item?.ownerName || ownerRawValue || "").trim(),
    contact: extra.contact || null,
    debug: {
      ...debugBase,
      matchedSource: source,
      matchedValue: extra.matchedValue || "",
    },
  });

  const directEmail = extractEmailAddress(item?.ownerEmail) || extractEmailAddress(ownerRawValue);
  if (directEmail) {
    return buildResult(directEmail, "open-item-email", {
      matchedValue: item?.ownerEmail || ownerRawValue,
      contact: resolveContactByValue(directEmail, contactsDirectory),
    });
  }

  const employeeContact = resolveContactByValue(item?.ownerName || ownerRawValue, contactsDirectory);
  if (employeeContact?.email) {
    return buildResult(employeeContact.email, employeeContact.source === "ccb-areas" ? "area-owner-name" : "employee-directory-name", {
      ownerName: employeeContact.displayName || employeeContact.name || item?.ownerName || "",
      matchedValue: employeeContact.name || employeeContact.displayName || "",
      contact: employeeContact,
    });
  }

  const ownerAreaByName = areas.find((area) => normalizePersonName(area.owner) === normalizedOwner && normalizeEmail(area.email));
  if (ownerAreaByName?.email) {
    return buildResult(ownerAreaByName.email, "area-owner-name", {
      ownerName: ownerAreaByName.owner || item?.ownerName || "",
      matchedValue: ownerAreaByName.owner || ownerAreaByName.name || ownerAreaByName.id || "",
      contact: ownerAreaByName,
    });
  }

  const primaryArea = areas.find((area) => area.id === ownerAreaId || normalizePersonName(area.name) === normalizePersonName(ownerAreaId));
  if (primaryArea?.email) {
    return buildResult(primaryArea.email, "primary-area-owner-email", {
      ownerName: primaryArea.owner || item?.ownerName || "",
      matchedValue: primaryArea.id || primaryArea.name || "",
      contact: primaryArea,
    });
  }

  if (impactedAreaIds.length === 1) {
    const impactedArea = areas.find((area) => area.id === impactedAreaIds[0]);
    if (impactedArea?.email) {
      return buildResult(impactedArea.email, "single-impacted-area-owner-email", {
        ownerName: impactedArea.owner || item?.ownerName || "",
        matchedValue: impactedArea.id || impactedArea.name || "",
        contact: impactedArea,
      });
    }
  }

  console.warn("[owner-email-resolution-failed]", JSON.stringify(debugBase));
  return {
    email: "",
    source: "",
    ownerName: String(item?.ownerName || ownerRawValue || "").trim(),
    contact: null,
    debug: {
      ...debugBase,
      matchedSource: "",
      matchedValue: "",
    },
  };
}

function formatImplementationFieldValue(value) {
  const text = String(value || "").trim();
  return text || "Not specified";
}

function formatEmailDateOnly(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "Not specified";
  }
  const match = text.match(/^(\d{4}-\d{2}-\d{2})/);
  if (match) {
    return match[1];
  }
  const reverseMatch = text.match(/^(\d{2})\/(\d{2})\/(\d{4})/);
  if (reverseMatch) {
    return `${reverseMatch[3]}-${reverseMatch[2]}-${reverseMatch[1]}`;
  }
  return text;
}

function buildImplementationUpdateSummary(before = {}, after = {}) {
  const fieldDefs = [
    ["status", "Status"],
    ["dateCreated", "Date Created"],
    ["dueDate", "Due Date"],
    ["comments", "Comments/Updates"],
    ["minutesRelated", "Minutes Related"],
    ["gitRepository", "Git Repository"],
    ["jiraTicketsRelated", "Jira Tickets Related"],
    ["branchLocation", "Branch / Stream / Release Location"],
    ["implementationNotes", "Implementation Notes"],
  ];

  return fieldDefs.reduce((changes, [key, label]) => {
    const beforeValue = String(before[key] || "").trim();
    const afterValue = String(after[key] || "").trim();
    if (beforeValue === afterValue) {
      return changes;
    }
    changes.push({
      key,
      label,
      before: beforeValue,
      after: afterValue,
    });
    return changes;
  }, []);
}

function buildImplementationAssignmentEmailSubject(item) {
  return `CCB Open Item Assigned for Implementation: ${item.id} - ${item.title}`;
}

function buildImplementationUpdateEmailSubject(item) {
  return `CCB Open Item Updated: ${item.id} - ${item.title}`;
}

function buildImplementationAssignedEmailBody(item, trackingData, owner, ticketLink, emailDelivery = {}, state = null) {
  const intendedRecipient = emailDelivery.intendedRecipient || "";
  const impactedAreaNames = (item.impactedAreaIds || [])
    .map((areaId) => state?.areas?.find((area) => area.id === areaId)?.name || areaId)
    .filter(Boolean);
  const lines = [
    `Hello ${owner?.name || item.ownerName || intendedRecipient || "Owner"},`,
    "",
    "The following CCB-approved Open Item has been assigned to you for implementation tracking.",
    "",
    `Open Item: ${item.id} - ${item.title}`,
    `Owner: ${item.ownerName || "Not specified"}`,
    `CCB Status: ${item.ccbDecisionRecommendation || "APPROVE"}`,
    `CCB Score: ${item.ccbDecisionAverageScore == null ? "Not specified" : Number(item.ccbDecisionAverageScore).toFixed(2)}`,
    `Implementation Status: ${formatImplementationFieldValue(trackingData.status)}`,
    `Impacted Areas: ${impactedAreaNames.join(", ") || "Not specified"}`,
    "",
    `Date Created: ${formatEmailDateOnly(trackingData.dateCreated)}`,
    `Due Date: ${formatEmailDateOnly(trackingData.dueDate)}`,
    `Jira Ticket: ${formatImplementationFieldValue(trackingData.jiraTicketsRelated)}`,
    `Git Repository: ${formatImplementationFieldValue(trackingData.gitRepository)}`,
    `Branch / Stream: ${formatImplementationFieldValue(trackingData.branchLocation)}`,
  ];

  if (String(trackingData.comments || "").trim()) {
    lines.push(`Comments / Updates: ${trackingData.comments}`);
  }
  if (String(trackingData.implementationNotes || "").trim()) {
    lines.push(`Implementation Notes: ${trackingData.implementationNotes}`);
  }
  if (emailDelivery.footerText) {
    lines.push("", emailDelivery.footerText);
  }
  lines.push("", "Review the assigned item here:", ticketLink);
  return lines.join("\n");
}

function buildImplementationUpdatedEmailBody(item, changes, afterTrackingData, owner, ticketLink, emailDelivery = {}) {
  const intendedRecipient = emailDelivery.intendedRecipient || "";
  const lines = [
    `Hello ${owner?.name || item.ownerName || intendedRecipient || "Owner"},`,
    "",
    "The following implementation tracking fields were updated:",
    "",
    ...changes.map((change) => {
      const beforeValue = /date/i.test(change.label) ? formatEmailDateOnly(change.before) : formatImplementationFieldValue(change.before);
      const afterValue = /date/i.test(change.label) ? formatEmailDateOnly(change.after) : formatImplementationFieldValue(change.after);
      return `- ${change.label}: ${beforeValue} -> ${afterValue}`;
    }),
    "",
    `Open Item: ${item.id} - ${item.title}`,
    `Current implementation status: ${formatImplementationFieldValue(afterTrackingData.status)}`,
  ];
  if (emailDelivery.footerText) {
    lines.push("", emailDelivery.footerText);
  }
  lines.push("", "Review the item:", ticketLink);
  return lines.join("\n");
}

function buildImplementationAssignedEmailHtml(item, trackingData, owner, ticketLink, emailDelivery = {}, state = null) {
  const intendedRecipient = emailDelivery.intendedRecipient || "";
  const ownerName = owner?.name || item.ownerName || intendedRecipient || "Owner";
  const impactedAreaNames = (item.impactedAreaIds || [])
    .map((areaId) => state?.areas?.find((area) => area.id === areaId)?.name || areaId)
    .filter(Boolean);
  const sansStack = "Inter, 'Avenir Next', 'Helvetica Neue', Arial, sans-serif";
  const introHtml = `Hello ${escapeHtmlEmail(ownerName)},<br/>The following CCB-approved Open Item has been assigned to you for implementation tracking.`;
  const bodyHtml = `
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin-bottom:6px;">
      <tr>
        <td style="padding:16px 16px;border:1px solid #EDF1F5;border-radius:14px;background:#FBFCFD;">
          <div style="font-family:${sansStack};font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:#7A8798;margin-bottom:6px;">${escapeHtmlEmail(item.id)}</div>
          <div style="font-family:${sansStack};font-size:22px;line-height:1.25;color:#0F172A;font-weight:700;margin-bottom:10px;">${escapeHtmlEmail(item.title)}</div>
          <div style="margin-bottom:12px;">
            <span style="display:inline-block;padding:6px 10px;border-radius:999px;background:#F3F4F6;color:#334155;font-family:${sansStack};font-size:11px;font-weight:600;margin-right:6px;">${escapeHtmlEmail(item.ccbDecisionRecommendation || "APPROVE")}</span>
            <span style="display:inline-block;padding:6px 10px;border-radius:999px;background:#FFF7ED;color:#B86A1D;font-family:${sansStack};font-size:11px;font-weight:600;margin-right:6px;">Score ${escapeHtmlEmail(item.ccbDecisionAverageScore == null ? "N/A" : Number(item.ccbDecisionAverageScore).toFixed(2))}</span>
            <span style="display:inline-block;padding:6px 10px;border-radius:999px;background:#F8FAFC;color:#475569;font-family:${sansStack};font-size:11px;font-weight:600;">${escapeHtmlEmail(trackingData.status || "OPEN")}</span>
          </div>
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="font-family:${sansStack};font-size:14px;line-height:1.55;color:#334155;">
            <tr><td style="padding:3px 0;width:140px;color:#6B7280;">Owner</td><td style="padding:3px 0;">${escapeHtmlEmail(item.ownerName || "Not specified")}</td></tr>
            <tr><td style="padding:3px 0;width:140px;color:#6B7280;">Impacted Areas</td><td style="padding:3px 0;">${escapeHtmlEmail(impactedAreaNames.join(", ") || "Not specified")}</td></tr>
            <tr><td style="padding:3px 0;width:140px;color:#6B7280;">Date Created</td><td style="padding:3px 0;">${escapeHtmlEmail(formatEmailDateOnly(trackingData.dateCreated))}</td></tr>
            <tr><td style="padding:3px 0;width:140px;color:#6B7280;">Due Date</td><td style="padding:3px 0;">${escapeHtmlEmail(formatEmailDateOnly(trackingData.dueDate))}</td></tr>
            <tr><td style="padding:3px 0;width:140px;color:#6B7280;">Jira Ticket</td><td style="padding:3px 0;">${escapeHtmlEmail(formatImplementationFieldValue(trackingData.jiraTicketsRelated))}</td></tr>
            <tr><td style="padding:3px 0;width:140px;color:#6B7280;">Git Repository</td><td style="padding:3px 0;">${escapeHtmlEmail(formatImplementationFieldValue(trackingData.gitRepository))}</td></tr>
            <tr><td style="padding:3px 0;width:140px;color:#6B7280;">Branch / Stream</td><td style="padding:3px 0;">${escapeHtmlEmail(formatImplementationFieldValue(trackingData.branchLocation))}</td></tr>
            ${String(trackingData.comments || "").trim() ? `<tr><td style="padding:3px 0;width:140px;color:#6B7280;">Comments</td><td style="padding:3px 0;">${escapeHtmlEmail(trackingData.comments)}</td></tr>` : ""}
            ${String(trackingData.implementationNotes || "").trim() ? `<tr><td style="padding:3px 0;width:140px;color:#6B7280;">Implementation Notes</td><td style="padding:3px 0;">${escapeHtmlEmail(trackingData.implementationNotes)}</td></tr>` : ""}
          </table>
        </td>
      </tr>
    </table>`;
  return renderEmailShell({
    title: "Open Item Assigned for Implementation",
    eyebrow: "Implementation Tracking",
    introHtml,
    bodyHtml,
    ctaHref: ticketLink,
    ctaLabel: "Review Open Item",
    footerHtml: emailDelivery.footerHtml || "",
  });
}

function buildImplementationUpdatedEmailHtml(item, changes, afterTrackingData, owner, ticketLink, emailDelivery = {}) {
  const intendedRecipient = emailDelivery.intendedRecipient || "";
  const ownerName = owner?.name || item.ownerName || intendedRecipient || "Owner";
  const sansStack = "Inter, 'Avenir Next', 'Helvetica Neue', Arial, sans-serif";
  const changeRows = changes.map((change) => {
    const beforeValue = /date/i.test(change.label) ? formatEmailDateOnly(change.before) : formatImplementationFieldValue(change.before);
    const afterValue = /date/i.test(change.label) ? formatEmailDateOnly(change.after) : formatImplementationFieldValue(change.after);
    return `
      <tr>
        <td style="padding:10px 0;border-top:1px solid #EEF2F6;">
          <div style="font-family:${sansStack};font-size:12px;color:#6B7280;margin-bottom:3px;">${escapeHtmlEmail(change.label)}</div>
          <div style="font-family:${sansStack};font-size:14px;color:#334155;">From: ${escapeHtmlEmail(beforeValue)}</div>
          <div style="font-family:${sansStack};font-size:14px;color:#0F172A;font-weight:600;">To: ${escapeHtmlEmail(afterValue)}</div>
        </td>
      </tr>
    `;
  }).join("");
  const introHtml = `Hello ${escapeHtmlEmail(ownerName)},<br/>The implementation tracking record for this CCB-approved Open Item was updated.`;
  const bodyHtml = `
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin-bottom:6px;">
      <tr>
        <td style="padding:16px 16px;border:1px solid #EDF1F5;border-radius:14px;background:#FBFCFD;">
          <div style="font-family:${sansStack};font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:#7A8798;margin-bottom:6px;">${escapeHtmlEmail(item.id)}</div>
          <div style="font-family:${sansStack};font-size:22px;line-height:1.25;color:#0F172A;font-weight:700;margin-bottom:10px;">${escapeHtmlEmail(item.title)}</div>
          <div style="margin-bottom:12px;">
            <span style="display:inline-block;padding:6px 10px;border-radius:999px;background:#F3F4F6;color:#334155;font-family:${sansStack};font-size:11px;font-weight:600;margin-right:6px;">${escapeHtmlEmail(item.ccbDecisionRecommendation || "APPROVE")}</span>
            <span style="display:inline-block;padding:6px 10px;border-radius:999px;background:#FFF7ED;color:#B86A1D;font-family:${sansStack};font-size:11px;font-weight:600;margin-right:6px;">Score ${escapeHtmlEmail(item.ccbDecisionAverageScore == null ? "N/A" : Number(item.ccbDecisionAverageScore).toFixed(2))}</span>
            <span style="display:inline-block;padding:6px 10px;border-radius:999px;background:#F8FAFC;color:#475569;font-family:${sansStack};font-size:11px;font-weight:600;">${escapeHtmlEmail(formatImplementationFieldValue(afterTrackingData.status))}</span>
          </div>
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="width:100%;">
            ${changeRows}
          </table>
        </td>
      </tr>
    </table>`;
  return renderEmailShell({
    title: "Implementation Tracking Updated",
    eyebrow: "Implementation Tracking",
    introHtml,
    bodyHtml,
    ctaHref: ticketLink,
    ctaLabel: "Review Open Item",
    footerHtml: emailDelivery.footerHtml || "",
  });
}

function buildPreSessionEmailBody(group, emailDelivery = {}) {
  const appUrl = buildAppDeepLink({ tab: "presession" });
  const lines = [
    `Hello ${group.ownerName || group.ownerEmail},`,
    "",
    "You have pending CCB pre-session reviews for the following Open Items where your area is impacted.",
    "Open the app and confirm whether each Open Item impacts your area.",
    "",
    ...group.pendingItems.flatMap((item) => {
      const reviewUrl = buildAppDeepLink({
        tab: "presession",
        openItemId: item.openItemId,
        areaId: item.areaId,
      });
      return [
        `- ${item.openItemId} | ${item.title} | Area: ${item.areaName}${item.externalStatus ? ` | Current CCB Status: ${item.externalStatus}` : ""}`,
        `  Direct review: ${reviewUrl}`,
      ];
    }),
    "",
    `Dashboard: ${appUrl}`,
  ];

  if (emailDelivery.footerText) {
    lines.push("", emailDelivery.footerText);
  }

  return lines.join("\n");
}

function escapeHtmlEmail(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function buildPreSessionEmailHtml(group, emailDelivery = {}) {
  const ownerName = group.ownerName || group.ownerEmail || "Owner";
  const dashboardUrl = buildAppDeepLink({ tab: "presession" });
  const introCount = `${group.pendingCount} open item${group.pendingCount === 1 ? "" : "s"}`;
  const sansStack = "Inter, 'Avenir Next', 'Helvetica Neue', Arial, sans-serif";

  const itemCards = group.pendingItems.map((item) => {
    const reviewUrl = buildAppDeepLink({
      tab: "presession",
      openItemId: item.openItemId,
      areaId: item.areaId,
    });
    return `
      <tr>
        <td style="padding:0 0 10px 0;">
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border:1px solid #EDF1F5;border-radius:14px;background:#FBFCFD;">
            <tr>
              <td style="padding:14px 16px;">
                <div style="font-family:${sansStack};font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:#7A8798;margin-bottom:6px;">
                  ${escapeHtmlEmail(item.openItemId)} · ${escapeHtmlEmail(item.areaName)}
                </div>
                <div style="font-family:${sansStack};font-size:18px;line-height:1.3;color:#0F172A;font-weight:700;margin-bottom:8px;">
                  ${escapeHtmlEmail(item.title)}
                </div>
                <div style="font-family:${sansStack};font-size:14px;line-height:1.6;color:#334155;margin-bottom:10px;">
                  Your area needs to confirm whether this Open Item impacts it.
                  ${item.externalStatus ? `Current CCB Status: <strong>${escapeHtmlEmail(item.externalStatus)}</strong>.` : ""}
                </div>
                <div>
                  <span style="display:inline-block;padding:6px 10px;border-radius:999px;background:#F8FAFC;color:#475569;font-family:${sansStack};font-size:11px;font-weight:600;text-transform:uppercase;">
                    Area: ${escapeHtmlEmail(item.areaName)}
                  </span>
                </div>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    `;
  }).join("");

  const introHtml = `Hello ${escapeHtmlEmail(ownerName)},<br/>Please review <strong>${escapeHtmlEmail(introCount)}</strong> and confirm whether each Open Item impacts your area.`;
  const bodyHtml = `
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin-bottom:12px;">
      <tr>
        <td style="padding:14px 16px;border:1px solid #EDF1F5;border-radius:14px;background:#FBFCFD;">
          <div style="font-family:${sansStack};font-size:12px;color:#6B7280;margin-bottom:6px;">Pending areas</div>
          <div style="height:2px;background:linear-gradient(90deg,#EC9435,#F2D1A0);border-radius:999px;margin:0 0 10px 0;"></div>
          <div style="font-family:${sansStack};font-size:16px;line-height:1.4;color:#0F172A;font-weight:600;">${escapeHtmlEmail(group.areaNames.join(", "))}</div>
        </td>
      </tr>
    </table>
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
      ${itemCards}
    </table>
    <div style="font-family:${sansStack};font-size:13px;line-height:1.6;color:#6B7280;margin-top:8px;">
      The link opens the pre-session review tab and, when possible, focuses the specific ticket for your area.
    </div>`;
  return renderEmailShell({
    title: "Pre-Session Review Request",
    eyebrow: "Impact Review",
    introHtml,
    bodyHtml,
    ctaHref: dashboardUrl,
    ctaLabel: "Review Open Item",
    footerHtml: emailDelivery.footerHtml || "",
  });
}

function toBase64Url(value) {
  return Buffer.from(value, "utf8")
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

async function sendGmailMessage({ to, subject, text, html = "" }) {
  if (getGoogleSheetsStorageMode() !== "user-oauth") {
    throw new Error("Email delivery requires a connected user Google OAuth session.");
  }

  const mixedBoundary = `ccb_mixed_${Date.now().toString(36)}`;
  const relatedBoundary = `ccb_related_${Date.now().toString(36)}`;
  const alternativeBoundary = `ccb_alt_${Date.now().toString(36)}`;
  const url = "https://gmail.googleapis.com/gmail/v1/users/me/messages/send";
  const headers = await getGoogleRequestHeaders(url);
  const logoAttachment = getEmailLogoAttachment();
  const attachments = logoAttachment ? [logoAttachment] : [];
  console.log("[email-logo]", JSON.stringify({
    to,
    subject,
    attachments: attachments.map((attachment) => ({
      filename: attachment.filename,
      cid: attachment.cid,
      exists: fs.existsSync(attachment.path),
    })),
  }));

  const messageLines = [
    `Content-Type: multipart/mixed; boundary="${mixedBoundary}"`,
    "MIME-Version: 1.0",
    `To: ${to}`,
    `Subject: ${subject}`,
    "",
    `--${mixedBoundary}`,
    `Content-Type: multipart/related; boundary="${relatedBoundary}"`,
    "",
    `--${relatedBoundary}`,
    `Content-Type: multipart/alternative; boundary="${alternativeBoundary}"`,
    "",
    `--${alternativeBoundary}`,
    "Content-Type: text/plain; charset=UTF-8",
    "Content-Transfer-Encoding: 7bit",
    "",
    text,
    "",
    `--${alternativeBoundary}`,
    "Content-Type: text/html; charset=UTF-8",
    "Content-Transfer-Encoding: 7bit",
    "",
    html || `<pre>${escapeHtmlEmail(text)}</pre>`,
    "",
    `--${alternativeBoundary}--`,
  ];

  attachments.forEach((attachment) => {
    const content = fs.readFileSync(attachment.path).toString("base64");
    messageLines.push(
      `--${relatedBoundary}`,
      `Content-Type: ${attachment.contentType || "application/octet-stream"}; name="${attachment.filename}"`,
      `Content-Disposition: inline; filename="${attachment.filename}"`,
      "Content-Transfer-Encoding: base64",
      `Content-ID: <${attachment.cid}>`,
      "",
      content,
      "",
    );
  });

  messageLines.push(
    `--${relatedBoundary}--`,
    "",
    `--${mixedBoundary}--`,
  );

  const rawMessage = messageLines.join("\r\n");

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

function getImplementationTrackingDataFromItem(item) {
  return {
    status: normalizeImplementationStatus(item.implementationStatus || item.rawSheetRow?.Status || ""),
    dateCreated: String(item.createdAt || "").trim(),
    dueDate: String(item.dueDate || "").trim(),
    comments: String(item.description || "").trim(),
    minutesRelated: String(item.minutesRelated || "").trim(),
    gitRepository: String(item.gitRepository || "").trim(),
    jiraTicketsRelated: String(item.jiraTicketsRelated || "").trim(),
    branchLocation: String(item.branchLocation || "").trim(),
    implementationNotes: String(item.implementationNotes || "").trim(),
  };
}

async function sendImplementationAssignedEmail(item, trackingData, owner, ticketLink, state) {
  const intendedRecipient = normalizeEmail(owner?.email || "");
  const emailDelivery = resolveNotificationRecipient(intendedRecipient);
  if (!emailDelivery.ok) {
    return {
      status: emailDelivery.status,
      attempted: true,
      sent: false,
      mode: emailDelivery.mode,
      recipient: emailDelivery.deliveryTarget,
      intendedRecipient,
      ownerEmail: intendedRecipient,
      deliveryTarget: emailDelivery.deliveryTarget,
      reason: emailDelivery.error || "",
      error: emailDelivery.error || "",
    };
  }
  await sendGmailMessage({
    to: emailDelivery.deliveryTarget,
    subject: buildImplementationAssignmentEmailSubject(item),
    text: buildImplementationAssignedEmailBody(item, trackingData, owner, ticketLink, emailDelivery, state),
    html: buildImplementationAssignedEmailHtml(item, trackingData, owner, ticketLink, emailDelivery, state),
  });
  return {
    status: emailDelivery.status,
    attempted: true,
    sent: true,
    mode: emailDelivery.mode,
    recipient: emailDelivery.deliveryTarget,
    intendedRecipient,
    ownerEmail: intendedRecipient,
    deliveryTarget: emailDelivery.deliveryTarget,
    reason: owner?.source || emailDelivery.mode,
  };
}

async function sendImplementationUpdatedEmail(item, beforeTrackingData, afterTrackingData, owner, ticketLink) {
  const changes = buildImplementationUpdateSummary(beforeTrackingData, afterTrackingData);
  if (!changes.length) {
    return { status: "no-changes", attempted: true, sent: false, reason: "No tracking changes to notify." };
  }
  const intendedRecipient = normalizeEmail(owner?.email || "");
  const emailDelivery = resolveNotificationRecipient(intendedRecipient);
  if (!emailDelivery.ok) {
    return {
      status: emailDelivery.status,
      attempted: true,
      sent: false,
      mode: emailDelivery.mode,
      recipient: emailDelivery.deliveryTarget,
      intendedRecipient,
      ownerEmail: intendedRecipient,
      deliveryTarget: emailDelivery.deliveryTarget,
      changes,
      reason: emailDelivery.error || "",
      error: emailDelivery.error || "",
    };
  }
  await sendGmailMessage({
    to: emailDelivery.deliveryTarget,
    subject: buildImplementationUpdateEmailSubject(item),
    text: buildImplementationUpdatedEmailBody(item, changes, afterTrackingData, owner, ticketLink, emailDelivery),
    html: buildImplementationUpdatedEmailHtml(item, changes, afterTrackingData, owner, ticketLink, emailDelivery),
  });
  return {
    status: emailDelivery.status,
    attempted: true,
    sent: true,
    mode: emailDelivery.mode,
    recipient: emailDelivery.deliveryTarget,
    intendedRecipient,
    ownerEmail: intendedRecipient,
    deliveryTarget: emailDelivery.deliveryTarget,
    changes,
    reason: owner?.source || emailDelivery.mode,
  };
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
    const emailDelivery = resolveNotificationRecipient(group.ownerEmail);
    const body = buildPreSessionEmailBody(group, emailDelivery);
    const htmlBody = buildPreSessionEmailHtml(group, emailDelivery);
    const deliveryTarget = emailDelivery.deliveryTarget;
    let deliveryMode = "preview";
    let sent = false;
    let errorMessage = emailDelivery.ok ? "" : (emailDelivery.error || "");

    try {
      if (emailDelivery.ok && getGoogleSheetsStorageMode() === "user-oauth" && isUserGoogleOAuthConnected()) {
        await sendGmailMessage({
          to: deliveryTarget,
          subject,
          text: body,
          html: htmlBody,
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
      deliveryTarget,
      ownerName: group.ownerName,
      pendingCount: group.pendingCount,
      deliveryMode,
      sent,
      error: errorMessage,
      recipientMode: emailDelivery.mode,
      recipientStatus: emailDelivery.status,
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

function getEvaluationForUser(openItemId, evaluatorEmail, state) {
  const normalizedEmail = normalizeEmail(evaluatorEmail);
  return (state.ccbEvaluations || []).filter((evaluation) => (
    evaluation.openItemId === openItemId && normalizeEmail(evaluation.evaluatorEmail) === normalizedEmail
  ));
}

async function saveCcbEvaluation(openItemId, evaluator, evaluationInput) {
  const state = await loadStateStore();
  const item = (state.openItems || []).find((candidate) => candidate.id === openItemId);
  if (!item) {
    throw new Error("Open item no encontrado.");
  }

  const evaluatorEmail = normalizeEmail(evaluator?.email);
  if (!evaluatorEmail) {
    throw new Error("No encontramos el correo del evaluador.");
  }
  const resolvedEvaluatorArea = String(
    evaluator?.area ||
    state.areas.find((area) => normalizeEmail(area.email) === evaluatorEmail)?.id ||
    ""
  ).trim();

  const criteria = Array.isArray(state.ccbDecisionCriteria) && state.ccbDecisionCriteria.length
    ? state.ccbDecisionCriteria
    : getDefaultCcbDecisionCriteria();
  const submittedEntries = Array.isArray(evaluationInput?.criteria) ? evaluationInput.criteria : [];
  const criterionMap = new Map(criteria.map((criterion) => [criterion.id, criterion]));
  const nextEntries = criteria.map((criterion) => {
    const submitted = submittedEntries.find((entry) => String(entry.criterionId || "").trim() === criterion.id) || {};
    return {
      openItemId,
      evaluatorEmail,
      evaluatorName: String(evaluator?.name || "").trim(),
      evaluatorArea: resolvedEvaluatorArea,
      criterionId: criterion.id,
      criterionName: criterion.name,
      score: Number.parseInt(String(submitted.score || "").trim(), 10) || 0,
      weight: Number(criterion.weight || 0),
      rationale: String(submitted.rationale || "").trim(),
      supportingReference: String(submitted.supportingReference || "").trim(),
      createdAt: "",
      updatedAt: "",
    };
  });

  const warnings = getEvaluationWarnings(item, nextEntries, criteria).filter((message) => !message.includes("Fast-track candidate."));
  const blockingWarnings = warnings.filter((message) => (
    message.includes("Please select a score") ||
    message.includes("Please provide a decision justification")
  ));
  if (blockingWarnings.length) {
    throw new Error(blockingWarnings[0]);
  }

  const now = new Date().toISOString();
  const existingEntries = getEvaluationForUser(openItemId, evaluatorEmail, state);
  const existingByCriterion = new Map(existingEntries.map((entry) => [entry.criterionId, entry]));
  const preserved = (state.ccbEvaluations || []).filter((entry) => !(
    entry.openItemId === openItemId && normalizeEmail(entry.evaluatorEmail) === evaluatorEmail
  ));

  const normalizedEntries = nextEntries.map((entry) => ({
    ...entry,
    createdAt: existingByCriterion.get(entry.criterionId)?.createdAt || now,
    updatedAt: now,
  }));

  state.ccbEvaluations = [...preserved, ...normalizedEntries];
  const aggregatedDecision = calculateOpenItemCcbDecisionAverages(openItemId, state, criteria);
  const tracking = deriveOpenItemTracking(state, item);
  item.impactedAreaIds = tracking.impactedAreaIds;
  item.impactedAreas = tracking.impactedAreas;
  item.impactedUsers = tracking.impactedUsers;
  item.evaluationParticipants = tracking.evaluationParticipants;
  item.voteParticipants = tracking.voteParticipants;
  item.pendingEvaluators = tracking.pendingEvaluators;
  item.lastEvaluationDate = tracking.lastEvaluationDate;
  updateOpenItemCcbDecision(openItemId, aggregatedDecision, state);
  const nextState = await persistEvaluationStateStore(state);
  return {
    state: nextState,
    evaluation: normalizedEntries,
    weightedScore: calculateWeightedScore(normalizedEntries, criteria),
    recommendation: getDecisionRecommendation(calculateWeightedScore(normalizedEntries, criteria)),
    aggregatedDecision,
    warnings: getEvaluationWarnings(item, normalizedEntries, criteria),
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
    throw new Error("Could not find the employee CSV header row.");
  }

  const headerRow = rows[headerIndex];
  const bodyRows = rows.slice(headerIndex + 1);
  const emailIndex = findEmployeeEmailColumnIndex(headerRow);
  if (emailIndex === -1) {
    throw new Error("Could not find a valid email column in the CSV. A column such as Email or Email Address is required.");
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
    throw new Error(`Could not detect the header row for ${SOURCE_SHEET_NAME}. Restore the sheet so it includes a row with 'ID' and 'Open Item Description'.`);
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
    throw new Error(`Could not detect the table for ${CCB_DECISION_SHEET_NAME}. A row with 'OI_ID' and 'Open Item Description' must exist.`);
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

function findExistingHeader(headerRow, aliases = []) {
  const normalizedRow = (Array.isArray(headerRow) ? headerRow : []).map((header) => String(header || "").trim());
  for (const alias of aliases) {
    const aliasValue = String(alias || "").trim();
    const index = normalizedRow.findIndex((header) => header.toLowerCase() === aliasValue.toLowerCase());
    if (index >= 0) {
      return normalizedRow[index];
    }
  }
  return "";
}

function buildImplementationTrackingHeaderMap(headerRow) {
  return Object.fromEntries(
    Object.entries(IMPLEMENTATION_TRACKING_COLUMN_ALIASES).map(([key, aliases]) => [key, findExistingHeader(headerRow, aliases)]),
  );
}

function getApprovedVoteAreaIds(item) {
  return new Set((item.votes || []).filter((vote) => vote.decision === "approve").map((vote) => vote.areaId));
}

function getPendingAreaIds(item) {
  const approvedAreaIds = getApprovedVoteAreaIds(item);
  return (item.impactedAreaIds || []).filter((areaId) => !approvedAreaIds.has(areaId));
}

function getOpenItemApprovalDate(item) {
  return String(
    item.implementationApprovalDate ||
    item.ccbDecisionLastUpdated ||
    item.lastEvaluationDate ||
    ""
  ).trim();
}

function isCcbApproved(item) {
  const recommendation = String(item.ccbDecisionRecommendation || "").trim().toUpperCase();
  const averageScore = Number(item.ccbDecisionAverageScore || 0);
  const evaluatorCount = Number(item.ccbDecisionEvaluatorCount || 0);
  return recommendation === "APPROVE" && averageScore > 0 && evaluatorCount > 0;
}

function hasCompletedImpactedApprovals(item) {
  const impactedAreaIds = Array.isArray(item.impactedAreaIds) ? item.impactedAreaIds.filter(Boolean) : [];
  if (!impactedAreaIds.length) {
    return false;
  }
  return getPendingAreaIds(item).length === 0;
}

function hasImplementationTrackingSheetSignals(item) {
  const trackingSignals = [
    item.createdAt,
    item.dueDate,
    item.description,
    item.jiraTicketsRelated,
    item.gitRepository,
    item.minutesRelated,
    item.branchLocation,
    item.implementationNotes,
  ];
  return trackingSignals.some((value) => Boolean(String(value || "").trim()));
}

function getImplementationTrackingStatusValue(item) {
  return normalizeImplementationStatus(item.implementationStatus || item.rawSheetRow?.Status || "");
}

function hasImplementationTracking(item) {
  return hasImplementationTrackingSheetSignals(item) || Boolean(String(item.implementationTrackingCreatedAt || "").trim());
}

function isOpenItemReadyForImplementation(item) {
  return isCcbApproved(item) && hasCompletedImpactedApprovals(item) && !hasImplementationTracking(item);
}

function isOpenItemImplementationActive(item) {
  return isCcbApproved(item) && hasCompletedImpactedApprovals(item) && hasImplementationTracking(item);
}

async function updateOpenItemImplementationTrackingSheetRow(openItemId, updates = {}) {
  const valuesMap = await batchGetSpreadsheetValues([SOURCE_SHEET_NAME]);
  const existingValues = valuesMap.get(SOURCE_SHEET_NAME) || [];
  const context = getOpenItemSheetContext(existingValues);
  const headerMap = buildImplementationTrackingHeaderMap(context.headerRow);
  const idColumnIndex = context.headerRow.findIndex((header) => String(header || "").trim() === "ID");
  if (idColumnIndex < 0) {
    throw new Error("Could not find the ID column in Open Item List.");
  }

  const rowIndex = context.bodyRows.findIndex((row) => String(row[idColumnIndex] || "").trim() === openItemId);
  if (rowIndex < 0) {
    throw new Error("Could not find the Open Item row in Open Item List.");
  }

  const rowNumber = context.headerRowNumber + 1 + rowIndex;
  const writes = [];
  const pushWrite = (fieldKey, value) => {
    const header = headerMap[fieldKey];
    if (!header) {
      return;
    }
    const columnIndex = context.headerRow.findIndex((entry) => String(entry || "").trim() === header);
    if (columnIndex < 0) {
      return;
    }
    const column = toA1Column(columnIndex);
    writes.push(writeSheetRange(`${SOURCE_SHEET_NAME}!${column}${rowNumber}`, [[value]], "USER_ENTERED"));
  };

  [
    "dateCreated",
    "dueDate",
    "status",
    "comments",
    "minutesRelated",
    "gitRepository",
    "jiraTicketsRelated",
    "branchLocation",
    "implementationNotes",
  ].forEach((fieldKey) => {
    if (Object.prototype.hasOwnProperty.call(updates, fieldKey)) {
      pushWrite(fieldKey, updates[fieldKey]);
    }
  });

  await Promise.all(writes);
}

function buildOpenItemHeaders(state, areas) {
  const existingHeaders = stripOpenItemDecisionHeaders(
    Array.isArray(state.source?.sheetHeaders) && state.source.sheetHeaders.length
      ? state.source.sheetHeaders
      : [],
  );

  if (existingHeaders.length) {
    return existingHeaders;
  }

  return stripOpenItemDecisionHeaders(OPEN_ITEM_HEADERS);
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
    "impactedUsers",
    "impactedAreas",
    "evaluationParticipants",
    "voteParticipants",
    "pendingEvaluators",
    "lastEvaluationDate",
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
    "CCB Decision Strategic Alignment Avg",
    "CCB Decision Risk Assessment Avg",
    "CCB Decision Resource Impact Avg",
    "CCB Decision Customer Value Avg",
    "CCB Decision Operational Feasibility Avg",
    "Strategic Alignment Avg",
    "Risk Assessment Avg",
    "Resource Impact Avg",
    "Customer Value Avg",
    "Operational Feasibility Avg",
    "Strategic Alignment Score",
    "Risk Assessment Score",
    "Resource Impact Score",
    "Customer Value Score",
    "Operational Feasibility Score",
    "CCB Decision Average Score",
    "CCB Evaluation Avg",
    "CCB Eval Avg",
    "CCB Ev",
    "Total",
    "CCB Decision Recommendation",
    "CCB Decision Recommend",
    "Recommendation",
    "Status",
    "CCB Decision Evaluator Count",
    "CCB Evaluator Count",
    "CCB Ev Count",
    "Evaluator Count",
    "CCB Decision Evaluators",
    "Evaluators",
    "CCB Decision Last Updated",
    "CCB Last Updated",
    "Last Updated",
    "Updated At",
  ]);
}

async function saveOpenItemVote(openItemId, user, input) {
  const state = await loadStateStore();
  const item = (state.openItems || []).find((candidate) => candidate.id === openItemId);
  if (!item) {
    throw new Error("Open item no encontrado.");
  }

  const email = normalizeEmail(user?.email);
  if (!email) {
    throw new Error("No encontramos el correo del usuario.");
  }

  const isAdmin = isAdminUser(user);
  const mappedArea = String(
    state.areas.find((area) => normalizeEmail(area.email) === email)?.id || ""
  ).trim();
  const selectedArea = String(input?.areaId || "").trim();
  const effectiveArea = isAdmin ? selectedArea : mappedArea;

  if (!isAdmin && !mappedArea) {
    throw new Error("You are not assigned as an owner for a CCB area.");
  }
  if (!effectiveArea) {
    throw new Error("No area selected for voting.");
  }
  if (!isAdmin && !(item.impactedAreaIds || []).includes(mappedArea)) {
    throw new Error("Your area is not impacted by this Open Item.");
  }

  const decision = String(input?.decision || "").trim();
  if (!["approve", "reject", "needs-info"].includes(decision)) {
    throw new Error("Decision de voto invalida.");
  }

  const nextVote = {
    areaId: effectiveArea,
    decision,
    comment: String(input?.comment || "").trim(),
    voterEmail: email,
    voterName: String(user?.name || "").trim(),
    voterArea: effectiveArea,
    createdAt: new Date().toISOString(),
  };

  const preservedVotes = (item.votes || []).filter((vote) => vote.areaId !== effectiveArea);
  item.votes = [...preservedVotes, nextVote];
  if (effectiveArea && !item.impactedAreaIds.includes(effectiveArea)) {
    item.impactedAreaIds.push(effectiveArea);
  }
  const tracking = deriveOpenItemTracking(state, item);
  item.impactedAreaIds = tracking.impactedAreaIds;
  item.impactedAreas = tracking.impactedAreas;
  item.impactedUsers = tracking.impactedUsers;
  item.evaluationParticipants = tracking.evaluationParticipants;
  item.voteParticipants = tracking.voteParticipants;
  item.pendingEvaluators = tracking.pendingEvaluators;
  item.lastEvaluationDate = tracking.lastEvaluationDate;

  const nextState = await persistStateStore(state);
  return {
    state: nextState,
    vote: nextVote,
  };
}

async function saveImplementationTracking(openItemId, user, payload = {}) {
  if (!isChangeManagerUser(user)) {
    throw new Error("Not authorized");
  }

  const state = await loadStateStore();
  const item = (state.openItems || []).find((candidate) => candidate.id === openItemId);
  if (!item) {
    throw new Error("Open item no encontrado.");
  }

  if (!isOpenItemReadyForImplementation(item) && !isOpenItemImplementationActive(item)) {
    throw new Error("Este Open Item aun no esta listo para implementation tracking.");
  }

  const beforeTrackingData = getImplementationTrackingDataFromItem(item);
  const hadTracking = hasImplementationTracking(item);
  const ownerResolution = resolveOwnerEmailForOpenItem(item, state);
  const ownerContact = ownerResolution.email
    ? {
      name: ownerResolution.ownerName || item.ownerName || "",
      email: ownerResolution.email,
      source: ownerResolution.source,
      debug: ownerResolution.debug,
    }
    : null;
  const ticketLink = buildTicketDeepLink(openItemId);
  const now = new Date().toISOString();
  const nextStatus = normalizeImplementationStatus(payload.status || item.implementationStatus || "Open") || "Open";
  const nextDateCreated = String(payload.dateCreated || "").trim() || item.createdAt || formatMexicoCityDateTime(now);
  const nextDueDate = String(payload.dueDate || "").trim();
  const nextComments = String(payload.comments || "").trim();
  const nextMinutesRelated = String(payload.minutesRelated || "").trim();
  const nextGitRepository = String(payload.gitRepository || "").trim();
  const nextJiraTicketsRelated = String(payload.jiraTicketsRelated || "").trim();
  const nextBranchLocation = String(payload.branchLocation || "").trim();
  const nextImplementationNotes = String(payload.implementationNotes || "").trim();
  const approvalDate = getOpenItemApprovalDate(item) || formatMexicoCityDateTime(now);

  await updateOpenItemImplementationTrackingSheetRow(openItemId, {
    dateCreated: item.implementationTrackingCreatedAt ? nextDateCreated : (item.createdAt || nextDateCreated),
    dueDate: nextDueDate,
    status: nextStatus,
    comments: nextComments,
    minutesRelated: nextMinutesRelated,
    gitRepository: nextGitRepository,
    jiraTicketsRelated: nextJiraTicketsRelated,
    branchLocation: nextBranchLocation,
    implementationNotes: nextImplementationNotes,
  });

  item.status = normalizeStatus(nextStatus);
  item.implementationStatus = nextStatus;
  item.createdAt = nextDateCreated || item.createdAt || now;
  item.dueDate = nextDueDate;
  item.description = nextComments;
  item.minutesRelated = nextMinutesRelated;
  item.gitRepository = nextGitRepository;
  item.jiraTicketsRelated = nextJiraTicketsRelated;
  item.branchLocation = nextBranchLocation;
  item.implementationNotes = nextImplementationNotes;
  item.implementationTrackingCreatedAt = item.implementationTrackingCreatedAt || now;
  item.implementationApprovalDate = item.implementationApprovalDate || approvalDate;
  if (item.rawSheetRow && typeof item.rawSheetRow === "object") {
    item.rawSheetRow["Date Created"] = item.createdAt;
    item.rawSheetRow["Due Date"] = item.dueDate;
    item.rawSheetRow.Status = nextStatus;
    item.rawSheetRow["Comments/Updates"] = nextComments;
    item.rawSheetRow["Minutes Related"] = nextMinutesRelated;
    item.rawSheetRow["Git Repository"] = nextGitRepository;
    item.rawSheetRow["Jira Tickets Related"] = nextJiraTicketsRelated;
    const branchHeader = findExistingHeader(Object.keys(item.rawSheetRow), IMPLEMENTATION_TRACKING_COLUMN_ALIASES.branchLocation);
    const notesHeader = findExistingHeader(Object.keys(item.rawSheetRow), IMPLEMENTATION_TRACKING_COLUMN_ALIASES.implementationNotes);
    if (branchHeader) {
      item.rawSheetRow[branchHeader] = nextBranchLocation;
    }
    if (notesHeader) {
      item.rawSheetRow[notesHeader] = nextImplementationNotes;
    }
  }

  const nextState = persistState(sanitizeState(state));
  const afterTrackingData = getImplementationTrackingDataFromItem(item);
  let notification = { status: "not-sent" };
  try {
    if (!hadTracking) {
      notification = await sendImplementationAssignedEmail(item, afterTrackingData, ownerContact, ticketLink, state);
    } else {
      notification = await sendImplementationUpdatedEmail(item, beforeTrackingData, afterTrackingData, ownerContact, ticketLink);
    }
  } catch (error) {
    notification = {
      status: "email-failed",
      attempted: true,
      sent: false,
      mode: getEmailModeStatus().mode,
      recipient: "",
      intendedRecipient: ownerResolution.email || "",
      reason: error.message || "Failed to send the notification.",
      detail: error.message || "Failed to send the notification.",
    };
  }

  if (!notification.reason && !notification.sent && !ownerResolution.email) {
    notification.reason = `No owner email was found for ${ownerResolution.ownerName || item.ownerName || "the assigned owner"}.`;
  }
  notification.attempted = notification.attempted !== false;
  notification.mode = notification.mode || getEmailModeStatus().mode;
  notification.recipient = notification.recipient || notification.deliveryTarget || "";
  notification.intendedRecipient = notification.intendedRecipient || ownerResolution.email || "";
  notification.ownerName = ownerResolution.ownerName || item.ownerName || "";
  notification.ownerResolution = {
    email: ownerResolution.email || "",
    source: ownerResolution.source || "",
  };

  return {
    saved: true,
    state: nextState,
    openItemId,
    implementationTrackingCreatedAt: item.implementationTrackingCreatedAt,
    notification,
  };
}

function normalizeHeaderKey(header) {
  return String(header || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function findExistingDecisionHeader(headers, candidates = []) {
  const normalizedCandidates = candidates.map((candidate) => normalizeHeaderKey(candidate)).filter(Boolean);
  const existingHeaders = Array.isArray(headers) ? headers : [];
  const exactMatch = existingHeaders.find((header) => normalizedCandidates.includes(normalizeHeaderKey(header)));
  if (exactMatch) {
    return exactMatch;
  }
  return existingHeaders.find((header) => {
    const normalizedHeader = normalizeHeaderKey(header);
    return normalizedCandidates.some((candidate) => (
      normalizedHeader.includes(candidate) ||
      candidate.includes(normalizedHeader)
    ));
  }) || "";
}

function getDecisionAggregateHeaderMap(headers = []) {
  return {
    strategicAlignment: findExistingDecisionHeader(headers, [
      "CCB Decision Strategic Alignment Avg",
      "Strategic Alignment Avg",
      "Strategic Alignment Score",
    ]),
    riskAssessment: findExistingDecisionHeader(headers, [
      "CCB Decision Risk Assessment Avg",
      "Risk Assessment Avg",
      "Risk Assessment Score",
    ]),
    resourceImpact: findExistingDecisionHeader(headers, [
      "CCB Decision Resource Impact Avg",
      "Resource Impact Avg",
      "Resource Impact Score",
    ]),
    customerValue: findExistingDecisionHeader(headers, [
      "CCB Decision Customer Value Avg",
      "Customer Value Avg",
      "Customer Value Score",
    ]),
    operationalFeasibility: findExistingDecisionHeader(headers, [
      "CCB Decision Operational Feasibility Avg",
      "Operational Feasibility Avg",
      "Operational Feasibility Score",
    ]),
    total: findExistingDecisionHeader(headers, [
      "CCB Decision Average Score",
      "CCB Evaluation Avg",
      "CCB Eval Avg",
      "CCB Ev",
      "Weighted Average Score",
      "Average Score",
      "Total",
    ]),
    recommendation: findExistingDecisionHeader(headers, [
      "CCB Decision Recommendation",
      "CCB Decision Recommend",
      "Recommendation",
      "Status",
      "CCB Status",
    ]),
    evaluatorCount: findExistingDecisionHeader(headers, [
      "CCB Decision Evaluator Count",
      "CCB Evaluator Count",
      "Evaluator Count",
      "Evaluators Count",
      "CCB Ev Count",
    ]),
    lastUpdated: findExistingDecisionHeader(headers, [
      "CCB Decision Last Updated",
      "CCB Last Updated",
      "Last Updated",
      "Updated At",
    ]),
    evaluators: findExistingDecisionHeader(headers, [
      "CCB Decision Evaluators",
      "CCB Evaluators",
      "Evaluators",
    ]),
  };
}

function normalizeDecisionSummary(summary = {}) {
  return {
    averageWeightedScore: Number.isFinite(Number(summary.averageWeightedScore))
      ? Number(summary.averageWeightedScore)
      : null,
    recommendation: String(summary.recommendation || "").trim() || "INCOMPLETE",
    evaluatorCount: Number(summary.evaluatorCount || 0) || 0,
    criterionAverages: {
      "strategic-alignment": Number.isFinite(Number(summary.criterionAverages?.["strategic-alignment"]))
        ? Number(summary.criterionAverages["strategic-alignment"])
        : null,
      "risk-assessment": Number.isFinite(Number(summary.criterionAverages?.["risk-assessment"]))
        ? Number(summary.criterionAverages["risk-assessment"])
        : null,
      "resource-impact": Number.isFinite(Number(summary.criterionAverages?.["resource-impact"]))
        ? Number(summary.criterionAverages["resource-impact"])
        : null,
      "customer-value": Number.isFinite(Number(summary.criterionAverages?.["customer-value"]))
        ? Number(summary.criterionAverages["customer-value"])
        : null,
      "operational-feasibility": Number.isFinite(Number(summary.criterionAverages?.["operational-feasibility"]))
        ? Number(summary.criterionAverages["operational-feasibility"])
        : null,
    },
    lastUpdated: String(summary.lastUpdated || "").trim(),
    evaluators: Array.isArray(summary.evaluators) ? summary.evaluators : [],
  };
}

function hasMeaningfulDecisionSummary(summary = {}) {
  const normalized = normalizeDecisionSummary(summary);
  return (
    (normalized.averageWeightedScore != null && normalized.averageWeightedScore > 0) ||
    normalized.evaluatorCount > 0 ||
    normalized.lastUpdated ||
    normalized.evaluators.length > 0 ||
    (normalized.recommendation && normalized.recommendation !== "INCOMPLETE") ||
    Object.values(normalized.criterionAverages).some((value) => value != null)
  );
}

function parseDecisionAggregateSummary(record = {}, headers = []) {
  const headerMap = getDecisionAggregateHeaderMap(headers);
  const parseNumeric = (value) => {
    const parsed = Number.parseFloat(String(value || "").trim());
    return Number.isFinite(parsed) ? parsed : null;
  };
  return {
    averageWeightedScore: parseNumeric(record[headerMap.total]),
    recommendation: String(record[headerMap.recommendation] || "").trim() || "INCOMPLETE",
    evaluatorCount: Number.parseInt(String(record[headerMap.evaluatorCount] || "").trim(), 10) || 0,
    criterionAverages: {
      "strategic-alignment": parseNumeric(record[headerMap.strategicAlignment]),
      "risk-assessment": parseNumeric(record[headerMap.riskAssessment]),
      "resource-impact": parseNumeric(record[headerMap.resourceImpact]),
      "customer-value": parseNumeric(record[headerMap.customerValue]),
      "operational-feasibility": parseNumeric(record[headerMap.operationalFeasibility]),
    },
    lastUpdated: String(record[headerMap.lastUpdated] || "").trim(),
    evaluators: String(record[headerMap.evaluators] || "").trim()
      ? String(record[headerMap.evaluators] || "").split(",").map((value) => String(value || "").trim()).filter(Boolean)
      : [],
  };
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
      voterEmail: normalizeEmail(record.voterEmail),
      voterName: String(record.voterName || "").trim(),
      voterArea: String(record.voterArea || "").trim(),
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
  const checkboxHeaders = new Set([
    ...areaColumns.map((area) => area.sourceColumn),
    ...ITL_AREA_HEADERS.filter((header) => headers.includes(header)),
  ]);

  return state.openItems.map((item) => {
    const baseRow = item.rawSheetRow && typeof item.rawSheetRow === "object"
      ? { ...item.rawSheetRow }
      : {};
    const tracking = deriveOpenItemTracking(state, item);
    const impactedAreaIds = new Set(tracking.impactedAreaIds);
    const preservedOwner = String(baseRow.Owner || baseRow.owner || "").trim();

    CCB_DECISION_AGGREGATE_HEADERS.forEach((header) => {
      delete baseRow[header];
    });

    baseRow.ID = item.id || "";
    baseRow["Open Item Description"] = item.title || "";
    baseRow.Owner = String(item.ownerName || preservedOwner).trim();
    baseRow["Date Created"] = formatMexicoCityDateTime(item.createdAt || baseRow["Date Created"] || "");
    baseRow["Due Date"] = formatMexicoCityDateTime(item.dueDate || baseRow["Due Date"] || "");
    const preservedImplementationStatus = normalizeImplementationStatus(item.implementationStatus || baseRow.Status || "");
    if (preservedImplementationStatus) {
      baseRow.Status = preservedImplementationStatus;
    } else if (!String(baseRow.Status || "").trim()) {
      baseRow.Status = item.status || "open";
    }
    baseRow["Comments/Updates"] = item.description || "";
    baseRow.impactedUsers = tracking.impactedUsers.join(", ");
    baseRow.impactedAreas = tracking.impactedAreas.join(", ");
    baseRow.evaluationParticipants = tracking.evaluationParticipants.join(", ");
    baseRow.voteParticipants = tracking.voteParticipants.join(", ");
    baseRow.pendingEvaluators = tracking.pendingEvaluators.join(", ");
    baseRow.lastEvaluationDate = tracking.lastEvaluationDate ? formatMexicoCityDateTime(tracking.lastEvaluationDate) : "";
    baseRow["Minutes Related"] = baseRow["Minutes Related"] || "";
    baseRow["Git Repository"] = baseRow["Git Repository"] || "";
    baseRow["Jira Tickets Related"] = baseRow["Jira Tickets Related"] || "";

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
        const rawValue = baseRow[header];
        if (rawValue === "") {
          return "";
        }
        if (typeof rawValue === "string") {
          const normalized = rawValue.trim().toUpperCase();
          if (normalized === "TRUE") {
            return true;
          }
          if (normalized === "FALSE") {
            return false;
          }
        }
        return Boolean(rawValue);
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
        vote.voterEmail || "",
        vote.voterName || "",
        vote.voterArea || "",
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

function parseCcbEvaluationRows(values) {
  const records = buildSheetValuesMap(values);
  return records
    .filter((record) => String(record.openItemId || "").trim() && String(record.evaluatorEmail || "").trim() && String(record.criterionId || "").trim())
    .map((record) => ({
      openItemId: String(record.openItemId || "").trim(),
      evaluatorEmail: normalizeEmail(record.evaluatorEmail),
      evaluatorName: String(record.evaluatorName || "").trim(),
      evaluatorArea: String(record.evaluatorArea || "").trim(),
      criterionId: String(record.criterionId || "").trim(),
      criterionName: String(record.criterionName || "").trim(),
      score: Number.parseInt(String(record.score || "").trim(), 10) || 0,
      weight: Number.parseFloat(String(record.weight || "").trim()) || 0,
      rationale: String(record.rationale || "").trim(),
      supportingReference: String(record.supportingReference || "").trim(),
      createdAt: String(record.createdAt || "").trim(),
      updatedAt: String(record.updatedAt || "").trim(),
    }));
}

function buildCcbEvaluationRows(state) {
  const rows = (state.ccbEvaluations || []).map((evaluation) => (
    CCB_EVALUATION_HEADERS.map((header) => {
      const value = evaluation[header];
      return value == null ? "" : String(value);
    })
  ));

  return [CCB_EVALUATION_HEADERS, ...rows];
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

function buildDecisionSheetHeaders(existingHeaders = []) {
  if (Array.isArray(existingHeaders) && existingHeaders.length) {
    return existingHeaders.map((header) => String(header || "").trim());
  }
  return [...CCB_DECISION_HEADERS];
}

function buildDecisionSheetRows(state, existingRowsByOpenItemId = new Map(), decisionHeaders = CCB_DECISION_HEADERS) {
  const headerMap = getDecisionAggregateHeaderMap(decisionHeaders);
  const rows = state.openItems.map((item) => {
    const existingRecord = existingRowsByOpenItemId.get(item.id) || {};
    const baseRecord = {};
    const ccbDecisionSummary = calculateOpenItemCcbDecisionAverages(
      item.id,
      state,
      Array.isArray(state?.ccbDecisionCriteria) && state.ccbDecisionCriteria.length
        ? state.ccbDecisionCriteria
        : getDefaultCcbDecisionCriteria(),
    );

    decisionHeaders.forEach((header) => {
      baseRecord[header] = String(existingRecord[header] || "").trim();
    });

    baseRecord.OI_ID = item.id || "";
    baseRecord["Open Item Description"] = item.title || "";
    if (headerMap.strategicAlignment) {
      baseRecord[headerMap.strategicAlignment] = ccbDecisionSummary?.criterionAverages?.["strategic-alignment"] == null ? "" : ccbDecisionSummary.criterionAverages["strategic-alignment"];
    }
    if (headerMap.riskAssessment) {
      baseRecord[headerMap.riskAssessment] = ccbDecisionSummary?.criterionAverages?.["risk-assessment"] == null ? "" : ccbDecisionSummary.criterionAverages["risk-assessment"];
    }
    if (headerMap.resourceImpact) {
      baseRecord[headerMap.resourceImpact] = ccbDecisionSummary?.criterionAverages?.["resource-impact"] == null ? "" : ccbDecisionSummary.criterionAverages["resource-impact"];
    }
    if (headerMap.customerValue) {
      baseRecord[headerMap.customerValue] = ccbDecisionSummary?.criterionAverages?.["customer-value"] == null ? "" : ccbDecisionSummary.criterionAverages["customer-value"];
    }
    if (headerMap.operationalFeasibility) {
      baseRecord[headerMap.operationalFeasibility] = ccbDecisionSummary?.criterionAverages?.["operational-feasibility"] == null ? "" : ccbDecisionSummary.criterionAverages["operational-feasibility"];
    }
    if (headerMap.total) {
      baseRecord[headerMap.total] = ccbDecisionSummary?.averageWeightedScore == null ? "" : ccbDecisionSummary.averageWeightedScore;
    }
    if (headerMap.recommendation) {
      baseRecord[headerMap.recommendation] = ccbDecisionSummary?.recommendation || "INCOMPLETE";
    }
    if (headerMap.evaluatorCount) {
      baseRecord[headerMap.evaluatorCount] = ccbDecisionSummary?.evaluatorCount || 0;
    }
    if (headerMap.lastUpdated) {
      baseRecord[headerMap.lastUpdated] = ccbDecisionSummary?.lastUpdated ? formatMexicoCityDateTime(ccbDecisionSummary.lastUpdated) : "";
    }
    if (headerMap.evaluators) {
      baseRecord[headerMap.evaluators] = Array.isArray(ccbDecisionSummary?.evaluators) ? ccbDecisionSummary.evaluators.join(", ") : "";
    }

    return decisionHeaders.map((header) => {
      const value = baseRecord[header];
      return value == null ? "" : value;
    });
  });

  return [decisionHeaders, ...rows];
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
      throw new Error("Failed to obtain a Google OAuth access token.");
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

function buildFormulaCopyRequests({
  sheetId,
  headerRow,
  managedHeaders,
  lastFormulaRow,
  existingDataRowCount,
  targetRowCount,
  headerRowNumber,
}) {
  if (
    sheetId == null ||
    existingDataRowCount < 1 ||
    targetRowCount <= existingDataRowCount ||
    !Array.isArray(lastFormulaRow)
  ) {
    return [];
  }

  const sourceRowIndex = headerRowNumber + existingDataRowCount - 1;
  const destinationStartRowIndex = sourceRowIndex + 1;
  const destinationEndRowIndex = headerRowNumber + targetRowCount;

  return headerRow
    .map((header, columnIndex) => {
      const formulaValue = lastFormulaRow[columnIndex];
      if (
        managedHeaders.has(header) ||
        typeof formulaValue !== "string" ||
        !formulaValue.trim().startsWith("=")
      ) {
        return null;
      }

      return {
        copyPaste: {
          source: {
            sheetId,
            startRowIndex: sourceRowIndex,
            endRowIndex: sourceRowIndex + 1,
            startColumnIndex: columnIndex,
            endColumnIndex: columnIndex + 1,
          },
          destination: {
            sheetId,
            startRowIndex: destinationStartRowIndex,
            endRowIndex: destinationEndRowIndex,
            startColumnIndex: columnIndex,
            endColumnIndex: columnIndex + 1,
          },
          pasteType: "PASTE_FORMULA",
          pasteOrientation: "NORMAL",
        },
      };
    })
    .filter(Boolean);
}

async function fetchSpreadsheetMetadata() {
  const url =
    `https://sheets.googleapis.com/v4/spreadsheets/${SOURCE_SPREADSHEET_ID}` +
    "?fields=properties(title),sheets(properties(sheetId,title),tables(tableId,name,range))";
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

  if (!requests.length) {
    return;
  }

  const maxPeopleChipRequestsPerBatch = 10;
  for (let index = 0; index < requests.length; index += maxPeopleChipRequestsPerBatch) {
    const nextBatch = requests.slice(index, index + maxPeopleChipRequestsPerBatch);
    await batchUpdateSpreadsheet(nextBatch);
  }
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

async function applyExplicitBooleanCells(sheetId, headerRow, startRow, rows, booleanHeaders = []) {
  if (!sheetId || !Array.isArray(rows) || !rows.length) {
    return;
  }

  const requests = [];
  const headerIndexes = booleanHeaders
    .map((header) => ({
      header,
      index: headerRow.findIndex((value) => String(value || "").trim() === header),
    }))
    .filter((entry) => entry.index >= 0);

  rows.forEach((row, rowOffset) => {
    headerIndexes.forEach((entry) => {
      const value = row[entry.index];
      if (value !== true && value !== false) {
        return;
      }
      requests.push({
        updateCells: {
          range: {
            sheetId,
            startRowIndex: startRow - 1 + rowOffset,
            endRowIndex: startRow + rowOffset,
            startColumnIndex: entry.index,
            endColumnIndex: entry.index + 1,
          },
          rows: [
            {
              values: [
                {
                  userEnteredValue: {
                    boolValue: value,
                  },
                },
              ],
            },
          ],
          fields: "userEnteredValue",
        },
      });
    });
  });

  if (!requests.length) {
    return;
  }

  await batchUpdateSpreadsheet(requests);
}

function getSheetMetadataByName(metadata, sheetName) {
  return Array.isArray(metadata?.sheets)
    ? metadata.sheets.find((sheet) => sheet.properties?.title === sheetName) || null
    : null;
}

function getPrimaryTableForSheet(sheetMetadata) {
  const tables = Array.isArray(sheetMetadata?.tables) ? sheetMetadata.tables : [];
  return tables[0] || null;
}

function buildTableResizeRequest(sheetMetadata, desiredEndRowIndex, desiredEndColumnIndex = null) {
  const table = getPrimaryTableForSheet(sheetMetadata);
  if (!table?.tableId || !table.range || desiredEndRowIndex == null) {
    return null;
  }

  const currentEndRowIndex = Number(table.range.endRowIndex);
  const currentEndColumnIndex = Number(table.range.endColumnIndex);
  const nextEndRowIndex = Number.isFinite(currentEndRowIndex)
    ? (desiredEndRowIndex > currentEndRowIndex ? desiredEndRowIndex : currentEndRowIndex)
    : desiredEndRowIndex;
  const nextEndColumnIndex = Number.isFinite(desiredEndColumnIndex)
    ? desiredEndColumnIndex
    : currentEndColumnIndex;

  if (
    (!Number.isFinite(currentEndRowIndex) || nextEndRowIndex === currentEndRowIndex) &&
    (!Number.isFinite(currentEndColumnIndex) || nextEndColumnIndex === currentEndColumnIndex)
  ) {
    return null;
  }

  return {
    updateTable: {
      table: {
        tableId: table.tableId,
        name: table.name,
        range: {
          ...table.range,
          endRowIndex: nextEndRowIndex,
          endColumnIndex: nextEndColumnIndex,
        },
      },
      fields: "range",
    },
  };
}

async function fetchOpenItemOwnerEmailMap(sheetName, headerRow, startRow, rowCount) {
  if (!sheetName || !Array.isArray(headerRow) || rowCount <= 0) {
    return new Map();
  }

  const ownerColumnIndex = headerRow.findIndex((value) => String(value || "").trim() === "Owner");
  const idColumnIndex = headerRow.findIndex((value) => String(value || "").trim() === "ID");
  if (ownerColumnIndex < 0 || idColumnIndex < 0) {
    return new Map();
  }

  const ownerColumnLabel = toA1Column(ownerColumnIndex);
  const idColumnLabel = toA1Column(idColumnIndex);
  const endRow = startRow + rowCount - 1;
  const encodedSheetName = encodeURIComponent(sheetName);
  const url =
    `https://sheets.googleapis.com/v4/spreadsheets/${SOURCE_SPREADSHEET_ID}` +
    `?ranges=${encodedSheetName}%21${ownerColumnLabel}${startRow}%3A${ownerColumnLabel}${endRow}` +
    `&includeGridData=true` +
    `&fields=sheets(data(rowData(values(chipRuns))))`;
  const idValuesUrl =
    `https://sheets.googleapis.com/v4/spreadsheets/${SOURCE_SPREADSHEET_ID}/values/` +
    `${encodeURIComponent(`${sheetName}!${idColumnLabel}${startRow}:${idColumnLabel}${endRow}`)}`;

  const [ownerPayload, idValuesPayload] = await Promise.all([
    fetchGoogleSheetsJson(url),
    fetchGoogleSheetsJson(idValuesUrl),
  ]);

  const idValues = Array.isArray(idValuesPayload?.values) ? idValuesPayload.values : [];
  const rowData = ownerPayload?.sheets?.[0]?.data?.[0]?.rowData || [];
  const ownerEmailById = new Map();

  for (let index = 0; index < rowCount; index += 1) {
    const itemId = String(idValues[index]?.[0] || "").trim();
    if (!itemId) {
      continue;
    }
    const chipRuns = rowData[index]?.values?.[0]?.chipRuns;
    const chipEmail = normalizeEmail(chipRuns?.find((entry) => entry?.chip?.personProperties?.email)?.chip?.personProperties?.email);
    if (chipEmail) {
      ownerEmailById.set(itemId, chipEmail);
    }
  }

  return ownerEmailById;
}

async function applyOpenItemOwnerPeopleChips(sheetId, headerRow, startRow, state, existingOwnerEmailById = new Map()) {
  if (sheetId == null) {
    return;
  }

  const ownerColumnIndex = headerRow.findIndex((value) => String(value || "").trim() === "Owner");
  if (ownerColumnIndex < 0) {
    return;
  }

  const contactsDirectory = buildKnownContactsDirectory(state.areas || []);
  const chipUpdates = state.openItems.map((item, index) => {
    const fallbackOwnerEmail = existingOwnerEmailById.get(item.id) || "";
    const contact = resolveContactByValue(item.ownerEmail || fallbackOwnerEmail || item.ownerName, contactsDirectory) || {
      name: String(item.ownerName || "").trim(),
      email: normalizeEmail(item.ownerEmail || fallbackOwnerEmail),
    };
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
  const desiredHeaders = buildOpenItemHeaders(state, state.areas || []);
  const matchedSheet = getSheetMetadataByName(metadata, SOURCE_SHEET_NAME);
  const sheetId = matchedSheet?.properties?.sheetId ?? null;
  const checkboxColumns = Array.from(
    new Set(
      (state.areas || [])
        .map((area) => String(area?.sourceColumn || "").trim())
        .filter(Boolean)
        .concat(ITL_AREA_HEADERS),
    ),
  );
  const headerWidth = Math.max(context.headerRow.length, desiredHeaders.length, rows.reduce((max, row) => Math.max(max, row.length), 0));
  const existingDataRowCount = Math.max(0, existingValues.length - context.headerRowNumber);
  const existingOwnerEmailById = await fetchOpenItemOwnerEmailMap(
    SOURCE_SHEET_NAME,
    context.headerRow,
    context.headerRowNumber + 1,
    existingDataRowCount,
  );
  const targetRowCount = Math.max(existingDataRowCount, rows.length, 1);
  const startRow = context.headerRowNumber + 1;
  const endRow = startRow + targetRowCount - 1;
  const endColumn = toA1Column(Math.max(0, headerWidth - 1));
  const clearRange = `${SOURCE_SHEET_NAME}!A${startRow}:${endColumn}${endRow}`;
  const nextRows = preserveOpenItemFormulaCells(rows, existingValues, existingFormulaValues, state.areas || []);
  const formulaContext = getOpenItemSheetContext(existingFormulaValues);
  const formulaCopyRequests = buildFormulaCopyRequests({
    sheetId,
    headerRow: desiredHeaders,
    managedHeaders: buildManagedOpenItemHeaders(state.areas || []),
    lastFormulaRow: formulaContext.bodyRows[existingDataRowCount - 1] || [],
    existingDataRowCount,
    targetRowCount: rows.length,
    headerRowNumber: context.headerRowNumber,
  });

  if (
    desiredHeaders.length !== context.headerRow.length ||
    desiredHeaders.some((header, index) => header !== context.headerRow[index])
  ) {
    const headerRange = `${SOURCE_SHEET_NAME}!A${context.headerRowNumber}:${endColumn}${context.headerRowNumber}`;
    const paddedHeaders = [...desiredHeaders];
    while (paddedHeaders.length < headerWidth) {
      paddedHeaders.push("");
    }
    await writeSheetRange(headerRange, [paddedHeaders], "USER_ENTERED");
  }

  await clearSheetRange(clearRange);

  if (nextRows.length) {
    const writeRange = `${SOURCE_SHEET_NAME}!A${startRow}:${endColumn}${startRow + nextRows.length - 1}`;
    await writeSheetRange(writeRange, nextRows, "USER_ENTERED");
  }
  await applyExplicitBooleanCells(sheetId, context.headerRow, startRow, nextRows, ITL_AREA_HEADERS);

  await batchUpdateSpreadsheet(formulaCopyRequests);
  const tableResizeRequest = buildTableResizeRequest(matchedSheet, context.headerRowIndex + 1 + rows.length, desiredHeaders.length);
  await batchUpdateSpreadsheet(tableResizeRequest ? [tableResizeRequest] : []);

  await applyOpenItemsCheckboxValidation(sheetId, context.headerRow, startRow, endRow, checkboxColumns);
  await applyOpenItemOwnerPeopleChips(sheetId, context.headerRow, startRow, state, existingOwnerEmailById);
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

  const decisionHeaders = buildDecisionSheetHeaders(context.headerRow);
  const decisionRows = buildDecisionSheetRows(state, rowsByOpenItemId, decisionHeaders);
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
  const formulaContext = getDecisionSheetContext(existingFormulaValues);
  const formulaCopyRequests = buildFormulaCopyRequests({
    sheetId,
    headerRow: context.headerRow,
    managedHeaders: buildManagedDecisionHeaders(),
    lastFormulaRow: formulaContext.bodyRows[existingDataRowCount - 1] || [],
    existingDataRowCount,
    targetRowCount: decisionRows.length - 1,
    headerRowNumber: context.headerRowNumber,
  });

  if (decisionHeaders.length !== context.headerRow.length || decisionHeaders.some((header, index) => header !== context.headerRow[index])) {
    const headerRange = `${CCB_DECISION_SHEET_NAME}!A${context.headerRowNumber}:${endColumn}${context.headerRowNumber}`;
    await writeSheetRange(headerRange, [decisionHeaders], "USER_ENTERED");
  }

  await clearSheetRange(clearRange);

  if (nextDecisionRows.length) {
    const writeRange = `${CCB_DECISION_SHEET_NAME}!A${startRow}:${endColumn}${startRow + nextDecisionRows.length - 1}`;
    await writeSheetRange(writeRange, nextDecisionRows, "USER_ENTERED");
  }

  await batchUpdateSpreadsheet(formulaCopyRequests);
  const tableResizeRequest = buildTableResizeRequest(matchedSheet, context.headerRowIndex + decisionRows.length, headerWidth);
  await batchUpdateSpreadsheet(tableResizeRequest ? [tableResizeRequest] : []);
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
  if (existingSheets.has(CCB_EVALUATIONS_SHEET_NAME)) {
    ranges.push(CCB_EVALUATIONS_SHEET_NAME);
  }
  if (existingSheets.has(CCB_DECISION_SHEET_NAME)) {
    ranges.push(CCB_DECISION_SHEET_NAME);
  }
  const valueMap = await batchGetSpreadsheetValues(ranges);
  const openItemValues = valueMap.get(SOURCE_SHEET_NAME) || [];
  const areaValues = valueMap.get(AREAS_SHEET_NAME) || [];
  const voteValues = valueMap.get(VOTES_SHEET_NAME) || [];
  const preSessionValues = valueMap.get(PRESESSION_SHEET_NAME) || [];
  const evaluationValues = valueMap.get(CCB_EVALUATIONS_SHEET_NAME) || [];
  const decisionValues = valueMap.get(CCB_DECISION_SHEET_NAME) || [];
  const openItemContext = getOpenItemSheetContext(openItemValues);
  const areas = parseAreasSheetRows(areaValues);
  const votesMap = parseVotesSheetRows(voteValues);
  const preSessionChecksMap = parsePreSessionSheetRows(preSessionValues);
  const ccbEvaluations = parseCcbEvaluationRows(evaluationValues);
  const ccbDecisionCriteria = loadCcbDecisionCriteria(decisionValues);
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

  const localState = loadState();
  const state = mapSnapshotToState(snapshot, localState, effectiveAreas, votesMap, preSessionChecksMap);
  state.ccbDecisionCriteria = ccbDecisionCriteria;
  state.ccbEvaluations = ccbEvaluations;
  const decisionSheet = decisionValues.length ? parseDecisionSheetRows(decisionValues) : { context: { headerRow: [] }, rowsByOpenItemId: new Map() };
  state.openItems.forEach((item) => {
    const tracking = deriveOpenItemTracking(state, item);
    item.impactedAreaIds = tracking.impactedAreaIds;
    item.impactedAreas = tracking.impactedAreas;
    item.impactedUsers = tracking.impactedUsers;
    item.evaluationParticipants = tracking.evaluationParticipants;
    item.voteParticipants = tracking.voteParticipants;
    item.pendingEvaluators = tracking.pendingEvaluators;
    item.lastEvaluationDate = tracking.lastEvaluationDate;
    const recalculatedSummary = calculateOpenItemCcbDecisionAverages(
      item.id,
      state,
      state.ccbDecisionCriteria,
    );
    if (recalculatedSummary.evaluatorCount > 0) {
      updateOpenItemCcbDecision(item.id, recalculatedSummary, state);
      return;
    }
    const existingDecisionRecord = decisionSheet.rowsByOpenItemId.get(item.id);
    if (existingDecisionRecord) {
      const summaryFromDecisionSheet = parseDecisionAggregateSummary(
        existingDecisionRecord,
        decisionSheet.context.headerRow,
      );
      const hasPersistedAggregate = hasMeaningfulDecisionSummary(summaryFromDecisionSheet);
      if (hasPersistedAggregate) {
        updateOpenItemCcbDecision(item.id, summaryFromDecisionSheet, state);
        return;
      }
    }
    updateOpenItemCcbDecision(item.id, null, state);
  });
  state.source = {
    ...state.source,
    type: "google-sheets",
    sheetName: SOURCE_SHEET_NAME,
    sheetHeaders: stripOpenItemDecisionHeaders(snapshot.headers),
  };
  return state;
}

async function persistStateToGoogleSheets(nextState) {
  const normalizedState = sanitizeState(nextState);
  const existingState = await loadStateFromGoogleSheets();
  const areas = resolveAreas(normalizedState.areas, existingState);
  normalizedState.areas = areas;
  normalizedState.source = {
    ...(normalizedState.source || {}),
    sheetHeaders: stripOpenItemDecisionHeaders(existingState?.source?.sheetHeaders || normalizedState.source?.sheetHeaders || []),
  };
  const headers = buildOpenItemHeaders(normalizedState, areas);
  normalizedState.source = {
    ...(normalizedState.source || {}),
    type: "google-sheets",
    spreadsheetId: SOURCE_SPREADSHEET_ID,
    sheetName: SOURCE_SHEET_NAME,
    sheetHeaders: stripOpenItemDecisionHeaders(headers),
  };
  const openItemRows = buildOpenItemsRowsFromState(normalizedState, areas);
  const areaRows = buildAreasSheetRows(areas);
  const voteRows = buildVotesSheetRows(normalizedState);
  const preSessionRows = buildPreSessionSheetRows(normalizedState);
  const evaluationRows = buildCcbEvaluationRows(normalizedState);

  await ensureSpreadsheetSheets([AREAS_SHEET_NAME, VOTES_SHEET_NAME, PRESESSION_SHEET_NAME, CCB_DECISION_SHEET_NAME, CCB_EVALUATIONS_SHEET_NAME]);
  await writeOpenItemsSheetPreservingTemplate(openItemRows, normalizedState);
  await Promise.all([
    clearAndWriteSheet(AREAS_SHEET_NAME, areaRows),
    clearAndWriteSheet(VOTES_SHEET_NAME, voteRows),
    clearAndWriteSheet(PRESESSION_SHEET_NAME, preSessionRows),
    clearAndWriteSheet(CCB_EVALUATIONS_SHEET_NAME, evaluationRows),
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
          ownerEmail: normalizeEmail(item.ownerEmail),
          ownerAreaHint: item.ownerAreaHint || "",
          impactedAreaIds: Array.isArray(item.impactedAreaIds) ? item.impactedAreaIds : [],
          impactedUsers: Array.isArray(item.impactedUsers) ? item.impactedUsers.map((value) => normalizeEmail(value)).filter(Boolean) : [],
          impactedAreas: Array.isArray(item.impactedAreas) ? item.impactedAreas.map((value) => String(value || "").trim()).filter(Boolean) : [],
          evaluationParticipants: Array.isArray(item.evaluationParticipants) ? item.evaluationParticipants.map((value) => normalizeEmail(value)).filter(Boolean) : [],
          voteParticipants: Array.isArray(item.voteParticipants) ? item.voteParticipants.map((value) => normalizeEmail(value)).filter(Boolean) : [],
          pendingEvaluators: Array.isArray(item.pendingEvaluators) ? item.pendingEvaluators.map((value) => normalizeEmail(value)).filter(Boolean) : [],
          lastEvaluationDate: item.lastEvaluationDate || "",
          ccbDecisionAverageScore: item.ccbDecisionAverageScore == null ? null : Number(item.ccbDecisionAverageScore),
          ccbDecisionRecommendation: String(item.ccbDecisionRecommendation || "").trim(),
          ccbDecisionEvaluatorCount: Number(item.ccbDecisionEvaluatorCount || 0),
          ccbDecisionCriterionAverages: item.ccbDecisionCriterionAverages && typeof item.ccbDecisionCriterionAverages === "object"
            ? {
                "strategic-alignment": parseScore(item.ccbDecisionCriterionAverages["strategic-alignment"]),
                "risk-assessment": parseScore(item.ccbDecisionCriterionAverages["risk-assessment"]),
                "resource-impact": parseScore(item.ccbDecisionCriterionAverages["resource-impact"]),
                "customer-value": parseScore(item.ccbDecisionCriterionAverages["customer-value"]),
                "operational-feasibility": parseScore(item.ccbDecisionCriterionAverages["operational-feasibility"]),
              }
            : {},
          ccbDecisionLastUpdated: item.ccbDecisionLastUpdated || "",
          ccbDecisionEvaluators: Array.isArray(item.ccbDecisionEvaluators) ? item.ccbDecisionEvaluators.map((value) => String(value || "").trim()).filter(Boolean) : [],
          isSubstantial: Boolean(item.isSubstantial),
          status: item.status || "open",
          implementationStatus: normalizeImplementationStatus(item.implementationStatus || ""),
          createdAt: item.createdAt || new Date().toISOString(),
          dueDate: item.dueDate || "",
          minutesRelated: item.minutesRelated || "",
          gitRepository: item.gitRepository || "",
          jiraTicketsRelated: item.jiraTicketsRelated || "",
          branchLocation: String(item.branchLocation || "").trim(),
          implementationNotes: String(item.implementationNotes || "").trim(),
          implementationTrackingCreatedAt: String(item.implementationTrackingCreatedAt || "").trim(),
          implementationApprovalDate: String(item.implementationApprovalDate || "").trim(),
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
                voterEmail: normalizeEmail(vote.voterEmail),
                voterName: String(vote.voterName || "").trim(),
                voterArea: String(vote.voterArea || "").trim(),
                createdAt: vote.createdAt || new Date().toISOString(),
              }))
            : [],
        }))
      : [],
    ccbDecisionCriteria: Array.isArray(input.ccbDecisionCriteria) && input.ccbDecisionCriteria.length
      ? input.ccbDecisionCriteria.map((criterion) => normalizeDecisionCriterion(
          criterion,
          getDefaultCcbDecisionCriteria().find((fallback) => fallback.id === String(criterion?.id || "").trim())
            || getDefaultCcbDecisionCriteria().find((fallback) => fallback.name === String(criterion?.name || "").trim())
            || {},
        ))
      : getDefaultCcbDecisionCriteria(),
    ccbEvaluations: Array.isArray(input.ccbEvaluations)
      ? input.ccbEvaluations.map((evaluation) => ({
          openItemId: String(evaluation.openItemId || "").trim(),
          evaluatorEmail: normalizeEmail(evaluation.evaluatorEmail),
          evaluatorName: String(evaluation.evaluatorName || "").trim(),
          evaluatorArea: String(evaluation.evaluatorArea || "").trim(),
          criterionId: String(evaluation.criterionId || "").trim(),
          criterionName: String(evaluation.criterionName || "").trim(),
          score: Number.parseInt(String(evaluation.score || "").trim(), 10) || 0,
          weight: Number(evaluation.weight || 0),
          rationale: String(evaluation.rationale || "").trim(),
          supportingReference: String(evaluation.supportingReference || "").trim(),
          createdAt: String(evaluation.createdAt || "").trim(),
          updatedAt: String(evaluation.updatedAt || "").trim(),
        })).filter((evaluation) => evaluation.openItemId && evaluation.evaluatorEmail && evaluation.criterionId)
      : [],
    source: input.source || null,
    lastSavedAt: new Date().toISOString(),
  };
}

function serveStatic(requestPath, response) {
  const targetPath = requestPath === "/" ? "/index.html" : requestPath;
  const staticRoot = targetPath.startsWith("/logo/") ? LOGO_DIR : PUBLIC_DIR;
  const relativeTarget = targetPath.startsWith("/logo/")
    ? targetPath.replace(/^\/logo\//, "")
    : targetPath;
  const filePath = path.join(staticRoot, path.normalize(relativeTarget));

  if (!filePath.startsWith(staticRoot)) {
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
      .replace(/__APP_BASE_PATH__/g, BASE_PATH)
      .replace(/__APP_LOGO_PATH__/g, getBrandLogoDocumentSrc());
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
  const pathname = isGlobalLogoPath(url.pathname) ? url.pathname : stripBasePath(url.pathname);

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
        redirectToApp(response, "error", String(url.searchParams.get("error_description") || url.searchParams.get("error") || "Google authorization was rejected."));
        return;
      }

      const code = String(url.searchParams.get("code") || "");
      if (!code) {
        redirectToApp(response, "error", "Google did not return an authorization code.");
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

    if (request.method === "GET" && pathname === "/api/email-mode") {
      sendJson(response, 200, getEmailModeStatus());
      return;
    }

    if (request.method === "POST" && pathname === "/api/email-mode") {
      const user = getCurrentUser(request);
      if (!isAdminUser(user)) {
        sendJson(response, 403, { error: "Not authorized" });
        return;
      }
      const body = await parseBody(request);
      const requestedMode = normalizeEmailMode(body.mode);
      const savedConfig = persistEmailModeConfig({ mode: requestedMode }, user);
      sendJson(response, 200, {
        ...getEmailModeStatus(),
        updatedAt: savedConfig.updatedAt,
        updatedBy: savedConfig.updatedBy,
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
      deleteIfExists(USER_GOOGLE_OAUTH_TOKEN_RUNTIME_FILE);
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
          detail: "Only the area owner can respond to this review.",
        });
        return;
      }

      const item = state.openItems.find((candidate) => candidate.id === openItemId);
      const hasExistingCheck = Boolean(item && getPreSessionCheck(item, areaId));
      const targetAreaIds = item ? new Set(getPreSessionTargetAreaIds(state, item)) : new Set();
      if (!item || (!targetAreaIds.has(areaId) && !hasExistingCheck)) {
        sendJson(response, 404, {
          error: "Not found",
          detail: "That Open Item was not found for the specified impacted area.",
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

    if (request.method === "POST" && pathname === "/api/ccb-evaluations/save") {
      const body = await parseBody(request);
      const user = getCurrentUser(request);
      const evaluatorArea = (await loadStateStore()).areas?.find((area) => normalizeEmail(area.email) === normalizeEmail(user?.email))?.id || "";
      const result = await saveCcbEvaluation(String(body.openItemId || "").trim(), {
        email: user?.email || "",
        name: user?.name || "",
        area: evaluatorArea,
      }, body);
      sendJson(response, 200, result);
      return;
    }

    if (request.method === "POST" && pathname === "/api/change-manager/tracking/save") {
      const body = await parseBody(request);
      const user = getCurrentUser(request);
      if (!isChangeManagerUser(user)) {
        sendJson(response, 403, { error: "Not authorized" });
        return;
      }
      const result = await saveImplementationTracking(String(body.openItemId || "").trim(), user, body);
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
      const user = getCurrentUser(request);
      if (!isAdminUser(user)) {
        sendJson(response, 403, { error: "Not authorized" });
        return;
      }
      const nextState = await persistStateStore(createDefaultState());
      sendJson(response, 200, {
        state: nextState,
        prerequisite: derivePrerequisiteStatus(nextState),
        dailyReport: buildDailyReport(nextState),
      });
      return;
    }

    if (request.method === "POST" && pathname === "/api/votes/save") {
      const body = await parseBody(request);
      const user = getCurrentUser(request);
      const result = await saveOpenItemVote(String(body.openItemId || "").trim(), user, body);
      sendJson(response, 200, result);
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
  console.log(`Conceivable CCB running on http://localhost:${PORT}${withBasePath("/")}`);
});

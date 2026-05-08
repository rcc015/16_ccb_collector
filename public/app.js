const state = {
  areas: [],
  openItems: [],
  ccbDecisionCriteria: [],
  ccbEvaluations: [],
  lastSavedAt: null,
  source: null,
};

const prerequisiteCard = document.getElementById("prerequisite-card");
const areasList = document.getElementById("areas-list");
const ownerAreaSelect = document.getElementById("owner-area-select");
const impactedAreasContainer = document.getElementById("impacted-areas");
const openItemForm = document.getElementById("open-item-form");
const openItemIdInput = document.getElementById("open-item-id-input");
const nextOpenItemHint = document.getElementById("next-open-item-hint");
const openItemsList = document.getElementById("open-items-list");
const dailyReport = document.getElementById("daily-report");
const openItemTemplate = document.getElementById("open-item-template");
const csvFileInput = document.getElementById("csv-file-input");
const employeesCsvFileInput = document.getElementById("employees-csv-file-input");
const liveSyncEnabled = document.getElementById("live-sync-enabled");
const liveSyncSourceType = document.getElementById("live-sync-source-type");
const liveSyncSourceUrl = document.getElementById("live-sync-source-url");
const liveSyncIntervalMs = document.getElementById("live-sync-interval-ms");
const liveSyncStatus = document.getElementById("live-sync-status");
const authScreen = document.getElementById("auth-screen");
const appShell = document.getElementById("app-shell");
const authStatus = document.getElementById("auth-status");
const currentUser = document.getElementById("current-user");
const sourceStatus = document.getElementById("source-status");
const employeesImportStatus = document.getElementById("employees-import-status");
const googleSheetsUserAuthStatus = document.getElementById("google-sheets-user-auth-status");
const preSessionOwnerSummary = document.getElementById("presession-owner-summary");
const preSessionPendingList = document.getElementById("presession-pending-list");
const preSessionAnsweredList = document.getElementById("presession-answered-list");
const preSessionJobSummary = document.getElementById("presession-job-summary");
const preSessionJobStatus = document.getElementById("presession-job-status");
const guidelineList = document.getElementById("guideline-list");
const decisionFramework = document.getElementById("decision-framework");
const ownerSuggestions = document.getElementById("owner-suggestions");
const emailSuggestions = document.getElementById("email-suggestions");
const tabButtons = Array.from(document.querySelectorAll("[data-tab]"));
const tabPanels = Array.from(document.querySelectorAll("[data-tab-panel]"));

const OIL_GUIDELINES = [
  {
    title: "OI_ID",
    body: "Usa un identificador unico con formato [Project]_[Type]_[Sequence].",
    example: "OI_E30",
    bullets: ["Type: E = Enhancement", "Type: B = Bug", "Type: C = Compliance"],
  },
  {
    title: "Open Item Description",
    body: "Describe el cambio de forma breve pero accionable.",
    example: "Add two-factor authentication (2FA) to login flow",
    bullets: ["Que cambia", "Donde cambia", "Por que se necesita"],
  },
  {
    title: "Strategic Alignment",
    body: "Explica alineacion con roadmap, objetivos de negocio y posicionamiento.",
    example: "Aligns with 2025 security roadmap",
    bullets: ["1 = No alignment", "3 = Indirect support", "5 = Critical to strategy"],
  },
  {
    title: "Risk Assessment",
    body: "Evalua complejidad tecnica, compliance y efecto al cliente si falla o se retrasa.",
    example: "Low technical risk; critical for GDPR compliance",
    bullets: ["1 = High risk", "5 = Low risk"],
  },
  {
    title: "Resource Impact",
    body: "Cuantifica horas, costo y disrupcion en timeline.",
    example: "120 eng hours; $8k vendor costs",
    bullets: ["Engineering hours", "Cost ($)", "Timeline impact (days/weeks)"],
  },
];

const CCB_FRAMEWORK = [
  {
    id: "strategic-alignment",
    name: "Strategic Alignment",
    weight: 0.3,
    note: "Strategic Alignment must score >=4 for approval.",
    factors: ["Supports product roadmap", "Advances business goals", "Enhances competitive position"],
    scoringGuide: ["1 = Misaligned", "3 = Neutral", "5 = Fully aligned"],
  },
  {
    id: "risk-assessment",
    name: "Risk Assessment",
    weight: 0.25,
    note: "Reject if safety/compliance risk >3.",
    factors: ["Technical complexity", "Safety/compliance risks", "Customer impact if failed"],
    scoringGuide: ["1 = High risk", "3 = Moderate", "5 = Low risk"],
  },
  {
    id: "resource-impact",
    name: "Resource Impact",
    weight: 0.2,
    note: "Flag if score <=2.",
    factors: ["Engineering hours", "Cost (dev, testing, rollout)", "Timeline disruption"],
    scoringGuide: ["1 = Major impact, >20% budget/timeline", "5 = Minimal impact, <5%"],
  },
  {
    id: "customer-value",
    name: "Customer Value",
    weight: 0.15,
    note: "Requires supporting data.",
    factors: ["Solves critical pain points", "Expected adoption/upsell", "CSAT/NPS impact"],
    scoringGuide: ["1 = Low value", "5 = High value / churn reduction"],
  },
  {
    id: "operational-feasibility",
    name: "Operational Feasibility",
    weight: 0.1,
    note: "Ease of execution and supportability.",
    factors: ["Ease of implementation", "Maintenance burden", "Supplier/partner readiness"],
    scoringGuide: ["1 = Not feasible", "5 = Easily executable"],
  },
];

const DECISION_THRESHOLDS = [
  ">= 4.0: APPROVE",
  "3.0-3.9: DEFER (Requires rework)",
  "< 3.0: REJECT",
  "Fast-track: risk = 5 + resource impact = 5 + critical security/legal fix",
];

let authConfig = null;
let importedContacts = [];
let preSessionDashboard = null;
let autoSavePromise = null;
let selectedPreSessionJobOpenItemId = "";
let selectedDailyReportOpenItemId = "";
let selectedOpenItemsStatusTab = "NEW";
let selectedOpenItemId = "";
const expandedEvaluationPanels = new Set();
const deepLinkState = {
  tab: "summary",
  openItemId: "",
  areaId: "",
};
const APP_BASE_PATH = document.documentElement.dataset.basePath || "";

function apiUrl(path) {
  return `${APP_BASE_PATH}${path}`;
}

function normalizeTabId(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (["summary", "items", "presession", "sources", "framework"].includes(normalized)) {
    return normalized;
  }
  return "summary";
}

function applyDeepLinkFromUrl() {
  const url = new URL(window.location.href);
  deepLinkState.tab = normalizeTabId(url.searchParams.get("tab"));
  deepLinkState.openItemId = String(url.searchParams.get("openItemId") || "").trim();
  deepLinkState.areaId = String(url.searchParams.get("areaId") || "").trim();

  if (deepLinkState.openItemId) {
    selectedPreSessionJobOpenItemId = deepLinkState.openItemId;
    selectedOpenItemId = deepLinkState.openItemId;
  }
}

function slugify(value) {
  return value
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function impactedAreaNames(item) {
  return item.impactedAreaIds
    .map((areaId) => state.areas.find((area) => area.id === areaId)?.name || areaId)
    .join(", ");
}

function ownerAreaName(areaId) {
  return state.areas.find((area) => area.id === areaId)?.name || "Unassigned";
}

function collectKnownContacts() {
  const contacts = new Map();
  const pushContact = (name, email) => {
    const normalizedName = String(name || "").trim();
    const normalizedEmail = String(email || "").trim().toLowerCase();
    if (!normalizedName && !normalizedEmail) {
      return;
    }

    const key = normalizedEmail || normalizedName.toLowerCase();
    const existing = contacts.get(key) || { name: "", email: "" };
    contacts.set(key, {
      name: normalizedName || existing.name,
      email: normalizedEmail || existing.email,
    });
  };

  state.areas.forEach((area) => pushContact(area.owner, area.email));
  state.openItems.forEach((item) => pushContact(item.ownerName, item.ownerEmail || ""));
  importedContacts.forEach((contact) => pushContact(contact.name, contact.email));
  if (authConfig?.user) {
    pushContact(authConfig.user.name, authConfig.user.email);
  }

  return Array.from(contacts.values())
    .filter((contact) => contact.name || contact.email)
    .sort((left, right) => (left.name || left.email).localeCompare(right.name || right.email));
}

function renderContactSuggestions() {
  const contacts = collectKnownContacts();
  ownerSuggestions.innerHTML = contacts
    .filter((contact) => contact.name)
    .map((contact) => `<option value="${contact.name}" label="${contact.email || contact.name}"></option>`)
    .join("");
  emailSuggestions.innerHTML = contacts
    .filter((contact) => contact.email)
    .map((contact) => `<option value="${contact.email}" label="${contact.name || contact.email}"></option>`)
    .join("");
}

function findContactByName(value) {
  const normalizedValue = String(value || "").trim().toLowerCase();
  return collectKnownContacts().find((contact) => (contact.name || "").trim().toLowerCase() === normalizedValue) || null;
}

function findContactByEmail(value) {
  const normalizedValue = String(value || "").trim().toLowerCase();
  return collectKnownContacts().find((contact) => (contact.email || "").trim().toLowerCase() === normalizedValue) || null;
}

function getMatchingContacts(query, options = {}) {
  const normalizedQuery = String(query || "").trim().toLowerCase();
  const requireEmail = Boolean(options.requireEmail);
  const contacts = collectKnownContacts().filter((contact) => !requireEmail || contact.email);
  if (!normalizedQuery) {
    return contacts.slice(0, 8);
  }

  return contacts
    .filter((contact) => {
      const name = (contact.name || "").toLowerCase();
      const email = (contact.email || "").toLowerCase();
      return name.includes(normalizedQuery) || email.includes(normalizedQuery);
    })
    .slice(0, 8);
}

function setAreaOwnerSelection(areaId, contact) {
  const area = state.areas.find((candidate) => candidate.id === areaId);
  if (!area || !contact) {
    return;
  }

  area.owner = contact.name || area.owner || "";
  area.email = contact.email || area.email || "";

  const ownerInput = areasList.querySelector(`[data-area-field="owner"][data-area-id="${areaId}"]`);
  const emailInput = areasList.querySelector(`[data-area-field="email"][data-area-id="${areaId}"]`);
  if (ownerInput) {
    ownerInput.value = area.owner;
  }
  if (emailInput) {
    emailInput.value = area.email;
  }
}

function renderContactAutocomplete(dropdown, query, onSelect, emptyMessage = "", options = {}) {
  const matches = getMatchingContacts(query, options);
  if (!matches.length) {
    dropdown.hidden = true;
    dropdown.innerHTML = emptyMessage ? `<div class="autocomplete-empty">${emptyMessage}</div>` : "";
    return;
  }

  dropdown.innerHTML = matches.map((contact, index) => `
    <button
      type="button"
      class="autocomplete-option"
      data-contact-index="${index}"
    >
      <span class="autocomplete-name">${contact.name || contact.email}</span>
      <span class="autocomplete-email">${contact.email || ""}</span>
    </button>
  `).join("");

  dropdown.hidden = false;
  dropdown.querySelectorAll(".autocomplete-option").forEach((button) => {
    button.addEventListener("mousedown", (event) => {
      event.preventDefault();
      const index = Number(button.dataset.contactIndex);
      const selectedContact = matches[index];
      onSelect(selectedContact);
      dropdown.hidden = true;
      dropdown.innerHTML = "";
    });
  });
}

function renderOwnerAutocomplete(dropdown, areaId, query) {
  renderContactAutocomplete(dropdown, query, (selectedContact) => {
    setAreaOwnerSelection(areaId, selectedContact);
  });
}

function detectOpenItemIdPattern() {
  const parsedIds = state.openItems
    .map((item) => {
      const match = String(item.id || "").trim().match(/^([A-Za-z]+)([_-])(\d+)$/);
      if (!match) {
        return null;
      }
      return {
        prefix: match[1].toUpperCase(),
        separator: match[2],
        number: Number(match[3]),
        width: match[3].length,
      };
    })
    .filter(Boolean);

  if (!parsedIds.length) {
    return { prefix: "OI", separator: "_", nextNumber: 1, width: 3 };
  }

  const oiPatternEntries = parsedIds.filter((entry) => entry.prefix === "OI");
  if (oiPatternEntries.length) {
    const maxOiEntry = oiPatternEntries.sort((left, right) => right.number - left.number || right.width - left.width)[0];
    return {
      prefix: "OI",
      separator: maxOiEntry.separator || "_",
      nextNumber: maxOiEntry.number + 1,
      width: Math.max(3, maxOiEntry.width),
    };
  }

  const summaryByPattern = new Map();
  parsedIds.forEach((entry) => {
    const key = `${entry.prefix}${entry.separator}`;
    const current = summaryByPattern.get(key) || {
      prefix: entry.prefix,
      separator: entry.separator,
      maxNumber: 0,
      width: entry.width,
      count: 0,
    };
    current.maxNumber = Math.max(current.maxNumber, entry.number);
    current.width = Math.max(current.width, entry.width);
    current.count += 1;
    summaryByPattern.set(key, current);
  });

  const bestPattern = Array.from(summaryByPattern.values())
    .sort((left, right) => right.count - left.count || right.maxNumber - left.maxNumber)[0];

  return {
    prefix: bestPattern.prefix,
    separator: bestPattern.separator,
    nextNumber: bestPattern.maxNumber + 1,
    width: bestPattern.width,
  };
}

function getNextOpenItemId() {
  const pattern = detectOpenItemIdPattern();
  return `${pattern.prefix}${pattern.separator}${String(pattern.nextNumber).padStart(pattern.width, "0")}`;
}

function updateNextOpenItemSuggestion(forceValue = false) {
  const nextId = getNextOpenItemId();
  nextOpenItemHint.textContent = `Siguiente ID: ${nextId}`;
  openItemIdInput.placeholder = `ID, ej. ${nextId}`;
  if (forceValue || !String(openItemIdInput.value || "").trim()) {
    openItemIdInput.value = nextId;
  }
}

function formatDateTime(value) {
  if (!value) {
    return "N/A";
  }

  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function loadCcbDecisionCriteria() {
  return Array.isArray(state.ccbDecisionCriteria) && state.ccbDecisionCriteria.length
    ? state.ccbDecisionCriteria
    : CCB_FRAMEWORK;
}

function getCurrentEvaluator() {
  const user = authConfig?.user || {};
  const evaluatorArea = state.areas.find((area) => (area.email || "").trim().toLowerCase() === (user.email || "").trim().toLowerCase());
  return {
    email: (user.email || "").trim().toLowerCase(),
    name: (user.name || "").trim(),
    area: evaluatorArea?.id || "",
  };
}

function getEvaluationForUser(openItemId, evaluatorEmail) {
  const normalizedEmail = String(evaluatorEmail || "").trim().toLowerCase();
  return (state.ccbEvaluations || []).filter((entry) => (
    entry.openItemId === openItemId && String(entry.evaluatorEmail || "").trim().toLowerCase() === normalizedEmail
  ));
}

function calculateWeightedScore(evaluation) {
  const criteria = loadCcbDecisionCriteria();
  const byCriterion = new Map((evaluation || []).map((entry) => [entry.criterionId, entry]));
  const allScored = criteria.every((criterion) => {
    const score = Number(byCriterion.get(criterion.id)?.score);
    return Number.isInteger(score) && score >= 1 && score <= 5;
  });
  if (!allScored) {
    return null;
  }
  return Number(criteria.reduce((sum, criterion) => {
    const score = Number(byCriterion.get(criterion.id)?.score || 0);
    return sum + (score * Number(criterion.weight || 0));
  }, 0).toFixed(2));
}

function getDecisionRecommendation(weightedScore) {
  const score = Number(weightedScore);
  if (!Number.isFinite(score)) {
    return "INCOMPLETE";
  }
  if (score >= 4) {
    return "APPROVE";
  }
  if (score >= 3) {
    return "DEFER";
  }
  return "REJECT";
}

function getEvaluationWarnings(openItem, evaluation) {
  const criteria = loadCcbDecisionCriteria();
  const byCriterion = new Map((evaluation || []).map((entry) => [entry.criterionId, entry]));
  const warnings = [];

  criteria.forEach((criterion) => {
    const entry = byCriterion.get(criterion.id) || {};
    const score = Number(entry.score);
    if (!Number.isInteger(score) || score < 1 || score > 5) {
      warnings.push(`${criterion.name}: score required.`);
    }
    if (!String(entry.rationale || "").trim()) {
      warnings.push(`${criterion.name}: rationale required.`);
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

  const riskScore = Number(byCriterion.get("risk-assessment")?.score || 0);
  const searchableText = [
    openItem?.title,
    openItem?.description,
    ...(evaluation || []).map((entry) => `${entry.rationale || ""} ${entry.supportingReference || ""}`),
  ].join(" ").toLowerCase();
  if (riskScore === 5 && resourceScore === 5 && /(critical|security|legal|compliance|regulatory|privacy|gdpr|safety)/i.test(searchableText)) {
    warnings.push("Fast-track candidate");
  }

  return warnings;
}

function buildEvaluationDraft(openItemId, evaluatorEmail) {
  const criteria = loadCcbDecisionCriteria();
  const existing = new Map(getEvaluationForUser(openItemId, evaluatorEmail).map((entry) => [entry.criterionId, entry]));
  return criteria.map((criterion) => {
    const entry = existing.get(criterion.id) || {};
    return {
      criterionId: criterion.id,
      criterionName: criterion.name,
      weight: Number(criterion.weight || 0),
      score: entry.score || "",
      rationale: entry.rationale || "",
      supportingReference: entry.supportingReference || "",
    };
  });
}

function getEvaluationSummary(openItem) {
  const criteria = loadCcbDecisionCriteria();
  const grouped = new Map();
  (state.ccbEvaluations || [])
    .filter((entry) => entry.openItemId === openItem.id)
    .forEach((entry) => {
      const key = String(entry.evaluatorEmail || "").trim().toLowerCase();
      if (!grouped.has(key)) {
        grouped.set(key, []);
      }
      grouped.get(key).push(entry);
    });

  const completeScores = Array.from(grouped.values())
    .map((entries) => calculateWeightedScore(entries))
    .filter((value) => Number.isFinite(value));
  const averageScore = completeScores.length
    ? Number((completeScores.reduce((sum, value) => sum + value, 0) / completeScores.length).toFixed(2))
    : null;
  const currentUserEntries = getEvaluationForUser(openItem.id, getCurrentEvaluator().email);
  const currentUserScore = calculateWeightedScore(currentUserEntries);
  const recommendation = getDecisionRecommendation(averageScore);
  const totalEvaluators = grouped.size;
  const fastTrack = Array.from(grouped.values()).some((entries) => getEvaluationWarnings(openItem, entries).includes("Fast-track candidate"));

  return {
    currentUserScore,
    averageScore,
    totalEvaluators,
    recommendation: totalEvaluators ? recommendation : "INCOMPLETE",
    fastTrack,
    criteriaCount: criteria.length,
  };
}

async function saveCcbEvaluation(openItemId, evaluator, evaluation) {
  const response = await fetch(apiUrl("/api/ccb-evaluations/save"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      openItemId,
      evaluator,
      criteria: evaluation,
    }),
  });
  const payload = await parseApiResponse(response, "No se pudo guardar la evaluacion CCB.");
  Object.assign(state, payload.state);
  return payload;
}

function renderSourceStatus() {
  const source = state.source || {};
  const sourceType = source.type || "local";
  const sourceLabel = {
    "google-sheets": "Google Sheets",
    "google-sheet-snapshot": "Snapshot de Google Sheet",
    local: "Estado local",
  }[sourceType] || sourceType;
  const title = source.spreadsheetTitle || "Sin titulo cargado";
  const sheetName = source.sheetName || "Open Item List";
  const importedAt = source.importedAt || state.lastSavedAt || "";

  sourceStatus.dataset.mode = sourceType;
  sourceStatus.innerHTML = [
    `<div><strong>Fuente activa:</strong> ${sourceLabel}</div>`,
    `<div><strong>Spreadsheet:</strong> ${title}</div>`,
    `<div><strong>Hoja:</strong> ${sheetName}</div>`,
    `<div><strong>Ultima carga:</strong> ${formatDateTime(importedAt)}</div>`,
    `<div><strong>Ultimo guardado:</strong> ${formatDateTime(state.lastSavedAt)}</div>`,
  ].join("");
}

function renderReferencePanels() {
  guidelineList.innerHTML = OIL_GUIDELINES.map((item) => `
    <article class="reference-card">
      <h3>${item.title}</h3>
      <p>${item.body}</p>
      <ul>${item.bullets.map((bullet) => `<li>${bullet}</li>`).join("")}</ul>
      <span class="reference-badge">Ejemplo: ${item.example}</span>
    </article>
  `).join("");

  decisionFramework.innerHTML = [
    ...loadCcbDecisionCriteria().map((item) => `
      <article class="reference-card">
        <h3>${item.name}</h3>
        <p>${item.note}</p>
        <ul>${(item.factors || []).map((bullet) => `<li>${bullet}</li>`).join("")}</ul>
        <span class="reference-badge">Weight ${Number(item.weight || 0).toFixed(2)}</span>
      </article>
    `),
    `
      <article class="reference-card">
        <h3>Decision Thresholds</h3>
        <ul>${DECISION_THRESHOLDS.map((bullet) => `<li>${bullet}</li>`).join("")}</ul>
      </article>
    `,
  ].join("");
}

function renderGoogleSheetsUserAuthStatus(status = authConfig?.googleSheetsAuth || {}) {
  const configuredMode = status.configuredMode || "auto";
  const activeMode = status.mode || "snapshot";
  const configured = status.userOAuthConfigured ? "Si" : "No";
  const connected = status.userOAuthConnected ? "Si" : "No";
  const redirectUri = status.userOAuthRedirectUri || "N/A";

  googleSheetsUserAuthStatus.dataset.mode = activeMode;
  googleSheetsUserAuthStatus.innerHTML = [
    `<div><strong>Modo configurado:</strong> ${configuredMode}</div>`,
    `<div><strong>Modo activo:</strong> ${activeMode}</div>`,
    `<div><strong>OAuth configurado:</strong> ${configured}</div>`,
    `<div><strong>Cuenta conectada:</strong> ${connected}</div>`,
    `<div><strong>Redirect URI:</strong> ${redirectUri}</div>`,
  ].join("");
}

function renderEmployeesImportStatus(directory = {}) {
  const count = Array.isArray(directory.contacts) ? directory.contacts.length : importedContacts.length;
  const sourceName = directory.sourceName || "Sin archivo importado";
  const importedAt = directory.importedAt ? formatDateTime(directory.importedAt) : "Nunca";

  employeesImportStatus.dataset.mode = count ? "google-sheet-snapshot" : "local";
  employeesImportStatus.innerHTML = [
    `<div><strong>Contactos cargados:</strong> ${count}</div>`,
    `<div><strong>Archivo:</strong> ${sourceName}</div>`,
    `<div><strong>Ultima importacion:</strong> ${importedAt}</div>`,
  ].join("");
}

function renderPreSessionDashboard() {
  const ownerView = preSessionDashboard?.ownerView || { ownedAreas: [], pendingItems: [], answeredItems: [] };
  const ownerQueue = Array.isArray(preSessionDashboard?.ownerQueue) ? preSessionDashboard.ownerQueue : [];
  const openItemQueue = Array.isArray(preSessionDashboard?.openItemQueue) ? preSessionDashboard.openItemQueue : [];
  const gmailDeliveryAvailable = Boolean(preSessionDashboard?.gmailDeliveryAvailable);
  const ownedAreas = ownerView.ownedAreas || [];
  const targetOpenItemId = deepLinkState.tab === "presession" ? deepLinkState.openItemId : "";
  const targetAreaId = deepLinkState.tab === "presession" ? deepLinkState.areaId : "";
  const sortTargetFirst = (left, right) => {
    const leftTarget = Number(left.openItemId === targetOpenItemId && (!targetAreaId || left.areaId === targetAreaId));
    const rightTarget = Number(right.openItemId === targetOpenItemId && (!targetAreaId || right.areaId === targetAreaId));
    return rightTarget - leftTarget;
  };
  const pendingItems = [...(ownerView.pendingItems || [])].sort(sortTargetFirst);
  const answeredItems = [...(ownerView.answeredItems || [])].sort(sortTargetFirst);

  preSessionOwnerSummary.dataset.mode = ownedAreas.length ? "google-sheet-snapshot" : "local";
  preSessionOwnerSummary.innerHTML = ownedAreas.length
    ? [
        `<div><strong>Areas detectadas:</strong> ${ownedAreas.map((area) => escapeHtml(area.name)).join(", ")}</div>`,
        `<div><strong>Pendientes:</strong> ${pendingItems.length}</div>`,
        `<div><strong>Respondidos:</strong> ${answeredItems.length}</div>`,
      ].join("")
    : "<div><strong>No encontramos areas</strong> asociadas a tu correo en CCB Areas.</div>";

  preSessionPendingList.innerHTML = pendingItems.length
    ? pendingItems.map((item) => `
        <article class="presession-card${item.openItemId === targetOpenItemId && (!targetAreaId || item.areaId === targetAreaId) ? " is-targeted" : ""}">
          <div class="presession-card-top">
            <div>
              <p class="presession-eyebrow">${escapeHtml(item.areaName)}</p>
              <h3>${escapeHtml(item.openItemId)} · ${escapeHtml(item.title)}</h3>
            </div>
            <span class="badge">${escapeHtml(item.externalStatus || item.itemStatus || "pending")}</span>
          </div>
          <p>${escapeHtml(item.description || "Sin descripcion")}</p>
          <p class="meta-line">Open item nuevo o pendiente para tu area. ${item.requestedAt ? `Solicitado: ${escapeHtml(formatDateTime(item.requestedAt))}.` : "Aun no se ha enviado solicitud."}</p>
          <textarea data-presession-comment data-open-item-id="${escapeHtml(item.openItemId)}" data-area-id="${escapeHtml(item.areaId)}" rows="3" placeholder="Comentario opcional para tu revision"></textarea>
          <div class="decision-row">
            <button type="button" data-presession-respond data-decision="impact" data-open-item-id="${escapeHtml(item.openItemId)}" data-area-id="${escapeHtml(item.areaId)}">Si impacta</button>
            <button type="button" class="secondary" data-presession-respond data-decision="no-impact" data-open-item-id="${escapeHtml(item.openItemId)}" data-area-id="${escapeHtml(item.areaId)}">No impacta</button>
          </div>
        </article>
      `).join("")
    : "<p class=\"meta-line\">No tienes open items pendientes por responder.</p>";

  preSessionAnsweredList.innerHTML = answeredItems.length
    ? answeredItems.map((item) => `
        <article class="presession-card${item.openItemId === targetOpenItemId && (!targetAreaId || item.areaId === targetAreaId) ? " is-targeted" : ""}">
          <div class="presession-card-top">
            <div>
              <p class="presession-eyebrow">${escapeHtml(item.areaName)}</p>
              <h3>${escapeHtml(item.openItemId)} · ${escapeHtml(item.title)}</h3>
            </div>
            <span class="decision-pill" data-decision="${escapeHtml(item.decision)}">${item.decision === "impact" ? "Si impacta" : "No impacta"}</span>
          </div>
          <p class="meta-line">Respondido: ${escapeHtml(formatDateTime(item.respondedAt))}</p>
          <p>${escapeHtml(item.comment || "Sin comentario")}</p>
        </article>
      `).join("")
    : "<p class=\"meta-line\">Todavia no has respondido revisiones.</p>";

  preSessionJobSummary.dataset.mode = gmailDeliveryAvailable ? "google-sheets" : "local";
  const totalPendingEvaluations = openItemQueue.reduce((total, item) => total + item.pendingOwners.length, 0);
  preSessionJobSummary.innerHTML = [
    `<div><strong>Owners pendientes:</strong> ${ownerQueue.length}</div>`,
    `<div><strong>Open items con evaluacion pendiente:</strong> ${openItemQueue.length}</div>`,
    `<div><strong>Evaluaciones pendientes:</strong> ${totalPendingEvaluations}</div>`,
    `<div><strong>Envio Gmail:</strong> ${gmailDeliveryAvailable ? "Disponible con tu OAuth conectado" : "No disponible, el job generara preview si falta autorizacion"}</div>`,
  ].join("");

  if (openItemQueue.length && !openItemQueue.some((item) => item.openItemId === selectedPreSessionJobOpenItemId)) {
    selectedPreSessionJobOpenItemId = openItemQueue[0].openItemId;
  }
  if (!openItemQueue.length) {
    selectedPreSessionJobOpenItemId = "";
  }

  const selectedJobItem = openItemQueue.find((item) => item.openItemId === selectedPreSessionJobOpenItemId) || openItemQueue[0] || null;

  preSessionJobStatus.innerHTML = openItemQueue.length
    ? `
        <div class="presession-job-tabs" role="tablist" aria-label="Open items pendientes">
          ${openItemQueue.map((item) => `
            <button
              type="button"
              class="presession-job-tab${selectedJobItem?.openItemId === item.openItemId ? " is-active" : ""}"
              data-presession-job-tab="${escapeHtml(item.openItemId)}"
            >
              <span>${escapeHtml(item.openItemId)}</span>
              <strong>${item.pendingOwners.length}</strong>
            </button>
          `).join("")}
        </div>
        <article class="presession-job-card">
          <div class="presession-card-top">
            <div>
              <p class="presession-eyebrow">${escapeHtml(selectedJobItem.externalStatus || selectedJobItem.itemStatus || "open")}</p>
              <h3>${escapeHtml(selectedJobItem.openItemId)} · ${escapeHtml(selectedJobItem.title)}</h3>
            </div>
            <span class="badge">${selectedJobItem.pendingOwners.length} pendiente(s)</span>
          </div>
          <div class="presession-owner-list presession-owner-list--matrix">
            ${selectedJobItem.pendingOwners.map((owner) => `
              <div class="presession-owner-row presession-owner-row--compact">
                <strong>${escapeHtml(owner.areaName)}</strong>
                <span>${escapeHtml(owner.ownerName || owner.ownerEmail || "Sin owner")}</span>
              </div>
            `).join("")}
          </div>
        </article>
      `
    : "<p class=\"meta-line\">No hay owners con solicitudes pendientes.</p>";

  preSessionJobStatus.querySelectorAll("[data-presession-job-tab]").forEach((button) => {
    button.addEventListener("click", () => {
      selectedPreSessionJobOpenItemId = button.dataset.presessionJobTab || "";
      renderPreSessionDashboard();
    });
  });

  preSessionPendingList.querySelectorAll("[data-presession-respond]").forEach((button) => {
    button.addEventListener("click", async () => {
      const openItemId = button.dataset.openItemId;
      const areaId = button.dataset.areaId;
      const decision = button.dataset.decision;
      const commentField = preSessionPendingList.querySelector(
        `[data-presession-comment][data-open-item-id="${openItemId}"][data-area-id="${areaId}"]`,
      );
      const comment = commentField ? commentField.value.trim() : "";

      try {
        const response = await fetch(apiUrl("/api/pre-session/respond"), {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            openItemId,
            areaId,
            decision,
            comment,
          }),
        });
        const payload = await parseApiResponse(response, "No se pudo guardar tu revision de pre-sesion.");
        Object.assign(state, payload.state);
        preSessionDashboard = payload.dashboard;
        preSessionJobStatus.textContent = `Revision guardada para ${openItemId} / ${areaId}.`;
        renderPreSessionDashboard();
      } catch (error) {
        window.alert(error.message || "No se pudo guardar tu revision de pre-sesion.");
      }
    });
  });
}

function activateTab(tabId) {
  tabButtons.forEach((button) => {
    button.classList.toggle("is-active", button.dataset.tab === tabId);
  });

  tabPanels.forEach((panel) => {
    panel.classList.toggle("is-active", panel.dataset.tabPanel === tabId);
  });

  if (tabId === "items") {
    updateNextOpenItemSuggestion(true);
  }
}

async function parseApiResponse(response, fallbackMessage) {
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.detail || payload.error || fallbackMessage);
  }
  return payload;
}

function formatOwnerLine(item) {
  const chunks = [];

  if (item.ownerName) {
    chunks.push(`Open item owner: ${item.ownerName}`);
  }

  if (item.ownerAreaId) {
    chunks.push(`Primary area: ${ownerAreaName(item.ownerAreaId)}`);
  }

  if (item.ccbScore || item.externalStatus) {
    chunks.push(`CCB: ${item.ccbScore || "N/A"} / ${item.externalStatus || "N/A"}`);
  }

  return chunks.join(" · ");
}

function renderPrerequisite(prerequisite) {
  prerequisiteCard.dataset.status = prerequisite.status;
  prerequisiteCard.innerHTML = `
    <p class="eyebrow">Prerequisito</p>
    <h2>${prerequisite.label}</h2>
    <p>${prerequisite.detail}</p>
    <p class="meta-line">Bloqueados: ${prerequisite.blockedItems.length}</p>
  `;
}

function renderAreas() {
  areasList.innerHTML = "";
  ownerAreaSelect.innerHTML = "";
  impactedAreasContainer.innerHTML = "";

  state.areas.forEach((area) => {
    const card = document.createElement("article");
    card.className = "area-editor-card";
    card.innerHTML = `
      <div class="area-editor-head">
        <div class="area-editor-title"></div>
        <span class="chip">${area.id}</span>
      </div>
      <div class="area-editor-grid">
        <div class="autocomplete-shell">
          <input
            data-area-field="owner"
            data-area-id="${area.id}"
            data-role="owner"
            placeholder="Owner / assignee"
            value="${area.owner || ""}"
            autocomplete="off"
            title="${area.email || ""}"
          />
          <div class="autocomplete-list" data-owner-autocomplete="${area.id}" hidden></div>
        </div>
      </div>
    `;
    areasList.appendChild(card);

    const option = document.createElement("option");
    option.value = area.id;
    option.textContent = area.name;
    ownerAreaSelect.appendChild(option);

    const label = document.createElement("label");
    label.className = "checkbox";
    label.innerHTML = `<input type="checkbox" value="${area.id}" /> ${area.name}`;
    impactedAreasContainer.appendChild(label);
  });

  areasList.querySelectorAll("[data-area-field]").forEach((input) => {
    input.addEventListener("input", (event) => {
      const areaId = event.currentTarget.dataset.areaId;
      const field = event.currentTarget.dataset.areaField;
      const area = state.areas.find((candidate) => candidate.id === areaId);
      if (!area || !field) {
        return;
      }
      area[field] = event.currentTarget.value.trim();

      if (field === "owner") {
        const contact = findContactByName(area.owner);
        if (contact?.email) {
          area.email = contact.email;
          event.currentTarget.title = contact.email;
        } else {
          event.currentTarget.title = area.email || "";
        }

        if (!contact?.email) {
          const contactByEmail = findContactByEmail(area.email);
          if (!contactByEmail) {
            area.email = area.email || "";
          }
        }

        const dropdown = areasList.querySelector(`[data-owner-autocomplete="${area.id}"]`);
        if (dropdown) {
          renderOwnerAutocomplete(dropdown, area.id, area.owner);
        }
      }
    });

    input.addEventListener("focus", (event) => {
      const field = event.currentTarget.dataset.areaField;
      const areaId = event.currentTarget.dataset.areaId;
      if (field !== "owner" || !areaId) {
        return;
      }
      const dropdown = areasList.querySelector(`[data-owner-autocomplete="${areaId}"]`);
      if (dropdown) {
        renderOwnerAutocomplete(dropdown, areaId, event.currentTarget.value);
      }
    });

    input.addEventListener("blur", (event) => {
      const field = event.currentTarget.dataset.areaField;
      const areaId = event.currentTarget.dataset.areaId;
      if (field !== "owner" || !areaId) {
        return;
      }
      const dropdown = areasList.querySelector(`[data-owner-autocomplete="${areaId}"]`);
      if (dropdown) {
        window.setTimeout(() => {
          dropdown.hidden = true;
        }, 120);
      }
    });
  });
}

function renderVotes(votes) {
  if (!votes.length) {
    return "<p class=\"meta-line\">Sin votos registrados.</p>";
  }

  return votes
    .map((vote) => {
      const area = state.areas.find((candidate) => candidate.id === vote.areaId);
      const owner = area ? `${area.name} · ${area.owner}` : vote.areaId;
      return `
        <div class="vote-pill">
          <div><strong data-decision="${vote.decision}">${vote.decision}</strong> · ${owner}</div>
          <div>${vote.comment || "Sin comentario"}</div>
        </div>
      `;
    })
    .join("");
}

function renderEvaluationSummary(openItem) {
  const summary = getEvaluationSummary(openItem);
  const currentUserScoreLabel = Number.isFinite(summary.currentUserScore) ? summary.currentUserScore.toFixed(2) : "N/A";
  const averageScoreLabel = Number.isFinite(summary.averageScore) ? summary.averageScore.toFixed(2) : "N/A";
  return `
    <div class="evaluation-summary">
      <div class="evaluation-summary-grid">
        <div><span>My score</span><strong>${currentUserScoreLabel}</strong></div>
        <div><span>Average</span><strong>${averageScoreLabel}</strong></div>
        <div><span>Evaluators</span><strong>${summary.totalEvaluators}</strong></div>
        <div><span>Recommendation</span><strong>${summary.recommendation}</strong></div>
      </div>
      <div class="evaluation-summary-actions">
        <span class="decision-pill" data-decision="${summary.recommendation.toLowerCase()}">${summary.recommendation}</span>
        ${summary.fastTrack ? '<span class="reference-badge">Fast-track candidate</span>' : ""}
      </div>
    </div>
  `;
}

function renderOpenItems() {
  openItemsList.innerHTML = "";

  const activeItems = state.openItems
    .filter((item) => item.status !== "closed")
    .slice()
    .sort((a, b) => Number(b.isSubstantial) - Number(a.isSubstantial) || a.id.localeCompare(b.id));
  const groupedItems = {
    NEW: activeItems.filter((item) => String(item.externalStatus || "").trim().toUpperCase() === "NEW"),
    APPROVED: activeItems.filter((item) => String(item.externalStatus || "").trim().toUpperCase() === "APPROVED"),
    REJECTED: activeItems.filter((item) => String(item.externalStatus || "").trim().toUpperCase() === "REJECTED"),
  };

  const availableStatusTabs = Object.entries(groupedItems).filter(([, items]) => items.length > 0);
  if (!availableStatusTabs.some(([status]) => status === selectedOpenItemsStatusTab)) {
    selectedOpenItemsStatusTab = availableStatusTabs[0]?.[0] || "NEW";
  }

  const selectedStatusItems = groupedItems[selectedOpenItemsStatusTab] || [];
  if (selectedStatusItems.length && !selectedStatusItems.some((item) => item.id === selectedOpenItemId)) {
    selectedOpenItemId = selectedStatusItems[0].id;
  }
  if (!selectedStatusItems.length) {
    selectedOpenItemId = "";
  }

  const selectedItem = selectedStatusItems.find((item) => item.id === selectedOpenItemId) || selectedStatusItems[0] || null;

  const renderOpenItemCard = (item) => {
    const node = openItemTemplate.content.firstElementChild.cloneNode(true);
    node.querySelector(".item-id").textContent = item.id;
    node.querySelector(".item-title").textContent = item.title;
    node.querySelector(".badge").textContent = item.isSubstantial ? "Substantial" : item.status;
    node.querySelector(".item-description").textContent = item.description || "Sin descripcion";
    node.querySelector(".meta-line").textContent = formatOwnerLine(item) || `Source: ${item.sourceRef || "N/A"}`;
    node.querySelector(".impact-line").textContent = `Impacto: ${impactedAreaNames(item) || "Sin areas impactadas"}`;
    node.querySelector(".vote-list").innerHTML = renderVotes(item.votes);

    const ownerInput = node.querySelector("[data-open-item-owner-input]");
    const ownerDropdown = node.querySelector("[data-open-item-owner-autocomplete]");
    let lastCommittedOwner = item.ownerName || "";
    let lastCommittedOwnerEmail = item.ownerEmail || "";
    const applyOpenItemOwnerSelection = async (selectedContact) => {
      const nextOwner = (selectedContact?.name || selectedContact?.email || "").trim();
      const nextOwnerEmail = (selectedContact?.email || "").trim().toLowerCase();
      if (!nextOwner || (nextOwner === lastCommittedOwner && nextOwnerEmail === lastCommittedOwnerEmail)) {
        ownerInput.value = nextOwner || ownerInput.value;
        return;
      }

      item.ownerName = nextOwner;
      item.ownerEmail = nextOwnerEmail;
      ownerInput.value = nextOwner;
      node.querySelector(".meta-line").textContent = formatOwnerLine(item) || `Source: ${item.sourceRef || "N/A"}`;

      try {
        await saveStateSilently();
        lastCommittedOwner = item.ownerName || nextOwner;
        lastCommittedOwnerEmail = item.ownerEmail || nextOwnerEmail;
      } catch (error) {
        item.ownerName = lastCommittedOwner;
        item.ownerEmail = lastCommittedOwnerEmail;
        ownerInput.value = lastCommittedOwner;
        node.querySelector(".meta-line").textContent = formatOwnerLine(item) || `Source: ${item.sourceRef || "N/A"}`;
        window.alert(error.message || "No se pudo guardar el open item owner.");
      }
    };

    ownerInput.value = item.ownerName || "";
    ownerInput.addEventListener("input", (event) => {
      item.ownerName = event.currentTarget.value.trim();
      item.ownerEmail = "";
      node.querySelector(".meta-line").textContent = formatOwnerLine(item) || `Source: ${item.sourceRef || "N/A"}`;
      renderContactAutocomplete(
        ownerDropdown,
        item.ownerName,
        applyOpenItemOwnerSelection,
        "Sin coincidencias en el directorio de empleados.",
        { requireEmail: true },
      );
    });
    ownerInput.addEventListener("focus", (event) => {
      renderContactAutocomplete(
        ownerDropdown,
        event.currentTarget.value,
        applyOpenItemOwnerSelection,
        "Sin coincidencias en el directorio de empleados.",
        { requireEmail: true },
      );
    });
    ownerInput.addEventListener("blur", () => {
      const exactContact = findContactByName(ownerInput.value) || findContactByEmail(ownerInput.value);
      if (exactContact?.email) {
        applyOpenItemOwnerSelection(exactContact);
      } else {
        item.ownerName = lastCommittedOwner;
        item.ownerEmail = lastCommittedOwnerEmail;
        ownerInput.value = lastCommittedOwner;
        node.querySelector(".meta-line").textContent = formatOwnerLine(item) || `Source: ${item.sourceRef || "N/A"}`;
      }
      window.setTimeout(() => {
        ownerDropdown.hidden = true;
      }, 120);
    });

    const voteAreaSelect = node.querySelector("select[name=\"areaId\"]");
    state.areas.forEach((area) => {
      const option = document.createElement("option");
      option.value = area.id;
      option.textContent = area.name;
      voteAreaSelect.appendChild(option);
    });

    node.querySelector(".vote-form").addEventListener("submit", (event) => {
      event.preventDefault();
      const formData = new FormData(event.currentTarget);
      item.votes.push({
        areaId: formData.get("areaId"),
        decision: formData.get("decision"),
        comment: String(formData.get("comment") || "").trim(),
        createdAt: new Date().toISOString(),
      });
      refreshDerivedViews();
    });

    node.querySelector(".close-item-button").addEventListener("click", () => {
      item.status = "closed";
      refreshDerivedViews();
    });

    const evaluationSummaryHost = node.querySelector("[data-open-item-evaluation-summary]");
    const evaluationToggleButton = node.querySelector("[data-open-item-evaluation-toggle]");
    const evaluationPanel = node.querySelector("[data-open-item-evaluation-panel]");
    const evaluator = getCurrentEvaluator();
    const criteria = loadCcbDecisionCriteria();
    let evaluationDraft = buildEvaluationDraft(item.id, evaluator.email);
    const renderEvaluationPanel = () => {
      const weightedScore = calculateWeightedScore(evaluationDraft);
      const recommendation = getDecisionRecommendation(weightedScore);
      const warnings = getEvaluationWarnings(item, evaluationDraft);
      evaluationSummaryHost.innerHTML = renderEvaluationSummary(item);

      evaluationPanel.innerHTML = `
        <div class="evaluation-panel-shell">
          <p class="meta-line">Score each criterion using the official CCB Decision criteria. The weighted score will drive the recommendation, but final CCB approval remains a board decision.</p>
          <div class="evaluation-overview">
            <div><span>Total weighted score</span><strong>${Number.isFinite(weightedScore) ? weightedScore.toFixed(2) : "Incomplete"}</strong></div>
            <div><span>Recommendation</span><strong>${recommendation}</strong></div>
          </div>
          ${warnings.length ? `<div class="evaluation-warning-list">${warnings.map((warning) => `<div class="evaluation-warning">${escapeHtml(warning)}</div>`).join("")}</div>` : ""}
          <div class="evaluation-criteria-list">
            ${criteria.map((criterion) => {
              const entry = evaluationDraft.find((candidate) => candidate.criterionId === criterion.id) || {};
              return `
                <section class="evaluation-criterion-card" data-criterion-id="${escapeHtml(criterion.id)}">
                  <div class="evaluation-criterion-head">
                    <div>
                      <h4>${escapeHtml(criterion.name)}</h4>
                      <p>${escapeHtml(criterion.note || "")}</p>
                    </div>
                    <span class="reference-badge">${Math.round(Number(criterion.weight || 0) * 100)}%</span>
                  </div>
                  <div class="evaluation-criterion-columns">
                    <div class="evaluation-copy">
                      <p class="field-label">Factors</p>
                      <ul>${(criterion.factors || []).map((factor) => `<li>${escapeHtml(factor)}</li>`).join("")}</ul>
                    </div>
                    <div class="evaluation-copy">
                      <p class="field-label">Scoring guide</p>
                      <ul>${(criterion.scoringGuide || []).map((guide) => `<li>${escapeHtml(`${guide.score} = ${guide.label}`)}</li>`).join("")}</ul>
                    </div>
                  </div>
                  <div class="evaluation-score-row">
                    <label class="field-label">Score</label>
                    <div class="evaluation-score-group">
                      ${[1, 2, 3, 4, 5].map((score) => `
                        <button
                          type="button"
                          class="evaluation-score-button${Number(entry.score) === score ? " is-active" : ""}"
                          data-evaluation-score
                          data-criterion-id="${escapeHtml(criterion.id)}"
                          data-score="${score}"
                        >${score}</button>
                      `).join("")}
                    </div>
                  </div>
                  <div class="stack">
                    <div>
                      <p class="field-label">Rationale</p>
                      <textarea data-evaluation-rationale data-criterion-id="${escapeHtml(criterion.id)}" rows="3" placeholder="Required rationale">${escapeHtml(entry.rationale || "")}</textarea>
                    </div>
                    <div>
                      <p class="field-label">Supporting data / reference</p>
                      <input data-evaluation-supporting data-criterion-id="${escapeHtml(criterion.id)}" value="${escapeHtml(entry.supportingReference || "")}" placeholder="Optional supporting reference" />
                    </div>
                  </div>
                </section>
              `;
            }).join("")}
          </div>
          <button type="button" data-save-evaluation>Save evaluation</button>
        </div>
      `;

      const updateDraft = () => {
        criteria.forEach((criterion) => {
          const draftEntry = evaluationDraft.find((candidate) => candidate.criterionId === criterion.id);
          const activeScoreButton = evaluationPanel.querySelector(`[data-evaluation-score][data-criterion-id="${criterion.id}"].is-active`);
          draftEntry.score = activeScoreButton ? Number(activeScoreButton.dataset.score) : "";
          draftEntry.rationale = evaluationPanel.querySelector(`[data-evaluation-rationale][data-criterion-id="${criterion.id}"]`)?.value.trim() || "";
          draftEntry.supportingReference = evaluationPanel.querySelector(`[data-evaluation-supporting][data-criterion-id="${criterion.id}"]`)?.value.trim() || "";
        });
      };

      evaluationPanel.querySelectorAll("[data-evaluation-score]").forEach((button) => {
        button.addEventListener("click", () => {
          const criterionId = button.dataset.criterionId;
          updateDraft();
          evaluationPanel.querySelectorAll(`[data-evaluation-score][data-criterion-id="${criterionId}"]`).forEach((candidate) => candidate.classList.remove("is-active"));
          button.classList.add("is-active");
          updateDraft();
          renderEvaluationPanel();
        });
      });

      evaluationPanel.querySelector("[data-save-evaluation]")?.addEventListener("click", async () => {
        updateDraft();
        try {
          const payload = await saveCcbEvaluation(item.id, evaluator, evaluationDraft);
          Object.assign(state, payload.state);
          renderReferencePanels();
          renderOpenItems();
        } catch (error) {
          window.alert(error.message || "No se pudo guardar la evaluacion.");
        }
      });
    };

    evaluationSummaryHost.innerHTML = renderEvaluationSummary(item);
    const isExpanded = expandedEvaluationPanels.has(item.id);
    evaluationPanel.hidden = !isExpanded;
    if (isExpanded) {
      renderEvaluationPanel();
    }
    evaluationToggleButton.addEventListener("click", () => {
      if (expandedEvaluationPanels.has(item.id)) {
        expandedEvaluationPanels.delete(item.id);
        evaluationPanel.hidden = true;
        return;
      }
      expandedEvaluationPanels.add(item.id);
      evaluationPanel.hidden = false;
      renderEvaluationPanel();
    });

    return node;
  };

  if (!availableStatusTabs.length) {
    openItemsList.innerHTML = "<p class=\"meta-line\">No hay open items activos.</p>";
    return;
  }

  openItemsList.innerHTML = `
    <div class="open-items-shell">
      <div class="open-items-status-tabs" role="tablist" aria-label="Estados de open items">
        ${availableStatusTabs.map(([status, items]) => `
          <button
            type="button"
            class="open-items-status-tab${selectedOpenItemsStatusTab === status ? " is-active" : ""}"
            data-open-items-status-tab="${escapeHtml(status)}"
          >
            <span>${escapeHtml(status)}</span>
            <strong>${items.length}</strong>
          </button>
        `).join("")}
      </div>
      <div class="open-items-ticket-tabs" role="tablist" aria-label="Open items">
        ${selectedStatusItems.map((item) => `
          <button
            type="button"
            class="open-items-ticket-tab${selectedItem?.id === item.id ? " is-active" : ""}"
            data-open-item-ticket-tab="${escapeHtml(item.id)}"
          >
            <span>${escapeHtml(item.id)}</span>
            <strong>${escapeHtml(item.title)}</strong>
          </button>
        `).join("")}
      </div>
      <div class="open-items-active-card"></div>
    </div>
  `;

  openItemsList.querySelectorAll("[data-open-items-status-tab]").forEach((button) => {
    button.addEventListener("click", () => {
      selectedOpenItemsStatusTab = button.dataset.openItemsStatusTab || "NEW";
      selectedOpenItemId = "";
      renderOpenItems();
    });
  });

  openItemsList.querySelectorAll("[data-open-item-ticket-tab]").forEach((button) => {
    button.addEventListener("click", () => {
      selectedOpenItemId = button.dataset.openItemTicketTab || "";
      renderOpenItems();
    });
  });

  const cardHost = openItemsList.querySelector(".open-items-active-card");
  if (cardHost && selectedItem) {
    cardHost.appendChild(renderOpenItemCard(selectedItem));
  }
}

function buildDailyReportText(prerequisite) {
  const today = new Date().toISOString().slice(0, 10);
  const todaysVotes = [];

  state.openItems.forEach((item) => {
    item.votes.forEach((vote) => {
      if ((vote.createdAt || "").startsWith(today)) {
        const area = state.areas.find((candidate) => candidate.id === vote.areaId);
        todaysVotes.push(`- ${item.id} / ${item.title}: ${vote.decision} by ${area?.name || vote.areaId} (${area?.owner || "Unknown"})`);
      }
    });
  });

  const ccbAgenda = state.openItems
    .filter((item) => item.status !== "closed")
    .filter((item) => item.isSubstantial || item.votes.some((vote) => vote.decision !== "approve"))
    .map((item) => `- ${item.id}: ${item.title} | Impacto: ${impactedAreaNames(item)}`);

  return [
    `Fecha: ${today}`,
    `Prerequisito: ${prerequisite.label}`,
    "",
    "Votaciones nuevas del dia:",
    todaysVotes.length ? todaysVotes.join("\n") : "- Sin votaciones nuevas",
    "",
    "Lista propuesta para sesion CCB:",
    ccbAgenda.length ? ccbAgenda.join("\n") : "- Sin temas pendientes",
  ].join("\n");
}

function buildDailyReportModel(prerequisite) {
  const today = new Date().toISOString().slice(0, 10);
  const todaysVotes = [];

  state.openItems.forEach((item) => {
    item.votes.forEach((vote) => {
      if ((vote.createdAt || "").startsWith(today)) {
        const area = state.areas.find((candidate) => candidate.id === vote.areaId);
        todaysVotes.push({
          openItemId: item.id,
          title: item.title,
          decision: vote.decision,
          areaName: area?.name || vote.areaId,
          ownerName: area?.owner || "Unknown",
        });
      }
    });
  });

  const ccbAgenda = state.openItems
    .filter((item) => item.status !== "closed")
    .filter((item) => item.isSubstantial || item.votes.some((vote) => vote.decision !== "approve"))
    .map((item) => {
      const impactedAreas = impactedAreaNames(item).split(", ").filter(Boolean);
      const pendingAreaIds = item.impactedAreaIds.filter(
        (areaId) => !item.votes.some((vote) => vote.areaId === areaId && vote.decision === "approve"),
      );
      const pendingAreas = pendingAreaIds
        .map((areaId) => state.areas.find((area) => area.id === areaId)?.name || areaId)
        .filter(Boolean);

      return {
        openItemId: item.id,
        title: item.title,
        impactedAreas,
        pendingAreas,
        isSubstantial: Boolean(item.isSubstantial),
      };
    });

  return {
    date: today,
    prerequisite,
    todaysVotes,
    ccbAgenda,
  };
}

function renderDailyReport(prerequisite) {
  const report = buildDailyReportModel(prerequisite);
  dailyReport.dataset.copyText = buildDailyReportText(prerequisite);

  if (report.ccbAgenda.length && !report.ccbAgenda.some((item) => item.openItemId === selectedDailyReportOpenItemId)) {
    selectedDailyReportOpenItemId = report.ccbAgenda[0].openItemId;
  }
  if (!report.ccbAgenda.length) {
    selectedDailyReportOpenItemId = "";
  }

  const selectedAgendaItem = report.ccbAgenda.find((item) => item.openItemId === selectedDailyReportOpenItemId) || report.ccbAgenda[0] || null;

  dailyReport.innerHTML = `
    <div class="daily-report-shell">
      <div class="daily-report-meta">
        <span><strong>Fecha:</strong> ${escapeHtml(report.date)}</span>
        <span><strong>Prerequisito:</strong> ${escapeHtml(report.prerequisite.label)}</span>
        <span><strong>Votaciones nuevas:</strong> ${report.todaysVotes.length}</span>
        <span><strong>Tickets CCB:</strong> ${report.ccbAgenda.length}</span>
      </div>
      ${report.todaysVotes.length ? `
        <div class="daily-report-votes">
          ${report.todaysVotes.map((vote) => `
            <div class="daily-report-vote">
              <strong>${escapeHtml(vote.openItemId)}</strong>
              <span>${escapeHtml(vote.areaName)} · ${escapeHtml(vote.decision)}</span>
            </div>
          `).join("")}
        </div>
      ` : "<p class=\"meta-line\">Sin votaciones nuevas hoy.</p>"}
      ${selectedAgendaItem ? `
        <div class="daily-report-tabs" role="tablist" aria-label="Agenda CCB">
          ${report.ccbAgenda.map((item) => `
            <button
              type="button"
              class="daily-report-tab${selectedAgendaItem.openItemId === item.openItemId ? " is-active" : ""}"
              data-daily-report-tab="${escapeHtml(item.openItemId)}"
            >
              <span>${escapeHtml(item.openItemId)}</span>
              <strong>${item.pendingAreas.length || item.impactedAreas.length}</strong>
            </button>
          `).join("")}
        </div>
        <article class="daily-report-card">
          <div class="presession-card-top">
            <div>
              <p class="presession-eyebrow">${selectedAgendaItem.isSubstantial ? "SUBSTANTIAL" : "CCB"}</p>
              <h3>${escapeHtml(selectedAgendaItem.openItemId)} · ${escapeHtml(selectedAgendaItem.title)}</h3>
            </div>
            <span class="badge">${selectedAgendaItem.pendingAreas.length || selectedAgendaItem.impactedAreas.length} area(s)</span>
          </div>
          <div class="daily-report-columns">
            <div class="daily-report-column">
              <p class="field-label">Impacto</p>
              <div class="daily-report-chip-list">
                ${(selectedAgendaItem.impactedAreas.length ? selectedAgendaItem.impactedAreas : ["Sin areas"]).map((area) => `<span class="chip">${escapeHtml(area)}</span>`).join("")}
              </div>
            </div>
            <div class="daily-report-column">
              <p class="field-label">Pendientes</p>
              <div class="daily-report-chip-list">
                ${(selectedAgendaItem.pendingAreas.length ? selectedAgendaItem.pendingAreas : ["Sin pendientes"]).map((area) => `<span class="chip">${escapeHtml(area)}</span>`).join("")}
              </div>
            </div>
          </div>
        </article>
      ` : "<p class=\"meta-line\">Sin temas pendientes para sesion CCB.</p>"}
    </div>
  `;

  dailyReport.querySelectorAll("[data-daily-report-tab]").forEach((button) => {
    button.addEventListener("click", () => {
      selectedDailyReportOpenItemId = button.dataset.dailyReportTab || "";
      renderDailyReport(prerequisite);
    });
  });
}

function derivePrerequisiteLocally() {
  const activeItems = state.openItems.filter((item) => item.status !== "closed");
  const substantialItems = activeItems.filter((item) => item.isSubstantial);
  const blockedItems = substantialItems.filter((item) => {
    const approvedAreaIds = new Set(item.votes.filter((vote) => vote.decision === "approve").map((vote) => vote.areaId));
    return item.impactedAreaIds.some((areaId) => !approvedAreaIds.has(areaId));
  });

  if (blockedItems.length) {
    return {
      status: "blocked",
      label: "No cumple prerequisito",
      detail: "Existen cambios sustanciales abiertos con aprobaciones pendientes.",
      blockedItems: blockedItems.map((item) => item.id),
    };
  }

  if (substantialItems.length) {
    return {
      status: "review",
      label: "Requiere revision",
      detail: "Existen cambios sustanciales abiertos ya aprobados por las areas impactadas.",
      blockedItems: [],
    };
  }

  return {
    status: "clear",
    label: "Cumple prerequisito",
    detail: "No existen cambios sustanciales abiertos.",
    blockedItems: [],
  };
}

function refreshDerivedViews() {
  const prerequisite = derivePrerequisiteLocally();
  renderPrerequisite(prerequisite);
  renderContactSuggestions();
  renderAreas();
  renderOpenItems();
  renderSourceStatus();
  updateNextOpenItemSuggestion();
  renderDailyReport(prerequisite);
}

async function saveState() {
  const response = await fetch(apiUrl("/api/state"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(state),
  });
  const payload = await parseApiResponse(response, "No se pudo guardar el estado.");
  Object.assign(state, payload.state);
  refreshDerivedViews();
  await loadPreSessionDashboard();
}

async function saveStateSilently() {
  if (autoSavePromise) {
    return autoSavePromise;
  }

  autoSavePromise = saveState()
    .catch((error) => {
      throw error;
    })
    .finally(() => {
      autoSavePromise = null;
    });

  return autoSavePromise;
}

async function loadState() {
  const response = await fetch(apiUrl("/api/state"));
  if (response.status === 401) {
    showAuthOnly("Inicia sesion con Google para continuar.");
    return;
  }
  const payload = await parseApiResponse(response, "No se pudo cargar el estado.");
  Object.assign(state, payload.state);
  renderReferencePanels();
  renderPrerequisite(payload.prerequisite);
  renderAreas();
  renderOpenItems();
  renderSourceStatus();
  renderDailyReport(payload.prerequisite);
  await loadPreSessionDashboard();
}

async function loadEmployeeDirectory() {
  const response = await fetch(apiUrl("/api/employees"));
  if (response.status === 401) {
    return;
  }
  const payload = await parseApiResponse(response, "No se pudo cargar el directorio de empleados.");
  importedContacts = Array.isArray(payload.contacts) ? payload.contacts : [];
  renderContactSuggestions();
  renderEmployeesImportStatus(payload);
}

async function loadPreSessionDashboard() {
  const response = await fetch(apiUrl("/api/pre-session/dashboard"));
  if (response.status === 401) {
    return;
  }
  const payload = await parseApiResponse(response, "No se pudo cargar la vista de pre-sesion.");
  preSessionDashboard = payload;
  renderPreSessionDashboard();
}

function showAuthOnly(message = "") {
  authScreen.style.display = "grid";
  appShell.classList.add("app-hidden");
  authStatus.textContent = message;
}

function showApp(user) {
  authScreen.style.display = "none";
  appShell.classList.remove("app-hidden");
  currentUser.textContent = user ? `${user.name} · ${user.email}` : "";
  renderGoogleSheetsUserAuthStatus();
}

async function loadAuthConfig() {
  const response = await fetch(apiUrl("/api/auth/config"));
  authConfig = await response.json();

  if (!authConfig.enabled) {
    showApp(authConfig.user);
    return true;
  }

  if (authConfig.user) {
    showApp(authConfig.user);
    return true;
  }

  showAuthOnly(`Acceso limitado a @${authConfig.allowedDomain}`);

  if (!authConfig.googleClientId) {
    authStatus.textContent = "Falta configurar GOOGLE_CLIENT_ID en el servidor.";
    return false;
  }

  google.accounts.id.initialize({
    client_id: authConfig.googleClientId,
    callback: handleGoogleCredential,
    hosted_domain: authConfig.allowedDomain,
  });

  const container = document.getElementById("google-signin");
  container.innerHTML = "";
  google.accounts.id.renderButton(container, {
    theme: "outline",
    size: "large",
    text: "signin_with",
    shape: "pill",
  });

  return false;
}

async function loadGoogleSheetsUserAuthStatus() {
  const response = await fetch(apiUrl("/api/google-sheets-auth/status"));
  if (response.status === 401) {
    return;
  }
  const payload = await parseApiResponse(response, "No se pudo consultar el estado de Google Sheets OAuth.");
  authConfig = {
    ...(authConfig || {}),
    googleSheetsAuth: payload,
  };
  renderGoogleSheetsUserAuthStatus(payload);
}

async function handleGoogleCredential(response) {
  authStatus.textContent = "Validando acceso...";
  const authResponse = await fetch(apiUrl("/api/auth/google"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ credential: response.credential }),
  });

  const payload = await authResponse.json();
  if (!authResponse.ok) {
    showAuthOnly(payload.detail || payload.error || "No se pudo autenticar.");
    return;
  }

  showApp(payload.user);
  await loadState();
  await loadLiveSyncConfig();
}

function renderLiveSyncStatus(payload) {
  const config = payload?.config || {};
  const status = payload?.status || {};

  liveSyncEnabled.checked = Boolean(config.enabled);
  liveSyncSourceType.value = config.sourceType || "csv";
  liveSyncSourceUrl.value = config.sourceUrl || "";
  liveSyncIntervalMs.value = config.intervalMs || 300000;
  updateLiveSyncSourceUi();

  liveSyncStatus.textContent = [
    `Enabled: ${config.enabled ? "Yes" : "No"}`,
    `Source type: ${config.sourceType || "csv"}`,
    `Interval ms: ${config.intervalMs || 300000}`,
    `Last checked: ${status.lastCheckedAt || "Never"}`,
    `Last synced: ${status.lastSyncedAt || "Never"}`,
    `Last change detected: ${status.lastChangeDetectedAt || "Never"}`,
    `Last error: ${status.lastError || "None"}`,
  ].join("\n");
}

function updateLiveSyncSourceUi() {
  const usesGoogleSheets = liveSyncSourceType.value === "google-sheets";
  liveSyncSourceUrl.disabled = usesGoogleSheets;
  liveSyncSourceUrl.placeholder = usesGoogleSheets
    ? "No se requiere URL: usa Google Sheets API con service account"
    : "https://script.google.com/... o URL CSV publicada";
}

async function loadLiveSyncConfig() {
  const response = await fetch(apiUrl("/api/live-sync"));
  if (response.status === 401) {
    return;
  }
  const payload = await response.json();
  renderLiveSyncStatus(payload);
}

function showGoogleSheetsOAuthResultFromQuery() {
  const url = new URL(window.location.href);
  const status = url.searchParams.get("googleSheetsAuth");
  const message = url.searchParams.get("message");
  if (status && message) {
    window.alert(message);
  }
  if (status) {
    url.searchParams.delete("googleSheetsAuth");
    url.searchParams.delete("message");
    window.history.replaceState({}, document.title, url.toString());
  }
}

async function applyImportedPayload(response) {
  const payload = await parseApiResponse(response, "No se pudo sincronizar la informacion.");
  Object.assign(state, payload.state);
  renderReferencePanels();
  renderPrerequisite(payload.prerequisite);
  renderAreas();
  renderOpenItems();
  renderSourceStatus();
  updateNextOpenItemSuggestion(true);
  renderDailyReport(payload.prerequisite);
  await loadPreSessionDashboard();
}

openItemForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const formData = new FormData(event.currentTarget);
  const impactedAreaIds = Array.from(
    impactedAreasContainer.querySelectorAll("input:checked"),
    (checkbox) => checkbox.value,
  );

  const nextOpenItem = {
    id: String(formData.get("id")).trim(),
    title: String(formData.get("title")).trim(),
    description: String(formData.get("description")).trim(),
    sourceRef: String(formData.get("sourceRef")).trim(),
    ownerAreaId: String(formData.get("ownerAreaId")).trim(),
    ownerName: "",
    ownerEmail: "",
    ownerAreaHint: "",
    impactedAreaIds,
    isSubstantial: formData.get("isSubstantial") === "on",
    status: String(formData.get("status")).trim(),
    createdAt: new Date().toISOString(),
    dueDate: "",
    externalStatus: "",
    ccbScore: "",
    rawSheetRow: null,
    preSessionChecks: [],
    votes: [],
  };

  state.openItems.push(nextOpenItem);

  event.currentTarget.reset();
  updateNextOpenItemSuggestion(true);
  refreshDerivedViews();

  try {
    await saveState();
  } catch (error) {
    state.openItems = state.openItems.filter((item) => item !== nextOpenItem);
    refreshDerivedViews();
    window.alert(error.message || "No se pudo crear el open item en Google Sheets.");
  }
});

document.getElementById("save-state").addEventListener("click", async () => {
  try {
    await saveState();
  } catch (error) {
    window.alert(error.message || "No se pudo guardar el estado.");
  }
});

document.getElementById("update-owners-button").addEventListener("click", async () => {
  try {
    await saveState();
  } catch (error) {
    window.alert(error.message || "No se pudieron actualizar los owners.");
  }
});

document.getElementById("copy-report").addEventListener("click", async () => {
  await navigator.clipboard.writeText(dailyReport.dataset.copyText || dailyReport.textContent);
});

document.getElementById("export-state").addEventListener("click", () => {
  const blob = new Blob([JSON.stringify(state, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `ccb-state-${new Date().toISOString().slice(0, 10)}.json`;
  link.click();
  URL.revokeObjectURL(url);
});

document.getElementById("load-seed").addEventListener("click", async () => {
  try {
    const response = await fetch(apiUrl("/api/reset"), { method: "POST" });
    await applyImportedPayload(response);
  } catch (error) {
    window.alert(error.message || "No se pudo restaurar la demo.");
  }
});

document.getElementById("sync-google-sheet").addEventListener("click", async () => {
  try {
    sourceStatus.textContent = "Sincronizando con la fuente configurada...";
    const response = await fetch(apiUrl("/api/import/google-sheet-snapshot"), { method: "POST" });
    await applyImportedPayload(response);
  } catch (error) {
    sourceStatus.dataset.mode = "local";
    sourceStatus.innerHTML = `<strong>Error de sincronizacion:</strong> ${error.message || "No se pudo sincronizar con Google Sheets."}`;
    window.alert(error.message || "No se pudo sincronizar con Google Sheets.");
  }
});

document.getElementById("import-csv-button").addEventListener("click", async () => {
  const file = csvFileInput.files?.[0];
  if (!file) {
    window.alert("Selecciona primero el archivo CSV exportado desde Open Item List.");
    return;
  }

  const csvText = await file.text();
  const response = await fetch(apiUrl("/api/import/google-sheet-csv"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      csvText,
      spreadsheetTitle: file.name.replace(/\.csv$/i, ""),
    }),
  });

  if (!response.ok) {
    const errorPayload = await response.json();
    window.alert(errorPayload.detail || errorPayload.error || "No se pudo importar el CSV.");
    return;
  }

  await applyImportedPayload(response);
  csvFileInput.value = "";
});

document.getElementById("import-employees-csv-button").addEventListener("click", async () => {
  const file = employeesCsvFileInput.files?.[0];
  if (!file) {
    window.alert("Selecciona primero el CSV de empleados.");
    return;
  }

  employeesImportStatus.dataset.mode = "local";
  employeesImportStatus.innerHTML = "<strong>Importando:</strong> leyendo directorio de empleados...";

  try {
    const csvText = await file.text();
    const response = await fetch(apiUrl("/api/import/employees-csv"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        csvText,
        fileName: file.name,
      }),
    });
    const payload = await parseApiResponse(response, "No se pudo importar el CSV de empleados.");
    importedContacts = Array.isArray(payload.contacts) ? payload.contacts : [];
    renderContactSuggestions();
    renderEmployeesImportStatus(payload);
    renderAreas();
    employeesCsvFileInput.value = "";
  } catch (error) {
    employeesImportStatus.dataset.mode = "local";
    employeesImportStatus.innerHTML = `<strong>Error:</strong> ${error.message || "No se pudo importar el CSV."}`;
    window.alert(error.message || "No se pudo importar el CSV de empleados.");
  }
});

document.getElementById("clear-employees-button").addEventListener("click", async () => {
  try {
    const response = await fetch(apiUrl("/api/employees/clear"), {
      method: "POST",
    });
    const payload = await parseApiResponse(response, "No se pudo limpiar el directorio de empleados.");
    importedContacts = [];
    renderContactSuggestions();
    renderEmployeesImportStatus(payload);
    renderAreas();
    employeesCsvFileInput.value = "";
  } catch (error) {
    window.alert(error.message || "No se pudo limpiar el directorio de empleados.");
  }
});

document.getElementById("run-presession-job").addEventListener("click", async () => {
  try {
    preSessionJobStatus.textContent = "Ejecutando job de solicitudes de pre-sesion...";
    const response = await fetch(apiUrl("/api/pre-session/job"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sendEmails: true }),
    });
    const payload = await parseApiResponse(response, "No se pudo ejecutar el job de pre-sesion.");
    Object.assign(state, payload.state);
    preSessionDashboard = payload.dashboard;
    renderPreSessionDashboard();
    preSessionJobStatus.textContent = payload.notifications.length
      ? payload.notifications.map((entry) => {
          const modeLabel = entry.sent ? "enviado" : entry.deliveryMode === "preview" ? "preview" : "pendiente";
          const targetSuffix = entry.deliveryTarget && entry.deliveryTarget !== entry.ownerEmail
            ? ` | destino prueba: ${entry.deliveryTarget}`
            : "";
          const suffix = entry.error ? ` | error: ${entry.error}` : "";
          return `- ${entry.ownerEmail}: ${entry.pendingCount} item(s) | ${modeLabel}${targetSuffix}${suffix}`;
        }).join("\n")
      : "No habia owners con solicitudes pendientes.";
  } catch (error) {
    preSessionJobStatus.textContent = `Error: ${error.message || "No se pudo ejecutar el job."}`;
    window.alert(error.message || "No se pudo ejecutar el job de pre-sesion.");
  }
});

document.getElementById("save-live-sync").addEventListener("click", async () => {
  try {
    const response = await fetch(apiUrl("/api/live-sync"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        enabled: liveSyncEnabled.checked,
        sourceType: liveSyncSourceType.value,
        sourceUrl: liveSyncSourceUrl.value.trim(),
        intervalMs: Number(liveSyncIntervalMs.value || 300000),
      }),
    });
    const payload = await parseApiResponse(response, "No se pudo guardar la configuracion de Live Sync.");
    renderLiveSyncStatus(payload);
  } catch (error) {
    window.alert(error.message || "No se pudo guardar la configuracion de Live Sync.");
  }
});

liveSyncSourceType.addEventListener("change", updateLiveSyncSourceUi);
tabButtons.forEach((button) => {
  button.addEventListener("click", () => activateTab(button.dataset.tab));
});

document.getElementById("run-live-sync").addEventListener("click", async () => {
  try {
    const response = await fetch(apiUrl("/api/live-sync/run"), { method: "POST" });
    if (response.status === 401) {
      showAuthOnly("Tu sesion expiro. Inicia sesion de nuevo.");
      return;
    }
    const payload = await parseApiResponse(response, "No se pudo ejecutar la sincronizacion.");
    renderLiveSyncStatus(payload);
    Object.assign(state, payload.state);
    renderReferencePanels();
    renderPrerequisite(payload.prerequisite);
    renderAreas();
    renderOpenItems();
    renderSourceStatus();
    renderDailyReport(payload.prerequisite);
    await loadPreSessionDashboard();
  } catch (error) {
    window.alert(error.message || "No se pudo ejecutar la sincronizacion.");
  }
});

document.getElementById("connect-google-sheets-user").addEventListener("click", () => {
  window.location.href = apiUrl("/api/google-sheets-auth/start");
});

document.getElementById("disconnect-google-sheets-user").addEventListener("click", async () => {
  try {
    const response = await fetch(apiUrl("/api/google-sheets-auth/disconnect"), { method: "POST" });
    const payload = await parseApiResponse(response, "No se pudo desconectar Google Sheets OAuth.");
    authConfig = {
      ...(authConfig || {}),
      googleSheetsAuth: payload.googleSheetsAuth,
    };
    renderGoogleSheetsUserAuthStatus(payload.googleSheetsAuth);
  } catch (error) {
    window.alert(error.message || "No se pudo desconectar Google Sheets OAuth.");
  }
});

document.getElementById("logout-button").addEventListener("click", async () => {
  await fetch(apiUrl("/api/auth/logout"), { method: "POST" });
  showAuthOnly(`Acceso limitado a @${authConfig?.allowedDomain || "conceivable.life"}`);
  currentUser.textContent = "";
});

(async () => {
  try {
    applyDeepLinkFromUrl();
    showGoogleSheetsOAuthResultFromQuery();
    const hasSession = await loadAuthConfig();
    renderReferencePanels();
    activateTab(deepLinkState.tab || "summary");
    if (hasSession) {
      await loadEmployeeDirectory();
      await loadGoogleSheetsUserAuthStatus();
      await loadState();
      await loadLiveSyncConfig();
    }
  } catch (error) {
    sourceStatus.dataset.mode = "local";
    sourceStatus.innerHTML = `<strong>Error inicial:</strong> ${error.message || "No se pudo cargar la app."}`;
  }
})();

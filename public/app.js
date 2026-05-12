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
const openItemsToolbarFilters = document.getElementById("open-items-toolbar-filters");
const newOpenItemDrawer = document.getElementById("new-open-item-drawer");
const openNewItemDrawerButton = document.getElementById("open-new-item-drawer");
const openItemDescriptionField = document.getElementById("open-item-description-field");
const toggleOpenItemDescriptionButton = document.getElementById("toggle-open-item-description");
const appToast = document.getElementById("app-toast");
const dailyReport = document.getElementById("daily-report");
const summaryKpiGrid = document.getElementById("summary-kpi-grid");
const summaryMoreMetrics = document.getElementById("summary-more-metrics");
const summaryRiskMatrices = document.getElementById("summary-risk-matrices");
const summaryRiskDetail = document.getElementById("summary-risk-detail");
const summaryCriticalItems = document.getElementById("summary-critical-items");
const summaryCriticalActions = document.getElementById("summary-critical-actions");
const summaryTrends = document.getElementById("summary-trends");
const sourceStatusPill = document.getElementById("source-status-pill");
const governanceSettingsDrawer = document.getElementById("governance-settings-drawer");
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
const headerOpenItemSearch = document.getElementById("header-open-item-search");
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
let selectedOpenItemsImpactTab = "impacting";
let selectedOpenItemsStatusTab = "NEW";
let selectedOpenItemId = "";
let openItemsSearchQuery = "";
let openItemsSearchDebounceId = null;
let openItemsSearchPanelOpen = false;
let highlightedOpenItemId = "";
let selectedSummaryKpiFilter = "all";
let selectedSummaryRiskCell = "";
const openItemDetailTabById = new Map();
const evaluationCriterionTabById = new Map();
const openItemInlineFieldState = new Set();
const openItemEditModeById = new Map();
const preSessionExpandedItems = new Set();
const preSessionCommentExpandedItems = new Set();
let summaryMoreMetricsExpanded = false;
let summaryCriticalExpanded = false;
let summaryTrendsExpanded = false;
let summarySourceExpanded = false;
let openItemDrawerDescriptionExpanded = false;
const expandedEvaluationPanels = new Set();
const evaluationUiState = new Map();
const evaluationSaveStatusById = new Map();
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
  if (!String(query || "").trim()) {
    dropdown.hidden = true;
    dropdown.innerHTML = "";
    return;
  }
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
      warnings.push(`Please select a score for ${criterion.name}.`);
    }
    if (!String(entry.rationale || "").trim()) {
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
  const currentUserEntries = getEvaluationForUser(openItem.id, getCurrentEvaluator().email);
  const currentUserScore = calculateWeightedScore(currentUserEntries);
  const averageScore = openItem.ccbDecisionAverageScore == null ? null : Number(openItem.ccbDecisionAverageScore);
  const recommendation = openItem.ccbDecisionRecommendation || getDecisionRecommendation(averageScore);
  const totalEvaluators = Number(openItem.ccbDecisionEvaluatorCount || 0);
  const fastTrack = (openItem.ccbDecisionEvaluators || []).length
    ? (state.ccbEvaluations || [])
        .filter((entry) => entry.openItemId === openItem.id)
        .reduce((groups, entry) => {
          const key = String(entry.evaluatorEmail || "").trim().toLowerCase();
          groups.set(key, [...(groups.get(key) || []), entry]);
          return groups;
        }, new Map())
        .values()
    : [];
  const hasFastTrack = Array.from(fastTrack).some((entries) => getEvaluationWarnings(openItem, entries).includes("Fast-track candidate"));

  return {
    currentUserScore,
    averageScore,
    totalEvaluators,
    recommendation: totalEvaluators ? recommendation : "INCOMPLETE",
    fastTrack: hasFastTrack,
    criteriaCount: criteria.length,
    criterionAverages: openItem.ccbDecisionCriterionAverages || {},
    lastUpdated: openItem.ccbDecisionLastUpdated || "",
    evaluators: Array.isArray(openItem.ccbDecisionEvaluators) ? openItem.ccbDecisionEvaluators : [],
  };
}

function isPrivilegedOpenItemsViewer() {
  const evaluator = getCurrentEvaluator();
  const email = evaluator.email;
  return evaluator.area === "CM" || /admin|coordinator/i.test(email);
}

function getOpenItemImpactContext(openItem) {
  const evaluator = getCurrentEvaluator();
  const email = evaluator.email;
  const areaId = evaluator.area;
  const userEvaluation = getEvaluationForUser(openItem.id, email);
  const userVote = (openItem.votes || []).some((vote) => (
    (vote.voterEmail || "").trim().toLowerCase() === email || vote.areaId === areaId
  ));
  const impactingMe = Boolean(
    (areaId && (openItem.impactedAreaIds || []).includes(areaId)) ||
    (email && (openItem.impactedUsers || []).includes(email)) ||
    userVote ||
    userEvaluation.length
  );
  const evaluationComplete = Number.isFinite(calculateWeightedScore(userEvaluation));
  const voteSubmitted = userVote;
  const pendingEvaluation = impactingMe && !evaluationComplete;
  const awaitingResponse = impactingMe && !evaluationComplete && !voteSubmitted;

  return {
    impactingMe,
    evaluationComplete,
    voteSubmitted,
    pendingEvaluation,
    awaitingResponse,
    autoAssigned: email && (openItem.impactedUsers || []).includes(email),
  };
}

function getOpenItemPriority(item) {
  const context = getOpenItemImpactContext(item);
  if (context.impactingMe && context.pendingEvaluation) {
    return 3;
  }
  if (context.impactingMe && context.evaluationComplete) {
    return 2;
  }
  return 1;
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
  const adminMenu = document.getElementById("open-items-admin-menu");
  if (adminMenu) {
    adminMenu.hidden = !isAdminOverrideUser();
  }
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
        <article class="presession-queue-row${item.openItemId === targetOpenItemId && (!targetAreaId || item.areaId === targetAreaId) ? " is-targeted" : ""}">
          <div class="presession-queue-main">
            <div>
              <h3>${escapeHtml(item.openItemId)} · ${escapeHtml(item.title)}</h3>
              <p class="meta-line">${escapeHtml(item.areaName)} · ${item.requestedAt ? `Requested ${escapeHtml(formatDateTime(item.requestedAt))}` : "Pending review"}</p>
            </div>
            <div class="summary-badge-row--compact">
              <span class="chip">${escapeHtml(item.areaName)}</span>
              <span class="badge">${escapeHtml(item.externalStatus || item.itemStatus || "pending")}</span>
            </div>
          </div>
          <div class="presession-queue-actions">
            <button type="button" data-presession-respond data-decision="impact" data-open-item-id="${escapeHtml(item.openItemId)}" data-area-id="${escapeHtml(item.areaId)}">Impacts me</button>
            <button type="button" class="secondary" data-presession-respond data-decision="no-impact" data-open-item-id="${escapeHtml(item.openItemId)}" data-area-id="${escapeHtml(item.areaId)}">No impact</button>
            <button type="button" class="secondary" data-presession-expand="${escapeHtml(item.openItemId)}:${escapeHtml(item.areaId)}">Expand</button>
          </div>
          ${preSessionExpandedItems.has(`${item.openItemId}:${item.areaId}`) ? `
            <div class="presession-queue-expanded">
              <p class="meta-line">Open item nuevo o pendiente para tu area. ${item.requestedAt ? `Solicitado: ${escapeHtml(formatDateTime(item.requestedAt))}.` : "Aun no se ha enviado solicitud."}</p>
              ${preSessionCommentExpandedItems.has(`${item.openItemId}:${item.areaId}`) ? `
                <textarea data-presession-comment data-open-item-id="${escapeHtml(item.openItemId)}" data-area-id="${escapeHtml(item.areaId)}" rows="3" placeholder="Comentario opcional para tu revision"></textarea>
              ` : `<button type="button" class="secondary" data-presession-comment-toggle="${escapeHtml(item.openItemId)}:${escapeHtml(item.areaId)}">Add comment</button>`}
            </div>
          ` : ""}
        </article>
      `).join("")
    : "<p class=\"meta-line\">No tienes open items pendientes por responder.</p>";

  preSessionAnsweredList.innerHTML = answeredItems.length
    ? answeredItems.map((item) => `
        <article class="presession-queue-row presession-queue-row--answered${item.openItemId === targetOpenItemId && (!targetAreaId || item.areaId === targetAreaId) ? " is-targeted" : ""}">
          <div class="presession-queue-main">
            <div>
              <h3>${escapeHtml(item.openItemId)} · ${escapeHtml(item.title)}</h3>
              <p class="meta-line">${escapeHtml(item.areaName)} · Respondido ${escapeHtml(formatDateTime(item.respondedAt))}</p>
            </div>
            <span class="decision-pill" data-decision="${escapeHtml(item.decision)}">${item.decision === "impact" ? "Impacts me" : "No impact"}</span>
          </div>
          ${item.comment ? `<p class="meta-line">${escapeHtml(item.comment)}</p>` : ""}
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
          <div class="presession-owner-pill-list">
            ${selectedJobItem.pendingOwners.map((owner) => `
              <div class="presession-owner-pill">
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

  preSessionPendingList.querySelectorAll("[data-presession-expand]").forEach((button) => {
    button.addEventListener("click", () => {
      const key = button.dataset.presessionExpand || "";
      if (preSessionExpandedItems.has(key)) {
        preSessionExpandedItems.delete(key);
      } else {
        preSessionExpandedItems.add(key);
      }
      renderPreSessionDashboard();
    });
  });

  preSessionPendingList.querySelectorAll("[data-presession-comment-toggle]").forEach((button) => {
    button.addEventListener("click", () => {
      const key = button.dataset.presessionCommentToggle || "";
      preSessionExpandedItems.add(key);
      preSessionCommentExpandedItems.add(key);
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
  closeOpenItemSearchPanel();
  renderHeaderOpenItemSearch();

  if (tabId === "items") {
    updateNextOpenItemSuggestion(true);
  }
}

function getGlobalOpenItemSearchResults() {
  const normalizedQuery = openItemsSearchQuery.trim().toLowerCase();
  if (normalizedQuery.length < 2) {
    return [];
  }
  return state.openItems
    .filter((item) => item.status !== "closed")
    .filter((item) => {
      const recommendation = getEvaluationSummary(item).recommendation || "";
      const searchableAreas = impactedAreaNames(item);
      const searchableText = [
        item.id,
        item.title,
        item.ownerName,
        searchableAreas,
        recommendation,
        item.externalStatus,
      ].join(" ").toLowerCase();
      return searchableText.includes(normalizedQuery);
    })
    .slice(0, 8);
}

function closeOpenItemSearchPanel() {
  openItemsSearchPanelOpen = false;
}

function renderHeaderOpenItemSearch() {
  if (!headerOpenItemSearch) {
    return;
  }
  const searchResults = getGlobalOpenItemSearchResults();
  const showSuggestionPanel = openItemsSearchPanelOpen && searchResults.length > 0;
  headerOpenItemSearch.hidden = false;
  headerOpenItemSearch.innerHTML = `
    <span class="open-items-nav-label">Find ticket</span>
    <div class="open-items-search-shell" data-open-item-search-shell>
      <input
        type="search"
        id="open-items-ticket-search"
        class="open-items-search-input"
        placeholder="Search ticket, owner, area or status"
        autocomplete="off"
        value="${escapeHtml(openItemsSearchQuery)}"
      />
      ${openItemsSearchQuery.trim() ? `
        <button type="button" class="open-items-search-clear" data-open-item-search-clear aria-label="Clear search">✕</button>
      ` : ""}
      ${showSuggestionPanel ? `
        <div class="open-items-search-suggestions">
          ${searchResults.map((item) => `
            <button
              type="button"
              class="open-items-search-suggestion"
              data-open-item-search-select="${escapeHtml(item.id)}"
            >
              <div class="open-items-search-suggestion-top">
                <strong>${escapeHtml(item.id)}</strong>
                <span class="decision-pill" data-decision="${escapeHtml(String((getEvaluationSummary(item).recommendation || item.externalStatus || "incomplete")).toLowerCase())}">${escapeHtml(getEvaluationSummary(item).recommendation || item.externalStatus || "INCOMPLETE")}</span>
              </div>
              <span>${escapeHtml(item.title)}</span>
              <small>${escapeHtml(impactedAreaNames(item) || "No impacted areas")}</small>
            </button>
          `).join("")}
        </div>
      ` : ""}
    </div>
  `;

  const searchField = headerOpenItemSearch.querySelector("#open-items-ticket-search");
  const clearButton = headerOpenItemSearch.querySelector("[data-open-item-search-clear]");
  const resultButtons = headerOpenItemSearch.querySelectorAll("[data-open-item-search-select]");

  searchField?.addEventListener("input", (event) => {
    const nextQuery = event.target.value || "";
    openItemsSearchQuery = nextQuery;
    if (nextQuery.trim().length < 2) {
      closeOpenItemSearchPanel();
      window.clearTimeout(openItemsSearchDebounceId);
      const suggestions = headerOpenItemSearch.querySelector(".open-items-search-suggestions");
      if (suggestions) {
        suggestions.remove();
      }
      return;
    }
    window.clearTimeout(openItemsSearchDebounceId);
    openItemsSearchDebounceId = window.setTimeout(() => {
      openItemsSearchPanelOpen = getGlobalOpenItemSearchResults().length > 0;
      renderHeaderOpenItemSearch();
      const field = headerOpenItemSearch.querySelector("#open-items-ticket-search");
      if (field) {
        field.focus();
        field.setSelectionRange(nextQuery.length, nextQuery.length);
      }
    }, 260);
  });

  searchField?.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeOpenItemSearchPanel();
      renderHeaderOpenItemSearch();
      searchField.focus();
    }
  });

  clearButton?.addEventListener("click", () => {
    openItemsSearchQuery = "";
    closeOpenItemSearchPanel();
    renderHeaderOpenItemSearch();
    headerOpenItemSearch.querySelector("#open-items-ticket-search")?.focus();
  });

  resultButtons.forEach((button) => {
    button.addEventListener("click", () => {
      handleFindTicketSelect(button.dataset.openItemSearchSelect || "");
    });
  });
}

function isAdminOverrideUser() {
  return String(getCurrentEvaluator().email || "").toLowerCase() === "rodrigo@conceivable.life";
}

function getVotingPermission(item) {
  const evaluator = getCurrentEvaluator();
  const isAdmin = isAdminOverrideUser();
  if (isAdmin) {
    return {
      canVote: true,
      isAdmin: true,
      areaId: "",
      message: "",
    };
  }
  if (!evaluator.area) {
    return {
      canVote: false,
      isAdmin: false,
      areaId: "",
      message: "You are not assigned as an owner for a CCB area.",
    };
  }
  if (!(item.impactedAreaIds || []).includes(evaluator.area)) {
    return {
      canVote: false,
      isAdmin: false,
      areaId: evaluator.area,
      message: "Your area is not impacted by this Open Item.",
    };
  }
  return {
    canVote: true,
    isAdmin: false,
    areaId: evaluator.area,
    message: "",
  };
}

async function saveVote(openItemId, voteInput) {
  const response = await fetch(apiUrl("/api/votes/save"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      openItemId,
      ...voteInput,
    }),
  });
  const payload = await parseApiResponse(response, "No se pudo guardar el voto.");
  Object.assign(state, payload.state);
  return payload;
}

function setOpenItemDrawerVisibility(isOpen) {
  newOpenItemDrawer.hidden = !isOpen;
  if (isOpen) {
    updateNextOpenItemSuggestion(true);
    openItemIdInput.focus();
  }
}

function syncOpenItemDrawerDescription() {
  const shouldShow = openItemDrawerDescriptionExpanded || Boolean(openItemDescriptionField.value.trim());
  openItemDescriptionField.hidden = !shouldShow;
  toggleOpenItemDescriptionButton.hidden = shouldShow;
}

function showAppToast(message) {
  appToast.textContent = message;
  appToast.hidden = false;
  window.clearTimeout(showAppToast.timeoutId);
  showAppToast.timeoutId = window.setTimeout(() => {
    appToast.hidden = true;
  }, 2600);
}

function notifyNotAuthorized() {
  showAppToast("Not authorized");
}

function setGlobalBusy(isBusy) {
  document.body.classList.toggle("app-busy", isBusy);
  document.querySelectorAll("button").forEach((button) => {
    if (isBusy) {
      button.dataset.wasDisabled = button.disabled ? "true" : "false";
      button.disabled = true;
    } else {
      if (button.dataset.wasDisabled !== "true") {
        button.disabled = false;
      }
      delete button.dataset.wasDisabled;
    }
  });
}

async function withGlobalBusy(task) {
  setGlobalBusy(true);
  try {
    return await task();
  } finally {
    await new Promise((resolve) => window.setTimeout(resolve, 900));
    setGlobalBusy(false);
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
    <strong>${prerequisite.status === "blocked" ? "BLOCKED" : prerequisite.status === "review" ? "REVIEW" : "CLEAR"}</strong>
    <span>${escapeHtml(
      prerequisite.status === "blocked"
        ? `${prerequisite.blockedItems.length} substantial items pending impacted approvals`
        : prerequisite.detail,
    )}</span>
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

const RISK_IMPACT_LABELS = [
  "Very low impact",
  "Low impact",
  "Medium impact",
  "High impact",
  "Very high impact",
];

const RISK_LIKELIHOOD_LABELS = [
  "Very Low",
  "Low",
  "Medium",
  "High",
  "Very High",
];

function getOpenItemPendingAreas(openItem) {
  const impactedAreaIds = Array.isArray(openItem.impactedAreaIds) ? openItem.impactedAreaIds : [];
  const approvedAreaIds = new Set((openItem.votes || []).filter((vote) => vote.decision === "approve").map((vote) => vote.areaId));
  return impactedAreaIds
    .filter((areaId) => !approvedAreaIds.has(areaId))
    .map((areaId) => state.areas.find((area) => area.id === areaId)?.name || areaId);
}

function getOpenItemEvaluationCompletion(openItem) {
  const impactedCount = Math.max(1, (openItem.impactedAreaIds || []).length);
  const completed = Number(openItem.ccbDecisionEvaluatorCount || 0);
  return Math.min(100, Math.round((completed / impactedCount) * 100));
}

function hasComplianceSignal(openItem) {
  const text = `${openItem.title || ""} ${openItem.description || ""} ${openItem.sourceRef || ""}`;
  return /(compliance|regulatory|legal|gdpr|privacy|security|audit|safety|iso|soc2)/i.test(text);
}

function calculateCurrentRisk(openItem) {
  const impactedCount = (openItem.impactedAreaIds || []).length;
  const pendingAreas = getOpenItemPendingAreas(openItem).length;
  const pendingEvaluations = (openItem.pendingEvaluators || []).length;
  const averageScore = Number(openItem.ccbDecisionAverageScore || 0);
  const recommendation = String(openItem.ccbDecisionRecommendation || "").toUpperCase();
  const ccbScore = Number.parseFloat(String(openItem.ccbScore || "").replace(/[^\d.-]/g, "")) || 0;
  const compliance = hasComplianceSignal(openItem);

  let impact = 1;
  impact += openItem.isSubstantial ? 2 : 0;
  impact += impactedCount >= 4 ? 1 : 0;
  impact += compliance ? 1 : 0;
  impact += recommendation === "REJECT" ? 1 : 0;

  let likelihood = 1;
  likelihood += pendingAreas >= 2 ? 1 : 0;
  likelihood += pendingEvaluations >= 2 ? 1 : 0;
  likelihood += recommendation === "DEFER" || recommendation === "REJECT" ? 1 : 0;
  likelihood += (averageScore > 0 && averageScore < 3) || (ccbScore > 0 && ccbScore < 3) ? 1 : 0;
  likelihood += openItem.status === "deferred" ? 1 : 0;

  impact = Math.min(5, impact);
  likelihood = Math.min(5, likelihood);
  const score = impact * likelihood;
  return {
    impact,
    likelihood,
    score,
    level: score >= 16 ? "high" : score >= 9 ? "medium" : "low",
  };
}

function calculateResidualRisk(openItem) {
  const current = calculateCurrentRisk(openItem);
  const averageScore = Number(openItem.ccbDecisionAverageScore || 0);
  const approvedVotes = (openItem.votes || []).filter((vote) => vote.decision === "approve").length;
  const impactedCount = Math.max(1, (openItem.impactedAreaIds || []).length);
  const evaluationComplete = Number(openItem.ccbDecisionEvaluatorCount || 0) > 0;
  const allAreasApproved = approvedVotes >= impactedCount;
  const recommendation = String(openItem.ccbDecisionRecommendation || "").toUpperCase();

  let impact = current.impact;
  let likelihood = current.likelihood;

  if (evaluationComplete) {
    likelihood -= 1;
  }
  if (averageScore >= 4) {
    impact -= 1;
    likelihood -= 1;
  }
  if (recommendation === "APPROVE") {
    likelihood -= 1;
  }
  if (allAreasApproved) {
    impact -= 1;
  }

  impact = Math.max(1, Math.min(5, impact));
  likelihood = Math.max(1, Math.min(5, likelihood));
  const score = impact * likelihood;
  return {
    impact,
    likelihood,
    score,
    level: score >= 16 ? "high" : score >= 9 ? "medium" : "low",
  };
}

function mapRiskToMatrix(risk) {
  return {
    x: Math.max(0, Math.min(4, Number(risk?.likelihood || 1) - 1)),
    y: Math.max(0, Math.min(4, Number(risk?.impact || 1) - 1)),
  };
}

function getRiskColor(riskLevel) {
  if (riskLevel === "high") {
    return "high";
  }
  if (riskLevel === "medium") {
    return "medium";
  }
  return "low";
}

function getGovernanceDashboardModel(prerequisite) {
  const activeItems = state.openItems.filter((item) => item.status !== "closed");
  const today = new Date().toISOString().slice(0, 10);
  const approvedToday = activeItems.filter((item) => String(item.externalStatus || "").toUpperCase() === "APPROVED" && String(item.ccbDecisionLastUpdated || item.lastEvaluationDate || "").startsWith(today)).length;
  const deferredItems = activeItems.filter((item) => {
    const recommendation = String(item.ccbDecisionRecommendation || "").toUpperCase();
    return recommendation.includes("DEFER") || item.status === "deferred";
  });
  const blockedItems = activeItems.filter((item) => item.isSubstantial && getOpenItemPendingAreas(item).length > 0);
  const highRiskItems = activeItems.filter((item) => calculateCurrentRisk(item).level === "high");
  const residualScores = activeItems
    .map((item) => calculateResidualRisk(item).score / 5)
    .filter((value) => Number.isFinite(value));
  const residualRiskAverage = residualScores.length
    ? residualScores.reduce((sum, value) => sum + value, 0) / residualScores.length
    : 0;

  const kpis = [
    { id: "active", label: "Active Open Items", value: activeItems.length, tone: "navy" },
    { id: "substantial", label: "Substantial Changes", value: activeItems.filter((item) => item.isSubstantial).length, tone: "ice" },
    { id: "high-risk", label: "High Risk Items", value: highRiskItems.length, tone: "danger" },
    { id: "pending-evaluations", label: "Pending Evaluations", value: activeItems.reduce((sum, item) => sum + (item.pendingEvaluators || []).length, 0), tone: "amber" },
    { id: "approved-today", label: "Approved Today", value: approvedToday, tone: "success" },
    { id: "deferred", label: "Deferred Items", value: deferredItems.length, tone: "amber" },
    { id: "residual-risk", label: "Residual Risk Average", value: residualRiskAverage.toFixed(2), tone: "ice" },
    { id: "blocked", label: "Blocked Items", value: blockedItems.length, tone: "danger" },
  ];

  const createMatrix = (type) => {
    const cells = Array.from({ length: 5 }, (_, y) => Array.from({ length: 5 }, (_, x) => ({
      x,
      y,
      count: 0,
      items: [],
      level: "low",
    })));
    activeItems.forEach((item) => {
      const risk = type === "current" ? calculateCurrentRisk(item) : calculateResidualRisk(item);
      const point = mapRiskToMatrix(risk);
      const cell = cells[point.y][point.x];
      cell.count += 1;
      cell.items.push({
        item,
        risk,
        pendingAreas: getOpenItemPendingAreas(item),
        completion: getOpenItemEvaluationCompletion(item),
      });
      cell.level = getRiskColor(risk.level);
    });
    return cells;
  };

  const criticalItems = activeItems
    .map((item) => {
      const currentRisk = calculateCurrentRisk(item);
      const residualRisk = calculateResidualRisk(item);
      const pendingAreas = getOpenItemPendingAreas(item);
      const badges = [];
      if (currentRisk.level === "high") badges.push("HIGH RISK");
      if (item.isSubstantial) badges.push("SUBSTANTIAL");
      if (pendingAreas.length) badges.push("PENDING REVIEW");
      if (item.isSubstantial && pendingAreas.length) badges.push("BLOCKED");
      if (hasComplianceSignal(item)) badges.push("COMPLIANCE");
      if (String(item.ccbDecisionRecommendation || "").toUpperCase().includes("DEFER")) badges.push("DEFER");
      return {
        item,
        currentRisk,
        residualRisk,
        pendingAreas,
        completion: getOpenItemEvaluationCompletion(item),
        badges,
      };
    })
    .filter((entry) => entry.badges.length > 0)
    .sort((left, right) => right.currentRisk.score - left.currentRisk.score || right.pendingAreas.length - left.pendingAreas.length || left.item.id.localeCompare(right.item.id));

  const filteredCriticalItems = criticalItems.filter((entry) => {
    switch (selectedSummaryKpiFilter) {
      case "substantial":
        return entry.item.isSubstantial;
      case "high-risk":
        return entry.currentRisk.level === "high";
      case "pending-evaluations":
        return (entry.item.pendingEvaluators || []).length > 0;
      case "approved-today":
        return String(entry.item.externalStatus || "").toUpperCase() === "APPROVED";
      case "deferred":
        return String(entry.item.ccbDecisionRecommendation || "").toUpperCase().includes("DEFER") || entry.item.status === "deferred";
      case "blocked":
        return entry.item.isSubstantial && entry.pendingAreas.length > 0;
      default:
        return true;
    }
  });

  const statusCounts = {
    Open: activeItems.filter((item) => item.status === "open").length,
    Approved: activeItems.filter((item) => String(item.externalStatus || "").toUpperCase() === "APPROVED").length,
    Deferred: activeItems.filter((item) => item.status === "deferred" || String(item.ccbDecisionRecommendation || "").toUpperCase().includes("DEFER")).length,
    Rejected: activeItems.filter((item) => String(item.externalStatus || "").toUpperCase() === "REJECTED" || String(item.ccbDecisionRecommendation || "").toUpperCase() === "REJECT").length,
    Closed: state.openItems.filter((item) => item.status === "closed").length,
  };

  const evaluationsTimelineMap = new Map();
  (state.ccbEvaluations || []).forEach((evaluation) => {
    const stamp = String(evaluation.updatedAt || evaluation.createdAt || "").slice(0, 10);
    if (!stamp) return;
    evaluationsTimelineMap.set(stamp, (evaluationsTimelineMap.get(stamp) || 0) + 1);
  });
  const evaluationsTimeline = Array.from(evaluationsTimelineMap.entries())
    .sort((left, right) => left[0].localeCompare(right[0]))
    .slice(-7)
    .map(([label, value]) => ({ label, value }));

  const riskByArea = state.areas
    .map((area) => ({
      label: area.name,
      value: activeItems.filter((item) => (item.impactedAreaIds || []).includes(area.id)).reduce((sum, item) => sum + calculateCurrentRisk(item).score, 0),
    }))
    .filter((entry) => entry.value > 0)
    .sort((left, right) => right.value - left.value)
    .slice(0, 6);

  const pendingByArea = state.areas
    .map((area) => ({
      label: area.name,
      value: activeItems.filter((item) => getOpenItemPendingAreas(item).includes(area.name)).length,
    }))
    .filter((entry) => entry.value > 0)
    .sort((left, right) => right.value - left.value)
    .slice(0, 6);

  const averageScoreTrend = activeItems
    .map((item) => ({
      label: item.id,
      value: Number.isFinite(Number(item.ccbDecisionAverageScore)) ? Number(item.ccbDecisionAverageScore) : 0,
    }))
    .filter((entry) => entry.value > 0)
    .sort((left, right) => left.label.localeCompare(right.label))
    .slice(-8);

  return {
    prerequisite,
    kpis,
    activeItems,
    currentMatrix: createMatrix("current"),
    residualMatrix: createMatrix("residual"),
    criticalItems: filteredCriticalItems,
    statusCounts,
    evaluationsTimeline,
    riskByArea,
    pendingByArea,
    averageScoreTrend,
  };
}

function renderSummaryBars(title, items, formatValue = (value) => String(value)) {
  const maxValue = Math.max(1, ...items.map((item) => Number(item.value || 0)));
  return `
    <article class="summary-chart-card">
      <div class="section-heading">
        <h3>${escapeHtml(title)}</h3>
      </div>
      <div class="summary-bar-list">
        ${items.length ? items.map((item) => `
          <div class="summary-bar-row">
            <span>${escapeHtml(item.label)}</span>
            <div class="summary-bar-track">
              <div class="summary-bar-fill" style="width:${Math.max(8, (Number(item.value || 0) / maxValue) * 100)}%"></div>
            </div>
            <strong>${escapeHtml(formatValue(item.value))}</strong>
          </div>
        `).join("") : '<p class="meta-line">Sin datos suficientes.</p>'}
      </div>
    </article>
  `;
}

function buildExecutiveSummaryText(model) {
  const topCritical = model.criticalItems.slice(0, 5).map((entry) => `- ${entry.item.id}: ${entry.item.title} [${entry.badges.join(", ")}]`).join("\n");
  return [
    `Date: ${new Date().toISOString().slice(0, 10)}`,
    `Governance posture: ${model.prerequisite.label}`,
    `Active Open Items: ${model.activeItems.length}`,
    `High Risk Items: ${model.kpis.find((kpi) => kpi.id === "high-risk")?.value || 0}`,
    `Pending Evaluations: ${model.kpis.find((kpi) => kpi.id === "pending-evaluations")?.value || 0}`,
    `Blocked Items: ${model.kpis.find((kpi) => kpi.id === "blocked")?.value || 0}`,
    `Residual Risk Average: ${model.kpis.find((kpi) => kpi.id === "residual-risk")?.value || "0.00"}`,
    "",
    "Critical Open Items:",
    topCritical || "- No critical items",
  ].join("\n");
}

function renderRiskMatrix(title, matrix, type) {
  const rows = [...matrix].reverse();
  return `
    <article class="risk-matrix-card">
      <div class="section-heading">
        <h3>${escapeHtml(title)}</h3>
      </div>
      <div class="risk-matrix-grid-shell">
        <div class="risk-matrix-y-axis">
          ${[...RISK_IMPACT_LABELS].reverse().map((label) => `<span>${escapeHtml(label)}</span>`).join("")}
        </div>
        <div class="risk-matrix-grid">
          ${rows.map((row) => row.map((cell) => `
            <button
              type="button"
              class="risk-cell risk-cell--${cell.level}${selectedSummaryRiskCell === `${type}:${cell.x}:${cell.y}` ? " is-active" : ""}"
              data-risk-cell="${type}:${cell.x}:${cell.y}"
              title="${cell.count} item(s) · ${RISK_LIKELIHOOD_LABELS[cell.x]} / ${RISK_IMPACT_LABELS[cell.y]}"
            >
              <strong>${cell.count}</strong>
            </button>
          `).join("")).join("")}
        </div>
      </div>
      <div class="risk-matrix-x-axis">
        ${RISK_LIKELIHOOD_LABELS.map((label) => `<span>${escapeHtml(label)}</span>`).join("")}
      </div>
    </article>
  `;
}

function renderSummaryRiskDetail(model) {
  if (!selectedSummaryRiskCell) {
    summaryRiskDetail.innerHTML = "";
    summaryRiskDetail.hidden = true;
    return;
  }

  const [type = "current", x = "0", y = "0"] = String(selectedSummaryRiskCell || "").split(":");
  const matrix = type === "residual" ? model.residualMatrix : model.currentMatrix;
  const cell = matrix?.[Number(y)]?.[Number(x)] || null;
  if (!cell || !cell.items.length) {
    summaryRiskDetail.innerHTML = "";
    summaryRiskDetail.hidden = true;
    return;
  }

  summaryRiskDetail.hidden = false;
  summaryRiskDetail.innerHTML = `
    <div class="summary-detail-card">
      <div class="section-heading">
        <div>
          <h3>${type === "residual" ? "Residual Risk" : "Current Risk"}</h3>
          <p class="meta-line">${escapeHtml(RISK_LIKELIHOOD_LABELS[Number(x)] || "")} · ${escapeHtml(RISK_IMPACT_LABELS[Number(y)] || "")}</p>
        </div>
        <span class="badge">${cell.items.length} item(s)</span>
      </div>
      <div class="summary-detail-list">
        ${cell.items.map(({ item, risk, pendingAreas, completion }) => `
          <article class="summary-detail-item">
            <div class="section-heading">
              <div>
                <h3>${escapeHtml(item.id)} · ${escapeHtml(item.title)}</h3>
                <p class="meta-line">Average CCB score: ${item.ccbDecisionAverageScore == null ? "N/A" : Number(item.ccbDecisionAverageScore).toFixed(2)} · Recommendation: ${escapeHtml(item.ccbDecisionRecommendation || "INCOMPLETE")}</p>
              </div>
              <button type="button" class="secondary" data-open-item-nav="${escapeHtml(item.id)}">Open Item</button>
            </div>
            <div class="summary-detail-grid">
              <div><span>Risk classification</span><strong>${escapeHtml(risk.level.toUpperCase())}</strong></div>
              <div><span>Impacted areas</span><strong>${escapeHtml(impactedAreaNames(item) || "None")}</strong></div>
              <div><span>Pending areas</span><strong>${escapeHtml(pendingAreas.join(", ") || "None")}</strong></div>
              <div><span>Evaluation completion</span><strong>${completion}%</strong></div>
              <div><span>Substantial</span><strong>${item.isSubstantial ? "Yes" : "No"}</strong></div>
              <div><span>Last updated</span><strong>${escapeHtml(formatDateTime(item.ccbDecisionLastUpdated || item.lastEvaluationDate || item.createdAt))}</strong></div>
            </div>
          </article>
        `).join("")}
      </div>
    </div>
  `;

  summaryRiskDetail.querySelectorAll("[data-open-item-nav]").forEach((button) => {
    button.addEventListener("click", () => {
      const openItemId = button.dataset.openItemNav || "";
      const openItem = state.openItems.find((candidate) => candidate.id === openItemId);
      if (!openItem) return;
      selectedOpenItemsImpactTab = isPrivilegedOpenItemsViewer() ? "all" : (getOpenItemImpactContext(openItem).impactingMe ? "impacting" : "not-impacting");
      selectedOpenItemsStatusTab = String(openItem.externalStatus || "NEW").trim().toUpperCase() || "NEW";
      selectedOpenItemId = openItemId;
      activateTab("items");
      renderOpenItems();
    });
  });
}

function renderSummaryDashboard(prerequisite) {
  const model = getGovernanceDashboardModel(prerequisite);

  dailyReport.dataset.copyText = buildExecutiveSummaryText(model);
  sourceStatusPill.textContent = `Google Sheets synced · Last saved ${formatDateTime(state.lastSavedAt)}`;
  sourceStatus.hidden = !summarySourceExpanded;
  const trendsToggle = document.getElementById("summary-trends-toggle");
  if (trendsToggle) {
    trendsToggle.textContent = summaryTrendsExpanded ? "Hide analytics" : "Show analytics";
  }

  const primaryKpis = model.kpis.filter((kpi) => ["active", "high-risk", "pending-evaluations", "blocked"].includes(kpi.id));
  const secondaryKpis = model.kpis.filter((kpi) => !["active", "high-risk", "pending-evaluations", "blocked"].includes(kpi.id));

  summaryKpiGrid.innerHTML = primaryKpis.map((kpi) => `
    <button type="button" class="summary-kpi-card summary-kpi-card--${kpi.tone}${selectedSummaryKpiFilter === kpi.id ? " is-active" : ""}" data-summary-kpi="${escapeHtml(kpi.id)}">
      <span>${escapeHtml(kpi.label)}</span>
      <strong>${escapeHtml(String(kpi.value))}</strong>
    </button>
  `).join("");

  summaryMoreMetrics.innerHTML = `
    <button type="button" class="secondary summary-disclosure-button" data-summary-more-metrics>
      ${summaryMoreMetricsExpanded ? "Hide more metrics" : "More metrics"}
    </button>
    ${summaryMoreMetricsExpanded ? `
      <div class="summary-kpi-grid summary-kpi-grid--secondary">
        ${secondaryKpis.map((kpi) => `
          <button type="button" class="summary-kpi-card summary-kpi-card--${kpi.tone}${selectedSummaryKpiFilter === kpi.id ? " is-active" : ""}" data-summary-kpi="${escapeHtml(kpi.id)}">
            <span>${escapeHtml(kpi.label)}</span>
            <strong>${escapeHtml(String(kpi.value))}</strong>
          </button>
        `).join("")}
      </div>
    ` : ""}
  `;

  summaryRiskMatrices.innerHTML = `
    ${renderRiskMatrix("Current Risk", model.currentMatrix, "current")}
    ${renderRiskMatrix("Residual Risk", model.residualMatrix, "residual")}
  `;
  renderSummaryRiskDetail(model);

  const visibleCriticalItems = summaryCriticalExpanded ? model.criticalItems : model.criticalItems.slice(0, 3);
  summaryCriticalItems.innerHTML = visibleCriticalItems.length
    ? visibleCriticalItems.map((entry) => `
      <article class="critical-item-row">
        <div class="critical-item-row-main">
          <div class="critical-item-row-title">
            <strong>${escapeHtml(entry.item.id)}</strong>
            <span>${escapeHtml(entry.item.title)}</span>
          </div>
          <div class="summary-badge-row summary-badge-row--compact">
            <span class="badge">${escapeHtml(entry.currentRisk.level.toUpperCase())}</span>
            ${entry.badges.slice(0, 3).map((badge) => `<span class="chip">${escapeHtml(badge)}</span>`).join("")}
          </div>
        </div>
        <div class="critical-item-row-metrics">
          <span><strong>${escapeHtml(entry.item.ccbDecisionRecommendation || "INCOMPLETE")}</strong> rec</span>
          <span><strong>${entry.item.ccbDecisionAverageScore == null ? "N/A" : Number(entry.item.ccbDecisionAverageScore).toFixed(2)}</strong> avg</span>
          <span><strong>${entry.pendingAreas.length}</strong> pending</span>
          <span><strong>${entry.completion}%</strong> complete</span>
        </div>
      </article>
    `).join("")
    : '<p class="meta-line">No critical items match the current governance filter.</p>';
  summaryCriticalActions.innerHTML = model.criticalItems.length > 3
    ? `<button type="button" class="secondary summary-disclosure-button" data-summary-critical-toggle>${summaryCriticalExpanded ? "Show top 3 critical items" : "View all critical items"}</button>`
    : "";

  summaryTrends.hidden = !summaryTrendsExpanded;
  summaryTrends.innerHTML = summaryTrendsExpanded
    ? [
        renderSummaryBars("Open Items by Status", Object.entries(model.statusCounts).map(([label, value]) => ({ label, value }))),
        renderSummaryBars("Evaluations Completed Over Time", model.evaluationsTimeline),
        renderSummaryBars("Risk Distribution by Area", model.riskByArea),
        renderSummaryBars("Areas with Most Pending Reviews", model.pendingByArea),
        renderSummaryBars("Average CCB Score Trend", model.averageScoreTrend, (value) => Number(value).toFixed(2)),
      ].join("")
    : "";

  summaryKpiGrid.querySelectorAll("[data-summary-kpi]").forEach((button) => {
    button.addEventListener("click", () => {
      const nextFilter = button.dataset.summaryKpi || "all";
      selectedSummaryKpiFilter = selectedSummaryKpiFilter === nextFilter ? "all" : nextFilter;
      renderSummaryDashboard(prerequisite);
    });
  });

  summaryMoreMetrics.querySelectorAll("[data-summary-kpi]").forEach((button) => {
    button.addEventListener("click", () => {
      const nextFilter = button.dataset.summaryKpi || "all";
      selectedSummaryKpiFilter = selectedSummaryKpiFilter === nextFilter ? "all" : nextFilter;
      renderSummaryDashboard(prerequisite);
    });
  });

  const moreMetricsButton = summaryMoreMetrics.querySelector("[data-summary-more-metrics]");
  if (moreMetricsButton) {
    moreMetricsButton.addEventListener("click", () => {
      summaryMoreMetricsExpanded = !summaryMoreMetricsExpanded;
      renderSummaryDashboard(prerequisite);
    });
  }

  summaryRiskMatrices.querySelectorAll("[data-risk-cell]").forEach((button) => {
    button.addEventListener("click", () => {
      selectedSummaryRiskCell = button.dataset.riskCell || "";
      renderSummaryDashboard(prerequisite);
    });
  });

  const criticalToggle = summaryCriticalActions.querySelector("[data-summary-critical-toggle]");
  if (criticalToggle) {
    criticalToggle.addEventListener("click", () => {
      summaryCriticalExpanded = !summaryCriticalExpanded;
      renderSummaryDashboard(prerequisite);
    });
  }
}

function renderEvaluationSummary(openItem) {
  const summary = getEvaluationSummary(openItem);
  const impactContext = getOpenItemImpactContext(openItem);
  const currentUserScoreLabel = Number.isFinite(summary.currentUserScore) ? summary.currentUserScore.toFixed(2) : "N/A";
  const averageScoreLabel = Number.isFinite(summary.averageScore) ? summary.averageScore.toFixed(2) : "N/A";
  const completion = getOpenItemEvaluationCompletion(openItem);
  return `
    <div class="evaluation-summary evaluation-summary--compact">
      <div class="evaluation-score-stack">
        <strong>${averageScoreLabel}</strong>
        <span>avg score</span>
        <span>${summary.totalEvaluators} evaluator${summary.totalEvaluators === 1 ? "" : "s"}</span>
        <span>${completion}% complete</span>
      </div>
      <div class="evaluation-summary-grid evaluation-summary-grid--compact">
        <div><span>My score</span><strong>${currentUserScoreLabel}</strong></div>
        <div><span>Average</span><strong>${averageScoreLabel}</strong></div>
        <div><span>Recommendation</span><strong>${summary.recommendation}</strong></div>
        <div><span>Updated</span><strong>${summary.lastUpdated ? formatDateTime(summary.lastUpdated) : "N/A"}</strong></div>
      </div>
      <div class="evaluation-summary-actions evaluation-summary-actions--compact">
        ${impactContext.pendingEvaluation ? '<span class="reference-badge">Pending evaluation</span>' : ""}
        ${impactContext.evaluationComplete ? '<span class="reference-badge">Evaluation completed</span>' : ""}
        ${impactContext.voteSubmitted ? '<span class="reference-badge">Vote submitted</span>' : ""}
        ${summary.fastTrack ? '<span class="reference-badge">Fast-track</span>' : ""}
      </div>
    </div>
  `;
}

function getOpenItemDetailTab(openItemId) {
  return openItemDetailTabById.get(openItemId) || "overview";
}

function getEvaluationCriterionTab(openItemId) {
  return evaluationCriterionTabById.get(openItemId) || "strategic-alignment";
}

function getInlineFieldKey(openItemId, criterionId, field) {
  return `${openItemId}:${criterionId}:${field}`;
}

function getOpenItemEditMode(openItemId) {
  return openItemEditModeById.get(openItemId) || "";
}

function handleFindTicketSelect(openItemId) {
  const targetItem = state.openItems.find((item) => item.id === openItemId);
  if (!targetItem) {
    return;
  }
  activateTab("items");
  selectedOpenItemsImpactTab = isPrivilegedOpenItemsViewer()
    ? "all"
    : (getOpenItemImpactContext(targetItem).impactingMe ? "impacting" : "not-impacting");
  selectedOpenItemsStatusTab = String(targetItem.externalStatus || "NEW").trim().toUpperCase() || "NEW";
  selectedOpenItemId = targetItem.id;
  closeOpenItemSearchPanel();
  highlightedOpenItemId = targetItem.id;
  renderHeaderOpenItemSearch();
  renderOpenItems();
  window.requestAnimationFrame(() => {
    const target = document.getElementById(`open-item-detail-${targetItem.id}`);
    if (target) {
      target.scrollIntoView({ behavior: "smooth", block: "start" });
    }
    window.setTimeout(() => {
      highlightedOpenItemId = "";
      renderOpenItems();
    }, 1800);
  });
}

function getOpenItemActivityModel(item) {
  const events = [];
  (item.votes || []).forEach((vote) => {
    events.push({
      at: vote.createdAt || "",
      label: `${vote.decision} vote`,
      detail: `${ownerAreaName(vote.areaId)}${vote.comment ? ` · ${vote.comment}` : ""}`,
    });
  });
  (item.preSessionChecks || []).forEach((check) => {
    if (!check.respondedAt && !check.requestedAt) return;
    events.push({
      at: check.respondedAt || check.requestedAt || "",
      label: check.decision ? `Pre-session ${check.decision}` : "Pre-session request",
      detail: `${ownerAreaName(check.areaId)}${check.comment ? ` · ${check.comment}` : ""}`,
    });
  });
  (state.ccbEvaluations || [])
    .filter((evaluation) => evaluation.openItemId === item.id)
    .forEach((evaluation) => {
      events.push({
        at: evaluation.updatedAt || evaluation.createdAt || "",
        label: `CCB eval · ${evaluation.criterionName}`,
        detail: `${evaluation.evaluatorName || evaluation.evaluatorEmail || "Evaluator"} · score ${evaluation.score}`,
      });
    });
  return events
    .sort((left, right) => String(right.at || "").localeCompare(String(left.at || "")))
    .slice(0, 10);
}

function renderOpenItems() {
  openItemsList.innerHTML = "";
  renderHeaderOpenItemSearch();

  const allActiveItems = state.openItems
    .filter((item) => item.status !== "closed")
    .slice()
    .sort((a, b) => getOpenItemPriority(b) - getOpenItemPriority(a) || Number(b.isSubstantial) - Number(a.isSubstantial) || a.id.localeCompare(b.id));
  const canSeeAllOpenItems = isPrivilegedOpenItemsViewer();
  const impactTabs = [
    {
      id: "impacting",
      label: "Impacting Me",
      items: allActiveItems.filter((item) => getOpenItemImpactContext(item).impactingMe),
    },
    {
      id: "not-impacting",
      label: "Not Impacting Me",
      items: allActiveItems.filter((item) => !getOpenItemImpactContext(item).impactingMe),
    },
    ...(canSeeAllOpenItems ? [{
      id: "all",
      label: "All Open Items",
      items: allActiveItems,
    }] : []),
  ];
  if (!impactTabs.some((tab) => tab.id === selectedOpenItemsImpactTab)) {
    selectedOpenItemsImpactTab = "impacting";
  }
  const selectedImpactTab = impactTabs.find((tab) => tab.id === selectedOpenItemsImpactTab) || impactTabs[0];
  const activeItems = selectedImpactTab?.items || [];
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
  const normalizedTicketSearch = openItemsSearchQuery.trim().toLowerCase();
  const matchesTicketSearch = (item) => {
    if (!normalizedTicketSearch) {
      return true;
    }
    const recommendation = getEvaluationSummary(item).recommendation || "";
    const searchableAreas = impactedAreaNames(item);
    const searchableText = [
      item.id,
      item.title,
      item.ownerName,
      searchableAreas,
      recommendation,
      item.externalStatus,
    ].join(" ").toLowerCase();
    return searchableText.includes(normalizedTicketSearch);
  };
  const visibleStatusItems = normalizedTicketSearch
    ? selectedStatusItems.filter(matchesTicketSearch)
    : selectedStatusItems;
  if (selectedStatusItems.length && !selectedStatusItems.some((item) => item.id === selectedOpenItemId)) {
    selectedOpenItemId = selectedStatusItems[0].id;
  }
  if (!selectedStatusItems.length) {
    selectedOpenItemId = "";
  }
  if (visibleStatusItems.length && !visibleStatusItems.some((item) => item.id === selectedOpenItemId)) {
    selectedOpenItemId = visibleStatusItems[0].id;
  }

  const selectedItem = visibleStatusItems.find((item) => item.id === selectedOpenItemId) || visibleStatusItems[0] || null;
  openItemsToolbarFilters.innerHTML = `
    <div class="open-items-nav-group">
      <span class="open-items-nav-label">Scope</span>
      <div class="open-items-impact-tabs" role="tablist" aria-label="Impact filter">
        ${impactTabs.map((tab) => `
          <button
            type="button"
            class="open-items-impact-tab${selectedOpenItemsImpactTab === tab.id ? " is-active" : ""}"
            data-open-items-impact-tab="${escapeHtml(tab.id)}"
          >
            <span>${escapeHtml(tab.label)}</span>
            <strong>${tab.items.length}</strong>
          </button>
        `).join("")}
      </div>
    </div>
    <div class="open-items-nav-group">
      <span class="open-items-nav-label">Status</span>
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
    </div>
  `;
  const renderOpenItemCard = (item) => {
    const node = openItemTemplate.content.firstElementChild.cloneNode(true);
    node.id = `open-item-detail-${item.id}`;
    node.classList.toggle("open-item-card--highlight", highlightedOpenItemId === item.id);
    const impactContext = getOpenItemImpactContext(item);
    const evaluationSummary = getEvaluationSummary(item);
    const pendingAreas = getOpenItemPendingAreas(item);
    const activityEntries = getOpenItemActivityModel(item);
    node.classList.toggle("open-item-card--impacting", impactContext.impactingMe);
    node.querySelector(".item-id").textContent = item.id;
    node.querySelector(".item-title").textContent = item.title;
    const statusRow = node.querySelector(".open-item-compact-status");
    statusRow.innerHTML = [
      item.isSubstantial ? '<span class="badge">SUBSTANTIAL</span>' : "",
      `<span class="decision-pill" data-decision="${escapeHtml(String(evaluationSummary.recommendation || "").toLowerCase())}">${escapeHtml(evaluationSummary.recommendation)}</span>`,
      (item.impactedAreaIds || []).length ? `<span class="chip">${escapeHtml(impactedAreaNames(item))}</span>` : "",
      pendingAreas.length ? `<span class="chip">Pending: ${escapeHtml(pendingAreas.join(", "))}</span>` : "",
      impactContext.pendingEvaluation ? '<span class="reference-badge">Pending evaluation</span>' : "",
      impactContext.voteSubmitted ? '<span class="reference-badge">Vote submitted</span>' : "",
      impactContext.evaluationComplete ? '<span class="reference-badge">Evaluation completed</span>' : "",
    ].filter(Boolean).join("");
    node.querySelector(".item-average-score").textContent = Number.isFinite(evaluationSummary.averageScore) ? evaluationSummary.averageScore.toFixed(2) : "N/A";
    node.querySelector(".item-evaluator-count").textContent = String(evaluationSummary.totalEvaluators || 0);
    node.querySelector(".item-pending-count").textContent = String(pendingAreas.length);
    node.querySelector(".item-impacted-count").textContent = String((item.impactedAreaIds || []).length);
    const tabsHost = node.querySelector("[data-open-item-detail-tabs]");
    const detailPanel = node.querySelector("[data-open-item-detail-panel]");
    let lastCommittedOwner = item.ownerName || "";
    let lastCommittedOwnerEmail = item.ownerEmail || "";
    let lastCommittedTitle = item.title || "";
    let lastCommittedDescription = item.description || "";
    const applyOpenItemOwnerSelection = async (selectedContact) => {
      const nextOwner = (selectedContact?.name || selectedContact?.email || "").trim();
      const nextOwnerEmail = (selectedContact?.email || "").trim().toLowerCase();
      if (!nextOwner || (nextOwner === lastCommittedOwner && nextOwnerEmail === lastCommittedOwnerEmail)) {
        return;
      }

      item.ownerName = nextOwner;
      item.ownerEmail = nextOwnerEmail;

      try {
        await saveStateSilently();
        lastCommittedOwner = item.ownerName || nextOwner;
        lastCommittedOwnerEmail = item.ownerEmail || nextOwnerEmail;
        if (getOpenItemDetailTab(item.id) === "overview") {
          renderDetailPanel();
        }
      } catch (error) {
        item.ownerName = lastCommittedOwner;
        item.ownerEmail = lastCommittedOwnerEmail;
        if (getOpenItemDetailTab(item.id) === "overview") {
          renderDetailPanel();
        }
        window.alert(error.message || "No se pudo guardar el open item owner.");
      }
    };

    const saveInlineOpenItemField = async (field, nextValue, options = {}) => {
      const previousValue = field === "title" ? lastCommittedTitle : field === "description" ? lastCommittedDescription : lastCommittedOwner;
      const previousEmail = lastCommittedOwnerEmail;
      if (field === "title") {
        item.title = nextValue.trim();
      } else if (field === "description") {
        item.description = nextValue.trim();
      } else if (field === "owner") {
        item.ownerName = nextValue.trim();
        if (!options.keepEmail) {
          item.ownerEmail = "";
        }
      }

      try {
        await saveStateSilently();
        if (field === "title") {
          lastCommittedTitle = item.title;
        } else if (field === "description") {
          lastCommittedDescription = item.description;
        } else if (field === "owner") {
          lastCommittedOwner = item.ownerName;
          lastCommittedOwnerEmail = item.ownerEmail || "";
        }
        openItemEditModeById.delete(item.id);
        renderOpenItems();
        showAppToast("Open Item updated");
      } catch (error) {
        if (field === "title") {
          item.title = previousValue;
        } else if (field === "description") {
          item.description = previousValue;
        } else if (field === "owner") {
          item.ownerName = previousValue;
          item.ownerEmail = previousEmail;
        }
        openItemEditModeById.delete(item.id);
        renderOpenItems();
        window.alert(error.message || "No se pudo actualizar el Open Item.");
      }
    };

    const evaluator = getCurrentEvaluator();
    const criteria = loadCcbDecisionCriteria();
    let evaluationDraft = buildEvaluationDraft(item.id, evaluator.email);
    const uiKey = `${item.id}:${evaluator.email || "anon"}`;
    const validationState = evaluationUiState.get(uiKey) || { touched: {}, attemptedSave: false };
    evaluationUiState.set(uiKey, validationState);
    const renderEvaluationPanel = () => {
      const saveStatus = evaluationSaveStatusById.get(item.id) || "";
      const weightedScore = calculateWeightedScore(evaluationDraft);
      const recommendation = getDecisionRecommendation(weightedScore);
      const warnings = getEvaluationWarnings(item, evaluationDraft);
      const shouldShowGlobalWarnings = validationState.attemptedSave;
      const visibleWarnings = shouldShowGlobalWarnings
        ? warnings.filter((warning) => !warning.includes(": score required.") && !warning.includes(": rationale required."))
        : [];
      const selectedCriterionId = getEvaluationCriterionTab(item.id);

      detailPanel.innerHTML = `
        <div class="evaluation-panel-shell">
          <div class="evaluation-summary evaluation-summary--compact">
            <div class="evaluation-score-stack">
              <strong>${recommendation}</strong>
              <span>${Number.isFinite(weightedScore) ? weightedScore.toFixed(2) : "Incomplete"} avg</span>
              <span>${evaluationSummary.totalEvaluators} evaluator${evaluationSummary.totalEvaluators === 1 ? "" : "s"}</span>
              <span>${getOpenItemEvaluationCompletion(item)}% complete</span>
            </div>
            <div class="evaluation-summary-actions">
              ${impactContext.pendingEvaluation ? '<span class="reference-badge">Pending evaluation</span>' : ""}
              ${impactContext.evaluationComplete ? '<span class="reference-badge">Completed</span>' : ""}
              ${saveStatus ? `<span class="evaluation-save-status">${escapeHtml(saveStatus)}</span>` : ""}
              <button type="button" class="icon-affirm" data-save-evaluation aria-label="Save evaluation">✓</button>
              <button type="button" class="icon-dismiss" data-cancel-evaluation aria-label="Cancel changes">✕</button>
            </div>
          </div>
          ${visibleWarnings.length ? `<div class="evaluation-warning-list">${visibleWarnings.map((warning) => `<div class="evaluation-warning">${escapeHtml(warning)}</div>`).join("")}</div>` : ""}
          <div class="evaluation-criterion-tabs">
            ${criteria.map((criterion) => `
              <button type="button" class="evaluation-criterion-tab${selectedCriterionId === criterion.id ? " is-active" : ""}" data-evaluation-criterion-tab="${escapeHtml(criterion.id)}">${escapeHtml(criterion.name.replace(" Alignment", "").replace(" Assessment", "").replace(" Impact", "").replace(" Value", "").replace(" Feasibility", ""))}</button>
            `).join("")}
          </div>
          <div class="evaluation-criteria-list">
            ${criteria.filter((criterion) => criterion.id === selectedCriterionId).map((criterion) => {
              const entry = evaluationDraft.find((candidate) => candidate.criterionId === criterion.id) || {};
              const rationaleTouched = Boolean(validationState.touched[`${criterion.id}:rationale`]);
              const scoreTouched = Boolean(validationState.touched[`${criterion.id}:score`]);
              const showRationaleError = (validationState.attemptedSave || rationaleTouched) && !String(entry.rationale || "").trim();
              const showScoreError = (validationState.attemptedSave || scoreTouched) && !Number.isInteger(Number(entry.score));
              const rationaleKey = getInlineFieldKey(item.id, criterion.id, "rationale");
              const showRationale = openItemInlineFieldState.has(rationaleKey) || Boolean(entry.rationale) || Number.isInteger(Number(entry.score)) || showRationaleError;
              const showSupporting = openItemInlineFieldState.has(getInlineFieldKey(item.id, criterion.id, "supporting")) || Boolean(entry.supportingReference);
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
                    ${showScoreError ? `<div class="evaluation-inline-warning">Please select a score for ${escapeHtml(criterion.name)}.</div>` : ""}
                  </div>
                  <div class="evaluation-inline-actions">
                    ${showRationale ? `
                      <div class="evaluation-inline-field">
                        <p class="field-label">Decision justification</p>
                        <textarea data-evaluation-rationale data-criterion-id="${escapeHtml(criterion.id)}" rows="3" placeholder="Required decision justification">${escapeHtml(entry.rationale || "")}</textarea>
                        <p class="evaluation-helper-text">Explain why this score was selected.</p>
                        ${showRationaleError ? `<div class="evaluation-inline-warning">Please provide a decision justification for ${escapeHtml(criterion.name)}.</div>` : ""}
                      </div>
                    ` : `<button type="button" class="secondary" data-show-inline-field="${escapeHtml(getInlineFieldKey(item.id, criterion.id, "rationale"))}">Add justification</button>`}
                    ${showSupporting ? `
                      <div class="evaluation-inline-field">
                        <p class="field-label">Supporting data / reference</p>
                        <input data-evaluation-supporting data-criterion-id="${escapeHtml(criterion.id)}" value="${escapeHtml(entry.supportingReference || "")}" placeholder="Optional supporting reference" />
                      </div>
                    ` : `<button type="button" class="secondary" data-show-inline-field="${escapeHtml(getInlineFieldKey(item.id, criterion.id, "supporting"))}">Add reference</button>`}
                  </div>
                </section>
              `;
            }).join("")}
          </div>
        </div>
      `;

      const updateDraft = () => {
        const visibleCriterionId = getEvaluationCriterionTab(item.id);
        const draftEntry = evaluationDraft.find((candidate) => candidate.criterionId === visibleCriterionId);
        if (!draftEntry) {
          return;
        }
        const activeScoreButton = detailPanel.querySelector(`[data-evaluation-score][data-criterion-id="${visibleCriterionId}"].is-active`);
        if (activeScoreButton) {
          draftEntry.score = Number(activeScoreButton.dataset.score);
        }
        const rationaleField = detailPanel.querySelector(`[data-evaluation-rationale][data-criterion-id="${visibleCriterionId}"]`);
        if (rationaleField) {
          draftEntry.rationale = rationaleField.value.trim();
        }
        const supportingField = detailPanel.querySelector(`[data-evaluation-supporting][data-criterion-id="${visibleCriterionId}"]`);
        if (supportingField) {
          draftEntry.supportingReference = supportingField.value.trim();
        }
      };

      detailPanel.querySelectorAll("[data-evaluation-criterion-tab]").forEach((button) => {
        button.addEventListener("click", () => {
          updateDraft();
          evaluationCriterionTabById.set(item.id, button.dataset.evaluationCriterionTab || "strategic-alignment");
          renderEvaluationPanel();
        });
      });

      detailPanel.querySelectorAll("[data-show-inline-field]").forEach((button) => {
        button.addEventListener("click", () => {
          openItemInlineFieldState.add(button.dataset.showInlineField || "");
          renderEvaluationPanel();
        });
      });

      detailPanel.querySelectorAll("[data-evaluation-score]").forEach((button) => {
        button.addEventListener("click", () => {
          const criterionId = button.dataset.criterionId;
          validationState.touched[`${criterionId}:score`] = true;
          evaluationSaveStatusById.delete(item.id);
          openItemInlineFieldState.add(getInlineFieldKey(item.id, criterionId, "rationale"));
          updateDraft();
          detailPanel.querySelectorAll(`[data-evaluation-score][data-criterion-id="${criterionId}"]`).forEach((candidate) => candidate.classList.remove("is-active"));
          button.classList.add("is-active");
          updateDraft();
          renderEvaluationPanel();
          window.requestAnimationFrame(() => {
            detailPanel.querySelector(`[data-evaluation-rationale][data-criterion-id="${criterionId}"]`)?.focus();
          });
        });
      });

      detailPanel.querySelectorAll("[data-evaluation-rationale]").forEach((field) => {
        field.addEventListener("blur", () => {
          validationState.touched[`${field.dataset.criterionId}:rationale`] = true;
          updateDraft();
          renderEvaluationPanel();
        });
        field.addEventListener("input", () => {
          evaluationSaveStatusById.delete(item.id);
          updateDraft();
        });
      });

      detailPanel.querySelectorAll("[data-evaluation-supporting]").forEach((field) => {
        field.addEventListener("input", () => {
          evaluationSaveStatusById.delete(item.id);
          updateDraft();
        });
      });

      detailPanel.querySelector("[data-save-evaluation]")?.addEventListener("click", async () => {
        validationState.attemptedSave = true;
        updateDraft();
        const blockingWarnings = getEvaluationWarnings(item, evaluationDraft).filter((warning) => (
          warning.startsWith("Please select a score") || warning.startsWith("Please provide a decision justification")
        ));
        if (blockingWarnings.length) {
          const firstMissingCriterion = criteria.find((criterion) => {
            const entry = evaluationDraft.find((candidate) => candidate.criterionId === criterion.id) || {};
            return !Number.isInteger(Number(entry.score)) || !String(entry.rationale || "").trim();
          });
          if (firstMissingCriterion) {
            evaluationCriterionTabById.set(item.id, firstMissingCriterion.criterionId);
            openItemInlineFieldState.add(getInlineFieldKey(item.id, firstMissingCriterion.criterionId, "rationale"));
          }
          renderEvaluationPanel();
          window.requestAnimationFrame(() => {
            const targetField = detailPanel.querySelector(`[data-evaluation-rationale][data-criterion-id="${firstMissingCriterion?.criterionId || ""}"]`);
            if (targetField) {
              targetField.scrollIntoView({ behavior: "smooth", block: "center" });
              targetField.focus();
            }
          });
          return;
        }
        try {
          await withGlobalBusy(async () => {
            if (evaluator.area && !item.impactedAreaIds.includes(evaluator.area)) {
              item.impactedAreaIds.push(evaluator.area);
            }
            const payload = await saveCcbEvaluation(item.id, evaluator, evaluationDraft);
            Object.assign(state, payload.state);
          });
          validationState.attemptedSave = false;
          evaluationSaveStatusById.set(item.id, "Dato actualizado");
          renderReferencePanels();
          renderOpenItems();
          showAppToast("Dato actualizado");
        } catch (error) {
          window.alert(error.message || "No se pudo guardar la evaluacion.");
        }
      });

      detailPanel.querySelector("[data-cancel-evaluation]")?.addEventListener("click", () => {
        evaluationDraft = buildEvaluationDraft(item.id, evaluator.email);
        validationState.attemptedSave = false;
        evaluationSaveStatusById.delete(item.id);
        renderEvaluationPanel();
      });
    };

    const renderDetailPanel = () => {
      const currentTab = getOpenItemDetailTab(item.id);
      const compactOwnerSummary = formatOwnerLine(item) || `Source: ${item.sourceRef || "N/A"}`;
      const recommendationLabel = evaluationSummary.recommendation;
      const averageScoreLabel = Number.isFinite(evaluationSummary.averageScore) ? evaluationSummary.averageScore.toFixed(2) : "N/A";
      const editMode = getOpenItemEditMode(item.id);
      tabsHost.innerHTML = ["overview", "votes", "evaluation", "risk", "activity"].map((tab) => `
        <button type="button" class="open-item-detail-tab${currentTab === tab ? " is-active" : ""}" data-open-item-detail-tab="${tab}">
          ${tab === "overview" ? "Overview" : tab === "votes" ? "Votes" : tab === "evaluation" ? "CCB Evaluation" : tab === "risk" ? "Risk" : "Activity"}
        </button>
      `).join("");

      if (currentTab === "overview") {
        const isEditingTitle = editMode === "title";
        const isEditingDescription = editMode === "description";
        const isEditingOwner = editMode === "owner";
        node.querySelector(".open-item-compact-title").innerHTML = `
          <p class="item-id">${escapeHtml(item.id)}</p>
          ${
            isEditingTitle
              ? `
                <div class="inline-edit-shell inline-edit-shell--title">
                  <input class="inline-edit-input" data-open-item-title-input value="${escapeHtml(item.title || "")}" />
                  <div class="inline-edit-actions">
                    <button type="button" class="icon-affirm" data-open-item-title-save aria-label="Save title">✓</button>
                    <button type="button" class="icon-dismiss" data-open-item-title-cancel aria-label="Cancel title">✕</button>
                  </div>
                </div>
              `
              : `
                <div class="inline-display-row">
                  <h3 class="item-title">${escapeHtml(item.title)}</h3>
                  <button type="button" class="icon-edit-button" data-open-item-edit="title" aria-label="Edit title">✎</button>
                </div>
              `
          }
        `;
        detailPanel.innerHTML = `
          <div class="open-item-pane-grid open-item-pane-grid--overview">
            <div class="open-item-pane-block">
              <p class="field-label">Owner</p>
              ${
                isEditingOwner
                  ? `
                    <div class="autocomplete-shell inline-edit-shell inline-edit-shell--field">
                      <input data-open-item-owner-input class="inline-edit-input" placeholder="Search owner" autocomplete="off" value="${escapeHtml(item.ownerName || "")}" />
                      <div class="inline-edit-actions">
                        <button type="button" class="icon-affirm" data-open-item-owner-save aria-label="Save owner">✓</button>
                        <button type="button" class="icon-dismiss" data-open-item-owner-cancel aria-label="Cancel owner">✕</button>
                      </div>
                      <div class="autocomplete-list" data-open-item-owner-autocomplete hidden></div>
                    </div>
                  `
                  : `
                    <div class="inline-display-row">
                      <strong class="inline-display-value">${escapeHtml(item.ownerName || "Unassigned")}</strong>
                      <button type="button" class="icon-edit-button" data-open-item-edit="owner" aria-label="Edit owner">✎</button>
                    </div>
                  `
              }
              <p class="meta-line" data-open-item-owner-summary>${escapeHtml(compactOwnerSummary)}</p>
            </div>
            <div class="open-item-pane-block">
              <p class="field-label">Status</p>
              <div class="open-item-inline-metrics">
                <span class="decision-pill" data-decision="${escapeHtml(recommendationLabel.toLowerCase())}">${escapeHtml(recommendationLabel)}</span>
                <span class="chip">${averageScoreLabel} avg</span>
                <span class="chip">${evaluationSummary.totalEvaluators} evaluators</span>
                <span class="chip">${pendingAreas.length} pending</span>
              </div>
            </div>
            <div class="open-item-pane-block open-item-pane-block--wide">
              <div class="inline-display-row">
                <p class="field-label">Description</p>
                ${isEditingDescription ? "" : '<button type="button" class="icon-edit-button" data-open-item-edit="description" aria-label="Edit description">✎</button>'}
              </div>
              ${
                isEditingDescription
                  ? `
                    <div class="inline-edit-shell inline-edit-shell--stack">
                      <textarea class="inline-edit-textarea" data-open-item-description-input rows="4" placeholder="Description">${escapeHtml(item.description || "")}</textarea>
                      <div class="inline-edit-actions">
                        <button type="button" class="icon-affirm" data-open-item-description-save aria-label="Save description">✓</button>
                        <button type="button" class="icon-dismiss" data-open-item-description-cancel aria-label="Cancel description">✕</button>
                      </div>
                    </div>
                  `
                  : `<p class="meta-line">${escapeHtml(item.description || "No description provided.")}</p>`
              }
            </div>
            <div class="open-item-pane-block open-item-pane-block--wide">
              ${renderEvaluationSummary(item)}
            </div>
          </div>
        `;
      } else if (currentTab === "votes") {
        const votingPermission = getVotingPermission(item);
        detailPanel.innerHTML = `
          <div class="open-item-pane-stack">
            <div class="vote-list">${renderVotes(item.votes)}</div>
            <form class="vote-form open-item-inline-form">
              ${
                votingPermission.isAdmin
                  ? '<select name="areaId"></select>'
                  : `<div class="vote-area-readonly"><span class="field-label">Voting as</span><span class="chip">${escapeHtml(votingPermission.areaId ? ownerAreaName(votingPermission.areaId) : "No area")}</span></div>`
              }
              <select name="decision">
                <option value="approve">Approve</option>
                <option value="reject">Reject</option>
                <option value="needs-info">Needs info</option>
              </select>
              <input name="comment" placeholder="Comentario del voto" />
              <button type="submit"${votingPermission.canVote ? "" : " disabled"}>Submit vote</button>
            </form>
            ${votingPermission.message ? `<p class="meta-line">${escapeHtml(votingPermission.message)}</p>` : ""}
          </div>
        `;
        const voteAreaSelect = detailPanel.querySelector("select[name=\"areaId\"]");
        if (voteAreaSelect) {
          state.areas.forEach((area) => {
            const option = document.createElement("option");
            option.value = area.id;
            option.textContent = area.name;
            voteAreaSelect.appendChild(option);
          });
          voteAreaSelect.value = votingPermission.areaId || voteAreaSelect.value;
        }
        detailPanel.querySelector(".vote-form").addEventListener("submit", async (event) => {
          event.preventDefault();
          if (!votingPermission.canVote) {
            window.alert(votingPermission.message || "Not authorized");
            return;
          }
          const formData = new FormData(event.currentTarget);
          try {
            await withGlobalBusy(async () => {
              await saveVote(item.id, {
                areaId: votingPermission.isAdmin ? formData.get("areaId") : votingPermission.areaId,
                decision: formData.get("decision"),
                comment: String(formData.get("comment") || "").trim(),
              });
            });
            renderReferencePanels();
            renderOpenItems();
            showAppToast("Dato actualizado");
          } catch (error) {
            window.alert(error.message || "No se pudo guardar el voto.");
          }
        });
      } else if (currentTab === "evaluation") {
        renderEvaluationPanel();
      } else if (currentTab === "risk") {
        const currentRisk = calculateCurrentRisk(item);
        const residualRisk = calculateResidualRisk(item);
        detailPanel.innerHTML = `
          <div class="open-item-pane-grid">
            <div class="open-item-pane-block">
              <p class="field-label">Current risk</p>
              <div class="risk-mini-score">
                <strong>${currentRisk.level.toUpperCase()}</strong>
                <span>${currentRisk.score}</span>
              </div>
            </div>
            <div class="open-item-pane-block">
              <p class="field-label">Residual risk</p>
              <div class="risk-mini-score">
                <strong>${residualRisk.level.toUpperCase()}</strong>
                <span>${residualRisk.score}</span>
              </div>
            </div>
            <div class="open-item-pane-block">
              <p class="field-label">Impacted areas</p>
              <p class="meta-line">${escapeHtml(impactedAreaNames(item) || "None")}</p>
            </div>
            <div class="open-item-pane-block">
              <p class="field-label">Pending reviews</p>
              <p class="meta-line">${escapeHtml(pendingAreas.join(", ") || "None")}</p>
            </div>
            <div class="open-item-pane-block open-item-pane-block--wide">
              <p class="field-label">Decision posture</p>
              ${renderEvaluationSummary(item)}
            </div>
          </div>
        `;
      } else {
        detailPanel.innerHTML = `
          <div class="open-item-activity-list">
            ${activityEntries.length ? activityEntries.map((entry) => `
              <div class="open-item-activity-row">
                <strong>${escapeHtml(entry.label)}</strong>
                <span>${escapeHtml(formatDateTime(entry.at))}</span>
                <p class="meta-line">${escapeHtml(entry.detail)}</p>
              </div>
            `).join("") : '<p class="meta-line">No activity yet.</p>'}
          </div>
        `;
      }

      tabsHost.querySelectorAll("[data-open-item-detail-tab]").forEach((button) => {
        button.addEventListener("click", () => {
          openItemDetailTabById.set(item.id, button.dataset.openItemDetailTab || "overview");
          renderDetailPanel();
        });
      });

      if (currentTab === "overview") {
        node.querySelectorAll("[data-open-item-edit]").forEach((button) => {
          button.addEventListener("click", () => {
            openItemEditModeById.set(item.id, button.dataset.openItemEdit || "");
            renderDetailPanel();
          });
        });

        detailPanel.querySelector("[data-open-item-description-save]")?.addEventListener("click", async () => {
          const value = detailPanel.querySelector("[data-open-item-description-input]")?.value || "";
          await saveInlineOpenItemField("description", value);
        });
        detailPanel.querySelector("[data-open-item-description-cancel]")?.addEventListener("click", () => {
          item.description = lastCommittedDescription;
          openItemEditModeById.delete(item.id);
          renderDetailPanel();
        });
        detailPanel.querySelector("[data-open-item-title-save]")?.addEventListener("click", async () => {
          const value = node.querySelector("[data-open-item-title-input]")?.value || "";
          if (!String(value).trim()) {
            return;
          }
          await saveInlineOpenItemField("title", value);
        });
        node.querySelector("[data-open-item-title-cancel]")?.addEventListener("click", () => {
          item.title = lastCommittedTitle;
          openItemEditModeById.delete(item.id);
          renderOpenItems();
        });
        node.querySelector("[data-open-item-title-input]")?.addEventListener("keydown", async (event) => {
          if (event.key === "Enter") {
            event.preventDefault();
            const value = event.currentTarget.value || "";
            if (!String(value).trim()) {
              return;
            }
            await saveInlineOpenItemField("title", value);
          }
          if (event.key === "Escape") {
            item.title = lastCommittedTitle;
            openItemEditModeById.delete(item.id);
            renderOpenItems();
          }
        });

        const overviewOwnerInput = detailPanel.querySelector("[data-open-item-owner-input]");
        const overviewOwnerDropdown = detailPanel.querySelector("[data-open-item-owner-autocomplete]");
        if (overviewOwnerInput && overviewOwnerDropdown) {
          overviewOwnerInput.addEventListener("input", (event) => {
            const query = event.currentTarget.value.trim();
            item.ownerName = query;
            item.ownerEmail = "";
            const summaryLine = detailPanel.querySelector("[data-open-item-owner-summary]");
            if (summaryLine) {
              summaryLine.textContent = formatOwnerLine(item) || `Source: ${item.sourceRef || "N/A"}`;
            }
            if (!query) {
              overviewOwnerDropdown.hidden = true;
              overviewOwnerDropdown.innerHTML = "";
              return;
            }
            renderContactAutocomplete(
              overviewOwnerDropdown,
              query,
              (selectedContact) => {
                overviewOwnerInput.value = selectedContact?.name || selectedContact?.email || "";
                item.ownerName = overviewOwnerInput.value;
                item.ownerEmail = selectedContact?.email || "";
                const ownerSummary = detailPanel.querySelector("[data-open-item-owner-summary]");
                if (ownerSummary) {
                  ownerSummary.textContent = formatOwnerLine(item) || `Source: ${item.sourceRef || "N/A"}`;
                }
              },
              "Sin coincidencias en el directorio de empleados.",
              { requireEmail: true },
            );
          });
          overviewOwnerInput.addEventListener("blur", () => {
            window.setTimeout(() => {
              overviewOwnerDropdown.hidden = true;
            }, 120);
          });
          detailPanel.querySelector("[data-open-item-owner-save]")?.addEventListener("click", async () => {
            const query = overviewOwnerInput.value.trim();
            const exactContact = findContactByName(query) || findContactByEmail(query);
            if (exactContact?.email) {
              item.ownerName = exactContact.name || exactContact.email;
              item.ownerEmail = exactContact.email;
              await saveInlineOpenItemField("owner", item.ownerName, { keepEmail: true });
              return;
            }
            if (query) {
              item.ownerName = query;
              await saveInlineOpenItemField("owner", query, { keepEmail: true });
            }
          });
          detailPanel.querySelector("[data-open-item-owner-cancel]")?.addEventListener("click", () => {
            item.ownerName = lastCommittedOwner;
            item.ownerEmail = lastCommittedOwnerEmail;
            openItemEditModeById.delete(item.id);
            renderDetailPanel();
          });
        }

        detailPanel.querySelector("[data-open-item-description-input]")?.addEventListener("keydown", async (event) => {
          if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
            event.preventDefault();
            await saveInlineOpenItemField("description", event.currentTarget.value || "");
          }
          if (event.key === "Escape") {
            item.description = lastCommittedDescription;
            openItemEditModeById.delete(item.id);
            renderDetailPanel();
          }
        });
      }
    };

    renderDetailPanel();

    return node;
  };

  if (!availableStatusTabs.length) {
    openItemsList.innerHTML = "<p class=\"meta-line\">No hay open items activos.</p>";
    return;
  }

  openItemsList.innerHTML = `
    <div class="open-items-shell">
      <div class="open-items-nav-group">
        <span class="open-items-nav-label">Open Items</span>
        <div class="open-items-ticket-tabs" role="tablist" aria-label="Open items">
          ${visibleStatusItems.map((item) => `
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
      </div>
      <div class="open-items-active-card"></div>
    </div>
  `;

  document.querySelectorAll("[data-open-items-impact-tab]").forEach((button) => {
    button.addEventListener("click", () => {
      selectedOpenItemsImpactTab = button.dataset.openItemsImpactTab || "impacting";
      selectedOpenItemId = "";
      renderOpenItems();
    });
  });

  document.querySelectorAll("[data-open-items-status-tab]").forEach((button) => {
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
  renderSummaryDashboard(prerequisite);
}

async function saveState() {
  await withGlobalBusy(async () => {
    const response = await fetch(apiUrl("/api/state"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(state),
    });
    const payload = await parseApiResponse(response, "No se pudo guardar el estado.");
    Object.assign(state, payload.state);
    refreshDerivedViews();
    await loadPreSessionDashboard();
  });
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
  renderSummaryDashboard(payload.prerequisite);
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
  renderSummaryDashboard(payload.prerequisite);
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
  selectedOpenItemsImpactTab = isPrivilegedOpenItemsViewer()
    ? "all"
    : ((getCurrentEvaluator().area && impactedAreaIds.includes(getCurrentEvaluator().area)) ? "impacting" : "not-impacting");
  selectedOpenItemsStatusTab = "NEW";
  selectedOpenItemId = nextOpenItem.id;

  event.currentTarget.reset();
  openItemDrawerDescriptionExpanded = false;
  syncOpenItemDrawerDescription();
  updateNextOpenItemSuggestion(true);
  refreshDerivedViews();

  try {
    await saveState();
    setOpenItemDrawerVisibility(false);
    showAppToast("Open Item created");
  } catch (error) {
    state.openItems = state.openItems.filter((item) => item !== nextOpenItem);
    refreshDerivedViews();
    window.alert(error.message || "No se pudo crear el open item en Google Sheets.");
  }
});

document.getElementById("save-state").addEventListener("click", async () => {
  if (!isAdminOverrideUser()) {
    notifyNotAuthorized();
    return;
  }
  try {
    await saveState();
    showAppToast("Dato actualizado");
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

document.getElementById("export-debug-state")?.addEventListener("click", () => {
  if (!isAdminOverrideUser()) {
    notifyNotAuthorized();
    return;
  }
  const blob = new Blob([JSON.stringify(state, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `ccb-debug-${new Date().toISOString().slice(0, 10)}.json`;
  link.click();
  URL.revokeObjectURL(url);
});

document.getElementById("load-seed").addEventListener("click", async () => {
  if (!isAdminOverrideUser()) {
    notifyNotAuthorized();
    return;
  }
  try {
    await withGlobalBusy(async () => {
      const response = await fetch(apiUrl("/api/reset"), { method: "POST" });
      await applyImportedPayload(response);
    });
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

document.addEventListener("click", (event) => {
  if (!headerOpenItemSearch || !openItemsSearchPanelOpen) {
    return;
  }
  const shell = headerOpenItemSearch.querySelector("[data-open-item-search-shell]");
  if (shell && !shell.contains(event.target)) {
    closeOpenItemSearchPanel();
    renderHeaderOpenItemSearch();
  }
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && openItemsSearchPanelOpen) {
    closeOpenItemSearchPanel();
    renderHeaderOpenItemSearch();
  }
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
    renderSummaryDashboard(payload.prerequisite);
    await loadPreSessionDashboard();
  } catch (error) {
    window.alert(error.message || "No se pudo ejecutar la sincronizacion.");
  }
});

openNewItemDrawerButton.addEventListener("click", () => {
  openItemDrawerDescriptionExpanded = false;
  syncOpenItemDrawerDescription();
  setOpenItemDrawerVisibility(true);
});

document.querySelectorAll("[data-new-item-close]").forEach((button) => {
  button.addEventListener("click", () => {
    setOpenItemDrawerVisibility(false);
  });
});

toggleOpenItemDescriptionButton.addEventListener("click", () => {
  openItemDrawerDescriptionExpanded = true;
  syncOpenItemDrawerDescription();
  openItemDescriptionField.focus();
});

openItemDescriptionField.addEventListener("input", () => {
  syncOpenItemDrawerDescription();
});

syncOpenItemDrawerDescription();

document.getElementById("governance-settings-toggle").addEventListener("click", () => {
  governanceSettingsDrawer.hidden = false;
});

document.getElementById("source-status-toggle").addEventListener("click", () => {
  summarySourceExpanded = !summarySourceExpanded;
  sourceStatus.hidden = !summarySourceExpanded;
});

document.getElementById("summary-trends-toggle").addEventListener("click", () => {
  summaryTrendsExpanded = !summaryTrendsExpanded;
  document.getElementById("summary-trends-toggle").textContent = summaryTrendsExpanded ? "Hide analytics" : "Show analytics";
  renderSummaryDashboard(derivePrerequisiteLocally());
});

governanceSettingsDrawer.querySelectorAll("[data-governance-close]").forEach((button) => {
  button.addEventListener("click", () => {
    governanceSettingsDrawer.hidden = true;
  });
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

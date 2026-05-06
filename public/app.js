const state = {
  areas: [],
  openItems: [],
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
    title: "Strategic Alignment",
    weight: "0.30",
    note: "Debe puntuar >= 4 para aprobar.",
    bullets: ["Supports product roadmap", "Advances business goals", "Enhances competitive position"],
  },
  {
    title: "Risk Assessment",
    weight: "0.25",
    note: "Rechazar si safety/compliance risk > 3.",
    bullets: ["Technical complexity", "Safety/compliance risks", "Customer impact if failed"],
  },
  {
    title: "Resource Impact",
    weight: "0.20",
    note: "Levantar bandera si score <= 2.",
    bullets: ["Engineering hours", "Cost (dev, testing, rollout)", "Timeline disruption"],
  },
  {
    title: "Customer Value",
    weight: "0.15",
    note: "Requiere supporting data.",
    bullets: ["Solves critical pain points", "Expected adoption/upsell", "CSAT/NPS impact"],
  },
  {
    title: "Operational Feasibility",
    weight: "0.10",
    note: "Evalua si realmente se puede ejecutar ahora.",
    bullets: ["Ease of implementation", "Maintenance burden", "Supplier/partner readiness"],
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
const APP_BASE_PATH = document.documentElement.dataset.basePath || "";

function apiUrl(path) {
  return `${APP_BASE_PATH}${path}`;
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
  state.openItems.forEach((item) => pushContact(item.ownerName, ""));
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

function getMatchingContacts(query) {
  const normalizedQuery = String(query || "").trim().toLowerCase();
  const contacts = collectKnownContacts();
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

function renderContactAutocomplete(dropdown, query, onSelect, emptyMessage = "") {
  const matches = getMatchingContacts(query);
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
  nextOpenItemHint.textContent = `Siguiente sugerido: ${nextId}`;
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
    ...CCB_FRAMEWORK.map((item) => `
      <article class="reference-card">
        <h3>${item.title}</h3>
        <p>${item.note}</p>
        <ul>${item.bullets.map((bullet) => `<li>${bullet}</li>`).join("")}</ul>
        <span class="reference-badge">Weight ${item.weight}</span>
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
  const gmailDeliveryAvailable = Boolean(preSessionDashboard?.gmailDeliveryAvailable);
  const ownedAreas = ownerView.ownedAreas || [];
  const pendingItems = ownerView.pendingItems || [];
  const answeredItems = ownerView.answeredItems || [];

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
        <article class="presession-card">
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
        <article class="presession-card">
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
  preSessionJobSummary.innerHTML = [
    `<div><strong>Owners pendientes:</strong> ${ownerQueue.length}</div>`,
    `<div><strong>Open items sin respuesta:</strong> ${ownerQueue.reduce((total, owner) => total + Number(owner.pendingCount || 0), 0)}</div>`,
    `<div><strong>Envio Gmail:</strong> ${gmailDeliveryAvailable ? "Disponible con tu OAuth conectado" : "No disponible, el job generara preview si falta autorizacion"}</div>`,
  ].join("");

  if (!preSessionJobStatus.textContent.trim()) {
    preSessionJobStatus.textContent = ownerQueue.length
      ? ownerQueue.map((owner) => `- ${owner.ownerEmail}: ${owner.pendingCount} pendiente(s)`).join("\n")
      : "No hay owners con solicitudes pendientes.";
  }

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
        <div>
          <h3>${area.name}</h3>
          <p class="meta-line">${area.sourceColumn || area.id}</p>
        </div>
        <span class="chip">${area.id}</span>
      </div>
      <div class="area-editor-grid">
        <div class="autocomplete-shell">
          <input data-area-field="owner" data-area-id="${area.id}" data-role="owner" placeholder="Owner" value="${area.owner || ""}" autocomplete="off" />
          <div class="autocomplete-list" data-owner-autocomplete="${area.id}" hidden></div>
        </div>
        <input data-area-field="email" data-area-id="${area.id}" data-role="email" list="email-suggestions" type="email" placeholder="Email del owner" value="${area.email || ""}" />
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
          const emailInput = areasList.querySelector(`[data-area-field="email"][data-area-id="${area.id}"]`);
          if (emailInput) {
            emailInput.value = contact.email;
          }
        }

        const dropdown = areasList.querySelector(`[data-owner-autocomplete="${area.id}"]`);
        if (dropdown) {
          renderOwnerAutocomplete(dropdown, area.id, area.owner);
        }
      }

      if (field === "email") {
        const contact = findContactByEmail(area.email);
        if (contact?.name) {
          area.owner = contact.name;
          const ownerInput = areasList.querySelector(`[data-area-field="owner"][data-area-id="${area.id}"]`);
          if (ownerInput) {
            ownerInput.value = contact.name;
          }
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

function renderOpenItems() {
  openItemsList.innerHTML = "";

  const activeItems = state.openItems
    .filter((item) => item.status !== "closed")
    .slice()
    .sort((a, b) => Number(b.isSubstantial) - Number(a.isSubstantial) || a.id.localeCompare(b.id));

  activeItems.forEach((item) => {
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
    const applyOpenItemOwnerSelection = async (selectedContact) => {
      const nextOwner = (selectedContact?.name || selectedContact?.email || "").trim();
      if (!nextOwner || nextOwner === lastCommittedOwner) {
        ownerInput.value = nextOwner || ownerInput.value;
        return;
      }

      item.ownerName = nextOwner;
      ownerInput.value = nextOwner;
      node.querySelector(".meta-line").textContent = formatOwnerLine(item) || `Source: ${item.sourceRef || "N/A"}`;

      try {
        await saveStateSilently();
        lastCommittedOwner = item.ownerName || nextOwner;
      } catch (error) {
        item.ownerName = lastCommittedOwner;
        ownerInput.value = lastCommittedOwner;
        node.querySelector(".meta-line").textContent = formatOwnerLine(item) || `Source: ${item.sourceRef || "N/A"}`;
        window.alert(error.message || "No se pudo guardar el open item owner.");
      }
    };

    ownerInput.value = item.ownerName || "";
    ownerInput.addEventListener("input", (event) => {
      item.ownerName = event.currentTarget.value.trim();
      node.querySelector(".meta-line").textContent = formatOwnerLine(item) || `Source: ${item.sourceRef || "N/A"}`;
      renderContactAutocomplete(
        ownerDropdown,
        item.ownerName,
        applyOpenItemOwnerSelection,
        "Sin coincidencias en el directorio de empleados.",
      );
    });
    ownerInput.addEventListener("focus", (event) => {
      renderContactAutocomplete(
        ownerDropdown,
        event.currentTarget.value,
        applyOpenItemOwnerSelection,
        "Sin coincidencias en el directorio de empleados.",
      );
    });
    ownerInput.addEventListener("blur", () => {
      const exactContact = findContactByName(ownerInput.value) || findContactByEmail(ownerInput.value);
      if (exactContact) {
        applyOpenItemOwnerSelection(exactContact);
      } else {
        item.ownerName = lastCommittedOwner;
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

    openItemsList.appendChild(node);
  });
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
  dailyReport.textContent = buildDailyReportText(prerequisite);
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
  renderPrerequisite(payload.prerequisite);
  renderAreas();
  renderOpenItems();
  renderSourceStatus();
  dailyReport.textContent = buildDailyReportText(payload.prerequisite);
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
  renderPrerequisite(payload.prerequisite);
  renderAreas();
  renderOpenItems();
  renderSourceStatus();
  dailyReport.textContent = buildDailyReportText(payload.prerequisite);
  await loadPreSessionDashboard();
}

openItemForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const formData = new FormData(event.currentTarget);
  const impactedAreaIds = Array.from(
    impactedAreasContainer.querySelectorAll("input:checked"),
    (checkbox) => checkbox.value,
  );

  state.openItems.push({
    id: String(formData.get("id")).trim(),
    title: String(formData.get("title")).trim(),
    description: String(formData.get("description")).trim(),
    sourceRef: String(formData.get("sourceRef")).trim(),
    ownerAreaId: String(formData.get("ownerAreaId")).trim(),
    ownerName: "",
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
  });

  event.currentTarget.reset();
  updateNextOpenItemSuggestion(true);
  refreshDerivedViews();
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
  await navigator.clipboard.writeText(dailyReport.textContent);
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
          const suffix = entry.error ? ` | error: ${entry.error}` : "";
          return `- ${entry.ownerEmail}: ${entry.pendingCount} item(s) | ${modeLabel}${suffix}`;
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
    renderPrerequisite(payload.prerequisite);
    renderAreas();
    renderOpenItems();
    renderSourceStatus();
    dailyReport.textContent = buildDailyReportText(payload.prerequisite);
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
    showGoogleSheetsOAuthResultFromQuery();
    const hasSession = await loadAuthConfig();
    renderReferencePanels();
    activateTab("summary");
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

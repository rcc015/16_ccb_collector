const state = {
  areas: [],
  openItems: [],
  lastSavedAt: null,
  source: null,
};

const prerequisiteCard = document.getElementById("prerequisite-card");
const areasList = document.getElementById("areas-list");
const areaForm = document.getElementById("area-form");
const ownerAreaSelect = document.getElementById("owner-area-select");
const impactedAreasContainer = document.getElementById("impacted-areas");
const openItemForm = document.getElementById("open-item-form");
const openItemsList = document.getElementById("open-items-list");
const dailyReport = document.getElementById("daily-report");
const openItemTemplate = document.getElementById("open-item-template");
const csvFileInput = document.getElementById("csv-file-input");
const liveSyncEnabled = document.getElementById("live-sync-enabled");
const liveSyncSourceType = document.getElementById("live-sync-source-type");
const liveSyncSourceUrl = document.getElementById("live-sync-source-url");
const liveSyncIntervalMs = document.getElementById("live-sync-interval-ms");
const liveSyncStatus = document.getElementById("live-sync-status");
const authScreen = document.getElementById("auth-screen");
const appShell = document.getElementById("app-shell");
const authStatus = document.getElementById("auth-status");
const currentUser = document.getElementById("current-user");

let authConfig = null;
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
    const chip = document.createElement("div");
    chip.className = "chip";
    chip.textContent = `${area.name} · ${area.owner}`;
    areasList.appendChild(chip);

    const option = document.createElement("option");
    option.value = area.id;
    option.textContent = area.name;
    ownerAreaSelect.appendChild(option);

    const label = document.createElement("label");
    label.className = "checkbox";
    label.innerHTML = `<input type="checkbox" value="${area.id}" /> ${area.name}`;
    impactedAreasContainer.appendChild(label);
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
  renderAreas();
  renderOpenItems();
  dailyReport.textContent = buildDailyReportText(prerequisite);
}

async function saveState() {
  const response = await fetch(apiUrl("/api/state"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(state),
  });
  const payload = await response.json();
  Object.assign(state, payload.state);
  refreshDerivedViews();
}

async function loadState() {
  const response = await fetch(apiUrl("/api/state"));
  if (response.status === 401) {
    showAuthOnly("Inicia sesion con Google para continuar.");
    return;
  }
  const payload = await response.json();
  Object.assign(state, payload.state);
  renderPrerequisite(payload.prerequisite);
  renderAreas();
  renderOpenItems();
  dailyReport.textContent = buildDailyReportText(payload.prerequisite);
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

async function loadLiveSyncConfig() {
  const response = await fetch(apiUrl("/api/live-sync"));
  if (response.status === 401) {
    return;
  }
  const payload = await response.json();
  renderLiveSyncStatus(payload);
}

async function applyImportedPayload(response) {
  const payload = await response.json();
  Object.assign(state, payload.state);
  renderPrerequisite(payload.prerequisite);
  renderAreas();
  renderOpenItems();
  dailyReport.textContent = buildDailyReportText(payload.prerequisite);
}

areaForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const formData = new FormData(event.currentTarget);
  const name = String(formData.get("name")).trim();
  const owner = String(formData.get("owner")).trim();
  const email = String(formData.get("email")).trim();

  state.areas.push({
    id: slugify(name),
    name,
    owner,
    email,
  });

  event.currentTarget.reset();
  refreshDerivedViews();
});

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
    votes: [],
  });

  event.currentTarget.reset();
  refreshDerivedViews();
});

document.getElementById("save-state").addEventListener("click", saveState);

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
  const response = await fetch(apiUrl("/api/reset"), { method: "POST" });
  await applyImportedPayload(response);
});

document.getElementById("sync-google-sheet").addEventListener("click", async () => {
  const response = await fetch(apiUrl("/api/import/google-sheet-snapshot"), { method: "POST" });
  await applyImportedPayload(response);
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

document.getElementById("save-live-sync").addEventListener("click", async () => {
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
  const payload = await response.json();
  renderLiveSyncStatus(payload);
});

document.getElementById("run-live-sync").addEventListener("click", async () => {
  const response = await fetch(apiUrl("/api/live-sync/run"), { method: "POST" });
  if (response.status === 401) {
    showAuthOnly("Tu sesion expiro. Inicia sesion de nuevo.");
    return;
  }
  const payload = await response.json();
  renderLiveSyncStatus(payload);
  Object.assign(state, payload.state);
  renderPrerequisite(payload.prerequisite);
  renderAreas();
  renderOpenItems();
  dailyReport.textContent = buildDailyReportText(payload.prerequisite);
});

document.getElementById("logout-button").addEventListener("click", async () => {
  await fetch(apiUrl("/api/auth/logout"), { method: "POST" });
  showAuthOnly(`Acceso limitado a @${authConfig?.allowedDomain || "conceivable.life"}`);
  currentUser.textContent = "";
});

(async () => {
  const hasSession = await loadAuthConfig();
  if (hasSession) {
    await loadState();
    await loadLiveSyncConfig();
  }
})();

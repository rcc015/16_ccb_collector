# CCB Collector

MVP web para validar si un prerequisito se cumple cuando existen cambios sustanciales en un `open item list`, identificar areas impactadas, registrar votaciones y preparar el resumen diario para la sesion de `CCB`.

## Que resuelve

- Mantiene una lista de `open items` con owner por area.
- Marca si un item representa un `substantial change`.
- Relaciona cada item con las areas impactadas.
- Registra votos por area: `approve`, `reject`, `needs-info`.
- Calcula el estado del prerequisito.
- Genera un resumen diario con nuevas votaciones y lista sugerida para sesion de `CCB`.

## Como usar

1. Ejecuta `npm start`.
2. Abre `http://localhost:3000`.
3. Captura areas, owners, open items y votos.
4. Usa `Guardar cambios` para persistir el estado en `data/state.json`.
5. Usa `Sincronizar Sheet` para importar el snapshot local del Google Sheet real.
6. O exporta `Open Item List` como `CSV` y usa `Importar CSV` para refrescar el snapshot local sin editar archivos.
7. O configura `Live Sync` con una URL CSV/JSON para sincronizacion automatica y deteccion de cambios.

## Estructura de datos esperada

Cada `open item` incluye:

- `id`
- `title`
- `description`
- `sourceRef`
- `ownerAreaId`
- `impactedAreaIds`
- `isSubstantial`
- `status`
- `votes`

## Recomendacion de siguiente fase

Para convertir este MVP en una app operativa en tu proceso real, te recomiendo:

1. Definir el formato oficial de entrada del `open item list` que me vas a compartir, idealmente `CSV` o `Excel`.
2. Reemplazar el snapshot local por una sincronizacion real con Google Sheets mediante Apps Script o credenciales de servicio.
3. Agregar autenticacion simple por owner.
4. Enviar el reporte diario por correo o Teams desde un backend real.
5. Guardar historial de sesiones de `CCB` y acuerdos por item.

## Archivos de integracion

- `data/google-open-items-snapshot.json`: snapshot transformado desde el Google Sheet real.
- `data/area-directory.json`: directorio de areas y owners; aqui debes completar los owners reales.

## Flujo operativo recomendado

1. En Google Sheets abre la pestaña `Open Item List`.
2. Exportala a `CSV`.
3. En la app usa `Importar CSV`.
4. Revisa areas impactadas, votos pendientes y reporte diario.
5. Guarda cambios para persistir votos y decisiones locales.

## Live Sync real

La app ya puede hacer polling automatico a una URL externa y sincronizar si detecta cambios.

Opciones recomendadas:

1. `Apps Script` que exponga JSON o CSV del sheet privado.
2. `Publish to web` del tab como CSV si el archivo puede ser publico.

La deteccion se hace comparando el contenido importado contra el ultimo snapshot.
Si cambia, la app actualiza `data/google-open-items-snapshot.json` y `data/state.json`.

## Apps Script sugerido

Crea un proyecto de Apps Script ligado al spreadsheet y publica un Web App:

```javascript
function doGet() {
  const ss = SpreadsheetApp.openById('1G6YNnnIrqEH_oIgq95bXmrER2lUjPYtkCeV5zwaXRms');
  const sheet = ss.getSheetByName('Open Item List');
  const values = sheet.getDataRange().getDisplayValues();
  const headers = values[0];
  const rows = values.slice(1).map((row) => ({
    id: row[0],
    description: row[1],
    owner: row[2],
    dateCreated: row[3],
    dueDate: row[4],
    status: row[5],
    comments: row[6],
    Software: row[7],
    Product: row[8],
    Quality: row[9],
    Machine: row[10],
    Testing: row[11],
    Infra: row[12],
    Optics: row[13],
    Data: row[14],
    Research: row[15],
    Exploration: row[16],
    Mecha: row[17],
    minutesRelated: row[18],
    gitRepository: row[19],
    ccbScore: row[20],
    ccbStatus: row[21],
    jiraTicketsRelated: row[22],
  }));

  return ContentService
    .createTextOutput(JSON.stringify({
      spreadsheetId: ss.getId(),
      spreadsheetTitle: ss.getName(),
      sheetName: sheet.getName(),
      headers,
      rows,
    }))
    .setMimeType(ContentService.MimeType.JSON);
}
```

Despues pegas la URL del Web App en `Live Sync`, eliges `JSON URL` y activas la sincronizacion automatica.

## Google Login para `@conceivable.life`

La app ya trae login Google y proteccion de APIs.

Configura:

1. Crea un OAuth Client ID en Google Cloud para web.
2. Autoriza el dominio/DNS donde expondras la app.
3. Copia `.env.example` a `.env`.
4. Completa `GOOGLE_CLIENT_ID`, `APP_BASE_URL` y `SESSION_SECRET`.
5. Mantén `ALLOWED_GOOGLE_DOMAIN=conceivable.life`.

La validacion actual del backend comprueba:

- `aud` igual al `GOOGLE_CLIENT_ID`
- `iss` de Google
- `email_verified`
- `hd = conceivable.life`

## Exponer la app por DNS

Lo correcto es publicar la app detras de un reverse proxy con TLS.

Ejemplo:

- DNS: `ccb.conceivable.life`
- Proxy: Nginx o Caddy
- App Node local escuchando en `localhost:3000`
- Proxy publico reenviando a `localhost:3000`

En Google Cloud, agrega `https://ccb.conceivable.life` como `Authorized JavaScript origin`.

## Exponer la app bajo una subruta

Si ya tienes un dominio como `https://repo-validation.conceivable.life`, no necesitas otro DNS para publicar esta app en `https://repo-validation.conceivable.life/ccb/`.

Configura el servidor Node con:

- `BASE_PATH=/ccb`
- `APP_BASE_URL=https://repo-validation.conceivable.life/ccb/`

Ejemplo con Nginx:

```nginx
location /ccb/ {
  proxy_pass http://127.0.0.1:3000/ccb/;
  proxy_http_version 1.1;
  proxy_set_header Host $host;
  proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
  proxy_set_header X-Forwarded-Proto $scheme;
}
```

Despues entra por:

- `https://repo-validation.conceivable.life/ccb/`

Importante:

- Usa la URL con slash final: `/ccb/`
- En Google Cloud OAuth agrega como origin `https://repo-validation.conceivable.life`
- Si tambien defines Authorized Redirect URIs, usa la ruta real que aplique a tu flujo

## Regla actual de cambio sustancial

El importador marca un item como `substantial change` cuando el row del sheet ya tiene `CCB Score` numerico o `CCB Status` distinto de `No Localized`.
Es una heuristica inicial y conviene ajustarla contigo cuando definamos la regla formal.

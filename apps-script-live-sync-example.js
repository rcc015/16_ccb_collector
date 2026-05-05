function doGet() {
  const ss = SpreadsheetApp.openById("1G6YNnnIrqEH_oIgq95bXmrER2lUjPYtkCeV5zwaXRms");
  const sheet = ss.getSheetByName("Open Item List");
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

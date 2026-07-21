// Google Apps Script Web App
// Supports generic CSV payload rows.

const DEFAULT_SHEET_NAME = "blog.aspose.com";
const SHARED_SECRET_PROPERTY = "SHARED_SECRET";

function doPost(e) {
  try {
    if (!e || !e.postData || !e.postData.contents) {
      return createResponse(false, "Missing request body");
    }

    const data = JSON.parse(e.postData.contents);
    const configuredSecret = getConfiguredSharedSecret();
    if (!configuredSecret) {
      return createResponse(false, "Server misconfigured: missing shared secret");
    }

    const providedSecret = extractProvidedSecret(data, e);
    if (!providedSecret || providedSecret !== configuredSecret) {
      return createResponse(false, "Unauthorized");
    }

    if (data.action === "import_data") {
      return handleImportData(data);
    }
    return createResponse(false, "Invalid action. Use: import_data");
  } catch (error) {
    console.error("Error:", error);
    return createResponse(false, "Error: " + error.message);
  }
}

function doGet() {
  return createResponse(true, {
    message: "Search Console Data Receiver",
    endpoint: "POST with action: import_data",
    parameters: {
      spreadsheetId: "Google Sheet ID",
      sheetName: "Target sheet name (optional)",
      rows: "Array of row objects",
      clearExisting: "true/false (optional, default: true)"
    },
    expected_row_fields: "Any CSV columns present in the uploaded rows"
  });
}

function extractDomainFromData(rows) {
  if (rows && rows.length > 0 && rows[0].page) {
    const url = rows[0].page;
    try {
      return new URL(url).hostname;
    } catch (e) {
      const domainMatch = url.match(/https?:\/\/([^\/]+)/);
      if (domainMatch && domainMatch[1]) return domainMatch[1];
    }
  }
  return "blog.conholdate.com";
}

function resolveSheetName(data, rows) {
  const explicitSheetName = data && typeof data.sheetName === "string" ? data.sheetName.trim() : "";
  if (explicitSheetName) {
    return explicitSheetName;
  }

  const inferredSheetName = extractDomainFromData(rows);
  if (inferredSheetName) {
    return inferredSheetName;
  }

  return DEFAULT_SHEET_NAME;
}

function getConfiguredSharedSecret() {
  const props = PropertiesService.getScriptProperties();
  return String(props.getProperty(SHARED_SECRET_PROPERTY) || "").trim();
}

function extractProvidedSecret(data, e) {
  const bodySecret = data && typeof data === "object"
    ? String(data.shared_secret || data.token || "").trim()
    : "";
  const querySecret = e && e.parameter
    ? String(e.parameter.shared_secret || e.parameter.token || "").trim()
    : "";
  return bodySecret || querySecret;
}

function trimTrailingBlanks(values) {
  const out = values.slice();
  while (out.length > 0 && String(out[out.length - 1] || "").trim() === "") {
    out.pop();
  }
  return out;
}

function getIncomingHeaders(rows) {
  if (!rows || rows.length === 0 || typeof rows[0] !== "object" || rows[0] === null) {
    return [];
  }
  return Object.keys(rows[0]);
}

function normalizeCellValue(value) {
  if (value === undefined || value === null) return "";
  if (value instanceof Date) return value;
  if (typeof value === "number" && !isFinite(value)) return "";
  return value;
}

function ensureSheetWidth(sheet, width) {
  const current = sheet.getMaxColumns();
  if (current < width) {
    sheet.insertColumnsAfter(current, width - current);
  }
}

function writeHeaderRow(sheet, headers) {
  ensureSheetWidth(sheet, headers.length);
  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  const headerRange = sheet.getRange(1, 1, 1, headers.length);
  headerRange.setFontWeight("bold");
  headerRange.setBackground("#1a73e8");
  headerRange.setFontColor("#ffffff");
  headerRange.setHorizontalAlignment("center");
  sheet.setFrozenRows(1);
}

function resolveHeaders(sheet, rows) {
  const existing = trimTrailingBlanks(
    sheet.getLastColumn() > 0
      ? sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0]
      : []
  );
  const incoming = getIncomingHeaders(rows);

  if (existing.length === 0) {
    return incoming;
  }

  const headers = existing.slice();
  for (const header of incoming) {
    if (!headers.includes(header)) {
      headers.push(header);
    }
  }
  return headers;
}

function buildSheetData(rows, headers) {
  return rows.map(row => headers.map(header => normalizeCellValue(row[header])));
}

function toBoolean(value, defaultValue) {
  if (value === undefined || value === null) return defaultValue;
  if (typeof value === "boolean") return value;
  if (typeof value === "string") return value.toLowerCase() === "true";
  return Boolean(value);
}

function handleImportData(data) {
  try {
    const { spreadsheetId, rows } = data;

    if (!spreadsheetId) return createResponse(false, "Missing spreadsheetId");
    if (!rows || !Array.isArray(rows)) return createResponse(false, "Missing or invalid rows data");
    if (rows.length === 0) {
      return createResponse(true, {
        message: "No rows to import",
        rows_imported: 0,
        total_rows_in_sheet: 0,
        sheet_name: resolveSheetName(data, rows),
        spreadsheet_url: SpreadsheetApp.openById(spreadsheetId).getUrl(),
        clear_existing: true
      });
    }

    const sheetName = resolveSheetName(data, rows);
    const ss = SpreadsheetApp.openById(spreadsheetId);
    let sheet = ss.getSheetByName(sheetName);
    if (!sheet) {
      sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);
    }

    const headers = resolveHeaders(sheet, rows);

    writeHeaderRow(sheet, headers);

    if (sheet.getLastRow() > 1) {
      ensureSheetWidth(sheet, headers.length);
      sheet.getRange(2, 1, sheet.getLastRow() - 1, headers.length).clearContent();
    }

    const sheetData = buildSheetData(rows, headers);
    if (sheetData.length > 0) {
      sheet.getRange(2, 1, sheetData.length, headers.length).setValues(sheetData);
    }

    const totalRowsInSheet = Math.max(sheet.getLastRow() - 1, 0);
    return createResponse(true, {
      message: `Imported ${sheetData.length} rows to ${sheetName}`,
      rows_imported: sheetData.length,
      total_rows_in_sheet: totalRowsInSheet,
      sheet_name: sheetName,
      spreadsheet_url: ss.getUrl(),
      clear_existing: true
    });
  } catch (error) {
    console.error("Import error:", error);
    return createResponse(false, "Import failed: " + error.message);
  }
}

function createResponse(success, data) {
  const response = { success: success };
  if (success) {
    if (typeof data === "string") response.message = data;
    else Object.assign(response, data);
  } else {
    response.error = data;
  }

  return ContentService
    .createTextOutput(JSON.stringify(response))
    .setMimeType(ContentService.MimeType.JSON);
}

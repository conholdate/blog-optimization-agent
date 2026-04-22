// Google Apps Script Web App
// Supports payload rows with:
// page, clicks, impressions, ctr, position, days_since_published (or "Days Since Published")
// Enforces CTR range 1%..4% and sorts by Days Since Published descending.

function doPost(e) {
  try {
    if (!e || !e.postData || !e.postData.contents) {
      return createResponse(false, "Missing request body");
    }

    const data = JSON.parse(e.postData.contents);
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
      rows: "Array of row objects",
      clearExisting: "true/false (optional, default: true)"
    },
    expected_row_fields: [
      "page",
      "clicks",
      "impressions",
      "ctr",
      "position",
      "days_since_published (or 'Days Since Published')"
    ],
    ctr_range: "0.01 <= ctr <= 0.04"
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

function toBoolean(value, defaultValue) {
  if (value === undefined || value === null) return defaultValue;
  if (typeof value === "boolean") return value;
  if (typeof value === "string") return value.toLowerCase() === "true";
  return Boolean(value);
}

function handleImportData(data) {
  try {
    const { spreadsheetId, rows } = data;
    const clearExisting = toBoolean(data.clearExisting, true);

    if (!spreadsheetId) return createResponse(false, "Missing spreadsheetId");
    if (!rows || !Array.isArray(rows)) return createResponse(false, "Missing or invalid rows data");

    const domain = extractDomainFromData(rows);
    const ss = SpreadsheetApp.openById(spreadsheetId);

    let sheet = ss.getSheetByName(domain);
    if (!sheet) {
      sheet = ss.insertSheet(domain);
      setupSheet(sheet);
    } else {
      ensureHeaders(sheet);
    }

    // Clear existing data rows (A:F) if requested.
    if (clearExisting && sheet.getLastRow() > 1) {
      sheet.getRange(2, 1, sheet.getLastRow() - 1, 6).clearContent();
    }

    const startRow = clearExisting ? 2 : (sheet.getLastRow() > 1 ? sheet.getLastRow() + 1 : 2);

    // Build rows + enforce CTR range: 1% <= CTR <= 4%
    const sheetData = [];
    for (const row of rows) {
      const ctr = Number(row.ctr || 0);
      if (isNaN(ctr) || ctr < 0.01 || ctr > 0.04) {
        continue;
      }

      const daysValue =
        row.days_since_published !== undefined && row.days_since_published !== null
          ? row.days_since_published
          : row["Days Since Published"];

      sheetData.push([
        String(row.page || ""),
        Number(row.clicks || 0),
        Number(row.impressions || 0),
        ctr,
        Number(row.position || 0),
        daysValue === undefined || daysValue === null || daysValue === "" ? "" : Number(daysValue)
      ]);
    }

    if (sheetData.length > 0) {
      // Write A:F
      sheet.getRange(startRow, 1, sheetData.length, 6).setValues(sheetData);

      // Format numeric columns
      sheet.getRange(startRow, 2, sheetData.length, 1).setNumberFormat("#,##0");
      sheet.getRange(startRow, 3, sheetData.length, 1).setNumberFormat("#,##0");
      sheet.getRange(startRow, 4, sheetData.length, 1).setNumberFormat("0.00%");
      sheet.getRange(startRow, 5, sheetData.length, 1).setNumberFormat("0.00");
      sheet.getRange(startRow, 6, sheetData.length, 1).setNumberFormat("0");

      // Sort by Days Since Published DESC (largest first)
      if (clearExisting) {
        const totalRows = sheet.getLastRow() - 1;
        if (totalRows > 1) {
          sheet.getRange(2, 1, totalRows, 6).sort({ column: 6, ascending: false });
        }
      }
    }

    const totalRowsInSheet = Math.max(sheet.getLastRow() - 1, 0);
    return createResponse(true, {
      message: `Imported ${sheetData.length} rows to ${domain}`,
      rows_imported: sheetData.length,
      total_rows_in_sheet: totalRowsInSheet,
      sheet_name: domain,
      spreadsheet_url: ss.getUrl(),
      clear_existing: clearExisting
    });
  } catch (error) {
    console.error("Import error:", error);
    return createResponse(false, "Import failed: " + error.message);
  }
}

function setupSheet(sheet) {
  sheet.clear();
  const headers = [[
    "Page",
    "Clicks",
    "Impressions",
    "CTR",
    "Position",
    "Days Since Published"
  ]];
  sheet.getRange(1, 1, 1, 6).setValues(headers);

  const headerRange = sheet.getRange(1, 1, 1, 6);
  headerRange.setFontWeight("bold");
  headerRange.setBackground("#1a73e8");
  headerRange.setFontColor("#ffffff");
  headerRange.setHorizontalAlignment("center");

  sheet.setColumnWidth(1, 500);
  sheet.setColumnWidth(2, 90);
  sheet.setColumnWidth(3, 110);
  sheet.setColumnWidth(4, 80);
  sheet.setColumnWidth(5, 90);
  sheet.setColumnWidth(6, 150);
  sheet.setFrozenRows(1);
}

function ensureHeaders(sheet) {
  const expected = [
    "Page",
    "Clicks",
    "Impressions",
    "CTR",
    "Position",
    "Days Since Published"
  ];
  const current = sheet.getRange(1, 1, 1, 6).getValues()[0];
  const mismatch = expected.some((h, i) => String(current[i] || "").trim() !== h);
  if (mismatch) setupSheet(sheet);
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

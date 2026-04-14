/**
 * One-time Gmail → Drive backfill for Dazpak quote emails.
 *
 * Deploy in Google Apps Script (script.google.com):
 *   1. Create a new project
 *   2. Paste this file
 *   3. Run backfillDazpakQuotes()
 *   4. Authorize when prompted (Gmail + Drive scopes)
 *
 * What it does:
 *   - Searches Gmail for Dazpak emails since 12/10/2024
 *   - Filters to quote-relevant messages (pricing, quotation, FL-DL, CQ)
 *   - Saves PDF/Excel attachments to the Drive Dazpak folder
 *   - Saves email body HTML for emails with inline quote content
 *   - Skips files that already exist in Drive (dedup by filename)
 *   - Logs everything to a spreadsheet
 */

// ── CONFIG ──────────────────────────────────────────────────────
var DAZPAK_FOLDER_ID = '1Q50UVUkpqov3PdYTiTpcZFCriGQfq3P1';
var SEARCH_QUERY     = 'from:dazpak.com after:2024/12/10';
var LOG_SHEET_NAME   = 'Dazpak Backfill Log';
// ────────────────────────────────────────────────────────────────

function backfillDazpakQuotes() {
  var folder = DriveApp.getFolderById(DAZPAK_FOLDER_ID);

  // Build set of existing filenames in Drive for dedup
  var existingFiles = {};
  var files = folder.getFiles();
  while (files.hasNext()) {
    var f = files.next();
    existingFiles[f.getName()] = true;
  }
  Logger.log('Existing files in Drive: ' + Object.keys(existingFiles).length);

  // Search Gmail
  var threads = GmailApp.search(SEARCH_QUERY);
  Logger.log('Gmail threads found: ' + threads.length);

  var saved = 0;
  var skipped = 0;
  var skippedNonQuote = 0;
  var logRows = [['Timestamp', 'Action', 'MessageId', 'Subject', 'Date', 'Filename', 'Type', 'Size']];

  for (var t = 0; t < threads.length; t++) {
    var messages = threads[t].getMessages();
    for (var m = 0; m < messages.length; m++) {
      var msg = messages[m];
      var from = msg.getFrom();

      // Only process emails FROM dazpak
      if (from.toLowerCase().indexOf('dazpak.com') === -1) continue;

      var subject = msg.getSubject();
      var body = msg.getBody();
      var plainBody = msg.getPlainBody();
      var date = msg.getDate();
      var msgId = msg.getId();

      // Filter: only quote-relevant emails
      if (!isQuoteEmail_(subject, plainBody)) {
        skippedNonQuote++;
        logRows.push([new Date(), 'SKIP_NOT_QUOTE', msgId, subject, date, '', '', '']);
        continue;
      }

      // Generate filename matching pipeline convention
      var dateStr = Utilities.formatDate(date, 'America/Denver', 'yyyy-MM-dd_HHmmss');
      var cleanSubject = sanitizeFilename_(subject);

      // Dazpak quotes are primarily PDF attachments, but also save body HTML
      var bodyFilename = dateStr + '_Dazpak_' + cleanSubject + '_body.html';
      if (!existingFiles[bodyFilename]) {
        var htmlBlob = Utilities.newBlob(body, 'text/html', bodyFilename);
        folder.createFile(htmlBlob);
        existingFiles[bodyFilename] = true;
        saved++;
        logRows.push([new Date(), 'SAVED_BODY', msgId, subject, date, bodyFilename, 'HTML', body.length]);
        Logger.log('Saved: ' + bodyFilename);
      } else {
        skipped++;
        logRows.push([new Date(), 'SKIP_EXISTS', msgId, subject, date, bodyFilename, 'HTML', '']);
      }

      // Save attachments (PDFs, Excel files)
      var attachments = msg.getAttachments();
      for (var a = 0; a < attachments.length; a++) {
        var att = attachments[a];
        var attName = att.getName();
        var attType = att.getContentType();

        if (!isQuoteAttachment_(attName, attType)) continue;

        var attFilename = dateStr + '_Dazpak_' + sanitizeFilename_(attName);

        if (!existingFiles[attFilename]) {
          folder.createFile(att.copyBlob().setName(attFilename));
          existingFiles[attFilename] = true;
          saved++;
          logRows.push([new Date(), 'SAVED_ATTACHMENT', msgId, subject, date, attFilename, attType, att.getSize()]);
          Logger.log('Saved attachment: ' + attFilename);
        } else {
          skipped++;
          logRows.push([new Date(), 'SKIP_ATT_EXISTS', msgId, subject, date, attFilename, attType, '']);
        }
      }
    }
  }

  writeLog_(logRows);

  var summary = 'Backfill complete: ' + saved + ' files saved, ' +
                skipped + ' duplicates skipped, ' +
                skippedNonQuote + ' non-quote emails skipped';
  Logger.log(summary);
  return summary;
}


function isQuoteEmail_(subject, plainBody) {
  var combined = (subject + ' ' + plainBody).toLowerCase();
  var hasQuoteId = /fl-dl-\d+|cq-\d+|fl-cq-\d+|quote\s*request/i.test(combined);
  var hasPricing = /quotation|quote.*request|pricing|price.*each|impressions/i.test(combined);
  var hasQuoteContent = /find\s*(below|attached)\s*(the\s*)?(quotation|pricing|quote)/i.test(combined);
  var hasDazpakQuote = /dazpak.*quote|quote.*dazpak|calyx.*quote/i.test(combined);
  return hasQuoteId || hasPricing || hasQuoteContent || hasDazpakQuote;
}


function isQuoteAttachment_(name, contentType) {
  var lowerName = name.toLowerCase();
  if (/\.(png|jpg|jpeg|gif|bmp|ico|svg)$/i.test(name)) return false;
  if (lowerName.indexOf('logo') !== -1) return false;
  if (lowerName.indexOf('signature') !== -1) return false;
  if (/\.(pdf|xlsx?|csv)$/i.test(name)) return true;
  if (contentType && contentType.indexOf('pdf') !== -1) return true;
  if (contentType && contentType.indexOf('spreadsheet') !== -1) return true;
  if (contentType && contentType.indexOf('excel') !== -1) return true;
  return false;
}


function sanitizeFilename_(name) {
  return name
    .replace(/[\/\\:*?"<>|]/g, '_')
    .replace(/\s+/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '')
    .substring(0, 150);
}


function writeLog_(rows) {
  var ss = SpreadsheetApp.create(LOG_SHEET_NAME + ' ' + Utilities.formatDate(new Date(), 'America/Denver', 'yyyy-MM-dd'));
  var sheet = ss.getActiveSheet();
  sheet.getRange(1, 1, rows.length, rows[0].length).setValues(rows);
  sheet.setFrozenRows(1);
  Logger.log('Log spreadsheet: ' + ss.getUrl());
}

/**
 * One-time Gmail → Drive backfill for TedPack quote emails.
 *
 * Deploy in Google Apps Script (script.google.com):
 *   1. Create a new project
 *   2. Paste this file
 *   3. Run backfillTedpackQuotes()
 *   4. Authorize when prompted (Gmail + Drive scopes)
 *
 * What it does:
 *   - Searches Gmail for TedPack emails since 12/10/2024
 *   - Filters to quote-relevant messages (pricing, quotation, FL-DL, CQ)
 *   - Saves email body HTML to the Drive Tedpack folder
 *   - Saves PDF/Excel attachments to the same folder
 *   - Skips files that already exist in Drive (dedup by filename)
 *   - Logs everything to a spreadsheet
 */

// ── CONFIG ──────────────────────────────────────────────────────
var TEDPACK_FOLDER_ID = '1JHIj9HM_r2SKZSz7Xm_xf9ZIGiFJX5xd';
var SEARCH_QUERY      = 'from:tedpack.com after:2024/12/10';
var LOG_SHEET_NAME    = 'TedPack Backfill Log';
// ────────────────────────────────────────────────────────────────

function backfillTedpackQuotes() {
  var folder = DriveApp.getFolderById(TEDPACK_FOLDER_ID);

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

      // Only process emails FROM tedpack
      if (from.toLowerCase().indexOf('tedpack.com') === -1) continue;

      var subject = msg.getSubject();
      var body = msg.getBody();         // HTML body
      var plainBody = msg.getPlainBody();
      var date = msg.getDate();
      var msgId = msg.getId();

      // Filter: only quote-relevant emails
      if (!isQuoteEmail_(subject, plainBody)) {
        skippedNonQuote++;
        logRows.push([new Date(), 'SKIP_NOT_QUOTE', msgId, subject, date, '', '', '']);
        continue;
      }

      // Generate filename matching Apps Script convention
      var dateStr = Utilities.formatDate(date, 'America/Denver', 'yyyy-MM-dd_HHmmss');
      var cleanSubject = sanitizeFilename_(subject);
      var bodyFilename = dateStr + '_Tedpack_' + cleanSubject + '_body.html';

      // Save HTML body
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

      // Save attachments (PDFs, Excel files with quote content)
      var attachments = msg.getAttachments();
      for (var a = 0; a < attachments.length; a++) {
        var att = attachments[a];
        var attName = att.getName();
        var attType = att.getContentType();

        // Only save quote-relevant attachments (PDFs, Excel)
        if (!isQuoteAttachment_(attName, attType)) continue;

        var attFilename = dateStr + '_Tedpack_' + sanitizeFilename_(attName);

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

  // Write log to spreadsheet
  writeLog_(logRows);

  var summary = 'Backfill complete: ' + saved + ' files saved, ' +
                skipped + ' duplicates skipped, ' +
                skippedNonQuote + ' non-quote emails skipped';
  Logger.log(summary);
  return summary;
}


/**
 * Determine if an email is quote-relevant based on subject and body content.
 */
function isQuoteEmail_(subject, plainBody) {
  var combined = (subject + ' ' + plainBody).toLowerCase();

  // Must contain at least one quote identifier or pricing indicator
  var hasQuoteId = /fl-dl-\d+|cq-\d+|fl-cq-\d+|quote\s*request/i.test(combined);
  var hasPricing = /quotation|factory\s*price|delivery\s*air|delivery\s*ocean|price.*pcs|\$[\d.]+\/pcs/i.test(combined);
  var hasQuoteContent = /find\s*(below|attached)\s*(the\s*)?(quotation|pricing|quote)/i.test(combined);

  return hasQuoteId || hasPricing || hasQuoteContent;
}


/**
 * Determine if an attachment is a quote document worth saving.
 */
function isQuoteAttachment_(name, contentType) {
  var lowerName = name.toLowerCase();

  // Skip images, signatures, logos
  if (/\.(png|jpg|jpeg|gif|bmp|ico|svg)$/i.test(name)) return false;
  if (lowerName.indexOf('logo') !== -1) return false;
  if (lowerName.indexOf('signature') !== -1) return false;

  // Accept PDFs and Excel files
  if (/\.(pdf|xlsx?|csv)$/i.test(name)) return true;
  if (contentType && contentType.indexOf('pdf') !== -1) return true;
  if (contentType && contentType.indexOf('spreadsheet') !== -1) return true;
  if (contentType && contentType.indexOf('excel') !== -1) return true;

  return false;
}


/**
 * Sanitize a string for use as a filename.
 */
function sanitizeFilename_(name) {
  return name
    .replace(/[\/\\:*?"<>|]/g, '_')   // illegal chars
    .replace(/\s+/g, '_')              // whitespace
    .replace(/_+/g, '_')               // collapse underscores
    .replace(/^_|_$/g, '')             // trim underscores
    .substring(0, 150);                // max length
}


/**
 * Write backfill log to a new spreadsheet.
 */
function writeLog_(rows) {
  var ss = SpreadsheetApp.create(LOG_SHEET_NAME + ' ' + Utilities.formatDate(new Date(), 'America/Denver', 'yyyy-MM-dd'));
  var sheet = ss.getActiveSheet();
  sheet.getRange(1, 1, rows.length, rows[0].length).setValues(rows);
  sheet.setFrozenRows(1);
  Logger.log('Log spreadsheet: ' + ss.getUrl());
}

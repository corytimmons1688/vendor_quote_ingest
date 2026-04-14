/**
 * One-time Gmail → Drive backfill for Ross quote emails.
 */

var ROSS_FOLDER_ID = '1tvvNQuXIrppg1h_eXt6SnCvFYWxBQGPe';
var SEARCH_QUERY   = 'from:rossprint.com after:2024/12/10';
var LOG_SHEET_NAME = 'Ross Backfill Log';

function backfillRossQuotes() {
  var folder = DriveApp.getFolderById(ROSS_FOLDER_ID);

  var existingFiles = {};
  var files = folder.getFiles();
  while (files.hasNext()) {
    var f = files.next();
    existingFiles[f.getName()] = true;
  }
  Logger.log('Existing files in Drive: ' + Object.keys(existingFiles).length);

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

      if (from.toLowerCase().indexOf('rossprint.com') === -1) continue;

      var subject = msg.getSubject();
      var body = msg.getBody();
      var plainBody = msg.getPlainBody();
      var date = msg.getDate();
      var msgId = msg.getId();

      if (!isQuoteEmail_(subject, plainBody)) {
        skippedNonQuote++;
        logRows.push([new Date(), 'SKIP_NOT_QUOTE', msgId, subject, date, '', '', '']);
        continue;
      }

      var dateStr = Utilities.formatDate(date, 'America/Denver', 'yyyy-MM-dd_HHmmss');
      var cleanSubject = sanitizeFilename_(subject);
      var bodyFilename = dateStr + '_Ross_' + cleanSubject + '_body.html';

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

      var attachments = msg.getAttachments();
      for (var a = 0; a < attachments.length; a++) {
        var att = attachments[a];
        var attName = att.getName();
        var attType = att.getContentType();

        if (!isQuoteAttachment_(attName, attType)) continue;

        var attFilename = dateStr + '_Ross_' + sanitizeFilename_(attName);

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
  var hasQuoteId = /fl-dl-\d+|cq-\d+|fl-cq-\d+|quote\s*request|estimate/i.test(combined);
  var hasPricing = /quotation|price\s*each|grand\s*total|quantity.*price/i.test(combined);
  var hasQuoteContent = /here\s*you\s*go|find\s*(below|attached)/i.test(combined);
  return hasQuoteId || hasPricing || hasQuoteContent;
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

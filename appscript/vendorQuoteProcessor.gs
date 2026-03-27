// ============================================================
// vendorQuoteProcessor.gs
// Runs daily via Apps Script trigger (6:00 AM MST)
// Extracts vendor quote emails and saves to Drive by vendor
// ============================================================

// --- Configuration -----------------------------------------------------------
const CONFIG = {
  vendors: {
    tedpack: {
      name: 'Tedpack',
      searchQuery: 'from:@tedpack.com (subject:"Quote Request" OR subject:"FL-")',
      folderId: null,
      quoteInBody: true,
    },
    ross: {
      name: 'Ross',
      searchQuery: 'from:@rossprint.com (subject:"Quote Request" OR subject:"FL-")',
      folderId: null,
      quoteInBody: false,
      domains: ['rossprint.com'],
    },
    dazpak: {
      name: 'Dazpak',
      searchQuery: 'from:@dazpak.com (subject:"Quote Request" OR subject:"FL-")',
      folderId: null,
      quoteInBody: false,
      domains: ['dazpak.com'],
      repliesInNewThread: true,
    },
  },
  rootFolderId: '1ZMKXumVzO_CCl4pNY0iWPximitOEC9Vr',
  logFolderName: '_logs',
  processedLabel: 'VendorQuotes/Processed',
  lookbackDays: 7,           // Daily lookback window
  batchSize: 25,             // Max threads per vendor per run
  maxRunTimeMs: 5 * 60 * 1000, // 5 minutes — stop before 6-min Apps Script limit
  fileLabels: {
    sensitivityLevel: 'Internal',
    documentType: 'Vendor Record',
    functionalDomain: 'Supply Chain',
    lifecycleStatus: 'Active',
    sourceOfAuthority: 'Third-Party',
    operationalCriticality: 'Business-Critical',
    aiHandlingRules: ['Read Only', 'Summarization Allowed', 'No External Use'],
    projectInitiative: 'Vendor_Estimate',
  },
};

// --- Timing Guard ------------------------------------------------------------
const RUN_START = Date.now();

function hasTimeRemaining_() {
  return (Date.now() - RUN_START) < CONFIG.maxRunTimeMs;
}

// --- Deduplication -----------------------------------------------------------
// Cache existing filenames per vendor folder to prevent re-saving the same file.
// Uses Gmail message ID tracking via ScriptProperties for thread-level dedup.
const PROCESSED_MSG_PROP_KEY = 'processedMessageIds';

function getProcessedMessageIds_() {
  const raw = PropertiesService.getScriptProperties().getProperty(PROCESSED_MSG_PROP_KEY);
  return raw ? JSON.parse(raw) : {};
}

function markMessageProcessed_(messageId) {
  const ids = getProcessedMessageIds_();
  ids[messageId] = Date.now();
  // Prune entries older than 30 days to prevent unbounded growth
  const thirtyDaysAgo = Date.now() - (30 * 24 * 60 * 60 * 1000);
  for (const [key, ts] of Object.entries(ids)) {
    if (ts < thirtyDaysAgo) delete ids[key];
  }
  PropertiesService.getScriptProperties().setProperty(PROCESSED_MSG_PROP_KEY, JSON.stringify(ids));
}

function isMessageAlreadyProcessed_(messageId) {
  const ids = getProcessedMessageIds_();
  return !!ids[messageId];
}

// Secondary dedup: check if a filename already exists in the vendor folder
function fileExistsInFolder_(folder, filename) {
  const existing = folder.getFilesByName(filename);
  return existing.hasNext();
}

// --- Entry Point -------------------------------------------------------------
function processVendorQuotes() {
  const lock = LockService.getScriptLock();
  try {
    lock.waitLock(30000);
  } catch (e) {
    Logger.log('Could not obtain lock. Another instance may be running.');
    return;
  }

  try {
    const rootFolder = DriveApp.getFolderById(CONFIG.rootFolderId);
    const logFolder = getOrCreateFolder_(rootFolder, CONFIG.logFolderName);
    const label = getOrCreateLabel_(CONFIG.processedLabel);
    const today = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd');
    const logs = [];
    let stoppedEarly = false;

    for (const [vendorKey, vendor] of Object.entries(CONFIG.vendors)) {
      if (!hasTimeRemaining_()) {
        Logger.log(`Time limit approaching — stopping before ${vendor.name}`);
        stoppedEarly = true;
        break;
      }

      const vendorFolder = getOrCreateFolder_(rootFolder, vendor.name);
      vendor.folderId = vendorFolder.getId();

      const dateFilter = buildDateFilter_(CONFIG.lookbackDays);
      const fullQuery = `${vendor.searchQuery} ${dateFilter} -label:${CONFIG.processedLabel}`;

      Logger.log(`Searching for ${vendor.name}: ${fullQuery}`);

      const threads = GmailApp.search(fullQuery, 0, CONFIG.batchSize);
      let fileCount = 0;

      for (const thread of threads) {
        if (!hasTimeRemaining_()) {
          Logger.log(`Time limit approaching — stopping mid-batch for ${vendor.name}`);
          stoppedEarly = true;
          break;
        }

        if (vendor.quoteInBody) {
          // Tedpack: process each message individually (pricing in email body)
          const messages = thread.getMessages();
          for (const message of messages) {
            if (!hasTimeRemaining_()) break;
            if (isMessageAlreadyProcessed_(message.getId())) {
              Logger.log(`Skipping already-processed message: ${message.getId()}`);
              continue;
            }
            const result = processTedpackMessage_(message, vendorFolder, vendor, today);
            fileCount += result.filesCreated;
            logs.push(...result.logEntries);
            if (result.filesCreated > 0) {
              markMessageProcessed_(message.getId());
            }
          }
          thread.addLabel(label);
        } else {
          // Ross/Dazpak: process at thread level — only when vendor PDF exists
          const threadId = thread.getId();
          if (isMessageAlreadyProcessed_(threadId)) {
            Logger.log(`Skipping already-processed thread: ${threadId}`);
            continue;
          }
          const result = processThread_(thread, vendorFolder, vendor, today);
          fileCount += result.filesCreated;
          logs.push(...result.logEntries);
          if (result.shouldLabel) {
            thread.addLabel(label);
            markMessageProcessed_(threadId);
          }
        }
      }

      Logger.log(`${vendor.name}: processed ${threads.length} threads, created ${fileCount} files`);
    }

    if (logs.length > 0) {
      writeLog_(logFolder, today, logs);
    }

    if (stoppedEarly) {
      Logger.log('Run stopped early due to time limit. Remaining threads will be picked up next run.');
    }

    Logger.log('Vendor quote processing complete.');
  } catch (error) {
    Logger.log(`FATAL ERROR: ${error.message}\n${error.stack}`);
    sendErrorNotification_(error);
  } finally {
    lock.releaseLock();
  }
}

// --- Historical Backfill (run manually, separate from daily trigger) ---------
// Processes older emails in small batches. Run repeatedly until no threads remain.
function backfillHistorical() {
  const lock = LockService.getScriptLock();
  try {
    lock.waitLock(30000);
  } catch (e) {
    Logger.log('Could not obtain lock.');
    return;
  }

  try {
    const rootFolder = DriveApp.getFolderById(CONFIG.rootFolderId);
    const logFolder = getOrCreateFolder_(rootFolder, CONFIG.logFolderName);
    const label = getOrCreateLabel_(CONFIG.processedLabel);
    const today = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd');
    const logs = [];
    let totalThreads = 0;

    for (const [vendorKey, vendor] of Object.entries(CONFIG.vendors)) {
      if (!hasTimeRemaining_()) break;

      const vendorFolder = getOrCreateFolder_(rootFolder, vendor.name);
      vendor.folderId = vendorFolder.getId();

      // No date filter — pull all unprocessed
      const fullQuery = `${vendor.searchQuery} -label:${CONFIG.processedLabel}`;
      const threads = GmailApp.search(fullQuery, 0, CONFIG.batchSize);
      totalThreads += threads.length;

      for (const thread of threads) {
        if (!hasTimeRemaining_()) break;

        if (vendor.quoteInBody) {
          const messages = thread.getMessages();
          for (const message of messages) {
            if (!hasTimeRemaining_()) break;
            if (isMessageAlreadyProcessed_(message.getId())) continue;
            const result = processTedpackMessage_(message, vendorFolder, vendor, today);
            logs.push(...result.logEntries);
            if (result.filesCreated > 0) {
              markMessageProcessed_(message.getId());
            }
          }
          thread.addLabel(label);
        } else {
          const threadId = thread.getId();
          if (isMessageAlreadyProcessed_(threadId)) continue;
          const result = processThread_(thread, vendorFolder, vendor, today);
          logs.push(...result.logEntries);
          if (result.shouldLabel) {
            thread.addLabel(label);
            markMessageProcessed_(threadId);
          }
        }
      }
    }

    if (logs.length > 0) {
      writeLog_(logFolder, today, logs);
    }

    Logger.log(`Backfill batch complete. Processed ${totalThreads} threads. Run again if more remain.`);
  } catch (error) {
    Logger.log(`FATAL ERROR: ${error.message}\n${error.stack}`);
  } finally {
    lock.releaseLock();
  }
}

// --- Cross-Thread Outbound Lookup (Dazpak) -----------------------------------
// When a vendor replies in a new thread, find the original outbound request
// by extracting the FL- identifier from the subject and searching Gmail.
function extractFlId_(subject) {
  const match = subject.match(/(FL-(?:DL|CQ)-\d{3,})/i);
  return match ? match[1] : null;
}

function findOutboundSpecsForSubject_(subject, vendorDomains) {
  const flId = extractFlId_(subject);
  if (!flId) return {};

  // Search for outbound threads containing this FL- ID
  const query = `subject:"${flId}" -from:@${vendorDomains[0]}`;
  const threads = GmailApp.search(query, 0, 5);

  for (const thread of threads) {
    const messages = thread.getMessages();
    for (const msg of messages) {
      // Skip vendor messages — we want the outbound request
      if (isFromVendor_(msg, vendorDomains)) continue;

      const body = msg.getPlainBody();
      if (!body) continue;

      let specs = extractSpecifications_(body);
      if (Object.keys(specs).length < 3) {
        specs = extractSpecsLoose_(body);
      }
      if (Object.keys(specs).length >= 2) {
        return specs;
      }
    }
  }

  return {};
}

// --- Thread-Level Processing (Ross/Dazpak) -----------------------------------
// Only saves files when the vendor has responded with a PDF attachment.
// Extracts requested specs from Dan's outbound message in the same thread,
// or from a linked outbound thread when the vendor replies in a new thread.
function processThread_(thread, vendorFolder, vendor, dateStr) {
  const result = { filesCreated: 0, logEntries: [], shouldLabel: false };
  const messages = thread.getMessages();
  const vendorName = vendor.name;

  // Partition messages into vendor responses vs outbound requests
  const vendorMessages = [];
  const outboundMessages = [];
  for (const message of messages) {
    if (isFromVendor_(message, vendor.domains)) {
      vendorMessages.push(message);
    } else {
      outboundMessages.push(message);
    }
  }

  // Check: does any vendor message have a relevant PDF attachment?
  const vendorHasPdf = vendorMessages.some(msg =>
    msg.getAttachments().some(att => isRelevantAttachment_(att.getContentType()))
  );

  if (!vendorHasPdf) {
    // Vendor hasn't responded with a PDF yet — skip and don't label as processed
    return result;
  }

  // --- Vendor has responded: process both sides ---
  result.shouldLabel = true;

  // 1. Extract requested specs from the outbound message (Dan's request)
  let requestedSpecs = {};
  for (const msg of outboundMessages) {
    const specs = extractSpecifications_(msg.getPlainBody());
    if (Object.keys(specs).length > 0) {
      requestedSpecs = specs;
      break;
    }
  }

  // Cross-thread lookup: if vendor replies in a new thread, the outbound
  // request lives in a separate Gmail thread. Search by FL- ID to find it.
  if (Object.keys(requestedSpecs).length === 0 && vendor.repliesInNewThread && vendorMessages.length > 0) {
    const subject = vendorMessages[0].getSubject();
    requestedSpecs = findOutboundSpecsForSubject_(subject, vendor.domains);
  }

  // Save requested specs JSON if we found any
  if (Object.keys(requestedSpecs).length > 0) {
    const firstVendorMsg = vendorMessages[0];
    const messageDate = Utilities.formatDate(
      firstVendorMsg.getDate(), Session.getScriptTimeZone(), 'yyyy-MM-dd_HHmmss'
    );
    const sanitizedSubject = sanitizeFilename_(firstVendorMsg.getSubject());
    const specsFilename = `${messageDate}_${vendorName}_${sanitizedSubject}_requested_specs.json`;

    try {
      const specsPayload = {
        vendor: vendorName,
        specType: 'requested',
        messageId: firstVendorMsg.getId(),
        emailDate: firstVendorMsg.getDate().toISOString(),
        emailSubject: firstVendorMsg.getSubject(),
        emailFrom: firstVendorMsg.getFrom(),
        extractedAt: new Date().toISOString(),
        specifications: requestedSpecs,
      };
      const specsBlob = Utilities.newBlob(
        JSON.stringify(specsPayload, null, 2),
        'application/json',
        specsFilename
      );
      const specsFile = vendorFolder.createFile(specsBlob);
      applyFileLabels_(specsFile);
      result.filesCreated++;
      result.logEntries.push({
        timestamp: new Date().toISOString(),
        vendor: vendorName,
        type: 'requested_specifications',
        filename: specsFilename,
        mimeType: 'application/json',
        size: specsBlob.getBytes().length,
        messageId: firstVendorMsg.getId(),
        status: 'SUCCESS',
      });
    } catch (e) {
      result.logEntries.push({
        timestamp: new Date().toISOString(),
        vendor: vendorName,
        type: 'requested_specifications',
        filename: specsFilename,
        mimeType: 'application/json',
        size: 0,
        messageId: firstVendorMsg.getId(),
        status: `ERROR: ${e.message}`,
      });
    }
  }

  // 2. Save PDF attachments from vendor messages
  for (const msg of vendorMessages) {
    if (!hasTimeRemaining_()) break;
    const msgResult = processAttachments_(msg, vendorFolder, vendor, dateStr);
    result.filesCreated += msgResult.filesCreated;
    result.logEntries.push(...msgResult.logEntries);
  }

  return result;
}

// --- Attachment-Only Processing (Ross/Dazpak vendor messages) -----------------
function processAttachments_(message, vendorFolder, vendor, dateStr) {
  const vendorName = vendor.name;
  const result = { filesCreated: 0, logEntries: [] };
  const messageDate = Utilities.formatDate(message.getDate(), Session.getScriptTimeZone(), 'yyyy-MM-dd_HHmmss');
  const sanitizedSubject = sanitizeFilename_(message.getSubject());

  const attachments = message.getAttachments();
  for (let i = 0; i < attachments.length; i++) {
    const att = attachments[i];
    const mimeType = att.getContentType();

    if (!isRelevantAttachment_(mimeType)) continue;

    const extension = getExtension_(att.getName(), mimeType);
    const filename = `${messageDate}_${vendorName}_${sanitizedSubject}_att${i + 1}.${extension}`;

    if (fileExistsInFolder_(vendorFolder, filename)) {
      result.logEntries.push({
        timestamp: new Date().toISOString(),
        vendor: vendorName,
        type: 'attachment',
        filename: filename,
        mimeType: mimeType,
        size: 0,
        messageId: message.getId(),
        status: 'SKIPPED: file already exists',
      });
      continue;
    }

    try {
      const file = vendorFolder.createFile(att.copyBlob().setName(filename));
      applyFileLabels_(file);
      result.filesCreated++;
      result.logEntries.push({
        timestamp: new Date().toISOString(),
        vendor: vendorName,
        type: 'attachment',
        filename: filename,
        mimeType: mimeType,
        size: att.getBytes().length,
        messageId: message.getId(),
        status: 'SUCCESS',
      });
    } catch (e) {
      result.logEntries.push({
        timestamp: new Date().toISOString(),
        vendor: vendorName,
        type: 'attachment',
        filename: filename,
        mimeType: mimeType,
        size: 0,
        messageId: message.getId(),
        status: `ERROR: ${e.message}`,
      });
    }
  }

  return result;
}

// --- Tedpack Message Processing ----------------------------------------------
// Only processes messages that contain pricing ($ amounts).
// Converts email body to PDF and extracts specifications.
function processTedpackMessage_(message, vendorFolder, vendor, dateStr) {
  const vendorName = vendor.name;
  const result = { filesCreated: 0, logEntries: [] };
  const plainBody = message.getPlainBody() || '';

  if (!hasPricing_(plainBody)) {
    // No pricing — skip (quote request, not a response)
    result.logEntries.push({
      timestamp: new Date().toISOString(),
      vendor: vendorName,
      type: 'skipped',
      filename: '',
      mimeType: '',
      size: 0,
      messageId: message.getId(),
      status: 'SKIPPED: no pricing found in email body',
    });
    return result;
  }

  const messageDate = Utilities.formatDate(message.getDate(), Session.getScriptTimeZone(), 'yyyy-MM-dd_HHmmss');
  const sanitizedSubject = sanitizeFilename_(message.getSubject());

  // Classify print method: Digital vs Rotogravure
  const printMethod = classifyPrintMethod_(plainBody);

  // Save the email body as HTML
  const htmlFilename = `${messageDate}_${vendorName}_${sanitizedSubject}_body.html`;

  if (fileExistsInFolder_(vendorFolder, htmlFilename)) {
    result.logEntries.push({
      timestamp: new Date().toISOString(),
      vendor: vendorName,
      type: 'email_body_html',
      filename: htmlFilename,
      mimeType: 'text/html',
      size: 0,
      messageId: message.getId(),
      status: 'SKIPPED: file already exists',
    });
  } else {
    try {
      const htmlBlob = Utilities.newBlob(message.getBody(), 'text/html', htmlFilename);
      const file = vendorFolder.createFile(htmlBlob);
      applyFileLabels_(file);
      result.filesCreated++;
      result.logEntries.push({
        timestamp: new Date().toISOString(),
        vendor: vendorName,
        type: 'email_body_html',
        filename: htmlFilename,
        mimeType: 'text/html',
        size: htmlBlob.getBytes().length,
        messageId: message.getId(),
        status: 'SUCCESS',
      });
    } catch (e) {
      result.logEntries.push({
        timestamp: new Date().toISOString(),
        vendor: vendorName,
        type: 'email_body_html',
        filename: htmlFilename,
        mimeType: 'text/html',
        size: 0,
        messageId: message.getId(),
        status: `ERROR: ${e.message}`,
      });
    }
  }

  // Extract specifications from email body and save as JSON sidecar
  const specs = extractSpecifications_(plainBody);
  specs['Print Method'] = printMethod;
  if (Object.keys(specs).length > 0) {
    const specsFilename = `${messageDate}_${vendorName}_${sanitizedSubject}_specs.json`;
    try {
      const specsPayload = {
        vendor: vendorName,
        specType: 'returned',
        messageId: message.getId(),
        emailDate: message.getDate().toISOString(),
        emailSubject: message.getSubject(),
        emailFrom: message.getFrom(),
        extractedAt: new Date().toISOString(),
        specifications: specs,
      };
      const specsBlob = Utilities.newBlob(
        JSON.stringify(specsPayload, null, 2),
        'application/json',
        specsFilename
      );
      const specsFile = vendorFolder.createFile(specsBlob);
      applyFileLabels_(specsFile);
      result.filesCreated++;
      result.logEntries.push({
        timestamp: new Date().toISOString(),
        vendor: vendorName,
        type: 'specifications',
        filename: specsFilename,
        mimeType: 'application/json',
        size: specsBlob.getBytes().length,
        messageId: message.getId(),
        status: 'SUCCESS',
      });
    } catch (e) {
      result.logEntries.push({
        timestamp: new Date().toISOString(),
        vendor: vendorName,
        type: 'specifications',
        filename: specsFilename,
        mimeType: 'application/json',
        size: 0,
        messageId: message.getId(),
        status: `ERROR: ${e.message}`,
      });
    }
  }

  // Also save any actual PDF/image attachments if present
  const attachments = message.getAttachments();
  if (attachments.length > 0) {
    for (let i = 0; i < attachments.length; i++) {
      const att = attachments[i];
      const mimeType = att.getContentType();
      if (!isRelevantAttachment_(mimeType)) continue;

      const extension = getExtension_(att.getName(), mimeType);
      const filename = `${messageDate}_${vendorName}_${sanitizedSubject}_att${i + 1}.${extension}`;
      try {
        const file = vendorFolder.createFile(att.copyBlob().setName(filename));
        applyFileLabels_(file);
        result.filesCreated++;
        result.logEntries.push({
          timestamp: new Date().toISOString(),
          vendor: vendorName,
          type: 'attachment',
          filename: filename,
          mimeType: mimeType,
          size: att.getBytes().length,
          messageId: message.getId(),
          status: 'SUCCESS',
        });
      } catch (e) {
        result.logEntries.push({
          timestamp: new Date().toISOString(),
          vendor: vendorName,
          type: 'attachment',
          filename: filename,
          mimeType: mimeType,
          size: 0,
          messageId: message.getId(),
          status: `ERROR: ${e.message}`,
        });
      }
    }
  }

  return result;
}

// --- Vendor Detection --------------------------------------------------------
function isFromVendor_(message, domains) {
  const from = message.getFrom().toLowerCase();
  return domains.some(domain => from.includes(domain.toLowerCase()));
}

// --- File Label Application --------------------------------------------------
function applyFileLabels_(file) {
  const labels = CONFIG.fileLabels;
  const description = [
    `Sensitivity: ${labels.sensitivityLevel}`,
    `Document Type: ${labels.documentType}`,
    `Functional Domain: ${labels.functionalDomain}`,
    `Lifecycle Status: ${labels.lifecycleStatus}`,
    `Source of Authority: ${labels.sourceOfAuthority}`,
    `Operational Criticality: ${labels.operationalCriticality}`,
    `AI Handling Rules: ${labels.aiHandlingRules.join(', ')}`,
    `Project/Initiative: ${labels.projectInitiative}`,
  ].join('\n');

  file.setDescription(description);
}

// --- Utility Functions -------------------------------------------------------
function getOrCreateFolder_(parent, name) {
  const folders = parent.getFoldersByName(name);
  if (folders.hasNext()) {
    return folders.next();
  }
  return parent.createFolder(name);
}

function getOrCreateLabel_(labelName) {
  let label = GmailApp.getUserLabelByName(labelName);
  if (!label) {
    label = GmailApp.createLabel(labelName);
  }
  return label;
}

function buildDateFilter_(lookbackDays) {
  const date = new Date();
  date.setDate(date.getDate() - lookbackDays);
  const formatted = Utilities.formatDate(date, Session.getScriptTimeZone(), 'yyyy/MM/dd');
  return `after:${formatted}`;
}

function sanitizeFilename_(name) {
  return (name || 'untitled')
    .replace(/[^a-zA-Z0-9\s\-_]/g, '')
    .replace(/\s+/g, '_')
    .substring(0, 80);
}

function isRelevantAttachment_(mimeType) {
  const relevant = [
    'application/pdf',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'application/vnd.ms-excel',
    'text/csv',
  ];
  return relevant.includes(mimeType);
}

function getExtension_(originalName, mimeType) {
  const ext = originalName.split('.').pop();
  if (ext && ext.length <= 5) return ext;

  const map = {
    'application/pdf': 'pdf',
    'image/png': 'png',
    'image/jpeg': 'jpg',
    'image/tiff': 'tiff',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'xlsx',
    'application/vnd.ms-excel': 'xls',
    'text/csv': 'csv',
  };
  return map[mimeType] || 'bin';
}

// --- HTML to PDF Conversion (lightweight — no temp Google Doc) ----------------
function convertHtmlToPdf_(html, filename) {
  // Wrap HTML in a minimal page structure for clean PDF rendering
  const fullHtml = `
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="utf-8">
      <style>
        body { font-family: Arial, sans-serif; font-size: 11pt; line-height: 1.4; margin: 40px; }
        p { margin: 0 0 8px 0; }
      </style>
    </head>
    <body>${html}</body>
    </html>
  `;

  // Use Drive API to create an HTML file and export as PDF in one step
  const htmlBlob = Utilities.newBlob(fullHtml, 'text/html', 'temp.html');
  const tempFile = DriveApp.createFile(htmlBlob);
  try {
    const pdfBlob = tempFile.getAs('application/pdf').setName(filename);
    return pdfBlob;
  } finally {
    tempFile.setTrashed(true);
  }
}

// --- Pricing Detection -------------------------------------------------------
function hasPricing_(plainBody) {
  // Look for dollar amounts (e.g. $0.249/PCS, $1,500, $120/color)
  return /\$\d/.test(plainBody);
}

// --- Print Method Classification ---------------------------------------------
function classifyPrintMethod_(plainBody) {
  if (/plate\s*cost/i.test(plainBody)) {
    return 'Rotogravure';
  }
  if (/digital/i.test(plainBody)) {
    return 'Digital';
  }
  return 'Unknown';
}

// --- Specification Extraction ------------------------------------------------
const SPEC_FIELDS = [
  'Bag',
  'Size',
  'Substrate',
  'Finish',
  'Material',
  'Embellishment',
  'Fill Style',
  'Seal Type',
  'Gusset Style',
  'Gusset Details',
  'Zipper',
  'Tear Notch',
  'Hole Punch',
  'Corners',
  'Printing Method',
  'Quantities',
];

function extractSpecifications_(plainBody) {
  if (!plainBody) return {};

  const specs = {};
  const lines = plainBody.split('\n');

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    for (const field of SPEC_FIELDS) {
      const regex = new RegExp(`^${escapeRegex_(field)}\\s*[:\\-]\\s*(.+)`, 'i');
      const match = trimmed.match(regex);
      if (match) {
        specs[field] = match[1].trim();
        break;
      }
    }
  }

  return specs;
}

function escapeRegex_(str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function writeLog_(logFolder, dateStr, logs) {
  const headers = 'timestamp,vendor,type,filename,mimeType,size,messageId,status\n';
  const rows = logs.map(l =>
    `${l.timestamp},${l.vendor},${l.type},"${l.filename}",${l.mimeType},${l.size},${l.messageId},${l.status}`
  ).join('\n');

  const logBlob = Utilities.newBlob(headers + rows, 'text/csv', `log_${dateStr}.csv`);
  logFolder.createFile(logBlob);
}

function sendErrorNotification_(error) {
  try {
    MailApp.sendEmail({
      to: 'ctimmons@calyxcontainers.com',
      subject: '[ALERT] Vendor Quote Processing Failed',
      body: `The daily vendor quote processing failed.\n\nError: ${error.message}\n\nStack: ${error.stack}\n\nTimestamp: ${new Date().toISOString()}`,
    });
  } catch (e) {
    Logger.log(`Could not send error notification: ${e.message}`);
  }
}

// --- Trigger Setup (run once manually) ---------------------------------------
function setupDailyTrigger() {
  ScriptApp.getProjectTriggers().forEach(trigger => {
    if (trigger.getHandlerFunction() === 'processVendorQuotes') {
      ScriptApp.deleteTrigger(trigger);
    }
  });

  ScriptApp.newTrigger('processVendorQuotes')
    .timeBased()
    .atHour(6)
    .everyDays(1)
    .inTimezone(Session.getScriptTimeZone())
    .create();

  Logger.log('Daily trigger created for 6:00 AM');
}

// --- ONE-TIME: Remove VendorQuotes/Processed label from all vendor threads ----
// Run this manually once, then delete it.
// Has a time guard so it won't exceed 6 minutes — run repeatedly until it logs "Done".
function resetProcessedLabels() {
  const label = GmailApp.getUserLabelByName(CONFIG.processedLabel);
  if (!label) {
    Logger.log('Label not found: ' + CONFIG.processedLabel);
    return;
  }

  // Also clear the ScriptProperties dedup cache
  PropertiesService.getScriptProperties().deleteProperty(PROCESSED_MSG_PROP_KEY);
  Logger.log('Cleared dedup cache.');

  const startTime = Date.now();
  const maxMs = 5 * 60 * 1000; // 5 minutes
  let total = 0;

  while ((Date.now() - startTime) < maxMs) {
    const threads = label.getThreads(0, 100);
    if (threads.length === 0) {
      Logger.log(`Done. Removed label from ${total} threads total.`);
      return;
    }
    for (const thread of threads) {
      thread.removeLabel(label);
      total++;
    }
    Logger.log(`Removed label from ${total} threads so far...`);
  }

  Logger.log(`Time limit reached. Removed label from ${total} threads. Run again to continue.`);
}

// --- Backfill missing sidecar JSON files for already-processed Ross/Dazpak threads ---
// Searches ALREADY PROCESSED threads (with the label) for outbound messages
// containing specs, and creates sidecar JSON files where they don't already exist.
// Run repeatedly until it logs "Done".
function backfillMissingSidecars() {
  const startTime = Date.now();
  const maxMs = 5 * 60 * 1000;
  const rootFolder = DriveApp.getFolderById(CONFIG.rootFolderId);
  const today = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd');
  let created = 0;
  let skipped = 0;
  let checked = 0;

  // Only process Ross and Dazpak (attachment-based vendors)
  const vendors = ['ross', 'dazpak'];

  for (const vendorKey of vendors) {
    if ((Date.now() - startTime) >= maxMs) break;

    const vendor = CONFIG.vendors[vendorKey];
    const vendorFolder = getOrCreateFolder_(rootFolder, vendor.name);
    const vendorName = vendor.name;

    // Search for PROCESSED threads (that already have the label)
    const query = `${vendor.searchQuery} label:${CONFIG.processedLabel}`;
    const threads = GmailApp.search(query, 0, 50);
    Logger.log(`${vendorName}: checking ${threads.length} processed threads for missing sidecars`);

    for (const thread of threads) {
      if ((Date.now() - startTime) >= maxMs) break;
      checked++;

      const messages = thread.getMessages();

      // Find the first vendor message to use for filename prefix
      let firstVendorMsg = null;
      const outboundMessages = [];
      for (const message of messages) {
        if (isFromVendor_(message, vendor.domains)) {
          if (!firstVendorMsg) firstVendorMsg = message;
        } else {
          outboundMessages.push(message);
        }
      }

      if (!firstVendorMsg) continue;

      // Check if a sidecar already exists for this thread
      const messageDate = Utilities.formatDate(
        firstVendorMsg.getDate(), Session.getScriptTimeZone(), 'yyyy-MM-dd_HHmmss'
      );
      const sanitizedSubject = sanitizeFilename_(firstVendorMsg.getSubject());
      const specsFilename = `${messageDate}_${vendorName}_${sanitizedSubject}_requested_specs.json`;

      if (fileExistsInFolder_(vendorFolder, specsFilename)) {
        skipped++;
        continue;
      }

      // Try to extract specs from outbound messages
      let requestedSpecs = {};
      for (const msg of outboundMessages) {
        const body = msg.getPlainBody();
        if (!body) continue;

        // Try standard spec extraction
        let specs = extractSpecifications_(body);

        // If standard extraction found very few fields, try looser parsing
        if (Object.keys(specs).length < 3) {
          specs = extractSpecsLoose_(body);
        }

        if (Object.keys(specs).length >= 2) {
          requestedSpecs = specs;
          break;
        }
      }

      // Cross-thread lookup for vendors that reply in new threads
      if (Object.keys(requestedSpecs).length < 2 && vendor.repliesInNewThread) {
        const subject = firstVendorMsg.getSubject();
        requestedSpecs = findOutboundSpecsForSubject_(subject, vendor.domains);
      }

      if (Object.keys(requestedSpecs).length < 2) continue;

      // Create the sidecar JSON
      try {
        const specsPayload = {
          vendor: vendorName,
          specType: 'requested',
          messageId: firstVendorMsg.getId(),
          emailDate: firstVendorMsg.getDate().toISOString(),
          emailSubject: firstVendorMsg.getSubject(),
          emailFrom: firstVendorMsg.getFrom(),
          extractedAt: new Date().toISOString(),
          specifications: requestedSpecs,
        };
        const specsBlob = Utilities.newBlob(
          JSON.stringify(specsPayload, null, 2),
          'application/json',
          specsFilename
        );
        vendorFolder.createFile(specsBlob);
        created++;
        Logger.log(`Created sidecar: ${specsFilename}`);
      } catch (e) {
        Logger.log(`ERROR creating sidecar ${specsFilename}: ${e.message}`);
      }
    }
  }

  Logger.log(`Done. Checked ${checked} threads, created ${created} sidecars, skipped ${skipped} (already exist).`);
}

// --- Loose spec extraction for older email formats ---
// Matches "Field: Value" or "Field - Value" anywhere in a line (not just at start)
// Also matches "W x H" size patterns and common spec indicators
function extractSpecsLoose_(plainBody) {
  if (!plainBody) return {};
  const specs = {};
  const lines = plainBody.split('\n');

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.length > 200) continue;

    // Size pattern: "4.7 x 7.7" or "5W x 6H x 2G" or "4.7W X 7.7H X 0G"
    if (!specs['Size']) {
      const sizeMatch = trimmed.match(/(\d+\.?\d*)\s*[Ww]?\s*[Xx×]\s*(\d+\.?\d*)\s*[Hh]?\s*(?:[Xx×]\s*(\d+\.?\d*)\s*[Gg]?)?/);
      if (sizeMatch && !trimmed.toLowerCase().includes('image') && !trimmed.toLowerCase().includes('pixel')) {
        specs['Size'] = sizeMatch[0].trim();
      }
    }

    // Standard field: value patterns (more lenient — anywhere in line)
    for (const field of SPEC_FIELDS) {
      if (specs[field]) continue;
      const regex = new RegExp(`${escapeRegex_(field)}\\s*[:\\-–—]\\s*(.{2,80})`, 'i');
      const match = trimmed.match(regex);
      if (match) {
        const value = match[1].trim();
        // Filter out junk
        if (value && !value.startsWith('http') && value.length < 100) {
          specs[field] = value;
        }
      }
    }

    // Bag/Quote ID pattern: "FL-DL-XXXX" or "FL-CQ-XXXX" or "CQ-XXXX"
    if (!specs['Bag']) {
      const bagMatch = trimmed.match(/((?:FL-)?(?:DL|CQ)-\d{3,}(?:\s*[-–]\s*.+)?)/i);
      if (bagMatch && trimmed.length < 150) {
        specs['Bag'] = bagMatch[1].trim();
      }
    }
  }

  return specs;
}

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
      // Filter on full domain; senders include Sherlin
      searchQuery: 'from:@tedpack.com (subject:"Quote Request" OR subject:"FL-")',
      folderId: null,
    },
    ross: {
      name: 'Ross',
      // Filter on full domain; senders include Brett Imlay, Kerry Ann Kauder
      searchQuery: 'from:@rossprint.com (subject:"Quote Request" OR subject:"FL-")',
      folderId: null,
    },
    dazpak: {
      name: 'Dazpak',
      // Filter on full domain; senders include Ernesto Bonilla, Stacey Heitkamp, Kevin Vance
      searchQuery: 'from:@dazpak.com (subject:"Quote Request" OR subject:"FL-")',
      folderId: null,
    },
  },
  rootFolderId: '1ZMKXumVzO_CCl4pNY0iWPximitOEC9Vr',
  logFolderName: '_logs',
  processedLabel: 'VendorQuotes/Processed',
  // Set to 0 for first run (pull ALL historical), then change to 2 for daily
  lookbackDays: 0,
  // Google Drive file sensitivity labels
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

    for (const [vendorKey, vendor] of Object.entries(CONFIG.vendors)) {
      const vendorFolder = getOrCreateFolder_(rootFolder, vendor.name);
      vendor.folderId = vendorFolder.getId();

      let fullQuery;
      if (CONFIG.lookbackDays === 0) {
        // First run: pull ALL historical quotes (no date filter)
        fullQuery = `${vendor.searchQuery} -label:${CONFIG.processedLabel}`;
      } else {
        const dateFilter = buildDateFilter_(CONFIG.lookbackDays);
        fullQuery = `${vendor.searchQuery} ${dateFilter} -label:${CONFIG.processedLabel}`;
      }

      Logger.log(`Searching for ${vendor.name}: ${fullQuery}`);

      const threads = GmailApp.search(fullQuery, 0, 500);
      let fileCount = 0;

      for (const thread of threads) {
        const messages = thread.getMessages();
        for (const message of messages) {
          const result = processMessage_(message, vendorFolder, vendor.name, today);
          fileCount += result.filesCreated;
          logs.push(...result.logEntries);
        }
        thread.addLabel(label);
      }

      Logger.log(`${vendor.name}: processed ${threads.length} threads, created ${fileCount} files`);
    }

    if (logs.length > 0) {
      writeLog_(logFolder, today, logs);
    }

    Logger.log('Vendor quote processing complete.');
  } catch (error) {
    Logger.log(`FATAL ERROR: ${error.message}\n${error.stack}`);
    sendErrorNotification_(error);
  } finally {
    lock.releaseLock();
  }
}

// --- Message Processing ------------------------------------------------------
function processMessage_(message, vendorFolder, vendorName, dateStr) {
  const result = { filesCreated: 0, logEntries: [] };
  const messageDate = Utilities.formatDate(message.getDate(), Session.getScriptTimeZone(), 'yyyy-MM-dd_HHmmss');
  const sanitizedSubject = sanitizeFilename_(message.getSubject());

  // Process attachments (PDFs, images, Excel files)
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

  // If no relevant attachments, save the email body as HTML
  if (attachments.length === 0 || !attachments.some(a => isRelevantAttachment_(a.getContentType()))) {
    const htmlFilename = `${messageDate}_${vendorName}_${sanitizedSubject}_body.html`;
    try {
      const htmlBlob = Utilities.newBlob(message.getBody(), 'text/html', htmlFilename);
      const file = vendorFolder.createFile(htmlBlob);
      applyFileLabels_(file);
      result.filesCreated++;
      result.logEntries.push({
        timestamp: new Date().toISOString(),
        vendor: vendorName,
        type: 'email_body',
        filename: htmlFilename,
        mimeType: 'text/html',
        size: message.getBody().length,
        messageId: message.getId(),
        status: 'SUCCESS',
      });
    } catch (e) {
      result.logEntries.push({
        timestamp: new Date().toISOString(),
        vendor: vendorName,
        type: 'email_body',
        filename: htmlFilename,
        mimeType: 'text/html',
        size: 0,
        messageId: message.getId(),
        status: `ERROR: ${e.message}`,
      });
    }
  }

  return result;
}

// --- File Label Application --------------------------------------------------
function applyFileLabels_(file) {
  // Set file description with structured label metadata
  // Google Drive Advanced API labels require Drive Labels API enabled
  // For now, store labels as structured description metadata
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
    'image/png',
    'image/jpeg',
    'image/tiff',
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

// --- Post-Initial-Run: Switch to daily lookback ------------------------------
// After the first historical run completes successfully,
// change CONFIG.lookbackDays from 0 to 2 for daily operation.
function switchToDailyMode() {
  Logger.log('NOTE: After verifying historical pull, update CONFIG.lookbackDays to 2');
}

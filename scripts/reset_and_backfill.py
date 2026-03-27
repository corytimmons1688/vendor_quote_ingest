#!/usr/bin/env python3
"""
Reset VendorQuotes/Processed labels and run backfill.
Uses Gmail + Drive APIs via OAuth tokens from clasp.
"""

import json
import os
import re
import time
from pathlib import Path
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account as sa_module
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload

# --- Config ---
CLASP_RC = Path.home() / ".clasprc.json"
ROOT_FOLDER_ID = "1ZMKXumVzO_CCl4pNY0iWPximitOEC9Vr"
LABEL_NAME = "VendorQuotes/Processed"

VENDORS = {
    "tedpack": {
        "name": "Tedpack",
        "query": 'from:@tedpack.com (subject:"Quote Request" OR subject:"FL-")',
        "quoteInBody": True,
        "domains": ["tedpack.com"],
    },
    "ross": {
        "name": "Ross",
        "query": 'from:@rossprint.com (subject:"Quote Request" OR subject:"FL-")',
        "quoteInBody": False,
        "domains": ["rossprint.com"],
    },
    "dazpak": {
        "name": "Dazpak",
        "query": 'from:@dazpak.com (subject:"Quote Request" OR subject:"FL-")',
        "quoteInBody": False,
        "domains": ["dazpak.com"],
    },
}

SPEC_FIELDS = [
    "Bag", "Size", "Substrate", "Finish", "Material", "Embellishment",
    "Fill Style", "Seal Type", "Gusset Style", "Gusset Details",
    "Zipper", "Tear Notch", "Hole Punch", "Corners", "Printing Method", "Quantities",
]

RELEVANT_MIMES = [
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-excel",
    "text/csv",
]

MIME_EXT = {
    "application/pdf": "pdf",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
    "application/vnd.ms-excel": "xls",
    "text/csv": "csv",
}

BATCH_SIZE = 25
MAX_RUN_SECONDS = 280  # ~4.5 minutes per batch


SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.labels",
    "https://mail.google.com/",
    "https://www.googleapis.com/auth/drive",
]
TOKEN_FILE = Path(__file__).parent / ".backfill_token.json"
SERVICE_ACCOUNT_FILE = Path(__file__).parent / "service_account.json"
IMPERSONATE_USER = "ctimmons@calyxcontainers.com"


def get_credentials():
    """Get credentials — prefer service account with domain-wide delegation, fall back to OAuth."""

    # --- Try service account first ---
    if SERVICE_ACCOUNT_FILE.exists():
        creds = sa_module.Credentials.from_service_account_file(
            str(SERVICE_ACCOUNT_FILE),
            scopes=SCOPES,
            subject=IMPERSONATE_USER,
        )
        return creds

    # --- Fall back to OAuth ---
    from google_auth_oauthlib.flow import InstalledAppFlow

    creds = None

    # Try loading saved token
    if TOKEN_FILE.exists():
        with open(TOKEN_FILE) as f:
            token_data = json.load(f)
        creds = Credentials(
            token=token_data.get("access_token"),
            refresh_token=token_data.get("refresh_token"),
            token_uri="https://oauth2.googleapis.com/token",
            client_id=token_data.get("client_id"),
            client_secret=token_data.get("client_secret"),
            scopes=SCOPES,
        )
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())

    if not creds or not creds.valid:
        # Use clasp's client ID/secret for the OAuth flow
        with open(CLASP_RC) as f:
            clasp_data = json.load(f)
        clasp_token = clasp_data["tokens"]["default"]

        client_config = {
            "installed": {
                "client_id": clasp_token["client_id"],
                "client_secret": clasp_token["client_secret"],
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": ["http://localhost"],
            }
        }
        flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
        creds = flow.run_local_server(port=0)

        # Save for next time
        with open(TOKEN_FILE, "w") as f:
            json.dump({
                "access_token": creds.token,
                "refresh_token": creds.refresh_token,
                "client_id": creds.client_id,
                "client_secret": creds.client_secret,
            }, f)

    return creds


def get_label_id(gmail, label_name):
    """Find Gmail label ID by name."""
    results = gmail.users().labels().list(userId="me").execute()
    for label in results.get("labels", []):
        if label["name"] == label_name:
            return label["id"]
    return None


def get_or_create_folder(drive, parent_id, name):
    """Get or create a Drive folder (supports Shared Drives)."""
    q = f"'{parent_id}' in parents and name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    results = drive.files().list(
        q=q, fields="files(id,name)",
        supportsAllDrives=True, includeItemsFromAllDrives=True,
    ).execute()
    files = results.get("files", [])
    if files:
        return files[0]["id"]
    meta = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    folder = drive.files().create(
        body=meta, fields="id", supportsAllDrives=True,
    ).execute()
    return folder["id"]


def sanitize_filename(name):
    cleaned = re.sub(r"[^a-zA-Z0-9\s\-_]", "", name or "untitled")
    cleaned = re.sub(r"\s+", "_", cleaned)
    return cleaned[:80]


def extract_specs(plain_body):
    if not plain_body:
        return {}
    specs = {}
    for line in plain_body.split("\n"):
        trimmed = line.strip()
        if not trimmed:
            continue
        for field in SPEC_FIELDS:
            pattern = re.compile(rf"^{re.escape(field)}\s*[:\-]\s*(.+)", re.IGNORECASE)
            m = pattern.match(trimmed)
            if m:
                specs[field] = m.group(1).strip()
                break
    return specs


def has_pricing(text):
    return bool(re.search(r"\$\d", text or ""))


def classify_print_method(text):
    if re.search(r"plate\s*cost", text or "", re.IGNORECASE):
        return "Rotogravure"
    if re.search(r"digital", text or "", re.IGNORECASE):
        return "Digital"
    return "Unknown"


def file_exists_in_folder(drive, folder_id, filename):
    q = f"'{folder_id}' in parents and name='{filename}' and trashed=false"
    results = drive.files().list(
        q=q, fields="files(id)",
        supportsAllDrives=True, includeItemsFromAllDrives=True,
    ).execute()
    return len(results.get("files", [])) > 0


def save_to_drive(drive, folder_id, filename, content, mime_type):
    """Save content to Drive, skip if exists."""
    if file_exists_in_folder(drive, folder_id, filename):
        print(f"    SKIP (exists): {filename}")
        return False
    media = MediaInMemoryUpload(
        content if isinstance(content, bytes) else content.encode("utf-8"),
        mimetype=mime_type,
    )
    meta = {"name": filename, "parents": [folder_id]}
    drive.files().create(
        body=meta, media_body=media, fields="id", supportsAllDrives=True,
    ).execute()
    print(f"    SAVED: {filename}")
    return True


def is_from_vendor(msg_from, domains):
    lower = (msg_from or "").lower()
    return any(d.lower() in lower for d in domains)


def get_header(headers, name):
    for h in headers:
        if h["name"].lower() == name.lower():
            return h["value"]
    return ""


def get_message_plain_body(gmail, msg_id):
    """Extract plain text body from a message."""
    msg = gmail.users().messages().get(userId="me", id=msg_id, format="full").execute()
    return _extract_body(msg.get("payload", {}), "text/plain"), msg


def get_message_html_body(gmail, msg_id):
    """Extract HTML body from a message."""
    msg = gmail.users().messages().get(userId="me", id=msg_id, format="full").execute()
    return _extract_body(msg.get("payload", {}), "text/html"), msg


def _extract_body(payload, target_mime):
    import base64
    if payload.get("mimeType") == target_mime and payload.get("body", {}).get("data"):
        return base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8", errors="replace")
    for part in payload.get("parts", []):
        result = _extract_body(part, target_mime)
        if result:
            return result
    return ""


def get_attachments(gmail, msg_id, msg_payload):
    """Get relevant attachments from a message."""
    import base64
    attachments = []
    parts = msg_payload.get("parts", [])
    for part in parts:
        filename = part.get("filename", "")
        mime_type = part.get("mimeType", "")
        body = part.get("body", {})
        if filename and mime_type in RELEVANT_MIMES and body.get("attachmentId"):
            att_data = gmail.users().messages().attachments().get(
                userId="me", messageId=msg_id, id=body["attachmentId"]
            ).execute()
            data = base64.urlsafe_b64decode(att_data["data"])
            attachments.append({"filename": filename, "mimeType": mime_type, "data": data})
    return attachments


# --- Phase 1: Reset Labels ---
def reset_labels(gmail):
    print("=" * 60)
    print("PHASE 1: Removing VendorQuotes/Processed label")
    print("=" * 60)

    label_id = get_label_id(gmail, LABEL_NAME)
    if not label_id:
        print(f"Label '{LABEL_NAME}' not found. Nothing to reset.")
        return

    total = 0
    while True:
        results = gmail.users().threads().list(
            userId="me", labelIds=[label_id], maxResults=100
        ).execute()
        threads = results.get("threads", [])
        if not threads:
            break
        for t in threads:
            gmail.users().threads().modify(
                userId="me", id=t["id"],
                body={"removeLabelIds": [label_id]}
            ).execute()
            total += 1
        print(f"  Removed label from {total} threads...")

    print(f"  Done. Reset {total} threads total.\n")


# --- Phase 2: Backfill ---
def backfill(gmail, drive):
    print("=" * 60)
    print("PHASE 2: Backfill — processing vendor quote emails")
    print("=" * 60)

    label_id = get_label_id(gmail, LABEL_NAME)
    if not label_id:
        # Create label
        body = {"name": LABEL_NAME, "labelListVisibility": "labelShow", "messageListVisibility": "show"}
        result = gmail.users().labels().create(userId="me", body=body).execute()
        label_id = result["id"]
        print(f"  Created label: {LABEL_NAME} ({label_id})")

    run_start = time.time()
    total_files = 0
    batch_num = 0

    while True:
        batch_num += 1
        batch_files = 0
        any_threads = False

        if (time.time() - run_start) > MAX_RUN_SECONDS:
            print(f"\n  Time limit reached. Run again to continue.")
            break

        for vendor_key, vendor in VENDORS.items():
            if (time.time() - run_start) > MAX_RUN_SECONDS:
                break

            folder_id = get_or_create_folder(drive, ROOT_FOLDER_ID, vendor["name"])
            query = f'{vendor["query"]} -label:{LABEL_NAME}'

            results = gmail.users().threads().list(
                userId="me", q=query, maxResults=BATCH_SIZE
            ).execute()
            threads = results.get("threads", [])

            if threads:
                any_threads = True
                print(f"\n  [{vendor['name']}] Batch {batch_num}: {len(threads)} threads")

            for t in threads:
                if (time.time() - run_start) > MAX_RUN_SECONDS:
                    break

                thread = gmail.users().threads().get(userId="me", id=t["id"]).execute()
                messages = thread.get("messages", [])

                if vendor["quoteInBody"]:
                    # Tedpack: process each message for pricing
                    for msg in messages:
                        msg_id = msg["id"]
                        headers = msg.get("payload", {}).get("headers", [])
                        msg_from = get_header(headers, "From")
                        subject = get_header(headers, "Subject")
                        date_str = get_header(headers, "Date")

                        plain_body = _extract_body(msg.get("payload", {}), "text/plain")
                        if not has_pricing(plain_body):
                            continue

                        ts = sanitize_filename(date_str[:19].replace(":", "").replace(" ", "_"))
                        subj = sanitize_filename(subject)
                        prefix = f"{ts}_{vendor['name']}_{subj}"

                        # Save HTML body
                        html_body = _extract_body(msg.get("payload", {}), "text/html")
                        if html_body:
                            fname = f"{prefix}_body.html"
                            if save_to_drive(drive, folder_id, fname, html_body, "text/html"):
                                batch_files += 1

                        # Save specs JSON
                        specs = extract_specs(plain_body)
                        specs["Print Method"] = classify_print_method(plain_body)
                        if specs:
                            fname = f"{prefix}_specs.json"
                            payload = {
                                "vendor": vendor["name"],
                                "specType": "returned",
                                "messageId": msg_id,
                                "emailSubject": subject,
                                "emailFrom": msg_from,
                                "specifications": specs,
                            }
                            if save_to_drive(drive, folder_id, fname, json.dumps(payload, indent=2), "application/json"):
                                batch_files += 1

                        # Save attachments
                        atts = get_attachments(gmail, msg_id, msg.get("payload", {}))
                        for i, att in enumerate(atts):
                            ext = MIME_EXT.get(att["mimeType"], "bin")
                            fname = f"{prefix}_att{i+1}.{ext}"
                            if save_to_drive(drive, folder_id, fname, att["data"], att["mimeType"]):
                                batch_files += 1

                else:
                    # Ross/Dazpak: thread-level — only when vendor has PDF
                    vendor_msgs = []
                    outbound_msgs = []
                    for msg in messages:
                        msg_from = get_header(msg.get("payload", {}).get("headers", []), "From")
                        if is_from_vendor(msg_from, vendor["domains"]):
                            vendor_msgs.append(msg)
                        else:
                            outbound_msgs.append(msg)

                    # Check for PDF attachments from vendor
                    has_pdf = False
                    for msg in vendor_msgs:
                        for part in msg.get("payload", {}).get("parts", []):
                            if part.get("filename") and part.get("mimeType") in RELEVANT_MIMES:
                                has_pdf = True
                                break
                        if has_pdf:
                            break

                    if not has_pdf:
                        continue  # Don't label — vendor hasn't responded yet

                    # Extract requested specs from outbound
                    requested_specs = {}
                    for msg in outbound_msgs:
                        plain = _extract_body(msg.get("payload", {}), "text/plain")
                        specs = extract_specs(plain)
                        if specs:
                            requested_specs = specs
                            break

                    first_vendor = vendor_msgs[0] if vendor_msgs else messages[0]
                    fv_headers = first_vendor.get("payload", {}).get("headers", [])
                    fv_subject = get_header(fv_headers, "Subject")
                    fv_date = get_header(fv_headers, "Date")
                    ts = sanitize_filename(fv_date[:19].replace(":", "").replace(" ", "_"))
                    subj = sanitize_filename(fv_subject)
                    prefix = f"{ts}_{vendor['name']}_{subj}"

                    # Save requested specs
                    if requested_specs:
                        fname = f"{prefix}_requested_specs.json"
                        payload = {
                            "vendor": vendor["name"],
                            "specType": "requested",
                            "messageId": first_vendor["id"],
                            "emailSubject": fv_subject,
                            "specifications": requested_specs,
                        }
                        if save_to_drive(drive, folder_id, fname, json.dumps(payload, indent=2), "application/json"):
                            batch_files += 1

                    # Save PDF attachments from vendor messages
                    for msg in vendor_msgs:
                        msg_id = msg["id"]
                        atts = get_attachments(gmail, msg_id, msg.get("payload", {}))
                        for i, att in enumerate(atts):
                            ext = MIME_EXT.get(att["mimeType"], "bin")
                            fname = f"{prefix}_att{i+1}.{ext}"
                            if save_to_drive(drive, folder_id, fname, att["data"], att["mimeType"]):
                                batch_files += 1

                # Label thread as processed
                gmail.users().threads().modify(
                    userId="me", id=t["id"],
                    body={"addLabelIds": [label_id]}
                ).execute()

            total_files += batch_files

        if not any_threads:
            print(f"\n  All threads processed! Total files created: {total_files}")
            break

    return total_files


def main():
    print("Initializing Google API clients...\n")
    creds = get_credentials()
    gmail = build("gmail", "v1", credentials=creds)
    drive = build("drive", "v3", credentials=creds)

    # Phase 1: Reset labels
    reset_labels(gmail)

    # Phase 2: Backfill (loop until done)
    total = 0
    iteration = 0
    while True:
        iteration += 1
        print(f"\n--- Backfill iteration {iteration} ---")
        files = backfill(gmail, drive)
        total += files
        if files == 0:
            break
        print(f"  Created {files} files this iteration. Checking for more...")

    print(f"\n{'=' * 60}")
    print(f"BACKFILL COMPLETE. Total files created: {total}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()

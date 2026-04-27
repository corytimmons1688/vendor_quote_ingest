#!/usr/bin/env python3
"""
Phase 2: For Dazpak rows where tesseract OCR lost the Quantities column,
re-pull the source PDF from Drive and extract its text with pdfplumber.
The Dazpak PDFs have an embedded text layer that pdfplumber reads cleanly
(no rasterization, no tesseract noise), so the Quantities column comes
through intact.

Updates:
  - raw_ocr_text  → pdfplumber output (so future re-extracts use clean text)
  - ocr_engine    → 'pdfplumber'
  - all DAZPAK_EXTRACTED_COLS → re-extracted from clean text

Usage:
    NEON_DATABASE_URL=... python3 scripts/reextract_via_pdfplumber.py [--apply]
    NEON_DATABASE_URL=... python3 scripts/reextract_via_pdfplumber.py \
        --where-raw "quote_number IN ('13804','13805')"      [--apply]
"""
import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

import psycopg2
import pdfplumber
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

sys.path.insert(0, str(Path(__file__).parent))
from vendor_extractors import extract_dazpak  # noqa: E402

DAZPAK_FOLDER_ID = '1Q50UVUkpqov3PdYTiTpcZFCriGQfq3P1'
SERVICE_ACCOUNT = Path(__file__).parent / 'service_account.json'
IMPERSONATE = 'ctimmons@calyxcontainers.com'

DAZPAK_EXTRACTED_COLS = [
    'quote_number', 'item_description', 'item_size', 'ink_colors',
    'material_structure', 'pricing_json', 'plate_cost', 'web_width',
    'repeat_length', 'terms', 'fob', 'quote_date', 'quote_validity',
]
RETURNED_SPEC_SUFFIXES = [
    'bag', 'size', 'substrate', 'finish', 'embellishment',
    'fill_style', 'seal_type', 'gusset',
    'zipper', 'tear_notch', 'hole_punch', 'corners',
    'quantities',
]


def drive_client():
    creds = service_account.Credentials.from_service_account_file(
        str(SERVICE_ACCOUNT),
        scopes=['https://www.googleapis.com/auth/drive'],
        subject=IMPERSONATE,
    )
    return build('drive', 'v3', credentials=creds)


def list_all_dazpak_files(drive):
    """List every file in the Dazpak folder once and return name → id map."""
    name_to_id = {}
    page_token = None
    while True:
        res = drive.files().list(
            q=f"'{DAZPAK_FOLDER_ID}' in parents and mimeType='application/pdf' and trashed=false",
            fields='nextPageToken,files(id,name)',
            supportsAllDrives=True, includeItemsFromAllDrives=True,
            pageSize=1000,
            pageToken=page_token,
        ).execute()
        for f in res.get('files', []):
            name_to_id[f['name']] = f['id']
        page_token = res.get('nextPageToken')
        if not page_token:
            break
    return name_to_id


def download_pdf(drive, file_id):
    fd, tmp = tempfile.mkstemp(suffix='.pdf')
    os.close(fd)
    with open(tmp, 'wb') as f:
        req = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
        dl = MediaIoBaseDownload(f, req)
        done = False
        while not done:
            _, done = dl.next_chunk()
    return tmp


def pdfplumber_text(path):
    parts = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ''
            parts.append(t)
    return '\n\n--- Page Break ---\n\n'.join(parts)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--where-raw',
                    default="pricing_json IS NULL AND file_type='pdf' AND raw_ocr_text ~ '\\$[0-9]+\\.'",
                    help='WHERE clause for selecting orphan rows')
    ap.add_argument('--apply', action='store_true')
    ap.add_argument('--limit', type=int, default=None)
    args = ap.parse_args()

    conn = psycopg2.connect(os.environ['NEON_DATABASE_URL'], sslmode='require')
    conn.autocommit = False
    cur = conn.cursor()

    sql = (
        "SELECT ingestion_id, source_file FROM est_ex_br_dazpak "
        f"WHERE {args.where_raw} ORDER BY source_file"
    )
    if args.limit:
        sql += f" LIMIT {args.limit}"
    cur.execute(sql)
    targets = cur.fetchall()
    print(f'Targets: {len(targets)}  Mode: {"APPLY" if args.apply else "DRY-RUN"}')
    print('=' * 78)

    drive = drive_client()
    print('Listing Dazpak Drive folder…')
    name_map = list_all_dazpak_files(drive)
    print(f'  {len(name_map)} PDFs in folder')
    fixed = 0
    no_drive = []
    no_pricing = []

    for ing_id, source_file in targets:
        print(f'\n[{ing_id}] {source_file[:75]}')
        fid = name_map.get(source_file)
        if not fid:
            print('   NOT FOUND in Drive')
            no_drive.append(source_file)
            continue
        try:
            tmp = download_pdf(drive, fid)
            text = pdfplumber_text(tmp)
            os.unlink(tmp)
        except Exception as e:
            print(f'   ERROR: {e}')
            no_drive.append(source_file)
            continue

        extracted = extract_dazpak(text)
        pricing = extracted.get('pricing_json')
        if not pricing:
            print('   pdfplumber text did NOT yield pricing — skipping')
            no_pricing.append(source_file)
            continue

        rows = json.loads(pricing)
        print(f'   recovered {len(rows)} tiers: ' +
              ', '.join(r['quantity'] for r in rows))
        fixed += 1

        if not args.apply:
            continue

        # Apply update: vendor-extracted cols + raw_ocr_text + ocr_engine
        set_clauses = [f'{c} = %s' for c in DAZPAK_EXTRACTED_COLS]
        params = [extracted.get(c) for c in DAZPAK_EXTRACTED_COLS]
        set_clauses.append('raw_ocr_text = %s')
        params.append(text)
        set_clauses.append("ocr_engine = 'pdfplumber'")

        # Returned-spec merge (fill nulls only)
        rs_updates = {
            k.replace('returned_spec_', ''): v
            for k, v in extracted.items()
            if k.startswith('returned_spec_')
        }
        if rs_updates:
            cur.execute(
                "SELECT " +
                ", ".join(f'returned_spec_{s}' for s in RETURNED_SPEC_SUFFIXES) +
                " FROM est_ex_br_dazpak WHERE ingestion_id = %s",
                (ing_id,),
            )
            existing = cur.fetchone() or (None,) * len(RETURNED_SPEC_SUFFIXES)
            for suffix, current in zip(RETURNED_SPEC_SUFFIXES, existing):
                new = rs_updates.get(suffix)
                if new and not current:
                    set_clauses.append(f'returned_spec_{suffix} = %s')
                    params.append(new)

        params.append(ing_id)
        cur.execute(
            "UPDATE est_ex_br_dazpak SET " + ", ".join(set_clauses) +
            " WHERE ingestion_id = %s",
            params,
        )

    if args.apply:
        conn.commit()
    else:
        conn.rollback()

    print('\n' + '=' * 78)
    print(f'Recovered pricing: {fixed} of {len(targets)}')
    if no_drive:
        print(f'NOT FOUND in Drive: {len(no_drive)}')
        for sf in no_drive[:5]:
            print(f'  {sf}')
    if no_pricing:
        print(f'pdfplumber text but no pricing parsed: {len(no_pricing)}')
        for sf in no_pricing[:5]:
            print(f'  {sf}')
    if not args.apply:
        print('Dry run — no changes written. Re-run with --apply.')
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())

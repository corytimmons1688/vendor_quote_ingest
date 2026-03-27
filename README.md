# Vendor Quote Ingest

Bronze-layer data pipeline for Calyx Containers vendor quote processing.

## Overview

Extracts vendor quote PDFs from Gmail, stores them in Google Drive, then OCRs and loads raw data into Neon Postgres bronze-layer tables.

## Pipeline

```
Gmail (vendor emails) → AppScript (6 AM daily) → Google Drive
Google Drive → GitHub Actions (8 AM daily) → OCR → Neon Postgres
```

## Vendors

| Vendor  | Domain          | Print Method  |
|---------|-----------------|---------------|
| Dazpak  | @dazpak.com     | Flexographic  |
| Ross    | @rossprint.com  | Digital       |
| Tedpack | @tedpack.com    | Gravure       |

## Bronze Tables (Neon)

- `est_bnz_tedpack`
- `est_bnz_ross`
- `est_bnz_dazpak`
- `est_bnz_ingestion_log`

## Setup

1. Deploy AppScript: See `appscript/vendorQuoteProcessor.gs`
2. Create Neon schema: Run `db/bronze_schema.sql`
3. Configure GitHub secrets:
   - `NEON_DATABASE_URL` — Neon dev-estimates branch connection string
   - `GOOGLE_SERVICE_ACCOUNT_KEY` — GCP service account JSON
   - `DRIVE_ROOT_FOLDER_ID` — Google Drive VendorQuotes folder ID

## Local Development

```bash
pip install -r scripts/requirements.txt
cp .env.example .env
# Fill in .env values
```

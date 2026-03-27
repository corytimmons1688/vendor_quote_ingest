"""
Loads OCR-processed JSON data into Neon Postgres bronze tables.
One row per extracted field line, plus one row with the full raw OCR text.
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values


VENDOR_TABLE_MAP = {
    'Tedpack': 'est_bnz_tedpack',
    'Ross':    'est_bnz_ross',
    'Dazpak':  'est_bnz_dazpak',
}


def get_connection():
    """Connect to Neon database."""
    return psycopg2.connect(
        os.environ['NEON_DATABASE_URL'],
        sslmode='require'
    )


def parse_filename_metadata(filename):
    """
    Extract metadata from the standardized filename.
    Format: 2026-03-24_060000_Tedpack_Quote-for-March_att1.pdf
    """
    parts = filename.rsplit('.', 1)[0].split('_', 3)
    if len(parts) >= 3:
        return {
            'email_date': parts[0] if parts[0] else None,
            'email_subject': parts[3] if len(parts) > 3 else None
        }
    return {'email_date': None, 'email_subject': None}


def load_file(conn, table_name, data, run_id):
    """Load a single processed JSON file into the database."""
    rows = []
    file_meta = parse_filename_metadata(data['source_file'])

    for page_data in data['pages']:
        # Row 1: Full raw OCR text for this page (field_key = NULL)
        rows.append((
            data['source_file'],
            data['vendor'],
            file_meta['email_date'],
            file_meta['email_subject'],
            None,  # email_from
            page_data['raw_text'],
            None,  # field_key
            None,  # field_value
            None,  # field_line_num
            None,  # field_confidence
            data['file_type'],
            data['file_size_bytes'],
            data['ocr_engine'],
            data['ocr_version'],
            page_data.get('page', 1),
            run_id,
            'raw',
            None  # error_message
        ))

        # Row per extracted field line
        for field in page_data.get('fields', []):
            text = field['text']
            key, value = split_key_value(text)

            rows.append((
                data['source_file'],
                data['vendor'],
                file_meta['email_date'],
                file_meta['email_subject'],
                None,
                None,  # raw_ocr_text (only on full-text row)
                key,
                value,
                field['line_num'],
                field.get('confidence'),
                data['file_type'],
                data['file_size_bytes'],
                data['ocr_engine'],
                data['ocr_version'],
                page_data.get('page', 1),
                run_id,
                'raw',
                None
            ))

    insert_sql = f"""
        INSERT INTO {table_name} (
            source_file, source_vendor, email_date, email_subject, email_from,
            raw_ocr_text, field_key, field_value, field_line_num, field_confidence,
            file_type, file_size_bytes, ocr_engine, ocr_version, page_number,
            processing_run, status, error_message
        ) VALUES %s
    """

    with conn.cursor() as cur:
        execute_values(cur, insert_sql, rows)

    return len(rows)


def split_key_value(text):
    """
    Attempt to split 'Key: Value' or 'Key\\tValue' patterns.
    Returns (key, value) if a pattern is found, else (text, text).
    Bronze layer: store both representations — don't lose data.
    """
    for delimiter in [':\t', ': ', ':\t\t', '\t']:
        if delimiter in text:
            parts = text.split(delimiter, 1)
            if len(parts) == 2 and parts[0].strip() and parts[1].strip():
                return parts[0].strip(), parts[1].strip()

    return text, text


def log_ingestion(conn, run_id, vendor, files_processed, rows_inserted, errors, error_details):
    """Write to the ingestion audit log."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO est_bnz_ingestion_log
            (processing_run, vendor, files_processed, rows_inserted, errors,
             completed_at, status, error_details)
            VALUES (%s, %s, %s, %s, %s, NOW(), %s, %s)
        """, (
            run_id, vendor, files_processed, rows_inserted, errors,
            'completed' if errors == 0 else 'completed_with_errors',
            json.dumps(error_details) if error_details else None
        ))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--vendor', required=True)
    parser.add_argument('--input-dir', required=True)
    parser.add_argument('--run-id', required=True)
    args = parser.parse_args()

    table_name = VENDOR_TABLE_MAP.get(args.vendor)
    if not table_name:
        print(f"ERROR: Unknown vendor '{args.vendor}'", file=sys.stderr)
        sys.exit(1)

    input_dir = Path(args.input_dir)
    json_files = [f for f in input_dir.glob('*.json') if not f.name.startswith('_')]

    print(f"Loading {len(json_files)} files into {table_name}")

    conn = get_connection()
    total_rows = 0
    errors = []

    try:
        for json_file in json_files:
            try:
                with open(json_file) as f:
                    data = json.load(f)

                rows = load_file(conn, table_name, data, args.run_id)
                total_rows += rows
                print(f"  Loaded {rows} rows from {json_file.name}")

            except Exception as e:
                print(f"  ERROR loading {json_file.name}: {e}", file=sys.stderr)
                errors.append({'file': json_file.name, 'error': str(e)})
                conn.rollback()
                continue

        conn.commit()

        log_ingestion(
            conn, args.run_id, args.vendor,
            len(json_files), total_rows, len(errors), errors
        )
        conn.commit()

        print(f"Done: {total_rows} total rows, {len(errors)} errors")

    finally:
        conn.close()

    if errors:
        sys.exit(1)


if __name__ == '__main__':
    main()

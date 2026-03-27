"""
Loads OCR-processed JSON data into Neon Postgres bronze tables.
One row per extracted field line, plus one row with the full raw OCR text.

Ross/Dazpak tables have additional requested_spec_* and returned_spec_* columns
populated from structured JSON specs and OCR'd PDF field extraction respectively.
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

# Vendors whose tables have the requested/returned spec columns
VENDORS_WITH_SPEC_COLUMNS = {'Ross', 'Dazpak'}

# Vendor-specific extracted columns (from vendor_extractors.py)
VENDOR_EXTRACTED_COLUMNS = {
    'Tedpack': [
        'spec_bag', 'spec_size', 'spec_substrate', 'spec_finish', 'spec_material',
        'spec_embellishment', 'spec_fill_style', 'spec_seal_type', 'spec_gusset_style',
        'spec_gusset_details', 'spec_zipper', 'spec_tear_notch', 'spec_hole_punch',
        'spec_corners', 'spec_printing_method', 'spec_quantities',
        'print_method', 'pricing_json', 'plate_cost', 'lead_time', 'quote_id',
    ],
    'Ross': [
        'estimate_number', 'application', 'product_size', 'colors', 'materials',
        'finishing', 'pricing_json', 'plate_cost', 'lead_time', 'quote_date',
        'quote_validity',
    ],
    'Dazpak': [
        'quote_number', 'item_description', 'item_size', 'ink_colors',
        'material_structure', 'pricing_json', 'plate_cost', 'web_width',
        'repeat_length', 'terms', 'fob', 'quote_date', 'quote_validity',
    ],
}

# Ordered list of spec column suffixes — must match schema column order
SPEC_COLUMN_SUFFIXES = [
    'bag', 'size', 'substrate', 'finish', 'material', 'embellishment',
    'fill_style', 'seal_type', 'gusset_style', 'gusset_details',
    'zipper', 'tear_notch', 'hole_punch', 'corners', 'printing_method',
    'quantities',
]

# Map from human-readable spec names (as extracted by Apps Script) to column suffixes
SPEC_NAME_TO_COLUMN = {
    'Bag': 'bag',
    'Size': 'size',
    'Substrate': 'substrate',
    'Finish': 'finish',
    'Material': 'material',
    'Embellishment': 'embellishment',
    'Fill Style': 'fill_style',
    'Seal Type': 'seal_type',
    'Gusset Style': 'gusset_style',
    'Gusset Details': 'gusset_details',
    'Zipper': 'zipper',
    'Tear Notch': 'tear_notch',
    'Hole Punch': 'hole_punch',
    'Corners': 'corners',
    'Printing Method': 'printing_method',
    'Quantities': 'quantities',
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


def specs_dict_to_column_values(specs_dict, name_map=SPEC_NAME_TO_COLUMN):
    """
    Convert a specs dictionary (human-readable keys) to an ordered tuple of values
    matching SPEC_COLUMN_SUFFIXES order. Unmatched fields are None.
    """
    # Build suffix -> value map
    suffix_values = {}
    for spec_name, value in specs_dict.items():
        suffix = name_map.get(spec_name)
        if suffix:
            suffix_values[suffix] = value

    # Return in column order
    return tuple(suffix_values.get(s) for s in SPEC_COLUMN_SUFFIXES)


def returned_specs_to_column_values(returned_specs):
    """
    Convert returned_specs dict (already keyed by column suffix from OCR processor)
    to an ordered tuple matching SPEC_COLUMN_SUFFIXES order.
    """
    if not returned_specs:
        return tuple(None for _ in SPEC_COLUMN_SUFFIXES)
    return tuple(returned_specs.get(s) for s in SPEC_COLUMN_SUFFIXES)


def load_file(conn, table_name, data, run_id):
    """Load a single processed JSON file into the database."""
    vendor = data['vendor']
    has_spec_columns = vendor in VENDORS_WITH_SPEC_COLUMNS
    file_meta = parse_filename_metadata(data['source_file'])

    # Determine spec column values (Ross/Dazpak only)
    requested_specs = tuple(None for _ in SPEC_COLUMN_SUFFIXES)
    returned_specs = tuple(None for _ in SPEC_COLUMN_SUFFIXES)

    if has_spec_columns:
        spec_type = data.get('spec_type')
        if spec_type == 'requested' and data.get('specifications'):
            requested_specs = specs_dict_to_column_values(data['specifications'])
        if data.get('returned_specs'):
            returned_specs = returned_specs_to_column_values(data['returned_specs'])

    # Vendor-extracted structured data (all vendors)
    vendor_extracted = data.get('vendor_extracted', {})
    vendor_cols = VENDOR_EXTRACTED_COLUMNS.get(vendor, [])
    vendor_values = tuple(vendor_extracted.get(col) for col in vendor_cols)

    rows = []
    for page_data in data['pages']:
        # Build base row tuple
        base = (
            data['source_file'],
            vendor,
            file_meta['email_date'],
            file_meta['email_subject'],
            None,  # email_from
        )

        # Row 1: Full raw OCR text for this page (field_key = NULL)
        ocr_fields = (
            page_data['raw_text'],
            None,  # field_key
            None,  # field_value
            None,  # field_line_num
            None,  # field_confidence
        )

        metadata = (
            data['file_type'],
            data['file_size_bytes'],
            data['ocr_engine'],
            data['ocr_version'],
            page_data.get('page', 1),
            run_id,
            'raw',
            None  # error_message
        )

        if has_spec_columns:
            rows.append(base + requested_specs + returned_specs + ocr_fields + metadata + vendor_values)
        else:
            rows.append(base + ocr_fields + metadata + vendor_values)

        # Row per extracted field line
        for field in page_data.get('fields', []):
            text = field['text']
            key, value = split_key_value(text)

            field_values = (
                None,  # raw_ocr_text (only on full-text row)
                key,
                value,
                field['line_num'],
                field.get('confidence'),
            )

            if has_spec_columns:
                rows.append(base + requested_specs + returned_specs + field_values + metadata + vendor_values)
            else:
                rows.append(base + field_values + metadata + vendor_values)

    # Build INSERT SQL
    vendor_col_names = ', '.join(vendor_cols) if vendor_cols else ''
    vendor_col_sql = f', {vendor_col_names}' if vendor_col_names else ''

    if has_spec_columns:
        requested_cols = ', '.join(f'requested_spec_{s}' for s in SPEC_COLUMN_SUFFIXES)
        returned_cols = ', '.join(f'returned_spec_{s}' for s in SPEC_COLUMN_SUFFIXES)
        insert_sql = f"""
            INSERT INTO {table_name} (
                source_file, source_vendor, email_date, email_subject, email_from,
                {requested_cols},
                {returned_cols},
                raw_ocr_text, field_key, field_value, field_line_num, field_confidence,
                file_type, file_size_bytes, ocr_engine, ocr_version, page_number,
                processing_run, status, error_message
                {vendor_col_sql}
            ) VALUES %s
        """
    else:
        insert_sql = f"""
            INSERT INTO {table_name} (
                source_file, source_vendor, email_date, email_subject, email_from,
                raw_ocr_text, field_key, field_value, field_line_num, field_confidence,
                file_type, file_size_bytes, ocr_engine, ocr_version, page_number,
                processing_run, status, error_message
                {vendor_col_sql}
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

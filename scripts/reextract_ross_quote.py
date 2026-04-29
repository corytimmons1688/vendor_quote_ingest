#!/usr/bin/env python3
"""
Re-run extract_ross() over raw_ocr_text already stored in est_ex_br_ross
and UPDATE the vendor-extracted columns on those rows. Sibling of
reextract_dazpak_quote.py.

Usage:
    NEON_DATABASE_URL=... python3 scripts/reextract_ross_quote.py \
        --where-raw "returned_spec_zipper = 'Other:'"      [--apply]
    NEON_DATABASE_URL=... python3 scripts/reextract_ross_quote.py \
        --estimate-number 88874                            [--apply]
    NEON_DATABASE_URL=... python3 scripts/reextract_ross_quote.py \
        --source-file <name>                               [--apply]
"""
import argparse
import json
import os
import sys
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).parent))
from vendor_extractors import extract_ross  # noqa: E402


ROSS_EXTRACTED_COLS = [
    'estimate_number', 'application', 'product_size', 'colors', 'materials',
    'finishing', 'pricing_json', 'plate_cost', 'lead_time', 'quote_date',
    'quote_validity',
]
RETURNED_SPEC_SUFFIXES = [
    'bag', 'size', 'substrate', 'finish', 'embellishment',
    'fill_style', 'seal_type', 'gusset',
    'zipper', 'tear_notch', 'hole_punch', 'corners',
    'quantities',
]
# Columns extract_ross may *clear* (e.g., 'Other:' → NULL). For these we
# overwrite even when the new value is None, so we can wipe stale data.
RETURNED_SPECS_OVERWRITE = {'zipper', 'corners', 'tear_notch', 'hole_punch',
                            'gusset', 'seal_type'}


def summarize_pricing(pj):
    if not pj:
        return '(null)'
    try:
        rows = pj if isinstance(pj, list) else json.loads(pj)
    except (TypeError, ValueError):
        return f'(unparseable: {str(pj)[:120]}…)'
    return ' | '.join(
        f"qty={r.get('quantity')} pe={r.get('price_each')}" for r in rows[:6]
    ) + (' …' if len(rows) > 6 else '')


def main():
    ap = argparse.ArgumentParser()
    sel = ap.add_mutually_exclusive_group(required=True)
    sel.add_argument('--estimate-number')
    sel.add_argument('--source-file')
    sel.add_argument('--ingestion-id')
    sel.add_argument('--where-raw', metavar='SQL')
    ap.add_argument('--apply', action='store_true')
    args = ap.parse_args()

    if args.estimate_number:
        where = 'estimate_number = %s'
        params = (args.estimate_number,)
    elif args.source_file:
        where = 'source_file = %s'
        params = (args.source_file,)
    elif args.ingestion_id:
        where = 'ingestion_id = %s'
        params = (args.ingestion_id,)
    else:
        where = args.where_raw
        params = ()

    conn = psycopg2.connect(os.environ['NEON_DATABASE_URL'], sslmode='require')
    conn.autocommit = False

    select_cols = (
        ['ingestion_id', 'source_file', 'raw_ocr_text', 'status', 'error_message']
        + ROSS_EXTRACTED_COLS
        + [f'returned_spec_{s}' for s in RETURNED_SPEC_SUFFIXES]
    )
    select_sql = (
        f"SELECT {', '.join(select_cols)} FROM est_ex_br_ross "
        f"WHERE {where} ORDER BY source_file"
    )

    with conn.cursor() as cur:
        if params:
            cur.execute(select_sql, params)
        else:
            cur.execute(select_sql)
        rows = cur.fetchall()

    if not rows:
        print('No rows matched.')
        return 0

    print(f'Matched {len(rows)} row(s). Mode: {"APPLY" if args.apply else "DRY-RUN"}')
    print('=' * 78)

    updated = 0
    for row in rows:
        ing_id = row[0]
        source_file = row[1]
        raw = row[2]
        before = {
            'status': row[3],
            'error_message': row[4],
            **dict(zip(ROSS_EXTRACTED_COLS, row[5:5 + len(ROSS_EXTRACTED_COLS)])),
            **{
                f'returned_spec_{s}': v
                for s, v in zip(
                    RETURNED_SPEC_SUFFIXES,
                    row[5 + len(ROSS_EXTRACTED_COLS):],
                )
            },
        }

        print(f'\n[{ing_id}] {source_file[:75]}')
        if not raw:
            print('  raw_ocr_text empty — skipping')
            continue

        ext = extract_ross(raw)
        ext_returned = {
            k.replace('returned_spec_', ''): v
            for k, v in ext.items()
            if k.startswith('returned_spec_')
        }

        # Build set/clauses
        set_clauses = []
        update_params = []
        change_log = []

        for col in ROSS_EXTRACTED_COLS:
            new = ext.get(col)
            old = before.get(col)
            if new != old:
                set_clauses.append(f'{col} = %s')
                update_params.append(new)
                if col == 'pricing_json':
                    change_log.append(
                        f'  pricing_json: '
                        f'before={summarize_pricing(old)} after={summarize_pricing(new)}'
                    )
                else:
                    change_log.append(f'  {col}: {old!r} -> {new!r}')

        # Status / error_message — overwrite so the parser can both add and clear
        new_status = ext.get('status', 'raw')
        new_err = ext.get('error_message')
        if new_status != before['status']:
            set_clauses.append('status = %s')
            update_params.append(new_status)
            change_log.append(f'  status: {before["status"]!r} -> {new_status!r}')
        if new_err != before['error_message']:
            set_clauses.append('error_message = %s')
            update_params.append(new_err)
            change_log.append(f'  error_message: {before["error_message"]!r} -> {new_err!r}')

        # Returned specs: fill nulls always; for the OVERWRITE set, also
        # actively clear stale junk like 'Other:'.
        for suffix in RETURNED_SPEC_SUFFIXES:
            old = before.get(f'returned_spec_{suffix}')
            new = ext_returned.get(suffix)
            if suffix in RETURNED_SPECS_OVERWRITE:
                # Overwrite when extractor result differs (incl. None to clear).
                if new != old:
                    set_clauses.append(f'returned_spec_{suffix} = %s')
                    update_params.append(new)
                    change_log.append(
                        f'  returned_spec_{suffix}: {old!r} -> {new!r}'
                    )
            else:
                # Fill-null only.
                if new and not old:
                    set_clauses.append(f'returned_spec_{suffix} = %s')
                    update_params.append(new)
                    change_log.append(
                        f'  returned_spec_{suffix} (fill-null): {old!r} -> {new!r}'
                    )

        if not set_clauses:
            print('  no changes')
            continue

        for line in change_log:
            print(line)

        if not args.apply:
            continue

        update_params.append(ing_id)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE est_ex_br_ross SET " + ", ".join(set_clauses) +
                " WHERE ingestion_id = %s",
                update_params,
            )
        updated += 1

    if args.apply:
        conn.commit()
        print('\n' + '=' * 78)
        print(f'Committed {updated} update(s).')
    else:
        conn.rollback()
        print('\n' + '=' * 78)
        print('Dry run — no changes written. Re-run with --apply.')
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())

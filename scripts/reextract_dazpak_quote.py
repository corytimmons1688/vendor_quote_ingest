#!/usr/bin/env python3
"""
Re-run the Dazpak vendor extractor over raw_ocr_text already stored in
est_ex_br_dazpak and UPDATE the vendor-extracted columns on those rows.

Does NOT re-OCR and does NOT touch raw_ocr_text. Useful after a fix to
vendor_extractors.extract_dazpak() when re-running the full backfill would
be overkill.

Usage:
    # Dry run (default): prints before/after for every matching row
    NEON_DATABASE_URL=... python3 scripts/reextract_dazpak_quote.py \
        --quote-number 14511

    # Apply the update
    NEON_DATABASE_URL=... python3 scripts/reextract_dazpak_quote.py \
        --quote-number 14511 --apply

Selectors (choose one):
    --quote-number TEXT   Match rows where quote_number = TEXT (plus source_file LIKE %TEXT%)
    --source-file TEXT    Match rows where source_file = TEXT (exact)
    --ingestion-id UUID   Match a single row by PK
"""

import argparse
import json
import os
import sys
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).parent))
from vendor_extractors import extract_dazpak  # noqa: E402


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


def summarize_pricing(pricing_json):
    if not pricing_json:
        return '(null)'
    try:
        rows = json.loads(pricing_json)
    except (TypeError, ValueError):
        return f'(unparseable: {str(pricing_json)[:120]}…)'
    return ' | '.join(
        f"qty={r.get('quantity')} pmi={r.get('price_per_m_imps')} "
        f"pmsi={r.get('price_per_msi')} pe={r.get('price_each')}"
        for r in rows
    )


def main():
    ap = argparse.ArgumentParser()
    sel = ap.add_mutually_exclusive_group(required=True)
    sel.add_argument('--quote-number')
    sel.add_argument('--source-file')
    sel.add_argument('--ingestion-id')
    sel.add_argument('--where-raw', metavar='SQL',
                     help='Raw WHERE clause (e.g. "file_type=\'html\' AND returned_spec_zipper IS NULL")')
    ap.add_argument('--apply', action='store_true',
                    help='Actually UPDATE rows (default: dry run)')
    args = ap.parse_args()

    if args.quote_number:
        where = 'quote_number = %s OR source_file ILIKE %s'
        params = (args.quote_number, f'%{args.quote_number}%')
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

    select_cols = ['ingestion_id', 'source_file', 'raw_ocr_text'] + DAZPAK_EXTRACTED_COLS
    select_sql = (
        f"SELECT {', '.join(select_cols)} FROM est_ex_br_dazpak "
        f"WHERE {where} ORDER BY source_file"
    )

    with conn.cursor() as cur:
        # When --where-raw is used, the clause may contain literal `%` (LIKE
        # patterns) which psycopg2 would otherwise treat as a parameter
        # placeholder. Skip param substitution entirely in that case.
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
        ingestion_id = row[0]
        source_file = row[1]
        raw_text = row[2]
        before = dict(zip(DAZPAK_EXTRACTED_COLS, row[3:]))

        print(f'\n[{ingestion_id}] {source_file}')

        if not raw_text:
            print('  raw_ocr_text is empty — skipping')
            continue

        extracted = extract_dazpak(raw_text)

        after = {col: extracted.get(col) for col in DAZPAK_EXTRACTED_COLS}
        returned_spec_updates = {
            k.replace('returned_spec_', ''): v
            for k, v in extracted.items()
            if k.startswith('returned_spec_')
        }

        # Show only fields that actually change
        changed = [c for c in DAZPAK_EXTRACTED_COLS if before.get(c) != after.get(c)]
        if not changed and not returned_spec_updates:
            print('  no changes')
            continue

        for col in changed:
            if col == 'pricing_json':
                print('  pricing_json:')
                print('     before:', summarize_pricing(before.get(col)))
                print('     after :', summarize_pricing(after.get(col)))
            else:
                print(f'  {col}: {before.get(col)!r} -> {after.get(col)!r}')

        if returned_spec_updates:
            print(f'  returned_spec_* (will fill nulls only): {returned_spec_updates}')

        if not args.apply:
            continue

        set_clauses = [f'{c} = %s' for c in DAZPAK_EXTRACTED_COLS]
        update_params = [after.get(c) for c in DAZPAK_EXTRACTED_COLS]

        # Merge returned_spec_* too (fill NULLs only, matching neon_loader behavior)
        if returned_spec_updates:
            merge_cols = [f'returned_spec_{s}' for s in RETURNED_SPEC_SUFFIXES]
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT {', '.join(merge_cols)} FROM est_ex_br_dazpak "
                    f"WHERE ingestion_id = %s",
                    (ingestion_id,),
                )
                existing = cur.fetchone() or (None,) * len(merge_cols)
            for suffix, current in zip(RETURNED_SPEC_SUFFIXES, existing):
                new_val = returned_spec_updates.get(suffix)
                if new_val and not current:
                    set_clauses.append(f'returned_spec_{suffix} = %s')
                    update_params.append(new_val)

        update_params.append(ingestion_id)
        update_sql = (
            f"UPDATE est_ex_br_dazpak SET {', '.join(set_clauses)} "
            f"WHERE ingestion_id = %s"
        )
        with conn.cursor() as cur:
            cur.execute(update_sql, update_params)
        updated += 1
        print('  UPDATED')

    if args.apply:
        conn.commit()
        print('\n' + '=' * 78)
        print(f'Committed {updated} update(s).')
    else:
        conn.rollback()
        print('\n' + '=' * 78)
        print('Dry run — no changes written. Re-run with --apply to commit.')

    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())

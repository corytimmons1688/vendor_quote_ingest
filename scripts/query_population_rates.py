#!/usr/bin/env python3
"""Report column population rates for a vendor bronze table."""

import os
import sys
import psycopg2


def main():
    vendor = sys.argv[1] if len(sys.argv) > 1 else 'Ross'
    table_map = {'Ross': 'est_bnz_ross', 'Tedpack': 'est_bnz_tedpack', 'Dazpak': 'est_bnz_dazpak'}
    table = table_map.get(vendor)
    if not table:
        print(f"Unknown vendor: {vendor}")
        sys.exit(1)

    conn = psycopg2.connect(os.environ['NEON_DATABASE_URL'], sslmode='require')
    cur = conn.cursor()

    # Get column data types
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
    """, (table,))
    col_info = cur.fetchall()
    col_types = {row[0]: row[1] for row in col_info}
    columns = [row[0] for row in col_info]

    # Count rows by type
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    total = cur.fetchone()[0]

    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE source_file LIKE '%%requested_specs%%'")
    requested_rows = cur.fetchone()[0]

    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE source_file NOT LIKE '%%requested_specs%%'")
    returned_rows = cur.fetchone()[0]

    print(f"\n{'='*65}")
    print(f"  {vendor} ({table})")
    print(f"  {total} total rows | {requested_rows} spec files | {returned_rows} PDF files")
    print(f"{'='*65}\n")

    skip = {'ingestion_id', 'ingested_at', 'processing_run', 'status', 'error_message',
            'source_vendor', 'ocr_engine', 'ocr_version'}

    # Legacy columns from migration 001 that aren't used by the loader
    legacy = {'requested_spec_material', 'requested_spec_gusset_style',
              'requested_spec_gusset_details', 'requested_spec_printing_method',
              'returned_spec_material', 'returned_spec_gusset_style',
              'returned_spec_gusset_details', 'returned_spec_printing_method'}

    def count_populated(col, where_extra=""):
        where = f"WHERE {where_extra} AND " if where_extra else "WHERE "
        if col_types.get(col) in ('text', 'character varying'):
            cur.execute(f"SELECT COUNT(*) FROM {table} {where}{col} IS NOT NULL AND TRIM({col}) != ''")
        else:
            cur.execute(f"SELECT COUNT(*) FROM {table} {where}{col} IS NOT NULL")
        return cur.fetchone()[0]

    def print_group(title, items, denom):
        if not items:
            return
        print(f"  {title} (out of {denom} possible rows)")
        print(f"  {'-'*58}")
        for col, populated in items:
            pct = (populated / denom * 100) if denom > 0 else 0
            bar = '#' * int(pct / 5) + '.' * (20 - int(pct / 5))
            print(f"  {col:40s} {populated:4d}/{denom:<4d} {pct:5.1f}%  [{bar}]")
        print()

    # Requested specs — only count against spec file rows
    req_cols = [(c, count_populated(c, "source_file LIKE '%%requested_specs%%'"))
                for c in columns if c.startswith('requested_spec_') and c not in skip and c not in legacy]

    # Returned specs — only count against PDF rows
    ret_cols = [(c, count_populated(c, "source_file NOT LIKE '%%requested_specs%%'"))
                for c in columns if c.startswith('returned_spec_') and c not in skip and c not in legacy]

    # Vendor-extracted — only against PDF rows
    vendor_extracted = [(c, count_populated(c, "source_file NOT LIKE '%%requested_specs%%'"))
                        for c in columns
                        if not c.startswith('requested_spec_') and not c.startswith('returned_spec_')
                        and c not in skip and c not in legacy]

    print_group("REQUESTED SPECS — from outbound email", req_cols, requested_rows)
    print_group("RETURNED SPECS — from vendor PDF", ret_cols, returned_rows)
    print_group("BASE / VENDOR-EXTRACTED — from PDF", vendor_extracted, returned_rows)

    cur.close()
    conn.close()


if __name__ == '__main__':
    main()

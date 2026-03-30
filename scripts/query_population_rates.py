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

    # Get all columns
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
    """, (table,))
    columns = [row[0] for row in cur.fetchall()]

    # Get total row count
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    total = cur.fetchone()[0]
    print(f"\n{'='*60}")
    print(f"  {vendor} ({table}) — {total} total rows")
    print(f"{'='*60}\n")

    # Skip metadata columns we don't care about
    skip = {'ingestion_id', 'ingested_at', 'processing_run', 'status', 'error_message',
            'source_vendor', 'ocr_engine', 'ocr_version'}

    # Query population for each column
    results = []
    for col in columns:
        if col in skip:
            continue
        cur.execute(f"""
            SELECT COUNT(*) FROM {table}
            WHERE {col} IS NOT NULL AND TRIM({col}) != ''
        """)
        populated = cur.fetchone()[0]
        pct = (populated / total * 100) if total > 0 else 0
        results.append((col, populated, pct))

    # Group by category
    requested = [(c, p, pct) for c, p, pct in results if c.startswith('requested_spec_')]
    returned = [(c, p, pct) for c, p, pct in results if c.startswith('returned_spec_')]
    vendor_cols = [(c, p, pct) for c, p, pct in results
                   if not c.startswith('requested_spec_') and not c.startswith('returned_spec_')]

    def print_group(title, items):
        if not items:
            return
        print(f"  {title}")
        print(f"  {'-'*50}")
        for col, populated, pct in items:
            bar = '#' * int(pct / 5) + '.' * (20 - int(pct / 5))
            print(f"  {col:40s} {populated:4d}/{total}  {pct:5.1f}%  [{bar}]")
        print()

    print_group("BASE COLUMNS", vendor_cols)
    print_group("REQUESTED SPECS (from outbound email)", requested)
    print_group("RETURNED SPECS (from vendor PDF)", returned)

    cur.close()
    conn.close()


if __name__ == '__main__':
    main()

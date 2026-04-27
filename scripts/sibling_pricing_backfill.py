#!/usr/bin/env python3
"""
Phase 1: Backfill orphan Dazpak pricing rows by copying quantities from a
sibling row that has the same quote_number and whose parsed price tuples
contain the orphan's price tuples as a contiguous subsequence.

Touches only pricing_json (and returned_spec_quantities). No other columns.

Usage:
    NEON_DATABASE_URL=... python3 scripts/sibling_pricing_backfill.py [--apply]
"""
import argparse
import json
import os
import re
import sys
from collections import defaultdict

import psycopg2

# Allow a "comma ghost" — when OCR drops the quantity digits but keeps the
# group separator, the line looks like "Impressions , $157.12 ..." instead
# of "Impressions 70,000 $157.12 ...".
PRICE_LINE_6 = re.compile(
    r'(?:Impressions\s+)?[,\s]*\$?([\d.]+)\s+\$?([\d.]+)\s+\$?([\d.]+)'
    r'\s+\$?([\d.]+)\s+\$?([\d.]+)\s+\$?([\d.]+)\s*$'
)
PRICE_LINE_3 = re.compile(
    r'(?:Impressions\s+)?[,\s]*\$?([\d.]+)\s+\$?([\d.]+)\s+\$?([\d.]+)\s*$'
)


def extract_orphan_prices(txt):
    """Extract (pmi, pmsi, pe) tuples from price-only lines (no quantity column)."""
    out = []
    for line in txt.split('\n'):
        s = line.strip()
        if re.match(r'\d[\d,]+\s', s):  # has a leading quantity → not an orphan line
            continue
        m6 = PRICE_LINE_6.match(s)
        m3 = PRICE_LINE_3.match(s)
        m = m6 or m3
        if not m:
            continue
        try:
            pmi, pmsi, pe = float(m.group(1)), float(m.group(2)), float(m.group(3))
        except ValueError:
            continue
        if 1 < pmi < 10000 and 0.01 < pe < 50 and pmsi < 100:
            out.append((m.group(1), m.group(2), m.group(3)))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true')
    args = ap.parse_args()

    conn = psycopg2.connect(os.environ['NEON_DATABASE_URL'], sslmode='require')
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute(
        "SELECT ingestion_id, quote_number, source_file, pricing_json, "
        "returned_spec_quantities, raw_ocr_text "
        "FROM est_ex_br_dazpak "
        "WHERE quote_number IS NOT NULL"
    )
    rows = cur.fetchall()

    by_q = defaultdict(list)
    for r in rows:
        by_q[r[1]].append(r)

    updates = []
    for q, qrows in by_q.items():
        siblings = []
        for r in qrows:
            if r[3] is None:
                continue
            pricing = r[3] if isinstance(r[3], list) else json.loads(r[3])
            tuples = [
                (p.get('price_per_m_imps'), p.get('price_per_msi'), p.get('price_each'))
                for p in pricing
            ]
            qtys = [p.get('quantity') for p in pricing]
            siblings.append((tuples, qtys, r[2]))
        if not siblings:
            continue
        for r in qrows:
            ing_id, _, sf, pj, _, txt = r
            if pj is not None or not txt:
                continue
            oprices = extract_orphan_prices(txt)
            if len(oprices) < 2:
                continue
            # Find a sibling whose prices contain oprices as contiguous subsequence
            for sib_tuples, sib_qtys, sib_sf in siblings:
                for start in range(len(sib_tuples) - len(oprices) + 1):
                    if sib_tuples[start:start + len(oprices)] == oprices:
                        new_pricing = [
                            {
                                'quantity': sib_qtys[start + i],
                                'price_per_m_imps': oprices[i][0],
                                'price_per_msi':   oprices[i][1],
                                'price_each':      oprices[i][2],
                            }
                            for i in range(len(oprices))
                        ]
                        new_qty_str = ', '.join(p['quantity'] for p in new_pricing)
                        updates.append((ing_id, sf, sib_sf, new_pricing, new_qty_str))
                        break
                else:
                    continue
                break

    print(f'Phase 1 candidates: {len(updates)} orphan(s) fixable via sibling copy')
    print('=' * 78)
    for ing_id, sf, sib_sf, pricing, qstr in updates:
        print(f'\n[{ing_id}] {sf[:80]}')
        print(f'   donor sibling: {sib_sf[:80]}')
        print(f'   new pricing ({len(pricing)} tiers): {qstr}')
        for p in pricing:
            print(f'     qty={p["quantity"]:>10}  pmi=${p["price_per_m_imps"]}  pmsi=${p["price_per_msi"]}  pe=${p["price_each"]}')

    if not args.apply:
        print('\nDry run. Re-run with --apply to commit.')
        return 0

    for ing_id, _, _, pricing, qstr in updates:
        cur.execute(
            "UPDATE est_ex_br_dazpak "
            "SET pricing_json = %s::jsonb, "
            "    returned_spec_quantities = COALESCE(returned_spec_quantities, %s) "
            "WHERE ingestion_id = %s AND pricing_json IS NULL",
            (json.dumps(pricing), qstr, ing_id),
        )
    conn.commit()
    print(f'\nCommitted {len(updates)} updates.')
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())

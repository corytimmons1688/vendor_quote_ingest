"""
Vendor-specific field extractors for structured quote data.
Each vendor has a unique PDF/HTML format — these parsers extract
specs, pricing, and metadata into column-ready dictionaries.
"""

import json
import re
from collections import Counter


def _parse_qty_num(q):
    """Normalize quantity string like '4.2K', '50K', '12.5M' to a float for sorting."""
    q_clean = q.strip().upper()
    multiplier = 1
    if q_clean.endswith('K'):
        multiplier = 1_000
        q_clean = q_clean[:-1]
    elif q_clean.endswith('M'):
        multiplier = 1_000_000
        q_clean = q_clean[:-1]
    try:
        return float(q_clean.replace(',', '')) * multiplier
    except ValueError:
        return 0


def _rejoin_split_numbers(text):
    """
    Fix OCR artefact where a comma-formatted number is split across two lines.
    e.g. "10,\n000 $0.652..." → "10,000 $0.652..."
    Handles thousands-comma splits (digit(s) + comma + newline + exactly 3 digits).
    """
    return re.sub(r'(\d+,)\n(\d{3})\b', r'\1\2', text)


# ============================================================
# TEDPACK — HTML email body
# ============================================================
# Specs: "Bag: ...", "Size: ...", "Substrate: ..." etc.
# Pricing: "1 SKU = 5K = $0.249/PCS" or "5K = $0.017/PCS"
# Sections: "Delivery Air Price to UT:", "Delivery Ocean Cost to UT:",
#           "Factory Price:", "Air shipping cost", "Ocean shipping cost"
# Plate cost: "Printing plate cost: $120/color"
# Lead time: "Lead time for air is 35 days, 55 days for ocean shipping."
# Print method: "Digital" in body = Digital, "Plate Cost" = Rotogravure

TEDPACK_SPEC_FIELDS = {
    'bag': 'spec_bag',
    'size': 'spec_size',
    'substrate': 'spec_substrate',
    'finish': 'spec_finish',
    'embellishment': 'spec_embellishment',
    'fill style': 'spec_fill_style',
    'seal type': 'spec_seal_type',
    'gusset style': 'spec_gusset',
    'gusset details': 'spec_gusset',
    'zipper': 'spec_zipper',
    'tear notch': 'spec_tear_notch',
    'hole punch': 'spec_hole_punch',
    'corners': 'spec_corners',
    'quantities': 'spec_quantities',
}


def extract_tedpack(text):
    """Extract specs and pricing from Tedpack email body text."""
    result = {}
    lines = text.split('\n')

    # --- Specs (strict: field at start of line) ---
    for line in lines:
        trimmed = line.strip()
        if not trimmed:
            continue
        for field_name, col_name in TEDPACK_SPEC_FIELDS.items():
            pattern = re.compile(rf'^{re.escape(field_name)}\s*[:\-]\s*(.+)', re.IGNORECASE)
            m = pattern.match(trimmed)
            if m:
                result[col_name] = m.group(1).strip()
                break

    # --- Loose extraction fallback (Issue #5) ---
    # If strict extraction found fewer than ~50% of expected spec fields, try
    # loose patterns that match field names anywhere in the line.
    expected_field_count = len(TEDPACK_SPEC_FIELDS)  # 13
    found_spec_count = sum(1 for col in TEDPACK_SPEC_FIELDS.values() if col in result)
    if found_spec_count < expected_field_count * 0.5:
        for line in lines:
            trimmed = line.strip()
            if not trimmed:
                continue
            for field_name, col_name in TEDPACK_SPEC_FIELDS.items():
                if col_name in result:
                    continue  # already have this field
                loose_pattern = re.compile(
                    rf'{re.escape(field_name)}\s*[:\-]\s*(.+)', re.IGNORECASE
                )
                m = loose_pattern.search(trimmed)
                if m:
                    result[col_name] = m.group(1).strip()

    # --- Print method ---
    lower_text = text.lower()
    if 'plate cost' in lower_text or 'plate' in lower_text:
        result['print_method'] = 'Rotogravure'
    elif re.search(r'printing\s*method\s*:\s*digital|digital\s+print', lower_text):
        result['print_method'] = 'Digital'
    else:
        result['print_method'] = 'Unknown'

    # --- Quote ID (from Bag field, e.g. "CQ-50448912933 - Planet Buds") ---
    bag = result.get('spec_bag', '')
    cq_match = re.search(r'(CQ-\d+)', bag)
    if cq_match:
        result['quote_id'] = cq_match.group(1)

    # --- Pricing ---
    pricing = {}
    current_section = 'default'

    for line in lines:
        trimmed = line.strip()
        lower = trimmed.lower()

        # Detect pricing sections
        # "and Duty" suffix = DDP (delivered duty paid) = delivered price, not freight
        # "Delivery Sea Price to UT" = ocean delivered (Vireo Health emails)
        if re.search(r'delivery\s+air|air\s+price', lower):
            current_section = 'air_delivered'
            continue
        elif re.search(r'air\s+shipping\s+cost\s+and\s+duty', lower):
            current_section = 'air_delivered'
            continue
        elif re.search(r'air\s+shipping\s+cost', lower):
            current_section = 'air_shipping'
            continue
        elif re.search(r'delivery\s+(?:ocean|sea)|(?:ocean|sea)\s+(?:cost|price)\s+to', lower):
            current_section = 'ocean_delivered'
            continue
        elif re.search(r'(?:ocean|sea)\s+shipping\s+cost\s+and\s+duty', lower):
            current_section = 'ocean_delivered'
            continue
        elif re.search(r'(?:ocean|sea)\s+shipping\s+cost', lower):
            current_section = 'ocean_shipping'
            continue
        elif re.search(r'factory\s+price', lower):
            current_section = 'factory'
            continue

        # Match pricing lines: "1 SKU = 5K = $0.249/PCS" or "5K = $0.017/PCS" or "50K = $1,500"
        price_match = re.match(
            r'(?:\d+\s*SKU\s*=\s*)?(\d+[\d,.]*[KkMm]?)\s*=\s*\$?([\d,.]+)(?:/(\w+))?',
            trimmed
        )
        if price_match:
            qty_str = price_match.group(1).strip()
            price_str = price_match.group(2).strip()
            unit = price_match.group(3) or ''

            if current_section not in pricing:
                pricing[current_section] = []
            pricing[current_section].append({
                'quantity': qty_str,
                'price': price_str,
                'unit': unit
            })

    # --- Post-extraction: validate shipping sections (Q13/Q14) ---
    # Per-piece prices (unit=PCS, price < $5) in shipping sections are misrouted delivered prices
    for ship_sec, deliv_sec in [('air_shipping', 'air_delivered'), ('ocean_shipping', 'ocean_delivered')]:
        if ship_sec in pricing:
            misrouted = []
            kept = []
            for entry in pricing[ship_sec]:
                price_val = float(entry.get('price', '0').replace(',', ''))
                unit = entry.get('unit', '').upper()
                if 'PCS' in unit or (price_val < 5.0 and unit):
                    misrouted.append(entry)
                else:
                    kept.append(entry)
            if misrouted:
                if deliv_sec not in pricing:
                    pricing[deliv_sec] = []
                pricing[deliv_sec].extend(misrouted)
                pricing[ship_sec] = kept
                if not kept:
                    del pricing[ship_sec]

    # --- Dual-price detection (Q4) ---
    for section_name in list(pricing.keys()):
        if section_name.startswith('_'):
            continue
        entries = pricing[section_name]
        if not isinstance(entries, list) or len(entries) < 2:
            continue
        qty_counts = Counter(e.get('quantity') for e in entries)
        repeated = {q: c for q, c in qty_counts.items() if c >= 2}
        if repeated:
            pricing[f'_{section_name}_dual_price'] = True
            pricing[f'_{section_name}_repeat_count'] = max(repeated.values())

    # --- High-quantity default section flagging (Q15) ---
    for entry in pricing.get('default', []):
        qty_num = _parse_qty_num(entry.get('quantity', '0'))
        if qty_num >= 1_000_000:
            pricing['_default_catalogue_suspected'] = True
            break

    if pricing:
        result['pricing_json'] = json.dumps(pricing)

    # --- Derive quantities from pricing tiers if not explicitly found ---
    if not result.get('spec_quantities') and pricing:
        all_qtys = set()
        for section_prices in pricing.values():
            if isinstance(section_prices, list):
                for entry in section_prices:
                    if isinstance(entry, dict) and 'quantity' in entry:
                        all_qtys.add(entry['quantity'])
        if all_qtys:
            sorted_qtys = sorted(all_qtys, key=_parse_qty_num)
            result['spec_quantities'] = ', '.join(sorted_qtys)
            result['returned_spec_quantities'] = result['spec_quantities']

    # --- Plate cost ---
    plate_match = re.search(r'(?:printing\s*)?plate\s*cost[:\s]*\$?([\d,.]+\s*/\s*\w+)', text, re.IGNORECASE)
    if plate_match:
        result['plate_cost'] = plate_match.group(1).strip()

    # --- Lead time ---
    lead_match = re.search(r'lead\s*time[^.]*\.', text, re.IGNORECASE)
    if lead_match:
        result['lead_time'] = lead_match.group(0).strip()

    # --- Material-line substrate fallback (Q5/Q7) ---
    # When Substrate says "Custom Substrate", extract actual substrate from Material line.
    # Format: "Material: {finish_oil}+{substrate}/{backing}-{thickness}"
    # e.g. "Matte Oil+ PET/METPET/PE -4mil" → substrate = METPET
    substrate_val = result.get('spec_substrate', '')
    if 'custom substrate' in substrate_val.lower():
        mat_match = re.search(r'Material\s*:\s*(.+)', text, re.IGNORECASE)
        if mat_match:
            mat_line = mat_match.group(1).strip()
            # Everything after "+" is substrate/backing structure
            plus_idx = mat_line.find('+')
            if plus_idx >= 0:
                structure = mat_line[plus_idx + 1:].strip()
                # Extract substrate keyword before "/" (backing separator)
                # Known substrates: METPET, PET, ALOX-PET, CLR PET, BOPP, NYLON
                sub_match = re.match(
                    r'\s*(METPET|MET\s*PET|ALOX[- ]?PET|CLR\s*PET|PET|BOPP|NYLON)',
                    structure, re.IGNORECASE
                )
                if sub_match:
                    result['returned_spec_substrate'] = sub_match.group(1).strip()

    # --- Copy spec_* fields to returned_spec_* ---
    # Only copy if returned_spec_* wasn't already set by a fallback (e.g. Material-line)
    spec_to_returned = {
        'spec_bag': 'returned_spec_bag',
        'spec_size': 'returned_spec_size',
        'spec_substrate': 'returned_spec_substrate',
        'spec_finish': 'returned_spec_finish',
        'spec_embellishment': 'returned_spec_embellishment',
        'spec_fill_style': 'returned_spec_fill_style',
        'spec_seal_type': 'returned_spec_seal_type',
        'spec_gusset': 'returned_spec_gusset',
        'spec_zipper': 'returned_spec_zipper',
        'spec_tear_notch': 'returned_spec_tear_notch',
        'spec_hole_punch': 'returned_spec_hole_punch',
        'spec_corners': 'returned_spec_corners',
        'spec_quantities': 'returned_spec_quantities',
    }
    for spec_key, returned_key in spec_to_returned.items():
        if spec_key in result and returned_key not in result:
            result[returned_key] = result[spec_key]

    # --- ALOX PET flagging (Q10) ---
    substrate = result.get('returned_spec_substrate', '') or result.get('spec_substrate', '')
    if re.search(r'alox', substrate, re.IGNORECASE):
        result['status'] = 'flagged'
        result['error_message'] = 'ALOX PET - excluded from training'

    return result


# ============================================================
# ROSS — OCR'd PDF
# ============================================================
# Application- FL-CQ-0687 BUCKEYE RELIEF POUCHES_4V
# Product Size- 5.00 (W) X 6.50 (H) X 2.50 (G)
# Colors- 4/COLOR PROCESS + SPOT WHITE + SPOT GLOSS
# Materials- Stock# 3905 ... / Stock# 5309 ...
# Finishing- Seal Width: 3/8" 2 Side Seal Tear Notch: 2 ...
# Estimate No. 86598
# Quantity/Price table
# Non-Recurring Preparation Charges- $760.00 SPOT VARNISH PLATES

def extract_ross(text):
    """Extract specs and pricing from Ross OCR'd PDF text."""
    text = _rejoin_split_numbers(text)
    result = {}

    # --- Estimate number ---
    est_match = re.search(r'Estimate\s*No\.?\s*(\d+)', text, re.IGNORECASE)
    if est_match:
        result['estimate_number'] = est_match.group(1)

    # --- Quote date ---
    # Format: "Date Fri, Apr 4, 2025" or "Date: Thu, Mar 20, 2025" or "Date Thu, Nov 14, 2024"
    # OCR variants: truncated weekday ("Fr", "Fi"), truncated month ("Ju", "Jl"),
    #               missing space before day ("Jul2"), full weekday ("Wednesday")
    date_match = re.search(r'Date\s*:?\s*(\w{2,},?\s+\w{2,}\s*\d{1,2},?\s*\d{4})', text, re.IGNORECASE)
    if date_match:
        result['quote_date'] = date_match.group(1)

    # --- Application ---
    app_match = re.search(r'Application[-–—]\s*(.+?)(?:\n|$)', text, re.IGNORECASE)
    if app_match:
        result['application'] = app_match.group(1).strip()

    # --- Derive bag spec from application field (Issue #7) ---
    app = result.get('application', '')
    if app:
        bag_keywords = ['pouch', 'bag', 'sachet', 'packet', 'wrapper', 'envelope', 'sleeve', 'pillow']
        if any(kw in app.lower() for kw in bag_keywords):
            result['returned_spec_bag'] = app

    # --- Product Size ---
    size_match = re.search(r'Product\s*Size[-–—]\s*(.+?)(?:\n|$)', text, re.IGNORECASE)
    if size_match:
        result['product_size'] = size_match.group(1).strip()
        result['returned_spec_size'] = result['product_size']

    # --- Colors ---
    colors_match = re.search(r'Colors[-–—]\s*(.+?)(?:\n|$)', text, re.IGNORECASE)
    if colors_match:
        result['colors'] = colors_match.group(1).strip()

    # --- Materials (may span multiple lines with Stock#) ---
    # Line 1 contains finish (keywords: Laminate, Matte, Gloss, Karess, Soft Touch)
    # Line 2 contains substrate (e.g. WHITE MET PET / 2.5 MIL LDPE)
    mat_match = re.search(r'Materials[-–—]\s*(.+?)(?=Finishing|Date|$)', text, re.IGNORECASE | re.DOTALL)
    if mat_match:
        materials_raw = mat_match.group(1).strip()
        # Keep full blob for backwards compat
        materials = materials_raw.replace('\n', ' ')
        materials = re.sub(r'\s+', ' ', materials)
        result['materials'] = materials

        # Split individual Stock# lines
        stock_lines = [l.strip() for l in materials_raw.split('\n') if l.strip()]
        finish_keywords = re.compile(r'laminate|matte|gloss|karess|soft\s*touch|tactile|varnish', re.IGNORECASE)
        for line in stock_lines:
            if finish_keywords.search(line):
                result['returned_spec_finish'] = line
            elif re.search(r'PET|LDPE|BOPP|NYLON|FOIL|MET', line, re.IGNORECASE):
                result['returned_spec_substrate'] = line

    # --- Finishing ---
    fin_match = re.search(r'Finishing[-–—]\s*(.+?)(?=Order|Quantity|Date|$)', text, re.IGNORECASE | re.DOTALL)
    if fin_match:
        finishing = fin_match.group(1).strip().replace('\n', ' ')
        finishing = re.sub(r'\s+', ' ', finishing)
        result['finishing'] = finishing

        # Parse individual finishing sub-fields into returned_spec_* keys
        # Known sub-fields in order they appear on Ross PDFs
        finishing_fields = [
            ('Seal Width', 'seal_type'),
            ('Tear Notch', 'tear_notch'),
            ('Hang Hole', 'hole_punch'),
            ('Gusset', 'gusset'),
            ('Zipper', 'zipper'),
            ('Other', 'corners'),
        ]
        # Build regex to split on known field labels
        field_names = [f[0] for f in finishing_fields]
        # Split the finishing text by known field labels
        # Pattern: lookahead for "FieldName:" or "FieldName ="
        split_pattern = '|'.join(re.escape(name) for name in field_names)
        parts = re.split(rf'({split_pattern})\s*[:=]\s*', finishing, flags=re.IGNORECASE)
        # parts = ['', 'Seal Width', '.3125" Seal', 'Tear Notch', '2 - Tear Notch', ...]
        field_map = {name.lower(): col for name, col in finishing_fields}
        i = 1  # skip leading empty string
        while i < len(parts) - 1:
            label = parts[i].strip().lower()
            value = parts[i + 1].strip()
            # Clean leading "=" from values (Ross format: "Seal Width: = .3125")
            value = re.sub(r'^=\s*', '', value)
            if label in field_map and value:
                result[f'returned_spec_{field_map[label]}'] = value
            i += 2

    # --- Pricing table ---
    # Ross has TWO formats:
    #
    # Format 1 (old): All values on horizontal lines
    #   "Quantity 5,000 10,000 25,000 ..."
    #   "Price Each $0.50100 $0.34630 ..."
    #   "Total $2,505.00 $3,463.00 ..."
    #
    # Format 2 (new): Vertical — header then one row per qty
    #   "Quantity Each Total Grand Total"
    #   "10,000 $0.65240 $6,524.00 $7,284.00"
    #   "25,000 $0.52628 $13,157.00 $13,917.00"
    pricing = []

    # Try Format 2 first (most common in recent quotes)
    # Look for "Quantity Each Total Grand Total" header, then parse rows
    fmt2_header = re.search(r'Quantity\s+Each\s+Total\s+Grand\s+Total', text, re.IGNORECASE)
    if fmt2_header:
        # Get lines after the header
        after_header = text[fmt2_header.end():]
        # Match rows: "10,000 $0.65240 $6,524.00 $7,284.00"
        row_pattern = re.compile(
            r'^\s*([\d,]+)\s+\$?([\d.]+)\s+\$?([\d,.]+)\s+\$?([\d,.]+)',
            re.MULTILINE
        )
        for m in row_pattern.finditer(after_header):
            # Stop if we hit non-data (e.g. "Non-Recurring", "Thank You")
            if re.match(r'\s*[A-Z][a-z]', after_header[m.start():m.start()+20]):
                # Check if it's actually a data line or text
                pass
            pricing.append({
                'quantity': m.group(1).replace(',', ''),
                'price_each': m.group(2),
                'total': m.group(3),
                'grand_total': m.group(4),
            })
            # Stop after a reasonable number or when format breaks
            if len(pricing) > 10:
                break

    # Try Format 1 if Format 2 didn't find anything
    if not pricing:
        qty_match = re.search(r'Quantity\s+([\d,\s]+?)(?:\n|$)', text, re.IGNORECASE)
        price_match = re.search(r'Price\s+Each\s+\$?([\d.,\s$]+?)(?:\n|$)', text, re.IGNORECASE)
        total_match = re.search(r'(?:^|\n)\s*Total\s+\$?([\d.,\s$]+?)(?:\n|$)', text, re.IGNORECASE | re.MULTILINE)
        grand_match = re.search(r'Grand\s+Total\s+\$?([\d.,\s$]+?)(?:\n|$)', text, re.IGNORECASE)

        if qty_match and price_match:
            quantities = re.findall(r'[\d,]+', qty_match.group(1))
            prices = re.findall(r'\d+\.\d+', price_match.group(1))
            totals = re.findall(r'[\d,]+\.?\d*', total_match.group(1)) if total_match else []
            grands = re.findall(r'[\d,]+\.?\d*', grand_match.group(1)) if grand_match else []

            for i in range(min(len(quantities), len(prices))):
                entry = {
                    'quantity': quantities[i].replace(',', ''),
                    'price_each': prices[i],
                }
                if i < len(totals):
                    entry['total'] = totals[i]
                if i < len(grands):
                    entry['grand_total'] = grands[i]
                pricing.append(entry)

    # Drop any tiers with a leading-zero quantity ("000") — OCR line-split artefact
    pricing = [p for p in pricing if p.get('quantity', '0')[0] != '0']

    if pricing:
        result['pricing_json'] = json.dumps(pricing)
        # Derive returned_spec_quantities from pricing table
        qtys = [p['quantity'] for p in pricing if p.get('quantity')]
        if qtys:
            result['returned_spec_quantities'] = ', '.join(f'{int(q):,}' for q in qtys)

    # --- Plate / non-recurring charges ---
    plate_match = re.search(
        r'(?:Non-Recurring|Preparation)\s*Charges?[-–—]?\s*\$?([\d,.]+)\s*(.+?)(?:\n|$)',
        text, re.IGNORECASE
    )
    if plate_match:
        result['plate_cost'] = f"${plate_match.group(1)} {plate_match.group(2).strip()}"

    # --- Quote validity ---
    validity_match = re.search(r'quotation\s+is\s+(?:valid|effective)\s+for\s+(\d+\s*days)', text, re.IGNORECASE)
    if validity_match:
        result['quote_validity'] = validity_match.group(1)

    # --- Lead time ---
    lead_match = re.search(r'(?:lead|production)\s+time[:\s]*(.+?)(?:\n|$)', text, re.IGNORECASE)
    if lead_match:
        result['lead_time'] = lead_match.group(1).strip()

    return result


# ============================================================
# DAZPAK — OCR'd PDF
# ============================================================
# Your Items: FL-DL-0963 Printed Laminated Sup With CR Zipper .48 Matte PET
# 5"W X 5.25" H + 3" BG    Ink- 4 Colors
# Material structure lines (Adhesive, White MET PET, etc.)
# Pricing table: Impressions | Quantities | Price/MImps | Price/MSI | Price/Ea
# Web Width | Repeat | Terms | FOB | Art & Plates
# Quote# at top

def extract_dazpak(text):
    """Extract specs and pricing from Dazpak OCR'd PDF text."""
    text = _rejoin_split_numbers(text)
    result = {}

    # --- Quote number and date ---
    # OCR format: "Calyx Containers 08/11/25 13766" or "| Date || Quote#"
    # The date and quote# appear on the same line as company name
    qdate_match = re.search(r'(\d{2}/\d{2}/\d{2,4})\s+(\d{4,6})', text)
    if qdate_match:
        result['quote_date'] = qdate_match.group(1)
        result['quote_number'] = qdate_match.group(2)
    else:
        # Fallback: try separate patterns
        qnum_match = re.search(r'Quote\s*#?\s*(\d{4,6})', text, re.IGNORECASE)
        if qnum_match:
            result['quote_number'] = qnum_match.group(1)
        date_match = re.search(r'(\d{2}/\d{2}/\d{2,4})', text)
        if date_match:
            result['quote_date'] = date_match.group(1)

    # --- Item description (line after "Your Items:") ---
    item_match = re.search(r'Your\s+Items?\s*:?\s*\n?\s*(.+?)(?:\n|$)', text, re.IGNORECASE)
    if item_match:
        result['item_description'] = item_match.group(1).strip()

    # --- Parse bag specs from item description line ---
    # Also scan OCR lines for description patterns (sometimes "Your Items:" is missing)
    desc_line = result.get('item_description', '')
    if not desc_line:
        # Fallback: find lines with bag type keywords + material hints
        for line in text.split('\n'):
            s = line.strip()
            if re.search(r'(?:Printed\s+(?:Laminated\s+)?)?(?:SUP|3\s*S(?:ide\s*)?S(?:eal)?|Pouch|Bag)\s+.*(?:PET|Varnish)', s, re.IGNORECASE):
                desc_line = s
                result['item_description'] = s
                break

    if desc_line:
        # Bag construction: SUP / 3 Side Seal / 3SS / Pouch
        if re.search(r'\bSUP\b', desc_line, re.IGNORECASE):
            result['returned_spec_bag'] = 'SUP'
        elif re.search(r'3\s*S(?:ide\s*)?S(?:eal)?|3SS', desc_line, re.IGNORECASE):
            result['returned_spec_bag'] = '3_SIDE_SEAL'
        elif re.search(r'\bPouch\b', desc_line, re.IGNORECASE):
            result['returned_spec_bag'] = 'SUP'

        # Zipper: CR Zipper (OCR may truncate to "Zippe[" or "Zippe")
        if re.search(r'CR\s*Zippe', desc_line, re.IGNORECASE):
            result['returned_spec_zipper'] = 'CR'

    # Fallback zipper scan: check full text if not found in description line
    if 'returned_spec_zipper' not in result:
        if re.search(r'CR\s*Zippe', text, re.IGNORECASE):
            result['returned_spec_zipper'] = 'CR'

    # Finish: extract from face film description
    # Order matters — check most specific first
    if desc_line:
        if re.search(r'Soft\s*Touch', desc_line, re.IGNORECASE):
            result['returned_spec_finish'] = 'SOFT_TOUCH'
        elif re.search(r'Registered\s+Matte\s+Varnish', desc_line, re.IGNORECASE):
            result['returned_spec_finish'] = 'MATTE'
            result['returned_spec_embellishment'] = 'REGISTERED_MATTE'
        elif re.search(r'Matte', desc_line, re.IGNORECASE):
            result['returned_spec_finish'] = 'MATTE'
        elif re.search(r'Gloss', desc_line, re.IGNORECASE):
            result['returned_spec_finish'] = 'GLOSS'

    # Fallback: scan full OCR text for bag/finish if not recovered from desc_line
    # (some PDFs have the description embedded in material keyword lines)
    if 'returned_spec_bag' not in result:
        if re.search(r'\bSUP\b', text):
            result['returned_spec_bag'] = 'SUP'
        elif re.search(r'3\s*S(?:ide\s*)?S(?:eal)?|3SS', text, re.IGNORECASE):
            result['returned_spec_bag'] = '3_SIDE_SEAL'
        elif re.search(r'\bBag\b', text):
            result['returned_spec_bag'] = 'SUP'
    if 'returned_spec_finish' not in result:
        if re.search(r'Soft\s*Touch', text, re.IGNORECASE):
            result['returned_spec_finish'] = 'SOFT_TOUCH'
        elif re.search(r'Registered\s+Matte\s+Varnish', text, re.IGNORECASE):
            result['returned_spec_finish'] = 'MATTE'
            result['returned_spec_embellishment'] = 'REGISTERED_MATTE'
        elif re.search(r'(?<!\w)[Mm]atte\s+PET', text):
            result['returned_spec_finish'] = 'MATTE'
        elif re.search(r'\bGloss\b', text, re.IGNORECASE):
            result['returned_spec_finish'] = 'GLOSS'

    # --- Item size (e.g. 5"W X 5.25" H + 3" BG, or 8 W X6"H + 2.625" BG) ---
    size_match = re.search(r'([\d.]+"?\s*W\s*[Xx×]\s*[\d.]+"?\s*H\s*(?:\+\s*[\d.]+"?\s*BG)?)', text)
    if size_match:
        result['item_size'] = size_match.group(1).strip()

    # --- Ink colors ---
    ink_match = re.search(r'Ink[-–—]?\s*(\d+\s*Colors?)', text, re.IGNORECASE)
    if ink_match:
        result['ink_colors'] = ink_match.group(1).strip()

    # --- Material structure (lines between size and pricing) ---
    # Include the description line (face film info) plus adhesive/backing lines
    materials = []
    for line in text.split('\n'):
        stripped = line.strip()
        if any(kw in stripped.lower() for kw in ['adhesive', 'met pet', 'metpet', 'ldpe', 'evoh', 'bopp', 'nylon', 'ppe']):
            if len(stripped) < 100:  # Avoid capturing full pricing lines
                materials.append(stripped)
    # Prepend description line if it contains face film info not already in materials
    if desc_line and re.search(r'PET|Varnish|BOPP', desc_line, re.IGNORECASE):
        desc_clean = desc_line
        if materials:
            result['material_structure'] = desc_clean + ' / ' + ' / '.join(materials)
        else:
            result['material_structure'] = desc_clean
    elif materials:
        result['material_structure'] = ' / '.join(materials)

    # --- Pricing table ---
    pricing = []
    # Match lines like: 50,000 $245.2500 $3.2782 $0.2453
    # or: Impressions 50,000 ...
    price_lines = re.findall(
        r'(?:Impressions\s+)?(\d[\d,]+)\s+\$?([\d.]+)\s+\$?([\d.]+)\s+\$?([\d.]+)',
        text
    )
    for match in price_lines:
        qty = match[0].replace(',', '')
        pricing.append({
            'quantity': qty,
            'price_per_m_imps': match[1],
            'price_per_msi': match[2],
            'price_each': match[3],
        })

    # Drop any tiers with a leading-zero quantity ("000") — OCR line-split artefact
    pricing = [p for p in pricing if p.get('quantity', '0')[0] != '0']

    if pricing:
        result['pricing_json'] = json.dumps(pricing)

    # --- Bottom metadata line ---
    # OCR format: "[Web Width [Repeat |Terms | FOB | _ An & Plates"
    # Values line: "13.5000 5.0000 Net 30 Origin $400 / Color"
    # These are on the line AFTER the header line containing "Web Width"
    lines = text.split('\n')
    for i, line in enumerate(lines):
        if 'web width' in line.lower() or 'Web Width' in line:
            # The values are on the next line
            if i + 1 < len(lines):
                vals_line = lines[i + 1].strip()
                # Parse: "13.5000 5.0000 Net 30 Origin $400 / Color"
                vals_match = re.match(
                    r'([\d.]+)\s+([\d.]+)\s+(Net\s*\d+|COD|Prepaid)\s+(\w+)\s+\$?([\d,.]+\s*/?\s*\w*)',
                    vals_line
                )
                if vals_match:
                    result['web_width'] = vals_match.group(1)
                    result['repeat_length'] = vals_match.group(2)
                    result['terms'] = vals_match.group(3)
                    result['fob'] = vals_match.group(4)
                    result['plate_cost'] = vals_match.group(5).strip()
                else:
                    # Try partial matches
                    nums = re.findall(r'[\d.]+', vals_line)
                    if len(nums) >= 2:
                        result['web_width'] = nums[0]
                        result['repeat_length'] = nums[1]
                    terms_m = re.search(r'(Net\s*\d+|COD|Prepaid)', vals_line, re.IGNORECASE)
                    if terms_m:
                        result['terms'] = terms_m.group(1)
                    fob_m = re.search(r'(Origin|Destination)', vals_line, re.IGNORECASE)
                    if fob_m:
                        result['fob'] = fob_m.group(1)
                    plate_m = re.search(r'\$?([\d,.]+\s*/\s*\w+)', vals_line)
                    if plate_m:
                        result['plate_cost'] = plate_m.group(1)
            break

    # Fallback plate cost if not found in metadata line
    if 'plate_cost' not in result:
        plate_match = re.search(r'(?:Art\s*&?\s*Plates|Plates)\s*\n?\s*\$?([\d,.]+\s*/?\s*\w*)', text, re.IGNORECASE)
        if plate_match:
            result['plate_cost'] = plate_match.group(1).strip()

    # --- Quote validity ---
    validity_match = re.search(r'(?:valid|Pricing valid)\s+for\s+(\d+\s*days)', text, re.IGNORECASE)
    if validity_match:
        result['quote_validity'] = validity_match.group(1)

    # --- returned_spec_* mappings ---
    if result.get('item_size'):
        result['returned_spec_size'] = result['item_size']
    if result.get('material_structure'):
        result['returned_spec_substrate'] = result['material_structure']
    if pricing:
        qtys = [p['quantity'] for p in pricing if p.get('quantity')]
        if qtys:
            result['returned_spec_quantities'] = ', '.join(qtys)

    return result


def extract_for_vendor(vendor, text):
    """Route to the appropriate vendor extractor."""
    extractors = {
        'Tedpack': extract_tedpack,
        'Ross': extract_ross,
        'Dazpak': extract_dazpak,
    }
    extractor = extractors.get(vendor)
    if extractor:
        return extractor(text)
    return {}

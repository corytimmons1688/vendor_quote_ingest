-- ============================================================
-- Migration 002: Add pricing columns to all vendors,
-- add spec columns to Tedpack, add quote-level metadata
-- ============================================================

-- -------------------------------------------------------
-- Tedpack: Add spec + pricing columns
-- -------------------------------------------------------
ALTER TABLE est_bnz_tedpack
    -- Spec columns (same as Ross/Dazpak)
    ADD COLUMN IF NOT EXISTS spec_bag                TEXT,
    ADD COLUMN IF NOT EXISTS spec_size               TEXT,
    ADD COLUMN IF NOT EXISTS spec_substrate          TEXT,
    ADD COLUMN IF NOT EXISTS spec_finish             TEXT,
    ADD COLUMN IF NOT EXISTS spec_material           TEXT,
    ADD COLUMN IF NOT EXISTS spec_embellishment      TEXT,
    ADD COLUMN IF NOT EXISTS spec_fill_style         TEXT,
    ADD COLUMN IF NOT EXISTS spec_seal_type          TEXT,
    ADD COLUMN IF NOT EXISTS spec_gusset_style       TEXT,
    ADD COLUMN IF NOT EXISTS spec_gusset_details     TEXT,
    ADD COLUMN IF NOT EXISTS spec_zipper             TEXT,
    ADD COLUMN IF NOT EXISTS spec_tear_notch         TEXT,
    ADD COLUMN IF NOT EXISTS spec_hole_punch         TEXT,
    ADD COLUMN IF NOT EXISTS spec_corners            TEXT,
    ADD COLUMN IF NOT EXISTS spec_printing_method    TEXT,
    ADD COLUMN IF NOT EXISTS spec_quantities         TEXT,
    -- Pricing columns
    ADD COLUMN IF NOT EXISTS print_method            TEXT,
    ADD COLUMN IF NOT EXISTS pricing_json            JSONB,
    ADD COLUMN IF NOT EXISTS plate_cost              TEXT,
    ADD COLUMN IF NOT EXISTS lead_time               TEXT,
    ADD COLUMN IF NOT EXISTS quote_id                TEXT;

-- -------------------------------------------------------
-- Ross: Add pricing columns
-- -------------------------------------------------------
ALTER TABLE est_bnz_ross
    ADD COLUMN IF NOT EXISTS estimate_number         TEXT,
    ADD COLUMN IF NOT EXISTS application             TEXT,
    ADD COLUMN IF NOT EXISTS product_size            TEXT,
    ADD COLUMN IF NOT EXISTS colors                  TEXT,
    ADD COLUMN IF NOT EXISTS materials               TEXT,
    ADD COLUMN IF NOT EXISTS finishing                TEXT,
    ADD COLUMN IF NOT EXISTS pricing_json            JSONB,
    ADD COLUMN IF NOT EXISTS plate_cost              TEXT,
    ADD COLUMN IF NOT EXISTS lead_time               TEXT,
    ADD COLUMN IF NOT EXISTS quote_date              TEXT,
    ADD COLUMN IF NOT EXISTS quote_validity          TEXT;

-- -------------------------------------------------------
-- Dazpak: Add pricing columns
-- -------------------------------------------------------
ALTER TABLE est_bnz_dazpak
    ADD COLUMN IF NOT EXISTS quote_number            TEXT,
    ADD COLUMN IF NOT EXISTS item_description        TEXT,
    ADD COLUMN IF NOT EXISTS item_size               TEXT,
    ADD COLUMN IF NOT EXISTS ink_colors              TEXT,
    ADD COLUMN IF NOT EXISTS material_structure      TEXT,
    ADD COLUMN IF NOT EXISTS pricing_json            JSONB,
    ADD COLUMN IF NOT EXISTS plate_cost              TEXT,
    ADD COLUMN IF NOT EXISTS web_width               TEXT,
    ADD COLUMN IF NOT EXISTS repeat_length           TEXT,
    ADD COLUMN IF NOT EXISTS terms                   TEXT,
    ADD COLUMN IF NOT EXISTS fob                     TEXT,
    ADD COLUMN IF NOT EXISTS quote_date              TEXT,
    ADD COLUMN IF NOT EXISTS quote_validity          TEXT;

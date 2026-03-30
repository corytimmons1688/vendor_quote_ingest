-- ============================================================
-- Migration 005: Bronze table cleanup and integrity
--
-- A) UNIQUE constraints on source_file (enables upsert)
-- B) Drop orphaned long-name spec columns never populated
-- C) Add lineage columns (source_message_id, request_date, response_date)
-- D) Fix pricing_json to JSONB where still TEXT
-- ============================================================

-- ----- A) UNIQUE constraints on source_file --------------------

ALTER TABLE est_bnz_tedpack ADD CONSTRAINT uq_tedpack_source_file UNIQUE (source_file);
ALTER TABLE est_bnz_ross ADD CONSTRAINT uq_ross_source_file UNIQUE (source_file);
ALTER TABLE est_bnz_dazpak ADD CONSTRAINT uq_dazpak_source_file UNIQUE (source_file);

-- ----- B) Drop orphaned columns --------------------------------

-- Ross: long-name variants from migration 001 that are never populated
ALTER TABLE est_bnz_ross
    DROP COLUMN IF EXISTS requested_spec_material,
    DROP COLUMN IF EXISTS requested_spec_gusset_style,
    DROP COLUMN IF EXISTS requested_spec_gusset_details,
    DROP COLUMN IF EXISTS requested_spec_printing_method,
    DROP COLUMN IF EXISTS returned_spec_material,
    DROP COLUMN IF EXISTS returned_spec_gusset_style,
    DROP COLUMN IF EXISTS returned_spec_gusset_details,
    DROP COLUMN IF EXISTS returned_spec_printing_method;

-- Dazpak: same long-name variants from migration 001
ALTER TABLE est_bnz_dazpak
    DROP COLUMN IF EXISTS requested_spec_material,
    DROP COLUMN IF EXISTS requested_spec_gusset_style,
    DROP COLUMN IF EXISTS requested_spec_gusset_details,
    DROP COLUMN IF EXISTS requested_spec_printing_method,
    DROP COLUMN IF EXISTS returned_spec_material,
    DROP COLUMN IF EXISTS returned_spec_gusset_style,
    DROP COLUMN IF EXISTS returned_spec_gusset_details,
    DROP COLUMN IF EXISTS returned_spec_printing_method;

-- Tedpack: old spec_* columns from migration 002 that are never populated
ALTER TABLE est_bnz_tedpack
    DROP COLUMN IF EXISTS spec_material,
    DROP COLUMN IF EXISTS spec_gusset_style,
    DROP COLUMN IF EXISTS spec_gusset_details,
    DROP COLUMN IF EXISTS spec_printing_method;

-- ----- C) Add lineage and date columns -------------------------

ALTER TABLE est_bnz_ross
    ADD COLUMN IF NOT EXISTS source_message_id TEXT,
    ADD COLUMN IF NOT EXISTS request_date TEXT,
    ADD COLUMN IF NOT EXISTS response_date TEXT;

ALTER TABLE est_bnz_tedpack
    ADD COLUMN IF NOT EXISTS source_message_id TEXT,
    ADD COLUMN IF NOT EXISTS request_date TEXT,
    ADD COLUMN IF NOT EXISTS response_date TEXT;

ALTER TABLE est_bnz_dazpak
    ADD COLUMN IF NOT EXISTS source_message_id TEXT,
    ADD COLUMN IF NOT EXISTS request_date TEXT,
    ADD COLUMN IF NOT EXISTS response_date TEXT;

-- ----- D) Fix pricing_json to JSONB ----------------------------

ALTER TABLE est_bnz_tedpack ALTER COLUMN pricing_json TYPE JSONB USING CASE WHEN pricing_json IS NOT NULL AND pricing_json != '' THEN pricing_json::jsonb ELSE NULL END;
ALTER TABLE est_bnz_ross ALTER COLUMN pricing_json TYPE JSONB USING CASE WHEN pricing_json IS NOT NULL AND pricing_json != '' THEN pricing_json::jsonb ELSE NULL END;
ALTER TABLE est_bnz_dazpak ALTER COLUMN pricing_json TYPE JSONB USING CASE WHEN pricing_json IS NOT NULL AND pricing_json != '' THEN pricing_json::jsonb ELSE NULL END;

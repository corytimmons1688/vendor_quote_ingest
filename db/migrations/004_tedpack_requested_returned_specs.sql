-- ============================================================
-- Migration 004: Add requested/returned spec columns to Tedpack
--
-- Brings est_bnz_tedpack in line with Ross and Dazpak by adding
-- the same 13 requested_spec_* and 13 returned_spec_* columns.
-- Uses ADD COLUMN IF NOT EXISTS for idempotency.
-- ============================================================

ALTER TABLE est_bnz_tedpack
    ADD COLUMN IF NOT EXISTS requested_spec_bag              TEXT,
    ADD COLUMN IF NOT EXISTS requested_spec_size             TEXT,
    ADD COLUMN IF NOT EXISTS requested_spec_substrate        TEXT,
    ADD COLUMN IF NOT EXISTS requested_spec_finish           TEXT,
    ADD COLUMN IF NOT EXISTS requested_spec_embellishment    TEXT,
    ADD COLUMN IF NOT EXISTS requested_spec_fill_style       TEXT,
    ADD COLUMN IF NOT EXISTS requested_spec_seal_type        TEXT,
    ADD COLUMN IF NOT EXISTS requested_spec_gusset           TEXT,
    ADD COLUMN IF NOT EXISTS requested_spec_zipper           TEXT,
    ADD COLUMN IF NOT EXISTS requested_spec_tear_notch       TEXT,
    ADD COLUMN IF NOT EXISTS requested_spec_hole_punch       TEXT,
    ADD COLUMN IF NOT EXISTS requested_spec_corners          TEXT,
    ADD COLUMN IF NOT EXISTS requested_spec_quantities       TEXT,
    ADD COLUMN IF NOT EXISTS returned_spec_bag               TEXT,
    ADD COLUMN IF NOT EXISTS returned_spec_size              TEXT,
    ADD COLUMN IF NOT EXISTS returned_spec_substrate         TEXT,
    ADD COLUMN IF NOT EXISTS returned_spec_finish            TEXT,
    ADD COLUMN IF NOT EXISTS returned_spec_embellishment     TEXT,
    ADD COLUMN IF NOT EXISTS returned_spec_fill_style        TEXT,
    ADD COLUMN IF NOT EXISTS returned_spec_seal_type         TEXT,
    ADD COLUMN IF NOT EXISTS returned_spec_gusset            TEXT,
    ADD COLUMN IF NOT EXISTS returned_spec_zipper            TEXT,
    ADD COLUMN IF NOT EXISTS returned_spec_tear_notch        TEXT,
    ADD COLUMN IF NOT EXISTS returned_spec_hole_punch        TEXT,
    ADD COLUMN IF NOT EXISTS returned_spec_corners           TEXT,
    ADD COLUMN IF NOT EXISTS returned_spec_quantities        TEXT;

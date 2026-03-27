-- ============================================================
-- Neon Postgres — Bronze Layer Vendor Quote Tables
-- Target: dev-estimates branch ONLY
-- All names lowercase per PostgreSQL convention
-- One row per source file (flat file style)
-- ============================================================

-- -------------------------------------------------------
-- est_bnz_tedpack
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS est_bnz_tedpack (
    ingestion_id        UUID            NOT NULL DEFAULT gen_random_uuid(),
    source_file         TEXT            NOT NULL,
    source_vendor       TEXT            NOT NULL DEFAULT 'Tedpack',
    email_date          TEXT,
    email_subject       TEXT,
    email_from          TEXT,

    -- Raw OCR text (full document, all pages combined)
    raw_ocr_text        TEXT,

    -- Metadata
    file_type           TEXT,
    file_size_bytes     BIGINT,
    ocr_engine          TEXT            DEFAULT 'tesseract',
    ocr_version         TEXT,
    page_count          INTEGER,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    processing_run      TEXT,
    status              TEXT            DEFAULT 'raw',
    error_message       TEXT
);

-- -------------------------------------------------------
-- est_bnz_ross
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS est_bnz_ross (
    ingestion_id        UUID            NOT NULL DEFAULT gen_random_uuid(),
    source_file         TEXT            NOT NULL,
    source_vendor       TEXT            NOT NULL DEFAULT 'Ross',
    email_date          TEXT,
    email_subject       TEXT,
    email_from          TEXT,

    -- Requested specs (from Dan's outbound quote request)
    requested_spec_bag              TEXT,
    requested_spec_size             TEXT,
    requested_spec_substrate        TEXT,
    requested_spec_finish           TEXT,
    requested_spec_material         TEXT,
    requested_spec_embellishment    TEXT,
    requested_spec_fill_style       TEXT,
    requested_spec_seal_type        TEXT,
    requested_spec_gusset_style     TEXT,
    requested_spec_gusset_details   TEXT,
    requested_spec_zipper           TEXT,
    requested_spec_tear_notch       TEXT,
    requested_spec_hole_punch       TEXT,
    requested_spec_corners          TEXT,
    requested_spec_printing_method  TEXT,
    requested_spec_quantities       TEXT,

    -- Returned specs (from vendor's PDF quote response)
    returned_spec_bag               TEXT,
    returned_spec_size              TEXT,
    returned_spec_substrate         TEXT,
    returned_spec_finish            TEXT,
    returned_spec_material          TEXT,
    returned_spec_embellishment     TEXT,
    returned_spec_fill_style        TEXT,
    returned_spec_seal_type         TEXT,
    returned_spec_gusset_style      TEXT,
    returned_spec_gusset_details    TEXT,
    returned_spec_zipper            TEXT,
    returned_spec_tear_notch        TEXT,
    returned_spec_hole_punch        TEXT,
    returned_spec_corners           TEXT,
    returned_spec_printing_method   TEXT,
    returned_spec_quantities        TEXT,

    -- Raw OCR text (full document, all pages combined)
    raw_ocr_text        TEXT,

    -- Metadata
    file_type           TEXT,
    file_size_bytes     BIGINT,
    ocr_engine          TEXT            DEFAULT 'tesseract',
    ocr_version         TEXT,
    page_count          INTEGER,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    processing_run      TEXT,
    status              TEXT            DEFAULT 'raw',
    error_message       TEXT
);

-- -------------------------------------------------------
-- est_bnz_dazpak
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS est_bnz_dazpak (
    ingestion_id        UUID            NOT NULL DEFAULT gen_random_uuid(),
    source_file         TEXT            NOT NULL,
    source_vendor       TEXT            NOT NULL DEFAULT 'Dazpak',
    email_date          TEXT,
    email_subject       TEXT,
    email_from          TEXT,

    -- Requested specs (from Dan's outbound quote request)
    requested_spec_bag              TEXT,
    requested_spec_size             TEXT,
    requested_spec_substrate        TEXT,
    requested_spec_finish           TEXT,
    requested_spec_material         TEXT,
    requested_spec_embellishment    TEXT,
    requested_spec_fill_style       TEXT,
    requested_spec_seal_type        TEXT,
    requested_spec_gusset_style     TEXT,
    requested_spec_gusset_details   TEXT,
    requested_spec_zipper           TEXT,
    requested_spec_tear_notch       TEXT,
    requested_spec_hole_punch       TEXT,
    requested_spec_corners          TEXT,
    requested_spec_printing_method  TEXT,
    requested_spec_quantities       TEXT,

    -- Returned specs (from vendor's PDF quote response)
    returned_spec_bag               TEXT,
    returned_spec_size              TEXT,
    returned_spec_substrate         TEXT,
    returned_spec_finish            TEXT,
    returned_spec_material          TEXT,
    returned_spec_embellishment     TEXT,
    returned_spec_fill_style        TEXT,
    returned_spec_seal_type         TEXT,
    returned_spec_gusset_style      TEXT,
    returned_spec_gusset_details    TEXT,
    returned_spec_zipper            TEXT,
    returned_spec_tear_notch        TEXT,
    returned_spec_hole_punch        TEXT,
    returned_spec_corners           TEXT,
    returned_spec_printing_method   TEXT,
    returned_spec_quantities        TEXT,

    -- Raw OCR text (full document, all pages combined)
    raw_ocr_text        TEXT,

    -- Metadata
    file_type           TEXT,
    file_size_bytes     BIGINT,
    ocr_engine          TEXT            DEFAULT 'tesseract',
    ocr_version         TEXT,
    page_count          INTEGER,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    processing_run      TEXT,
    status              TEXT            DEFAULT 'raw',
    error_message       TEXT
);

-- -------------------------------------------------------
-- Indexes for common query patterns
-- -------------------------------------------------------
CREATE INDEX idx_est_bnz_tedpack_ingested ON est_bnz_tedpack (ingested_at DESC);
CREATE INDEX idx_est_bnz_tedpack_source   ON est_bnz_tedpack (source_file);
CREATE INDEX idx_est_bnz_tedpack_run      ON est_bnz_tedpack (processing_run);

CREATE INDEX idx_est_bnz_ross_ingested    ON est_bnz_ross (ingested_at DESC);
CREATE INDEX idx_est_bnz_ross_source      ON est_bnz_ross (source_file);
CREATE INDEX idx_est_bnz_ross_run         ON est_bnz_ross (processing_run);

CREATE INDEX idx_est_bnz_dazpak_ingested  ON est_bnz_dazpak (ingested_at DESC);
CREATE INDEX idx_est_bnz_dazpak_source    ON est_bnz_dazpak (source_file);
CREATE INDEX idx_est_bnz_dazpak_run       ON est_bnz_dazpak (processing_run);

-- -------------------------------------------------------
-- Ingestion audit log
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS est_bnz_ingestion_log (
    est_bnz_ingestion_log_pk  BIGSERIAL   PRIMARY KEY,
    processing_run      TEXT            NOT NULL,
    vendor              TEXT            NOT NULL,
    files_processed     INTEGER         DEFAULT 0,
    rows_inserted       INTEGER         DEFAULT 0,
    errors              INTEGER         DEFAULT 0,
    started_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    status              TEXT            DEFAULT 'running',
    error_details       JSONB
);

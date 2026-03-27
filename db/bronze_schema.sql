-- ============================================================
-- Neon Postgres — Bronze Layer Vendor Quote Tables
-- Target: dev-estimates branch ONLY
-- All names lowercase per PostgreSQL convention
-- ============================================================

-- -------------------------------------------------------
-- est_bnz_tedpack
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS est_bnz_tedpack (
    est_bnz_tedpack_pk  BIGSERIAL       PRIMARY KEY,
    ingestion_id        UUID            NOT NULL DEFAULT gen_random_uuid(),
    source_file         TEXT            NOT NULL,
    source_vendor       TEXT            NOT NULL DEFAULT 'Tedpack',
    email_date          TEXT,
    email_subject       TEXT,
    email_from          TEXT,

    -- Raw OCR / parsed fields (all TEXT to preserve original data)
    raw_ocr_text        TEXT,
    field_key           TEXT,
    field_value         TEXT,
    field_line_num      INTEGER,
    field_confidence    NUMERIC(5,4),

    -- Metadata
    file_type           TEXT,
    file_size_bytes     BIGINT,
    ocr_engine          TEXT            DEFAULT 'tesseract',
    ocr_version         TEXT,
    page_number         INTEGER,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    processing_run      TEXT,
    status              TEXT            DEFAULT 'raw',
    error_message       TEXT
);

-- -------------------------------------------------------
-- est_bnz_ross
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS est_bnz_ross (
    est_bnz_ross_pk     BIGSERIAL       PRIMARY KEY,
    ingestion_id        UUID            NOT NULL DEFAULT gen_random_uuid(),
    source_file         TEXT            NOT NULL,
    source_vendor       TEXT            NOT NULL DEFAULT 'Ross',
    email_date          TEXT,
    email_subject       TEXT,
    email_from          TEXT,

    raw_ocr_text        TEXT,
    field_key           TEXT,
    field_value         TEXT,
    field_line_num      INTEGER,
    field_confidence    NUMERIC(5,4),

    file_type           TEXT,
    file_size_bytes     BIGINT,
    ocr_engine          TEXT            DEFAULT 'tesseract',
    ocr_version         TEXT,
    page_number         INTEGER,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    processing_run      TEXT,
    status              TEXT            DEFAULT 'raw',
    error_message       TEXT
);

-- -------------------------------------------------------
-- est_bnz_dazpak
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS est_bnz_dazpak (
    est_bnz_dazpak_pk   BIGSERIAL       PRIMARY KEY,
    ingestion_id        UUID            NOT NULL DEFAULT gen_random_uuid(),
    source_file         TEXT            NOT NULL,
    source_vendor       TEXT            NOT NULL DEFAULT 'Dazpak',
    email_date          TEXT,
    email_subject       TEXT,
    email_from          TEXT,

    raw_ocr_text        TEXT,
    field_key           TEXT,
    field_value         TEXT,
    field_line_num      INTEGER,
    field_confidence    NUMERIC(5,4),

    file_type           TEXT,
    file_size_bytes     BIGINT,
    ocr_engine          TEXT            DEFAULT 'tesseract',
    ocr_version         TEXT,
    page_number         INTEGER,
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

-- ============================================================================
-- KeToanAI — Postgres schema (deliverable 1 of the Postgres architecture)
-- Target: Supabase Postgres (Postgres 15+). No live Supabase project exists
-- yet; this file has NOT been run against a live database. Apply with:
--   psql "$DATABASE_URL" -f schema.sql
-- or via the Supabase SQL editor (paste in full).
--
-- Source of truth for the migration: the collector SQLite database
--   ~/workspace/goals/ketoanai-accounting-web-app/files/collector-congbao/documents.db
-- (83 documents / 17,998 provisions as of 2026-11-08). See
-- db/migrate_sqlite_to_postgres.py for the migration.
--
-- ROBOTS / ToS RULES THE COLLECTOR MUST OBEY (enforced by collector code,
-- recorded here so no future collector change can claim ignorance):
--   1. VBPL (vbpl.vn): NEVER call its internal /api/ or /Pages/ endpoints
--      (robots.txt disallows them). Only fetch public detail pages and
--      files served through the official gateway. VBPL sitemap:
--      https://vbpl.vn/sitemap.xml
--   2. Cong bao (congbao.chinhphu.vn): max 1 request/second, sequential
--      fetching only. No parallel scraping.
--   3. Only fetch from official domains: congbao.chinhphu.vn, vbpl.vn,
--      and (later) mof.gov.vn / gdt.gov.vn. No third-party mirrors.
--   4. Respect robots.txt on every domain before each fetch run; if a
--      domain starts blocking or rate-limiting, stop and report instead
--      of working around it.
--   5. All rows are labelled unverified until a human checks them against
--      the official text; 'legal_verified' requires a legal review pass,
--      never an automated promotion.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Extensions
-- ----------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- RAG (later phase): vector search over provision/document chunks.
-- Deliberately NOT enabled now — chunks do not exist yet and enabling an
-- unused extension adds nothing. Uncomment when the chunk table lands:
-- CREATE EXTENSION IF NOT EXISTS vector;

-- ----------------------------------------------------------------------------
-- documents — one row per legal document (law, decree, circular, ...)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS documents (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    number       TEXT NOT NULL,                       -- e.g. '99/2025/TT-BTC'
    title        TEXT NOT NULL,
    category     TEXT NOT NULL,                       -- e.g. 'Thuế GTGT'
    source       TEXT NOT NULL
                 CHECK (source IN ('congbao', 'vbpl', 'mof', 'gdt')),
    source_url   TEXT NOT NULL UNIQUE,                -- canonical detail-page URL
    issued_date  DATE,
    effective_date DATE,
    fetched_at   TIMESTAMPTZ,
    -- Verification ladder. Migration maps: all SQLite rows -> 'collected'
    -- except the 5 metadata-verified docs -> 'source_verified'.
    -- 'legal_verified' is NEVER set by the migration or any collector run;
    -- it requires a human legal review against the official text.
    status       TEXT NOT NULL DEFAULT 'collected'
                 CHECK (status IN ('collected', 'source_verified', 'legal_verified')),
    sha256       TEXT,                                -- hash of extracted text
    file_path    TEXT,                                -- object-storage path of source PDF/DOC (NULL until files are stored)
    file_sha256  TEXT,                                -- hash of the source file (NULL until files are stored)
    text_length  INT,                                 -- len(extracted_text), for integrity checks
    -- NOTE: extracted_text stays in this table for now so the MVP search
    -- works without a second table. The RAG phase will move searchable text
    -- into a chunks table (with pgvector embeddings) and this column will
    -- become a short excerpt / be dropped. Do not build new features on it.
    extracted_text TEXT NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ----------------------------------------------------------------------------
-- provisions — one row per Điều / Khoản / Điểm (or preamble group, dieu = 0)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS provisions (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    document_id BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    chuong      TEXT,                                -- e.g. 'I: QUY ĐỊNH CHUNG'
    muc         TEXT,
    dieu        INT,                                 -- 0 = preamble group (header text before Điều 1)
    khoan       INT,                                 -- NULL when the provision is a whole Điều
    diem        TEXT,                                -- NULL when the provision is a whole Khoản/Điều
    ordinal     INT NOT NULL,                        -- position inside the document (1..N)
    text        TEXT NOT NULL,                       -- SQLite column was named noi_dung; renamed here
    confidence  TEXT,                                -- 'high' | 'medium' | 'low' (parser confidence)
    -- UNIQUE KEY MUST INCLUDE ordinal. The SQLite key without ordinal
    -- (document_number, dieu, khoan, diem) collapsed 111 provisions in
    -- 4 amending decrees that reuse the same Điều/Khoản/Điểm numbers for
    -- different amended text. Never repeat that bug.
    -- NULLS NOT DISTINCT (Postgres 15+): khoan/diem are NULL for most
    -- whole-Điều rows, and plain UNIQUE treats NULLs as distinct, so
    -- without this the upsert in the migration could NOT detect a
    -- conflict on re-run and would insert duplicates. Supabase runs
    -- Postgres 15, which supports this.
    UNIQUE NULLS NOT DISTINCT (document_id, dieu, khoan, diem, ordinal)
);

-- ----------------------------------------------------------------------------
-- document_versions — one row per fetched version of a document
-- (re-fetch of a changed official text creates a new version; the
-- documents row always points at the latest text)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS document_versions (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    document_id    BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    sha256         TEXT,                             -- hash of extracted text for this version
    file_sha256    TEXT,                             -- hash of the source file for this version
    fetched_at     TIMESTAMPTZ,
    change_summary TEXT,                             -- human/notes: what changed vs previous version
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ----------------------------------------------------------------------------
-- fetch_log — collector run history (copied verbatim from SQLite)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fetch_log (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    document_number TEXT,
    source_url      TEXT,
    pdf_url         TEXT,
    pages           INT,
    chars           INT,
    status          TEXT,                            -- 'ok' | 'failed'
    error           TEXT,
    fetched_at      TIMESTAMPTZ
);

-- ----------------------------------------------------------------------------
-- Indexes
-- ----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_documents_number       ON documents (number);
CREATE INDEX IF NOT EXISTS idx_documents_category     ON documents (category);
CREATE INDEX IF NOT EXISTS idx_documents_status       ON documents (status);
CREATE INDEX IF NOT EXISTS idx_documents_source       ON documents (source);
CREATE INDEX IF NOT EXISTS idx_documents_issued_date  ON documents (issued_date);
-- documents(source_url) is already UNIQUE, so no separate index is needed;
-- the unique constraint creates its own btree index.

CREATE INDEX IF NOT EXISTS idx_provisions_doc_ordinal ON provisions (document_id, ordinal);
CREATE INDEX IF NOT EXISTS idx_provisions_doc_dieu    ON provisions (document_id, dieu);

-- ----------------------------------------------------------------------------
-- Full-text search — CHOICE: pg_trgm (trigram), NOT tsvector. Justification:
--   * Postgres ships no Vietnamese dictionary, and the 'simple' tsvector
--     config does no stemming or stop-word handling for Vietnamese, so
--     tsvector ranking adds complexity for almost no benefit here.
--   * pg_trgm does substring / similarity search without any language
--     model: it matches "thuế thu nhập" inside titles even with typos or
--     partial input, which is exactly how users search law titles.
--   * Cost is one GIN index on the (short) title column. A trigram index
--     on the multi-MB extracted_text column would be enormous, so full
--     text search over document bodies is deliberately deferred to the
--     RAG chunk table (small chunks + pgvector), not indexed here.
-- ----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_documents_title_trgm  ON documents USING gin (title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_documents_number_trgm ON documents USING gin (number gin_trgm_ops);

-- ----------------------------------------------------------------------------
-- updated_at maintenance
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_documents_updated_at ON documents;
CREATE TRIGGER trg_documents_updated_at
    BEFORE UPDATE ON documents
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

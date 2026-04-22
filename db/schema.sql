-- Triple Match Audit — schema v2
-- Applied idempotently: safe to re-run.
-- v2 change: financial_docs is now header-only; line items live in line_items.

-- ---------------------------------------------------------------------------
-- Drop dependent view first so ALTER TABLE below can DROP COLUMN cleanly.
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS v_triple_match;

-- ---------------------------------------------------------------------------
-- Tables
-- ---------------------------------------------------------------------------

-- Every PDF lands here exactly once. Checksum dedupes re-ingestion.
CREATE TABLE IF NOT EXISTS documents (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    filename        text NOT NULL,
    checksum        text UNIQUE NOT NULL,
    doc_type        text NOT NULL CHECK (doc_type IN (
                        'po','invoice','gr',
                        'email','memo','amendment','shipping',
                        'unknown'
                    )),
    raw_text        text NOT NULL,
    page_count      int,
    ingested_at     timestamptz NOT NULL DEFAULT now(),
    extractor_ver   text NOT NULL
);

-- Header-level financial facts. Line items live in line_items.
CREATE TABLE IF NOT EXISTS financial_docs (
    id              uuid PRIMARY KEY REFERENCES documents(id) ON DELETE CASCADE,
    doc_type        text NOT NULL CHECK (doc_type IN ('po','invoice','gr')),
    doc_number      text NOT NULL,           -- e.g. PO-4010, INV-8891, GR-3301
    po_reference    text,                    -- NULL for POs; = a PO's doc_number otherwise
    vendor          text NOT NULL,
    doc_date        date,
    total           numeric,                 -- document grand total; NULL for GR
    UNIQUE (doc_type, doc_number)
);

-- v1 -> v2 migration: remove obsolete item-level columns if they exist.
ALTER TABLE financial_docs DROP COLUMN IF EXISTS item;
ALTER TABLE financial_docs DROP COLUMN IF EXISTS quantity;
ALTER TABLE financial_docs DROP COLUMN IF EXISTS unit_price;

CREATE INDEX IF NOT EXISTS financial_docs_po_ref_idx
    ON financial_docs (po_reference);

-- Line items for POs, invoices, and GRs.
-- GR lines have NULL unit_price and line_total (goods receipts don't carry pricing).
-- Single-item docs are represented as a single line_items row (line_no = 1).
CREATE TABLE IF NOT EXISTS line_items (
    id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    financial_doc_id  uuid NOT NULL REFERENCES financial_docs(id) ON DELETE CASCADE,
    line_no           int NOT NULL,
    item              text NOT NULL,
    quantity          numeric NOT NULL,
    unit_price        numeric,                 -- NULL on GR lines
    line_total        numeric,                 -- NULL on GR lines
    UNIQUE (financial_doc_id, line_no)
);
CREATE INDEX IF NOT EXISTS line_items_doc_idx
    ON line_items (financial_doc_id);

-- Free-form supporting docs.
CREATE TABLE IF NOT EXISTS supporting_docs (
    id              uuid PRIMARY KEY REFERENCES documents(id) ON DELETE CASCADE,
    subtype         text,
    vendor_guess    text,
    date_guess      date,
    po_mentions     text[],
    summary         text
);
CREATE INDEX IF NOT EXISTS supporting_docs_summary_fts_idx
    ON supporting_docs USING gin (to_tsvector('english', coalesce(summary, '')));

-- Links.
CREATE TABLE IF NOT EXISTS document_links (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    supporting_id   uuid NOT NULL REFERENCES supporting_docs(id) ON DELETE CASCADE,
    po_number       text NOT NULL,
    method          text NOT NULL CHECK (method IN (
                        'explicit_reference','vendor_date','llm_classified','auditor_corrected'
                    )),
    confidence      numeric NOT NULL CHECK (confidence BETWEEN 0 AND 1),
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (supporting_id, po_number)
);

-- Cached LLM analyses.
CREATE TABLE IF NOT EXISTS discrepancy_analyses (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    po_number       text NOT NULL,
    explanation     text,
    classification  text CHECK (classification IN ('explained','unexplained','false_positive')),
    confidence      numeric CHECK (confidence BETWEEN 0 AND 1),
    citations       jsonb,
    prompt_version  text,
    model_version   text,
    analyzed_at     timestamptz NOT NULL DEFAULT now(),
    input_hash      text NOT NULL
);
CREATE INDEX IF NOT EXISTS discrepancy_analyses_po_idx
    ON discrepancy_analyses (po_number, analyzed_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS discrepancy_analyses_input_hash_idx
    ON discrepancy_analyses (input_hash);

CREATE INDEX IF NOT EXISTS documents_raw_text_fts_idx
    ON documents USING gin (to_tsvector('english', raw_text));

-- ---------------------------------------------------------------------------
-- View: triple match status per PO (derived; never stored)
-- Compares document totals and aggregate line-item quantities.
-- Per-line comparison is intentionally left to the analyzer — summing
-- quantities across heterogeneous items is a rough signal, not exact math.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_triple_match AS
WITH po AS (
    SELECT fd.id, fd.doc_number, fd.vendor, fd.doc_date, fd.total,
           coalesce(sum(li.quantity), 0) AS total_qty
      FROM financial_docs fd
 LEFT JOIN line_items li ON li.financial_doc_id = fd.id
     WHERE fd.doc_type = 'po'
  GROUP BY fd.id
),
inv AS (
    SELECT fd.id, fd.po_reference, fd.total,
           coalesce(sum(li.quantity), 0) AS total_qty
      FROM financial_docs fd
 LEFT JOIN line_items li ON li.financial_doc_id = fd.id
     WHERE fd.doc_type = 'invoice'
  GROUP BY fd.id
),
gr AS (
    SELECT fd.id, fd.po_reference,
           coalesce(sum(li.quantity), 0) AS total_qty
      FROM financial_docs fd
 LEFT JOIN line_items li ON li.financial_doc_id = fd.id
     WHERE fd.doc_type = 'gr'
  GROUP BY fd.id
)
SELECT
    po.doc_number                                 AS po_number,
    po.vendor,
    po.doc_date                                   AS po_date,
    po.total_qty                                  AS po_qty,
    inv.total_qty                                 AS inv_qty,
    gr.total_qty                                  AS gr_qty,
    po.total                                      AS po_total,
    inv.total                                     AS inv_total,
    (inv.id IS NOT NULL)                          AS has_invoice,
    (gr.id  IS NOT NULL)                          AS has_gr,
    (po.total_qty = inv.total_qty
     AND inv.total_qty = gr.total_qty)            AS qty_match,
    (po.total = inv.total)                        AS total_match,
    CASE
        WHEN inv.id IS NULL                              THEN 'awaiting_invoice'
        WHEN gr.id IS NULL                               THEN 'awaiting_gr'
        WHEN po.total_qty = inv.total_qty
         AND inv.total_qty = gr.total_qty
         AND po.total     = inv.total                    THEN 'matched'
        ELSE                                                  'discrepancy'
    END                                           AS status
FROM po
LEFT JOIN inv ON inv.po_reference = po.doc_number
LEFT JOIN gr  ON gr.po_reference  = po.doc_number;

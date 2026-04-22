-- Triple Match Audit — schema v1
-- Applied idempotently: safe to re-run.

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

-- Strongly-typed financial facts. One row per PO / invoice / goods receipt.
CREATE TABLE IF NOT EXISTS financial_docs (
    id              uuid PRIMARY KEY REFERENCES documents(id) ON DELETE CASCADE,
    doc_type        text NOT NULL CHECK (doc_type IN ('po','invoice','gr')),
    doc_number      text NOT NULL,           -- e.g. PO-4010, INV-8891, GR-3301
    po_reference    text,                    -- NULL for POs; = a PO's doc_number otherwise
    vendor          text NOT NULL,
    item            text,
    quantity        numeric,
    unit_price      numeric,                 -- NULL for GR
    total           numeric,                 -- NULL for GR
    doc_date        date,
    UNIQUE (doc_type, doc_number)
);
CREATE INDEX IF NOT EXISTS financial_docs_po_ref_idx
    ON financial_docs (po_reference);

-- Free-form supporting docs (emails, memos, amendments, shipping notices).
CREATE TABLE IF NOT EXISTS supporting_docs (
    id              uuid PRIMARY KEY REFERENCES documents(id) ON DELETE CASCADE,
    subtype         text,                    -- email / memo / amendment / shipping
    vendor_guess    text,
    date_guess      date,
    po_mentions     text[],                  -- PO numbers explicitly found in text
    summary         text                     -- optional LLM summary
);
CREATE INDEX IF NOT EXISTS supporting_docs_summary_fts_idx
    ON supporting_docs USING gin (to_tsvector('english', coalesce(summary, '')));

-- How each supporting doc links to an order, with provenance + confidence.
CREATE TABLE IF NOT EXISTS document_links (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    supporting_id   uuid NOT NULL REFERENCES supporting_docs(id) ON DELETE CASCADE,
    po_number       text NOT NULL,
    method          text NOT NULL CHECK (method IN (
                        'explicit_reference',
                        'vendor_date',
                        'llm_classified',
                        'auditor_corrected'
                    )),
    confidence      numeric NOT NULL CHECK (confidence BETWEEN 0 AND 1),
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (supporting_id, po_number)
);

-- Cached LLM analysis of each discrepancy (internal write; not auditor-facing).
CREATE TABLE IF NOT EXISTS discrepancy_analyses (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    po_number       text NOT NULL,
    explanation     text,
    classification  text CHECK (classification IN (
                        'explained','unexplained','false_positive'
                    )),
    confidence      numeric CHECK (confidence BETWEEN 0 AND 1),
    citations       jsonb,                   -- [{doc_id, char_start, char_end}, ...]
    prompt_version  text,
    model_version   text,
    analyzed_at     timestamptz NOT NULL DEFAULT now(),
    input_hash      text NOT NULL            -- hash of inputs; skip re-analysis if unchanged
);
CREATE INDEX IF NOT EXISTS discrepancy_analyses_po_idx
    ON discrepancy_analyses (po_number, analyzed_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS discrepancy_analyses_input_hash_idx
    ON discrepancy_analyses (input_hash);

-- Full-text over raw_text; supports search_supporting_docs and investigation queries.
CREATE INDEX IF NOT EXISTS documents_raw_text_fts_idx
    ON documents USING gin (to_tsvector('english', raw_text));

-- ---------------------------------------------------------------------------
-- Views (the match itself — derived, never stored)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW v_triple_match AS
SELECT
    po.doc_number                                                AS po_number,
    po.vendor,
    po.item,
    po.doc_date                                                  AS po_date,
    po.quantity                                                  AS po_qty,
    inv.quantity                                                 AS inv_qty,
    gr.quantity                                                  AS gr_qty,
    po.unit_price                                                AS po_unit_price,
    inv.unit_price                                               AS inv_unit_price,
    po.total                                                     AS po_total,
    inv.total                                                    AS inv_total,
    (inv.id IS NOT NULL)                                         AS has_invoice,
    (gr.id  IS NOT NULL)                                         AS has_gr,
    (po.quantity = inv.quantity AND inv.quantity = gr.quantity)  AS qty_match,
    (po.unit_price = inv.unit_price)                             AS price_match,
    (po.total = inv.total)                                       AS total_match,
    CASE
        WHEN inv.id IS NULL                              THEN 'awaiting_invoice'
        WHEN gr.id IS NULL                               THEN 'awaiting_gr'
        WHEN po.quantity   = inv.quantity
         AND inv.quantity  = gr.quantity
         AND po.unit_price = inv.unit_price
         AND po.total      = inv.total                   THEN 'matched'
        ELSE                                                  'discrepancy'
    END                                                          AS status
FROM financial_docs po
LEFT JOIN financial_docs inv
       ON inv.po_reference = po.doc_number AND inv.doc_type = 'invoice'
LEFT JOIN financial_docs gr
       ON gr.po_reference  = po.doc_number AND gr.doc_type  = 'gr'
WHERE po.doc_type = 'po';

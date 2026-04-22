"""
Tools:
  * list_pos              — summary table of every PO with match status
  * get_po_detail         — full breakdown for one PO (header, lines, invoice, GR)
  * get_discrepancies     — only POs in 'discrepancy' status, with computed deltas
  * get_supporting_docs   — supporting docs (email/memo/amendment/shipping) linked
                            to a PO, including raw_text for LLM investigation
  * search_documents      — full-text search across all raw document text
"""

from __future__ import annotations

import os
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

import psycopg
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(_PROJECT_ROOT / ".env")

mcp = FastMCP("Triple Match Audit Server")

def _db_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set; cannot connect to Postgres")
    return url


def _connect() -> psycopg.Connection:
    return psycopg.connect(_db_url())


def _jsonable(v: Any) -> Any:
    """Coerce DB values into JSON-safe primitives for MCP tool returns."""
    if isinstance(v, Decimal):
        # Numeric totals — preserve exactness as string. The LLM can parse.
        return str(v)
    if hasattr(v, "isoformat"):  # date / datetime
        return v.isoformat()
    return v


def _rows_to_dicts(cur: psycopg.Cursor) -> list[dict]:
    cols = [d.name for d in cur.description]
    return [
        {c: _jsonable(v) for c, v in zip(cols, row)}
        for row in cur.fetchall()
    ]

@mcp.tool()
def list_pos() -> list[dict]:
    """List every purchase order with its current triple-match status.

    Use this first to get an overview of the audit. Returns one row per PO:
      - po_number, vendor, po_date
      - po_qty, inv_qty, gr_qty           (aggregate quantities)
      - po_total, inv_total               (header totals)
      - has_invoice, has_gr               (booleans)
      - qty_match, total_match            (booleans)
      - status: one of {matched, discrepancy, awaiting_invoice, awaiting_gr}

    Backed by the v_triple_match view, so results always reflect current DB state.
    """
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT po_number, vendor, po_date, po_qty, inv_qty, gr_qty,
                   po_total, inv_total, has_invoice, has_gr,
                   qty_match, total_match, status
              FROM v_triple_match
          ORDER BY po_number
            """
        )
        return _rows_to_dicts(cur)


@mcp.tool()
def get_po_detail(po_number: str) -> dict:
    """Get the full breakdown for a single PO: header, line items, invoice, GR.

    Use this when investigating a specific PO. Returns:
      - po:       { doc_number, vendor, doc_date, total, lines: [...] }
      - invoice:  { doc_number, doc_date, total, lines: [...] } or None
      - gr:       { doc_number, doc_date, lines: [...] } or None
      - match:    row from v_triple_match (status, flags, aggregate qtys)

    Each line item is {line_no, item, quantity, unit_price, line_total}.

    Args:
        po_number: PO doc_number, e.g. "PO-4011".
    """
    with _connect() as conn, conn.cursor() as cur:
        # Find the PO financial doc.
        cur.execute(
            """
            SELECT id, doc_number, vendor, doc_date, total
              FROM financial_docs
             WHERE doc_type = 'po' AND doc_number = %s
            """,
            (po_number,),
        )
        po_row = cur.fetchone()
        if po_row is None:
            return {"error": f"PO {po_number!r} not found"}
        po_id, po_num, vendor, po_date, po_total = po_row

        def _load(doc_id: str) -> list[dict]:
            cur.execute(
                """
                SELECT line_no, item, quantity, unit_price, line_total
                  FROM line_items
                 WHERE financial_doc_id = %s
              ORDER BY line_no
                """,
                (doc_id,),
            )
            return _rows_to_dicts(cur)

        po_lines = _load(po_id)

        # Invoice (if any) referencing this PO.
        cur.execute(
            """
            SELECT id, doc_number, doc_date, total
              FROM financial_docs
             WHERE doc_type = 'invoice' AND po_reference = %s
          ORDER BY doc_date
             LIMIT 1
            """,
            (po_number,),
        )
        inv_row = cur.fetchone()
        invoice: Optional[dict] = None
        if inv_row is not None:
            inv_id, inv_num, inv_date, inv_total = inv_row
            invoice = {
                "doc_number": inv_num,
                "doc_date": _jsonable(inv_date),
                "total": _jsonable(inv_total),
                "lines": _load(inv_id),
            }

        # GR (if any).
        cur.execute(
            """
            SELECT id, doc_number, doc_date
              FROM financial_docs
             WHERE doc_type = 'gr' AND po_reference = %s
          ORDER BY doc_date
             LIMIT 1
            """,
            (po_number,),
        )
        gr_row = cur.fetchone()
        gr: Optional[dict] = None
        if gr_row is not None:
            gr_id, gr_num, gr_date = gr_row
            gr = {
                "doc_number": gr_num,
                "doc_date": _jsonable(gr_date),
                "lines": _load(gr_id),
            }

        # Match row.
        cur.execute(
            """
            SELECT po_number, vendor, po_date, po_qty, inv_qty, gr_qty,
                   po_total, inv_total, has_invoice, has_gr,
                   qty_match, total_match, status
              FROM v_triple_match
             WHERE po_number = %s
            """,
            (po_number,),
        )
        match_rows = _rows_to_dicts(cur)
        match = match_rows[0] if match_rows else None

    return {
        "po": {
            "doc_number": po_num,
            "vendor": vendor,
            "doc_date": _jsonable(po_date),
            "total": _jsonable(po_total),
            "lines": po_lines,
        },
        "invoice": invoice,
        "gr": gr,
        "match": match,
    }


@mcp.tool()
def get_discrepancies() -> list[dict]:
    """List only POs currently in 'discrepancy' status, with computed deltas.

    Discrepancy = invoice and GR both present, but quantities or totals don't match.
    For each discrepancy, returns:
      - po_number, vendor
      - qty_delta_inv: inv_qty - po_qty
      - qty_delta_gr:  gr_qty  - po_qty
      - total_delta:   inv_total - po_total
      - qty_match, total_match  (so caller can see which dimension failed)

    Start here when the user asks "what needs investigating?".
    """
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT po_number, vendor,
                   po_qty, inv_qty, gr_qty,
                   po_total, inv_total,
                   qty_match, total_match,
                   (inv_qty   - po_qty)   AS qty_delta_inv,
                   (gr_qty    - po_qty)   AS qty_delta_gr,
                   (inv_total - po_total) AS total_delta
              FROM v_triple_match
             WHERE status = 'discrepancy'
          ORDER BY po_number
            """
        )
        return _rows_to_dicts(cur)


@mcp.tool()
def get_supporting_docs(po_number: str) -> list[dict]:
    """Get supporting docs (emails, memos, amendments, shipping) linked to a PO.

    Use this after get_po_detail to read human-written context that might explain
    a discrepancy. Each result includes the full raw_text, so the LLM can cite
    specific sentences.

    Returns one entry per linked supporting doc:
      - filename, subtype, doc_date, vendor_guess
      - link_method (explicit_reference | vendor_date | llm_classified | auditor_corrected)
      - link_confidence
      - raw_text  (complete document text for citation)

    Args:
        po_number: PO doc_number, e.g. "PO-4011".
    """
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT d.filename, sd.subtype, sd.date_guess, sd.vendor_guess,
                   dl.method, dl.confidence, d.raw_text
              FROM document_links dl
              JOIN supporting_docs sd ON sd.id = dl.supporting_id
              JOIN documents d         ON d.id  = sd.id
             WHERE dl.po_number = %s
          ORDER BY sd.date_guess NULLS LAST, d.filename
            """,
            (po_number,),
        )
        rows = _rows_to_dicts(cur)
        # Rename keys for clarity in tool output.
        return [
            {
                "filename": r["filename"],
                "subtype": r["subtype"],
                "doc_date": r["date_guess"],
                "vendor_guess": r["vendor_guess"],
                "link_method": r["method"],
                "link_confidence": r["confidence"],
                "raw_text": r["raw_text"],
            }
            for r in rows
        ]


@mcp.tool()
def search_documents(query: str, limit: int = 10) -> list[dict]:
    """Full-text search across every document's raw_text.

    Uses the GIN index on to_tsvector(raw_text). Good for finding mentions
    of items, PO numbers, vendors, or phrases like 'backordered' or 'rush order'
    across the whole corpus — including docs not yet linked to any PO.

    Returns up to `limit` matches ranked by relevance:
      - filename, doc_type, ingested_at
      - snippet  (excerpt with query terms highlighted by ts_headline)
      - rank     (ts_rank_cd score; higher = more relevant)

    Args:
        query: websearch-style query, e.g. 'backordered AND apex' or 'rush order'.
        limit: max results (default 10).
    """
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT d.filename, d.doc_type, d.ingested_at,
                   ts_headline(
                       'english', d.raw_text,
                       websearch_to_tsquery('english', %s),
                       'MaxWords=30, MinWords=10, ShortWord=3'
                   ) AS snippet,
                   ts_rank_cd(
                       to_tsvector('english', d.raw_text),
                       websearch_to_tsquery('english', %s)
                   ) AS rank
              FROM documents d
             WHERE to_tsvector('english', d.raw_text)
                   @@ websearch_to_tsquery('english', %s)
          ORDER BY rank DESC
             LIMIT %s
            """,
            (query, query, query, limit),
        )
        return _rows_to_dicts(cur)


if __name__ == "__main__":
    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
    except Exception as e:
        print(f"MCP server: DB preflight failed: {e}", file=sys.stderr)
        sys.exit(1)
    mcp.run()

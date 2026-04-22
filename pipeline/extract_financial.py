"""
Reads rows from `documents` where doc_type IN ('po','invoice','gr'), parses
the raw text, and populates `financial_docs` + `line_items`.

Handles two formats found in the corpus:
  * Labeled / single-item:   "Item: X\\nQuantity: N\\nUnit Price: $..."
  * Tabular / multi-item:    "Item Qty Unit Price Line Total\\nRow1...\\nRow2..."

Idempotent: skips documents already present in financial_docs.

Usage:
    python pipeline/extract_financial.py
"""

from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

import psycopg
from dotenv import load_dotenv

EXTRACTOR_VERSION = "extract-financial-v1"

DOC_NUMBER_PATTERNS = {
    "po":      re.compile(r"(?mi)^PO Number:\s*(\S+)"),
    "invoice": re.compile(r"(?mi)^Invoice Number:\s*(\S+)"),
    "gr":      re.compile(r"(?mi)^Receipt ID:\s*(\S+)"),
}
VENDOR_RE      = re.compile(r"(?mi)^Vendor:\s*(.+?)\s*$")
PO_REF_RE      = re.compile(r"(?mi)^Reference PO:\s*(\S+)")
DATE_RE        = re.compile(r"(?mi)^Date(?:\s+Received)?:\s*(\d{4}-\d{2}-\d{2})")
TOTAL_RE       = re.compile(r"(?mi)^(?:Total|Subtotal):\s*\$?([\d,]+\.\d{2})")

# Labeled / single-item: separate labeled lines.
LABELED_ITEM_RE       = re.compile(r"(?mi)^Item:\s*(.+?)\s*$")
LABELED_QTY_RE        = re.compile(r"(?mi)^Quantity(?:\s+Received)?:\s*([\d,]+)")
LABELED_UNIT_PRICE_RE = re.compile(r"(?mi)^Unit Price:\s*\$?([\d,]+\.\d{2})")

# Tabular header detector — presence of "Item Qty" on its own line.
TABULAR_HEADER_RE = re.compile(r"(?mi)^\s*Item\s+Qty\b")

# Tabular row patterns.
# Priced (PO/invoice):  <item...> <qty> $<unit_price> $<line_total>
# GR tabular:           <item...> <qty_ordered> <qty_received> [(annotation)]
#   stored quantity = qty_received — the "what actually arrived" fact.
PRICED_LINE_RE = re.compile(
    r"^(?P<item>.+?)\s+(?P<qty>\d[\d,]*)\s+\$(?P<unit_price>[\d,]+\.\d{2})\s+\$(?P<line_total>[\d,]+\.\d{2})\s*$"
)
GR_TABULAR_LINE_RE = re.compile(
    r"^(?P<item>.+?)\s+(?P<ordered>\d[\d,]*)\s+(?P<received>\d[\d,]*)"
    r"(?:\s+\([^)]*\))?\s*$"
)
STOP_LINE_RE = re.compile(r"(?i)^\s*(?:Subtotal|Total|Payment\s+Terms)\s*:")


@dataclass
class Header:
    doc_number: str
    vendor: str
    po_reference: Optional[str]
    doc_date: Optional[str]
    total: Optional[Decimal]


@dataclass
class Line:
    line_no: int
    item: str
    quantity: Decimal
    unit_price: Optional[Decimal]
    line_total: Optional[Decimal]


def _money(s: str) -> Decimal:
    return Decimal(s.replace(",", "").replace("$", "").strip())


def _num(s: str) -> Decimal:
    return Decimal(s.replace(",", "").strip())


def _search(pat: re.Pattern[str], text: str) -> Optional[str]:
    m = pat.search(text)
    return m.group(1).strip() if m else None


def detect_format(text: str) -> str:
    """'tabular' if an 'Item Qty' header line exists; else 'labeled'."""
    return "tabular" if TABULAR_HEADER_RE.search(text) else "labeled"


def extract_header(text: str, doc_type: str) -> Header:
    doc_num_pat = DOC_NUMBER_PATTERNS[doc_type]
    doc_number = _search(doc_num_pat, text)
    if doc_number is None:
        raise ValueError(f"{doc_type}: could not find doc_number")

    vendor = _search(VENDOR_RE, text)
    if vendor is None:
        raise ValueError(f"{doc_type} {doc_number}: missing Vendor")

    po_ref = _search(PO_REF_RE, text) if doc_type in ("invoice", "gr") else None
    doc_date = _search(DATE_RE, text)

    total = None
    if doc_type != "gr":
        raw = _search(TOTAL_RE, text)
        total = _money(raw) if raw is not None else None

    return Header(
        doc_number=doc_number,
        vendor=vendor,
        po_reference=po_ref,
        doc_date=doc_date,
        total=total,
    )

def extract_lines_labeled(text: str, with_price: bool) -> list[Line]:
    item_m = LABELED_ITEM_RE.search(text)
    qty_m  = LABELED_QTY_RE.search(text)
    if not item_m or not qty_m:
        raise ValueError("labeled: missing Item or Quantity line")

    item = item_m.group(1).strip()
    qty  = _num(qty_m.group(1))

    unit_price = line_total = None
    if with_price:
        up_m = LABELED_UNIT_PRICE_RE.search(text)
        if up_m is None:
            raise ValueError("labeled priced doc: missing Unit Price")
        unit_price = _money(up_m.group(1))
        line_total = unit_price * qty  # implicit; no per-line 'Total:' label

    return [Line(
        line_no=1, item=item, quantity=qty,
        unit_price=unit_price, line_total=line_total,
    )]


def extract_lines_tabular(text: str, with_price: bool) -> list[Line]:
    lines = text.splitlines()
    header_idx = next(
        (i for i, ln in enumerate(lines) if TABULAR_HEADER_RE.match(ln)),
        None,
    )
    if header_idx is None:
        raise ValueError("tabular: header row not found")

    pattern = PRICED_LINE_RE if with_price else GR_TABULAR_LINE_RE
    out: list[Line] = []
    for raw in lines[header_idx + 1:]:
        ln = raw.strip()
        if not ln:
            continue
        if STOP_LINE_RE.match(ln):
            break
        m = pattern.match(ln)
        if not m:
            # Row we don't understand — log and stop. Better to flag than guess.
            print(f"    warn: unparseable row: {ln!r}", file=sys.stderr)
            break
        line_no = len(out) + 1
        d = m.groupdict()
        if with_price:
            out.append(Line(
                line_no=line_no,
                item=d["item"].strip(),
                quantity=_num(d["qty"]),
                unit_price=_money(d["unit_price"]),
                line_total=_money(d["line_total"]),
            ))
        else:
            # GR tabular: use received qty as the canonical quantity.
            out.append(Line(
                line_no=line_no,
                item=d["item"].strip(),
                quantity=_num(d["received"]),
                unit_price=None,
                line_total=None,
            ))
    if not out:
        raise ValueError("tabular: no rows parsed")
    return out


def extract(text: str, doc_type: str) -> tuple[Header, list[Line]]:
    header = extract_header(text, doc_type)
    fmt = detect_format(text)
    with_price = doc_type != "gr"
    if fmt == "tabular":
        lines = extract_lines_tabular(text, with_price)
    else:
        lines = extract_lines_labeled(text, with_price)
    return header, lines


def main() -> int:
    load_dotenv()
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("ERROR: DATABASE_URL not set.", file=sys.stderr)
        return 1

    reset = "--reset" in sys.argv[1:]

    inserted = skipped = failed = 0
    with psycopg.connect(url, autocommit=False) as conn:
        with conn.cursor() as cur:
            if reset:
                cur.execute("TRUNCATE line_items, financial_docs CASCADE")
                conn.commit()
                print("  reset: truncated financial_docs + line_items\n")

            cur.execute("""
                SELECT d.id, d.filename, d.doc_type, d.raw_text
                  FROM documents d
             LEFT JOIN financial_docs fd ON fd.id = d.id
                 WHERE d.doc_type IN ('po','invoice','gr')
                   AND fd.id IS NULL
              ORDER BY d.filename
            """)
            rows = cur.fetchall()

        for doc_id, filename, doc_type, raw_text in rows:
            try:
                header, lines = extract(raw_text, doc_type)
            except Exception as e:
                failed += 1
                print(f"  FAIL    {filename}: {e}", file=sys.stderr)
                continue

            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO financial_docs
                            (id, doc_type, doc_number, po_reference, vendor, doc_date, total)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            doc_id, doc_type, header.doc_number, header.po_reference,
                            header.vendor, header.doc_date, header.total,
                        ),
                    )
                    for ln in lines:
                        cur.execute(
                            """
                            INSERT INTO line_items
                                (financial_doc_id, line_no, item, quantity, unit_price, line_total)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """,
                            (doc_id, ln.line_no, ln.item, ln.quantity,
                             ln.unit_price, ln.line_total),
                        )
                conn.commit()
                inserted += 1
                n = len(lines)
                print(f"  ok      {filename:<48} {doc_type:<8} {header.doc_number:<10} lines={n}")
            except Exception as e:
                conn.rollback()
                failed += 1
                print(f"  FAIL    {filename}: DB error: {e}", file=sys.stderr)

        # Summary query.
        with conn.cursor() as cur:
            cur.execute("SELECT doc_type, count(*) FROM financial_docs GROUP BY doc_type ORDER BY doc_type")
            fd_counts = cur.fetchall()
            cur.execute("SELECT count(*) FROM line_items")
            li_count = cur.fetchone()[0]

    print(f"\nInserted {inserted}, skipped {skipped}, failed {failed}")
    print("\nfinancial_docs:")
    for dt, n in fd_counts:
        print(f"  {dt:<10} {n}")
    print(f"line_items total: {li_count}")
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    sys.exit(main())

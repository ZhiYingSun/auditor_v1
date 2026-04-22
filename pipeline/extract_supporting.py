"""
Reads rows from `documents` where doc_type IN
('email','memo','amendment','shipping','unknown') and populates
`supporting_docs` with:
  * subtype       — copied from doc_type (1:1 mapping for v1)
  * date_guess    — YYYY-MM-DD pulled from filename, then raw_text as fallback
  * po_mentions   — all PO-\\d+ matches in raw_text (deduped, order-preserving)
  * vendor_guess  — substring match of filename slug against known vendors
                    (the 5 vendors already in financial_docs)
  * summary       — NULL for v1 (LLM can fill this later for the analyzer)

Idempotent: skips documents already present in supporting_docs.

Usage:
    python pipeline/extract_supporting.py
    python pipeline/extract_supporting.py --reset
"""

from __future__ import annotations

import os
import re
import sys
from typing import Optional

import psycopg
from dotenv import load_dotenv

SUPPORTING_DOC_TYPES = ("email", "memo", "amendment", "shipping", "unknown")

FILENAME_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")
TEXT_DATE_RE     = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
PO_MENTION_RE    = re.compile(r"\bPO-\d+\b")


def extract_date(filename: str, raw_text: str) -> Optional[str]:
    m = FILENAME_DATE_RE.search(filename)
    if m:
        return m.group(1)
    m = TEXT_DATE_RE.search(raw_text)
    return m.group(1) if m else None


def extract_po_mentions(raw_text: str) -> list[str]:
    seen: dict[str, None] = {}
    for m in PO_MENTION_RE.findall(raw_text):
        seen.setdefault(m, None)
    return list(seen.keys())


def guess_vendor(filename: str, known_vendors: list[str]) -> Optional[str]:
    """Match filename slug against known vendor names.

    Filenames look like AMENDMENT-2024-10-05-nova-plastics.pdf.
    We normalize both sides to lowercase alphanum and check substring.
    """
    norm_fname = re.sub(r"[^a-z0-9]+", "", filename.lower())
    best: Optional[str] = None
    best_len = 0
    for v in known_vendors:
        norm_v = re.sub(r"[^a-z0-9]+", "", v.lower())
        if not norm_v:
            continue
        if norm_v in norm_fname and len(norm_v) > best_len:
            best = v
            best_len = len(norm_v)
    return best


def main() -> int:
    load_dotenv()
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("ERROR: DATABASE_URL not set.", file=sys.stderr)
        return 1

    reset = "--reset" in sys.argv[1:]

    inserted = failed = 0
    with psycopg.connect(url, autocommit=False) as conn:
        with conn.cursor() as cur:
            if reset:
                cur.execute("TRUNCATE supporting_docs CASCADE")
                conn.commit()
                print("  reset: truncated supporting_docs\n")

            cur.execute(
                "SELECT DISTINCT vendor FROM financial_docs WHERE vendor IS NOT NULL"
            )
            known_vendors = [r[0] for r in cur.fetchall()]

            cur.execute(
                """
                SELECT d.id, d.filename, d.doc_type, d.raw_text
                  FROM documents d
             LEFT JOIN supporting_docs sd ON sd.id = d.id
                 WHERE d.doc_type = ANY(%s)
                   AND sd.id IS NULL
              ORDER BY d.filename
                """,
                (list(SUPPORTING_DOC_TYPES),),
            )
            rows = cur.fetchall()

        for doc_id, filename, doc_type, raw_text in rows:
            try:
                subtype = doc_type
                date_guess = extract_date(filename, raw_text)
                po_mentions = extract_po_mentions(raw_text)
                vendor_guess = guess_vendor(filename, known_vendors)

                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO supporting_docs
                            (id, subtype, vendor_guess, date_guess, po_mentions, summary)
                        VALUES (%s, %s, %s, %s, %s, NULL)
                        """,
                        (doc_id, subtype, vendor_guess, date_guess, po_mentions),
                    )
                conn.commit()
                inserted += 1
                pos = ",".join(po_mentions) if po_mentions else "-"
                print(
                    f"  ok   {filename:<48} "
                    f"subtype={subtype:<9} date={date_guess or '-':<10} "
                    f"vendor={(vendor_guess or '-'):<22} pos={pos}"
                )
            except Exception as e:
                conn.rollback()
                failed += 1
                print(f"  FAIL  {filename}: {e}", file=sys.stderr)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT subtype, count(*) FROM supporting_docs "
                "GROUP BY subtype ORDER BY subtype"
            )
            counts = cur.fetchall()

    print(f"\nInserted {inserted}, failed {failed}")
    print("\nsupporting_docs by subtype:")
    for st, n in counts:
        print(f"  {st:<10} {n}")
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    sys.exit(main())

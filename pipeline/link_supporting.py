"""
Strategy (in priority order per supporting doc):
  1. explicit_reference — if po_mentions contains POs that exist in
     financial_docs, link each with confidence=1.0.
  2. vendor_date — else if vendor_guess + date_guess are present,
     find POs from that vendor within +/- DATE_WINDOW_DAYS and link
     each with confidence=0.7.
  3. nothing — docs we can't link are left alone; a human (or later
     LLM classifier) can fix them.

Idempotent: uses ON CONFLICT DO NOTHING against the UNIQUE
(supporting_id, po_number) constraint, so re-runs are safe.

Usage:
    python pipeline/link_supporting.py
    python pipeline/link_supporting.py --reset
"""

from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from typing import Optional

import psycopg
from dotenv import load_dotenv

DATE_WINDOW_DAYS = 14


def parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def main() -> int:
    load_dotenv()
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("ERROR: DATABASE_URL not set.", file=sys.stderr)
        return 1

    reset = "--reset" in sys.argv[1:]

    explicit = fuzzy = skipped = 0
    with psycopg.connect(url, autocommit=False) as conn:
        with conn.cursor() as cur:
            if reset:
                cur.execute("TRUNCATE document_links CASCADE")
                conn.commit()
                print("  reset: truncated document_links\n")

            # All PO doc_numbers that actually exist — filters orphan mentions.
            cur.execute(
                "SELECT doc_number FROM financial_docs WHERE doc_type = 'po'"
            )
            known_pos: set[str] = {r[0] for r in cur.fetchall()}

            # All POs with vendor+date, for the vendor_date fallback.
            cur.execute(
                """
                SELECT doc_number, vendor, doc_date
                  FROM financial_docs
                 WHERE doc_type = 'po'
                """
            )
            pos_meta = cur.fetchall()  # list of (doc_number, vendor, date)

            cur.execute(
                """
                SELECT sd.id, d.filename, sd.subtype,
                       sd.vendor_guess, sd.date_guess, sd.po_mentions
                  FROM supporting_docs sd
                  JOIN documents d ON d.id = sd.id
              ORDER BY d.filename
                """
            )
            rows = cur.fetchall()

        for sd_id, filename, subtype, vendor_guess, date_guess, po_mentions in rows:
            po_mentions = po_mentions or []
            matched_pos: list[tuple[str, str, float]] = []  # (po, method, conf)

            # 1. explicit_reference
            for po in po_mentions:
                if po in known_pos:
                    matched_pos.append((po, "explicit_reference", 1.0))

            # 2. vendor_date fallback (only if no explicit matches)
            if not matched_pos and vendor_guess and date_guess:
                d = date_guess if isinstance(date_guess, date) else parse_date(date_guess)
                if d is not None:
                    lo = d - timedelta(days=DATE_WINDOW_DAYS)
                    hi = d + timedelta(days=DATE_WINDOW_DAYS)
                    for po_num, po_vendor, po_date in pos_meta:
                        if po_vendor == vendor_guess and po_date is not None:
                            if lo <= po_date <= hi:
                                matched_pos.append((po_num, "vendor_date", 0.7))

            if not matched_pos:
                skipped += 1
                print(f"  skip  {filename}: no link (pos={po_mentions}, vendor={vendor_guess}, date={date_guess})")
                continue

            with conn.cursor() as cur:
                for po_num, method, conf in matched_pos:
                    cur.execute(
                        """
                        INSERT INTO document_links
                            (supporting_id, po_number, method, confidence)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (supporting_id, po_number) DO NOTHING
                        """,
                        (sd_id, po_num, method, conf),
                    )
                    if method == "explicit_reference":
                        explicit += 1
                    else:
                        fuzzy += 1
            conn.commit()

            summary = ", ".join(f"{p}({m[:3]},{c})" for p, m, c in matched_pos)
            print(f"  ok    {filename:<48} -> {summary}")

        # Final report: one row per link.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT dl.po_number, sd.subtype, d.filename, dl.method, dl.confidence
                  FROM document_links dl
                  JOIN supporting_docs sd ON sd.id = dl.supporting_id
                  JOIN documents d        ON d.id  = sd.id
              ORDER BY dl.po_number, d.filename
                """
            )
            links = cur.fetchall()

    print(f"\nExplicit links: {explicit}, fuzzy links: {fuzzy}, unlinked supporting docs: {skipped}")
    print("\ndocument_links:")
    print(f"  {'po':<10} {'subtype':<10} {'filename':<48} {'method':<20} conf")
    print(f"  {'-'*10} {'-'*10} {'-'*48} {'-'*20} ----")
    for po, subtype, filename, method, conf in links:
        print(f"  {po:<10} {subtype:<10} {filename:<48} {method:<20} {conf}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

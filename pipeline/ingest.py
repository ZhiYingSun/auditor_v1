"""Phase 1: ingest PDFs into the `documents` table.

For each PDF in the given directory (default: ./documents):
  - compute SHA-256; skip if already ingested
  - extract raw text with pdfplumber
  - classify doc_type from the filename prefix
  - insert a row into `documents`

Idempotent: re-running only picks up new files.

Usage:
    python pipeline/ingest.py                # uses ./documents
    python pipeline/ingest.py path/to/dir    # custom path
"""

from __future__ import annotations

import hashlib
import os
import pathlib
import re
import sys

import pdfplumber
import psycopg
from dotenv import load_dotenv

EXTRACTOR_VERSION = "ingest-v1"

# Filename prefix → doc_type. Order matters only if patterns overlap (they don't here).
FILENAME_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"^PO-",         re.IGNORECASE), "po"),
    (re.compile(r"^INV-",        re.IGNORECASE), "invoice"),
    (re.compile(r"^GR-",         re.IGNORECASE), "gr"),
    (re.compile(r"^EMAIL-",      re.IGNORECASE), "email"),
    (re.compile(r"^MEMO-",       re.IGNORECASE), "memo"),
    (re.compile(r"^AMENDMENT-",  re.IGNORECASE), "amendment"),
    (re.compile(r"^SHIPPING-",   re.IGNORECASE), "shipping"),
]


def classify(filename: str) -> str:
    for rx, dt in FILENAME_RULES:
        if rx.match(filename):
            return dt
    return "unknown"


def sha256(path: pathlib.Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def extract_text(path: pathlib.Path) -> tuple[str, int]:
    parts: list[str] = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            parts.append(page.extract_text() or "")
        return "\n".join(parts), len(pdf.pages)


def main() -> int:
    load_dotenv()
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("ERROR: DATABASE_URL not set.", file=sys.stderr)
        return 1

    if len(sys.argv) > 1:
        docs_dir = pathlib.Path(sys.argv[1])
    else:
        docs_dir = pathlib.Path(__file__).parent.parent / "documents"

    pdfs = sorted(docs_dir.glob("*.pdf"))
    if not pdfs:
        print(f"No PDFs found in {docs_dir}", file=sys.stderr)
        return 1

    inserted = skipped = 0
    with psycopg.connect(url) as conn, conn.cursor() as cur:
        for path in pdfs:
            checksum = sha256(path)
            cur.execute("SELECT 1 FROM documents WHERE checksum = %s", (checksum,))
            if cur.fetchone() is not None:
                skipped += 1
                print(f"  skip    {path.name}")
                continue

            doc_type = classify(path.name)
            raw_text, pages = extract_text(path)

            cur.execute(
                """
                INSERT INTO documents
                    (filename, checksum, doc_type, raw_text, page_count, extractor_ver)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (path.name, checksum, doc_type, raw_text, pages, EXTRACTOR_VERSION),
            )
            inserted += 1
            print(f"  ingest  {path.name:<48} doc_type={doc_type}")

        conn.commit()

        # Summary by doc_type.
        cur.execute("""
            SELECT doc_type, count(*)
              FROM documents
             GROUP BY doc_type
             ORDER BY doc_type
        """)
        rows = cur.fetchall()

    print(f"\nInserted {inserted}, skipped {skipped}")
    print("\nBy doc_type:")
    for dt, n in rows:
        print(f"  {dt:<12} {n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

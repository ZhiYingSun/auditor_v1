"""Sanity-check that the schema was applied.

Prints the list of tables, views, and row counts. Exits non-zero if any
expected object is missing.
"""

import os
import sys

import psycopg
from dotenv import load_dotenv

EXPECTED_TABLES = {
    "documents",
    "financial_docs",
    "supporting_docs",
    "document_links",
    "discrepancy_analyses",
}
EXPECTED_VIEWS = {"v_triple_match"}


def main() -> int:
    load_dotenv()
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("ERROR: DATABASE_URL not set.", file=sys.stderr)
        return 1

    with psycopg.connect(url) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT table_name
              FROM information_schema.tables
             WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        """)
        tables = {row[0] for row in cur.fetchall()}

        cur.execute("""
            SELECT table_name
              FROM information_schema.views
             WHERE table_schema = 'public'
        """)
        views = {row[0] for row in cur.fetchall()}

        print("Tables:", sorted(tables))
        print("Views: ", sorted(views))

        missing_t = EXPECTED_TABLES - tables
        missing_v = EXPECTED_VIEWS - views
        if missing_t or missing_v:
            print(f"MISSING tables: {missing_t or '-'}; views: {missing_v or '-'}",
                  file=sys.stderr)
            return 1

        print()
        print("Row counts:")
        for t in sorted(EXPECTED_TABLES):
            cur.execute(f"SELECT count(*) FROM {t}")
            print(f"  {t:<22} {cur.fetchone()[0]}")

    print("\nOK: schema looks good.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

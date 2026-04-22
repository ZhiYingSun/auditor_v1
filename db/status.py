"""Print current triple-match status for all POs.

Renders v_triple_match as a readable table. No psql needed.

Usage:
    python db/status.py
"""

import os
import sys

import psycopg
from dotenv import load_dotenv


COLS = [
    ("po_number",    10),
    ("vendor",       20),
    ("status",       18),
    ("has_invoice",   4),
    ("has_gr",        4),
    ("po_qty",        8),
    ("inv_qty",       8),
    ("gr_qty",        8),
    ("po_total",     12),
    ("inv_total",    12),
    ("total_match",   4),
    ("qty_match",     4),
]


def fmt(val, width: int) -> str:
    if val is None:
        s = "-"
    elif isinstance(val, bool):
        s = "Y" if val else "N"
    else:
        s = str(val)
    return s[:width].ljust(width)


def main() -> int:
    load_dotenv()
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("ERROR: DATABASE_URL not set.", file=sys.stderr)
        return 1

    with psycopg.connect(url) as conn, conn.cursor() as cur:
        cols_sql = ", ".join(c for c, _ in COLS)
        cur.execute(f"SELECT {cols_sql} FROM v_triple_match ORDER BY po_number")
        rows = cur.fetchall()

    header = "  ".join(name.ljust(w) for name, w in COLS)
    print(header)
    print("-" * len(header))
    for r in rows:
        print("  ".join(fmt(v, w) for v, (_, w) in zip(r, COLS)))

    print()
    print(f"{len(rows)} POs")
    return 0


if __name__ == "__main__":
    sys.exit(main())

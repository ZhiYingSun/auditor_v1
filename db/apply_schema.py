"""Apply db/schema.sql to the database in $DATABASE_URL.

Idempotent: every DDL statement uses IF NOT EXISTS / OR REPLACE, so running
this multiple times is safe.
"""

import os
import pathlib
import sys

import psycopg
from dotenv import load_dotenv


def main() -> int:
    load_dotenv()
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("ERROR: DATABASE_URL not set. Copy .env.example to .env and fill it in.",
              file=sys.stderr)
        return 1

    schema_path = pathlib.Path(__file__).parent / "schema.sql"
    sql = schema_path.read_text()

    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()

    print(f"Applied {schema_path} to {_redact(url)}")
    return 0


def _redact(url: str) -> str:
    # Hide password in log output.
    import re
    return re.sub(r"://([^:]+):([^@]+)@", r"://\1:***@", url)


if __name__ == "__main__":
    sys.exit(main())

# Setup

How to install and run the Triple Match Audit Tool on a fresh machine.

## Prerequisites

- Python 3.10+
- A Postgres database (we use [Neon](https://neon.tech) — free tier works)
- [Claude Desktop](https://claude.ai/download) (for the MCP integration)

## 1. Clone and install

```bash
git clone <repo-url> auditor
cd auditor

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r optional/mcp_scaffold/requirements.txt
```

## 2. Create a Neon database

1. Sign up at [neon.tech](https://neon.tech) and create a project.
2. Copy the **connection string** from the dashboard (starts with `postgresql://`).
3. Create `.env` in the project root:

```bash
cp .env.example .env
```

Edit `.env` and paste your connection string:

```
DATABASE_URL=postgresql://user:password@ep-xxx.neon.tech/neondb?sslmode=require
```

> Any Postgres works — not just Neon. Point `DATABASE_URL` at a local instance
> if you prefer (`postgresql://localhost/auditor`).

## 3. Apply the schema

```bash
python db/apply_schema.py
python db/verify.py     # sanity check: expected tables + views exist
```

## 4. Run the pipeline

Order matters — each step depends on the previous.

```bash
python pipeline/ingest.py              # pdfs -> documents table
python pipeline/extract_financial.py   # po/invoice/gr headers + line items
python pipeline/extract_supporting.py  # emails/memos/amendments metadata
python pipeline/link_supporting.py     # link supporting docs to POs
```

Each script is idempotent — safe to re-run. Use `--reset` on the extractors
to truncate and reprocess from scratch.

## 5. Check the state

```bash
python db/status.py
```

Prints the `v_triple_match` view as a formatted table.

## 6. Wire up Claude Desktop (MCP)

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`
(macOS) or the equivalent path on your OS:

```json
{
  "mcpServers": {
    "triple-match": {
      "command": "/absolute/path/to/python",
      "args": ["/absolute/path/to/auditor/optional/mcp_scaffold/server.py"]
    }
  }
}
```

- Use an **absolute** python path (Claude Desktop doesn't inherit your shell PATH).
  Find it with `which python` inside your activated venv.
- If the file already has a `mcpServers` key, merge — don't overwrite.

Fully quit and relaunch Claude Desktop (⌘Q on macOS). You should see 5 tools
under `triple-match` in a new chat: `list_pos`, `get_discrepancies`,
`get_po_detail`, `get_supporting_docs`, `search_documents`.

## 7. Try it

In Claude Desktop:

> "What discrepancies exist in the audit?"

Claude should call `get_discrepancies`, drill into specific POs with
`get_po_detail` + `get_supporting_docs`, and cite the emails/memos that
explain each mismatch.

## Troubleshooting

- **Tools don't appear in Claude Desktop** — check logs at
  `~/Library/Logs/Claude/mcp*.log`. Usually a bad python path or a missing dep.
- **`DATABASE_URL` error** — confirm `.env` is in the project root and
  doesn't start with `psql '` (Neon's UI shows a full `psql` command; you
  only need the URL part).
- **First query is slow** — Neon's serverless compute cold-starts in ~1–3s
  after idle. Subsequent queries are fast.
- **`psycopg` not found** — you're likely running a different python than the
  one you pip-installed into. Check `which python` matches the interpreter in
  your MCP config.

# MCP Scaffold (Optional)

A starter MCP server if you want to expose your tools via [Model Context Protocol](https://modelcontextprotocol.io/).

## Setup

```bash
pip install -r requirements.txt
python server.py
```

Requires Python 3.10+.

## Connecting to a client

**Claude Desktop** — add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "triple-match": {
      "command": "python",
      "args": ["/absolute/path/to/server.py"]
    }
  }
}
```

**Claude Code** — run from this directory:

```bash
claude mcp add triple-match python server.py
```

**Cursor** — add to `.cursor/mcp.json` in your project root:

```json
{
  "mcpServers": {
    "triple-match": {
      "command": "python",
      "args": ["/absolute/path/to/server.py"]
    }
  }
}
```

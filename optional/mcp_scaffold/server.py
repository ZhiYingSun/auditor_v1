from mcp.server.fastmcp import FastMCP

mcp = FastMCP("Triple Match Audit Server")


@mcp.tool()
def list_documents() -> list[dict]:
    """List all available documents in the audit folder.

    Returns document filenames and basic metadata for each file
    in the documents directory.
    """
    import os

    docs_dir = os.path.join(os.path.dirname(__file__), "documents")
    documents = []
    for filename in sorted(os.listdir(docs_dir)):
        if filename.endswith(".pdf"):
            filepath = os.path.join(docs_dir, filename)
            documents.append(
                {
                    "filename": filename,
                    "size_bytes": os.path.getsize(filepath),
                }
            )
    return documents


# ---------------------------------------------------------------
# Add your tools below. Each tool should have:
#   - A clear, descriptive name
#   - Type-hinted parameters
#   - A docstring that helps Claude understand when and how to use it
# ---------------------------------------------------------------


if __name__ == "__main__":
    mcp.run()

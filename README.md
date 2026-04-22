# Triple Match Audit Tool

## Context

Our auditors use Claude to help verify financial transactions. They need Claude
to have access to their internal documents so it can search, compare, and
investigate discrepancies.

You're given a folder of documents from a recent audit. Your job is to build a
system that helps an auditor perform a **triple match** — matching purchase
orders, invoices, and goods receipts to verify transactions.

## What's Provided

- `documents/` — PDFs from the audit, including:
  - **Financial documents**: purchase orders (PO), invoices, and goods receipts
  - **Supporting documents**: vendor emails, internal memos, contract amendments,
    and shipping notices that provide context for transactions
- `optional/` — An MCP server scaffold, if you want to use it. Entirely optional.

## What to Build

Build a system that lets an auditor (with Claude's help) find transactions,
check whether POs, invoices, and goods receipts agree, and investigate any
discrepancies using the supporting documents.

How you structure this is up to you. Think about what data you need, how to get
it out of the PDFs, how to make it searchable, and how to expose it so Claude
can use it effectively.

The system should handle new documents being added over time — not just the
ones in this folder.

You can use any tools or libraries. Install whatever you need.
**Clarity of thinking matters most** — a half-built system with thoughtful
decisions is better than a complete one you can't explain.

## Getting Started

```bash
pip install -r requirements.txt
```

This installs common PDF parsing libraries. Add whatever else you need.

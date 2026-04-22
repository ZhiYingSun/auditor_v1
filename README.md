### Auditor
auditor is a system that helps auditors verify financial transactions by cross-checking purchase orders, invoices, and goods receipts (the "triple match"), and explains discrepancies using supporting documents.                                                                                                                                 
                                                                                                                                                                                     
#### Flow:                                                                                                                                                                              
  1. Ingest and parse PDFs into a Postgres database                                                                                                                 
  2. Extract financial docs (PO / invoice / GR) and transform into structured rows with line items; supporting docs (emails, memos, amendments, shipping notices) get metadata extracted       
  3. Link supporting docs with their related POs                                                                                                                  
  4. Match whether PO / invoice / GR agree per transaction                                                                                             
  5. Surface via MCP — an MCP server exposes 5 tools (list_pos, get_discrepancies, get_po_detail, get_supporting_docs, search_documents) so Claude Desktop can investigate           
  discrepancies and cite the emails/memos explaining them                                                                                                                            
                                                                                                                                                                                     
  Stack: Python pipeline, Postgres (Neon), MCP server for Claude Desktop integration. New PDFs can be added over time — the pipeline is idempotent.   

### Auditor in action
  
#### Auditor scans for discrepancies:
  <img width="826" height="461" alt="image" src="https://github.com/user-attachments/assets/7a7951e4-7bb0-445e-a445-5ae5de611128" />

#### Auditor reads through supporting documents  
<img width="814" height="725" alt="image" src="https://github.com/user-attachments/assets/b8893cab-19a2-4ce6-95f7-25a8212e26c8" />

### Design

Design files are versioned, the most recent version is [here](https://github.com/ZhiYingSun/auditor_v1/blob/main/design_v3.txt)

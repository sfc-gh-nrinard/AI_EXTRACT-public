## Extract Anything (Cortex AI_EXTRACT + Streamlit)

> Upload any document, extract structured data with AI_EXTRACT, validate with AI_COMPLETE, and approve results – all inside Snowsight.

### Highlights
- ⚙️ Configurable doc types and prompts (no code changes required)
- 📄 Multi‑page PDF preview with quick paging
- 🤖 Classify → extract → validate pipeline
- ✅ One‑click review and approval

### Prerequisites
- Snowflake account with Cortex AI enabled
- Role with ACCOUNTADMIN privileges (for initial setup)
- Access to create databases, schemas, stages, and integration

### Setup
1) Run the SQL setup script at `sql_scripts/demo_setup.sql`

2) Load documents (use the app’s Upload tab to add PDFs/images).

3) Processing can be triggered in two ways:
   - From the app Upload tab (calls `PROCESS_ONE_FILE` per file)
   - Or create a task to run `PROCESS_RAW()` for batch processing

4) Open the Streamlit app in Snowsight (Projects → Streamlit) named `AI_EXTRACT_ANYTHING` to review, edit, and approve records.

### Quick start
1. Go to Prompts → create a document type and add fields/prompts.
2. Go to Upload → add one or more PDFs/images.
3. Go to Review → select a record, edit values if needed, and Approve.

### How it works
- Define document types and prompts in the Prompts tab (saved to `DOC_TYPES` and `DOC_TYPE_PROMPTS`).
- Upload files → server‑side pipeline runs: classify (AI_EXTRACT) → extract (AI_EXTRACT with your prompts) → validate (AI_COMPLETE) → write to `RAW`.
- Review tab shows a PDF preview (single/multi‑page) and dynamic editor; Approve updates `RAW.extract_json` and sets `approved=TRUE` via `APPROVE_RECORD`.
- Validation results are saved to `RAW.validation_json` and surfaced as VALID / REVIEW with notes.

### Objects created
| Type | Name | Purpose |
|---|---|---|
| Database/Schema | `AI_EXTRACT_DEMOS.EXTRACT_ANYTHING` | App workspace |
| Stages | `STREAMLIT_STAGE`, `DOCS_ROUTER_STAGE` (+ `DOCS_ROUTER_STREAM`) | App code/files; document ingress |
| Tables | `RAW`, `DOC_TYPES`, `DOC_TYPE_PROMPTS` | Results and configuration |
| Procedures | `PROCESS_RAW`, `PROCESS_ONE_FILE`, `UPSERT_DOC_TYPE`, `REPLACE_PROMPTS`, `APPROVE_RECORD` | Snowflake pipeline & CRUD |
| Streamlit | `AI_EXTRACT_ANYTHING` | App UI |

### Documentation
- AI_EXTRACT: https://docs.snowflake.com/en/sql-reference/functions/ai_extract
- Streamlit in Snowflake: https://docs.snowflake.com/en/developer-guide/streamlit/getting-started.html

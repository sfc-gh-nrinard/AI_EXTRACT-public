import json
from typing import Any, Dict, Optional
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pypdfium2 as pdfium


# --- Configuration ---
DB_NAME = "AI_EXTRACT_DEMOS"
SCHEMA_NAME = "EXTRACT_ANYTHING"
STAGE_NAME = "DOCS_ROUTER_STAGE"

RAW_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.RAW"
DOC_TYPES_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.DOC_TYPES"
DOC_PROMPTS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.DOC_TYPE_PROMPTS"


# --- Helpers ---
def get_file_type(filename: Optional[str]) -> str:
    if not filename:
        return "unknown"
    ext = filename.lower().split(".")[-1] if "." in filename else ""
    if ext == "pdf":
        return "pdf"
    if ext in ["png", "jpg", "jpeg", "gif", "bmp", "webp"]:
        return "image"
    return "unknown"


def ensure_dict(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return {"raw": value}
    return {"value": value}


def extract_response_fields(extract_json: Any) -> Dict[str, Any]:
    obj = ensure_dict(extract_json)
    payload = obj.get("response", obj)
    normalized: Dict[str, Any] = {}
    for key, val in payload.items():
        if isinstance(val, list):
            normalized[key] = ", ".join([str(v) for v in val])
        else:
            normalized[key] = val
    return normalized


def escape_json_for_sql(json_str: str) -> str:
    # Escape single quotes and backslashes for safe SQL string literal
    return json_str.replace("\\", "\\\\").replace("'", "''")


def esc(value: Any) -> str:
    s = "" if value is None else str(value)
    return s.replace("'", "''")


@st.cache_data(show_spinner=False)
def get_presigned_url(_session, file_name: str) -> Optional[str]:
    try:
        sql = f"SELECT GET_PRESIGNED_URL('@{DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME}', '{file_name}') AS URL"
        row = _session.sql(sql).collect()[0]
        return row["URL"]
    except Exception:
        return None


@st.cache_data(show_spinner=False)
def fetch_stage_file(_session, file_name: str) -> Optional[bytes]:
        stage_path = f"@{DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME}/{file_name}"
        stream = _session.file.get_stream(stage_path, decompress=False)
        data = stream.read()
        stream.close()
        return data


def render_document_preview(file_name: str, file_url: Optional[str], file_bytes: Optional[bytes], scale: float = 2.0) -> None:
    ftype = get_file_type(file_name)
    if ftype == "image":
        if file_bytes:
            st.image(file_bytes, use_container_width=True)
        elif file_url:
            st.image(file_url, use_container_width=True)
        else:
            st.info("No document data available.")
    elif ftype == "pdf":
        if file_bytes and pdfium is not None:
            try:
                pdf = pdfium.PdfDocument(file_bytes)
                total_pages = len(pdf)
                if total_pages <= 0:
                    st.info("PDF has no pages.")
                elif total_pages == 1:
                    # Single page: render without slider/navigation
                    page = pdf[0]
                    bitmap = page.render(scale=scale, rotation=0)
                    pil_image = bitmap.to_pil()
                    st.image(pil_image, use_container_width=True)
                    st.caption("Page 1 of 1")
                else:
                    # Stable session keys for page state and slider
                    state_key = f"pdf_page::{file_name}"
                    slider_key = f"pdf_page_slider::{file_name}"
                    current = int(st.session_state.get(state_key, 1))
                    # Pagination controls
                    c_nav1, c_nav2, c_nav3 = st.columns([1, 6, 1])
                    with c_nav1:
                        prev_clicked = st.button(
                            "◀",
                            key=f"prev_btn::{file_name}",
                            disabled=(current <= 1),
                        )
                    with c_nav3:
                        next_clicked = st.button(
                            "▶",
                            key=f"next_btn::{file_name}",
                            disabled=(current >= total_pages),
                        )
                    # Apply button changes first
                    if prev_clicked:
                        current = max(1, current - 1)
                    if next_clicked:
                        current = min(total_pages, current + 1)
                    # Then render slider using the (possibly updated) current value
                    with c_nav2:
                        slider_val = st.slider(
                            "Page",
                            min_value=1,
                            max_value=total_pages,
                            value=current,
                            key=slider_key,
                            label_visibility="collapsed",
                        )
                    # If slider moved, update current
                    if int(slider_val) != current:
                        current = int(slider_val)
                    # Persist current page
                    st.session_state[state_key] = current

                    page_index = current - 1
                    page = pdf[page_index]
                    bitmap = page.render(scale=scale, rotation=0)
                    pil_image = bitmap.to_pil()
                    st.image(pil_image, use_container_width=True)
                    st.caption(f"Page {current} of {total_pages}")
            except Exception:
                st.info("PDF preview unavailable.")
        # Always provide download button when URL is available
        if file_url:
            st.link_button("Download PDF", file_url, use_container_width=True)
        elif not file_bytes:
            st.info("PDF preview unavailable.")
    else:
        if file_url:
            st.link_button("Download file", file_url, use_container_width=True)
        else:
            st.info("Unsupported file type for preview.")


# --- App UI ---
st.set_page_config(layout="wide", page_title="Extract Anything")
# Global styles & theming
st.markdown(
    """
    <style>
    :root { --accent:#2E7CF6; --ok:#1b5e20; --warn:#7c2d12; --muted:#64748b; --bg:#f8fafc; }
    .stApp { background: var(--bg); }
    .block-container { padding-top: 0.75rem; }
    h1 { letter-spacing: -0.02em; }
    .badge { display:inline-block; padding:4px 10px; border-radius:999px; font-size:12px; font-weight:600; margin-right:6px; }
    .badge.type { background:#eef2ff; color:#1e3a8a; }
    .badge.ok { background:#e7f5ee; color:#1b5e20; }
    .badge.warn { background:#fff4e5; color:#7c2d12; }
    .card { padding:16px; border:1px solid #e2e8f0; border-radius:12px; background:#ffffff; box-shadow: 0 1px 2px rgba(16,24,40,0.04); }
    .card:hover { box-shadow: 0 4px 12px rgba(16,24,40,0.08); transition: box-shadow .2s ease; }
    .section-title { font-weight:600; color:#334155; margin: 6px 0 2px 0; }
    .pill { display:inline-flex; align-items:center; gap:6px; padding:6px 10px; border-radius:999px; background:#f1f5f9; color:#334155; font-size:12px; }
    </style>
    """,
    unsafe_allow_html=True,
)
st.markdown(
    """
    <div style="display:flex; align-items:center; justify-content:space-between;">
      <div>
        <h1 style="margin:0;">Extract Anything</h1>
        <div style="color:var(--muted);">Classify, extract, and approve any document</div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# Snowflake session
try:
    session = get_active_session()
except Exception as e:
    st.error(f"Error getting Snowflake session: {e}")
    st.stop()


# Cache control helper
if "_cache_buster" not in st.session_state:
    st.session_state["_cache_buster"] = 0

def _bump_cache() -> None:
    st.session_state["_cache_buster"] = int(st.session_state.get("_cache_buster", 0)) + 1


@st.cache_data(show_spinner=False, ttl=10)
def list_doc_types(_buster: int):
    rows = session.sql(f"SELECT document_type FROM {DOC_TYPES_TABLE} ORDER BY document_type").collect()
    return [r[0] for r in rows]


@st.cache_data(show_spinner=False, ttl=10)
def load_prompts(doc_type: str, _buster: int):
    sql = (
            f"SELECT field_name, retrieval_prompt, sort_order "
            f"FROM {DOC_PROMPTS_TABLE} WHERE document_type = '{esc(doc_type)}' "
            f"ORDER BY sort_order, field_name"
    )
    df = session.sql(sql).to_pandas()
    df.rename(columns=str.lower, inplace=True)
    return df


def upsert_doc_type(doc_type: str, description: str) -> None:
    # Delegate to Snowflake procedure
    sql = f"CALL {DB_NAME}.{SCHEMA_NAME}.UPSERT_DOC_TYPE('{esc(doc_type)}', '{esc(description)}')"
    session.sql(sql).collect()


def replace_prompts(doc_type: str, prompts_df) -> int:
    # Build JSON array of prompt objects and delegate to procedure
    if prompts_df is None or getattr(prompts_df, 'empty', True):
        # Still clear prompts via proc with empty array
        payload = "[]"
        sql = (
            f"CALL {DB_NAME}.{SCHEMA_NAME}.REPLACE_PROMPTS('" + esc(doc_type) + f"', PARSE_JSON('{escape_json_for_sql(payload)}'))"
        )
        session.sql(sql).collect()
        return 0
    rows = []
    for _, row in prompts_df.iterrows():
        field_raw = row.get("field_name", "")
        prompt_raw = row.get("retrieval_prompt", "")
        so = row.get("sort_order")
        try:
            so_val = int(so) if str(so).strip() != '' else 0
        except Exception:
            so_val = 0
        # Normalize list/colon formats
        if (prompt_raw in (None, "")) and isinstance(field_raw, str) and (":" in field_raw):
            parts = field_raw.split(":", 1)
            if len(parts) == 2:
                field_raw, prompt_raw = parts[0], parts[1]
        field = str(field_raw).strip()
        prompt = str(prompt_raw).strip()
        if field == '' or prompt == '':
            continue
        rows.append({"field_name": field, "retrieval_prompt": prompt, "sort_order": so_val})
    import json as _json
    payload = _json.dumps(rows)
    sql = (
        f"CALL {DB_NAME}.{SCHEMA_NAME}.REPLACE_PROMPTS('" + esc(doc_type) + f"', PARSE_JSON('{escape_json_for_sql(payload)}'))"
    )
    session.sql(sql).collect()
    return len(rows)
@st.cache_data(show_spinner=False, ttl=10)
def load_records(doc_type: str, approval_filter: str, _buster: int):
    where_clauses = []
    if doc_type and doc_type != "All":
        where_clauses.append(f"document_type = '{esc(doc_type)}'")

    approved_cond_true = "approved = TRUE"
    approved_cond_false = "approved = FALSE"

    # Apply approval filter by join presence
    if approval_filter == "Approved":
        where_clauses.append(approved_cond_true)
    elif approval_filter == "Not Approved":
        where_clauses.append(approved_cond_false)

    where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    sql = f"""
        SELECT r.file_name, r.file_url, r.document_type, r.extract_json, r.validation_json, r.created_at, r.approved
        FROM {RAW_TABLE} r
        {where_sql}
        ORDER BY r.created_at DESC
    """
    return session.sql(sql).to_pandas()


# --- Tabs Navigation ---
tab_prompts, tab_upload, tab_review = st.tabs(["Prompts", "Upload", "Review"])


# --- Help Sidebar ---
with st.sidebar:
    st.subheader("How to use this app")
    st.markdown(
        """
        - **Prompts**: Define the fields and prompts for each document type.
          - Add a new document type or select an existing one.
          - Enter `field_name`.
          - Enter a concise, specific **retrieval_prompt** the model should answer.
          - Use the optional **sort_order** to control field display order.
        - **Upload**: Add PDF/images to process.
          - Select files and click "Upload & Process".
          - Each file is classified, matched to a document type, and extracted using your prompts.
        - **Review**: Inspect, edit, and approve extracted results.
          - Pick a record, adjust values, then click **Approve & Save**.
          - Approved records are flagged and remain visible for auditing.

        ---
        **Snowflake resources**
        - [Streamlit in Snowflake – Getting started](https://docs.snowflake.com/en/developer-guide/streamlit/getting-started.html)
        - [Create and edit Streamlit UI in Snowsight](https://docs.snowflake.com/en/developer-guide/streamlit/create-streamlit-ui)
        - [Cortex AI – AI Extract overview](https://docs.snowflake.com/en/user-guide/snowflake-cortex)
        """
    )


with tab_prompts:
    st.subheader("📚 Prompt Manager")
    dtype_list = list_doc_types(st.session_state.get("_cache_buster", 0))
    col_a, col_b = st.columns([1, 2])
    with col_a:
        existing = ["(New)"] + dtype_list
        sel = st.selectbox("Document Type", existing, index=0, key="pm_dtype_sel")
        if sel == "(New)":
            new_type = st.text_input("New type name", value="")
            desc = st.text_input("Description", value="")
            active_type = new_type.strip()
        else:
            desc_row = session.sql(f"SELECT description FROM {DOC_TYPES_TABLE} WHERE document_type = '{esc(sel)}'").collect()
            desc = desc_row[0][0] if desc_row else ""
            st.text_input("Description", value=str(desc or ""), key="pm_desc_readonly", disabled=True)
            active_type = sel
    with col_b:
        st.caption("Define fields and retrieval prompts for the selected type.")
        st.markdown("<div class='card' style='margin-bottom:8px;'><div class='section-title'>Tips</div><div style='font-size:12px; color:var(--muted);'>Use clear field names. Write specific, unambiguous prompts; prefer declarative questions over keywords. The document description is utilized to provide context for the Tier 1 Cortex AI data validation.</div></div>", unsafe_allow_html=True)
        rc1, rc2 = st.columns([1,1])
        with rc1:
            refresh_now = st.button("↻ Refresh", help="Reload types and prompts", use_container_width=True, key="pm_refresh")
        with rc2:
            st.empty()
        if refresh_now:
            _bump_cache()
        if active_type:
            data = load_prompts(active_type, st.session_state.get("_cache_buster", 0))
        else:
            data = None
        # Ensure editor has expected columns when empty
        try:
            import pandas as pd  # local import to avoid top-level dependency if unused
        except Exception:
            pd = None  # type: ignore[assignment]
        if data is None or getattr(data, 'empty', True):
            if pd is not None:
                data = pd.DataFrame(columns=["field_name", "retrieval_prompt", "sort_order"])
        edited = st.data_editor(
            data if data is not None else None,
            num_rows="dynamic",
            use_container_width=True,
            key=f"pm_grid_{active_type or 'new'}",
            column_config={
                "field_name": st.column_config.TextColumn("field_name"),
                "retrieval_prompt": st.column_config.TextColumn("retrieval_prompt", width="large"),
                "sort_order": st.column_config.NumberColumn("sort_order", step=1),
            },
        )
        c1, c2 = st.columns([1,1])
        with c1:
            if st.button("Save Prompts", use_container_width=True, disabled=(not active_type)):
                try:
                    dtype_val = (active_type or "").strip()
                    if not dtype_val:
                        st.error("Document type is required.")
                    else:
                        upsert_doc_type(dtype_val, desc if sel == "(New)" else (desc or ""))
                        inserted = replace_prompts(dtype_val, edited)
                    _bump_cache()
                    if inserted > 0:
                        st.success(f"Prompts saved ({inserted}).")
                        # Show a live preview of saved prompts
                        latest = load_prompts(dtype_val, st.session_state.get("_cache_buster", 0))
                        if latest is not None:
                            st.dataframe(latest, use_container_width=True)
                    else:
                        st.warning("No prompts saved. Ensure 'field_name' and 'retrieval_prompt' are filled.")
                except Exception as e:
                    st.error(f"Error saving prompts: {e}")
        with c2:
            st.caption("Saving replaces prompts for this type.")

with tab_upload:
    st.subheader("⬆️ Upload Documents")
    files = st.file_uploader("Upload PDFs or images", type=["pdf","png","jpg","jpeg","gif","bmp","webp"], accept_multiple_files=True)
    if files:
        names = [f.name for f in files]
        st.markdown(f"<div class='card'><div class='section-title'>Queued files</div><div class='pill'>{len(names)} selected</div><div style='margin-top:8px; font-size:12px; color:var(--muted);'>" + ", ".join(names) + "</div></div>", unsafe_allow_html=True)
    if st.button("Upload & Process", use_container_width=True, disabled=not files):
        uploaded = 0
        errors = []
        filenames = []
        for f in files or []:
            try:
                import io
                stage_path = f"@{DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME}/{f.name}"
                byte_stream = io.BytesIO(f.getvalue())
                session.file.put_stream(byte_stream, stage_path, auto_compress=False, overwrite=True)  # type: ignore[attr-defined]
                uploaded += 1
                filenames.append(f.name)
            except Exception as e:
                errors.append(f"{f.name}: {e}")
        try:
            # Ensure stage directory metadata is current for stream detection
            session.sql(f"ALTER STAGE {DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME} REFRESH").collect()
            # Delegate processing to Snowflake stored procedure per file
            for fname in filenames:
                session.sql(f"CALL {DB_NAME}.{SCHEMA_NAME}.PROCESS_ONE_FILE('{esc(fname)}')").collect()
        except Exception as e:
            errors.append(f"Processing error: {e}")
        if uploaded and not errors:
            st.success(f"Uploaded and processed {uploaded} file(s).")
        elif uploaded:
            st.warning(f"Uploaded {uploaded} file(s) with some errors:\n" + "\n".join(errors))
        else:
            st.error("No files uploaded. See errors above.")
            if errors:
                st.error("\n".join(errors))
with tab_review:
    # Filters inline on Review tab
    dtypes = list_doc_types(st.session_state.get("_cache_buster", 0))
    dtype_options = ["All"] + dtypes + (["NO_MATCH"] if "NO_MATCH" not in dtypes else [])
    fc1, fc2 = st.columns([2,1])
    with fc1:
        doc_type = st.selectbox("Document Type", dtype_options, index=0, key="filter_doc_type")
    with fc2:
        approval_filter = st.selectbox("Approval Status", ["All", "Approved", "Not Approved"], index=0, key="filter_approval")

    # Review
    records_df = load_records(doc_type, approval_filter, st.session_state.get("_cache_buster", 0))
    if records_df is None or records_df.empty:
        st.info("No records found.")
        st.stop()
    st.subheader("Record Detail")
    selected = st.selectbox(
        "Select a record by file name",
        options=[r.get("FILE_NAME") for _, r in records_df.iterrows()],
    )
    detail = records_df[records_df["FILE_NAME"] == selected].iloc[0]

    approved = bool(detail.get("APPROVED"))
    dtype = str(detail.get("DOCUMENT_TYPE") or "").upper()
    when = detail.get("CREATED_AT")
    when_str = when.strftime('%Y-%m-%d %H:%M') if hasattr(when, 'strftime') else str(when)
    status_html = '<span class="badge ok">APPROVED</span>' if approved else '<span class="badge warn">NOT APPROVED</span>'
    # Validation card
    v = ensure_dict(detail.get("VALIDATION_JSON"))
    v_resp = ensure_dict(v.get("response", v))
    v_valid = str(v_resp.get("valid", "")).lower() in ("true", "yes", "1")
    v_notes = v_resp.get("notes") or v_resp.get("message") or ""
    val_badge = '<span class="badge ok">VALID</span>' if v_valid else '<span class="badge warn">REVIEW</span>'
    st.markdown(
        f"""
        <div class="card" style="margin-bottom:8px;">
          <div>{val_badge} <span style=\"color:var(--muted); font-size:12px;\">AI validation</span></div>
          <div style=\"margin-top:6px; font-size:12px; color:#111827;\">{esc(v_notes)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        f"""
        <div class="card">
          <div>
            <span class="badge type">{dtype}</span>
            {status_html}
          </div>
          <div style=\"margin-top:8px; color:var(--muted); font-size:12px;\">Processed: {when_str}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.divider()

    cols = st.columns([1, 1])

    with cols[0]:
        st.markdown("### ✏️ Edit & Approve")
        resp = extract_response_fields(detail.get("EXTRACT_JSON"))
        if not resp:
            st.info("No extracted fields.")
        else:
            # Generic editor for any document type
            # Convert dict to table for editing
            import pandas as pd
            items = sorted(list(resp.items()), key=lambda kv: kv[0])
            df = pd.DataFrame(items, columns=["Field Name", "Extracted Value"])
            edited_df = st.data_editor(df, num_rows="dynamic", use_container_width=True, key="generic_editor")
            submitted = st.button("✅ Approve & Save", use_container_width=True)
            if submitted:
                try:
                    # Rebuild dict
                    edited = {}
                    for _, r in edited_df.iterrows():
                        fname = str(r.get("Field Name", "")).strip()
                        fval = r.get("Extracted Value")
                        if fname != "":
                            edited[fname] = fval
                    file_name = detail.get("FILE_NAME")
                    payload = json.dumps(edited)
                    # Delegate approval to Snowflake proc
                    session.sql(
                        f"CALL {DB_NAME}.{SCHEMA_NAME}.APPROVE_RECORD('{esc(file_name)}', PARSE_JSON('{escape_json_for_sql(payload)}'))"
                    ).collect()
                    # Refresh caches so approval state is visible immediately
                    _bump_cache()
                    load_records.clear()
                    # Force a rerun to refresh widgets
                    st.rerun()
                except Exception as e:
                    st.error(f"Error saving approval: {e}")

    with cols[1]:
        st.markdown("### 📄 Original Document Preview")
        file_name = detail.get("FILE_NAME")
        url = get_presigned_url(session, file_name)
        data = fetch_stage_file(session, file_name)
        ftype = get_file_type(file_name)
        container_class = "card"
        st.markdown(f"<div class='{container_class}'>", unsafe_allow_html=True)
        if ftype == "pdf":
            render_document_preview(file_name, url, data, scale=1.5)
        else:
            render_document_preview(file_name, url, data, scale=2.0)
        st.markdown("</div>", unsafe_allow_html=True)
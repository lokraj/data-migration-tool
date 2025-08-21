# streamlit_app.py
# Professional Dual-DB Migration Tool (PostgreSQL / MSSQL / MySQL)
# - Connect to two databases (A & B) simultaneously
# - Choose SOURCE and DESTINATION
# - Map columns visually and migrate in chunks
# - Upload Excel/CSV and load into any connected DB with column mapping

from urllib.parse import quote_plus
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text, inspect, Table, MetaData
from sqlalchemy.engine import URL

# ----------------------------- Page & Tabs -----------------------------
st.set_page_config(page_title="Pro Data Migration Tool", layout="wide")
st.title("🚚 Data Migration Tool")
st.caption("Connect two databases (PostgreSQL / MSSQL / MySQL), map fields visually, and migrate data safely.")

tab_main, tab_file = st.tabs(["DB ↔ DB Migration", "📄 File → 🗄️ DB Upload"])

# ----------------------------- Helpers (module-level so both tabs can use) -----------------------------
def build_sqlalchemy_url(db_type: str, host: str, port: str, db: str, user: str, pwd: str, ssl_mode: str):
    db_type = (db_type or "").strip().lower()
    if db_type == "postgresql":
        query = {"sslmode": ssl_mode} if ssl_mode and ssl_mode != "disable" else None
        return URL.create(
            "postgresql+psycopg2",
            username=user or None,
            password=pwd or None,
            host=host or None,
            port=int(port) if port else None,
            database=db or None,
            query=query,
        )
    if db_type == "mssql":
        # Requires ODBC Driver 18
        encrypt = "no" if ssl_mode in ("disable", "", None) else "yes"
        trust = "yes" if ssl_mode in ("disable", "", None) else "no"
        odbc = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={host},{port};"
            f"DATABASE={db};"
            f"UID={user};"
            f"PWD={pwd};"
            f"Encrypt={encrypt};"
            f"TrustServerCertificate={trust}"
        )
        return f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc)}"
    if db_type == "mysql":
        return URL.create(
            "mysql+pymysql",
            username=user or None,
            password=pwd or None,
            host=host or None,
            port=int(port) if port else None,
            database=db or None,
        )
    raise ValueError("Unsupported db_type. Choose PostgreSQL, MSSQL, or MySQL.")

def connect_engine(url):
    eng = create_engine(url, future=True, pool_pre_ping=True)
    with eng.connect() as conn:
        conn.execute(text("SELECT 1"))
    return eng

def list_schemas(inspector, db_type: str, default_schema: str):
    try:
        if (db_type or "").lower() == "mysql":
            return [default_schema] if default_schema else [None]
        schemas = inspector.get_schema_names()
        hidden = {"information_schema", "pg_catalog", "sys"}
        filtered = [s for s in schemas if s not in hidden]
        return filtered or ([default_schema] if default_schema else [])
    except Exception:
        return [default_schema] if default_schema else [None]

def qualify_name(db_type: str, schema: str, table: str):
    dbt = (db_type or "").lower()
    if dbt == "postgresql":
        return f'"{schema}"."{table}"' if schema else f'"{table}"'
    if dbt == "mssql":
        return f"[{schema}].[{table}]" if schema else f"[{table}]"
    return f"{schema}.{table}" if schema else table  # MySQL / generic

def select_sql_for_preview(db_type: str, qualified_name: str, limit: int = 20):
    return text(f"SELECT TOP {limit} * FROM {qualified_name}") if (db_type or "").lower() == "mssql" \
           else text(f"SELECT * FROM {qualified_name} LIMIT {limit}")

def select_sql_for_migration(db_type: str, qualified_name: str, select_list: list[str]):
    return f"SELECT {', '.join(select_list)} FROM {qualified_name}"

def quote_source_identifier(db_type: str, identifier: str):
    if not identifier:
        return identifier
    if any(c in identifier for c in "() +-*/'\""):  # treat as expression/constant
        return identifier
    dbt = (db_type or "").lower()
    if dbt == "postgresql":
        return f'"{identifier}"'
    if dbt == "mssql":
        return f"[{identifier}]"
    return f"`{identifier}`"  # MySQL

# ----------------------------- Session defaults -----------------------------
for k in ["engine_A","engine_B","inspector_A","inspector_B","url_A","url_B","dbtype_A","dbtype_B","dbname_A","dbname_B"]:
    if k not in st.session_state:
        st.session_state[k] = None

# ============================= TAB 1: DB ↔ DB =============================
with tab_main:
    st.markdown("### ⚙️ Connection Settings")
    colA, colB = st.columns(2)

    # --- helper to DRY connection logic and also enable "Connect Both" ---
    def _connect_and_store(which, db_type, host, port, db, user, pwd, ssl_mode):
        try:
            url = build_sqlalchemy_url(db_type, host, port, db, user, pwd, ssl_mode)
            eng = connect_engine(url)
            insp = inspect(eng)
            st.session_state[f"engine_{which}"]    = eng
            st.session_state[f"inspector_{which}"] = insp
            st.session_state[f"url_{which}"]       = str(url)
            st.session_state[f"dbtype_{which}"]    = db_type
            st.session_state[f"dbname_{which}"]    = db
            st.success(f"[{which}] Connected")
            st.code(str(url), language="text")
            return True
        except Exception as e:
            st.error(f"[{which}] Connection failed: {e}")
            return False

    with colA:
        st.subheader("🅰️ Connect First DB")
        db_type_A = st.selectbox("DB Type (A)", ["PostgreSQL", "MSSQL", "MySQL"], key="type_A")
        host_A = st.text_input("Host (A)", value="localhost", key="host_A")
        port_A = st.text_input(
            "Port (A)",
            value=("5432" if db_type_A == "PostgreSQL" else "1433" if db_type_A == "MSSQL" else "3306"),
            key="port_A",
        )
        db_A   = st.text_input(
            "Database (A)",
            value=("postgres" if db_type_A == "PostgreSQL" else "master" if db_type_A == "MSSQL" else "mysql"),
            key="db_A",
        )
        user_A = st.text_input(
            "User (A)",
            value=("postgres" if db_type_A == "PostgreSQL" else "sa" if db_type_A == "MSSQL" else "root"),
            key="user_A",
        )
        pwd_A  = st.text_input("Password (A)", type="password", key="pwd_A")
        ssl_A  = st.selectbox("SSL/TLS Mode (A)", ["disable", "require", "prefer", "verify-ca", "verify-full"], index=0, key="ssl_A")

        if st.button("Connect A", key="btn_connect_A"):
            _connect_and_store("A", db_type_A, host_A, port_A, db_A, user_A, pwd_A, ssl_A)

    with colB:
        st.subheader("🅱️ Connect Second DB")
        db_type_B = st.selectbox("DB Type (B)", ["PostgreSQL", "MSSQL", "MySQL"], key="type_B")
        host_B = st.text_input("Host (B)", value="localhost", key="host_B")
        port_B = st.text_input(
            "Port (B)",
            value=("5432" if db_type_B == "PostgreSQL" else "1433" if db_type_B == "MSSQL" else "3306"),
            key="port_B",
        )
        db_B   = st.text_input(
            "Database (B)",
            value=("postgres" if db_type_B == "PostgreSQL" else "master" if db_type_B == "MSSQL" else "mysql"),
            key="db_B",
        )
        user_B = st.text_input(
            "User (B)",
            value=("postgres" if db_type_B == "PostgreSQL" else "sa" if db_type_B == "MSSQL" else "root"),
            key="user_B",
        )
        pwd_B  = st.text_input("Password (B)", type="password", key="pwd_B")
        ssl_B  = st.selectbox("SSL/TLS Mode (B)", ["disable", "require", "prefer", "verify-ca", "verify-full"], index=0, key="ssl_B")

        if st.button("Connect B", key="btn_connect_B"):
            _connect_and_store("B", db_type_B, host_B, port_B, db_B, user_B, pwd_B, ssl_B)

    # --- centered "Connect Both" button ---
    _, col_btn, _ = st.columns([1, 1, 1])
    with col_btn:
        if st.button("🔗 Connect Both", key="btn_connect_both"):
            okA = _connect_and_store("A", db_type_A, host_A, port_A, db_A, user_A, pwd_A, ssl_A)
            okB = _connect_and_store("B", db_type_B, host_B, port_B, db_B, user_B, pwd_B, ssl_B)
            if okA and okB:
                st.success("✅ Both connections established.")

    st.markdown("---")

    # ============================== Source/Destination Selection ==============================
    st.subheader(" 🔀 Choose Source and Destination")
    col_sd1, col_sd2 = st.columns(2)
    with col_sd1:
        src_choice = st.radio("Source Connection", ["A", "B"], horizontal=True, key="src_choice")
    with col_sd2:
        dst_choice = st.radio("Destination Connection", ["A", "B"], horizontal=True, key="dst_choice")

    if src_choice == dst_choice:
        st.warning("Source and Destination must be different.")
    else:
        # Build contexts
        src_eng   = st.session_state.get("engine_A")    if src_choice == "A" else st.session_state.get("engine_B")
        src_insp  = st.session_state.get("inspector_A") if src_choice == "A" else st.session_state.get("inspector_B")
        src_type  = st.session_state.get("dbtype_A")    if src_choice == "A" else st.session_state.get("dbtype_B")
        src_db    = st.session_state.get("dbname_A")    if src_choice == "A" else st.session_state.get("dbname_B")

        dst_eng   = st.session_state.get("engine_A")    if dst_choice == "A" else st.session_state.get("engine_B")
        dst_insp  = st.session_state.get("inspector_A") if dst_choice == "A" else st.session_state.get("inspector_B")
        dst_type  = st.session_state.get("dbtype_A")    if dst_choice == "A" else st.session_state.get("dbtype_B")
        dst_db    = st.session_state.get("dbname_A")    if dst_choice == "A" else st.session_state.get("dbname_B")

        if not src_eng or not dst_eng:
            st.info("Connect both databases first.")
        else:
            # ---------------- Pick tables ----------------
            st.subheader(" 📑 Pick Tables")
            c1, c2 = st.columns(2)

            with c1:
                st.markdown(f"**Source ({src_choice})**")
                default_src_schema = "public" if src_type == "PostgreSQL" else ("dbo" if src_type == "MSSQL" else src_db)
                src_schemas = list_schemas(src_insp, src_type, default_src_schema)
                src_schema = st.selectbox(
                    "Source Schema",
                    options=src_schemas,
                    index=(src_schemas.index(default_src_schema) if default_src_schema in src_schemas else 0),
                    key="src_schema",
                )
                try:
                    src_tables = src_insp.get_table_names(schema=src_schema)
                except Exception:
                    src_tables = src_insp.get_table_names()
                if src_tables:
                    src_table = st.selectbox("Source Table", options=src_tables, key="src_table")
                    try:
                        src_cols_meta = src_insp.get_columns(src_table, schema=src_schema)
                    except Exception:
                        src_cols_meta = src_insp.get_columns(src_table)
                    src_cols = [c["name"] for c in src_cols_meta]
                    st.caption(f"Columns: {', '.join(src_cols) if src_cols else '—'}")
                else:
                    src_table, src_cols = None, []
                    st.warning("No tables found in source.")

            with c2:
                st.markdown(f"**Destination ({dst_choice})**")
                default_dst_schema = "public" if dst_type == "PostgreSQL" else ("dbo" if dst_type == "MSSQL" else dst_db)
                dst_schemas = list_schemas(dst_insp, dst_type, default_dst_schema)
                dst_schema = st.selectbox(
                    "Destination Schema",
                    options=dst_schemas,
                    index=(dst_schemas.index(default_dst_schema) if default_dst_schema in dst_schemas else 0),
                    key="dst_schema",
                )
                try:
                    dst_tables = dst_insp.get_table_names(schema=dst_schema)
                except Exception:
                    dst_tables = dst_insp.get_table_names()
                if dst_tables:
                    dst_table = st.selectbox("Destination Table", options=dst_tables, key="dst_table")
                    try:
                        dst_cols_meta = dst_insp.get_columns(dst_table, schema=dst_schema)
                    except Exception:
                        dst_cols_meta = dst_insp.get_columns(dst_table)
                    dst_cols = [c["name"] for c in dst_cols_meta]
                    # Build destination type map once (used in normalization)
                    dest_type_map = {c["name"]: str(c.get("type", "")).lower() for c in dst_cols_meta}
                    st.caption(f"Columns: {', '.join(dst_cols) if dst_cols else '—'}")
                else:
                    dst_table, dst_cols, dest_type_map = None, [], {}
                    st.warning("No tables found in destination.")

            if src_table and dst_table:
                # ---------------- Mapping UI ----------------
                st.markdown("---")
                st.subheader(" 🗺️ Map Fields (dest → source/constant)")
                st.caption("Tip: Auto-map for same-name columns. You can use constants like NULL or 'static_value'.")

                map_key = f"mapping_{src_choice}_to_{dst_choice}_{src_schema}.{src_table}_to_{dst_schema}.{dst_table}"
                if map_key not in st.session_state:
                    st.session_state[map_key] = {c: (c if c in src_cols else "") for c in dst_cols}

                colm1, colm2, colm3 = st.columns(3)
                with colm1:
                    if st.button("↔ Auto-map same names", key="btn_auto_map"):
                        st.session_state[map_key] = {c: (c if c in src_cols else "") for c in dst_cols}
                with colm2:
                    add_dest = st.selectbox("Add dest column", [""] + dst_cols, key="add_dest")
                    add_src = st.selectbox("Map to source/constant", [""] + src_cols + ["NULL", "'static_value'"], key="add_src")
                    if st.button("➕ Add/Update Mapping", key="btn_add_update_mapping"):
                        if add_dest:
                            st.session_state[map_key][add_dest] = add_src
                with colm3:
                    del_dest = st.selectbox("Remove mapping for dest", [""] + list(st.session_state[map_key].keys()), key="del_dest")
                    if st.button("🗑 Remove", key="btn_remove_mapping"):
                        if del_dest in st.session_state[map_key]:
                            del st.session_state[map_key][del_dest]

                rows = [{"dest_col": d, "source_expr": st.session_state[map_key].get(d, "")} for d in dst_cols]
                edited = st.data_editor(
                    rows,
                    use_container_width=True,
                    num_rows="dynamic",
                    column_config={
                        "dest_col": st.column_config.TextColumn("Destination Column", disabled=True),
                        "source_expr": st.column_config.TextColumn("Source Column / Constant (e.g., NULL or 'static')"),
                    },
                    key="editor_mapping",
                )
                for r in edited:
                    d = r.get("dest_col", "")
                    s = (r.get("source_expr", "") or "").strip()
                    if d:
                        st.session_state[map_key][d] = s

                final_mapping = {d: s for d, s in st.session_state[map_key].items() if d in dst_cols and s}

                # ---------- Helpers for DB→DB type normalization ----------
                import json, uuid, pandas as pd

                def normalize_rows_for_dest(dict_rows, type_map, coerce_invalid_to_null=True):
                    """
                    Normalize a list[dict] chunk to match destination column types.
                    - Booleans: robust mapping from 0/1, true/false, yes/no, y/n, on/off
                    - Integers: parse int; invalid -> NULL (if coerce_invalid_to_null)
                    - Numerics: parse float/decimal; invalid -> NULL
                    - Dates/Timestamps: parsed with pandas; invalid -> NULL
                    - UUID: validate; invalid -> NULL
                    - JSON: parse strings as JSON when possible
                    - Text: cast to str
                    """
                    if not dict_rows:
                        return dict_rows

                    truthy = {"true","t","yes","y","1","on"}
                    falsy  = {"false","f","no","n","0","off"}

                    df = pd.DataFrame(dict_rows)
                    cols = [c for c in df.columns if c in type_map]

                    for col in cols:
                        t = (type_map.get(col, "") or "").lower()
                        s = df[col]

                        # Booleans
                        if "bool" in t:
                            if pd.api.types.is_bool_dtype(s):
                                df[col] = s.astype("boolean")
                            elif pd.api.types.is_integer_dtype(s):
                                df[col] = s.astype("Int64").map({1: True, 0: False}).astype("boolean")
                            else:
                                as_str = s.astype(str).str.strip().str.lower()
                                mapped = as_str.map(lambda v: True if v in truthy else (False if v in falsy else (None if coerce_invalid_to_null else v)))
                                df[col] = mapped.astype("boolean")

                        # Integers
                        elif any(k in t for k in ["int", "bigint", "smallint", "integer"]):
                            def _to_int(x):
                                if pd.isna(x): return None
                                if isinstance(x, (int, bool)): return int(x)
                                xs = str(x).strip()
                                if xs == "": return None if coerce_invalid_to_null else x
                                if xs.lstrip("+-").isdigit():
                                    try: return int(xs)
                                    except: return None if coerce_invalid_to_null else x
                                return None if coerce_invalid_to_null else x
                            df[col] = df[col].map(_to_int)

                        # Numerics
                        elif any(k in t for k in ["numeric","decimal","float","double","real"]):
                            def _to_float(x):
                                if pd.isna(x): return None
                                if isinstance(x, (int, float, bool)): return float(x)
                                xs = str(x).strip()
                                if xs == "": return None if coerce_invalid_to_null else x
                                try: return float(xs.replace(",", ""))
                                except: return None if coerce_invalid_to_null else x
                            df[col] = df[col].map(_to_float)

                        # Timestamps / Dates / Times
                        elif "timestamp" in t or "timestamptz" in t:
                            parsed = pd.to_datetime(s, errors="coerce", utc=False)
                            df[col] = parsed.where(~parsed.isna(), None)
                        elif t.startswith("date"):
                            parsed = pd.to_datetime(s, errors="coerce").dt.date
                            df[col] = parsed.where(parsed.notna(), None)
                        elif t.startswith("time"):
                            parsed = pd.to_datetime(s, errors="coerce").dt.time
                            df[col] = parsed.where(pd.notna(parsed), None)

                        # UUID
                        elif "uuid" in t:
                            def _to_uuid(x):
                                if pd.isna(x) or x is None or str(x).strip() == "": return None
                                try:
                                    return str(uuid.UUID(str(x)))
                                except:
                                    return None if coerce_invalid_to_null else x
                            df[col] = df[col].map(_to_uuid)

                        # JSON
                        elif "json" in t:
                            def _to_json(x):
                                if x is None or pd.isna(x): return None
                                if isinstance(x, (dict, list)): return x
                                xs = str(x).strip()
                                if xs == "": return None
                                try:
                                    return json.loads(xs)
                                except:
                                    return xs if not coerce_invalid_to_null else None
                            df[col] = df[col].map(_to_json)

                        # Text-ish
                        elif any(k in t for k in ["char", "text", "varchar", "string"]):
                            def _to_str(x):
                                if x is None or pd.isna(x): return None
                                try:
                                    return str(x)
                                except Exception:
                                    return None if coerce_invalid_to_null else x
                            df[col] = df[col].map(_to_str)

                        # else: leave as-is

                    return df.where(pd.notnull(df), None).to_dict(orient="records")

                # ---------------- Execute migration (robust: no silent data loss) ----------------
                st.markdown("---")
                st.subheader("🚀 Execute Migration (safe)")

                coerce_invalid = st.checkbox("Coerce invalid values to NULL (recommended)", value=True, key="coerce_invalid_to_null")
                chunk_size     = st.number_input("Chunk size", min_value=100, max_value=200_000, value=5000, step=100, key="chunk_size")

                conf_policy = st.selectbox(
                    "On duplicate key / conflict",
                    ["error (strict)", "skip duplicates", "upsert/update (PG/MySQL)"],
                    index=1,
                    key="conflict_policy",
                )
                continue_on_error = st.checkbox("Continue on other errors and collect rejects", value=True, key="continue_on_error")

                if st.button("🚀 Migrate Data", key="btn_run_migration"):
                    from sqlalchemy.exc import IntegrityError
                    from sqlalchemy import Table, MetaData

                    try:
                        if not final_mapping:
                            st.error("No mappings defined.")
                        else:
                            # Build SELECT
                            src_qname = qualify_name(src_type, src_schema, src_table)
                            select_list = []
                            for dest_col, src_expr in final_mapping.items():
                                expr = src_expr.strip()
                                if expr.upper() != "NULL" and not expr.startswith("'"):
                                    expr = quote_source_identifier(src_type, expr)
                                alias = (
                                    f"\"{dest_col}\"" if (src_type or '').lower()=="postgresql"
                                    else f"[{dest_col}]" if (src_type or '').lower()=="mssql"
                                    else f"`{dest_col}`"
                                )
                                select_list.append(f"{expr} AS {alias}")
                            sql_select = select_sql_for_migration(src_type, src_qname, select_list)

                            # Destination metadata & PKs
                            md = MetaData(schema=dst_schema)
                            dtbl = Table(dst_table, md, autoload_with=dst_eng, schema=dst_schema)
                            try:
                                pkc = dst_insp.get_pk_constraint(dst_table, schema=dst_schema) or {}
                                pk_cols = [c for c in (pkc.get("constrained_columns") or []) if c in final_mapping]
                            except Exception:
                                pk_cols = []

                            # Dialect helpers
                            dialect = (dst_type or "").lower()
                            pg_insert = None; my_insert = None
                            if dialect == "postgresql":
                                from sqlalchemy.dialects.postgresql import insert as pg_insert
                            if dialect == "mysql":
                                from sqlalchemy.dialects.mysql import insert as my_insert

                            total = inserted = duplicates = rejected = 0
                            rejected_rows = []

                            with src_eng.connect() as sconn, dst_eng.begin() as dconn:
                                result = sconn.execution_options(stream_results=True).execute(text(sql_select))
                                keys = result.keys()
                                while True:
                                    rows = result.fetchmany(int(chunk_size))
                                    if not rows:
                                        break
                                    dict_rows = [dict(zip(keys, r)) for r in rows]
                                    total += len(dict_rows)

                                    # Normalize to destination types
                                    dict_rows = normalize_rows_for_dest(dict_rows, dest_type_map, coerce_invalid_to_null=coerce_invalid)

                                    # Fast set-based path with conflict policy
                                    try_set_based = True
                                    try:
                                        if conf_policy == "error (strict)":
                                            dconn.execute(dtbl.insert(), dict_rows)
                                            inserted += len(dict_rows)
                                        elif conf_policy == "skip duplicates":
                                            if dialect == "postgresql" and pg_insert and pk_cols:
                                                stmt = pg_insert(dtbl).values(dict_rows).on_conflict_do_nothing(index_elements=pk_cols)
                                                dconn.execute(stmt); inserted += len(dict_rows)
                                            elif dialect == "mysql" and my_insert:
                                                stmt = my_insert(dtbl).values(dict_rows).prefix_with("IGNORE")
                                                dconn.execute(stmt); inserted += len(dict_rows)
                                            else:
                                                try_set_based = False
                                        else:  # upsert/update
                                            if dialect == "postgresql" and pg_insert and pk_cols:
                                                stmt = pg_insert(dtbl).values(dict_rows)
                                                update_cols = {c: stmt.excluded.c[c] for c in final_mapping.keys() if c not in pk_cols}
                                                stmt = stmt.on_conflict_do_update(index_elements=pk_cols, set_=update_cols)
                                                dconn.execute(stmt); inserted += len(dict_rows)
                                            elif dialect == "mysql" and my_insert:
                                                stmt = my_insert(dtbl).values(dict_rows)
                                                update_cols = {c: stmt.inserted[c] for c in final_mapping.keys()}
                                                stmt = stmt.on_duplicate_key_update(**update_cols)
                                                dconn.execute(stmt); inserted += len(dict_rows)
                                            else:
                                                try_set_based = False
                                    except IntegrityError:
                                        try_set_based = False
                                    except Exception as e:
                                        if continue_on_error:
                                            try_set_based = False
                                        else:
                                            raise

                                    # Row-by-row fallback
                                    if not try_set_based:
                                        for row in dict_rows:
                                            try:
                                                dconn.execute(dtbl.insert(), row)
                                                inserted += 1
                                            except IntegrityError:
                                                if conf_policy in ("skip duplicates", "upsert/update (PG/MySQL)"):
                                                    duplicates += 1
                                                    continue
                                                if continue_on_error:
                                                    r = dict(row); r["__reason"] = "IntegrityError"
                                                    rejected_rows.append(r); rejected += 1
                                                    continue
                                                raise
                                            except Exception as e:
                                                if continue_on_error:
                                                    r = dict(row); r["__reason"] = f"{type(e).__name__}: {e}"
                                                    rejected_rows.append(r); rejected += 1
                                                    continue
                                                raise

                            st.success(f"✅ Done. Total fetched: {total} | Inserted/Upserted: {inserted} | Duplicates skipped: {duplicates} | Rejected: {rejected}")

                            if rejected_rows:
                                rej_df = pd.DataFrame(rejected_rows)
                                csv = rej_df.to_csv(index=False).encode("utf-8")
                                st.download_button(
                                    "⬇️ Download rejected rows (CSV)",
                                    data=csv,
                                    file_name=f"rejected_rows_{dst_schema}_{dst_table}.csv",
                                    mime="text/csv",
                                )

                    except Exception as e:
                        st.error("Migration failed.")
                        st.exception(e)

                # ---------------- Optional previews ----------------
                st.markdown("---")
                st.subheader("🔍 Explore Data")
                left, right = st.columns(2)
                with left:
                    st.markdown("**Source Preview**")
                    try:
                        qn = qualify_name(src_type, src_schema, src_table)
                        with src_eng.connect() as c:
                            rows = c.execute(select_sql_for_preview(src_type, qn, 20)).mappings().all()
                            st.dataframe([dict(r) for r in rows], use_container_width=True)
                    except Exception as e:
                        st.info(f"Preview failed: {e}")
                with right:
                    st.markdown("**Destination Preview**")
                    try:
                        qn = qualify_name(dst_type, dst_schema, dst_table)
                        with dst_eng.connect() as c:
                            rows = c.execute(select_sql_for_preview(dst_type, qn, 20)).mappings().all()
                            st.dataframe([dict(r) for r in rows], use_container_width=True)
                    except Exception as e:
                        st.info(f"Preview failed: {e}")

    st.caption("Drivers required: psycopg2-binary (PostgreSQL), pyodbc + ODBC Driver 18 (MSSQL), PyMySQL (MySQL).")




# ============================= TAB 2: File → DB =============================
with tab_file:
    import pandas as pd

    st.subheader("📂 Upload Excel/CSV → Database (A or B)")

    # Pick destination connection (A/B)
    dest_pick = st.radio("Destination Connection", ["A", "B"], horizontal=True, key="file_dest_pick_tab")
    dest_engine = st.session_state["engine_A"] if dest_pick == "A" else st.session_state["engine_B"]
    dest_insp   = st.session_state["inspector_A"] if dest_pick == "A" else st.session_state["inspector_B"]
    dest_type   = st.session_state["dbtype_A"] if dest_pick == "A" else st.session_state["dbtype_B"]
    dest_dbname = st.session_state["dbname_A"] if dest_pick == "A" else st.session_state["dbname_B"]

    if not dest_engine or not dest_insp or not dest_type:
        st.info("Connect the chosen destination (A or B) first.")
    else:
        # Destination schema & table
        default_schema = "public" if dest_type == "PostgreSQL" else ("dbo" if dest_type == "MSSQL" else dest_dbname)
        dest_schemas = list_schemas(dest_insp, dest_type, default_schema)
        dest_schema = st.selectbox(
            "Destination Schema",
            dest_schemas,
            index=(dest_schemas.index(default_schema) if default_schema in dest_schemas else 0)
        )

        try:
            dest_tables = dest_insp.get_table_names(schema=dest_schema)
        except Exception:
            dest_tables = dest_insp.get_table_names()
        dest_table = st.selectbox("Destination Table", dest_tables)

        # Columns + type map
        try:
            dest_cols_meta = dest_insp.get_columns(dest_table, schema=dest_schema)
        except Exception:
            dest_cols_meta = dest_insp.get_columns(dest_table)
        dest_cols = [c["name"] for c in dest_cols_meta]
        dest_type_map = {c["name"]: str(c.get("type", "")).lower() for c in dest_cols_meta}

        # ---------- Helpers (file → df mapping, normalization) ----------
        def build_df_for_insert(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
            """
            Create a dataframe with destination columns based on file columns/constants.
            (Type casting happens later in normalize_for_dest_types.)
            """
            out = pd.DataFrame()
            for dest_col, src_expr in mapping.items():
                src_expr = (src_expr or "").strip()
                if not src_expr:
                    continue
                if src_expr.upper() == "NULL":
                    out[dest_col] = None
                elif src_expr.startswith("'") and src_expr.endswith("'") and len(src_expr) >= 2:
                    out[dest_col] = src_expr[1:-1]  # strip single quotes
                else:
                    if src_expr not in df.columns:
                        raise ValueError(f"File is missing required column: {src_expr}")
                    out[dest_col] = df[src_expr]
            return out

        def normalize_for_dest_types(df: pd.DataFrame, type_map: dict) -> pd.DataFrame:
            """
            Cast/normalize dataframe columns to match destination SQL types as best as possible.
            - Robust boolean coercion (0/1/true/false/yes/no/y/n/on/off -> boolean)
            Extend with date/numeric parsing as needed.
            """
            if df.empty:
                return df

            truthy = {"true", "t", "yes", "y", "1", "on"}
            falsy  = {"false", "f", "no", "n", "0", "off"}

            out = df.copy()
            for col in out.columns:
                t = (type_map.get(col, "") or "").lower()

                # ---- Boolean handling ----
                if "bool" in t:  # matches 'boolean', 'bool'
                    s = out[col]
                    if pd.api.types.is_bool_dtype(s):
                        out[col] = s.astype("boolean")
                    elif pd.api.types.is_integer_dtype(s):
                        out[col] = s.astype("Int64").map({1: True, 0: False}).astype("boolean")
                    else:
                        as_str = s.astype(str).str.strip().str.lower()
                        mapped = as_str.map(lambda v: True if v in truthy else (False if v in falsy else None))
                        out[col] = mapped.astype("boolean")

                # (Optional) Empty string -> NULL for non-textual types can be added here.
                # elif any(x in t for x in ["int", "numeric", "decimal", "date", "timestamp"]):
                #     out[col] = out[col].replace({"": None})

            return out

        # ---------- File upload ----------
        uploaded = st.file_uploader("Upload .xlsx / .xls / .csv", type=["xlsx", "xls", "csv"])
        if uploaded:
            file_df = pd.read_csv(uploaded) if uploaded.name.lower().endswith(".csv") else pd.read_excel(uploaded)
            st.caption(f"Detected **{len(file_df)} rows** and **{len(file_df.columns)} columns**.")
            with st.expander("Preview (first 50 rows)"):
                st.dataframe(file_df.head(50), use_container_width=True)

            # Mapping state (dest → file column / constant)
            fmap_key = f"filemap_tab_{dest_pick}_{dest_schema}.{dest_table}"
            if fmap_key not in st.session_state:
                st.session_state[fmap_key] = {c: (c if c in file_df.columns else "") for c in dest_cols}

            c1, c2, c3 = st.columns(3)
            with c1:
                if st.button("↔ Auto-map same names (file → dest)"):
                    st.session_state[fmap_key] = {c: (c if c in file_df.columns else "") for c in dest_cols}
            with c2:
                add_d = st.selectbox("Add/Update dest", [""] + dest_cols, key="file_add_dest_tab")
                add_s = st.selectbox("Map to file column/constant", [""] + list(file_df.columns) + ["NULL", "'static_value'"], key="file_add_src_tab")
                if st.button("➕ Apply", key="file_apply_btn"):
                    if add_d:
                        st.session_state[fmap_key][add_d] = add_s
            with c3:
                rem_d = st.selectbox("Remove mapping", [""] + list(st.session_state[fmap_key].keys()), key="file_rem_dest_tab")
                if st.button("🗑 Remove", key="file_remove_btn"):
                    if rem_d in st.session_state[fmap_key]:
                        del st.session_state[fmap_key][rem_d]

            map_rows = [{"dest_col": d, "source_expr": st.session_state[fmap_key].get(d, "")} for d in dest_cols]
            map_edited = st.data_editor(
                map_rows,
                use_container_width=True,
                num_rows="dynamic",
                column_config={
                    "dest_col": st.column_config.TextColumn("Destination Column", disabled=True),
                    "source_expr": st.column_config.TextColumn("File Column / Constant (e.g., NULL or 'static')"),
                },
                key="file_editor_map_tab",
            )
            # Persist edits -> final mapping
            final_map = {}
            for r in map_edited:
                d = (r.get("dest_col") or "").strip()
                s = (r.get("source_expr") or "").strip()
                if d and s:
                    final_map[d] = s

            o1, o2 = st.columns(2)
            with o1:
                chunksize = st.number_input("Insert chunk size", 100, 200_000, 5000, 100)
            with o2:
                st.caption("Booleans are auto-normalized (0/1/yes/no/true/false → boolean).")

            # ---------- Run load ----------
            if st.button("🚀 Load File → DB", key="file_run_btn"):
                try:
                    if not final_map:
                        st.error("No mappings defined.")
                    else:
                        insert_df = build_df_for_insert(file_df, final_map)
                        total = len(insert_df)
                        if total == 0:
                            st.warning("Nothing to insert.")
                        else:
                            prog = st.progress(0, text="Inserting…")
                            done = 0
                            for start in range(0, total, int(chunksize)):
                                end = min(start + int(chunksize), total)
                                chunk = insert_df.iloc[start:end]

                                # Normalize types to match destination schema
                                chunk = normalize_for_dest_types(chunk, dest_type_map)

                                chunk.to_sql(
                                    dest_table,
                                    dest_engine,
                                    schema=dest_schema if dest_schema else None,
                                    if_exists="append",
                                    index=False,
                                    method=None,
                                )
                                done = end
                                pct = int(done / total * 100)
                                prog.progress(pct, text=f"Inserting… {done}/{total} rows")

                            prog.progress(100, text=f"Done. Inserted {total} rows.")
                            st.success(f"✅ Insert complete: {total} rows → {dest_schema}.{dest_table}")
                except Exception as e:
                    st.error("File load failed.")
                    st.exception(e)

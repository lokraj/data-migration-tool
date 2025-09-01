# DB Migrator (MSSQL ➜ PostgreSQL)

A lightweight, configurable Python CLI to migrate data from Microsoft SQL Server to PostgreSQL with explicit table/column mappings, chunked transfers, and optional table auto-creation.

## Features
- YAML config for connections and mappings
- Explicit table and column mapping (source ➜ destination)
- Optional destination table auto-creation (basic type mapping)
- Chunked streaming (constant memory)
- Per-table transactions & resume-safe chunks
- Dry-run mode
- Progress bars & logs
- Optional high-watermark incremental migration

## Quickstart

1) Create and activate a virtual environment, then install deps:
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
streamlit run streamlit_app.py
```


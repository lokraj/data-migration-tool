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
```

2) Copy `config.example.yaml` to `config.yaml` and edit it.

3) Run a dry-run to validate mappings:
```bash
python migrator.py run --config config.yaml --dry-run
```

4) Migrate (real run):
```bash
python migrator.py run --config config.yaml
```

## Connection Strings

- **MSSQL (ODBC)**: `mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 18 for SQL Server};SERVER=host,1433;DATABASE=db;UID=user;PWD=pass;Encrypt=yes;TrustServerCertificate=no`
- **PostgreSQL**: `postgresql+psycopg2://user:pass@host:5432/dbname`

> Ensure the ODBC driver is installed on your machine (e.g., "ODBC Driver 18 for SQL Server").

## Config Overview

See inline comments in `config.example.yaml`. Minimal example:

```yaml
source:
  url: "mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,1433;DATABASE=Northwind;UID=sa;PWD=Passw0rd;Encrypt=no"
dest:
  url: "postgresql+psycopg2://postgres:postgres@localhost:5432/target_db"

options:
  chunk_size: 5000
  create_tables: true
  schema: public
  use_identity_insert: false   # only relevant to MSSQL sources
  vacuum_analyze: true
  on_conflict: "nothing"        # "nothing" or "update"
  dry_run: false

mappings:
  - source_table: dbo.Customers
    dest_table: customers
    # optional: schema override per table
    dest_schema: public
    # optional: incremental
    high_watermark:
      column: ModifiedAt
      # ISO 8601, exclusive lower bound; if omitted, full copy
      since: "2024-01-01T00:00:00Z"
    columns:
      # dest_col: source_col (or constant, e.g. "'static'")
      customer_id: CustomerID
      company_name: CompanyName
      contact_name: ContactName
      country: Country
```

## Notes
- Auto-creation maps common MSSQL types to Postgres. Review generated DDL in logs.
- For complex transforms, migrate into staging tables first, or extend `transform_row()` hook.
- For upserts (`on_conflict: update`), ensure a unique constraint/PK exists in destination.


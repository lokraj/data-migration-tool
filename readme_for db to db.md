# 🚀 Pro Data Migration Tool

(PostgreSQL / MSSQL / MySQL)

A Streamlit-based, GUI-first tool that lets you connect two databases simultaneously, choose Source and Destination, visually map fields, and safely migrate data in chunks across PostgreSQL, SQL Server (MSSQL), and MySQL.

## ✨ Features

- 🔗 Connect two databases at once (A & B): PostgreSQL, MSSQL, MySQL

- 🔄 Dynamic Source ↔ Destination selection

- 📋 Browse schemas → tables → columns with previews

- 🎯 Field Mapping UI:

    - Auto-map columns with identical names

    - Inline mapping editor with support for constants (NULL, 'static_value')

- ⚡ Chunked migration for performance & reliability

- 🔒 Secure connections: SSL for PostgreSQL, ODBC Driver for MSSQL

## ⚙️ Requirements
- Runtime

    - Python 3.9+ (3.10+ recommended)

  - Python Packages
    - pip install streamlit SQLAlchemy psycopg2-binary pyodbc PyMySQL

## Database Client / Driver Setup

- PostgreSQL → psycopg2-binary only

- MSSQL → requires ODBC Driver 18

    - sudo apt-get install msodbcsql18


    - MySQL → PyMySQL only

- ✅ Ensure DB servers allow connections and the user has privileges

## 🚀 Getting Started
### 1. Clone Project

- cd data-migration-tool

### 2. Create Virtual Environment (Recommended)
- python -m venv .venv
- source .venv/bin/activate        # Windows: .venv\Scripts\activate

### 3. Install Dependencies
- pip install -r requirements.txt

### 4. Run the App
- streamlit run streamlit_app.py

## 🖥️ Using the App
### 1️⃣ Connect to Databases

- Select DB type (Postgres / MSSQL / MySQL)

- Enter host, port, database, username, password

- (Postgres only) choose SSL mode

- Click Connect A, Connect B, or 🔗 Connect Both

### 2️⃣ Choose Source & Destination

- Pick one as Source, the other as Destination

### 3️⃣ Pick Tables

- Source: choose Schema + Table → view columns

- Destination: choose Schema + Table → view columns

### 4️⃣ Map Fields

- Auto-map same-name columns

- Inline edit destination → source/constant

- Supports constants: NULL, 'static_value'

### 5️⃣ Set Chunk Size

- Default: 5000 rows per batch

--🔹 Larger = faster, heavier load

--🔹 Smaller = safer, slower

### 6️⃣ Run Migration

- Click 🚀 Migrate Data

- Progress updates with row counts

- Preview panels for both source & destination

## 📝 Notes & Best Practices

- Chunked transfers improve safety & performance

- Transactions ensure safe batch inserts

- Quoting rules auto-applied ("col" Postgres, [col] MSSQL, `col` MySQL)

- Postgres SSL →

- disable: local dev

- require, verify-ca, verify-full: production

- MSSQL → ODBC Driver 18 required

## 🛠️ Troubleshooting

- ❌ Auth failed → Check credentials (try with psql, sqlcmd, mysql)

- 🌐 Host errors → Try IP instead of hostname

- 🔐 Postgres rejects remote → Update postgresql.conf & pg_hba.conf

- 🧩 MSSQL driver not found → Install msodbcsql18

- 🔏 SSL issues → Adjust SSL mode or provide certs

- 🚫 Permission denied → Ensure INSERT rights on destination

## 🔐 Security

#### - Use least-privilege DB accounts

#### - Run over VPN or secure networks

#### - Enable TLS/SSL in production

## ⚡ Performance Tips

- Disable heavy indexes/constraints during migration

- Increase chunk size if server resources allow

- Run migrations off-peak for best speed

- For very large tables → split by ranges (e.g., date ranges)
# 🚀 Pro Data Migration Tool  
*(PostgreSQL / MSSQL / MySQL)*

A **Streamlit-based, GUI-first tool** to connect two databases simultaneously, choose **Source** and **Destination**, visually **map columns**, and migrate data in **chunks** across **PostgreSQL, SQL Server (MSSQL), and MySQL**.

---

## ✨ Features
- 🔗 Connect to **two databases at once** (A & B): PostgreSQL, MSSQL, or MySQL.  
- 🔄 Choose **Source ↔ Destination** dynamically.  
- 📋 Browse **schemas → tables → columns**, preview sample rows.  
- 🎯 **Field mapping UI**:  
  - Auto-map identically named columns  
  - Inline editing of mappings  
  - Constants supported (`NULL`, `'static'`)  
- ⚡ **Chunked migration** for performance & reliability.  
- 🔒 Optional **SSL mode** (Postgres), **ODBC Driver settings** (MSSQL).  

---

## ⚙️ Requirements

### Runtime
- **Python 3.9+** (3.10+ recommended)

### Python Packages
```bash
pip install streamlit SQLAlchemy psycopg2-binary pyodbc PyMySQL


## Database Client / Driver Prerequisites
- PostgreSQL: psycopg2-binary only.

- MSSQL: requires ODBC Driver 18 for SQL Server
<pre> ```bash sudo apt-get install msodbcsql18 ``` </pre>

- MySQL: PyMySQL only.

- ✅ Ensure your DB servers accept connections and your user has the required privileges.

# 🚀 Getting Started
## 1. Setup Project
<pre> ``` git clone <your-repo-url>
cd <your-repo-folder> ``` </pre>
## 2. Create Virtual Environment (Recommended)

<pre> ``` python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
 ``` </pre>

## 3. Install Dependencies
<pre> ``` pip install streamlit SQLAlchemy psycopg2-binary pyodbc PyMySQL  ``` </pre>

## 4. Run the App
<pre> ``` streamlit run streamlit_app.py  ``` </pre>


# 🖥️ Using the App
1## ️⃣ Connect to Two Databases

- Select DB type (PostgreSQL / MSSQL / MySQL)

- Enter Host, Port, Database, User, Password

- For Postgres: set SSL mode (disable for localhost)

- Click Connect A, Connect B, or 🔗 Connect Both

2## ️⃣ Choose Source & Destination

- Pick Source Connection (A or B)

- Pick Destination Connection (must be the other one)

## 3 ️⃣ Pick Tables

- Source: choose Schema + Table, view columns

- Destination: choose Schema + Table, view columns

## 4️⃣ Map Fields

- Use ↔ Auto-map to auto-fill identical names

- Adjust mappings inline (dest → source/constant)

- Supports constants: NULL or 'static_value'

##5️⃣ Set Chunk Size

- Default: 5000 rows per batch

- 🔹 Larger = fewer trips but heavier load

- 🔹 Smaller = safer but slower

## 6#️⃣ Run Migration

- Click 🚀 Migrate Data

- Progress shown as rows copied

- Preview panels available for both source & destination

## 📝 Notes & Best Practices

- Chunked transfer: safer & faster for large tables.

- Transactions: Inserts run in transactional batches.

- Constants: Map destination columns to NULL or 'value'.

- Quoting: Auto-handled ("col" for Postgres, [col] for MSSQL).

- Postgres SSL:

    - disable → local dev

    - require, verify-ca, verify-full → production

- MSSQL: Ensure ODBC Driver 18 is installed.

## 🛠️ Troubleshooting

- Auth failed → Check user/pass/DB. Test with psql, sqlcmd, mysql.

- DNS/host errors → Use IP instead of hostname.

- Postgres remote reject → Check postgresql.conf & pg_hba.conf.

- MSSQL driver not found → Install msodbcsql18.

- SSL errors → Adjust SSL mode or certs.

- Permission errors → Ensure user has INSERT rights on destination.

## 🔐 Security

- Use least-privilege DB accounts.

- Use VPN or secure networks for remote DBs.

- Use TLS/SSL for production.

## ⚡ Performance Tips

- Disable heavy indexes/constraints on destination during migration.

- Increase chunk size if resources allow.

- Run during off-peak hours.

- For huge tables → split by ranges (e.g., dates) & run in parallel.


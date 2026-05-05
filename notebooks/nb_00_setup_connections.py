# Databricks notebook source
# ============================================================
# nb_setup_connections
# One-time setup notebook for Databricks secret scopes and
# Unity Catalog foreign catalog connections.
#
# Run this notebook once per environment to establish all
# source system connections before running the migration
# framework notebooks.
#
# Sections:
#   1. Secret scope creation via REST API
#   2. Secret population per source system
#   3. Foreign catalog creation per source
#   4. Connection verification
#
# Prerequisites:
#   - Databricks workspace with Unity Catalog enabled
#   - Network access from workspace to source systems
#   - Source system credentials available
#
# Note: Secret values are never printed or logged.
#   Use dbutils.secrets.get() to retrieve them safely.
# ============================================================

# COMMAND ----------

# MAGIC %md
# MAGIC # ## Section 1 — Helper functions

# COMMAND ----------

import requests
import json

def get_api_headers():
    """Get workspace URL and auth token for REST API calls."""
    workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
    token = dbutils.notebook.entry_point \
        .getDbutils().notebook().getContext() \
        .apiToken().get()
    return f"https://{workspace_url}", {"Authorization": f"Bearer {token}"}

def create_scope(scope_name):
    """Create a secret scope if it does not already exist."""
    base_url, headers = get_api_headers()

    # Check if scope exists
    r = requests.get(f"{base_url}/api/2.0/secrets/scopes/list", headers=headers)
    existing = [s['name'] for s in r.json().get('scopes', [])]
    if scope_name in existing:
        print(f"  Scope '{scope_name}' already exists — skipping creation")
        return True

    r = requests.post(
        f"{base_url}/api/2.0/secrets/scopes/create",
        headers=headers,
        json={"scope": scope_name, "initial_manage_principal": "users"}
    )
    if r.status_code == 200:
        print(f"  Scope '{scope_name}' created")
        return True
    else:
        print(f"  ERROR creating scope '{scope_name}': {r.json()}")
        return False

def put_secret(scope_name, key, value):
    """Add or update a single secret in a scope."""
    base_url, headers = get_api_headers()
    r = requests.post(
        f"{base_url}/api/2.0/secrets/put",
        headers=headers,
        json={"scope": scope_name, "key": key, "string_value": value}
    )
    if r.status_code == 200:
        print(f"  {scope_name}/{key} : OK")
    else:
        print(f"  {scope_name}/{key} : ERROR — {r.json()}")

def list_secrets(scope_name):
    """List secret keys in a scope (values are never returned)."""
    base_url, headers = get_api_headers()
    r = requests.get(
        f"{base_url}/api/2.0/secrets/list",
        headers=headers,
        params={"scope": scope_name}
    )
    keys = [s['key'] for s in r.json().get('secrets', [])]
    print(f"  Scope '{scope_name}' keys: {keys}")
    return keys

def create_foreign_catalog(catalog_name, connection_name, database_name):
    """Create a foreign catalog if it does not already exist."""
    try:
        existing = [r.catalog for r in spark.sql("SHOW CATALOGS").collect()]
        if catalog_name in existing:
            print(f"  Foreign catalog '{catalog_name}' already exists — skipping")
            return
        spark.sql(f"""
            CREATE FOREIGN CATALOG {catalog_name}
            USING CONNECTION {connection_name}
            OPTIONS (database '{database_name}')
        """)
        print(f"  Foreign catalog '{catalog_name}' created")
    except Exception as e:
        print(f"  ERROR creating catalog '{catalog_name}': {str(e)[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2 — Databricks samples catalog (built-in)
# MAGIC The `samples` catalog is available by default in all
# MAGIC Databricks workspaces. No setup needed — just verify.

# COMMAND ----------

print("Verifying samples catalog...")
try:
    count = spark.sql("SELECT COUNT(*) AS cnt FROM samples.tpch.customer").collect()[0]['cnt']
    print(f"  samples.tpch.customer : {count:,} rows — OK")
except Exception as e:
    print(f"  samples catalog not available: {str(e)[:120]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3 — Neon PostgreSQL (pg_neon)
# MAGIC Free hosted PostgreSQL at neon.tech.
# MAGIC Connected via JDBC — no UC foreign catalog needed.
# MAGIC The dispatcher reads directly using the PostgreSQL JDBC driver
# MAGIC and credentials from the pg_neon secret scope.
# MAGIC
# MAGIC Before running this section:
# MAGIC   - 1. Sign up at neon.tech (free, no credit card)
# MAGIC   - 2. Create a project and note your connection string
# MAGIC   - 3. Fill in the credentials below
# MAGIC   - 4. Create supplier_pg table in Neon SQL Editor:
# MAGIC
# MAGIC    CREATE TABLE supplier_pg (
# MAGIC   -     s_suppkey   INTEGER PRIMARY KEY,
# MAGIC   -     s_name      VARCHAR(25),
# MAGIC   -     s_address   VARCHAR(40),
# MAGIC   -     s_nationkey INTEGER,
# MAGIC   -     s_phone     VARCHAR(15),
# MAGIC   -     s_acctbal   DECIMAL(15,2),
# MAGIC   -     s_comment   VARCHAR(101),
# MAGIC   -     updated_at  TIMESTAMP DEFAULT NOW()
# MAGIC    );
# MAGIC    
# MAGIC    INSERT INTO supplier_pg VALUES
# MAGIC    - (1,'Supplier#000000001','0ywSH7t7rPhQkFST6gp',17,'27-918-335-1736',5755.94,'each slyly above',NOW()),
# MAGIC    - (2,'Supplier#000000002','89eJ5ksX3ImxJQBvxObC',5,'15-679-861-2259',4032.68,'furiously ironic',NOW()),
# MAGIC    - (3,'Supplier#000000003','q1,G3Pj6OjIuUYfUoH18',1,'11-383-516-1199',4192.40,'blithely express',NOW()),
# MAGIC    - (4,'Supplier#000000004','Bk7ah4CK8SYQTepEmvMk',15,'25-843-787-7479',4641.08,'final accounts',NOW()),
# MAGIC    - (5,'Supplier#000000005','Gcdm2rJRzl5qlTVzc',11,'21-151-690-3663',-283.84,'pending requests',NOW());
# MAGIC
# MAGIC  Connection string format (from Neon console → Connect):
# MAGIC    postgresql://{user}:{password}@{host}/{database}?sslmode=require

# COMMAND ----------

# ── Neon credentials — fill in your values ────────────────────
# Get the connection string from console.neon.tech → your project → Connect
# Leave NEON_PASSWORD blank — fill in at runtime, never commit to repo
NEON_HOST     = "ep-summer-sun-ajvmjk1o-pooler.c-3.us-east-2.aws.neon.tech"
NEON_PORT     = "5432"
NEON_USER     = "neondb_owner"
NEON_PASSWORD = ""        # ← paste your current Neon password here — do not commit
NEON_DATABASE = "neondb"
NEON_SCOPE    = "pg_neon"

# COMMAND ----------

# ── Create scope and populate secrets ────────────────────────
print(f"Setting up secret scope: {NEON_SCOPE}")
if NEON_PASSWORD == "":
    print("  SKIPPED — NEON_PASSWORD is empty. Fill in the password above and re-run.")
else:
    create_scope(NEON_SCOPE)
    put_secret(NEON_SCOPE, "host",     NEON_HOST)
    put_secret(NEON_SCOPE, "port",     NEON_PORT)
    put_secret(NEON_SCOPE, "user",     NEON_USER)
    put_secret(NEON_SCOPE, "password", NEON_PASSWORD)
    put_secret(NEON_SCOPE, "database", NEON_DATABASE)
    list_secrets(NEON_SCOPE)

# COMMAND ----------

# ── Verify JDBC connection ────────────────────────────────────
# Tests direct JDBC connectivity using the secret scope credentials.
# No UC foreign catalog needed — dispatcher reads via JDBC driver.
print("Verifying pg_neon JDBC connection...")
if NEON_PASSWORD == "":
    print("  SKIPPED — NEON_PASSWORD is empty")
else:
    try:
        host     = dbutils.secrets.get(NEON_SCOPE, "host")
        port     = dbutils.secrets.get(NEON_SCOPE, "port")
        user     = dbutils.secrets.get(NEON_SCOPE, "user")
        password = dbutils.secrets.get(NEON_SCOPE, "password")

        jdbc_url = f"jdbc:postgresql://{host}:{port}/{NEON_DATABASE}?sslmode=require"

        # Test 1 — basic connectivity
        df = spark.read.format("jdbc")             .option("url",      jdbc_url)             .option("dbtable",  "(SELECT 1 AS test) AS t")             .option("user",     user)             .option("password", password)             .option("driver",   "org.postgresql.Driver")             .load()
        df.show()
        print("   JDBC connection successful")

        # Test 2 — read supplier_pg table
        count = spark.read.format("jdbc")             .option("url",      jdbc_url)             .option("dbtable",  "public.supplier_pg")             .option("user",     user)             .option("password", password)             .option("driver",   "org.postgresql.Driver")             .load().count()
        print(f"   pg_neon.public.supplier_pg : {count:,} rows — OK")

    except Exception as e:
        print(f"   JDBC connection failed: {str(e)[:200]}")
        print("  Check: host, port, user, password in secret scope")
        print("  Check: PostgreSQL JDBC driver installed on cluster")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4 — Additional source systems
# MAGIC Add new sections below following the same pattern for each
# MAGIC additional source system:
# MAGIC  - 1. Fill in credentials
# MAGIC  - 2. create_scope() + put_secret() for each key
# MAGIC  - 3. CREATE CONNECTION in UC
# MAGIC  - 4. CREATE FOREIGN CATALOG (for UC-supported sources)
# MAGIC  -    OR use JDBC directly via secret scope (for Oracle etc.)
# MAGIC
# MAGIC Source type reference:
# MAGIC  -postgresql  → CREATE CONNECTION TYPE postgresql
# MAGIC  - mysql       → CREATE CONNECTION TYPE mysql
# MAGIC  - sqlserver   → CREATE CONNECTION TYPE sqlserver
# MAGIC  - snowflake   → CREATE CONNECTION TYPE snowflake
# MAGIC  - redshift    → CREATE CONNECTION TYPE redshift
# MAGIC  - oracle      → JDBC only — no UC foreign catalog support
# MAGIC
# MAGIC Secret scope key convention:
# MAGIC  - host, port, user, password, database
# MAGIC  - Snowflake also needs: account, warehouse
# MAGIC  - Oracle also needs:    sid (instead of database)

# COMMAND ----------

# MAGIC %md
# MAGIC # ## Section 5 — Verify all connections

# COMMAND ----------

print("="*60)
print("CONNECTION SUMMARY")
print("="*60)

checks = [
    ("samples.tpch.customer",       "SELECT COUNT(*) AS cnt FROM samples.tpch.customer"),
    # pg_neon uses JDBC — tested separately above in Section 3
    # ("pg_neon.public.supplier_pg", "SELECT COUNT(*) AS cnt FROM pg_neon.public.supplier_pg"),
]

for label, sql in checks:
    try:
        cnt = spark.sql(sql).collect()[0]['cnt']
        print(f"  {label:<45} {cnt:>10,} rows  OK")
    except Exception as e:
        print(f"  {label:<45} FAILED — {str(e)[:80]}")

print("="*60)

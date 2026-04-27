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
# ## Section 1 — Helper functions

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
# ## Section 2 — Databricks samples catalog (built-in)
# The `samples` catalog is available by default in all
# Databricks workspaces. No setup needed — just verify.

# COMMAND ----------

print("Verifying samples catalog...")
try:
    count = spark.sql("SELECT COUNT(*) AS cnt FROM samples.tpch.customer").collect()[0]['cnt']
    print(f"  samples.tpch.customer : {count:,} rows — OK")
except Exception as e:
    print(f"  samples catalog not available: {str(e)[:120]}")

# COMMAND ----------

# MAGIC %md
# ## Section 3 — Neon PostgreSQL (pg_neon)
# Free hosted PostgreSQL at neon.tech.
# Connected via JDBC — no UC foreign catalog needed.
# The dispatcher reads directly using the PostgreSQL JDBC driver
# and credentials from the pg_neon secret scope.
#
# Before running this section:
#   1. Sign up at neon.tech (free, no credit card)
#   2. Create a project and note your connection string
#   3. Fill in the credentials below
#   4. Create supplier_pg table in Neon SQL Editor:
#
#   CREATE TABLE supplier_pg (
#       s_suppkey   INTEGER PRIMARY KEY,
#       s_name      VARCHAR(25),
#       s_address   VARCHAR(40),
#       s_nationkey INTEGER,
#       s_phone     VARCHAR(15),
#       s_acctbal   DECIMAL(15,2),
#       s_comment   VARCHAR(101),
#       updated_at  TIMESTAMP DEFAULT NOW()
#   );
#   INSERT INTO supplier_pg VALUES
#   (1,'Supplier#000000001','0ywSH7t7rPhQkFST6gp',17,'27-918-335-1736',5755.94,'each slyly above',NOW()),
#   (2,'Supplier#000000002','89eJ5ksX3ImxJQBvxObC',5,'15-679-861-2259',4032.68,'furiously ironic',NOW()),
#   (3,'Supplier#000000003','q1,G3Pj6OjIuUYfUoH18',1,'11-383-516-1199',4192.40,'blithely express',NOW()),
#   (4,'Supplier#000000004','Bk7ah4CK8SYQTepEmvMk',15,'25-843-787-7479',4641.08,'final accounts',NOW()),
#   (5,'Supplier#000000005','Gcdm2rJRzl5qlTVzc',11,'21-151-690-3663',-283.84,'pending requests',NOW());
#
# Connection string format (from Neon console → Connect):
#   postgresql://{user}:{password}@{host}/{database}?sslmode=require

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
        print("  ✅ JDBC connection successful")

        # Test 2 — read supplier_pg table
        count = spark.read.format("jdbc")             .option("url",      jdbc_url)             .option("dbtable",  "public.supplier_pg")             .option("user",     user)             .option("password", password)             .option("driver",   "org.postgresql.Driver")             .load().count()
        print(f"  ✅ pg_neon.public.supplier_pg : {count:,} rows — OK")

    except Exception as e:
        print(f"  ❌ JDBC connection failed: {str(e)[:200]}")
        print("  Check: host, port, user, password in secret scope")
        print("  Check: PostgreSQL JDBC driver installed on cluster")

# COMMAND ----------

# MAGIC %md
# ## Section 4 — Additional source systems
# Add new sections below following the same pattern for each
# additional source system:
#   1. Fill in credentials
#   2. create_scope() + put_secret() for each key
#   3. CREATE CONNECTION in UC
#   4. CREATE FOREIGN CATALOG (for UC-supported sources)
#      OR use JDBC directly via secret scope (for Oracle etc.)
#
# Source type reference:
#   postgresql  → CREATE CONNECTION TYPE postgresql
#   mysql       → CREATE CONNECTION TYPE mysql
#   sqlserver   → CREATE CONNECTION TYPE sqlserver
#   snowflake   → CREATE CONNECTION TYPE snowflake
#   redshift    → CREATE CONNECTION TYPE redshift
#   oracle      → JDBC only — no UC foreign catalog support
#
# Secret scope key convention:
#   host, port, user, password, database
#   Snowflake also needs: account, warehouse
#   Oracle also needs:    sid (instead of database)

# COMMAND ----------

# MAGIC %md
# ## Section 5 — Verify all connections

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


# Databricks notebook source
# ============================================================
# nb_00_setup
# One-time setup notebook.
# Creates table_migration_config and source_connection_config
# in Unity Catalog and inserts all POC rows.
#
# All target_catalog, target_schema, and volume paths are
# derived from widget parameters — no hardcoded values.
# Safe to run in any environment by changing widgets only.
#
# Widget parameters:
#   admin_catalog  — catalog for config tables
#   config_schema  — schema for config tables
#   target_catalog — catalog for bronze Delta tables
#   target_schema  — schema for bronze Delta tables
#
# Environment examples:
#   POC/dev : admin_catalog=it,     target_schema=tpch_bronze
#   Sandbox : admin_catalog=sandbox, target_schema=migration_config_bronze
#   Prod    : admin_catalog=prod,    target_schema=bronze
# ============================================================

# COMMAND ----------

dbutils.widgets.text("admin_catalog",  "sandbox")
dbutils.widgets.text("config_schema",  "migration_config")
dbutils.widgets.text("target_catalog", "sandbox")
dbutils.widgets.text("target_schema",  "migration_config_bronze")

ADMIN_CATALOG  = dbutils.widgets.get("admin_catalog")
CONFIG_SCHEMA  = dbutils.widgets.get("config_schema")
TARGET_CATALOG = dbutils.widgets.get("target_catalog")
TARGET_SCHEMA  = dbutils.widgets.get("target_schema")
CONFIG_TABLE   = f"{ADMIN_CATALOG}.{CONFIG_SCHEMA}.table_migration_config"
CONN_TABLE     = f"{ADMIN_CATALOG}.{CONFIG_SCHEMA}.source_connection_config"

# Volume path derived from target catalog and schema
# Convention: /Volumes/{target_catalog}/{target_schema}/landing/
VOLUME_BASE    = f"/Volumes/{TARGET_CATALOG}/{TARGET_SCHEMA}/landing"

print(f"Admin catalog  : {ADMIN_CATALOG}")
print(f"Config schema  : {CONFIG_SCHEMA}")
print(f"Target catalog : {TARGET_CATALOG}")
print(f"Target schema  : {TARGET_SCHEMA}")
print(f"Volume base    : {VOLUME_BASE}")
print(f"Config table   : {CONFIG_TABLE}")
print(f"Conn table     : {CONN_TABLE}")

# COMMAND ----------

# ── Create schemas ────────────────────────────────────────────
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ADMIN_CATALOG}.{CONFIG_SCHEMA}")
print(f"✅ Schema {ADMIN_CATALOG}.{CONFIG_SCHEMA} ready")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")
print(f"✅ Schema {TARGET_CATALOG}.{TARGET_SCHEMA} ready")

# COMMAND ----------

# ── Create source_connection_config ──────────────────────────
# Central registry for all source system connections.
# src_database in table_migration_config references
# connection_name here. Adding a new source requires only
# an INSERT — no code changes in dispatcher or init notebooks.

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CONN_TABLE} (
  connection_name    STRING   COMMENT 'Unique name — matches src_database in table_migration_config',
  source_type        STRING   COMMENT 'snowflake | postgresql | sqlserver | oracle | mysql | adls | delta',
  connection_method  STRING   COMMENT 'foreign_catalog | jdbc | volume',
  catalog_name       STRING   COMMENT 'UC foreign catalog name — for foreign_catalog method',
  jdbc_url_template  STRING   COMMENT 'JDBC URL with {{host}} {{port}} {{database}} placeholders',
  driver_class       STRING   COMMENT 'JDBC driver class — for jdbc method',
  secret_scope       STRING   COMMENT 'Databricks secret scope for credentials',
  volume_base_path   STRING   COMMENT 'Base volume path — for volume method',
  ssl_enabled        BOOLEAN  COMMENT 'Adds sslmode=require to JDBC URL',
  extra_options      STRING   COMMENT 'JSON string of additional Spark read options',
  enabled            BOOLEAN  COMMENT 'Active connection',
  notes              STRING   COMMENT 'Engineer notes'
)
USING DELTA
COMMENT 'Central registry of source system connections for the migration framework'
""")
print(f"✅ source_connection_config created")

# COMMAND ----------

# ── Create table_migration_config ─────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CONFIG_TABLE} (
  table_id              STRING    COMMENT 'Unique ID for this migration row',
  source_type           STRING    COMMENT 'snowflake | sqlserver | postgresql | oracle | mysql | adls | delta',
  src_database          STRING    COMMENT 'Matches connection_name in source_connection_config',
  src_schema            STRING    COMMENT 'Source schema name',
  src_table             STRING    COMMENT 'Source table name',
  target_catalog        STRING    COMMENT 'Unity Catalog target catalog',
  target_schema         STRING    COMMENT 'Unity Catalog target schema',
  target_table          STRING    COMMENT 'Delta target table name',
  load_strategy         STRING    COMMENT 'full | incremental',
  load_mode             STRING    COMMENT 'overwrite | append | merge | copy_into | autoloader',
  incremental_col       STRING    COMMENT 'Watermark column name — nullable',
  incremental_col_type  STRING    COMMENT 'timestamp | date | integer | none',
  last_loaded_value     STRING    COMMENT 'High watermark from last successful run',
  batch_filter          STRING    COMMENT 'Optional WHERE clause for full load',
  primary_keys          STRING    COMMENT 'Comma-separated primary key columns',
  merge_keys            STRING    COMMENT 'Comma-separated columns for MERGE INTO join condition',
  clustering_type       STRING    COMMENT 'liquid | partition | none',
  cluster_by            STRING    COMMENT 'Comma-separated columns for liquid clustering',
  partition_col         STRING    COMMENT 'Partition column (clustering_type=partition only)',
  process_group         STRING    COMMENT 'Logical group — maps to a Databricks Workflow',
  process_subgroup      STRING    COMMENT 'Optional sub-group within a process group',
  depends_on_group      STRING    COMMENT 'process_group that must complete first — nullable',
  src_secret_scope      STRING    COMMENT 'Deprecated — use source_connection_config. Kept for compatibility',
  enabled               BOOLEAN   COMMENT 'Default false — must be set true before workflow runs',
  priority              INT       COMMENT 'Run order within process group (lower = first)',
  last_run_status       STRING    COMMENT 'PENDING | RUNNING | PASSED | FAILED',
  last_run_at           TIMESTAMP COMMENT 'Timestamp of last run',
  last_sf_row_count     BIGINT    COMMENT 'Rows read from source on last run',
  last_delta_count      BIGINT    COMMENT 'Rows in Delta after last run',
  file_format           STRING    COMMENT 'parquet | json | csv — for Autoloader and volume loads',
  autoloader_mode       STRING    COMMENT 'availableNow | continuous — Autoloader trigger mode',
  schema_hints          STRING    COMMENT 'Optional schema hints for Autoloader',
  notes                 STRING    COMMENT 'Free-text notes and error messages for engineers'
)
USING DELTA
COMMENT 'Central config and run-state table for the migration framework'
""")
print(f"✅ table_migration_config created")

# COMMAND ----------

# ── Insert source_connection_config rows ──────────────────────
# One row per source system connection.
# connection_name must match src_database in table_migration_config.

conn_rows = [
    {
        'connection_name'  : 'samples',
        'source_type'      : 'snowflake',
        'connection_method': 'foreign_catalog',
        'catalog_name'     : 'samples',
        'jdbc_url_template': None,
        'driver_class'     : None,
        'secret_scope'     : None,
        'volume_base_path' : None,
        'ssl_enabled'      : False,
        'extra_options'    : None,
        'enabled'          : True,
        'notes'            : 'Databricks built-in sample data. TPC-H schema.',
    },
    {
        'connection_name'  : 'pg_neon',
        'source_type'      : 'postgresql',
        'connection_method': 'jdbc',
        'catalog_name'     : None,
        'jdbc_url_template': 'jdbc:postgresql://{host}:{port}/{database}?sslmode=require',
        'driver_class'     : 'org.postgresql.Driver',
        'secret_scope'     : 'pg_neon',
        'volume_base_path' : None,
        'ssl_enabled'      : True,
        'extra_options'    : '{"fetchsize":"10000","queryTimeout":"0"}',
        'enabled'          : True,
        'notes'            : 'Neon PostgreSQL free tier. JDBC connection via pg_neon secret scope.',
    },
    {
        'connection_name'  : TARGET_CATALOG,
        'source_type'      : 'volume',
        'connection_method': 'volume',
        'catalog_name'     : None,
        'jdbc_url_template': None,
        'driver_class'     : None,
        'secret_scope'     : None,
        'volume_base_path' : VOLUME_BASE,
        'ssl_enabled'      : False,
        'extra_options'    : None,
        'enabled'          : True,
        'notes'            : f'UC Volume landing zone at {VOLUME_BASE}',
    },
]

def conn_val(v):
    if v is None:    return 'NULL'
    if v is True:    return 'true'
    if v is False:   return 'false'
    return f"'{str(v).replace(chr(39), chr(39)+chr(39))}'"

# Get existing connection names
existing_conns = set(
    r['connection_name'] for r in spark.sql(f"""
        SELECT connection_name FROM {CONN_TABLE}
    """).collect()
)

conn_inserted = 0
conn_skipped  = 0
for row in conn_rows:
    if row['connection_name'] in existing_conns:
        print(f"  Skipped (exists): {row['connection_name']}")
        conn_skipped += 1
        continue
    spark.sql(f"""
        INSERT INTO {CONN_TABLE} VALUES (
            {conn_val(row['connection_name'])},
            {conn_val(row['source_type'])},
            {conn_val(row['connection_method'])},
            {conn_val(row['catalog_name'])},
            {conn_val(row['jdbc_url_template'])},
            {conn_val(row['driver_class'])},
            {conn_val(row['secret_scope'])},
            {conn_val(row['volume_base_path'])},
            {conn_val(row['ssl_enabled'])},
            {conn_val(row['extra_options'])},
            {conn_val(row['enabled'])},
            {conn_val(row['notes'])}
        )
    """)
    print(f"  Inserted connection: {row['connection_name']}")
    conn_inserted += 1

print(f"\n✅ source_connection_config — Inserted: {conn_inserted} | Skipped: {conn_skipped}")

# COMMAND ----------

# ── Insert table_migration_config rows ────────────────────────
# All rows use TARGET_CATALOG and TARGET_SCHEMA from widgets.
# All run-state columns reset to initial state:
#   enabled = false, last_run_status = PENDING, all counts = NULL

rows = [

    # ══ TPCH_foreign_catalog — priority 1 ════════════════════
    {
        'table_id'            : 'tpch_region_001',
        'source_type'         : 'snowflake',
        'src_database'        : 'samples',
        'src_schema'          : 'tpch',
        'src_table'           : 'region',
        'target_table'        : 'tpch_region',
        'load_strategy'       : 'full',
        'load_mode'           : 'overwrite',
        'incremental_col'     : None,
        'incremental_col_type': 'none',
        'primary_keys'        : 'r_regionkey',
        'merge_keys'          : 'r_regionkey',
        'clustering_type'     : 'none',
        'cluster_by'          : None,
        'process_group'       : 'TPCH_foreign_catalog',
        'process_subgroup'    : 'tpch',
        'src_secret_scope'    : None,
        'priority'            : 1,
        'file_format'         : None,
        'autoloader_mode'     : None,
        'notes'               : 'Tiny reference table. 5 rows. Loads first.',
    },
    {
        'table_id'            : 'tpch_customer_001',
        'source_type'         : 'snowflake',
        'src_database'        : 'samples',
        'src_schema'          : 'tpch',
        'src_table'           : 'customer',
        'target_table'        : 'customer',
        'load_strategy'       : 'full',
        'load_mode'           : 'overwrite',
        'incremental_col'     : None,
        'incremental_col_type': 'none',
        'primary_keys'        : 'c_custkey',
        'merge_keys'          : 'c_custkey',
        'clustering_type'     : 'none',
        'cluster_by'          : None,
        'process_group'       : 'TPCH_foreign_catalog',
        'process_subgroup'    : 'tpch',
        'src_secret_scope'    : None,
        'priority'            : 1,
        'file_format'         : None,
        'autoloader_mode'     : None,
        'notes'               : 'Full overwrite. ~750K rows.',
    },

    # ══ TPCH_foreign_catalog — priority 2 ════════════════════
    {
        'table_id'            : 'tpch_nation_001',
        'source_type'         : 'snowflake',
        'src_database'        : 'samples',
        'src_schema'          : 'tpch',
        'src_table'           : 'nation',
        'target_table'        : 'tpch_nation',
        'load_strategy'       : 'full',
        'load_mode'           : 'overwrite',
        'incremental_col'     : None,
        'incremental_col_type': 'none',
        'primary_keys'        : 'n_nationkey',
        'merge_keys'          : 'n_nationkey',
        'clustering_type'     : 'none',
        'cluster_by'          : None,
        'process_group'       : 'TPCH_foreign_catalog',
        'process_subgroup'    : 'tpch',
        'src_secret_scope'    : None,
        'priority'            : 2,
        'file_format'         : None,
        'autoloader_mode'     : None,
        'notes'               : 'Reference table. 25 rows.',
    },
    {
        'table_id'            : 'tpch_orders_001',
        'source_type'         : 'snowflake',
        'src_database'        : 'samples',
        'src_schema'          : 'tpch',
        'src_table'           : 'orders',
        'target_table'        : 'orders',
        'load_strategy'       : 'incremental',
        'load_mode'           : 'merge',
        'incremental_col'     : 'o_orderdate',
        'incremental_col_type': 'date',
        'primary_keys'        : 'o_orderkey',
        'merge_keys'          : 'o_orderkey',
        'clustering_type'     : 'liquid',
        'cluster_by'          : 'o_orderdate',
        'process_group'       : 'TPCH_foreign_catalog',
        'process_subgroup'    : 'tpch',
        'src_secret_scope'    : None,
        'priority'            : 2,
        'file_format'         : None,
        'autoloader_mode'     : None,
        'notes'               : 'Incremental merge on o_orderdate. ~7.5M rows.',
    },

    # ══ TPCH_foreign_catalog — priority 3 ════════════════════
    {
        'table_id'            : 'tpch_supplier_001',
        'source_type'         : 'snowflake',
        'src_database'        : 'samples',
        'src_schema'          : 'tpch',
        'src_table'           : 'supplier',
        'target_table'        : 'tpch_supplier',
        'load_strategy'       : 'full',
        'load_mode'           : 'overwrite',
        'incremental_col'     : None,
        'incremental_col_type': 'none',
        'primary_keys'        : 's_suppkey',
        'merge_keys'          : 's_suppkey',
        'clustering_type'     : 'none',
        'cluster_by'          : None,
        'process_group'       : 'TPCH_foreign_catalog',
        'process_subgroup'    : 'tpch',
        'src_secret_scope'    : None,
        'priority'            : 3,
        'file_format'         : None,
        'autoloader_mode'     : None,
        'notes'               : 'Full overwrite. ~50K rows.',
    },
    {
        'table_id'            : 'tpch_part_001',
        'source_type'         : 'snowflake',
        'src_database'        : 'samples',
        'src_schema'          : 'tpch',
        'src_table'           : 'part',
        'target_table'        : 'tpch_part',
        'load_strategy'       : 'full',
        'load_mode'           : 'overwrite',
        'incremental_col'     : None,
        'incremental_col_type': 'none',
        'primary_keys'        : 'p_partkey',
        'merge_keys'          : 'p_partkey',
        'clustering_type'     : 'none',
        'cluster_by'          : None,
        'process_group'       : 'TPCH_foreign_catalog',
        'process_subgroup'    : 'tpch',
        'src_secret_scope'    : None,
        'priority'            : 3,
        'file_format'         : None,
        'autoloader_mode'     : None,
        'notes'               : 'Full overwrite. ~1M rows.',
    },

    # ══ TPCH_foreign_catalog — priority 4 ════════════════════
    {
        'table_id'            : 'tpch_partsupp_001',
        'source_type'         : 'snowflake',
        'src_database'        : 'samples',
        'src_schema'          : 'tpch',
        'src_table'           : 'partsupp',
        'target_table'        : 'tpch_partsupp',
        'load_strategy'       : 'full',
        'load_mode'           : 'overwrite',
        'incremental_col'     : None,
        'incremental_col_type': 'none',
        'primary_keys'        : 'ps_partkey,ps_suppkey',
        'merge_keys'          : 'ps_partkey,ps_suppkey',
        'clustering_type'     : 'liquid',
        'cluster_by'          : 'ps_partkey',
        'process_group'       : 'TPCH_foreign_catalog',
        'process_subgroup'    : 'tpch',
        'src_secret_scope'    : None,
        'priority'            : 4,
        'file_format'         : None,
        'autoloader_mode'     : None,
        'notes'               : 'Composite PK. ~4M rows. Runs after part and supplier.',
    },

    # ══ TPCH_foreign_catalog — priority 5 ════════════════════
    {
        'table_id'            : 'tpch_lineitem_001',
        'source_type'         : 'snowflake',
        'src_database'        : 'samples',
        'src_schema'          : 'tpch',
        'src_table'           : 'lineitem',
        'target_table'        : 'tpch_lineitem',
        'load_strategy'       : 'incremental',
        'load_mode'           : 'merge',
        'incremental_col'     : 'l_shipdate',
        'incremental_col_type': 'date',
        'primary_keys'        : 'l_orderkey,l_linenumber',
        'merge_keys'          : 'l_orderkey,l_linenumber',
        'clustering_type'     : 'liquid',
        'cluster_by'          : 'l_shipdate',
        'process_group'       : 'TPCH_foreign_catalog',
        'process_subgroup'    : 'tpch',
        'src_secret_scope'    : None,
        'priority'            : 5,
        'file_format'         : None,
        'autoloader_mode'     : None,
        'notes'               : 'Largest table. ~30M rows. Composite PK. Incremental on l_shipdate.',
    },

    # ══ TPCH_autoloader — UC Volume CSV ══════════════════════
    {
        'table_id'            : 'tpch_supplier_adls_001',
        'source_type'         : 'volume',
        'src_database'        : TARGET_CATALOG,
        'src_schema'          : TARGET_SCHEMA,
        'src_table'           : 'supplier',
        'target_table'        : 'tpch_supplier_autoloader',
        'load_strategy'       : 'incremental',
        'load_mode'           : 'autoloader',
        'incremental_col'     : None,
        'incremental_col_type': 'none',
        'primary_keys'        : 's_suppkey',
        'merge_keys'          : None,
        'clustering_type'     : 'none',
        'cluster_by'          : None,
        'process_group'       : 'TPCH_autoloader',
        'process_subgroup'    : 'tpch',
        'src_secret_scope'    : None,
        'priority'            : 1,
        'file_format'         : 'csv',
        'autoloader_mode'     : 'availableNow',
        'notes'               : f'Autoloader CSV from {VOLUME_BASE}/supplier/',
    },
    {
        'table_id'            : 'tpch_nation_adls_001',
        'source_type'         : 'volume',
        'src_database'        : TARGET_CATALOG,
        'src_schema'          : TARGET_SCHEMA,
        'src_table'           : 'nation',
        'target_table'        : 'tpch_nation_autoloader',
        'load_strategy'       : 'incremental',
        'load_mode'           : 'autoloader',
        'incremental_col'     : None,
        'incremental_col_type': 'none',
        'primary_keys'        : 'n_nationkey',
        'merge_keys'          : None,
        'clustering_type'     : 'none',
        'cluster_by'          : None,
        'process_group'       : 'TPCH_autoloader',
        'process_subgroup'    : 'tpch',
        'src_secret_scope'    : None,
        'priority'            : 2,
        'file_format'         : 'csv',
        'autoloader_mode'     : 'availableNow',
        'notes'               : f'Autoloader CSV from {VOLUME_BASE}/nation/',
    },

    # ══ VOLUME_copy_into — Volume Parquet ════════════════════
    {
        'table_id'            : 'volume_lineitem_copy_into_001',
        'source_type'         : 'volume',
        'src_database'        : TARGET_CATALOG,
        'src_schema'          : TARGET_SCHEMA,
        'src_table'           : 'lineitem',
        'target_table'        : 'lineitem_copy_into',
        'load_strategy'       : 'full',
        'load_mode'           : 'copy_into',
        'incremental_col'     : None,
        'incremental_col_type': 'none',
        'primary_keys'        : 'l_orderkey,l_linenumber',
        'merge_keys'          : 'l_orderkey,l_linenumber',
        'clustering_type'     : 'liquid',
        'cluster_by'          : 'l_shipdate',
        'process_group'       : 'VOLUME_copy_into',
        'process_subgroup'    : TARGET_SCHEMA,
        'src_secret_scope'    : None,
        'priority'            : 1,
        'file_format'         : 'parquet',
        'autoloader_mode'     : None,
        'notes'               : f'copy_into POC from UC Volume Parquet. Source: {VOLUME_BASE}/lineitem/',
    },

    # ══ PG_NEON_jdbc — Neon PostgreSQL ═══════════════════════
    {
        'table_id'            : 'pg_neon_public_supplier_pg_001',
        'source_type'         : 'postgresql',
        'src_database'        : 'pg_neon',
        'src_schema'          : 'public',
        'src_table'           : 'supplier_pg',
        'target_table'        : 'pg_neon_supplier_pg',
        'load_strategy'       : 'incremental',
        'load_mode'           : 'merge',
        'incremental_col'     : 'updated_at',
        'incremental_col_type': 'timestamp',
        'primary_keys'        : 's_suppkey',
        'merge_keys'          : 's_suppkey',
        'clustering_type'     : 'none',
        'cluster_by'          : None,
        'process_group'       : 'PG_NEON_jdbc',
        'process_subgroup'    : 'public',
        'src_secret_scope'    : 'pg_neon',
        'priority'            : 1,
        'file_format'         : None,
        'autoloader_mode'     : None,
        'notes'               : 'Neon PostgreSQL via foreign catalog pg_neon. Incremental merge on updated_at.',
    },
]

# ── Execute inserts ───────────────────────────────────────────
def val(v):
    if v is None:  return 'NULL'
    if v is True:  return 'true'
    if v is False: return 'false'
    return f"'{str(v).replace(chr(39), chr(39)+chr(39))}'"

existing_ids = set(
    r['table_id'] for r in spark.sql(f"""
        SELECT table_id FROM {CONFIG_TABLE}
    """).collect()
)

inserted = 0
skipped  = 0

for row in rows:
    if row['table_id'] in existing_ids:
        print(f"  Skipped (exists): {row['table_id']}")
        skipped += 1
        continue

    spark.sql(f"""
        INSERT INTO {CONFIG_TABLE} VALUES (
            {val(row['table_id'])},
            {val(row['source_type'])},
            {val(row['src_database'])},
            {val(row['src_schema'])},
            {val(row['src_table'])},
            {val(TARGET_CATALOG)},
            {val(TARGET_SCHEMA)},
            {val(row['target_table'])},
            {val(row['load_strategy'])},
            {val(row['load_mode'])},
            {val(row.get('incremental_col'))},
            {val(row.get('incremental_col_type','none'))},
            NULL,
            NULL,
            {val(row.get('primary_keys'))},
            {val(row.get('merge_keys'))},
            {val(row.get('clustering_type','none'))},
            {val(row.get('cluster_by'))},
            NULL,
            {val(row['process_group'])},
            {val(row.get('process_subgroup'))},
            NULL,
            {val(row.get('src_secret_scope'))},
            false,
            {row['priority']},
            'PENDING',
            NULL,
            NULL,
            NULL,
            {val(row.get('file_format'))},
            {val(row.get('autoloader_mode'))},
            NULL,
            {val(row.get('notes'))}
        )
    """)
    print(f"  Inserted: {row['table_id']}")
    inserted += 1

print(f"\n✅ table_migration_config — Inserted: {inserted} | Skipped: {skipped}")

# COMMAND ----------

# ── Verify config table ───────────────────────────────────────
display(spark.sql(f"""
    SELECT
        table_id,
        source_type,
        src_database || '.' || src_schema || '.' || src_table AS source,
        target_catalog || '.' || target_schema || '.' || target_table AS target,
        load_mode,
        incremental_col,
        primary_keys,
        process_group,
        priority,
        enabled,
        last_run_status
    FROM {CONFIG_TABLE}
    ORDER BY process_group, priority, table_id
"""))

# COMMAND ----------

# ── Verify connection table ───────────────────────────────────
display(spark.sql(f"""
    SELECT
        connection_name,
        source_type,
        connection_method,
        catalog_name,
        secret_scope,
        volume_base_path,
        ssl_enabled,
        enabled,
        notes
    FROM {CONN_TABLE}
    ORDER BY source_type, connection_name
"""))

# COMMAND ----------

# ── Connection tests ──────────────────────────────────────────
print("Testing source connections...")
tests = [
    ("samples.tpch.customer",      "SELECT COUNT(*) AS cnt FROM samples.tpch.customer"),
    ("samples.tpch.lineitem",      "SELECT COUNT(*) AS cnt FROM samples.tpch.lineitem"),
    ("pg_neon.public.supplier_pg", "SELECT COUNT(*) AS cnt FROM pg_neon.public.supplier_pg"),
]

for label, sql in tests:
    try:
        cnt = spark.sql(sql).collect()[0]['cnt']
        print(f"  {label:<45} {cnt:>12,} rows  OK")
    except Exception as e:
        print(f"  {label:<45} FAILED — {str(e)[:80]}")

# Volume check
for subfolder in ['supplier', 'nation', 'lineitem']:
    path = f"{VOLUME_BASE}/{subfolder}/"
    try:
        files = dbutils.fs.ls(path)
        print(f"  {path:<45} {len(files):>12} files  OK")
    except Exception as e:
        print(f"  {path:<45} NOT FOUND — {str(e)[:60]}")

print(f"""
All rows inserted with enabled = false.
Enable by process group when ready:

  -- TPCH foreign catalog (samples.tpch)
  UPDATE {CONFIG_TABLE}
  SET enabled = true
  WHERE process_group = 'TPCH_foreign_catalog';

  -- Autoloader (requires CSV files in volume)
  UPDATE {CONFIG_TABLE}
  SET enabled = true
  WHERE process_group = 'TPCH_autoloader';

  -- Volume copy_into (requires Parquet files in volume)
  UPDATE {CONFIG_TABLE}
  SET enabled = true
  WHERE process_group = 'VOLUME_copy_into';

  -- Neon PostgreSQL (requires pg_neon foreign catalog)
  UPDATE {CONFIG_TABLE}
  SET enabled = true
  WHERE process_group = 'PG_NEON_jdbc';
""")


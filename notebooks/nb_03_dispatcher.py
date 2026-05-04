# Databricks notebook source
# ============================================================
# nb_03_dispatcher
# Core execution engine for the migration workflow.
#
# Reads table_migration_config for the given process_group,
# groups tables by priority tier, then:
#   - Runs tiers SEQUENTIALLY (tier 1 completes before tier 2)
#   - Within each tier runs tables IN PARALLEL via ThreadPool
#
# Routes each table by load_mode:
#   overwrite  → load_full_overwrite()   foreign catalog read
#   append     → load_full_append()      foreign catalog read
#   merge      → load_incremental_merge() foreign catalog read + MERGE INTO
#   copy_into  → load_copy_into()        JDBC export Snowflake→ADLS(Parquet) + COPY INTO Delta
#
# Source routing by source_type:
# src_database = EXACT foreign catalog name as registered in Unity Catalog.
# The dispatcher uses it directly — no prefix added.
# Examples: samples, sf_edw, mssql_crm, pg_marketing, sfdc_prod
#   snowflake   → foreign catalog (sf_{database})
#   postgresql  → foreign catalog (pg_{database}) or JDBC
#   sqlserver   → foreign catalog (mssql_{database}) or JDBC
#   salesforce  → foreign catalog (sfdc_{instance})
#   oracle      → JDBC only (not supported by UC lakehouse federation)
#   mysql       → foreign catalog or JDBC
#   delta       → direct spark.table()
#
# copy_into always uses Parquet format via ADLS intermediary.
# Run copy_into process_groups with max_workers=1 for
# sequential execution to avoid overwhelming Snowflake export.
#
# Workflow parameters:
#   process_group   — which group to run
#   admin_catalog   — Unity Catalog catalog for config table
#   config_schema   — schema for config table
#   max_workers     — max parallel threads per tier (default 8)
#                     set to 1 for copy_into process groups
#   adls_base_path  — base ADLS path for copy_into Parquet files
#   sf_jdbc_url     — Snowflake JDBC URL for copy_into export
#                     format: account.snowflakecomputing.com
# ============================================================

# COMMAND ----------

dbutils.widgets.text("process_group",  "TPCH_foreign_catalog")
dbutils.widgets.text("admin_catalog",  "sandbox")
dbutils.widgets.text("config_schema",  "migration_config")
dbutils.widgets.text("max_workers",    "2")
dbutils.widgets.text("adls_base_path", "abfss://migration@storageaccount.dfs.core.windows.net")
dbutils.widgets.text("sf_jdbc_driver", "net.snowflake.client.jdbc.SnowflakeDriver")
dbutils.widgets.text("notebook_base",   "")  # auto-resolved if blank

PROCESS_GROUP  = dbutils.widgets.get("process_group")
ADMIN_CATALOG  = dbutils.widgets.get("admin_catalog")
CONFIG_SCHEMA  = dbutils.widgets.get("config_schema")
MAX_WORKERS    = int(dbutils.widgets.get("max_workers") or "8")
ADLS_BASE      = dbutils.widgets.get("adls_base_path").rstrip('/')
SF_JDBC_DRIVER = dbutils.widgets.get("sf_jdbc_driver")

# Resolve notebook folder — used by load_autoloader to call nb_autoloader_bronze
_nb_base = dbutils.widgets.get("notebook_base").strip()
if not _nb_base:
    # Auto-resolve from current notebook path if widget not set
    _nb_base = dbutils.notebook.entry_point         .getDbutils().notebook().getContext()         .notebookPath().get()         .rsplit("/", 1)[0]
NOTEBOOK_BASE = _nb_base
CONFIG_TABLE   = f"{ADMIN_CATALOG}.{CONFIG_SCHEMA}.table_migration_config"
CONN_TABLE     = f"{ADMIN_CATALOG}.{CONFIG_SCHEMA}.source_connection_config"

print(f"Process group  : {PROCESS_GROUP}")
print(f"Max workers    : {MAX_WORKERS}")
print(f"ADLS base      : {ADLS_BASE}")

# COMMAND ----------

import concurrent.futures
from datetime import datetime, timezone

# ── Helper: update config row status ─────────────────────────
# On PASSED: clears notes so old failure messages do not persist.
# On FAILED: writes error_msg into notes for diagnosis.
def update_status(table_id, status, sf_count=None, delta_count=None,
                  last_loaded_value=None, error_msg=None):
    sets = [f"last_run_status='{status}'", "last_run_at=CURRENT_TIMESTAMP()"]
    if sf_count    is not None: sets.append(f"last_sf_row_count={sf_count}")
    if delta_count is not None: sets.append(f"last_delta_count={delta_count}")
    if last_loaded_value is not None:
        v = str(last_loaded_value).replace("'","''")
        sets.append(f"last_loaded_value='{v}'")
    if status == 'PASSED':
        # Always clear notes on success — removes previous failure messages
        sets.append("notes=NULL")
    elif error_msg is not None:
        m = str(error_msg)[:2000].replace("'","''")
        sets.append(f"notes='{m}'")
    spark.sql(f"UPDATE {CONFIG_TABLE} SET {', '.join(sets)} WHERE table_id='{table_id}'")

def get_merge_keys(row):
    keys = row['merge_keys'] or row['primary_keys'] or ''
    return [k.strip() for k in keys.split(',') if k.strip()]

def ensure_schema(catalog, schema):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# ── Source reference builder ──────────────────────────────────
# Returns a SQL-addressable table reference string for
# foreign catalog sources, or None for JDBC sources.
#
# src_database in table_migration_config stores the EXACT
# foreign catalog name as registered in Unity Catalog.
# The dispatcher uses it directly — no prefix is added.
#
# Examples:
#   src_database = 'samples'     → samples.tpch.customer
#   src_database = 'mssql_crm'  → mssql_crm.dbo.Accounts
#   src_database = 'sf_edw'     → sf_edw.CONSUMER_PRIVATE.DIM_USER
#   src_database = 'pg_mktg'    → pg_mktg.public.campaigns
#
# Engineers register the foreign catalog in Unity Catalog with
# their chosen name and store that exact name in src_database.
# ── Connection registry lookup ────────────────────────────────
# Reads source_connection_config for the given connection_name.
# Caches results within the session to avoid repeated queries.
# Returns None if table does not exist or no row found —
# dispatcher falls back to legacy direct catalog behavior.
_conn_cache = {}
def get_connection(connection_name):
    if connection_name in _conn_cache:
        return _conn_cache[connection_name]
    try:
        rows = spark.sql(f"""
            SELECT * FROM {CONN_TABLE}
            WHERE connection_name = '{connection_name}'
              AND enabled = true
        """).collect()
        result = rows[0] if rows else None
    except Exception as e:
        # Table may not exist in older environments — degrade gracefully
        print(f"  [CONN] source_connection_config not available: {str(e)[:80]}")
        result = None
    _conn_cache[connection_name] = result
    return result

# ── Source reference builder ──────────────────────────────────
# Looks up connection_method from source_connection_config.
# foreign_catalog → returns SQL reference string for spark.sql()
# jdbc / volume   → returns None (handled by read_jdbc / load_copy_into)
# Falls back to using src_database directly as catalog name if
# no connection config found — backward compatible with existing rows.
def source_ref(row):
    src_database = row['src_database']
    schema       = row['src_schema']
    table        = row['src_table']
    src_type     = (row['source_type'] or '').lower()

    conn = get_connection(src_database)

    if conn:
        method = (conn['connection_method'] or '').lower()
        if method == 'foreign_catalog':
            catalog = conn['catalog_name'] or src_database
            return f"{catalog}.{schema}.{table}"
        elif method in ('jdbc', 'volume'):
            return None  # handled by read_jdbc or load_copy_into
        else:
            return f"{src_database}.{schema}.{table}"

    # Fallback — no connection config found, use src_database directly
    # Covers: all existing foreign catalog rows (samples, pg_neon etc.)
    if src_type in ('oracle', 'teradata', 'sap'):
        return None  # JDBC only
    if src_type in ('adls', 'volume'):
        return None  # UC Volume or ADLS — handled by load functions
    return f"{src_database}.{schema}.{table}"

# ── JDBC read — driven by source_connection_config ───────────
# Builds connection from connection registry.
# Falls back to legacy src_secret_scope if no connection found.
def read_jdbc(row, where_clause=''):
    import json
    conn     = get_connection(row['src_database'])
    schema   = row['src_schema']
    table    = row['src_table']
    dbtable  = f"(SELECT * FROM {schema}.{table} {where_clause}) AS t" \
               if where_clause else f"{schema}.{table}"

    if conn and conn['connection_method'] == 'jdbc':
        scope    = conn['secret_scope']
        host     = dbutils.secrets.get(scope, 'host')
        port     = dbutils.secrets.get(scope, 'port')
        user     = dbutils.secrets.get(scope, 'user')
        password = dbutils.secrets.get(scope, 'password')
        try:
            db = dbutils.secrets.get(scope, 'database')
        except Exception:
            db = row['src_database']

        # Build URL from template — replaces {host} {port} {database}
        url = conn['jdbc_url_template'] \
            .replace('{host}', host) \
            .replace('{port}', port) \
            .replace('{database}', db)

        driver = conn['driver_class']
        extra  = json.loads(conn['extra_options'] or '{}')

        reader = spark.read.format("jdbc") \
            .option("url",      url) \
            .option("dbtable",  dbtable) \
            .option("user",     user) \
            .option("password", password) \
            .option("driver",   driver) \
            .option("pushDownPredicate", "true")

        for k, v in extra.items():
            reader = reader.option(k, v)

        return reader.load()

    # Fallback — legacy secret scope approach
    src_type = (row['source_type'] or '').lower()
    scope    = row['src_secret_scope']
    if not scope:
        raise Exception(f"No connection config or src_secret_scope for '{row['src_database']}'")

    host     = dbutils.secrets.get(scope, 'host')
    port     = dbutils.secrets.get(scope, 'port')
    user     = dbutils.secrets.get(scope, 'user')
    password = dbutils.secrets.get(scope, 'password')
    db       = row['src_database']

    url_map = {
        'oracle'     : f"jdbc:oracle:thin:@{host}:{port}:{db}",
        'postgresql' : f"jdbc:postgresql://{host}:{port}/{db}?sslmode=require",
        'sqlserver'  : f"jdbc:sqlserver://{host}:{port};databaseName={db}",
        'mysql'      : f"jdbc:mysql://{host}:{port}/{db}",
        'redshift'   : f"jdbc:redshift://{host}:{port}/{db}",
    }
    driver_map = {
        'oracle'     : 'oracle.jdbc.driver.OracleDriver',
        'postgresql' : 'org.postgresql.Driver',
        'sqlserver'  : 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
        'mysql'      : 'com.mysql.cj.jdbc.Driver',
        'redshift'   : 'com.amazon.redshift.jdbc42.Driver',
    }
    url    = url_map.get(src_type)
    driver = driver_map.get(src_type)
    if not url or not driver:
        raise Exception(f"JDBC not configured for source_type: {src_type}")

    return spark.read.format("jdbc") \
        .option("url",               url) \
        .option("dbtable",           dbtable) \
        .option("user",              user) \
        .option("password",          password) \
        .option("driver",            driver) \
        .option("queryTimeout",      "0") \
        .option("fetchsize",         "10000") \
        .option("pushDownPredicate", "true") \
        .load()

# ── Read source into DataFrame ────────────────────────────────
def read_source(row, where_clause=''):
    src_type  = (row['source_type'] or 'snowflake').lower()
    load_mode = (row['load_mode']   or '').lower()

    # Autoloader and volume copy_into never use read_source
    # They build their own paths — should never reach here
    if load_mode == 'autoloader' or src_type in ('adls', 'volume'):
        raise Exception(
            f"read_source() should not be called for "
            f"load_mode='{load_mode}' / source_type='{src_type}'. "
            f"These are handled by load_autoloader() and load_copy_into()."
        )

    ref = source_ref(row)

    if ref is None or src_type in ('oracle', 'teradata', 'sap'):
        # JDBC path
        return read_jdbc(row, where_clause)

    # Foreign catalog path
    sql = f"SELECT * FROM {ref}"
    if where_clause:
        sql += f" WHERE {where_clause}"
    return spark.sql(sql)

# ── Apply clustering if configured ────────────────────────────
def apply_clustering(row, tgt):
    cluster_cols = row['cluster_by'] or ''
    if row['clustering_type'] == 'liquid' and cluster_cols:
        cols = ', '.join(c.strip() for c in cluster_cols.split(',') if c.strip())
        try:
            spark.sql(f"ALTER TABLE {tgt} CLUSTER BY ({cols})")
        except Exception:
            pass

# COMMAND ----------

# ── Load function 1: Full overwrite ──────────────────────────
def load_full_overwrite(row):
    table_id     = row['table_id']
    tgt          = f"{row['target_catalog']}.{row['target_schema']}.{row['target_table']}"
    batch_filter = row['batch_filter'] or ''

    print(f"  [OVERWRITE] {row['src_schema']}.{row['src_table']} → {tgt}")
    ensure_schema(row['target_catalog'], row['target_schema'])

    df       = read_source(row, batch_filter)
    sf_count = df.count()

    # ── Respect pre-created DDL schema ───────────────────────
    # If DDL notebook was run first the table already exists with
    # correct data types, column comments and TBLPROPERTIES.
    # Write data only — do NOT overwrite the schema.
    # If table does not exist, create it from source schema.
    table_exists = spark.catalog.tableExists(tgt)
    if table_exists:
        print(f"  [OVERWRITE] Pre-created table found — preserving schema")
        df.write.format('delta').mode('overwrite').saveAsTable(tgt)
    else:
        print(f"  [OVERWRITE] No pre-created table — creating from source schema")
        df.write.format('delta').mode('overwrite') \
          .option('overwriteSchema', 'true').saveAsTable(tgt)

    apply_clustering(row, tgt)
    delta_count = spark.table(tgt).count()
    update_status(table_id, 'PASSED', sf_count=sf_count, delta_count=delta_count)
    print(f"  [OVERWRITE] {tgt} | SF: {sf_count:,} | Delta: {delta_count:,}")
    return table_id, True, None

# ── Load function 2: Full append ─────────────────────────────
def load_full_append(row):
    table_id     = row['table_id']
    tgt          = f"{row['target_catalog']}.{row['target_schema']}.{row['target_table']}"
    batch_filter = row['batch_filter'] or ''

    print(f"  [APPEND] {row['src_schema']}.{row['src_table']} → {tgt}")
    ensure_schema(row['target_catalog'], row['target_schema'])

    df       = read_source(row, batch_filter)
    sf_count = df.count()

    # ── Respect pre-created DDL schema ───────────────────────
    # Append always preserves existing schema.
    # If table does not exist, create from source schema first.
    table_exists = spark.catalog.tableExists(tgt)
    if not table_exists:
        print(f"  [APPEND] No pre-created table — creating from source schema")
        df.write.format('delta').mode('overwrite') \
          .option('overwriteSchema', 'true').saveAsTable(tgt)
    else:
        df.write.format('delta').mode('append').saveAsTable(tgt)

    delta_count = spark.table(tgt).count()
    update_status(table_id, 'PASSED', sf_count=sf_count, delta_count=delta_count)
    print(f"  [APPEND] {tgt} | SF: {sf_count:,} | Delta: {delta_count:,}")
    return table_id, True, None

# ── Load function 3: Incremental merge ───────────────────────
def load_incremental_merge(row):
    table_id       = row['table_id']
    tgt            = f"{row['target_catalog']}.{row['target_schema']}.{row['target_table']}"
    inc_col        = row['incremental_col']
    inc_col_type   = row['incremental_col_type'] or 'timestamp'
    last_val       = row['last_loaded_value']
    merge_key_cols = get_merge_keys(row)

    print(f"  [MERGE] {row['src_schema']}.{row['src_table']} → {tgt} | WM: {inc_col} > {last_val or 'NULL'}")
    ensure_schema(row['target_catalog'], row['target_schema'])

    if last_val:
        where = f"{inc_col} > '{last_val}'" if inc_col_type in ('timestamp','date') \
                else f"{inc_col} > {last_val}"
    else:
        where = ''

    src_df   = read_source(row, where)

    # ── Validate incremental_col exists in source ─────────────
    # The watermark column MUST exist in the source system.
    # Common mistake: adding a column to the target Delta table
    # and using it as a watermark — the dispatcher reads from the
    # source so the column must be there, not in the target.
    # Fix: add the column to the source system, or use a different
    # column that already exists in the source as the watermark.
    source_cols = list(src_df.columns)
    if inc_col not in source_cols:
        raise Exception(
            f"incremental_col '{inc_col}' not found in source {row['src_database']}."
            f"{row['src_schema']}.{row['src_table']}. "
            f"Source columns are: {source_cols}. "
            f"The watermark column must exist in the SOURCE system — "
            f"not just in the target Delta table. "
            f"Options: (1) add '{inc_col}' to the source table, "
            f"(2) use an existing source column as incremental_col, "
            f"(3) switch to load_mode=overwrite if no watermark exists."
        )

    sf_count = src_df.count()

    if sf_count == 0:
        print(f"  [MERGE] No new rows since {last_val} — skipping")
        update_status(table_id, 'PASSED', sf_count=0)
        return table_id, True, None

    view_name = f"_src_{row['src_table'].lower()}_{table_id.replace('-','_')}"
    src_df.createOrReplaceTempView(view_name)

    if not merge_key_cols:
        raise Exception(f"merge_keys is empty for {table_id} — required for merge")

    merge_cond  = ' AND '.join(f"tgt.{k}=src.{k}" for k in merge_key_cols)
    # Only include columns that exist in source — never add target-only
    # audit or derived columns to the merge. Target schema may have
    # additional columns not present in source (e.g. added downstream).
    all_cols    = [c for c in src_df.columns]
    update_set  = ', '.join(f"tgt.{c}=src.{c}" for c in all_cols if c not in merge_key_cols)
    insert_cols = ', '.join(all_cols)
    insert_vals = ', '.join(f"src.{c}" for c in all_cols)

    # ── Respect pre-created DDL schema ───────────────────────
    # If DDL notebook pre-created the table use it as-is.
    # Only create from source schema if table does not exist.
    # MERGE INTO always preserves the existing target schema —
    # only source columns are used in UPDATE SET and INSERT.
    if not spark.catalog.tableExists(tgt):
        print(f"  [MERGE] No pre-created table — creating from source schema")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {tgt} USING DELTA AS SELECT * FROM {view_name} WHERE 1=0")
    else:
        print(f"  [MERGE] Pre-created table found — preserving schema")
    apply_clustering(row, tgt)

    spark.sql(f"""
        MERGE INTO {tgt} AS tgt USING {view_name} AS src
        ON {merge_cond}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)

    new_wm      = spark.sql(f"SELECT MAX({inc_col}) AS wm FROM {view_name}").collect()[0]['wm']
    delta_count = spark.table(tgt).count()
    update_status(table_id, 'PASSED', sf_count=sf_count,
                  delta_count=delta_count, last_loaded_value=str(new_wm))
    print(f"  [MERGE] {tgt} | New: {sf_count:,} | Total: {delta_count:,} | WM: {new_wm}")
    return table_id, True, None

# ── Load function 4: COPY INTO via ADLS (Parquet) ────────────
# Two-step process:
#   Step 1 — JDBC: tell Snowflake to COPY INTO @ADLS_stage as Parquet
#   Step 2 — Databricks: COPY INTO Delta from ADLS Parquet files
#
# Always uses Parquet format for maximum compatibility and
# performance between Snowflake and Databricks.
# Run this load_mode in a dedicated copy_into process_group
# with max_workers=1 to avoid overwhelming Snowflake exports.
def load_copy_into(row):
    table_id    = row['table_id']
    tgt         = f"{row['target_catalog']}.{row['target_schema']}.{row['target_table']}"
    source_type = (row['source_type'] or '').lower()

    # ── Route by source_type ──────────────────────────────────
    # source_type = adls   → UC Volume path (POC / external orchestrator)
    # source_type = snowflake → ADLS blob via Snowflake JDBC export
    if source_type in ('adls', 'volume'):
        # Volume path: /Volumes/{src_database}/{src_schema}/landing/{src_table}/
        src_path = (
            f"/Volumes/{row['src_database']}/{row['src_schema']}/"
            f"landing/{row['src_table']}/"
        )
        print(f"  [COPY INTO] Volume path → {tgt}")
        print(f"  Source     : {src_path}")
        ensure_schema(row['target_catalog'], row['target_schema'])

        # Verify Parquet files exist in volume
        try:
            files = dbutils.fs.ls(src_path)
            parquet_files = [f for f in files if f.name.endswith('.parquet')]
            if not parquet_files:
                raise Exception(f"No Parquet files found in {src_path}")
            print(f"  Found {len(parquet_files)} Parquet file(s)")
        except Exception as e:
            raise Exception(f"Volume path verification failed: {str(e)}")

    else:
        # Snowflake JDBC export path → ADLS blob → COPY INTO Delta
        scope     = row['src_secret_scope']
        src_path  = (
            f"{ADLS_BASE}/{row['process_group']}/"
            f"{row['src_schema']}/{row['src_table']}/"
        )
        sf_stage_path = (
            f"SANDBOX.DBX_MIGRATION_PRIVATE.ADLS_MIGRATION_STAGE/"
            f"{row['process_group']}/{row['src_schema']}/{row['src_table']}/"
        )

        print(f"  [COPY INTO] Snowflake → ADLS → {tgt}")
        print(f"  ADLS       : {src_path}")
        ensure_schema(row['target_catalog'], row['target_schema'])

        # Step 1: Trigger Snowflake COPY INTO @ADLS_stage via JDBC
        sf_account  = dbutils.secrets.get(scope, 'account')
        sf_user     = dbutils.secrets.get(scope, 'user')
        sf_password = dbutils.secrets.get(scope, 'password')
        sf_wh       = dbutils.secrets.get(scope, 'warehouse')
        sf_jdbc_url = f"jdbc:snowflake://{sf_account}.snowflakecomputing.com"

        copy_sql = f"""
            COPY INTO @{sf_stage_path}
            FROM {row['src_database']}.{row['src_schema']}.{row['src_table']}
            FILE_FORMAT = (TYPE=PARQUET SNAPPY_COMPRESSION=TRUE)
            OVERWRITE=TRUE HEADER=TRUE MAX_FILE_SIZE=104857600
        """
        try:
            spark.read.format("jdbc") \
                .option("url",          sf_jdbc_url) \
                .option("user",         sf_user) \
                .option("password",     sf_password) \
                .option("driver",       SF_JDBC_DRIVER) \
                .option("warehouse",    sf_wh) \
                .option("query",        copy_sql) \
                .option("queryTimeout", "0") \
                .load()
            print(f"  Snowflake export complete → {src_path}")
        except Exception as e:
            print(f"  JDBC note: {str(e)[:200]} — checking ADLS for files")

        # Verify files landed in ADLS
        try:
            files = dbutils.fs.ls(src_path)
            parquet_files = [f for f in files if f.name.endswith('.parquet')]
            if not parquet_files:
                raise Exception(f"No Parquet files found in {src_path} after export")
            print(f"  Found {len(parquet_files)} Parquet file(s) in ADLS")
        except Exception as e:
            raise Exception(f"ADLS verification failed: {str(e)}")

    # ── COPY INTO Delta from source path (volume or ADLS) ─────
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tgt}
        USING DELTA
        COMMENT 'Loaded via COPY INTO Parquet. Source: {row["src_database"]}.{row["src_schema"]}.{row["src_table"]}'
    """)

    apply_clustering(row, tgt)

    spark.sql(f"""
        COPY INTO {tgt}
        FROM '{src_path}'
        FILEFORMAT = PARQUET
        FORMAT_OPTIONS (
            'mergeSchema'    = 'true',
            'pathGlobFilter' = '*.parquet'
        )
        COPY_OPTIONS (
            'mergeSchema' = 'true',
            'force'       = 'false'
        )
    """)

    delta_count = spark.table(tgt).count()
    update_status(table_id, 'PASSED', delta_count=delta_count)
    print(f"  [COPY INTO] {tgt} | Delta rows: {delta_count:,}")
    return table_id, True, None

# COMMAND ----------

# ── Load function 5: Autoloader ──────────────────────────────
# Calls nb_autoloader_bronze as a child notebook via
# dbutils.notebook.run(). Autoloader tracks processed files
# via checkpoint — no watermark column needed.
# Returns PASSED/FAILED based on notebook exit result.
def load_autoloader(row):
    table_id = row['table_id']
    print(f"  [AUTOLOADER] {row['src_schema']}.{row['src_table']} → "
          f"{row['target_catalog']}.{row['target_schema']}.{row['target_table']}")

    try:
        result = dbutils.notebook.run(
            f"{NOTEBOOK_BASE}/nb_autoloader_bronze",
            timeout_seconds = 3600,
            arguments = {
                "table_id"        : table_id,
                "admin_catalog"   : ADMIN_CATALOG,
                "config_schema"   : CONFIG_SCHEMA,
                "volume_name"     : "landing",
                "autoloader_mode" : row['autoloader_mode'] or "availableNow",
            }
        )
        print(f"  [AUTOLOADER] {table_id} : {result}")
        # nb_autoloader_bronze updates config table directly on success
        return table_id, True, None
    except Exception as e:
        raise Exception(f"Autoloader notebook failed: {str(e)[:500]}")

# ── Router ────────────────────────────────────────────────────
def run_table(row):
    table_id  = row['table_id']
    load_mode = (row['load_mode'] or '').lower()
    try:
        if load_mode == 'overwrite':
            return load_full_overwrite(row)
        elif load_mode == 'append':
            return load_full_append(row)
        elif load_mode == 'merge':
            return load_incremental_merge(row)
        elif load_mode == 'copy_into':
            return load_copy_into(row)
        elif load_mode == 'autoloader':
            return load_autoloader(row)
        else:
            raise Exception(f"Unknown load_mode: '{load_mode}'")
    except Exception as e:
        err = f"{load_mode.upper()} FAILED: {str(e)[:1800]}"
        print(f"  [{table_id}] {err}")
        update_status(table_id, 'FAILED', error_msg=err)
        return table_id, False, err

# COMMAND ----------

# ── Load enabled rows ────────────────────────────────────────
# Looks for RUNNING rows first (set by nb_02_workflow_init).
# Falls back to PENDING rows if running dispatcher standalone
# without the init task — useful for testing.

all_rows = spark.sql(f"""
    SELECT * FROM {CONFIG_TABLE}
    WHERE process_group   = '{PROCESS_GROUP}'
      AND enabled         = true
      AND last_run_status = 'RUNNING'
    ORDER BY priority, table_id
""").collect()

if not all_rows:
    print("No RUNNING rows found — checking for PENDING rows (standalone mode)...")
    all_rows = spark.sql(f"""
        SELECT * FROM {CONFIG_TABLE}
        WHERE process_group   = '{PROCESS_GROUP}'
          AND enabled         = true
          AND last_run_status = 'PENDING'
        ORDER BY priority, table_id
    """).collect()
    if all_rows:
        print(f"Found {len(all_rows)} PENDING rows — marking RUNNING for standalone run")
        ids = "', '".join(r['table_id'] for r in all_rows)
        spark.sql(f"""
            UPDATE {CONFIG_TABLE}
            SET last_run_status = 'RUNNING',
                last_run_at     = CURRENT_TIMESTAMP()
            WHERE table_id IN ('{ids}')
        """)
    else:
        # process_group exists but nothing to run — check if it exists at all
        total_in_group = spark.sql(f"""
            SELECT COUNT(*) AS cnt FROM {CONFIG_TABLE}
            WHERE process_group = '{PROCESS_GROUP}'
        """).collect()[0]['cnt']

        if total_in_group == 0:
            raise Exception(
                f"process_group '{PROCESS_GROUP}' not found in table_migration_config. "
                f"Check the workflow job parameter — this may be a typo."
            )

        print(f"process_group '{PROCESS_GROUP}' has {total_in_group} rows but none "
              f"are RUNNING or PENDING. Nothing to dispatch.")
        print("This is expected if init was skipped or all tables already ran.")
        dbutils.notebook.exit(
            f"DISPATCHER_SKIPPED | {PROCESS_GROUP} | {total_in_group} total rows | "
            f"0 RUNNING or PENDING — nothing to dispatch"
        )

# ── Group by priority tier ────────────────────────────────────
tier_map = {}
for row in all_rows:
    p = row['priority'] or 1
    if p not in tier_map: tier_map[p] = []
    tier_map[p].append(row)

print(f"Tables: {len(all_rows)} | Tiers: {sorted(tier_map.keys())} | Max workers: {MAX_WORKERS}")

if MAX_WORKERS > 1 and any(r['load_mode'] == 'copy_into' for r in all_rows):
    print("WARNING: copy_into tables detected with max_workers > 1.")
    print("         Set max_workers=1 for copy_into process groups.")

# COMMAND ----------

# ── Execute: sequential tiers, parallel within each tier ──────
results    = []
all_passed = True

for priority in sorted(tier_map.keys()):
    tier_rows = tier_map[priority]
    print(f"\n{'='*60}")
    print(f"Tier {priority} — {len(tier_rows)} table(s) | workers: {min(MAX_WORKERS, len(tier_rows))}")
    print(f"{'='*60}")

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=min(MAX_WORKERS, len(tier_rows))
    ) as executor:
        futures = {executor.submit(run_table, row): row['table_id'] for row in tier_rows}
        for future in concurrent.futures.as_completed(futures):
            tid = futures[future]
            try:
                t, passed, err = future.result()
                results.append({'table_id': t, 'passed': passed, 'error': err})
                if not passed: all_passed = False
            except Exception as e:
                results.append({'table_id': tid, 'passed': False, 'error': str(e)})
                all_passed = False
                update_status(tid, 'FAILED', error_msg=str(e)[:1800])

    tier_pass = sum(1 for r in results if r['passed'] and r['table_id'] in [rw['table_id'] for rw in tier_rows])
    print(f"Tier {priority} — Passed: {tier_pass} | Failed: {len(tier_rows)-tier_pass}")

# COMMAND ----------

total_passed = sum(1 for r in results if r['passed'])
total_failed = len(results) - total_passed

print(f"\n{'='*60}")
print(f"DISPATCHER COMPLETE — {PROCESS_GROUP}")
print(f"Passed: {total_passed} | Failed: {total_failed}")
if total_failed > 0:
    for r in results:
        if not r['passed']:
            print(f"  {r['table_id']}: {r['error']}")

dbutils.notebook.exit(f"DISPATCHER | Passed: {total_passed} | Failed: {total_failed} | {PROCESS_GROUP}")
if not rows:
    raise Exception(
        f"No enabled rows found for process_group='{PROCESS_GROUP}'. ..."
    )

# Databricks notebook source
# ============================================================
# nb_02_workflow_init
# First task in every migration workflow.
# Validates config, marks enabled tables as RUNNING,
# and emits priority tier information for the dispatcher.
#
# Workflow parameters:
#   process_group  — which group to run e.g. SNOWFLAKE_SAMPLE_DATA
#   admin_catalog  — Unity Catalog catalog for config table
#   config_schema  — schema for config table
# ============================================================

# COMMAND ----------

# ── Parameters ────────────────────────────────────────────────
dbutils.widgets.text("process_group",  "TPCH_foreign_catalog")
dbutils.widgets.text("admin_catalog",  "sandbox")
dbutils.widgets.text("config_schema",  "migration_config")

PROCESS_GROUP = dbutils.widgets.get("process_group")
ADMIN_CATALOG = dbutils.widgets.get("admin_catalog")
CONFIG_SCHEMA = dbutils.widgets.get("config_schema")
CONFIG_TABLE  = f"{ADMIN_CATALOG}.{CONFIG_SCHEMA}.table_migration_config"
CONN_TABLE    = f"{ADMIN_CATALOG}.{CONFIG_SCHEMA}.source_connection_config"

print(f"Process group : {PROCESS_GROUP}")
print(f"Config table  : {CONFIG_TABLE}")

# COMMAND ----------

from datetime import datetime, timezone

# ── Two-stage row check ───────────────────────────────────────
# Stage 1: does the process_group exist at all in the config table?
#           If not — likely a typo in the workflow parameter — raise.
# Stage 2: are there enabled rows to process?
#           If not — nothing to do, exit cleanly (not an error).

total_in_group = spark.sql(f"""
    SELECT COUNT(*) AS cnt FROM {CONFIG_TABLE}
    WHERE process_group = '{PROCESS_GROUP}'
""").collect()[0]['cnt']

if total_in_group == 0:
    raise Exception(
        f"process_group '{PROCESS_GROUP}' not found in table_migration_config. "
        f"Check the workflow job parameter — this may be a typo."
    )

config_df = spark.sql(f"""
    SELECT *
    FROM {CONFIG_TABLE}
    WHERE process_group = '{PROCESS_GROUP}'
      AND enabled       = true
    ORDER BY priority, table_id
""")

rows = config_df.collect()

if not rows:
    # process_group exists but nothing enabled — not an error, just nothing to do
    print(f"process_group '{PROCESS_GROUP}' has {total_in_group} total rows, 0 enabled.")
    print("Nothing to process — tables may already be loaded or not yet enabled.")
    print("Set enabled=true to include tables in the next run.")
    dbutils.notebook.exit(
        f"INIT_SKIPPED | {PROCESS_GROUP} | {total_in_group} total rows | "
        f"0 enabled — nothing to do"
    )

print(f"Found {len(rows)} enabled tables for process_group: {PROCESS_GROUP}")

# COMMAND ----------

# ── Validate each row ─────────────────────────────────────────
validation_errors = []

for row in rows:
    tid = row['table_id']

    # Required fields
    if not row['src_database']:
        validation_errors.append(f"{tid}: src_database is null")
    if not row['src_schema']:
        validation_errors.append(f"{tid}: src_schema is null")
    if not row['src_table']:
        validation_errors.append(f"{tid}: src_table is null")
    if not row['target_catalog']:
        validation_errors.append(f"{tid}: target_catalog is null")
    if not row['target_schema']:
        validation_errors.append(f"{tid}: target_schema is null")
    if not row['target_table']:
        validation_errors.append(f"{tid}: target_table is null")
    if not row['load_mode']:
        validation_errors.append(f"{tid}: load_mode is null")
    if row['load_mode'] not in ('overwrite', 'append', 'merge', 'copy_into', 'autoloader'):
        validation_errors.append(f"{tid}: invalid load_mode '{row['load_mode']}' — must be overwrite|append|merge|copy_into|autoloader")

    # Incremental-specific validation
    # Autoloader tracks state via checkpoint — no watermark column needed
    # Only validate incremental_col for merge load_mode
    if row['load_strategy'] == 'incremental' and row['load_mode'] != 'autoloader':
        if not row['incremental_col']:
            validation_errors.append(f"{tid}: incremental_col is null but load_strategy=incremental")
        if not row['primary_keys'] and not row['merge_keys']:
            validation_errors.append(f"{tid}: primary_keys and merge_keys both null — required for merge")

    # Validate connection exists in source_connection_config
    # This replaces hardcoded source_type checks — any new source
    # just needs a row in source_connection_config, no code change.
    try:
        conn_rows = spark.sql(f"""
            SELECT connection_method, secret_scope
            FROM {CONN_TABLE}
            WHERE connection_name = '{row["src_database"]}'
              AND enabled = true
        """).collect()
        if not conn_rows:
            # volume and adls source types use UC Volume paths —
            # no connection registry row required
            # All other source types must be registered
            no_registry_needed = (
                row['source_type'] in ('volume', 'adls') or
                row['load_mode'] == 'autoloader'
            )
            if not no_registry_needed:
                validation_errors.append(
                    f"{tid}: no active connection found for src_database="
                    f"'{row['src_database']}' in source_connection_config. "
                    f"Add a row to source_connection_config first."
                )
        else:
            conn = conn_rows[0]
            # JDBC connections must have a secret scope defined
            if conn['connection_method'] == 'jdbc' and not conn['secret_scope']:
                validation_errors.append(
                    f"{tid}: connection '{row['src_database']}' is jdbc "
                    f"but has no secret_scope in source_connection_config"
                )
            # Snowflake copy_into needs scope for JDBC export step
            if (row['load_mode'] == 'copy_into' and
                row['source_type'] == 'snowflake' and
                not conn['secret_scope']):
                validation_errors.append(
                    f"{tid}: Snowflake copy_into requires secret_scope "
                    f"in source_connection_config for JDBC export step"
                )
    except Exception as e:
        # source_connection_config may not exist in older environments
        # Fall back to legacy secret scope validation
        jdbc_only_sources = ('oracle', 'teradata', 'sap')
        snowflake_copy_into = (
            row['load_mode'] == 'copy_into' and
            row['source_type'] == 'snowflake'
        )
        needs_scope = (
            row['source_type'] in jdbc_only_sources or
            snowflake_copy_into
        )
        if needs_scope and not row['src_secret_scope']:
            validation_errors.append(
                f"{tid}: src_secret_scope is null but required for "
                f"source_type='{row['source_type']}' or load_mode='copy_into'"
            )

if validation_errors:
    error_msg = f"Validation failed for process_group '{PROCESS_GROUP}':\n" + "\n".join(validation_errors)
    print(f" {error_msg}")
    raise Exception(error_msg)

print(f" All {len(rows)} rows passed validation")

# COMMAND ----------

# ── Connection smoke test ─────────────────────────────────────
# Validates each unique source connection is reachable before
# marking any rows as RUNNING. Fails fast with a clear message
# rather than letting the dispatcher fail mid-run.
#
# Tests by connection_method:
#   foreign_catalog → SHOW TABLES IN {catalog}.{schema}
#   volume          → dbutils.fs.ls(volume_base_path)
#   jdbc            → read one row via JDBC

print("Running connection smoke tests...")
conn_errors = []

# Get unique src_database values for this run
# Skip autoloader and copy_into — they validate paths differently
test_rows = [
    r for r in rows
    if r['load_mode'] not in ('autoloader', 'copy_into')
]
unique_sources = {r['src_database'] for r in test_rows}

for src_db in unique_sources:
    conn = None
    try:
        conn_rows = spark.sql(f"""
            SELECT * FROM {CONN_TABLE}
            WHERE connection_name = '{src_db}'
              AND enabled = true
        """).collect()
        conn = conn_rows[0] if conn_rows else None
    except Exception:
        conn = None

    if conn is None:
        # No connection registry row — try direct catalog access as fallback
        try:
            spark.sql(f"SHOW CATALOGS").filter(f"catalog = '{src_db}'").collect()
            # If we get here the catalog at least exists
            print(f"   {src_db} — accessible (no registry row, direct catalog check)")
        except Exception as e:
            conn_errors.append(
                f"{src_db}: not found in source_connection_config and "
                f"direct catalog check failed: {str(e)[:120]}"
            )
        continue

    method = (conn['connection_method'] or '').lower()

    if method == 'foreign_catalog':
        catalog = conn['catalog_name'] or src_db
        try:
            # Test the foreign catalog is registered and reachable
            spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()
            print(f"   {src_db} — foreign catalog '{catalog}' reachable")
        except Exception as e:
            conn_errors.append(
                f"{src_db}: foreign catalog '{catalog}' not reachable. "
                f"Run nb_setup_connections to recreate it. "
                f"Error: {str(e)[:120]}"
            )

    elif method == 'volume':
        vol_path = conn['volume_base_path']
        if vol_path:
            try:
                dbutils.fs.ls(vol_path)
                print(f"   {src_db} — volume path '{vol_path}' accessible")
            except Exception as e:
                conn_errors.append(
                    f"{src_db}: volume path '{vol_path}' not accessible. "
                    f"Check the volume exists in Unity Catalog. "
                    f"Error: {str(e)[:120]}"
                )
        else:
            print(f"  ⚠️  {src_db} — volume connection has no volume_base_path set")

    elif method == 'jdbc':
        scope = conn['secret_scope']
        template = conn['jdbc_url_template']
        if scope and template:
            try:
                host     = dbutils.secrets.get(scope, 'host')
                port     = dbutils.secrets.get(scope, 'port')
                user     = dbutils.secrets.get(scope, 'user')
                password = dbutils.secrets.get(scope, 'password')
                try:
                    db = dbutils.secrets.get(scope, 'database')
                except Exception:
                    db = src_db
                url = template                     .replace('{host}', host)                     .replace('{port}', port)                     .replace('{database}', db)
                # Test with a lightweight query
                spark.read.format("jdbc")                     .option("url",    url)                     .option("dbtable","(SELECT 1 AS test) AS t")                     .option("user",   user)                     .option("password", password)                     .option("driver", conn['driver_class'])                     .load().collect()
                print(f"   {src_db} — JDBC connection reachable")
            except Exception as e:
                conn_errors.append(
                    f"{src_db}: JDBC connection failed. "
                    f"Check secret scope '{scope}' and network access. "
                    f"Error: {str(e)[:150]}"
                )
        else:
            conn_errors.append(
                f"{src_db}: jdbc connection missing secret_scope or "
                f"jdbc_url_template in source_connection_config"
            )
    else:
        print(f"    {src_db} — unknown connection_method '{method}', skipping test")

# Also test volume paths for autoloader rows
autoloader_rows = [r for r in rows if r['load_mode'] == 'autoloader']
unique_autoloader_sources = {r['src_database'] for r in autoloader_rows}
for src_db in unique_autoloader_sources:
    try:
        conn_rows = spark.sql(f"""
            SELECT volume_base_path FROM {CONN_TABLE}
            WHERE connection_name = '{src_db}' AND enabled = true
        """).collect()
        if conn_rows and conn_rows[0]['volume_base_path']:
            vol_path = conn_rows[0]['volume_base_path']
            dbutils.fs.ls(vol_path)
            print(f"   {src_db} — autoloader volume '{vol_path}' accessible")
        else:
            # Try building path from convention
            sample_row = next(r for r in autoloader_rows if r['src_database'] == src_db)
            vol_path = f"/Volumes/{src_db}/{sample_row['src_schema']}/landing"
            dbutils.fs.ls(vol_path)
            print(f"   {src_db} — autoloader volume '{vol_path}' accessible")
    except Exception as e:
        conn_errors.append(
            f"{src_db}: autoloader volume not accessible. "
            f"Error: {str(e)[:120]}"
        )

if conn_errors:
    raise Exception(
        f"Connection smoke test failed for {len(conn_errors)} source(s) "
        f"in process_group '{PROCESS_GROUP}':\n" +
        "\n".join(f"  - {e}" for e in conn_errors)
    )

print(f" All connections reachable — proceeding to mark rows RUNNING")

# COMMAND ----------

# ── Mark all enabled rows as RUNNING ──────────────────────────
now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

table_ids = "', '".join(row['table_id'] for row in rows)

spark.sql(f"""
    UPDATE {CONFIG_TABLE}
    SET
        last_run_status = 'RUNNING',
        last_run_at     = CURRENT_TIMESTAMP()
    WHERE process_group = '{PROCESS_GROUP}'
      AND enabled       = true
      AND table_id IN ('{table_ids}')
""")

print(f" Marked {len(rows)} rows as RUNNING")

# COMMAND ----------

# ── Build priority tier summary ───────────────────────────────
# Group table_ids by priority tier
# Emitted as notebook output for reference
tier_map = {}
for row in rows:
    p = row['priority'] or 1
    if p not in tier_map:
        tier_map[p] = []
    tier_map[p].append(row['table_id'])

tier_summary = []
for priority in sorted(tier_map.keys()):
    tids = tier_map[priority]
    tier_summary.append(f"Priority {priority} ({len(tids)} tables): {', '.join(tids)}")
    print(f"  Tier {priority}: {tids}")

print(f"\n Workflow init complete — {len(tier_map)} priority tiers")
print(f"   Tiers: {sorted(tier_map.keys())}")

# ── Pass summary to job output ────────────────────────────────
dbutils.notebook.exit(f"INIT_OK | {len(rows)} tables | {len(tier_map)} tiers | {PROCESS_GROUP}")

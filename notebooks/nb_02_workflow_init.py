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

# ── Load enabled rows for this process_group ──────────────────
config_df = spark.sql(f"""
    SELECT *
    FROM {CONFIG_TABLE}
    WHERE process_group = '{PROCESS_GROUP}'
      AND enabled       = true
    ORDER BY priority, table_id
""")

rows = config_df.collect()

if not rows:
    dbutils.notebook.exit(
        f"No enabled rows found for process_group='{PROCESS_GROUP}'. "
        f"Check table_migration_config — enabled must be true."
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

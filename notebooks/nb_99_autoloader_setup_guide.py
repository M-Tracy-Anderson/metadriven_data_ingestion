# Databricks notebook source
# ============================================================
# nb_autoloader_setup_guide
# Reference notebook — not executed as part of a workflow.
# Documents how to configure and run Autoloader bronze
# ingestion alongside the existing migration framework.
#
# Covers:
#   - When to use Autoloader vs COPY INTO
#   - table_migration_config setup for Autoloader tables
#   - Workflow setup for scheduled (availableNow) mode
#   - Continuous streaming job setup
#   - Monitoring and checkpoint management
# ============================================================

# COMMAND ----------

# MAGIC %md
# MAGIC  ## Autoloader vs COPY INTO — decision guide
# MAGIC
# MAGIC  | Scenario | Use |
# MAGIC  |---|---|
# MAGIC  | Snowflake migration — point in time | COPY INTO |
# MAGIC  | External batch files, known schedule | COPY INTO (availableNow trigger) |
# MAGIC  | Continuous file arrival, sub-hourly | Autoloader (continuous trigger) |
# MAGIC  | High volume small files | Autoloader |
# MAGIC  | IoT, clickstream, event feeds | Autoloader |
# MAGIC  | Initial historical backfill | COPY INTO |
# MAGIC  | Ongoing operational ingestion | Autoloader |
# MAGIC
# MAGIC  Both land data into **bronze Delta tables**.
# MAGIC  Neither does silver or gold transformations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Add autoloader support columns to table_migration_config
# MAGIC
# MAGIC  Run once to extend the config table schema.

# COMMAND ----------

dbutils.widgets.text("admin_catalog", "it")
dbutils.widgets.text("config_schema", "migration_config")

ADMIN_CATALOG = dbutils.widgets.get("admin_catalog")
CONFIG_SCHEMA = dbutils.widgets.get("config_schema")
CONFIG_TABLE  = f"{ADMIN_CATALOG}.{CONFIG_SCHEMA}.table_migration_config"

# Add new columns for Autoloader support
# These are additive — existing rows unaffected
new_columns = [
    ("file_format",       "STRING",  "parquet | json | csv — source file format in ADLS"),
    ("autoloader_mode",   "STRING",  "availableNow | continuous — trigger mode for Autoloader"),
    ("schema_hints",      "STRING",  "Optional schema hints for Autoloader e.g. col1 INT, col2 STRING"),
]

for col_name, col_type, comment in new_columns:
    try:
        spark.sql(f"""
            ALTER TABLE {CONFIG_TABLE}
            ADD COLUMN IF NOT EXISTS {col_name} {col_type}
            COMMENT '{comment}'
        """)
        print(f"Added column: {col_name}")
    except Exception as e:
        print(f"Column {col_name} may already exist: {str(e)[:80]}")

print("Schema update complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Insert config rows for Autoloader tables

# COMMAND ----------

# ── Example 1: JSON feed — CRM events (continuous, sub-hourly) ─
# External orchestrator (ADF) lands JSON files continuously
# Files arrive every few minutes — use continuous Autoloader

spark.sql(f"""
INSERT INTO {CONFIG_TABLE}
(table_id, source_type,
 src_database, src_schema, src_table,
 target_catalog, target_schema, target_table,
 load_strategy, load_mode,
 incremental_col, incremental_col_type, last_loaded_value,
 batch_filter, primary_keys, merge_keys,
 clustering_type, cluster_by, partition_col,
 process_group, process_subgroup, depends_on_group,
 src_secret_scope, enabled, priority,
 last_run_status, last_run_at,
 last_sf_row_count, last_delta_count,
 file_format, autoloader_mode, notes)
VALUES (
    'adls_crm_events_autoloader_001',
    'adls',
    'raw', 'crm', 'events',
    'it', 'bronze', 'crm_events',
    'incremental', 'autoloader',
    NULL, 'none', NULL,
    NULL, NULL, NULL,
    'liquid', 'event_date', NULL,
    'CRM_streaming', 'crm', NULL,
    NULL, false, 1,
    'PENDING', NULL,
    NULL, NULL,
    'json', 'continuous',
    'CRM events JSON feed from ADF. Arrives continuously. Autoloader continuous mode. file_format=json'
)
""")
print("Inserted: crm_events (continuous JSON)")

# ── Example 2: CSV feed — daily batch from Salesforce export ──
# Salesforce exports CSV files daily at 2am via ADF
# Use availableNow trigger — process daily batch then stop

spark.sql(f"""
INSERT INTO {CONFIG_TABLE}
(table_id, source_type,
 src_database, src_schema, src_table,
 target_catalog, target_schema, target_table,
 load_strategy, load_mode,
 incremental_col, incremental_col_type, last_loaded_value,
 batch_filter, primary_keys, merge_keys,
 clustering_type, cluster_by, partition_col,
 process_group, process_subgroup, depends_on_group,
 src_secret_scope, enabled, priority,
 last_run_status, last_run_at,
 last_sf_row_count, last_delta_count,
 file_format, autoloader_mode, notes)
VALUES (
    'adls_sfdc_accounts_autoloader_001',
    'adls',
    'raw', 'salesforce', 'accounts',
    'it', 'bronze', 'sfdc_accounts',
    'incremental', 'autoloader',
    NULL, 'none', NULL,
    NULL, 'AccountId', NULL,
    'none', NULL, NULL,
    'SFDC_batch', 'salesforce', NULL,
    NULL, false, 1,
    'PENDING', NULL,
    NULL, NULL,
    'csv', 'availableNow',
    'Salesforce accounts daily CSV export via ADF. file_format=csv'
)
""")
print("Inserted: sfdc_accounts (daily CSV batch)")

# ── Example 3: Parquet feed — IoT sensor data ─────────────────
# Sensor platform lands Parquet files every 5 minutes
# High volume — Autoloader continuous

spark.sql(f"""
INSERT INTO {CONFIG_TABLE}
(table_id, source_type,
 src_database, src_schema, src_table,
 target_catalog, target_schema, target_table,
 load_strategy, load_mode,
 incremental_col, incremental_col_type, last_loaded_value,
 batch_filter, primary_keys, merge_keys,
 clustering_type, cluster_by, partition_col,
 process_group, process_subgroup, depends_on_group,
 src_secret_scope, enabled, priority,
 last_run_status, last_run_at,
 last_sf_row_count, last_delta_count,
 file_format, autoloader_mode, notes)
VALUES (
    'adls_iot_telemetry_autoloader_001',
    'adls',
    'raw', 'iot', 'telemetry',
    'it', 'bronze', 'iot_telemetry',
    'incremental', 'autoloader',
    NULL, 'none', NULL,
    NULL, 'device_id', NULL,
    'liquid', 'event_timestamp,device_id', NULL,
    'IOT_streaming', 'iot', NULL,
    NULL, false, 1,
    'PENDING', NULL,
    NULL, NULL,
    'parquet', 'continuous',
    'IoT sensor telemetry. Files every 5 min. High volume. file_format=parquet'
)
""")
print("Inserted: iot_telemetry (continuous Parquet)")

# COMMAND ----------

# MAGIC %md
# MAGIC  ## Step 3 — Workflow setup by trigger mode
# MAGIC
# MAGIC  ### Option A: availableNow (scheduled batch)
# MAGIC  Create a Databricks Workflow with one task per table:
# MAGIC  ```
# MAGIC  Task: load_sfdc_accounts
# MAGIC    Notebook: nb_autoloader_bronze
# MAGIC    Parameters:
# MAGIC      table_id         = adls_sfdc_accounts_autoloader_001
# MAGIC      admin_catalog    = it
# MAGIC      config_schema    = migration_config
# MAGIC      adls_base_path   = abfss://raw@storage.dfs.core.windows.net
# MAGIC      checkpoint_base  = abfss://checkpoints@storage.dfs.core.windows.net
# MAGIC      autoloader_mode  = availableNow
# MAGIC  Schedule: daily at 3am (after ADF lands files at 2am)
# MAGIC  ```
# MAGIC
# MAGIC  ### Option B: continuous (always-on streaming)
# MAGIC  Create a Databricks Job with one task per streaming table.
# MAGIC  Set the cluster to always-on (no auto-termination).
# MAGIC  ```
# MAGIC  Task: stream_crm_events
# MAGIC    Notebook: nb_autoloader_bronze
# MAGIC    Parameters:
# MAGIC      table_id         = adls_crm_events_autoloader_001
# MAGIC      autoloader_mode  = continuous
# MAGIC      trigger_interval = 30 seconds
# MAGIC  Cluster: dedicated streaming cluster, no auto-termination
# MAGIC  ```
# MAGIC
# MAGIC  Note: continuous jobs do not terminate on their own.
# MAGIC  Monitor via Spark UI > Streaming tab.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Verify and monitor

# COMMAND ----------

# Current status of all Autoloader tables
display(spark.sql(f"""
    SELECT
        table_id,
        src_schema,
        src_table,
        target_schema || '.' || target_table AS target,
        file_format,
        autoloader_mode,
        load_mode,
        enabled,
        last_run_status,
        last_run_at,
        last_delta_count,
        notes
    FROM {CONFIG_TABLE}
    WHERE load_mode = 'autoloader'
    ORDER BY process_group, priority
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC  ## Checkpoint management
# MAGIC
# MAGIC  Each Autoloader stream maintains a checkpoint at:
# MAGIC  `{checkpoint_base}/{process_group}/{src_schema}/{src_table}/`
# MAGIC
# MAGIC  The checkpoint tracks which files have been loaded.
# MAGIC  **Never delete the checkpoint** unless you want to reprocess
# MAGIC  all files from scratch (full reload).
# MAGIC
# MAGIC  To reset a single stream and reload from scratch:
# MAGIC  ```python
# MAGIC  dbutils.fs.rm(
# MAGIC      'abfss://checkpoints@storage.dfs.core.windows.net/CRM_streaming/crm/events/',
# MAGIC      recurse=True
# MAGIC  )
# MAGIC  ```
# MAGIC  Then re-trigger the notebook. All files will be reprocessed.
# MAGIC
# MAGIC  ## Metadata columns added by Autoloader
# MAGIC
# MAGIC  Every bronze table gets these lineage columns:
# MAGIC  | Column | Value |
# MAGIC  |---|---|
# MAGIC  | _source_file | Full ADLS path of the source file |
# MAGIC  | _source_format | parquet / json / csv |
# MAGIC  | _ingested_at | Timestamp when row was loaded |
# MAGIC  | _process_group | Process group from config |
# MAGIC  | _src_schema | Source schema folder name |
# MAGIC  | _src_table | Source table folder name |

# COMMAND ----------

# MAGIC %md
# MAGIC  ## Coexistence with COPY INTO migration framework
# MAGIC
# MAGIC  The two patterns share the same table_migration_config table.
# MAGIC  They are completely independent — different process groups,
# MAGIC  different workflows, different load_mode values.
# MAGIC
# MAGIC  | load_mode | Pattern | Workflow |
# MAGIC  |---|---|---|
# MAGIC  | overwrite | COPY INTO via foreign catalog | EDW_federated |
# MAGIC  | merge | COPY INTO via foreign catalog | EDW_federated |
# MAGIC  | copy_into | JDBC + ADLS Parquet + COPY INTO | EDW_copy_into |
# MAGIC  | autoloader | Autoloader structured streaming | CRM_streaming |
# MAGIC
# MAGIC  The dispatcher (nb_03_dispatcher) handles overwrite/merge/copy_into.
# MAGIC  Autoloader tables run via nb_autoloader_bronze separately.
# MAGIC  nb_04_workflow_summary can monitor all load_modes together.

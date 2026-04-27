# Databricks notebook source
# ============================================================
# nb_05_workflow_setup_guide
# Reference notebook — not executed as part of any workflow.
# Documents workflow setup, enable patterns, re-run patterns,
# and load_mode reference.
# ============================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workflow Setup Guide
# MAGIC ### One Databricks Workflow per process_group
# MAGIC Each workflow has three tasks in sequence:
# MAGIC
# MAGIC Task 1: init     → nb_02_workflow_init
# MAGIC Task 2: dispatch → nb_03_dispatcher     (depends on: init)
# MAGIC Task 3: summary  → nb_04_workflow_summary (depends on: dispatch)
# MAGIC ```
# MAGIC Set parameters at the JOB level (not per task):
# MAGIC ```
# MAGIC process_group  = TPCH_foreign_catalog
# MAGIC admin_catalog  = sandbox
# MAGIC config_schema  = migration_config
# MAGIC max_workers    = 8
# MAGIC ```

# COMMAND ----------

dbutils.widgets.text("admin_catalog",  "sandbox")
dbutils.widgets.text("config_schema",  "migration_config")
dbutils.widgets.text("process_group",  "TPCH_foreign_catalog")

ADMIN_CATALOG = dbutils.widgets.get("admin_catalog")
CONFIG_SCHEMA = dbutils.widgets.get("config_schema")
PROCESS_GROUP = dbutils.widgets.get("process_group")
CONFIG_TABLE  = f"{ADMIN_CATALOG}.{CONFIG_SCHEMA}.table_migration_config"
CONN_TABLE    = f"{ADMIN_CATALOG}.{CONFIG_SCHEMA}.source_connection_config"

# COMMAND ----------

# Current config state for selected process group
display(spark.sql(f"""
    SELECT
        table_id, src_database, src_schema, src_table,
        target_schema || '.' || target_table AS target,
        load_mode, incremental_col, primary_keys,
        process_group, priority, enabled,
        last_run_status, last_run_at,
        last_sf_row_count, last_delta_count,
        last_loaded_value
    FROM {CONFIG_TABLE}
    WHERE process_group = '{PROCESS_GROUP}'
    ORDER BY priority, table_id
"""))

# COMMAND ----------

# Connection registry
display(spark.sql(f"""
    SELECT * FROM {CONN_TABLE}
    ORDER BY source_type, connection_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable tables by process group
# MAGIC ```sql
# MAGIC UPDATE sandbox.migration_config.table_migration_config
# MAGIC SET enabled = true
# MAGIC WHERE process_group = 'TPCH_foreign_catalog';
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Re-run failed tables
# MAGIC ```sql
# MAGIC UPDATE sandbox.migration_config.table_migration_config
# MAGIC SET last_run_status = 'PENDING'
# MAGIC WHERE process_group   = 'TPCH_foreign_catalog'
# MAGIC   AND last_run_status = 'FAILED';
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Full reset (clears watermark — reloads all data)
# MAGIC ```sql
# MAGIC UPDATE sandbox.migration_config.table_migration_config
# MAGIC SET last_run_status    = 'PENDING',
# MAGIC     last_loaded_value   = NULL,
# MAGIC     last_sf_row_count   = NULL,
# MAGIC     last_delta_count    = NULL,
# MAGIC     notes               = NULL
# MAGIC WHERE process_group = 'TPCH_foreign_catalog';
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process groups and workflows
# MAGIC | process_group | Workflow | max_workers | Notes |
# MAGIC |---|---|---|---|
# MAGIC | TPCH_foreign_catalog | TPCH_foreign_catalog_wf | 8 | All samples.tpch tables via foreign catalog |
# MAGIC | TPCH_autoloader | TPCH_autoloader_wf | 8 | CSV files in UC Volume via Autoloader |
# MAGIC | VOLUME_copy_into | VOLUME_copy_into_wf | 1 | Parquet files in UC Volume via COPY INTO |
# MAGIC | PG_NEON_jdbc | PG_NEON_wf | 8 | Neon PostgreSQL via foreign catalog |
# MAGIC ## load_mode reference
# MAGIC | load_mode | load_strategy | Source read | Best for |
# MAGIC |---|---|---|---|
# MAGIC | overwrite | full | Foreign catalog or JDBC | Reference tables, small-medium tables |
# MAGIC | append | full | Foreign catalog or JDBC | Insert-only sources, audit logs |
# MAGIC | merge | incremental | Foreign catalog + watermark | Fact/dim tables with updates |
# MAGIC | copy_into | full | UC Volume (adls) or ADLS blob (snowflake) | Large tables, bulk loads |
# MAGIC | autoloader | incremental | UC Volume CSV/JSON/Parquet | Continuous or scheduled file ingestion |

# COMMAND ----------

# MAGIC %md
# MAGIC  ## Adding a new source system
# MAGIC
# MAGIC  1. Insert a row into source_connection_config:
# MAGIC  ```sql
# MAGIC  INSERT INTO sandbox.migration_config.source_connection_config VALUES (
# MAGIC      'my_new_source',      -- connection_name (matches src_database)
# MAGIC      'sqlserver',          -- source_type
# MAGIC      'foreign_catalog',    -- connection_method
# MAGIC      'my_new_source',      -- catalog_name (UC registered catalog)
# MAGIC      NULL,                 -- jdbc_url_template
# MAGIC      NULL,                 -- driver_class
# MAGIC      NULL,                 -- secret_scope
# MAGIC      NULL,                 -- volume_base_path
# MAGIC      false,                -- ssl_enabled
# MAGIC      NULL,                 -- extra_options
# MAGIC      true,                 -- enabled
# MAGIC      'New SQL Server source via foreign catalog'
# MAGIC  );
# MAGIC  ```
# MAGIC
# MAGIC  2. Insert rows into table_migration_config with src_database = 'my_new_source'
# MAGIC  3. No code changes required in any notebook

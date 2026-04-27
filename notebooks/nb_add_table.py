# Databricks notebook source
# ============================================================
# nb_add_table
# Utility notebook to add a single table to table_migration_config.
# Fill in the widgets below and run — no SQL editing needed.
#
# All parameters have examples in comments.
# Required fields are marked with (required).
# Leave optional fields blank for NULL.
# ============================================================

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sandbox.migration_config.table_migration_config
# MAGIC VALUES (
# MAGIC     'tpcds_promotion_001',
# MAGIC     'snowflake',
# MAGIC     'samples',
# MAGIC     'tpcds_sf1',
# MAGIC     'promotion',
# MAGIC     'sandbox',
# MAGIC     'migration_config_bronze',
# MAGIC     'tpcds_promotion',
# MAGIC     'full',
# MAGIC     'overwrite',
# MAGIC     NULL, 'none', NULL, NULL,
# MAGIC     'p_promo_sk', 'p_promo_sk',
# MAGIC     'none', NULL, NULL,
# MAGIC     'TPCH_foreign_catalog',
# MAGIC     'tpcds_sf1', NULL,
# MAGIC     NULL,
# MAGIC     false, 1,
# MAGIC     'PENDING', NULL, NULL, NULL,
# MAGIC     NULL, NULL, NULL,
# MAGIC     'TPC-DS promotion dimension. 19 columns. Static reference table. samples.tpcds_sf1.promotion.'
# MAGIC );

# COMMAND ----------



# COMMAND ----------

# ── Verify ────────────────────────────────────────────────────
display(spark.sql(f"""
    SELECT
        table_id,
        src_database || '.' || src_schema || '.' || src_table AS source,
        target_catalog || '.' || target_schema || '.' || target_table AS target,
        load_mode, incremental_col, primary_keys,
        process_group, priority, enabled, last_run_status
    FROM {CONFIG_TABLE}
    WHERE table_id = '{TABLE_ID}'
"""))

# COMMAND ----------

# MAGIC %sql DESCRIBE samples.tpcds_sf1.promotion

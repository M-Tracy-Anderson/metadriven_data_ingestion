# Databricks notebook source
# ============================================================
# nb_04_workflow_summary
# Final task in every migration workflow.
# Reads final run status for all tables in the process_group,
# prints a summary report, and raises an exception if any
# table FAILED so the Databricks Workflow shows as failed.
# ============================================================

# COMMAND ----------

dbutils.widgets.text("process_group",  "TPCH_foreign_catalog")
dbutils.widgets.text("admin_catalog",  "sandbox")
dbutils.widgets.text("config_schema",  "migration_config")

PROCESS_GROUP = dbutils.widgets.get("process_group")
ADMIN_CATALOG = dbutils.widgets.get("admin_catalog")
CONFIG_SCHEMA = dbutils.widgets.get("config_schema")
CONFIG_TABLE  = f"{ADMIN_CATALOG}.{CONFIG_SCHEMA}.table_migration_config"

# COMMAND ----------

from datetime import datetime, timezone

results_df = spark.sql(f"""
    SELECT
        table_id, src_database, src_schema, src_table,
        load_mode, last_run_status, last_run_at,
        last_sf_row_count, last_delta_count,
        last_loaded_value, priority, notes
    FROM {CONFIG_TABLE}
    WHERE process_group = '{PROCESS_GROUP}'
      AND enabled       = true
    ORDER BY priority, table_id
""")

rows = results_df.collect()

if not rows:
    total_in_group = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {CONFIG_TABLE}
        WHERE process_group = '{PROCESS_GROUP}'
    """).collect()[0]['cnt']

    if total_in_group == 0:
        raise Exception(
            f"process_group '{PROCESS_GROUP}' not found in table_migration_config. "
            f"Check the workflow job parameter — this may be a typo."
        )

    print(f"process_group '{PROCESS_GROUP}' has {total_in_group} rows but none enabled.")
    print("Nothing to summarize — this is expected if the workflow was skipped.")
    dbutils.notebook.exit(
        f"SUMMARY_SKIPPED | {PROCESS_GROUP} | {total_in_group} total | "
        f"0 enabled — nothing to summarize"
    )

# COMMAND ----------

passed_rows  = [r for r in rows if r['last_run_status'] == 'PASSED']
failed_rows  = [r for r in rows if r['last_run_status'] == 'FAILED']
running_rows = [r for r in rows if r['last_run_status'] == 'RUNNING']
pending_rows = [r for r in rows if r['last_run_status'] == 'PENDING']

total_sf_rows    = sum(r['last_sf_row_count'] or 0 for r in passed_rows)
total_delta_rows = sum(r['last_delta_count']  or 0 for r in passed_rows)

print(f"{'='*70}")
print(f"MIGRATION WORKFLOW SUMMARY")
print(f"Process Group : {PROCESS_GROUP}")
print(f"Run Time      : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"{'='*70}")
print(f"Total tables  : {len(rows)}")
print(f"PASSED        : {len(passed_rows)}")
print(f"FAILED        : {len(failed_rows)}")
print(f"RUNNING       : {len(running_rows)}")
print(f"PENDING       : {len(pending_rows)}")
print(f"{'─'*70}")
print(f"Rows from source : {total_sf_rows:,}")
print(f"Rows in Delta    : {total_delta_rows:,}")
print(f"{'='*70}")

if passed_rows:
    print(f"\nPASSED ({len(passed_rows)}):")
    for r in passed_rows:
        sf  = f"{r['last_sf_row_count']:,}"  if r['last_sf_row_count']  else 'N/A'
        dt  = f"{r['last_delta_count']:,}"   if r['last_delta_count']   else 'N/A'
        wm  = f" | WM: {r['last_loaded_value']}" if r['last_loaded_value'] else ''
        print(f"  [{r['priority']}] {r['src_schema']}.{r['src_table']} ({r['load_mode']}) | SF: {sf} | Delta: {dt}{wm}")

if failed_rows:
    print(f"\nFAILED ({len(failed_rows)}):")
    for r in failed_rows:
        print(f"  [{r['priority']}] {r['src_schema']}.{r['src_table']} ({r['load_mode']})")
        if r['notes']:
            print(f"       {r['notes'][:200]}")

if running_rows:
    print(f"\nSTUCK RUNNING — marking as FAILED:")
    running_ids = "', '".join(r['table_id'] for r in running_rows)
    spark.sql(f"""
        UPDATE {CONFIG_TABLE}
        SET last_run_status = 'FAILED',
            notes = 'Marked FAILED by workflow_summary — did not complete'
        WHERE table_id IN ('{running_ids}')
    """)
    for r in running_rows:
        print(f"  [{r['priority']}] {r['src_schema']}.{r['src_table']}")

# COMMAND ----------

display(results_df)

# COMMAND ----------

total_failures = len(failed_rows) + len(running_rows)
exit_msg = (
    f"SUMMARY | {PROCESS_GROUP} | "
    f"Passed: {len(passed_rows)} | Failed: {total_failures} | "
    f"SF Rows: {total_sf_rows:,} | Delta Rows: {total_delta_rows:,}"
)

if total_failures > 0:
    raise Exception(
        f"Workflow completed with {total_failures} failure(s). "
        f"Check table_migration_config for details. | {exit_msg}"
    )

print(f"\nAll tables passed — {exit_msg}")
dbutils.notebook.exit(exit_msg)


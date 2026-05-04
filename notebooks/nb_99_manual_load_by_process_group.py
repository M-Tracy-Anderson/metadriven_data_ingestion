# Databricks notebook source
# ============================================================
# nb_run_workflow
# POC convenience notebook — runs the three workflow notebooks
# sequentially in a single interactive session.
#
# Assumes this notebook lives in the same folder as:
#   nb_02_workflow_init
#   nb_03_dispatcher
#   nb_04_workflow_summary
#
# The folder path is resolved automatically from the current
# notebook location — no hardcoded paths needed.
#
# Not intended for production use — use a Databricks Workflow
# for scheduled or automated runs.
# ============================================================

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE sandbox.migration_config.table_migration_config
# MAGIC  SET
# MAGIC      last_run_status = 'PENDING',
# MAGIC      notes           = NULL,
# MAGIC      last_loaded_value  = NULL,
# MAGIC      last_sf_row_count  = 0,
# MAGIC      last_delta_count  = 0,
# MAGIC      last_run_at = NULL
# MAGIC WHERE process_group = 'Postgress_jdbc'

# COMMAND ----------

dbutils.widgets.text("process_group",  "TPCH_POC")
dbutils.widgets.text("admin_catalog",  "it")
dbutils.widgets.text("config_schema",  "migration_config")
dbutils.widgets.text("max_workers",    "8")
dbutils.widgets.text("adls_base_path", "abfss://migration@storageaccount.dfs.core.windows.net")

PROCESS_GROUP = dbutils.widgets.get("process_group")
ADMIN_CATALOG = dbutils.widgets.get("admin_catalog")
CONFIG_SCHEMA = dbutils.widgets.get("config_schema")
MAX_WORKERS   = dbutils.widgets.get("max_workers")
ADLS_BASE     = dbutils.widgets.get("adls_base_path")

# ── Resolve folder path from current notebook location ────────
# Strips the notebook filename from the full path leaving
# just the folder. Works regardless of where notebooks live.
NOTEBOOK_BASE = dbutils.notebook.entry_point \
    .getDbutils().notebook().getContext() \
    .notebookPath().get() \
    .rsplit("/", 1)[0]

# Shared parameters passed to every notebook
params = {
    "process_group"  : PROCESS_GROUP,
    "admin_catalog"  : ADMIN_CATALOG,
    "config_schema"  : CONFIG_SCHEMA,
    "max_workers"    : MAX_WORKERS,
    "adls_base_path" : ADLS_BASE,
}

print(f"Process group  : {PROCESS_GROUP}")
print(f"Admin catalog  : {ADMIN_CATALOG}")
print(f"Notebook folder: {NOTEBOOK_BASE}")

# COMMAND ----------

# DBTITLE 1,Cell 4
# ── Task 1: init ──────────────────────────────────────────────
print("="*60)
print("TASK 1 — nb_02_workflow_init")
print("="*60)

try:
    result_init = dbutils.notebook.run(
        f"{NOTEBOOK_BASE}/nb_02_workflow_init",
        timeout_seconds = 300,
        arguments       = params
    )
    print(f"Init result: {result_init}")
except Exception as e:
    # Child notebook failed — extract the detailed error message
    error_msg = str(e)
    print("\n" + "="*60)
    print("ERROR: nb_02_workflow_init failed")
    print("="*60)
    print(error_msg)
    print("\nCommon causes:")
    print("  - Connection smoke test failed (JDBC/foreign catalog unreachable)")
    print("  - Secret scope missing required secrets (host, port, user, password)")
    print("  - Network/firewall blocking access to source database")
    print("  - Validation errors in config table rows")
    print("\nTo debug: Run nb_02_workflow_init directly with the same parameters.")
    raise

# COMMAND ----------

# ── Task 2: dispatch ──────────────────────────────────────────
print("="*60)
print("TASK 2 — nb_03_dispatcher")
print("="*60)

result_dispatch = dbutils.notebook.run(
    f"{NOTEBOOK_BASE}/nb_03_dispatcher",
    timeout_seconds = 3600,
    arguments       = params
)
print(f"Dispatch result: {result_dispatch}")

# COMMAND ----------

# ── Task 3: summary ───────────────────────────────────────────
print("="*60)
print("TASK 3 — nb_04_workflow_summary")
print("="*60)

result_summary = dbutils.notebook.run(
    f"{NOTEBOOK_BASE}/nb_04_workflow_summary",
    timeout_seconds = 300,
    arguments       = params
)
print(f"Summary result: {result_summary}")

# COMMAND ----------

print(f"\n{'='*60}")
print(f"RUN COMPLETE — {PROCESS_GROUP}")
print(f"{'='*60}")
print(f"Init     : {result_init}")
print(f"Dispatch : {result_dispatch}")
print(f"Summary  : {result_summary}")

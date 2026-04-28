# Databricks notebook source
# ============================================================
# nb_autoloader_bronze
# Autoloader-based bronze ingestion notebook.
# Processes files landing in a Unity Catalog Volume or ADLS
# path and loads them into a Delta bronze table using
# Structured Streaming.
#
# Supports JSON, CSV, and Parquet source formats.
# Schema is inferred automatically — no DDL needed in advance.
# Target Delta table is created on first run if it does not exist.
# All files in the source folder are processed on first run.
# Subsequent runs process only new files (checkpoint tracking).
#
# Source path types:
#   UC Volume : /Volumes/{catalog}/{schema}/{volume}/{table}/
#   ADLS      : abfss://container@storage.dfs.core.windows.net/path/
#
# Path is derived from config table columns:
#   src_database = catalog name   e.g. it
#   src_schema   = schema name    e.g. tpch_bronze
#   src_table    = subfolder name e.g. supplier
#   volume_name  = volume name    e.g. landing  (widget default)
#
# Full UC Volume path built as:
#   /Volumes/{src_database}/{src_schema}/{volume_name}/{src_table}/
#
# Trigger modes (from autoloader_mode column or widget):
#   availableNow — process all pending files then stop (batch)
#   continuous   — run indefinitely, pick up files as they arrive
#
# Checkpoint stored in DBFS:
#   /Volumes/{src_database}/{src_schema}/{volume_name}/_checkpoints/{process_group}/{src_table}/
# ============================================================

# COMMAND ----------

dbutils.widgets.text("table_id",             "")
dbutils.widgets.text("admin_catalog",        "sandbox")
dbutils.widgets.text("config_schema",        "migration_config")
dbutils.widgets.text("volume_name",          "landing")
dbutils.widgets.text("autoloader_mode",      "availableNow")
dbutils.widgets.text("trigger_interval",     "60 seconds")
dbutils.widgets.text("max_files_per_trigger","1000")

TABLE_ID         = dbutils.widgets.get("table_id")
ADMIN_CATALOG    = dbutils.widgets.get("admin_catalog")
CONFIG_SCHEMA    = dbutils.widgets.get("config_schema")
VOLUME_NAME      = dbutils.widgets.get("volume_name")
AUTOLOADER_MODE  = dbutils.widgets.get("autoloader_mode")
TRIGGER_INTERVAL = dbutils.widgets.get("trigger_interval")
MAX_FILES        = int(dbutils.widgets.get("max_files_per_trigger") or "1000")
CONFIG_TABLE     = f"{ADMIN_CATALOG}.{CONFIG_SCHEMA}.table_migration_config"

print(f"Table ID         : {TABLE_ID}")
print(f"Admin catalog    : {ADMIN_CATALOG}")
print(f"Volume name      : {VOLUME_NAME}")
print(f"Autoloader mode  : {AUTOLOADER_MODE}")

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, timezone

# ── Load config row ───────────────────────────────────────────
if not TABLE_ID:
    raise Exception("table_id widget is required — set it before running")

rows = spark.sql(f"""
    SELECT * FROM {CONFIG_TABLE}
    WHERE table_id = '{TABLE_ID}'
      AND enabled  = true
""").collect()

if not rows:
    raise Exception(
        f"No enabled row found for table_id='{TABLE_ID}'. "
        f"Check table_migration_config — enabled must be true."
    )

row            = rows[0]
src_catalog    = row['src_database']   # e.g. it
src_schema     = row['src_schema']     # e.g. tpch_bronze
src_table      = row['src_table']      # e.g. supplier
target_catalog = row['target_catalog'] # e.g. it
target_schema  = row['target_schema']  # e.g. tpch_bronze
target_table   = row['target_table']   # e.g. tpch_supplier_autoloader
process_group  = row['process_group']  # e.g. TPCH_autoloader

# File format — from config table column, fall back to widget default
file_format = (row['file_format'] or 'csv').lower()

# Autoloader mode — config table overrides widget if set
if row['autoloader_mode']:
    AUTOLOADER_MODE = row['autoloader_mode']

# Schema hints — optional, from config table
schema_hints = row['schema_hints'] or ''

# ── Build paths ───────────────────────────────────────────────
# UC Volume path — derived from config table columns
# /Volumes/{src_database}/{src_schema}/{volume_name}/{src_table}/
src_path = f"/Volumes/{src_catalog}/{src_schema}/{VOLUME_NAME}/{src_table}/"

# Target Delta table
tgt = f"{target_catalog}.{target_schema}.{target_table}"

# Checkpoint in UC Volume — persists across runs, tracks loaded files
# Stored alongside the landing files in the same volume
# /Volumes/{src_catalog}/{src_schema}/{volume_name}/_checkpoints/{process_group}/{src_table}/
ckpt        = f"/Volumes/{src_catalog}/{src_schema}/{VOLUME_NAME}/_checkpoints/{process_group}/{src_table}/"
schema_ckpt = f"/Volumes/{src_catalog}/{src_schema}/{VOLUME_NAME}/_checkpoints/{process_group}/{src_table}_schema/"

print(f"\nSource path    : {src_path}")
print(f"Target table   : {tgt}")
print(f"File format    : {file_format}")
print(f"Autoloader mode: {AUTOLOADER_MODE}")
print(f"Checkpoint     : {ckpt}")
if schema_hints:
    print(f"Schema hints   : {schema_hints}")

# ── Verify source path exists ─────────────────────────────────
try:
    files = dbutils.fs.ls(src_path)
    data_files = [f for f in files if not f.name.startswith('_') and not f.name.startswith('.')]
    print(f"\nFiles found    : {len(data_files)}")
    for f in data_files[:5]:
        print(f"  {f.name}  ({f.size:,} bytes)")
    if len(data_files) > 5:
        print(f"  ... and {len(data_files)-5} more")
    if not data_files:
        print("   No data files found — check the volume path and that files have been uploaded")
except Exception as e:
    raise Exception(f"Cannot access source path {src_path}: {str(e)}")

# COMMAND ----------

# ── Ensure target schema exists ───────────────────────────────
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")

# ── Build Autoloader read stream ──────────────────────────────
# cloudFiles handles:
#   - Tracking processed files via checkpoint (no re-processing)
#   - Schema inference on first run, saved to schema checkpoint
#   - Schema evolution — new columns added automatically
#   - Exactly-once delivery guarantee across runs

reader = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",              file_format)
    .option("cloudFiles.maxFilesPerTrigger",  MAX_FILES)
    .option("cloudFiles.schemaLocation",      schema_ckpt)
    .option("cloudFiles.inferColumnTypes",    "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
)

# Apply schema hints if provided — overrides inference for named columns
if schema_hints:
    reader = reader.option("cloudFiles.schemaHints", schema_hints)

# Format-specific options
if file_format == 'csv':
    reader = reader \
        .option("header",     "true") \
        .option("escape",     '"') \
        .option("multiLine",  "false")

elif file_format == 'json':
    reader = reader \
        .option("multiLine",        "true") \
        .option("allowComments",    "true") \
        .option("allowSingleQuotes","true")

elif file_format == 'parquet':
    reader = reader \
        .option("mergeSchema", "true")

# Load stream
df = reader.load(src_path)

# Add lineage metadata columns to every row
df = df \
    .withColumn("_source_file",   F.col("_metadata.file_path")) \
    .withColumn("_source_format", F.lit(file_format)) \
    .withColumn("_ingested_at",   F.current_timestamp()) \
    .withColumn("_process_group", F.lit(process_group)) \
    .withColumn("_src_table",     F.lit(src_table))

print(f"\nStream ready — {len(df.columns)} columns (including {len([c for c in df.columns if c.startswith('_')])} metadata)")
print(f"Columns: {[c for c in df.columns if not c.startswith('_')]}")

# COMMAND ----------

# ── Configure trigger ─────────────────────────────────────────
# availableNow: process all pending files then stop.
#   Best for scheduled batch runs — workflow triggers on schedule
#   or when orchestrator signals files are ready.
#
# continuous/processingTime: run indefinitely.
#   Best for near-real-time feeds with frequent file arrival.
#   Use a dedicated always-on cluster — do not auto-terminate.

if AUTOLOADER_MODE == "availableNow":
    trigger_opts = {"availableNow": True}
    print("Trigger: availableNow — processes all pending files then stops")
else:
    trigger_opts = {"processingTime": TRIGGER_INTERVAL}
    print(f"Trigger: continuous — processing every {TRIGGER_INTERVAL}")

# ── Write stream to Delta bronze table ───────────────────────
# Delta table is created automatically if it does not exist.
# mergeSchema handles schema evolution as new columns arrive.
# No pre-created DDL or schema definition required.

write_stream = (
    df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", ckpt)
    .option("mergeSchema",        "true")
    .trigger(**trigger_opts)
    .toTable(tgt)
)

print(f"Stream writing  → {tgt}")
print(f"Awaiting completion...")

write_stream.awaitTermination()

# COMMAND ----------

# ── Post-load stats ───────────────────────────────────────────
# Get row count before and after to calculate rows loaded this run.
# lastProgress is unreliable with availableNow trigger — stream
# terminates before progress can be read. Use count difference instead.
delta_count = spark.table(tgt).count()

try:
    progress    = write_stream.lastProgress
    rows_loaded = int(progress.get('numInputRows', 0)) if progress else 0
except Exception:
    rows_loaded = 0

# If lastProgress gave nothing use delta_count as best estimate
rows_loaded = rows_loaded if rows_loaded > 0 else delta_count

# Update config table — clear notes on success
spark.sql(f"""
    UPDATE {CONFIG_TABLE}
    SET last_run_status  = 'PASSED',
        last_run_at      = CURRENT_TIMESTAMP(),
        last_delta_count = {delta_count},
        notes            = NULL
    WHERE table_id = '{TABLE_ID}'
""")

print(f"\n Autoloader complete")
print(f"   Source path  : {src_path}")
print(f"   Target table : {tgt}")
print(f"   Rows this run: {rows_loaded:,}")
print(f"   Delta total  : {delta_count:,}")
print(f"   Checkpoint   : {ckpt}")
print(f"\n   Run again to pick up any new files added to {src_path}")
print(f"   Already-loaded files will be skipped automatically.")

# COMMAND ----------

# ── Preview loaded data ───────────────────────────────────────
print(f"Sample rows from {tgt}:")
display(spark.table(tgt).limit(10))

# Databricks notebook source
# ============================================================
# nb_create_volume_parquet
# Utility notebook — writes sample TPC-H tables as Parquet
# files to a UC Volume landing zone for copy_into POC testing.
#
# In production, Parquet files would be written to the volume
# by an external orchestrator (ADF, Airflow, Talend) or by
# a Snowflake COPY INTO @external_stage command.
#
# This notebook simulates that landing step by reading from
# the samples foreign catalog and writing Parquet locally.
#
# Volume path convention:
#   /Volumes/{target_catalog}/{target_schema}/landing/{table}/
#
# Tables written:
#   lineitem  — 10K rows sample (copy_into POC)
#   supplier  — full table (autoloader Parquet alternative)
#   part      — 5K rows sample
# ============================================================

# COMMAND ----------

dbutils.widgets.text("target_catalog",  "sandbox")
dbutils.widgets.text("target_schema",   "migration_config_bronze")
dbutils.widgets.text("lineitem_limit",  "10000")   # rows to write for lineitem
dbutils.widgets.text("part_limit",      "5000")    # rows to write for part
dbutils.widgets.text("mode",            "overwrite")  # overwrite | append

TARGET_CATALOG  = dbutils.widgets.get("target_catalog")
TARGET_SCHEMA   = dbutils.widgets.get("target_schema")
LINEITEM_LIMIT  = int(dbutils.widgets.get("lineitem_limit"))
PART_LIMIT      = int(dbutils.widgets.get("part_limit"))
MODE            = dbutils.widgets.get("mode")
VOLUME_BASE     = f"/Volumes/{TARGET_CATALOG}/{TARGET_SCHEMA}/landing"

print(f"Target catalog : {TARGET_CATALOG}")
print(f"Target schema  : {TARGET_SCHEMA}")
print(f"Volume base    : {VOLUME_BASE}")
print(f"Write mode     : {MODE}")

# COMMAND ----------

# ── Verify volume is accessible ───────────────────────────────
try:
    dbutils.fs.ls(VOLUME_BASE)
    print(f"Volume accessible: {VOLUME_BASE}")
except Exception as e:
    raise Exception(
        f"Cannot access {VOLUME_BASE}. "
        f"Ensure the volume exists: CREATE VOLUME IF NOT EXISTS "
        f"{TARGET_CATALOG}.{TARGET_SCHEMA}.landing"
    )

# COMMAND ----------

def write_parquet(sql, path, label):
    """Write a DataFrame as Parquet to a volume path."""
    df    = spark.sql(sql)
    count = df.count()
    df.write \
        .format("parquet") \
        .mode(MODE) \
        .option("compression", "snappy") \
        .save(path)

    # Verify files landed
    files         = dbutils.fs.ls(path)
    parquet_files = [f for f in files if f.name.endswith('.parquet')]
    total_size_mb = sum(f.size for f in parquet_files) / 1024 / 1024

    print(f"\n  {label}")
    print(f"    Path         : {path}")
    print(f"    Rows written : {count:,}")
    print(f"    Files        : {len(parquet_files)}")
    print(f"    Total size   : {total_size_mb:.1f} MB")
    for f in parquet_files[:3]:
        print(f"    {f.name}  ({f.size/1024:.0f} KB)")
    if len(parquet_files) > 3:
        print(f"    ... and {len(parquet_files)-3} more")
    return count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write lineitem sample (copy_into POC)
# MAGIC  Writes a sample of lineitem rows as Parquet.
# MAGIC  Used by the VOLUME_copy_into process group.

# COMMAND ----------

lineitem_path = f"{VOLUME_BASE}/lineitem/"
lineitem_count = write_parquet(
    sql   = f"SELECT * FROM samples.tpch.lineitem LIMIT {LINEITEM_LIMIT}",
    path  = lineitem_path,
    label = f"lineitem ({LINEITEM_LIMIT:,} rows sample)"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write part sample (optional copy_into test)

# COMMAND ----------

part_path  = f"{VOLUME_BASE}/part/"
part_count = write_parquet(
    sql   = f"SELECT * FROM samples.tpch.part LIMIT {PART_LIMIT}",
    path  = part_path,
    label = f"part ({PART_LIMIT:,} rows sample)"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"\n{'='*60}")
print(f"PARQUET FILES WRITTEN TO VOLUME")
print(f"{'='*60}")
print(f"Volume base  : {VOLUME_BASE}")
print(f"lineitem     : {lineitem_count:,} rows → {lineitem_path}")
print(f"part         : {part_count:,} rows → {part_path}")
print(f"""
Next steps:
  1. Verify files exist:
     dbutils.fs.ls('{VOLUME_BASE}/lineitem/')

  2. Enable the copy_into config row:
     UPDATE {TARGET_CATALOG}.migration_config.table_migration_config
     SET enabled = true
     WHERE table_id = 'volume_lineitem_copy_into_001';

  3. Run VOLUME_copy_into workflow or nb_run_workflow with
     process_group = VOLUME_copy_into
""")

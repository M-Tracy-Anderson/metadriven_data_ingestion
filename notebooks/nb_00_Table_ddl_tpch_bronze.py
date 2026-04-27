# Databricks notebook source
# ============================================================
# nb_ddl_tpch_bronze
# Creates all TPC-H bronze Delta tables in Databricks with
# correct data types, column comments, and table properties.
#
# In production this notebook would be generated automatically
# by GENERATE_TABLE_DDL running against Snowflake and would
# reflect the actual source schema including:
#   - Snowflake to Databricks type conversions
#   - Column comments from source
#   - TBLPROPERTIES with source metadata
#   - Clustering recommendations from UPDATE_APPROX_SIZE_GB
#
# Tables created:
#   tpch_region, tpch_nation, customer, tpch_supplier,
#   tpch_part, tpch_partsupp, orders, tpch_lineitem
#
# Run this notebook BEFORE running the migration workflow
# to ensure Delta tables have correct schemas and comments.
# The dispatcher will populate them without overwriting schema.
#
# Snowflake → Databricks type mapping used here:
#   NUMBER(p, 0) where p <= 9  → INT
#   NUMBER(p, 0) where p > 9   → BIGINT
#   NUMBER(p, s) where s > 0   → DECIMAL(p, s)
#   VARCHAR / TEXT              → STRING
#   CHAR                        → STRING
#   DATE                        → DATE
#   TIMESTAMP_NTZ               → TIMESTAMP_NTZ
#   FLOAT / DOUBLE              → DOUBLE
# ============================================================

# COMMAND ----------

dbutils.widgets.text("target_catalog", "sandbox")
dbutils.widgets.text("target_schema",  "migration_config_bronze")
dbutils.widgets.text("mode",           "create")  # create | recreate

TARGET_CATALOG = dbutils.widgets.get("target_catalog")
TARGET_SCHEMA  = dbutils.widgets.get("target_schema")
MODE           = dbutils.widgets.get("mode").lower()

print(f"Target catalog : {TARGET_CATALOG}")
print(f"Target schema  : {TARGET_SCHEMA}")
print(f"Mode           : {MODE}")
print()
if MODE == "recreate":
    print("WARNING: mode=recreate will DROP and recreate all tables.")
    print("All existing data will be lost.")

# COMMAND ----------

# ── Ensure schema exists ──────────────────────────────────────
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")
print(f"Schema {TARGET_CATALOG}.{TARGET_SCHEMA} ready")

# COMMAND ----------

def create_table(name, ddl, comment, tblproperties=None):
    """Create a Delta table — drop first if mode=recreate."""
    tgt = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{name}"

    if MODE == "recreate":
        spark.sql(f"DROP TABLE IF EXISTS {tgt}")
        print(f"  Dropped : {tgt}")

    props = ""
    if tblproperties:
        props_str = ", ".join(f"'{k}' = '{v}'" for k,v in tblproperties.items())
        props = f"\nTBLPROPERTIES ({props_str})"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tgt} (
            {ddl}
        )
        USING DELTA
        COMMENT '{comment}'{props}
    """)
    print(f"  Created : {tgt}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Region table
# MAGIC Source: SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION
# MAGIC 5 rows — tiny reference table

# COMMAND ----------

create_table(
    name    = "tpch_region",
    comment = "TPC-H region reference table. Source: samples.tpch.region. 5 rows.",
    ddl     = """
        r_regionkey  INT            NOT NULL  COMMENT 'Region unique identifier. Source: R_REGIONKEY NUMBER(38,0)',
        r_name       STRING                   COMMENT 'Region name. Source: R_NAME CHAR(25)',
        r_comment    STRING                   COMMENT 'Region comment. Source: R_COMMENT VARCHAR(152)'
    """,
    tblproperties = {
        "source.database"  : "SNOWFLAKE_SAMPLE_DATA",
        "source.schema"    : "TPCH_SF1",
        "source.table"     : "REGION",
        "migration.layer"  : "bronze",
        "migration.mode"   : "overwrite",
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Nation table
# MAGIC Source: SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION
# MAGIC 25 rows — reference table

# COMMAND ----------

create_table(
    name    = "tpch_nation",
    comment = "TPC-H nation reference table. Source: samples.tpch.nation. 25 rows.",
    ddl     = """
        n_nationkey  INT            NOT NULL  COMMENT 'Nation unique identifier. Source: N_NATIONKEY NUMBER(38,0)',
        n_name       STRING                   COMMENT 'Nation name. Source: N_NAME CHAR(25)',
        n_regionkey  INT                      COMMENT 'Foreign key to region. Source: N_REGIONKEY NUMBER(38,0)',
        n_comment    STRING                   COMMENT 'Nation comment. Source: N_COMMENT VARCHAR(152)'
    """,
    tblproperties = {
        "source.database"  : "SNOWFLAKE_SAMPLE_DATA",
        "source.schema"    : "TPCH_SF1",
        "source.table"     : "NATION",
        "migration.layer"  : "bronze",
        "migration.mode"   : "overwrite",
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer table
# MAGIC Source: SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
# MAGIC ~750K rows — dimension table, full overwrite

# COMMAND ----------

create_table(
    name    = "customer",
    comment = "TPC-H customer dimension. Source: samples.tpch.customer. ~750K rows.",
    ddl     = """
        c_custkey    BIGINT         NOT NULL  COMMENT 'Customer unique identifier. Source: C_CUSTKEY NUMBER(38,0)',
        c_name       STRING                   COMMENT 'Customer name. Source: C_NAME VARCHAR(25)',
        c_address    STRING                   COMMENT 'Customer address. Source: C_ADDRESS VARCHAR(40)',
        c_nationkey  INT                      COMMENT 'Foreign key to nation. Source: C_NATIONKEY NUMBER(38,0)',
        c_phone      STRING                   COMMENT 'Customer phone number. Source: C_PHONE CHAR(15)',
        c_acctbal    DECIMAL(12, 2)           COMMENT 'Customer account balance. Source: C_ACCTBAL NUMBER(12,2)',
        c_mktsegment STRING                   COMMENT 'Market segment. Source: C_MKTSEGMENT CHAR(10)',
        c_comment    STRING                   COMMENT 'Customer comment. Source: C_COMMENT VARCHAR(117)'
    """,
    tblproperties = {
        "source.database"  : "SNOWFLAKE_SAMPLE_DATA",
        "source.schema"    : "TPCH_SF1",
        "source.table"     : "CUSTOMER",
        "migration.layer"  : "bronze",
        "migration.mode"   : "overwrite",
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Supplier table
# MAGIC Source: SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER
# MAGIC ~50K rows — reference/dimension table

# COMMAND ----------

create_table(
    name    = "tpch_supplier",
    comment = "TPC-H supplier dimension. Source: samples.tpch.supplier. ~50K rows.",
    ddl     = """
        s_suppkey    BIGINT         NOT NULL  COMMENT 'Supplier unique identifier. Source: S_SUPPKEY NUMBER(38,0)',
        s_name       STRING                   COMMENT 'Supplier name. Source: S_NAME CHAR(25)',
        s_address    STRING                   COMMENT 'Supplier address. Source: S_ADDRESS VARCHAR(40)',
        s_nationkey  INT                      COMMENT 'Foreign key to nation. Source: S_NATIONKEY NUMBER(38,0)',
        s_phone      STRING                   COMMENT 'Supplier phone number. Source: S_PHONE CHAR(15)',
        s_acctbal    DECIMAL(12, 2)           COMMENT 'Supplier account balance. Source: S_ACCTBAL NUMBER(12,2)',
        s_comment    STRING                   COMMENT 'Supplier comment. Source: S_COMMENT VARCHAR(101)'
    """,
    tblproperties = {
        "source.database"  : "SNOWFLAKE_SAMPLE_DATA",
        "source.schema"    : "TPCH_SF1",
        "source.table"     : "SUPPLIER",
        "migration.layer"  : "bronze",
        "migration.mode"   : "overwrite",
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part table
# MAGIC Source: SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.PART
# MAGIC ~1M rows — product reference table

# COMMAND ----------

create_table(
    name    = "tpch_part",
    comment = "TPC-H part reference table. Source: samples.tpch.part. ~1M rows.",
    ddl     = """
        p_partkey    BIGINT         NOT NULL  COMMENT 'Part unique identifier. Source: P_PARTKEY NUMBER(38,0)',
        p_name       STRING                   COMMENT 'Part name. Source: P_NAME VARCHAR(55)',
        p_mfgr       STRING                   COMMENT 'Manufacturer. Source: P_MFGR CHAR(25)',
        p_brand      STRING                   COMMENT 'Brand. Source: P_BRAND CHAR(10)',
        p_type       STRING                   COMMENT 'Part type. Source: P_TYPE VARCHAR(25)',
        p_size       INT                      COMMENT 'Part size. Source: P_SIZE NUMBER(38,0)',
        p_container  STRING                   COMMENT 'Container type. Source: P_CONTAINER CHAR(10)',
        p_retailprice DECIMAL(12, 2)          COMMENT 'Retail price. Source: P_RETAILPRICE NUMBER(12,2)',
        p_comment    STRING                   COMMENT 'Part comment. Source: P_COMMENT VARCHAR(23)'
    """,
    tblproperties = {
        "source.database"  : "SNOWFLAKE_SAMPLE_DATA",
        "source.schema"    : "TPCH_SF1",
        "source.table"     : "PART",
        "migration.layer"  : "bronze",
        "migration.mode"   : "overwrite",
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## PartSupp table
# MAGIC Source: SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.PARTSUPP
# MAGIC ~4M rows — bridge table, composite PK
# MAGIC Liquid clustering on ps_partkey recommended (>1GB)

# COMMAND ----------

create_table(
    name    = "tpch_partsupp",
    comment = "TPC-H part-supplier bridge table. Source: samples.tpch.partsupp. ~4M rows. Composite PK.",
    ddl     = """
        ps_partkey    BIGINT        NOT NULL  COMMENT 'Foreign key to part. Source: PS_PARTKEY NUMBER(38,0)',
        ps_suppkey    BIGINT        NOT NULL  COMMENT 'Foreign key to supplier. Source: PS_SUPPKEY NUMBER(38,0)',
        ps_availqty   INT                     COMMENT 'Available quantity. Source: PS_AVAILQTY NUMBER(38,0)',
        ps_supplycost DECIMAL(12,2)           COMMENT 'Supply cost. Source: PS_SUPPLYCOST NUMBER(12,2)',
        ps_comment    STRING                  COMMENT 'Part-supplier comment. Source: PS_COMMENT VARCHAR(199)'
    """,
    tblproperties = {
        "source.database"       : "SNOWFLAKE_SAMPLE_DATA",
        "source.schema"         : "TPCH_SF1",
        "source.table"          : "PARTSUPP",
        "migration.layer"       : "bronze",
        "migration.mode"        : "overwrite",
        "migration.cluster_by"  : "ps_partkey",
        "migration.cluster_note": "Recommended — table >1GB",
    }
)

# Apply liquid clustering
spark.sql(f"""
    ALTER TABLE {TARGET_CATALOG}.{TARGET_SCHEMA}.tpch_partsupp
    CLUSTER BY (ps_partkey)
""")
print(f"  Clustered: tpch_partsupp by ps_partkey")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders table
# MAGIC Source: SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
# MAGIC ~7.5M rows — fact table, incremental merge on o_orderdate
# MAGIC Liquid clustering on o_orderdate recommended (>1GB)

# COMMAND ----------

create_table(
    name    = "orders",
    comment = "TPC-H orders fact table. Source: samples.tpch.orders. ~7.5M rows. Incremental on o_orderdate.",
    ddl     = """
        o_orderkey      BIGINT       NOT NULL  COMMENT 'Order unique identifier. Source: O_ORDERKEY NUMBER(38,0)',
        o_custkey       BIGINT                 COMMENT 'Foreign key to customer. Source: O_CUSTKEY NUMBER(38,0)',
        o_orderstatus   STRING                 COMMENT 'Order status flag. Source: O_ORDERSTATUS CHAR(1)',
        o_totalprice    DECIMAL(12,2)          COMMENT 'Total order price. Source: O_TOTALPRICE NUMBER(12,2)',
        o_orderdate     DATE                   COMMENT 'Order date — watermark for incremental load. Source: O_ORDERDATE DATE',
        o_orderpriority STRING                 COMMENT 'Order priority. Source: O_ORDERPRIORITY CHAR(15)',
        o_clerk         STRING                 COMMENT 'Clerk responsible. Source: O_CLERK CHAR(15)',
        o_shippriority  INT                    COMMENT 'Ship priority. Source: O_SHIPPRIORITY NUMBER(38,0)',
        o_comment       STRING                 COMMENT 'Order comment. Source: O_COMMENT VARCHAR(79)'
    """,
    tblproperties = {
        "source.database"           : "SNOWFLAKE_SAMPLE_DATA",
        "source.schema"             : "TPCH_SF1",
        "source.table"              : "ORDERS",
        "migration.layer"           : "bronze",
        "migration.mode"            : "incremental_merge",
        "migration.watermark_col"   : "o_orderdate",
        "migration.watermark_type"  : "date",
        "migration.cluster_by"      : "o_orderdate",
        "migration.cluster_note"    : "Recommended — table >1GB",
    }
)

spark.sql(f"""
    ALTER TABLE {TARGET_CATALOG}.{TARGET_SCHEMA}.orders
    CLUSTER BY (o_orderdate)
""")
print(f"  Clustered: orders by o_orderdate")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lineitem table
# MAGIC Source: SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM
# MAGIC ~30M rows — largest fact table, composite PK
# MAGIC Incremental merge on l_shipdate
# MAGIC Liquid clustering on l_shipdate required (>10GB)

# COMMAND ----------

create_table(
    name    = "tpch_lineitem",
    comment = "TPC-H line item fact table. Source: samples.tpch.lineitem. ~30M rows. Composite PK. Incremental on l_shipdate.",
    ddl     = """
        l_orderkey      BIGINT       NOT NULL  COMMENT 'Foreign key to orders. Source: L_ORDERKEY NUMBER(38,0)',
        l_partkey       BIGINT                 COMMENT 'Foreign key to part. Source: L_PARTKEY NUMBER(38,0)',
        l_suppkey       BIGINT                 COMMENT 'Foreign key to supplier. Source: L_SUPPKEY NUMBER(38,0)',
        l_linenumber    INT          NOT NULL  COMMENT 'Line item number within order. Source: L_LINENUMBER NUMBER(38,0)',
        l_quantity      DECIMAL(12,2)          COMMENT 'Quantity ordered. Source: L_QUANTITY NUMBER(12,2)',
        l_extendedprice DECIMAL(12,2)          COMMENT 'Extended price. Source: L_EXTENDEDPRICE NUMBER(12,2)',
        l_discount      DECIMAL(12,2)          COMMENT 'Discount applied. Source: L_DISCOUNT NUMBER(12,2)',
        l_tax           DECIMAL(12,2)          COMMENT 'Tax applied. Source: L_TAX NUMBER(12,2)',
        l_returnflag    STRING                 COMMENT 'Return flag. Source: L_RETURNFLAG CHAR(1)',
        l_linestatus    STRING                 COMMENT 'Line status. Source: L_LINESTATUS CHAR(1)',
        l_shipdate      DATE                   COMMENT 'Ship date — watermark for incremental load. Source: L_SHIPDATE DATE',
        l_commitdate    DATE                   COMMENT 'Commit date. Source: L_COMMITDATE DATE',
        l_receiptdate   DATE                   COMMENT 'Receipt date. Source: L_RECEIPTDATE DATE',
        l_shipinstruct  STRING                 COMMENT 'Shipping instructions. Source: L_SHIPINSTRUCT CHAR(25)',
        l_shipmode      STRING                 COMMENT 'Ship mode. Source: L_SHIPMODE CHAR(10)',
        l_comment       STRING                 COMMENT 'Line item comment. Source: L_COMMENT VARCHAR(44)'
    """,
    tblproperties = {
        "source.database"           : "SNOWFLAKE_SAMPLE_DATA",
        "source.schema"             : "TPCH_SF1",
        "source.table"              : "LINEITEM",
        "migration.layer"           : "bronze",
        "migration.mode"            : "incremental_merge",
        "migration.watermark_col"   : "l_shipdate",
        "migration.watermark_type"  : "date",
        "migration.cluster_by"      : "l_shipdate",
        "migration.cluster_note"    : "Required — table >10GB",
    }
)

spark.sql(f"""
    ALTER TABLE {TARGET_CATALOG}.{TARGET_SCHEMA}.tpch_lineitem
    CLUSTER BY (l_shipdate)
""")
print(f"  Clustered: tpch_lineitem by l_shipdate")

# COMMAND ----------

# ── Verify all tables ─────────────────────────────────────────
print("\nVerifying all tables...")
tables = [
    "tpch_region", "tpch_nation", "customer", "tpch_supplier",
    "tpch_part", "tpch_partsupp", "orders", "tpch_lineitem"
]

all_ok = True
for t in tables:
    tgt = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{t}"
    try:
        exists = spark.catalog.tableExists(tgt)
        cols   = len(spark.table(tgt).columns)
        print(f"  {t:<20} exists={exists}  columns={cols}")
    except Exception as e:
        print(f"  {t:<20} ERROR — {str(e)[:80]}")
        all_ok = False

if all_ok:
    print(f"\nAll {len(tables)} tables created in {TARGET_CATALOG}.{TARGET_SCHEMA}")
    print("Run the migration workflow to populate them.")
else:
    print("\nSome tables failed — review errors above")

# COMMAND ----------

# ── Show schemas ──────────────────────────────────────────────
for t in tables:
    tgt = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{t}"
    print(f"\n{t}:")
    try:
        spark.sql(f"DESCRIBE TABLE {tgt}").show(30, truncate=False)
    except Exception as e:
        print(f"  ERROR: {str(e)[:80]}")

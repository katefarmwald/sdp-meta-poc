# Databricks notebook source
# MAGIC %md
# MAGIC # SDP-META POC — 01 Setup & Onboard (Databricks Free Edition)
# MAGIC
# MAGIC This notebook does everything except run the pipeline:
# MAGIC
# MAGIC 1. Creates the schemas + a Volume in your catalog
# MAGIC 2. Copies two **TPC-H sample tables** (`samples.tpch.customer`, `samples.tpch.orders`) into a *landing* schema
# MAGIC    (so you own the source and can append rows later to see incremental processing)
# MAGIC 3. Writes the SDP-META **metadata files** (onboarding JSON, data-quality rules, silver transformations) to the Volume
# MAGIC 4. Runs **onboarding** → produces the `bronze_dataflowspec` / `silver_dataflowspec` Delta tables
# MAGIC 5. (Optional) Creates the Lakeflow Declarative Pipeline that points at `02_sdp_meta_pipeline`
# MAGIC
# MAGIC Run on **Serverless** compute. Nothing needs to be installed on your laptop.

# COMMAND ----------

# MAGIC %pip install databricks-labs-sdp-meta
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("prefix", "sdp_meta_poc", "Schema prefix")

catalog = dbutils.widgets.get("catalog")
prefix = dbutils.widgets.get("prefix")

meta_schema = prefix                   # holds the DataflowSpec tables + the config Volume
landing_schema = f"{prefix}_landing"   # our "raw" source tables (copied from samples)
bronze_schema = f"{prefix}_bronze"
silver_schema = f"{prefix}_silver"
volume_path = f"/Volumes/{catalog}/{meta_schema}/conf"

print(catalog, meta_schema, landing_schema, bronze_schema, silver_schema, volume_path, sep="\n")

# COMMAND ----------

# MAGIC %md ## 1. Schemas, Volume and landing data

# COMMAND ----------

for s in [meta_schema, landing_schema, bronze_schema, silver_schema]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{s}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{meta_schema}.conf")

# Full customer table (~750k rows); only the 1998 orders to keep the first run small.
spark.sql(f"""
  CREATE OR REPLACE TABLE {catalog}.{landing_schema}.customer AS
  SELECT * FROM samples.tpch.customer
""")
spark.sql(f"""
  CREATE OR REPLACE TABLE {catalog}.{landing_schema}.orders AS
  SELECT * FROM samples.tpch.orders WHERE o_orderdate >= '1998-01-01'
""")
display(spark.sql(f"SHOW TABLES IN {catalog}.{landing_schema}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Metadata files
# MAGIC
# MAGIC This is the part SDP-META is about: **you describe the pipeline, you don't code it.**
# MAGIC
# MAGIC * `onboarding.json` — one entry per data flow (source → bronze → silver)
# MAGIC * `dqe/*.json` — data-quality expectations (`expect`, `expect_or_drop`, `expect_or_fail`, `expect_or_quarantine`)
# MAGIC * `silver_transformations.json` — SQL `select_exp` / `where_clause` per silver table
# MAGIC
# MAGIC Keys ending in `_dev` are resolved by the `env` parameter used during onboarding (`env = "dev"`).

# COMMAND ----------

import json
import os

onboarding = [
    {
        "data_flow_id": "100",
        "data_flow_group": "poc",
        "source_system": "tpch",
        "source_format": "delta",
        "source_details": {
            "source_database": f"{catalog}.{landing_schema}",
            "source_table": "customer",
        },
        "bronze_catalog_dev": catalog,
        "bronze_database_dev": bronze_schema,
        "bronze_table": "customer",
        "bronze_table_comment": "Bronze customers ingested by SDP-META",
        "bronze_data_quality_expectations_json_dev": f"{volume_path}/dqe/customer_bronze.json",
        "bronze_catalog_quarantine_dev": catalog,
        "bronze_database_quarantine_dev": bronze_schema,
        "bronze_quarantine_table": "customer_quarantine",
        "silver_catalog_dev": catalog,
        "silver_database_dev": silver_schema,
        "silver_table": "customer",
        "silver_table_comment": "Cleaned customers",
        "silver_transformation_json_dev": f"{volume_path}/silver_transformations.json",
    },
    {
        "data_flow_id": "101",
        "data_flow_group": "poc",
        "source_system": "tpch",
        "source_format": "delta",
        "source_details": {
            "source_database": f"{catalog}.{landing_schema}",
            "source_table": "orders",
        },
        "bronze_catalog_dev": catalog,
        "bronze_database_dev": bronze_schema,
        "bronze_table": "orders",
        "bronze_data_quality_expectations_json_dev": f"{volume_path}/dqe/orders_bronze.json",
        "bronze_catalog_quarantine_dev": catalog,
        "bronze_database_quarantine_dev": bronze_schema,
        "bronze_quarantine_table": "orders_quarantine",
        "silver_catalog_dev": catalog,
        "silver_database_dev": silver_schema,
        "silver_table": "orders",
        "silver_table_comment": "Orders, upserted on o_orderkey (SCD type 1)",
        "silver_transformation_json_dev": f"{volume_path}/silver_transformations.json",
        # Turns the silver table into an AUTO CDC (merge/upsert) target instead of append-only
        "silver_cdc_apply_changes": {
            "keys": ["o_orderkey"],
            "sequence_by": "o_orderdate",
            "scd_type": "1",
        },
    },
]

# Main table keeps rows passing expect_or_drop; the quarantine table keeps rows
# matching expect_or_quarantine, so that rule describes the *bad* rows.
customer_bronze_dqe = {
    "expect_or_drop": {
        "valid_custkey": "c_custkey IS NOT NULL",
        "non_negative_balance": "c_acctbal >= 0",
    },
    "expect_or_quarantine": {
        "bad_customer": "c_custkey IS NULL OR c_acctbal < 0",
    },
}

orders_bronze_dqe = {
    "expect": {"has_priority": "o_orderpriority IS NOT NULL"},
    "expect_or_drop": {"valid_orderkey": "o_orderkey IS NOT NULL"},
    "expect_or_quarantine": {"bad_order": "o_orderkey IS NULL"},
}

silver_transformations = [
    {
        "target_table": "customer",
        "select_exp": [
            "c_custkey AS customer_id",
            "c_name AS name",
            "c_nationkey AS nation_id",
            "c_acctbal AS account_balance",
            "lower(c_mktsegment) AS market_segment",
        ],
        "where_clause": ["c_mktsegment IS NOT NULL"],
    },
    {
        "target_table": "orders",
        "select_exp": [
            "o_orderkey",
            "o_custkey AS customer_id",
            "o_orderstatus AS status",
            "o_totalprice AS total_price",
            "o_orderdate",
            "year(o_orderdate) AS order_year",
        ],
        "where_clause": [],
    },
]

def write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)
    print("wrote", path)

write_json(f"{volume_path}/onboarding.json", onboarding)
write_json(f"{volume_path}/dqe/customer_bronze.json", customer_bronze_dqe)
write_json(f"{volume_path}/dqe/orders_bronze.json", orders_bronze_dqe)
write_json(f"{volume_path}/silver_transformations.json", silver_transformations)

# COMMAND ----------

# MAGIC %md ## 3. Onboard → DataflowSpec tables

# COMMAND ----------

from databricks.labs.sdp_meta.onboard_dataflowspec import OnboardDataflowspec

onboarding_params_map = {
    "database": f"{catalog}.{meta_schema}",
    "onboarding_file_path": f"{volume_path}/onboarding.json",
    "bronze_dataflowspec_table": "bronze_dataflowspec",
    "silver_dataflowspec_table": "silver_dataflowspec",
    "overwrite": "True",
    "env": "dev",
    "version": "v1",
    "import_author": "poc",
}
OnboardDataflowspec(spark, onboarding_params_map, uc_enabled=True).onboard_dataflow_specs()

# COMMAND ----------

# The generic pipeline reads these rows at runtime to decide what to build
display(spark.table(f"{catalog}.{meta_schema}.bronze_dataflowspec"))
display(spark.table(f"{catalog}.{meta_schema}.silver_dataflowspec"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. (Optional) Create the pipeline from here
# MAGIC
# MAGIC Creates a serverless pipeline that runs `02_sdp_meta_pipeline` (expected next to this notebook) with
# MAGIC `layer = bronze_silver`. If you prefer the UI, see the README for the same settings.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import NotebookLibrary, PipelineLibrary

this_nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
pipeline_nb = os.path.join(os.path.dirname(this_nb), "02_sdp_meta_pipeline")

w = WorkspaceClient()
created = w.pipelines.create(
    name=f"{prefix}_bronze_silver",
    catalog=catalog,
    schema=meta_schema,
    serverless=True,
    development=True,
    libraries=[PipelineLibrary(notebook=NotebookLibrary(path=pipeline_nb))],
    configuration={
        "layer": "bronze_silver",
        "bronze.dataflowspecTable": f"{catalog}.{meta_schema}.bronze_dataflowspec",
        "bronze.group": "poc",
        "silver.dataflowspecTable": f"{catalog}.{meta_schema}.silver_dataflowspec",
        "silver.group": "poc",
    },
)
print(f"Pipeline created: {w.config.host}/pipelines/{created.pipeline_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. After the first pipeline run: see it process *only new data*
# MAGIC
# MAGIC Append 1997 orders to the landing table, then click **Start** on the pipeline again.
# MAGIC Bronze picks up just the new rows (streaming), and silver upserts them on `o_orderkey`.

# COMMAND ----------

# spark.sql(f"""
#   INSERT INTO {catalog}.{landing_schema}.orders
#   SELECT * FROM samples.tpch.orders WHERE o_orderdate >= '1997-12-01' AND o_orderdate < '1998-01-01'
# """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC Delete the pipeline in the UI, then uncomment and run:

# COMMAND ----------

# for s in [silver_schema, bronze_schema, landing_schema, meta_schema]:
#     spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{s} CASCADE")

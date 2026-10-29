# Databricks notebook source
# MAGIC %md
# MAGIC # SDP-META POC — 02 Generic pipeline
# MAGIC
# MAGIC Don't run this notebook interactively. It is the source code of the Lakeflow Declarative Pipeline.
# MAGIC Every SDP-META pipeline uses these same two cells. What gets built is decided entirely by the
# MAGIC DataflowSpec tables named in the pipeline's configuration (`layer`, `bronze.*`, `silver.*`).

# COMMAND ----------

# MAGIC %pip install databricks-labs-sdp-meta

# COMMAND ----------

layer = spark.conf.get("layer", None)

from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)

# Databricks notebook source
# MAGIC %pip install databricks-labs-sdp-meta

# COMMAND ----------

# SDP-META POC — 02 Generic pipeline
#
# Don't run this notebook interactively. It is the source code of the Lakeflow Declarative Pipeline.
# Every SDP-META pipeline uses this same code. What gets built is decided entirely by the
# DataflowSpec tables named in the pipeline's configuration (layer, bronze.*, silver.*).
# Note: %pip cells must be at the top of a pipeline notebook, so no markdown cell above them.

layer = spark.conf.get("layer", None)

from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)

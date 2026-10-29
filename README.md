# SDP-META POC on Databricks Free Edition

A minimal end-to-end SDP-META run using the built-in `samples.tpch` dataset.
You don't need to install anything locally. The SDP-META library is `pip install`ed inside Databricks by the notebooks.

## What it builds

```
samples.tpch.customer / orders
        │  (copied once by notebook 01)
        ▼
<catalog>.sdp_meta_poc_landing.{customer, orders}        ← "raw" source (Delta)
        │  bronze: streaming read + DQ expectations + quarantine
        ▼
<catalog>.sdp_meta_poc_bronze.{customer, customer_quarantine, orders, orders_quarantine}
        │  silver: select_exp / where_clause; orders uses AUTO CDC (SCD1 on o_orderkey)
        ▼
<catalog>.sdp_meta_poc_silver.{customer, orders}
```

All of this comes from metadata (onboarding JSON + DQE JSON + silver transformations JSON). The pipeline code is the same 2 lines for every SDP-META pipeline.

## Steps

1. **Get the two notebooks into your workspace, in the same folder.** If this folder is in a Databricks Git folder, just pull. Otherwise use **Workspace → Import** for `01_setup_and_onboard.py` and `02_sdp_meta_pipeline.py`.
2. **Run `01_setup_and_onboard`** on Serverless compute. The default catalog `workspace` is the Free Edition default; change the widget if yours differs. It:
   - creates the schemas and a `conf` Volume
   - copies the sample tables into the landing schema
   - writes the metadata files to `/Volumes/<catalog>/sdp_meta_poc/conf/`
   - onboards them into `bronze_dataflowspec` and `silver_dataflowspec`
   - creates a serverless pipeline named `sdp_meta_poc_bronze_silver`
3. **Open the pipeline** (link printed by the last cell, or **Jobs & Pipelines**) and click **Start**. The graph shows the bronze, quarantine and silver tables.
4. **Optional: incremental run.** Uncomment the `INSERT` cell in notebook 01, run it, then start the pipeline again. Only the new rows are processed.

### Creating the pipeline in the UI instead (if step 2's last cell fails)

**Jobs & Pipelines → Create → ETL pipeline**, serverless, default catalog `workspace`, schema `sdp_meta_poc`, source = the `02_sdp_meta_pipeline` notebook, and these **Configuration** key/values:

| Key | Value |
|---|---|
| `layer` | `bronze_silver` |
| `bronze.dataflowspecTable` | `workspace.sdp_meta_poc.bronze_dataflowspec` |
| `bronze.group` | `poc` |
| `silver.dataflowspecTable` | `workspace.sdp_meta_poc.silver_dataflowspec` |
| `silver.group` | `poc` |

## Things to try next

- Add a third flow (e.g. `samples.tpch.lineitem`) by adding an entry to `onboarding` in notebook 01, re-running onboarding, and restarting the pipeline. No pipeline code changes are needed.
- Change a DQ rule, re-onboard, and watch the expectation metrics in the pipeline UI.
- Compare with [`demo/conf/json/onboarding.template`](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/onboarding.template) in the sdp-meta repo for the full set of options (Autoloader, append flows, sinks, clustering…).

## Cleanup

Delete the pipeline, then run the commented cleanup cell at the bottom of notebook 01.

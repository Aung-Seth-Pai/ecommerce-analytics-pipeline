# ecommerce-analytics-pipeline

An end-to-end analytics pipeline: a simulated e-commerce source that changes every day, a Python loader into BigQuery, a tested Kimball star schema built with dbt, and a Data Studio (formerly Looker Studio) dashboard.

![Python](https://img.shields.io/badge/Python-3.13-3776AB?logo=python&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-snapshot-FFF000?logo=duckdb&logoColor=black)
![BigQuery](https://img.shields.io/badge/BigQuery-warehouse-669DF6?logo=googlebigquery&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-v2-FF694B?logo=dbt&logoColor=white)
![License: MIT](https://img.shields.io/badge/License-MIT-green)

**Dashboard:** [Ecommerce Performance Overview](https://datastudio.google.com/reporting/d6868a15-0562-41e3-a816-f83315099fa3)

> Phase 1 Complete: end-to-end pipeline, tested star schema, dashboard. Depth phases planned below - See the [roadmap](#roadmap).

## Overview

This repository is a complete, working analytics pipeline on Google Cloud's free tier, from source data to dashboard. It replays Google's public `thelook_ecommerce` dataset as daily batches, loads them into BigQuery, models them into a tested star schema with dbt, and serves them to Data Studio.

## What this answers

The marts and the [dashboard](https://datastudio.google.com/reporting/d6868a15-0562-41e3-a816-f83315099fa3) answer these questions, in the order the dashboard reads:

| Question | Built from |
|---|---|
| How is the business doing against the previous period? Net sales, gross profit and margin, orders and AOV, plus cancellations, returns and delivery time | `fct_orders` |
| Are net sales growing week over week? | `fct_orders` |
| Why did net sales change: more orders, bigger baskets, higher item prices, or more repeat customers? | `fct_orders` |
| Which departments and categories drive net sales and gross margin? | `fct_order_items`, `dim_products` |
| How fast do orders ship and arrive, and how often are they returned? Measured only on orders old enough for the outcome to be known | `fct_orders` |
| Which traffic sources bring sessions that reach a product page, the cart and a purchase? | `fct_sessions` |
| How much money came in and went back out each month? Counted on the date money moved, so a closed month never changes | `fct_sales_movements` |

Metric definitions (GMV, net sales, gross profit, the two date bases for revenue, and the maturity windows) are in the [dbt README](dbt/ecommerce_analytics/README.md#key-design-choices).

## Architecture

<table align="center">
  <tr>
    <td align="center" valign="top" width="120">
      <img src="docs/assets/icons/duckdb.svg" alt="DuckDB" height="44"><br>
      <b>Snapshot</b><br>
      <sub>frozen copy of the source</sub>
    </td>
    <td align="center">➜</td>
    <td align="center" valign="top" width="120">
      <img src="docs/assets/icons/python.svg" alt="Python" height="44"><br>
      <b>Simulator and runner</b><br>
      <sub>daily change batches</sub>
    </td>
    <td align="center">➜</td>
    <td align="center" valign="top" width="120">
      <img src="docs/assets/icons/apache-parquet.svg" alt="Apache Parquet" height="44"><br>
      <b>Landing zone</b><br>
      <sub>one folder per batch</sub>
    </td>
    <td align="center">➜</td>
    <td align="center" valign="top" width="120">
      <img src="docs/assets/icons/python.svg" alt="Python" height="44"><br>
      <b>Loader</b><br>
      <sub>append-only, at least once</sub>
    </td>
    <td align="center">➜</td>
    <td align="center" valign="top" width="120">
      <img src="docs/assets/icons/bigquery.svg" alt="BigQuery" height="44"><br>
      <b>Raw</b><br>
      <sub><code>ecommerce_raw</code></sub>
    </td>
    <td align="center">➜</td>
    <td align="center" valign="top" width="120">
      <img src="docs/assets/icons/dbt.svg" alt="dbt" height="44"><br>
      <b>dbt</b><br>
      <sub>staging, intermediate, marts</sub>
    </td>
    <td align="center">➜</td>
    <td align="center" valign="top" width="120">
      <img src="docs/assets/icons/data-studio.svg" alt="Data Studio" height="44"><br>
      <b>Dashboard</b><br>
      <sub>Data Studio</sub>
    </td>
  </tr>
</table>

## Getting started

**Prerequisites**
- [uv](https://docs.astral.sh/uv/) and Python 3.13.
- A Google Cloud project with BigQuery. The free sandbox works. Create an empty dataset for raw, for example `ecommerce_raw`, and one for dbt, for example `ecommerce_dev`.
- The `gcloud` CLI, logged in with Application Default Credentials. Impersonating a service account keeps key files off your machine:

  ```bash
  gcloud auth application-default login --impersonate-service-account=SERVICE_ACCOUNT_EMAIL
  ```

**Set up**

```bash
uv sync --all-groups
cp .env.example .env        # then set GCP_PROJECT and RAW_DATASET
uv run python -m ecom_pipeline.config      # check the values the commands will use
```

**Run the pipeline**

```bash
# 1. Copy the public dataset into a local snapshot (once)
uv run python -m ecom_pipeline.generator.snapshot

# 2. Write batch 0: the initial full load, all history before --start delivered as one
#    batch instead of being replayed day by day. With --end equal to --start, the runner
#    writes batch 0 only. Each table is built whole in memory (not streamed yet), so the
#    later the --start, the more memory batch 0 needs.
uv run python -m ecom_pipeline.generator.runner --start 2019-02-19 --end 2019-02-19

# 3. Write the daily batches: one per day from --start up to --end (exclusive).
#    Batches already written, including batch 0, are skipped.
uv run python -m ecom_pipeline.generator.runner --start 2019-02-19 --end 2020-01-01

# 4. Load every landed batch that is not loaded yet
uv run python -m ecom_pipeline.loader

# 5. Build and test the warehouse (profile setup: see the dbt README)
cd dbt/ecommerce_analytics
uv run dbt deps
uv run dbt build
```

Practical limits:
- **Memory:** every batch currently reads each source table in full, so one runner process needs about 4 GB with this dataset, whatever the batch size; batch 0 needs more with a later `--start`. `--workers` multiplies this, so raise it only with memory to spare.
- **Fixed replay start:** keep the same `--start` on every run, because it fixes each batch's window. Extend `--end` to add days.
- **Load quota:** BigQuery allows 1,500 load jobs per table per day, so load at most about 1,400 batches in any 24 hours.

## How it works

Each part has its own README with the details: the [Python package](src/ecom_pipeline/README.md) (snapshot, simulator, runner, landing contract, loader) and the [dbt project](dbt/ecommerce_analytics/README.md) (layers, models, tests).

**Snapshot.** Google regenerates thelook every day, so reading it live would change the data under the tests and the dashboard. `ecom_pipeline.generator.snapshot` copies every table once into a local DuckDB file, with a manifest that records row counts and when the copy was taken.

**Simulator and runner.** The simulator rebuilds what the shop's systems would have sent on a given day: for each day it hides everything that had not happened yet and returns each order's state at the end of that day, with change metadata (`_op`, `_source_updated_at`, `_seq`). It only produces data and writes nothing. The runner asks it for a first batch holding everything before the replay start, then one batch per day, and writes each batch to the landing zone.

**Landing zone.** Each batch is a folder of Parquet files, one per table, written to a temporary folder with its manifest last and then renamed, so a half-written batch is never visible. The format is defined once in `ecom_pipeline.landing_contract`, which the runner and the loader both import. The landed files are the system of record: if BigQuery tables are lost or expire, raw is reloaded from them.

**Loader.** `ecom_pipeline.loader` appends each finished batch to the raw table of the same name, adds `_batch_id`, `_batch_date` and `_loaded_at`, and records the batch in `_loaded_batches` once all its tables are in. That record is the loader's only state. If a run stops partway, the next run loads the whole batch again, and staging keeps one copy. See the [package README](src/ecom_pipeline/README.md) for the replay rules, the landing contract and the loader in detail.

**dbt.** dbt turns raw into staging, intermediate and mart models. See the [dbt project README](dbt/ecommerce_analytics/README.md) for the layers, models, bus matrix, design choices and tests.

## Key features

- **Simulated change data.** The source is copied once into a frozen snapshot, then replayed as daily batches of inserts and updates, so the pipeline handles new and changing data every day, as in production.
- **Idempotent, at-least-once loading.** The loader only appends and may load a batch twice; staging removes the copies. This works on BigQuery's free sandbox, which allows no updates or deletes.
- **Current state from a change log.** Raw keeps every version of every row; dbt picks the latest version per key with a deterministic tiebreak and keeps deletes as tombstones.
- **A tested dimensional model.** Four dimensions and six facts, with tests at every layer and reconciliations between facts.
- **Consistent metric definitions.** Standard retail terms (GMV, net sales, gross profit), revenue by order date and by the date money moved, and return rates and delivery times measured only once their outcome can be known.
- **No stored credentials.** Local runs use Application Default Credentials with service account impersonation.

## Project structure

```
├── src/ecom_pipeline/          Python package
│   ├── generator/              snapshot, simulator and runner
│   ├── landing_contract.py     the shared format of landed batches
│   ├── loader.py               loads landed batches into BigQuery raw
│   └── config.py               settings for every command, from .env
├── tests/                      pytest tests for the package
├── dbt/ecommerce_analytics/    the dbt project
├── notebook/eda/               exploratory analysis of the source
└── docs/                       documentation and assets
```

## Testing

```bash
uv run pytest               # the Python package
```

The dbt tests run as part of `dbt build`.

## Roadmap

| Phase | Scope | Status |
|---|---|---|
| 0. Foundations | Exploratory analysis, source snapshot, BigQuery sandbox checks | Done |
| 1. End to end | Landing, loading, staging, current state, Kimball marts, dashboard, documentation | Done |
| 2. Automation | CI/CD with GitHub Actions and workload identity federation; separate CI and production datasets | Planned |
| 3. Simulator, loader, runner | Daily batches, safe reruns, a load record, scheduled runs | Built early; scheduling planned |
| 4. Change over time | Status changes and deletes flowing through to history models | Status changes replayed; deletes and history planned |
| 5. Messy reality | Late, duplicate and missing batches, schema drift, dirty values, and a test for each | Planned |
| 6. Warehouse and BI depth | Type 2 dimensions, incremental models, partitioning, full dashboard | Planned |

## Known limitations

- **The data is synthetic.** thelook is generated, and some of its patterns would not occur in a real shop. They are listed in the [dbt README](dbt/ecommerce_analytics/README.md#known-data-caveats).
- **Free tier.** The BigQuery sandbox allows no updates or deletes, so every model is rebuilt in full for now, and tables expire after 60 days; raw is reloaded from the landed files and dbt rebuilds the rest.
- **The snapshot file is not shared.** Each user takes their own copy from the public dataset, so two copies taken on different days can differ.

## Acknowledgements

- Source data: Google's public BigQuery dataset `bigquery-public-data.thelook_ecommerce`.
- Icons: Google Cloud product icons, Simple Icons and Streamline. Product names and logos are trademarks of their owners.

## License

[MIT](LICENSE).
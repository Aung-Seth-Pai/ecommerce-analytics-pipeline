# ecommerce_analytics (dbt project)

The dbt project of [ecommerce-analytics-pipeline](../../README.md). It turns the append-only change log that the loader writes to BigQuery (`ecommerce_raw`) into a tested Kimball star schema, which the Data Studio dashboard reads.

- **Engine:** dbt v2 (the Fusion engine) on BigQuery, with `static_analysis: strict`.
- **Build style (Phase 1):** every model is rebuilt in full on each run. Incremental models come after Phase 1 is complete.
- **Source data:** a frozen snapshot of Google's public `thelook_ecommerce` dataset, replayed as daily batches. The data is synthetic; see [Known data caveats](#known-data-caveats).

## Layers

| Layer | Folder | Built as | Medallion | What it does |
|---|---|---|---|---|
| Sources | `models/staging/_src_ecommerce.yml` | – | Bronze | `ecommerce_raw`: every delivered version of every row, plus `_loaded_batches`, the record of fully loaded batches |
| Staging | `models/staging/` | views | Silver | One row per key per batch: finished batches only, reload copies removed, columns renamed and typed |
| Intermediate | `models/intermediate/` | views | Silver | Latest version per key, joins and roll-ups that the marts build on |
| Marts | `models/marts/` | tables | Gold | Kimball dimensions and facts for BI |

How data flows through the layers:

<table align="center">
  <tr>
    <td align="center" valign="top" width="150">
      <img src="../../docs/assets/icons/bigquery.svg" alt="BigQuery" height="48"><br>
      <b>Raw</b><br>
      <sub><code>ecommerce_raw</code><br>append-only change log</sub>
    </td>
    <td align="center">➜</td>
    <td align="center" valign="top" width="150">
      <img src="../../docs/assets/icons/dbt.svg" alt="dbt" height="48"><br>
      <b>Staging</b><br>
      <sub><code>stg_*</code> views</sub>
    </td>
    <td align="center">➜</td>
    <td align="center" valign="top" width="150">
      <img src="../../docs/assets/icons/dbt.svg" alt="dbt" height="48"><br>
      <b>Intermediate</b><br>
      <sub><code>int_*</code> views</sub>
    </td>
    <td align="center">➜</td>
    <td align="center" valign="top" width="150">
      <img src="../../docs/assets/icons/dbt.svg" alt="dbt" height="48"><br>
      <b>Marts</b><br>
      <sub><code>dim_*</code> and <code>fct_*</code> tables<br>+ <code>order_statuses</code> seed</sub>
    </td>
    <td align="center">➜</td>
    <td align="center" valign="top" width="150">
      <img src="../../docs/assets/icons/data-studio.svg" alt="Data Studio" height="48"><br>
      <b>Dashboard</b><br>
      <sub>Data Studio</sub>
    </td>
  </tr>
</table>

Each environment has one dataset (`ecommerce_dev` locally; `ecommerce_ci` and `ecommerce_prod` come with CI/CD in Phase 2), and the layers inside it are told apart by table prefix.

## Models

### Staging

Every staging model has one row per key per batch. It reads only batches recorded in `_loaded_batches`, keeps the latest load of each (key, batch), renames and types the columns, and casts money to `NUMERIC`.

| Model | Notes |
|---|---|
| `stg_orders` | `order_status`, `item_count` |
| `stg_order_items` | `order_item_status`, `sale_price` |
| `stg_users` | address as a `GEOGRAPHY` point (`user_location`), `user_traffic_source` |
| `stg_products` | `product_cost`, `retail_price`, department and category |
| `stg_inventory_items` | `unit_cost`; its `created_at` and `sold_at` are synthetic and unused downstream |
| `stg_distribution_centers` | location as a `GEOGRAPHY` point |
| `stg_events` | one row per event (append-only); IP-derived location as `ip_city`, `ip_state`, `ip_postal_code` |

### Intermediate

| Model | Grain | What it adds |
|---|---|---|
| `int_<entity>_current` (6 models) | one row per key | the latest version of each order, order item, user, product, stock unit and distribution center (SCD Type 1); deletes kept and flagged by `is_deleted` |
| `int_order_items_joined` | one row per order item | the order's status and creation time, the stock unit's cost and distribution center, and the straight-line shipping distance |
| `int_orders_enriched` | one row per order | item counts and money rolled up from the items, each user's order sequence, and maturity flags against the data cutoff |
| `int_users_web_activity` | one row per user with events | site activity summarised from sessions |
| `int_users_order_history` | one row per user with orders | first and latest order, orders placed and cancelled, lifetime net sales |
| `int_data_cutoff` | one row | the end of the latest loaded batch window: the data's own "now" |

### Marts

All dimensions are Type 1 with natural keys and keep deleted members, flagged by `is_deleted`; facts leave deleted rows out. Marts hold no personal data (names, emails, street addresses, IP addresses).

| Model | Type | Grain | Main contents |
|---|---|---|---|
| `dim_date` | dimension | one row per UTC day, 2018–2028 | calendar attributes; joined in several date roles |
| `dim_users` | dimension | one row per user | age and age band, location, traffic source, sign-up time |
| `dim_products` | dimension | one row per product | name, brand, category, department, retail price, cost |
| `dim_distribution_centers` | dimension | one row per distribution center | name and location |
| `fct_order_items` | transaction fact | one row per order item | sale price, unit cost, net sales, gross profit, shipping distance, four milestone dates |
| `fct_sales_movements` | transaction fact (ledger) | one row per order item per sale or return | money in and out on the date it moved |
| `fct_orders` | accumulating snapshot | one row per order | milestones, lags in minutes, maturity flags, item mix, GMV, net sales, gross profit, the user's order sequence |
| `fct_user_summary` | aggregate fact | one row per user | order history and site activity as of the last build |
| `fct_events` | factless fact | one row per website event | event type and page; product views carry `product_id` |
| `fct_sessions` | accumulating snapshot | one row per session | attributes, duration in seconds, landing and exit page, event counts per type, the user's session sequence |

### Bus matrix

Which dimensions each fact joins to. `order_id` is a degenerate dimension on the item-level facts.

| Fact | `dim_date` (roles) | `dim_users` | `dim_products` | `dim_distribution_centers` |
|---|---|---|---|---|
| `fct_order_items` | order, shipped, delivered, returned | ✓ | ✓ | ✓ |
| `fct_sales_movements` | movement | ✓ | ✓ | ✓ |
| `fct_orders` | order, shipped, delivered, returned | ✓ | | |
| `fct_user_summary` | | ✓ | | |
| `fct_events` | event | ✓ (null when anonymous) | ✓ (product views) | |
| `fct_sessions` | session | ✓ (null when anonymous) | | |

## Key design choices

- **Change log in, current state out.** Raw keeps every version of every row. The latest version per key is chosen by `_source_updated_at`, then `_batch_id`, then `_seq`, never by `_loaded_at`, because a rerun of an old batch gets a newer load time. One macro, `current_state`, implements this for all six current-state models.
- **Money is exact.** Amounts are `NUMERIC` (prices to cents, costs to 4 decimals), assumed to be USD, because the source stores floats.
- **Retail P&L vocabulary.** GMV (`gmv`) is everything ordered. Gross sales exclude cancellations. Net sales (`net_sales`) also exclude returns. Gross profit (`gross_profit`) is net sales minus the cost of the kept items. Gross margin % is a ratio computed in the BI tool. Returned items are assumed restocked.
- **Two date bases for net sales.** `fct_order_items` and `fct_orders` count net sales on the order date, from each item's current state, so past months change when returns arrive. `fct_sales_movements` books sales and refunds on the date money moved, so a month that has passed never changes. Both agree over all time, and a test reconciles them per item.
- **Outcome maturity.** Return rate and delivery time are measured only on orders old enough for the outcome to have happened. Age is measured against `int_data_cutoff`, the end of the loaded data, never `current_date`, so a replay or a delayed load is handled and rebuilding the same data gives the same results.
- **Measures in facts, descriptions in dimensions.** A user's lifetime order and activity measures live in `fct_user_summary`, so `dim_users` depends only on the user record and stays stable, ready for Type 2 history later.
- **No link between orders and sessions.** Events carry no order id, and purchase events can't be matched to orders by time, so the funnel is reported as totals per step.
- **Status sort order lives in a seed,** so every tool shows statuses in lifecycle order.
- **SQL portability.** ANSI SQL first. BigQuery-only features are used only where needed: `QUALIFY`, `ST_DISTANCE`, `TIMESTAMP_DIFF`.

## Conventions

- **Model names:** `stg_<entity>`, `int_<entity>_current`, `_joined` or `_enriched`, `dim_<entity>`, `fct_<process>`, and `fct_<entity>_summary` for aggregate facts. Entity names are plural, as in the source.
- **Column names:** keys are renamed to `<entity>_id`. An attribute whose plain name means different things in different tables gets a specific name: `order_status` / `order_item_status`, `user_traffic_source` / `session_traffic_source`, `ip_city`, `product_cost` / `unit_cost`. Timestamps keep their source names in staging and are named by role where tables meet (`order_created_at`, `order_date`).
- **Documentation:** every source, model and column is described in the YAML file of its folder (`_src_`, `_stg_`, `_int_`, `_marts_ecommerce.yml`). The shared metadata columns are described once in `models/staging/_docs.md`.

## Testing

Each layer tests what it adds, so a failure points to where things broke. Tests that only restate the code, or that would fail on normal business behaviour, are left out.

| Layer | What is tested |
|---|---|
| Sources | key and metadata columns not null, valid `_op` values, the same row twice within one load (warn), freshness (warn after 24 h, error after 48 h) |
| Staging | one row per key and batch, foreign keys not null, accepted values and ranges; singular tests for the order lifecycle, statuses matching timestamps, constant session attributes, and money precision (warn) |
| Intermediate | unique keys and relationships; singular tests that finished orders have no open items, item counts match, no order comes before its account and no event long before it, and stock unit cost equals product cost (warn); a unit test for `current_state` |
| Marts | grain, relationships to every dimension and date role, statuses against the seed, maturity windows against the data (warn), and reconciliations between facts |

The reconciliations form a chain: `int_order_items_joined` → `fct_order_items` → `fct_orders` → `fct_user_summary`, plus `fct_order_items` ↔ `fct_sales_movements` and `fct_events` ↔ `fct_sessions`.

Singular tests live in `tests/<layer>/`.

## Configuration

**Variables** (`dbt_project.yml`):

| Variable | Value | Meaning |
|---|---|---|
| `return_window_days` | 3 | days after delivery within which a return counts; an order's return outcome is final this long after delivery |
| `delivery_window_days` | 8 | days after ordering by which an order's delivery outcome is final |

Both values are thelook's hard limits: every return happens within 3 days of delivery and every delivery within 8 days of the order. Real data would need windows taken from percentiles or a return policy, and warn-level tests flag a window shorter than the data. Override them for a run with `--vars`, for example `dbt build --vars '{return_window_days: 30}'`.

**Seed:** `seeds/order_statuses.csv` holds each order and item status's lifecycle position (`status_order`), whether it is final, and what it means.

**Packages:** `dbt_utils` for generic tests and surrogate keys; `codegen` as a development helper.

## Running it

**Prerequisites:**
- `ecommerce_raw` loaded by the pipeline's loader; see the [project README](../../README.md).
- dbt v2, installed with the project's dependency group: `uv sync --group dbt`, from the repository root.
- Google Cloud credentials **without key files**: Application Default Credentials that impersonate a service account.

```bash
gcloud auth application-default login --impersonate-service-account=SERVICE_ACCOUNT_EMAIL
```

The service account needs BigQuery Job User and BigQuery Read Session User on the project, Data Editor on the target dataset, and Data Viewer on `ecommerce_raw`.

**Profile** (`~/.dbt/profiles.yml`, outside the repository):

```yaml
ecommerce_analytics:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: YOUR_GCP_PROJECT
      dataset: ecommerce_dev
      location: US
      threads: 4
```

**Commands**, run from `dbt/ecommerce_analytics` with the virtual environment active:

```bash
dbt deps                  # install packages
dbt build                 # seed, build and test everything
dbt source freshness      # check that new batches are arriving
dbt docs generate         # build the documentation site
dbt docs serve            # browse models, columns and lineage
```

## Known data caveats

thelook is generated data, and some of its patterns would not hold in a real shop. They are documented as properties of the source, not fixed:

- **Orders move as one.** Every item in an order shares the order's status and timestamps, and every return covers the whole order at one moment. The models still handle partial returns and mixed statuses.
- **Hard time limits:** every order is delivered within 8 days of being placed, and every return happens within 3 days of delivery.
- **Distribution centers are not chosen by distance.** Centers are all in the US while many customers are not, so straight-line shipping distances are often intercontinental.
- **Most sessions are anonymous** (about three quarters), and purchase events cannot be matched to orders.
- **Some events come shortly before the account they belong to was created,** by a few hours at most; a test allows up to 24 hours.
- **Inventory `created_at` and `sold_at` describe no real stock process,** so no mart uses them; stock units only supply each item's cost.
- **A stock unit's cost always equals its product's cost.**
- **Amounts are stored as floats** in the source; prices are whole cents and costs have at most 4 decimals. The currency is assumed to be USD.

## Roadmap

- **Phase 2:** CI/CD with GitHub Actions and workload identity federation: every pull request builds into `ecommerce_ci`; merges and a daily schedule build into `ecommerce_prod`.
- **After Phase 1:** billing, incremental models with lookback windows (the maturity flags and session rows change for recent data), partitioning and clustering.
- **Later phases:** status changes and deletes from the simulator, late and messy batches, Type 2 history with surrogate keys, an unknown-user member for anonymous activity, and a monthly per-user snapshot.
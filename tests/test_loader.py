"""Tests for the loader, against batches written by the real runner and a fake BigQuery client.

The fake client keeps every appended table in memory instead of sending it to BigQuery. It has
only the four methods the loader calls (get_dataset, query, load_table_from_file and the job's
result), so the tests check the loader's own logic: what it appends, in which order, what it
records, and what happens when a load fails. Whether BigQuery accepts the files is checked by
the real run, not here.
"""
import json
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from google.api_core.exceptions import NotFound, ServiceUnavailable
from google.cloud import bigquery

from ecom_pipeline import loader
from ecom_pipeline.generator import runner
from ecom_pipeline.landing_contract import MANIFEST_NAME, batch_dir_name

DATASET = "test-project.ecommerce_raw"
REPLAY_START = pd.Timestamp("2026-01-03", tz="UTC")
REPLAY_END = pd.Timestamp("2026-01-06", tz="UTC")      # batch 0 plus daily batches 1, 2 and 3
TABLE_KEYS = {"orders": "order_id", "order_items": "id", "users": "id", "events": "id"}


# --- the fake BigQuery client ---------------------------------------------------------------

class FakeLoadJob:
    """A finished load job. output_rows is what the job reports it wrote."""

    def __init__(self, output_rows: int):
        self.output_rows = output_rows

    def result(self):
        return self


class FakeBigQuery:
    """Stands in for bigquery.Client and keeps each table's appended rows in memory.

    Two switches make it misbehave:
        fail_on_table: loading this table raises ServiceUnavailable, like a network error.
        rows_lost: every job reports this many rows fewer than it was given.
    """

    def __init__(self):
        self.appends: dict[str, list[pa.Table]] = {}    # table name -> one Arrow table per load job
        self.job_configs: list[bigquery.LoadJobConfig] = []
        self.fail_on_table: str | None = None
        self.rows_lost = 0

    def get_dataset(self, dataset_id):
        if dataset_id != DATASET:
            raise NotFound(f"Dataset {dataset_id} not found")

    def query(self, sql):
        if loader.LOADED_BATCHES_TABLE not in self.appends:
            raise NotFound("Table _loaded_batches not found")
        ids = set(self.rows(loader.LOADED_BATCHES_TABLE)["batch_id"])
        rows = [SimpleNamespace(batch_id=batch_id) for batch_id in ids]
        return SimpleNamespace(result=lambda: rows)

    def load_table_from_file(self, file, table_id, job_config):
        table_name = table_id.removeprefix(f"{DATASET}.")
        if table_name == self.fail_on_table:
            raise ServiceUnavailable(f"simulated failure loading {table_name}")
        data = pq.read_table(file)                      # read what the loader would upload
        self.appends.setdefault(table_name, []).append(data)
        self.job_configs.append(job_config)
        return FakeLoadJob(data.num_rows - self.rows_lost)

    def rows(self, table_name) -> pd.DataFrame:
        """Every row appended to a table so far."""
        return pa.concat_tables(self.appends[table_name]).to_pandas()


# --- fixtures ---------------------------------------------------------------------------------

@pytest.fixture
def snapshot_file(duckdb_conn, tmp_path):
    """The fixture snapshot from conftest.py saved as a DuckDB file, which the runner needs."""
    path = tmp_path / "snapshot.duckdb"
    duckdb_conn.execute(f"ATTACH '{path}' AS snapshot")
    duckdb_conn.execute("COPY FROM DATABASE memory TO snapshot")
    duckdb_conn.execute("DETACH snapshot")
    return path


@pytest.fixture
def land(snapshot_file, tmp_path):
    """Return a function that lands batches up to a given replay end with the real runner."""
    landing_dir = tmp_path / "landing"

    def _land(replay_end):
        runner.run(snapshot_file, replay_start=REPLAY_START, replay_end=replay_end, base_dir=landing_dir)
        return landing_dir

    return _land


@pytest.fixture
def landing_dir(land):
    """A landing zone holding batches 0 to 3."""
    return land(REPLAY_END)


@pytest.fixture
def client():
    return FakeBigQuery()


def manifest_rows(landing_dir, table_name):
    """The total rows of a table across every landed manifest."""
    return sum(
        json.loads(path.read_text())["tables"][table_name]["row_count"]
        for path in landing_dir.glob(f"batch_*/{MANIFEST_NAME}")
    )


def edit_manifest(landing_dir, batch_id, table_name, **changes):
    """Change one table's entry in a landed manifest, to simulate a bad delivery."""
    path = landing_dir / batch_dir_name(batch_id) / MANIFEST_NAME
    manifest = json.loads(path.read_text())
    manifest["tables"][table_name].update(changes)
    path.write_text(json.dumps(manifest))


# --- finding the work -------------------------------------------------------------------------

def test_a_dry_run_lists_every_finished_batch_and_loads_nothing(landing_dir, client):
    (landing_dir / "batch_00004.tmp").mkdir()           # unfinished: must be ignored
    assert loader.run(client, DATASET, landing_dir, dry_run=True) == [0, 1, 2, 3]
    assert client.appends == {}


def test_a_missing_dataset_stops_before_anything_is_loaded(landing_dir, client):
    with pytest.raises(NotFound):
        loader.run(client, "test-project.other_dataset", landing_dir)
    assert client.appends == {}


# --- a normal run -----------------------------------------------------------------------------

def test_every_table_is_appended_and_every_batch_recorded(landing_dir, client):
    assert loader.run(client, DATASET, landing_dir) == [0, 1, 2, 3]
    for table_name in [*TABLE_KEYS, "products", "inventory_items", "distribution_centers"]:
        assert len(client.rows(table_name)) == manifest_rows(landing_dir, table_name)
    assert sorted(client.rows(loader.LOADED_BATCHES_TABLE)["batch_id"]) == [0, 1, 2, 3]


def test_every_load_job_appends_parquet(landing_dir, client):
    loader.run(client, DATASET, landing_dir)
    for config in client.job_configs:
        assert config.source_format == bigquery.SourceFormat.PARQUET
        assert config.write_disposition == bigquery.WriteDisposition.WRITE_APPEND


def test_rows_carry_their_batch_id_and_delivery_date(landing_dir, client):
    loader.run(client, DATASET, landing_dir)
    orders = client.rows("orders")
    pairs = sorted(set(zip(orders["_batch_id"], orders["_batch_date"].astype(str))))
    # _batch_date is the UTC date of window_end: batch 0 ends at --start, batch k a day later each
    assert pairs == [(0, "2026-01-03"), (1, "2026-01-04"), (2, "2026-01-05"), (3, "2026-01-06")]


def test_ingestion_columns_have_the_types_bigquery_should_create(landing_dir, client):
    loader.run(client, DATASET, landing_dir)
    schema = client.appends["orders"][0].schema
    assert schema.field("_batch_id").type == pa.int64()                     # INTEGER
    assert schema.field("_batch_date").type == pa.date32()                  # DATE
    assert schema.field("_loaded_at").type == pa.timestamp("us", tz="UTC")  # TIMESTAMP


def test_all_tables_of_one_batch_share_one_loaded_at_from_the_run(landing_dir, client):
    started = pd.Timestamp.now(tz="UTC")
    loader.run(client, DATASET, landing_dir)
    finished = pd.Timestamp.now(tz="UTC")
    batch_0 = [table for name, tables in client.appends.items()
               if name != loader.LOADED_BATCHES_TABLE
               for table in tables if table["_batch_id"][0].as_py() == 0]
    loaded_at = {table["_loaded_at"][0].as_py() for table in batch_0}
    assert len(loaded_at) == 1
    assert started <= loaded_at.pop() <= finished


def test_empty_tables_are_skipped(landing_dir, client):
    loader.run(client, DATASET, landing_dir)
    # the static tables have rows only in batch 0, so each gets exactly one load job
    assert len(client.appends["products"]) == 1
    assert set(client.rows("products")["_batch_id"]) == {0}


# --- later runs -------------------------------------------------------------------------------

def test_a_second_run_loads_nothing(landing_dir, client):
    loader.run(client, DATASET, landing_dir)
    assert loader.run(client, DATASET, landing_dir) == []
    assert len(client.rows("orders")) == manifest_rows(landing_dir, "orders")


def test_a_later_run_loads_only_the_batches_landed_since(land, client):
    landing_dir = land(pd.Timestamp("2026-01-04", tz="UTC"))       # batches 0 and 1
    assert loader.run(client, DATASET, landing_dir) == [0, 1]
    land(REPLAY_END)                                                # batches 2 and 3 arrive
    assert loader.run(client, DATASET, landing_dir) == [2, 3]
    assert len(client.rows("orders")) == manifest_rows(landing_dir, "orders")


# --- failures ---------------------------------------------------------------------------------

def test_a_failed_table_leaves_its_batch_unrecorded(landing_dir, client):
    client.fail_on_table = "orders"
    with pytest.raises(ServiceUnavailable):
        loader.run(client, DATASET, landing_dir)
    assert loader.LOADED_BATCHES_TABLE not in client.appends      # batch 0 is not marked loaded
    assert len(client.appends) > 0                                 # but the tables before orders are in


def test_a_rerun_after_a_failure_leaves_duplicates_that_staging_removes(landing_dir, client):
    client.fail_on_table = "orders"
    with pytest.raises(ServiceUnavailable):
        loader.run(client, DATASET, landing_dir)
    client.fail_on_table = None
    assert loader.run(client, DATASET, landing_dir) == [0, 1, 2, 3]

    # at least once: tables appended before the failure were appended again
    duplicated = [name for name in TABLE_KEYS
                  if len(client.rows(name)) > manifest_rows(landing_dir, name)]
    assert duplicated
    # the staging rule: one row per key and _batch_id, latest _loaded_at, gives back the landed rows
    for table_name, key in TABLE_KEYS.items():
        rows = client.rows(table_name).sort_values("_loaded_at")
        deduplicated = rows.drop_duplicates(subset=[key, "_batch_id"], keep="last")
        assert len(deduplicated) == manifest_rows(landing_dir, table_name)


def test_a_file_whose_rows_differ_from_its_manifest_stops_the_run(landing_dir, client):
    edit_manifest(landing_dir, 0, "orders", row_count=999)
    with pytest.raises(RuntimeError, match="manifest says 999"):
        loader.run(client, DATASET, landing_dir)
    assert loader.LOADED_BATCHES_TABLE not in client.appends


def test_a_job_that_writes_other_rows_than_it_was_given_stops_the_run(landing_dir, client):
    client.rows_lost = 1
    with pytest.raises(RuntimeError, match="load job wrote"):
        loader.run(client, DATASET, landing_dir)
    assert loader.LOADED_BATCHES_TABLE not in client.appends


def test_a_file_in_another_format_is_refused(landing_dir, client):
    edit_manifest(landing_dir, 0, "orders", file_format="csv", file_name="orders.csv")
    with pytest.raises(ValueError, match="only parquet"):
        loader.run(client, DATASET, landing_dir)
    assert loader.LOADED_BATCHES_TABLE not in client.appends
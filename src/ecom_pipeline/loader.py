"""Load finished batches from the landing zone into BigQuery raw tables.

Every finished batch (see landing_contract) is loaded once, in batch id order. Each table file is
appended to the raw table of the same name, with three columns added:
    _batch_id    the batch the row arrived in
    _batch_date  the day the batch was delivered: the UTC date of its window_end
    _loaded_at   when this loader appended the batch

When every table of a batch is appended, one row is appended to _loaded_batches. That table is
the loader's only state: a batch listed there is not loaded again.

Delivery is at least once. If a run stops partway through a batch, the tables already appended
stay, and the next run appends the whole batch again. The two copies are identical except for
_loaded_at, so dbt staging keeps one row per (key, _batch_id). dbt runs only after the loader
has finished without an error.

    uv run python -m ecom_pipeline.loader              # load every batch not loaded yet
    uv run python -m ecom_pipeline.loader --dry-run    # list them, load nothing
"""
import argparse
import io
import logging
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from ecom_pipeline.config import Settings
from ecom_pipeline.landing_contract import Manifest, read_manifest

LOADED_BATCHES_TABLE = "_loaded_batches"

log = logging.getLogger(__name__)


def find_finished_batches(landing_dir: Path) -> list[tuple[Path, Manifest]]:
    """Return (folder, manifest) for every finished batch in landing_dir, ordered by batch id."""
    batches = []
    for folder in landing_dir.iterdir():
        manifest = read_manifest(folder)       # None for .tmp folders and anything else
        if manifest is not None:
            batches.append((folder, manifest))
    return sorted(batches, key=lambda batch: batch[1].batch_id)


def get_loaded_batch_ids(client: bigquery.Client, dataset_id: str) -> set[int]:
    """Return the batch ids in _loaded_batches; empty before the first batch is loaded."""
    sql = f"SELECT DISTINCT batch_id FROM `{dataset_id}.{LOADED_BATCHES_TABLE}`"
    try:
        rows = client.query(sql).result()
    except NotFound:                           # the table is created by the first load
        return set()
    return {row.batch_id for row in rows}


def add_ingestion_columns(
    table: pa.Table, *, batch_id: int, batch_date, loaded_at: datetime
) -> pa.Table:
    """Return the table with _batch_id, _batch_date and _loaded_at appended."""
    rows = table.num_rows
    return (
        table.append_column("_batch_id", pa.repeat(pa.scalar(batch_id, pa.int64()), rows))
        .append_column("_batch_date", pa.repeat(pa.scalar(batch_date, pa.date32()), rows))
        .append_column(
            "_loaded_at", pa.repeat(pa.scalar(loaded_at, pa.timestamp("us", tz="UTC")), rows)
        )
    )


def append_to_table(client: bigquery.Client, table_id: str, table: pa.Table) -> None:
    """Append an Arrow table to a BigQuery table with one load job, creating the table if needed.

    A load job is atomic: either all its rows are committed or none are.

    Raises:
        google.api_core.exceptions.GoogleAPIError: the job failed; nothing was written.
        RuntimeError: the job wrote another number of rows than it was given.
    """
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_file(buffer, table_id, job_config=job_config)
    job.result()                               # waits for the job; raises if it failed
    if job.output_rows != table.num_rows:
        raise RuntimeError(
            f"{table_id}: the load job wrote {job.output_rows} rows, expected {table.num_rows}"
        )


def load_batch(client: bigquery.Client, dataset_id: str, folder: Path, manifest: Manifest) -> int:
    """Append every non-empty table of one batch, then record the batch in _loaded_batches.

    Returns:
        The number of tables appended.

    Raises:
        ValueError: a file is not Parquet, the only format this loader reads.
        RuntimeError: a file holds another number of rows than its manifest says.
    """
    loaded_at = datetime.now(timezone.utc) 
    batch_date = manifest.window_end.astimezone(timezone.utc).date()
    appended = 0
    for table_name, summary in manifest.tables.items():
        if summary.row_count == 0:             # e.g. the static tables after batch 0 have no data
            continue
        if summary.file_format != "parquet":
            raise ValueError(f"{folder / summary.file_name}: only parquet files can be loaded")
        table = pq.read_table(folder / summary.file_name)
        if table.num_rows != summary.row_count:
            raise RuntimeError(
                f"{folder / summary.file_name} has {table.num_rows} rows, "
                f"its manifest says {summary.row_count}"
            )
        table = add_ingestion_columns(
            table, batch_id=manifest.batch_id, batch_date=batch_date, loaded_at=loaded_at
        )
        append_to_table(client, f"{dataset_id}.{table_name}", table)
        appended += 1

    # written last: a batch is listed here only once all its tables are in
    marker = pa.table({
        "batch_id": pa.array([manifest.batch_id], pa.int64()),
        "loaded_at": pa.array([loaded_at], pa.timestamp("us", tz="UTC")),
    })
    append_to_table(client, f"{dataset_id}.{LOADED_BATCHES_TABLE}", marker)
    return appended


def run(
    client: bigquery.Client, dataset_id: str, landing_dir: Path, *, dry_run: bool = False
) -> list[int]:
    """Load every finished batch that is not in _loaded_batches yet, in batch id order.

    Returns:
        The ids of the batches loaded (or, with dry_run, the ones that would be).

    Raises:
        google.api_core.exceptions.NotFound: the dataset does not exist.
    """
    client.get_dataset(dataset_id)             # fail before any work if the dataset is missing
    loaded = get_loaded_batch_ids(client, dataset_id)
    pending = [
        (folder, manifest)
        for folder, manifest in find_finished_batches(landing_dir)
        if manifest.batch_id not in loaded
    ]
    pending_ids = [manifest.batch_id for _, manifest in pending]
    log.info("%d batches already loaded, %d to load: %s", len(loaded), len(pending), pending_ids)
    if dry_run:
        return pending_ids

    for folder, manifest in pending:
        appended = load_batch(client, dataset_id, folder, manifest)
        log.info("batch %d loaded: %d tables appended", manifest.batch_id, appended)
    return pending_ids


def main() -> None:
    """Command-line entry point: read Settings, then load."""
    parser = argparse.ArgumentParser(description="Load landed batches into BigQuery raw tables.")
    parser.add_argument("--landing-dir", type=Path, default=None,
                        help="default: LANDING_DIR from Settings")
    parser.add_argument("--dry-run", action="store_true", help="list the batches to load and stop")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings()
    project = settings.require_gcp_project()
    landing_dir = args.landing_dir if args.landing_dir is not None else settings.landing_dir
    client = bigquery.Client(project=project, location=settings.gcp_location)
    run(client, f"{project}.{settings.raw_dataset}", landing_dir, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
"""Run the simulator over a date range and write every batch to the landing zone.

Batch 0 holds everything written before --start (the replay start). Each later batch holds one day:
batch k is the window [replay_start + (k-1) days, replay_start + k days). --end (the replay end) is
the exclusive end of the last window.

Each batch is a folder of one Parquet file per table plus a _manifest.json that is written last.
A folder with its final name is therefore complete, and a rerun skips it. The names, the manifest's
shape and these rules are defined in ecom_pipeline.landing_contract, which the loader also uses.

A batch is built one table at a time, so memory use is about the size of the biggest table. Batch 0
always runs first and alone, because it is by far the biggest. With --workers N the daily batches
then run N at a time, each in its own process with its own read-only connection to the snapshot,
so memory use is about N times the biggest table. The default is 1 worker.

    uv run python -m ecom_pipeline.generator.runner --start 2020-01-01 --end 2020-01-08
    uv run python -m ecom_pipeline.generator.runner --start 2020-01-01 --end 2021-01-01 --workers 4
"""
import argparse
import multiprocessing
import shutil
from collections.abc import Iterable
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import closing
from pathlib import Path

import duckdb
import pandas as pd
from tqdm import tqdm

from ecom_pipeline.config import Settings
from ecom_pipeline.generator import simulator
from ecom_pipeline.landing_contract import (
    MANIFEST_NAME,
    Manifest,
    TableSummary,
    batch_dir_name,
    check_metadata_columns,
    read_manifest,
    table_file_name,
    temp_dir_name,
)


def batch_window(batch_id: int, replay_start: pd.Timestamp) -> tuple[pd.Timestamp | None, pd.Timestamp]:
    """Return the (window_start, window_end) of a batch.

    Batch 0 has no lower bound and ends at replay_start. Batch k >= 1 is the day that begins
    k - 1 days after replay_start.

    Args:
        batch_id: 0 for the initial load, 1 and up for the daily batches.
        replay_start: Midnight UTC of the first replayed day. Always a real date, never None.

    Returns:
        (window_start, window_end) of this one batch. window_start is inclusive and is None for
        batch 0; window_end is exclusive.
    """
    if batch_id == 0:
        return None, replay_start
    return (
        replay_start + pd.Timedelta(days=batch_id - 1),
        replay_start + pd.Timedelta(days=batch_id),
    )


def save_batch_to_landing(
    tables: Iterable[tuple[str, pd.DataFrame]],
    batch_id: int,
    *,
    window_start: pd.Timestamp | None,
    window_end: pd.Timestamp,
    seed: int,
    base_dir: Path,
) -> Path:
    """Write one batch as <base_dir>/batch_<id>/<table>.parquet plus _manifest.json.

    Everything is written into a temporary folder, the manifest last, and the folder is renamed
    to its final name only when complete. A crash therefore never leaves a final-named folder
    that is missing files or holds a manifest from an earlier run. A table without the contract's
    metadata columns stops the write before anything is published.

    Args:
        tables: (table name, DataFrame) pairs. Pass a generator such as
            simulator.iter_batch_tables(...) to keep only one table in memory at a time, or
            batch.items() for a dictionary that is already built.
        batch_id: Batch number; it names the folder.
        window_start: Inclusive start of the batch window, or None for batch 0.
        window_end: Exclusive end of the batch window.
        seed: The simulator seed the batch was built with.
        base_dir: The landing zone.

    Returns:
        The final batch folder.

    Raises:
        ValueError: a table lacks a metadata column; the .tmp folder is left for the next run to remove.
    """
    final_dir = base_dir / batch_dir_name(batch_id)
    temp_dir = base_dir / temp_dir_name(batch_id)
    if temp_dir.exists():                      # left behind by a crashed run
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True)

    table_summaries: dict[str, TableSummary] = {}
    for table_name, df in tables:
        check_metadata_columns(table_name, df.columns)   # never publish a table that breaks the contract
        parquet_path = temp_dir / table_file_name(table_name)
        df.to_parquet(parquet_path, engine="pyarrow", index=False, compression="snappy")
        table_summaries[table_name] = TableSummary(
            row_count=len(df),
            file_name=parquet_path.name,
            byte_size=parquet_path.stat().st_size,
            file_format="parquet",
        )
        del df                                 # let go of this table before the next one is built

    manifest = Manifest(
        batch_id=batch_id,
        window_start=window_start.to_pydatetime() if window_start is not None else None,
        window_end=window_end.to_pydatetime(),
        seed=seed,
        tables=table_summaries,
    )
    (temp_dir / MANIFEST_NAME).write_text(manifest.model_dump_json(indent=2))

    if final_dir.exists():                     # no manifest (else the batch would have been skipped): incomplete
        shutil.rmtree(final_dir)
    temp_dir.rename(final_dir)                 # the batch becomes visible, complete, in one step
    return final_dir


def write_batch(
    connection: duckdb.DuckDBPyConnection,
    batch_id: int,
    *,
    replay_start: pd.Timestamp,
    base_dir: Path,
    seed: int,
) -> Path:
    """Build one batch from the snapshot, one table at a time, and write it to the landing zone."""
    window_start, window_end = batch_window(batch_id, replay_start)
    tables = simulator.iter_batch_tables(
        connection, window_start=window_start, window_end=window_end, seed=seed
    )
    return save_batch_to_landing(
        tables, batch_id, window_start=window_start, window_end=window_end, seed=seed, base_dir=base_dir
    )


def is_batch_written(batch_dir: Path, window_end: pd.Timestamp) -> bool:
    """Return True if batch_dir is a finished batch for the expected window.

    Whether a folder is a finished batch is the contract's rule (read_manifest); whether it was
    written for this run's window is the runner's own check, because only the runner knows --start.

    Raises:
        ValueError: The folder holds a batch for a different window, which means --start changed
            since it was written. Mixing the two would corrupt the landing zone.
        pydantic.ValidationError: The folder's manifest is broken.
    """
    manifest = read_manifest(batch_dir)
    if manifest is None:
        return False
    if manifest.window_end != window_end:
        raise ValueError(
            f"{batch_dir} ends at {manifest.window_end.isoformat()} but this run expects "
            f"{window_end.isoformat()}. Was --start changed? Use a new landing directory or "
            "delete the old batches."
        )
    return True


# Each worker process opens its own read-only connection once and keeps it for all its batches.
_worker_connection: duckdb.DuckDBPyConnection | None = None


def _init_worker(duckdb_path: str) -> None:
    """Open this worker process's connection to the snapshot."""
    global _worker_connection
    _worker_connection = duckdb.connect(duckdb_path, read_only=True)


def _write_batch_in_worker(batch_id: int, replay_start: pd.Timestamp, base_dir: Path, seed: int) -> int:
    """Run write_batch on the worker's own connection. Returns batch_id."""
    write_batch(_worker_connection, batch_id, replay_start=replay_start, base_dir=base_dir, seed=seed)
    return batch_id


def run(
    duckdb_path: str | Path,
    *,
    replay_start: pd.Timestamp,
    replay_end: pd.Timestamp,
    base_dir: Path,
    seed: int = 42,
    workers: int = 1,
) -> int:
    """Write batch 0 and one batch per day from replay_start up to replay_end, skipping finished ones.

    Batch 0 runs first and alone. The daily batches then run one after another (workers=1) or
    several at a time, each in its own process (workers > 1). Memory use grows with the number of
    workers, so raise it only as far as the RAM allows.

    Args:
        duckdb_path: The snapshot file. Each process opens it read-only on its own.
        replay_start: Midnight UTC of the first replayed day; batch 0 holds everything before it.
        replay_end: Exclusive end of the last window, midnight UTC, after replay_start.
        base_dir: The landing zone.
        seed: The simulator seed.
        workers: How many daily batches to build at the same time.

    Returns:
        The number of batches written in this run (skipped ones are not counted).

    Raises:
        ValueError: A batch already in base_dir was written for a different window.
    """
    daily_batches = (replay_end - replay_start).days

    # decide what is left to do before starting any work, so a mismatch fails immediately
    pending = []
    for batch_id in range(daily_batches + 1):
        _, window_end = batch_window(batch_id, replay_start)
        if not is_batch_written(base_dir / batch_dir_name(batch_id), window_end):
            pending.append(batch_id)
    if not pending:
        return 0
    to_write = len(pending)

    with tqdm(total=to_write, desc="batches") as progress:
        with closing(duckdb.connect(str(duckdb_path), read_only=True)) as connection:
            if pending[0] == 0:                # batch 0 is by far the biggest: run it alone, first
                write_batch(connection, 0, replay_start=replay_start, base_dir=base_dir, seed=seed)
                progress.update(1)
                pending = pending[1:]
            if workers == 1:
                for batch_id in pending:
                    write_batch(connection, batch_id, replay_start=replay_start, base_dir=base_dir, seed=seed)
                    progress.update(1)
                pending = []

        if pending:
            # "spawn" starts clean processes; forking after a DuckDB connection was opened is unsafe
            with ProcessPoolExecutor(
                max_workers=workers,
                mp_context=multiprocessing.get_context("spawn"),
                initializer=_init_worker,
                initargs=(str(duckdb_path),),
            ) as pool:
                futures = [
                    pool.submit(_write_batch_in_worker, batch_id, replay_start, base_dir, seed)
                    for batch_id in pending
                ]
                try:
                    for future in as_completed(futures):
                        future.result()        # re-raises an error from the worker
                        progress.update(1)
                except BaseException:
                    for future in futures:     # do not start the batches that have not begun
                        future.cancel()
                    raise
    return to_write


def _utc_midnight(text: str) -> pd.Timestamp:
    """Parse a YYYY-MM-DD argument as midnight UTC."""
    return pd.Timestamp(text, tz="UTC")


def main() -> None:
    """Command-line entry point: read Settings, then run the range."""
    parser = argparse.ArgumentParser(description="Write simulated batches to the landing zone.")
    parser.add_argument("--start", type=_utc_midnight, required=True, help="first replayed day, YYYY-MM-DD")
    parser.add_argument("--end", type=_utc_midnight, required=True, help="exclusive end of the last window, YYYY-MM-DD; equal to --start writes batch 0 only")
    parser.add_argument("--landing-dir", type=Path, default=None, help="where batches are written; default is $LANDING_DIR from Settings")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--workers", type=int, default=1,
        help="daily batches built at the same time; each needs about one table's worth of RAM (default 1)",
    )
    args = parser.parse_args()
    if args.end < args.start:
        parser.error("--end must not be before --start (equal means batch 0 only)")
    if args.workers < 1:
        parser.error("--workers must be at least 1")

    settings = Settings()
    landing_dir = args.landing_dir if args.landing_dir is not None else settings.landing_dir
    if not settings.duckdb_path.exists():
        raise FileNotFoundError(f"Snapshot not found: {settings.duckdb_path}. Check DUCKDB_PATH in .env.")

    written = run(
        settings.duckdb_path,
        replay_start=args.start,
        replay_end=args.end,
        base_dir=landing_dir,
        seed=args.seed,
        workers=args.workers,
    )
    print(f"Wrote {written} batches to {landing_dir}")

if __name__ == "__main__":
    main()
"""The landing contract: the format the runner writes and the loader reads.

The runner and the loader never call each other. They only share files, so the shape of those
files is their interface, and this module is its single definition. Both sides import it, so
neither can change the format without the other noticing.

    <landing_dir>/
        batch_00063/                  a finished batch: final name and a manifest inside
            orders.parquet ...        one Parquet file per table
            _manifest.json            written last; validated by the Manifest model
        batch_00064.tmp/              a batch being written, or left by a crash: never read

Rules every writer follows:
    1. Write everything into the .tmp folder, the manifest written last, then rename it to its final name.
    2. A folder counts as a batch only when its name is a finished-batch name and it holds a manifest.
    3. Every table carries the metadata columns in METADATA_COLUMNS.

This module defines names, rules and the manifest's shape. How a batch is built (runner) and how it
is loaded (loader) stay in those programs.
"""
import re
from pathlib import Path

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

MANIFEST_NAME = "_manifest.json"
TEMP_SUFFIX = ".tmp"
BATCH_PREFIX = "batch_"
BATCH_ID_WIDTH = 5
MAX_BATCH_ID = 10**BATCH_ID_WIDTH - 1      # 99999: the most a five-digit name can hold

# Columns the simulator adds to every table. Part of the contract because the loader and dbt
# depend on them, so the runner checks them before writing.
METADATA_COLUMNS = ("_op", "_source_updated_at", "_seq")

_BATCH_DIR_PATTERN = re.compile(rf"{re.escape(BATCH_PREFIX)}(\d{{{BATCH_ID_WIDTH}}})")


def batch_dir_name(batch_id: int) -> str:
    """Return the folder name of a finished batch, e.g. batch_00007.

    Zero padding makes alphabetical order equal numeric order.

    Raises:
        ValueError: batch_id is outside 0..MAX_BATCH_ID, where the name would get another digit
            and stop sorting and parsing correctly.
    """
    if not 0 <= batch_id <= MAX_BATCH_ID:
        raise ValueError(f"batch_id must be between 0 and {MAX_BATCH_ID}, got {batch_id}")
    return f"{BATCH_PREFIX}{batch_id:0{BATCH_ID_WIDTH}d}"


def temp_dir_name(batch_id: int) -> str:
    """Return the folder name a batch is written under before it is complete, e.g. batch_00007.tmp."""
    return batch_dir_name(batch_id) + TEMP_SUFFIX


def parse_batch_id(name: str) -> int | None:
    """Return the batch id of a finished-batch folder name, or None for any other name.

    Only an exact match counts: batch_00063 gives 63, while batch_00063.tmp, batch_63 and
    unrelated names give None. None is not an error: a landing folder may hold other entries.
    """
    match = _BATCH_DIR_PATTERN.fullmatch(name)
    return int(match.group(1)) if match else None


def table_file_name(table_name: str) -> str:
    """Return the Parquet file name of a table inside a batch folder, e.g. orders.parquet."""
    return f"{table_name}.parquet"


def check_metadata_columns(table_name: str, columns) -> None:
    """Raise if a table lacks any of the metadata columns.

    Raises:
        ValueError: names the table and the missing columns.
    """
    missing = [column for column in METADATA_COLUMNS if column not in columns]
    if missing:
        raise ValueError(f"{table_name} is missing metadata columns {missing}")


class TableSummary(BaseModel):
    """What the manifest records about one table file."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    row_count: int = Field(ge=0)
    file_name: str                 # equal to table_file_name(<table>)
    byte_size: int = Field(ge=0)


class Manifest(BaseModel):
    """The manifest of one batch: written last by the runner, validated on read by the loader.

    Fields:
        batch_id: the same number as in the folder name.
        window_start: inclusive start of the batch window, in UTC; None only for batch 0.
        window_end: exclusive end of the batch window, in UTC.
        seed: the simulator seed the batch was built with.
        tables: table name to its summary; every table has a file in the folder.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    batch_id: int = Field(ge=0, le=MAX_BATCH_ID)
    window_start: AwareDatetime | None
    window_end: AwareDatetime
    seed: int
    tables: dict[str, TableSummary]

    @model_validator(mode="after")
    def _check_rules(self) -> "Manifest":
        if (self.window_start is None) != (self.batch_id == 0):
            raise ValueError("window_start must be None for batch 0 and set for every other batch")
        if self.window_start is not None and self.window_start >= self.window_end:
            raise ValueError("window_start must be before window_end")
        for table_name, summary in self.tables.items():
            if summary.file_name != table_file_name(table_name):
                raise ValueError(f"{table_name}: file_name must be {table_file_name(table_name)!r}")
        return self


def read_manifest(batch_dir: Path) -> Manifest | None:
    """Return the validated manifest of a finished batch, or None if batch_dir is not one.

    Three outcomes:
        None:     the folder name is not a finished-batch name (e.g. a .tmp folder), or it has no
                  manifest yet. Neither is an error; the batch is simply not there.
        Manifest: a finished batch whose manifest is valid.
        Raises:   a finished-batch folder whose manifest is broken, or names a different batch.
                  That is corruption, and it must stop the caller instead of being skipped.

    Raises:
        pydantic.ValidationError: the manifest is not valid JSON or breaks the Manifest rules.
        ValueError: the manifest's batch_id differs from the folder name.
    """
    folder_batch_id = parse_batch_id(batch_dir.name)
    manifest_path = batch_dir / MANIFEST_NAME
    if folder_batch_id is None or not manifest_path.is_file():
        return None
    manifest = Manifest.model_validate_json(manifest_path.read_text())
    if manifest.batch_id != folder_batch_id:
        raise ValueError(f"{batch_dir} holds a manifest for batch {manifest.batch_id}")
    return manifest
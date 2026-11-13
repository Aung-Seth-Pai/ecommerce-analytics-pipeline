"""Tests for the landing contract: the names, rules and manifest shape shared by the runner and the loader.

The loader trusts this module completely, so every rule here is tested at its edges:
names that must parse and names that must not, manifests that must pass and ones that must fail,
and the three outcomes of read_manifest.
"""
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pydantic
import pytest

from ecom_pipeline import landing_contract


# ---------------------------------------------------------------------------------------------
# Folder and file names
# ---------------------------------------------------------------------------------------------

@pytest.mark.parametrize(
    "batch_id, expected",
    [
        pytest.param(0, "batch_00000", id="first batch"),
        pytest.param(7, "batch_00007", id="padded to five digits"),
        pytest.param(10000, "batch_10000", id="five digits, no padding"),
        pytest.param(99999, "batch_99999", id="largest id"),
    ],
)
def test_batch_dir_name_pads_the_id_to_five_digits(batch_id, expected):
    assert landing_contract.batch_dir_name(batch_id) == expected


@pytest.mark.parametrize("batch_id", [pytest.param(-1, id="negative"), pytest.param(100000, id="six digits")])
def test_batch_dir_name_raises_outside_the_five_digit_range(batch_id):
    with pytest.raises(ValueError, match="between 0 and 99999"):
        landing_contract.batch_dir_name(batch_id)


def test_temp_dir_name_adds_the_tmp_suffix():
    assert landing_contract.temp_dir_name(7) == "batch_00007.tmp"


@pytest.mark.parametrize(
    "name, expected",
    [
        pytest.param("batch_00000", 0, id="batch 0"),
        pytest.param("batch_00004", 4, id="leading zeros"),
        pytest.param("batch_00064", 64, id="two digits"),
        pytest.param("batch_10000", 10000, id="five significant digits"),
        pytest.param("batch_10001", 10001, id="any five digits"),
    ],
)
def test_parse_batch_id_reads_finished_batch_names(name, expected):
    assert landing_contract.parse_batch_id(name) == expected


@pytest.mark.parametrize(
    "name",
    [
        pytest.param("batch_00004.tmp", id="half-written batch"),
        pytest.param("batch_0004", id="four digits"),
        pytest.param("batch_000004", id="six digits"),
        pytest.param("batch_0000a", id="letter in the id"),
        pytest.param("BATCH_00004", id="wrong case"),
        pytest.param("old_batch_00004", id="prefix in the middle"),
        pytest.param("abc", id="unrelated name"),
        pytest.param("", id="empty name"),
    ],
)
def test_parse_batch_id_returns_none_for_any_other_name(name):
    assert landing_contract.parse_batch_id(name) is None


@pytest.mark.parametrize("batch_id", [0, 7, 99999])
def test_parse_batch_id_reads_back_what_batch_dir_name_builds(batch_id):
    assert landing_contract.parse_batch_id(landing_contract.batch_dir_name(batch_id)) == batch_id


def test_table_file_name_is_the_table_name_with_parquet_suffix():
    assert landing_contract.table_file_name("orders") == "orders.parquet"


# ---------------------------------------------------------------------------------------------
# Metadata columns
# ---------------------------------------------------------------------------------------------

@pytest.mark.parametrize(
    "columns",
    [
        pytest.param(["order_id", "_op", "_source_updated_at", "_seq"], id="list"),
        pytest.param(pd.Index(["order_id", "_op", "_source_updated_at", "_seq"]), id="DataFrame columns"),
    ],
)
def test_check_metadata_columns_accepts_a_table_with_all_of_them(columns):
    landing_contract.check_metadata_columns("orders", columns)   # passes when it raises nothing


@pytest.mark.parametrize("missing", landing_contract.METADATA_COLUMNS)
def test_check_metadata_columns_names_the_table_and_the_missing_column(missing):
    columns = [column for column in landing_contract.METADATA_COLUMNS if column != missing]
    with pytest.raises(ValueError, match=f"orders is missing metadata columns.*{missing}"):
        landing_contract.check_metadata_columns("orders", columns)


def test_check_metadata_columns_is_case_sensitive():
    with pytest.raises(ValueError, match="_op"):
        landing_contract.check_metadata_columns("orders", ["_OP", "_source_updated_at", "_seq"])


# ---------------------------------------------------------------------------------------------
# The Manifest model
# ---------------------------------------------------------------------------------------------

@pytest.fixture
def batch_0_manifest() -> dict:
    """A valid manifest for batch 0, shaped like the one the runner writes (two tables to keep it short)."""
    return {
        "batch_id": 0,
        "window_start": None,
        "window_end": "2019-02-19T00:00:00Z",
        "seed": 42,
        "tables": {
            "users": {"row_count": 1641, "file_name": "users.parquet", "byte_size": 209340},
            "orders": {"row_count": 22, "file_name": "orders.parquet", "byte_size": 8589},
        },
    }


@pytest.fixture
def daily_manifest(batch_0_manifest) -> dict:
    """A valid manifest for batch 1: the day after batch 0."""
    return {**batch_0_manifest, "batch_id": 1,
            "window_start": "2019-02-19T00:00:00Z", "window_end": "2019-02-20T00:00:00Z"}


def test_valid_manifests_pass(batch_0_manifest, daily_manifest):
    landing_contract.Manifest.model_validate(batch_0_manifest)
    landing_contract.Manifest.model_validate(daily_manifest)


def test_manifest_accepts_the_older_plus_zero_offset_format(batch_0_manifest):
    manifest = landing_contract.Manifest.model_validate({**batch_0_manifest, "window_end": "2019-02-19T00:00:00+00:00"})
    assert manifest.window_end == datetime(2019, 2, 19, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "change, error",
    [
        pytest.param({"window_start": "2019-02-18T00:00:00Z"}, "None for batch 0", id="batch 0 with a start"),
        pytest.param({"window_end": "2019-02-19T00:00:00"}, "timezone", id="time without a time zone"),
        pytest.param({"batch_id": -1}, "greater than or equal to 0", id="negative batch id"),
        pytest.param({"seed": "forty-two"}, "valid integer", id="seed not a number"),
        pytest.param({"extra": 1}, "Extra inputs", id="unknown field"),
        pytest.param({"tables": {"orders": {"row_count": 22, "file_name": "x.parquet", "byte_size": 1}}},
                     "file_name must be", id="file name not matching the table"),
        pytest.param({"tables": {"orders": {"row_count": -1, "file_name": "orders.parquet", "byte_size": 1}}},
                     "greater than or equal to 0", id="negative row count"),
    ],
)
def test_batch_0_manifest_breaking_a_rule_is_rejected(batch_0_manifest, change, error):
    with pytest.raises(pydantic.ValidationError, match=error):
        landing_contract.Manifest.model_validate({**batch_0_manifest, **change})


@pytest.mark.parametrize(
    "change, error",
    [
        pytest.param({"window_start": None}, "set for every other batch", id="daily batch without a start"),
        pytest.param({"window_start": "2019-02-20T00:00:00Z"}, "before window_end", id="start equals end"),
        pytest.param({"window_start": "2019-02-21T00:00:00Z"}, "before window_end", id="start after end"),
    ],
)
def test_daily_manifest_breaking_a_window_rule_is_rejected(daily_manifest, change, error):
    with pytest.raises(pydantic.ValidationError, match=error):
        landing_contract.Manifest.model_validate({**daily_manifest, **change})


# ---------------------------------------------------------------------------------------------
# read_manifest: three outcomes
# ---------------------------------------------------------------------------------------------

def make_folder(parent: Path, name: str, manifest_text: str | None) -> Path:
    """Create a folder, with a _manifest.json holding manifest_text unless it is None."""
    folder = parent / name
    folder.mkdir()
    if manifest_text is not None:
        (folder / landing_contract.MANIFEST_NAME).write_text(manifest_text)
    return folder


@pytest.mark.parametrize(
    "name, has_manifest",
    [
        pytest.param("batch_00001.tmp", True, id="half-written batch, even with a manifest"),
        pytest.param("batch_00001", False, id="finished name, no manifest yet"),
        pytest.param("scratch", True, id="not a batch folder"),
    ],
)
def test_read_manifest_returns_none_when_the_folder_is_not_a_finished_batch(tmp_path, daily_manifest, name, has_manifest):
    text = landing_contract.Manifest.model_validate(daily_manifest).model_dump_json() if has_manifest else None
    assert landing_contract.read_manifest(make_folder(tmp_path, name, text)) is None


def test_read_manifest_returns_the_validated_manifest(tmp_path, daily_manifest):
    text = landing_contract.Manifest.model_validate(daily_manifest).model_dump_json()
    manifest = landing_contract.read_manifest(make_folder(tmp_path, "batch_00001", text))
    assert manifest.batch_id == 1
    assert manifest.window_end == datetime(2019, 2, 20, tzinfo=timezone.utc)
    assert manifest.tables["orders"].row_count == 22


@pytest.mark.parametrize(
    "text",
    [
        pytest.param("{not json", id="not JSON"),
        pytest.param('{"batch_id": 1}', id="fields missing"),
    ],
)
def test_read_manifest_raises_on_a_broken_manifest(tmp_path, text):
    with pytest.raises(pydantic.ValidationError):
        landing_contract.read_manifest(make_folder(tmp_path, "batch_00001", text))


def test_read_manifest_raises_when_the_manifest_names_another_batch(tmp_path, batch_0_manifest):
    text = landing_contract.Manifest.model_validate(batch_0_manifest).model_dump_json()
    with pytest.raises(ValueError, match="manifest for batch 0"):
        landing_contract.read_manifest(make_folder(tmp_path, "batch_00003", text))


def test_a_manifest_written_as_json_reads_back_unchanged(tmp_path):
    original = landing_contract.Manifest(
        batch_id=1,
        window_start=datetime(2019, 2, 19, tzinfo=timezone.utc),
        window_end=datetime(2019, 2, 20, tzinfo=timezone.utc),
        seed=42,
        tables={"orders": landing_contract.TableSummary(row_count=3, file_name="orders.parquet", byte_size=900)},
    )
    folder = make_folder(tmp_path, "batch_00001", original.model_dump_json(indent=2))
    assert landing_contract.read_manifest(folder) == original
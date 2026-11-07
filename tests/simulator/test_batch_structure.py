"""Shape of a batch and consistency of the module-level config.
 
Which tables and columns come back, what the metadata columns look like in batch 0, and whether
TABLE_KEYS, TABLE_TIMESTAMPS, STATIC_TABLES and the status config agree with each other.
the values inside a batch are tested in test_batch_windows.py and test_source_updated_at.py.
"""

import pytest
import pandas as pd

from ecom_pipeline.generator import simulator

TABLE_NAMES = ['orders', 'order_items', 'products', 'users', 'events', 'inventory_items', 'distribution_centers']

# --- shape of batch 0 (the batch_result fixture) -------------------------------------------------
 
def test_returns_all_seven_tables(batch_result):
    assert set(batch_result.keys())==set(TABLE_NAMES)

@pytest.mark.parametrize("table_name",TABLE_NAMES)
def test_return_all_expected_columns(duckdb_conn, batch_result, table_name):
    assert list(batch_result[table_name].columns) == duckdb_conn.table(table_name).columns + ["_op", "_source_updated_at", "_seq"]

@pytest.mark.parametrize("table_name",TABLE_NAMES)
def test_op_column_return_expected_value(batch_result, table_name):
    assert (batch_result[table_name]["_op"] == "I").all()

@pytest.mark.parametrize("table_name",TABLE_NAMES)
def test_seq_column_is_not_duplicated(batch_result, table_name):
    assert batch_result[table_name]["_seq"].duplicated().sum() == 0

@pytest.mark.parametrize("table_name",TABLE_NAMES)
def test_source_updated_at_return_datatype(batch_result, table_name):
    assert batch_result[table_name]["_source_updated_at"].dtype == pd.DatetimeTZDtype(unit="us", tz="UTC")

# --- the module-level config agrees with itself --------------------------------------------------

def test_every_table_has_a_key_and_a_timestamp_entry():
    assert set(simulator.TABLE_KEYS) == set(simulator.TABLE_TIMESTAMPS)
 
def test_static_tables_are_known_tables():
    assert set(simulator.STATIC_TABLES) <= set(simulator.TABLE_KEYS)

@pytest.mark.parametrize("table_name", simulator.STATIC_TABLES)
def test_static_tables_have_no_replay_timestamps(table_name):
    # static tables are loaded once, so none of their columns drive a replay
    assert simulator.TABLE_TIMESTAMPS[table_name] == ()

@pytest.mark.parametrize("table_name", [t for t in simulator.TABLE_KEYS if t not in simulator.STATIC_TABLES])
def test_replayed_tables_have_at_least_one_timestamp(table_name):
    # without a timestamp every row would have a null _source_updated_at and be dropped
    assert len(simulator.TABLE_TIMESTAMPS[table_name]) > 0
 
def test_status_config_covers_the_same_tables():
    assert set(simulator.STATUS_COLUMN_BY_TABLE) == set(simulator.STATUS_MAPPING_BY_TABLE)
    assert set(simulator.STATUS_COLUMN_BY_TABLE) <= set(simulator.TABLE_KEYS)
 
@pytest.mark.parametrize("table_name", simulator.STATUS_COLUMN_BY_TABLE)
def test_status_mapping_only_uses_masked_timestamp_columns(table_name):
    # derive_status reads these columns after masking; one that was not masked would leak the future
    mapping = simulator.STATUS_MAPPING_BY_TABLE[table_name]
    assert set(mapping) <= set(simulator.TABLE_TIMESTAMPS[table_name])
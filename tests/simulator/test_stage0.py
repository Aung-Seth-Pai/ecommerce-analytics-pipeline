import pytest
import pandas as pd

TABLE_NAMES = ['orders', 'order_items', 'products', 'users', 'events', 'inventory_items', 'distribution_centers']

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
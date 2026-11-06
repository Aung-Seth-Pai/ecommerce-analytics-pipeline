"""Reading DuckDB tables into pandas: read_table and read_empty_table.
 
Checks that dtypes come from the schema (UTC timestamps, str text), whatever the connection's
time zone or the values in the rows, and that an empty read has exactly the same shape.
"""
import duckdb
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
from ecom_pipeline import simulator

UTC_US = pd.DatetimeTZDtype(unit="us", tz="UTC")
 
@pytest.fixture
def relation():
    """A small table read through a connection set to a non-UTC time zone.
 
    Rows are inserted out of key order (3, 1, 2). The text column note is null in every row.
    created_at is stored as 08:00 UTC, which is 12:00 in Asia/Dubai.
    """
    connection = duckdb.connect()
    connection.execute("SET TimeZone = 'Asia/Dubai'")
    connection.execute("CREATE TABLE t (id INTEGER, name VARCHAR, note VARCHAR, created_at TIMESTAMPTZ)")
    connection.execute("""
        INSERT INTO t VALUES
            (3, 'c', NULL, '2026-01-01 08:00:00+00'),
            (1, 'a', NULL, '2026-01-01 08:00:00+00'),
            (2, NULL, NULL, NULL)
    """)
    yield connection.table("t")
    connection.close()

def test_read_table_sorts_by_the_key_ascending(relation):
    result = simulator.read_table(relation, order_by="id")
    assert list(result["id"]) == [1, 2, 3]
 
def test_read_table_returns_timestamps_in_utc_whatever_the_connection_time_zone(relation):
    result = simulator.read_table(relation, order_by="id")
    assert result["created_at"].dtype == UTC_US
    # same instant, now shown in UTC: 08:00 and not the connection's 12:00
    assert result["created_at"].iloc[0] == pd.Timestamp("2026-01-01 08:00", tz="UTC")
 
def test_read_table_returns_text_columns_as_str_even_when_all_null(relation):
    result = simulator.read_table(relation, order_by="id")
    assert str(result["name"].dtype) == "str"
    assert str(result["note"].dtype) == "str"
 
def test_read_empty_table_has_no_rows_and_the_same_columns_and_dtypes(relation):
    full = simulator.read_table(relation, order_by="id")
    empty = simulator.read_empty_table(relation)
    assert len(empty) == 0
    assert list(empty.columns) == list(full.columns)
    assert empty.dtypes.equals(full.dtypes)
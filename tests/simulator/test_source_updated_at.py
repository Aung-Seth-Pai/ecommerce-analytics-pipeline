import pytest
import pandas as pd
from pandas.testing import assert_series_equal, assert_frame_equal
from ecom_pipeline import simulator

UTC_US = pd.DatetimeTZDtype(unit="us", tz="UTC")
def utc(*values):
    """Build a UTC microsecond timestamp Series from strings; None becomes NaT."""
    return pd.Series(pd.to_datetime(list(values), utc=True)).astype(UTC_US)

def test_latest_timestamp_picks_the_max_and_ignore_nulls():
    sample_data = pd.DataFrame({
        "a": utc("2026-01-03 10:00",    "2026-01-03 12:00",     None),
        "b": utc(None,                  "2026-01-04 09:00",     "2026-01-05 14:00"),
    })
    expected = utc("2026-01-03 10:00", "2026-01-04 09:00", "2026-01-05 14:00")
    result_series = simulator.get_latest_timestamp(sample_data, ("a","b"))
    assert_series_equal(result_series, expected)

def test_latest_timestamp_is_null_for_an_empty_column_list():
    sample_data = pd.DataFrame({
        "a": utc("2026-01-03 10:00",    "2026-01-03 12:00",     None),
        "b": utc(None,                  "2026-01-04 09:00",     "2026-01-05 14:00"),
    })
    expected = utc(None, None, None)
    result_series = simulator.get_latest_timestamp(sample_data, ())
    assert_series_equal(result_series, expected)

def test_latest_timestamp_does_not_modify_the_input():
    sample_data = pd.DataFrame({
        "a": utc("2026-01-03 10:00",    "2026-01-03 12:00",     None),
        "b": utc(None,                  "2026-01-04 09:00",     "2026-01-05 14:00"),
    })
    before = sample_data.copy()
    simulator.get_latest_timestamp(sample_data, ("a", "b"))
    assert_frame_equal(sample_data, before)

def test_latest_timestamp_is_null_when_all_columns_are_null():
    sample_data = pd.DataFrame({
        "a": utc(None),
        "b": utc(None)
    })
    expected = utc(None)
    result_series = simulator.get_latest_timestamp(sample_data, ("a","b"))
    assert_series_equal(result_series, expected)

def test_latest_timestamp_ignores_the_order_of_the_columns_argument():
    sample_data = pd.DataFrame({
        "a": utc("2026-01-03 10:00",    "2026-01-03 12:00",     None),
        "b": utc(None,                  "2026-01-04 09:00",     "2026-01-05 14:00"),
    })
    expected = utc("2026-01-03 10:00", "2026-01-04 09:00", "2026-01-05 14:00")
    result_series = simulator.get_latest_timestamp(sample_data, ("b","a"))
    assert_series_equal(result_series, expected)

EXPECTED_SOURCE_UPDATED_AT = {
    "orders":               utc("2026-01-03 10:00", "2026-01-04 09:00", "2026-01-05 14:00", "2026-01-03 09:00"),
    "order_items":          utc("2026-01-03 10:00", "2026-01-04 09:00", "2026-01-05 14:00", "2026-01-05 14:00", "2026-01-03 09:00"),
    "users":                utc("2026-01-01 08:00", "2026-01-02 09:00"),
    "events":               utc("2026-01-01 08:05", "2026-01-02 10:00", "2026-01-02 14:50"),
    "inventory_items":      utc("2025-12-01 10:00", "2025-12-01 10:00", "2025-12-01 10:00", "2025-12-01 10:00", "2025-12-01 10:00"),
    "products":             utc(None, None),
    "distribution_centers": utc(None, None),
}
@pytest.mark.parametrize("table_name", EXPECTED_SOURCE_UPDATED_AT)
def test_source_updated_at_return_expected_data(batch_result, table_name):
    result_series = batch_result[table_name]["_source_updated_at"]
    assert_series_equal(result_series, EXPECTED_SOURCE_UPDATED_AT[table_name], check_names=False)

def test_order_item_latest_timestamp_combines_own_timestamps_with_order_created_at():
    orders = pd.DataFrame({
        "order_id":   [1, 2],
        "created_at": utc("2026-01-03 10:00", "2026-01-03 12:00"),
    })
    order_items = pd.DataFrame({
        "order_id":     [1,                 2,                  2,                  99,                 99],
        "shipped_at":   utc(None,           "2026-01-04 09:00", "2026-01-04 09:00", "2026-01-05 00:00", None),
        "delivered_at": utc(None,           None,               "2026-01-06 12:00", None,               None),
    })
    expected = utc(
        "2026-01-03 10:00",   # row 1: no timestamps of its own, so the order's created_at
        "2026-01-04 09:00",   # row 2: own shipped_at is later than the order's created_at
        "2026-01-06 12:00",   # row 3: delivered_at is the latest of the item's own columns
        "2026-01-05 00:00",   # row 4: orphan item (order 99), falls back to its own timestamp
        None,                 # row 5: orphan item with nothing at all, so null
    )
    result_series = simulator.get_order_item_latest_timestamp(
        order_items, orders, ("shipped_at", "delivered_at")
    )
    assert_series_equal(result_series, expected)


def test_get_order_item_latest_timestamp_does_not_modify_the_input():
    orders = pd.DataFrame({
        "order_id":   [1, 2],
        "created_at": utc("2026-01-03 10:00", "2026-01-03 12:00"),
    })
    order_items = pd.DataFrame({
        "order_id":     [1,                 2,                  2,                  99,                 99],
        "shipped_at":   utc(None,           "2026-01-04 09:00", "2026-01-04 09:00", "2026-01-05 00:00", None),
        "delivered_at": utc(None,           None,               "2026-01-06 12:00", None,               None),
    })
    orders_before = orders.copy()
    order_items_before = order_items.copy()
    simulator.get_order_item_latest_timestamp(order_items, orders, ("shipped_at", "delivered_at"))
    assert_frame_equal(orders, orders_before)
    assert_frame_equal(order_items, order_items_before)
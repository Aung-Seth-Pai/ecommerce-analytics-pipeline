import pandas as pd
from pandas.testing import assert_series_equal, assert_frame_equal
from ecom_pipeline import simulator

TABLE_NAMES = ['orders', 'order_items', 'products', 'users', 'events', 'inventory_items', 'distribution_centers']

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
    assert_series_equal(expected, result_series)
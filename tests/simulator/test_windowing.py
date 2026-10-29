import pytest
from ecom_pipeline import simulator
import pandas as pd
from pandas.testing import assert_frame_equal

UTC_US = pd.DatetimeTZDtype(unit="us", tz="UTC")
def utc(*values):
    """Build a UTC microsecond timestamp Series from strings; None becomes NaT."""
    return pd.Series(pd.to_datetime(list(values), utc=True)).astype(UTC_US)

@pytest.fixture
def masking_sample_dataframe():
    """Four rows around a Jan 4 cut-off: a and b are the columns to mask, c must never change.

    Row 0: a is before the cut-off (kept), b is after it (hidden)
    Row 1: a is exactly at the cut-off (hidden), b is null (stays null)
    Row 2: a is after the cut-off (hidden), b is before it (kept)
    Row 3: a is null (stays null), b is one minute before the cut-off (kept)
    c is always after the cut-off, but it is not a column to mask, so it never changes.
    """
    return pd.DataFrame({
        "a": utc("2026-01-03 08:00", "2026-01-04 00:00", "2026-01-05 14:00", None),
        "b": utc("2026-01-05 14:00", None,               "2026-01-03 12:00", "2026-01-03 23:59"),
        "c": utc("2026-01-05 14:00", "2026-01-05 14:00", "2026-01-05 14:00", "2026-01-05 14:00"),
    })

def test_mask_future_timestamps_hides_timestamps_at_or_after_the_cutoff(masking_sample_dataframe):
    # a and b are masked, c is not in timestamp_cols and must come back unchanged
    timestamp_cols = ("a", "b")
    cutoff = pd.Timestamp(2026, 1, 4, tz="UTC")
    expected = pd.DataFrame({
        "a": utc("2026-01-03 08:00", None,               None,               None),
        "b": utc(None,               None,               "2026-01-03 12:00", "2026-01-03 23:59"),
        "c": utc("2026-01-05 14:00", "2026-01-05 14:00", "2026-01-05 14:00", "2026-01-05 14:00"),
    })
    result_dataframe = simulator.mask_future_timestamps(masking_sample_dataframe, timestamp_cols, cutoff)
    assert_frame_equal(result_dataframe, expected)

def test_mask_future_timestamps_leaves_the_table_unchanged_for_an_empty_column_list(masking_sample_dataframe):
    # no columns masked so table must come back unchanged
    timestamp_cols = ()
    cutoff = pd.Timestamp(2026, 1, 4, tz="UTC")
    result_dataframe = simulator.mask_future_timestamps(masking_sample_dataframe, timestamp_cols, cutoff)
    assert_frame_equal(result_dataframe, masking_sample_dataframe)

def test_mask_future_timestamps_does_not_modify_the_input(masking_sample_dataframe):
    # a and b are masked, c is not in timestamp_cols and must come back unchanged
    sample_dataframe_backup = masking_sample_dataframe.copy()
    timestamp_cols = ("a", "b")
    cutoff = pd.Timestamp(2026, 1, 4, tz="UTC")
    simulator.mask_future_timestamps(masking_sample_dataframe, timestamp_cols, cutoff)
    assert_frame_equal(masking_sample_dataframe, sample_dataframe_backup)
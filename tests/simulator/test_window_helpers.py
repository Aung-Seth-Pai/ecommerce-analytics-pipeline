"""Helper functions of the windowed replay, tested on their own with tiny hand-made data.

Hiding future timestamps (mask_future_timestamps), deriving status from the timestamps that
survive (derive_status) and deciding which rows fall inside a window (is_written_in_window).
Whole batches are tested in test_batch_windows.py.
"""

import pytest
from ecom_pipeline.generator import simulator
import pandas as pd
from pandas.testing import assert_frame_equal, assert_index_equal, assert_series_equal

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

def test_derive_status_uses_the_highest_precedence_timestamp_present():
    # status is the original status: it must never leak into the result
    sample_df = pd.DataFrame({
        "status":       ["Processing", "Complete", "Complete", "Returned", "Complete", "Cancelled"],
        "created_at":   utc("2026-01-03 10:00", "2026-01-03 10:00", "2026-01-03 10:00", "2026-01-03 10:00", "2026-01-03 10:00", None),
        "shipped_at":   utc(None,               "2026-01-04 09:00", "2026-01-04 09:00", "2026-01-04 09:00", "2026-01-03 10:00", None),
        "delivered_at": utc(None,               None,               "2026-01-05 14:00", "2026-01-05 14:00", None,               None),
        "returned_at":  utc(None,               None,               None,               "2026-01-06 12:00", None,               None),
    })
    # rows: created only, shipped, delivered, returned, tie (created == shipped), no timestamps at all
    expected = pd.Series(["Processing", "Shipped", "Complete", "Returned", "Shipped", None])
    result = simulator.derive_status(sample_df, 'status',  simulator.ORDER_STATUS_MAPPING, ['Cancelled'])
    assert_series_equal(result, expected)

def test_derive_status_return_same_index_as_input():
    # a non-default index: a function that reset it would still pass a length-only check
    sample_df = pd.DataFrame({
        "status":       ["Processing", "Complete", "Cancelled"],
        "created_at":   utc("2026-01-03 10:00", "2026-01-03 10:00", None),
        "shipped_at":   utc(None,               "2026-01-04 09:00", None),
        "delivered_at": utc(None,               None,               None),
        "returned_at":  utc(None,               None,               None),
    }).set_axis([5, 3, 9])   # set the index afterwards: passing index= to the constructor would misalign the Series
    result = simulator.derive_status(sample_df, 'status', simulator.ORDER_STATUS_MAPPING, ['Cancelled'])
    assert len(result) == len(sample_df)
    assert_index_equal(result.index, sample_df.index)

def test_derive_status_raises_when_a_mapped_column_is_missingF():
    # the mapping names shipped_at, delivered_at and returned_at, which this table does not have
    sample_df = pd.DataFrame({
        "status":     ["Processing"],
        "created_at": utc("2026-01-03 10:00"),
    })
    with pytest.raises(KeyError):
        simulator.derive_status(sample_df, "status", simulator.ORDER_STATUS_MAPPING, ["Cancelled"])

def test_derive_status_works_without_a_status_column_when_there_are_no_passthrough_statuses():
    # no passthrough statuses, so the original status is never read and the table may lack a status column
    # rows: created_at present (Processing), no timestamps at all (null)
    sample_df = pd.DataFrame({
        "created_at":   utc("2026-01-03 10:00", None),
        "shipped_at":   utc(None,               None),
        "delivered_at": utc(None,               None),
        "returned_at":  utc(None,               None),
    })
    expected = pd.Series(["Processing", None])
    result = simulator.derive_status(sample_df, "status", simulator.ORDER_STATUS_MAPPING, [])
    assert_series_equal(result, expected)

def test_derive_status_works_with_a_one_column_mapping():
    # the same function serves tables with a single timestamp, given its own mapping
    sample_df = pd.DataFrame({
        "created_at": utc("2026-01-03 10:00", None),
    })
    expected = pd.Series(["Inserted", None])
    result = simulator.derive_status(sample_df, "status", {"created_at": "Inserted"}, [])
    assert_series_equal(result, expected)

def test_derive_status_does_not_modify_the_input():
    sample_df = pd.DataFrame({
        "status":       ["Complete", "Cancelled"],
        "created_at":   utc("2026-01-03 10:00", "2026-01-03 10:00"),
        "shipped_at":   utc("2026-01-04 09:00", None),
        "delivered_at": utc(None,               None),
        "returned_at":  utc(None,               None),
    })
    before = sample_df.copy()
    simulator.derive_status(sample_df, "status", simulator.ORDER_STATUS_MAPPING, ["Cancelled"])
    assert_frame_equal(sample_df, before)


@pytest.mark.parametrize("cutoff, expected_status", [
    (pd.Timestamp(2026, 1, 3, tz="UTC"), "Processing"),   # only created_at is before the cutoff
    (pd.Timestamp(2026, 1, 4, tz="UTC"), "Shipped"),      # shipped_at is before it, delivered_at is not
    (pd.Timestamp(2026, 1, 6, tz="UTC"), "Complete"),     # everything has happened
])
def test_status_follows_the_cutoff(cutoff, expected_status):
    order = pd.DataFrame({
        "status":       ["Complete"],   # the original status; masking must not let it leak through
        "created_at":   utc("2026-01-02 15:00"),
        "shipped_at":   utc("2026-01-03 08:00"),
        "delivered_at": utc("2026-01-05 14:00"),
        "returned_at":  utc(None),
    })
    timestamp_cols = ("created_at", "shipped_at", "delivered_at", "returned_at")
    masked = simulator.mask_future_timestamps(order, timestamp_cols, cutoff)
    assert_series_equal(simulator.derive_status(masked, 'status', simulator.ORDER_STATUS_MAPPING, ["Cancelled"]), pd.Series([expected_status]))

def test_cancelled_status_passes_through_while_the_order_exists():
    sample_df = pd.DataFrame({
        "status":       ["Cancelled",       "Cancelled", "Complete",         "Cancelled"],
        "created_at":   utc("2026-01-03 10:00", None,     "2026-01-03 10:00", "2026-01-03 10:00"),
        "shipped_at":   utc(None,               None,     None,               "2026-01-04 09:00"),
        "delivered_at": utc(None,               None,     None,               None),
        "returned_at":  utc(None,               None,     None,               None),
    })
    # rows: cancelled with only created_at (passes through), cancelled with no timestamps left
    # (order not created yet, so null), complete with only created_at (not a passthrough
    # status, so derived: Processing), cancelled with a later timestamp (passthrough wins)
    expected = pd.Series(["Cancelled", None, "Processing", "Cancelled"])
    result = simulator.derive_status(sample_df, "status", simulator.ORDER_STATUS_MAPPING, ["Cancelled"])
    assert_series_equal(result, expected)

 
JAN_4 = pd.Timestamp(2026, 1, 4, tz="UTC")
JAN_5 = pd.Timestamp(2026, 1, 5, tz="UTC")
@pytest.fixture
def written_at():
    """Six _source_updated_at values around a window of Jan 4 to Jan 5.
 
    Row 0: one minute before Jan 4 (before the window)
    Row 1: exactly Jan 4 00:00 (the start, which is inclusive)
    Row 2: inside the window
    Row 3: exactly Jan 5 00:00 (the end, which is exclusive)
    Row 4: after the window
    Row 5: null (never inside any window)
    """
    return utc("2026-01-03 23:59", "2026-01-04 00:00", "2026-01-04 09:00",
               "2026-01-05 00:00", "2026-01-05 10:00", None)

@pytest.mark.parametrize("window_start, window_end, expected", [
    (JAN_4, JAN_5, [False, True, True, False, False, False]),   # both bounds: start inclusive, end exclusive
    (None,  JAN_5, [True,  True, True, False, False, False]),   # no lower bound (batch 0)
    (JAN_4, None,  [False, True, True, True,  True,  False]),   # no upper bound
    (None,  None,  [True,  True, True, True,  True,  False]),   # no bounds: everything but the null
], ids=["both-bounds", "no-lower-bound", "no-upper-bound", "no-bounds"])
def test_is_written_in_window_keeps_rows_written_inside_the_window(written_at, window_start, window_end, expected):
    result = simulator.is_written_in_window(written_at, window_start, window_end)
    assert_series_equal(result, pd.Series(expected))

def test_is_written_in_window_keeps_the_index_of_the_input():
    written_at = utc("2026-01-04 09:00", "2026-01-06 09:00", None).set_axis([7, 3, 9])
    expected = pd.Series([True, False, False], index=[7, 3, 9])
    result = simulator.is_written_in_window(written_at, JAN_4, JAN_5)
    assert_series_equal(result, expected)

def test_is_written_in_window_keeps_nothing_when_start_equals_end(written_at):
    # a half-open window [Jan 4, Jan 4) is empty
    result = simulator.is_written_in_window(written_at, JAN_4, JAN_4)
    assert_series_equal(result, pd.Series([False] * 6))

def test_is_written_in_window_does_not_modify_the_input(written_at):
    before = written_at.copy()
    simulator.is_written_in_window(written_at, JAN_4, JAN_5)
    assert_series_equal(written_at, before)
"""Windowed batches through simulate_batch, on the fixture data in conftest.py.
 
Which rows each daily batch holds and with what status and _op, how the window edges behave,
that batches are independent of each other, and that replaying every batch gives back the snapshot.
 
The fixture data, by day (all UTC, 2026):
    Jan 1  user 1 and event 1 written
    Jan 2  user 2 and events 2, 3 written; order 3 and its items 3, 4 created
    Jan 3  orders 1, 2, 4 created (4 is Cancelled); order 3 shipped (items 3, 4 too)
    Jan 4  order 2 shipped (item 2 too)
    Jan 5  order 3 delivered (items 3, 4 too)
"""
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
from ecom_pipeline.generator import simulator

METADATA_COLUMNS = ["_op", "_source_updated_at", "_seq"]

def day(number: int) -> pd.Timestamp:
    """Midnight UTC of January <number>, 2026."""
    return pd.Timestamp(2026, 1, number, tz="UTC")

def rows_of(table: pd.DataFrame, key: str):
    """The (key, status, _op) of every row, as tuples, so a test can compare them in one line."""
    return list(zip(table[key], table["status"], table["_op"]))

def replay(simulate):
    """Run batch 0 (everything before Jan 3), then one daily batch for Jan 3 to Jan 9.
 
    Returns each table's batches stacked in the order they arrived.
    """
    batches = [simulate(None, day(3))] + [simulate(day(d), day(d + 1)) for d in range(3, 10)]
    return {table: pd.concat([batch[table] for batch in batches], ignore_index=True) for table in batches[0]}

# --- what each daily batch holds ------

# first day of the window; orders and order_items as (key, status, _op); user ids; event ids
DAILY_BATCHES = [
    pytest.param(1, [], [], [1], [1], id="jan-01"),
    pytest.param(2,
        [(3, "Processing", "I")],
        [(3, "Processing", "I"), (4, "Processing", "I")],
        [2], [2, 3], id="jan-02"),
    pytest.param(3,
        [(1, "Processing", "I"), (2, "Processing", "I"), (3, "Shipped", "U"), (4, "Cancelled", "I")],
        [(1, "Processing", "I"), (2, "Processing", "I"), (3, "Shipped", "U"), (4, "Shipped", "U"), (5, "Cancelled", "I")],
        [], [], id="jan-03"),
    pytest.param(4, [(2, "Shipped", "U")], [(2, "Shipped", "U")], [], [], id="jan-04"),
    pytest.param(5,
        [(3, "Complete", "U")],
        [(3, "Complete", "U"), (4, "Complete", "U")],
        [], [], id="jan-05"),
    pytest.param(6, [], [], [], [], id="jan-06"),
]

@pytest.mark.parametrize("first_day, orders, order_items, user_ids, event_ids", DAILY_BATCHES)
def test_daily_batch_holds_the_rows_written_that_day(simulate, first_day, orders, order_items, user_ids, event_ids):
    batch = simulate(day(first_day), day(first_day + 1))
    assert rows_of(batch["orders"], "order_id") == orders
    assert rows_of(batch["order_items"], "id") == order_items
    assert list(batch["users"]["id"]) == user_ids
    assert list(batch["events"]["id"]) == event_ids


def test_window_start_is_inclusive_and_window_end_is_exclusive(simulate):
    # order 1 is created at exactly 10:00 (the start: in), order 2 at exactly 12:00 (the end: out),
    # order 4 at 09:00 (before the window: out)
    batch = simulate(pd.Timestamp("2026-01-03 10:00", tz="UTC"), pd.Timestamp("2026-01-03 12:00", tz="UTC"))
    assert rows_of(batch["orders"], "order_id") == [(1, "Processing", "I")]
 
 
def test_seq_counts_from_one_without_gaps_after_filtering(simulate):
    batch = simulate(day(3), day(4))
    for table in simulator.TABLE_KEYS:
        assert list(batch[table]["_seq"]) == list(range(1, len(batch[table]) + 1))
 
 
# --- static tables and empty batches -------------------------------------------------------------
 
@pytest.mark.parametrize("table_name", simulator.STATIC_TABLES)
def test_static_tables_are_only_in_batch_0(simulate, batch_result, table_name):
    assert len(batch_result[table_name]) > 0
    assert len(simulate(day(3), day(4))[table_name]) == 0
 
 
@pytest.mark.parametrize("table_name", simulator.TABLE_KEYS)
def test_an_empty_batch_keeps_the_columns_and_dtypes_of_batch_0(simulate, batch_result, table_name):
    empty_batch = simulate(day(6), day(7))   # nothing was written on Jan 6
    assert len(empty_batch[table_name]) == 0
    assert list(empty_batch[table_name].columns) == list(batch_result[table_name].columns)
    assert empty_batch[table_name].dtypes.equals(batch_result[table_name].dtypes)
 
 
# --- batches do not depend on each other ---------------------------------------------------------
 
def test_the_same_call_twice_gives_identical_batches(simulate):
    first = simulate(day(3), day(4))
    second = simulate(day(3), day(4))
    for table in first:
        assert_frame_equal(first[table], second[table])
 
def test_a_batch_does_not_depend_on_batches_built_before_it(simulate):
    alone = simulate(day(4), day(5))
    simulate(None, day(3))          # build other batches first, on the same connection
    simulate(day(3), day(4))
    after_others = simulate(day(4), day(5))
    for table in alone:
        assert_frame_equal(alone[table], after_others[table])

# --- replaying every batch -----------------------------------------------------------------------
 
@pytest.mark.parametrize("table_name", simulator.TABLE_KEYS)
def test_replaying_every_batch_and_keeping_the_latest_row_gives_back_the_snapshot(duckdb_conn, simulate, table_name):
    key = simulator.TABLE_KEYS[table_name]
    # versions arrive in time order, so the last arrival of a key is its latest version
    latest = (
        replay(simulate)[table_name]
        .drop_duplicates(subset=key, keep="last")
        .drop(columns=METADATA_COLUMNS)
        .sort_values(key)
        .reset_index(drop=True)
    )
    snapshot = simulator.read_table(duckdb_conn.table(table_name), order_by=key)
    assert_frame_equal(latest, snapshot)
 
 
@pytest.mark.parametrize("table_name", simulator.TABLE_KEYS)
def test_replay_delivers_each_version_only_once(simulate, table_name):
    # two rows with the same key and the same _source_updated_at would be the same version sent twice
    key = simulator.TABLE_KEYS[table_name]
    replayed = replay(simulate)[table_name]
    assert not replayed.duplicated(subset=[key, "_source_updated_at"]).any()
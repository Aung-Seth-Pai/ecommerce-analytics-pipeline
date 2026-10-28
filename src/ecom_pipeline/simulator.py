import pandas as pd
import duckdb
from collections.abc import Sequence

# maps each table to the key column which will be used to order its rows
TABLE_KEYS = {
    "users":"id",
    "orders":"order_id",
    "order_items":"id",
    "products":"id",
    "inventory_items":"id",
    "distribution_centers":"id",
    "events":"id"
}

TABLE_TIMESTAMPS = {
    "users":("created_at",),
    "orders":("created_at", "shipped_at", "delivered_at", "returned_at"),
    "order_items":("shipped_at", "delivered_at", "returned_at"),
    "inventory_items":("created_at",),
    "events":("created_at",),
    "products":(),
    "distribution_centers":(),
}

TIMESTAMP_DTYPE = pd.DatetimeTZDtype(unit="us", tz="UTC")

def get_order_item_latest_timestamp(order_items: pd.DataFrame, orders: pd.DataFrame, timestamp_columns: Sequence[str]) -> pd.Series:
    """Return, per order item, the latest of its own timestamps and its order's created_at (nulls ignored).

    The thelook order_items.created_at is synthetic and unrelated to the order
    (EDA: it differs from the order's on almost every row), so it is not used.
    The order's created_at stands in for when the item row was first written in the source.

    An item whose order_id is not in orders gets no order timestamp, so
    only its own timestamps count. If it has none, the result is NaT.
    Neither input is modified.

    Args:
        order_items: Item rows. Needs an order_id column and the timestamp columns.
        orders: Order rows, one per order_id. Needs order_id and created_at.
        timestamp_columns: The item's own timestamp columns to include,
            e.g. shipped_at, delivered_at, returned_at.

    Returns:
        A datetime64[us, UTC] Series with the same index as order_items.
    """
    order_created_at_by_order_id = orders.set_index("order_id")["created_at"]
    items_with_order_created_at = order_items.assign(order_created_at=order_items['order_id'].map(order_created_at_by_order_id))

    effective_cols = (*timestamp_columns, "order_created_at")
    return get_latest_timestamp(items_with_order_created_at, effective_cols)

def get_latest_timestamp(input_dataframe: pd.DataFrame, timestamp_columns: Sequence[str]) -> pd.Series:
    """Return, per row, the latest of the given timestamp columns (nulls ignored).
        A row whose columns are all null, or an empty column list, gives NaT.
        The input is not modified.
    """
    cols = list(timestamp_columns)
    if not cols:
        return pd.Series(pd.NaT, index=input_dataframe.index, dtype=TIMESTAMP_DTYPE)
    return input_dataframe[cols].max(axis=1).astype(TIMESTAMP_DTYPE)

def add_metadata_columns(input_dataframe: pd.DataFrame, source_updated_at: pd.Series) -> pd.DataFrame:
    """Return a copy of the table with _op, _source_updated_at and _seq appended.
        The input is not modified. _seq numbers the rows in their current order,
        so the caller must read them ordered by key.
    """
    return input_dataframe.assign(
        _op = "I",
        _source_updated_at = source_updated_at,
        _seq = range(1, len(input_dataframe)+1)
    )

def simulate_batch(
        database_connection: duckdb.DuckDBPyConnection,
        window_start: pd.Timestamp | None=None,
        window_end: pd.Timestamp | None=None,
        seed: int=42
) -> dict[str, pd.DataFrame]:
    """Return one slice of the thelook source as a dictionary of DataFrames.

    Replays the frozen snapshot. As for this stage, it ignores the window, seed and returns every table 100% as batch 0. 
    Later stages return only rows that changed inside the window, in the state they had at window_end.

    Each DataFrame has the source columns unchanged, followed by three metadata columns:
        _op: "I" for the first version of a row, "U" for a later version, "D" for a delete (deletes arrive in a later phase).
        _source_updated_at: when this version of the row was written in the
            source, i.e. the latest lifecycle timestamp that had happened by window_end. 
            null only for tables with no timestamps (products, distribution_centers).
        _seq: row number within the table, starting at 1. Rows are ordered by their keys in ascending order. 
            It is the final tiebreak between versions of the same key.
    The loader adds _batch_id, _batch_date and _loaded_at. This function only reads and writes nothing.

    Args:
        database_connection: Open DuckDB connection to the snapshot. 
            The caller opens and closes it.
        window_start: Start of the batch window, timezone-aware (UTC), inclusive.
            None means no lower bound (for batch 0).
        window_end: End of the window, timezone-aware (UTC), exclusive.
        seed: Integer seed for the randomised quirks added in later phases.

    Returns:
        A dictionary mapping each of the seven table names to its DataFrame.
    """
    
    data_tables: dict[str, pd.DataFrame] = {}
    for table, key in TABLE_KEYS.items():
        rel = database_connection.table(table)
        sort_key = f'"{key}" ASC'
        data_tables[table] = rel.order(sort_key).df()

        timestamp_cols = TABLE_TIMESTAMPS[table]
        # order_items needs the order's created_at, so orders must already be built:
        # keep "orders" before "order_items" in TABLE_KEYS.
        if table == "order_items":
            latest_timestamp = get_order_item_latest_timestamp(data_tables['order_items'], data_tables['orders'], timestamp_cols)
        else:
            latest_timestamp = get_latest_timestamp(data_tables[table], timestamp_cols)
        data_tables[table] = add_metadata_columns(data_tables[table], latest_timestamp)

    # return the data object
    return data_tables
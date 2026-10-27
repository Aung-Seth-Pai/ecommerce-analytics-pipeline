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
        latest_timestamp = get_latest_timestamp(data_tables[table], timestamp_cols) 
        data_tables[table] = add_metadata_columns(data_tables[table], latest_timestamp)

    # return the data object
    return data_tables
import pandas as pd
import duckdb
from collections.abc import Sequence, Mapping, Iterator

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

# lifecycle timestamp columns per table: used for masking and for _source_updated_at.
# since order_items.created_at is synthetic and unrelated to the order, we use the created_at column from order data instead as added by attach_order_created_at.
# static tables (inventory_items, products, distribution_centers) have no replay timestamps:
# they are loaded once in batch 0 and their _source_updated_at is null.
# inventory_items.created_at is unrelated to the orders (EDA), so it is not used.
TABLE_TIMESTAMPS = {
    "users":("created_at",),
    "orders":("created_at", "shipped_at", "delivered_at", "returned_at"),
    "order_items":("order_created_at", "shipped_at", "delivered_at", "returned_at"),
    "inventory_items":(),
    "events":("created_at",),
    "products":(),
    "distribution_centers":(),
}
STATIC_TABLES = ("inventory_items", "products", "distribution_centers")

# the column that says when a row was created: it decides _op (I if created inside the window, else U).
# order_items uses the parent order's created_at (the working column), because its own created_at is synthetic.
CREATION_COLUMN_BY_TABLE = {
    "users":       "created_at",
    "orders":      "created_at",
    "order_items": "order_created_at",
    "events":      "created_at",
}


# status from timestamps, highest precedence first: the first timestamp still present decides the status.
# orders and order items differ only in where "Processing" comes from: the order's own created_at, or
# (for items) the parent order's created_at.
ORDER_STATUS_MAPPING = {
    "returned_at":  "Returned",
    "delivered_at": "Complete",
    "shipped_at":   "Shipped",
    "created_at":   "Processing",
}
ORDER_ITEM_STATUS_MAPPING = {
    "returned_at":      "Returned",
    "delivered_at":     "Complete",
    "shipped_at":       "Shipped",
    "order_created_at": "Processing",
}
STATUS_MAPPING_BY_TABLE = {
    "orders":      ORDER_STATUS_MAPPING,
    "order_items": ORDER_ITEM_STATUS_MAPPING,
}
# source data has no timestamp for Cancelled (we do not know when it happened), so no timestamp can imply it.
# therefore it is not used to derive status. It is kept as-is for rows that exist at the cutoff.
PASSTHROUGH_STATUSES = ("Cancelled",) 
STATUS_COLUMN_BY_TABLE = {
    "orders": "status",
    "order_items": "status",
}
TIMESTAMP_DTYPE = pd.DatetimeTZDtype(unit="us", tz="UTC")

def mask_future_timestamps(
        input_dataframe: pd.DataFrame, 
        timestamp_columns: Sequence[str], 
        cutoff: pd.Timestamp
) -> pd.DataFrame:
    """Return a copy of the table with every timestamp at or after the cutoff set to null.

    This reproduces what the source knew at the cutoff: anything that had not
    happened yet is hidden. The cutoff itself counts as not yet happened,
    because windows are half-open (window_end is exclusive).
    Columns outside timestamp_columns, the row order and the index are unchanged.
    The input is not modified.

    Args:
        input_dataframe: Table with timezone-aware (UTC) timestamp columns.
        timestamp_columns: The lifecycle columns to mask.
        cutoff: Timezone-aware (UTC) moment; timestamps >= cutoff are hidden.

    Returns:
        A new DataFrame with the same columns and dtypes as the input.
    """
    timestamp_columns = list(timestamp_columns)
    masked_df = input_dataframe.copy()
    condition = masked_df[timestamp_columns] >= cutoff
    masked_df[timestamp_columns]  = masked_df[timestamp_columns].mask(condition, None)
    return masked_df

def derive_status(
    input_dataframe: pd.DataFrame,
    status_column: str,
    status_mapping : Mapping[str, str],
    passthrough_statuses: Sequence[str] = PASSTHROUGH_STATUSES,
) -> pd.Series:
    """
    Derive each row's status at the cutoff from the timestamps that survive masking.
 
    The status is decided by presence, not by which timestamp is latest: the first
    column of status_mapping (highest precedence first) that is not null wins,
    so equal timestamps do not matter.
 
    Rows whose original status is in passthrough_statuses keep it. These are statuses
    that no timestamp implies, such as Cancelled (it is undated, so it is replayed at
    created_at). A passthrough status applies only while the row has at least one
    timestamp left, i.e. the row already exists at the cutoff.
 
    A row with no timestamp left gives null, whatever its original status.
 
    Args:
        input_dataframe: Table already passed through mask_future_timestamps.
            Needs every column named in status_mapping, and status_column
            when passthrough_statuses is not empty. A missing column raises KeyError.
        status_column: Column holding the original, unmasked status.
            Only read when passthrough_statuses is not empty.
        status_mapping: Timestamp column to status, highest precedence first.
        passthrough_statuses: Original statuses that are kept as they are.
 
    Returns:
        A Series of str with the same index as input_dataframe,
        null where no timestamp is left.
    """
    # retain priority order (highest stage first); a missing column raises KeyError
    priority_cols = list(status_mapping.keys())
    # boolean mask: True where a timestamp survived masking
    present_mask = input_dataframe[priority_cols].notna()
    # rows that still have at least one timestamp, i.e. that exist at the cutoff
    row_has_any_timestamp = present_mask.any(axis=1)
    # idxmax on booleans selects the FIRST True column, which is the highest precedence
    first_present_col = present_mask.idxmax(axis=1).where(row_has_any_timestamp, None)
    derived_status = first_present_col.map(status_mapping)

    if passthrough_statuses:
        original_status = input_dataframe[status_column]
        keeps_original_status = original_status.isin(passthrough_statuses) & row_has_any_timestamp
        derived_status = derived_status.where(~keeps_original_status, original_status)
    return derived_status

def get_latest_timestamp(input_dataframe: pd.DataFrame, timestamp_columns: Sequence[str]) -> pd.Series:
    """Return, per row, the latest of the given timestamp columns (nulls ignored).
 
    A row whose columns are all null, or an empty column list, gives NaT.
    The input is not modified.
 
    Args:
        input_dataframe: Table with timezone-aware (UTC) timestamp columns.
        timestamp_columns: The columns to take the latest of.
 
    Returns:
        A datetime64[us, UTC] Series with the same index as input_dataframe.
    """
    cols = list(timestamp_columns)
    if not cols:
        return pd.Series(pd.NaT, index=input_dataframe.index, dtype=TIMESTAMP_DTYPE)
    return input_dataframe[cols].max(axis=1).astype(TIMESTAMP_DTYPE)

def add_metadata_columns(
        input_dataframe: pd.DataFrame, 
        source_updated_at: pd.Series,
        op: pd.Series | str
    ) -> pd.DataFrame:
    """Return a copy of the table with _op, _source_updated_at and _seq appended.

        _seq numbers the rows in their current order, from 1, so the caller must pass them
        ordered by key. The input is not modified.

        Args:
            input_dataframe: The table, already filtered to the rows of the batch.
            source_updated_at: Latest timestamp per row, aligned to input_dataframe by index.
            op: "I" or "U" per row, aligned to input_dataframe by index; or one value for every row.

        Returns:
            A new DataFrame: the input columns, then _op, _source_updated_at and _seq.
    """
    return input_dataframe.assign(
        _op = op,
        _source_updated_at = source_updated_at,
        _seq = range(1, len(input_dataframe)+1)
    )

def attach_order_created_at(order_items: pd.DataFrame, orders: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of order_items with its parent order's created_at as order_created_at.

    The thelook order_items.created_at is synthetic and unrelated to the order (as seen in EDA),
    so the simulator uses the parent's created_at instead, as a working column
    An item whose order_id is not in orders gets NaT.

    Args:
        order_items: grain is line item rows of each order. Needs an order_id column.
        orders: Order rows, one per order_id (a duplicate raises). Needs order_id and created_at.

    Returns:
        A copy of order_items with one extra column, order_created_at.
    """
    created_at_by_order_id = orders.set_index("order_id")["created_at"]
    return order_items.assign(order_created_at=order_items["order_id"].map(created_at_by_order_id))

def _dataframe_with_schema_dtypes(relation: duckdb.DuckDBPyRelation) -> pd.DataFrame:
    """Convert a DuckDB relation to pandas with dtypes taken from its schema, not its data.

    Text columns (VARCHAR) become str and timestamptz columns become UTC microseconds.
    The result then does not depend on the connection's time zone, or on whether a text
    column happens to be all null in the rows read. Other types are left as DuckDB gives them.
    """
    dataframe = relation.df()
    dtypes = {}
    for column, duckdb_type in zip(relation.columns, relation.types):
        if str(duckdb_type) == "VARCHAR":
            dtypes[column] = "str"
        elif str(duckdb_type) == "TIMESTAMP WITH TIME ZONE":
            dtypes[column] = TIMESTAMP_DTYPE
    return dataframe.astype(dtypes)

def read_table(relation: duckdb.DuckDBPyRelation, order_by: str | None = None) -> pd.DataFrame:
    """Read every row of a relation, with dtypes taken from its schema (see above).
 
    Args:
        relation: The DuckDB table or query to read.
        order_by: Key column, sorted ascending. The simulator always passes it, because _seq
            numbers the rows in the order read. None keeps DuckDB's own order, which is not
            guaranteed, so use None only where the order does not matter.
    """
    if order_by:
        relation = relation.order(f'"{order_by}" ASC')
    return _dataframe_with_schema_dtypes(relation)

def read_empty_table(relation: duckdb.DuckDBPyRelation) -> pd.DataFrame:
    """Return zero rows with exactly the columns and dtypes that read_table gives.

    Used for the static tables in every batch after batch 0, so every batch has the same
    shape. It reads no data.
    """
    return _dataframe_with_schema_dtypes(relation.limit(0))

def is_written_in_window(
    source_updated_at: pd.Series,
    window_start: pd.Timestamp | None,
    window_end: pd.Timestamp | None,
) -> pd.Series:
    """Return a boolean Series indicating which rows are within the window.

    The window is half-open: window_start is inclusive, window_end is exclusive. A bound of
    None means no limit on that side. A null source_updated_at is never inside the window,
    even when both bounds are None: a row with no timestamp does not exist in the replay.

    Args:
        source_updated_at: Latest timestamp per row, aligned to the table by index (timezone-aware UTC). Null means the row does not exist in the replay.
        window_start: Inclusive start of the window, or None for no lower bound.
        window_end: Exclusive end of the window, or None for no upper bound.

    Returns:
        A boolean Series with the same index as source_updated_at, True for rows inside the window.
    """
    mask = source_updated_at.notna()
    if window_start is not None:
        mask &= source_updated_at >= window_start
    if window_end is not None:
        mask &= source_updated_at < window_end
    return mask

def derive_op(
        created_at: pd.Series,
        window_start: pd.Timestamp | None,
        window_end: pd.Timestamp | None
)  -> pd.Series:
    """Return "I" for rows created inside the window and "U" for the rest.

    A row is an insert the first time it appears, which is the window its creation timestamp
    falls in (or batch 0). In any later window the same row is an update. This looks only at
    the row itself, never at earlier batches, so a batch can be built alone.

    Args:
        created_at: When each row was created, aligned to the table by index (timezone-aware UTC).
            After masking, a creation time at or after window_end is null.
        window_start: Inclusive start of the window, or None for no lower bound (batch 0).
        window_end: Exclusive end of the window, or None for no upper bound.

    Returns:
        A str Series with the same index as created_at: "I" or "U".
    """
    is_insert = is_written_in_window(created_at, window_start, window_end)
    return is_insert.map({True: "I", False: "U"})

def build_table(
        database_connection: duckdb.DuckDBPyConnection,
        table: str,
        *,
        window_start: pd.Timestamp | None,
        window_end: pd.Timestamp,
) -> pd.DataFrame:
    """Return one table of a batch, with _op, _source_updated_at and _seq appended.

    This is the per-table part of simulate_batch. Everything it builds along the way (the full
    table, the masked copy, the filtered copy) goes out of scope when it returns, so only the
    returned DataFrame stays in memory.

    Args:
        database_connection: Open DuckDB connection to the snapshot.
        table: One of the keys of TABLE_KEYS.
        window_start: Inclusive start of the window (UTC), or None for no lower bound (batch 0).
        window_end: Exclusive end of the window (UTC).

    Returns:
        The table's rows for this batch. See simulate_batch for the columns.
    """
    # key is required to order the rows, so that _seq is consistent across runs
    key = TABLE_KEYS[table]
    rel = database_connection.table(table)
    timestamp_cols = TABLE_TIMESTAMPS[table] # the lifecycle timestamp columns required for masking and for _source_updated_at

    if table in STATIC_TABLES:
        # static tables are loaded once, in batch 0, with a null _source_updated_at
        if window_start is None:
            # batch 0: the whole table, ordered by key
            temp_table = read_table(rel, order_by=key)
        else:
            # later batches: no rows, but the same columns and dtypes as batch 0
            temp_table = read_empty_table(rel)
        # all null for static tables; it makes sure the _source_updated_at column exists
        latest_timestamp = get_latest_timestamp(temp_table, timestamp_cols)
        op = "I"  # every static row is a first appearance (batch 0)
    else:
        # replayed tables: hide the future, derive the status, then keep only rows that exist
        temp_table = read_table(rel, order_by=key)

        if table == "order_items":
            # working column: the parent's created_at, read from the orders table itself
            orders_created_at = read_table(database_connection.table("orders").project("order_id, created_at"))
            temp_table = attach_order_created_at(temp_table, orders_created_at)

        temp_table = mask_future_timestamps(temp_table, timestamp_cols, window_end)

        # only orders and order_items have a status column (users and events do not),
        # so only they get a status derived from the timestamps that survived masking
        if table in STATUS_COLUMN_BY_TABLE:
            temp_table[STATUS_COLUMN_BY_TABLE[table]] = derive_status(
                temp_table,
                STATUS_COLUMN_BY_TABLE[table],
                STATUS_MAPPING_BY_TABLE[table],
                PASSTHROUGH_STATUSES
                )

        latest_timestamp = get_latest_timestamp(temp_table, timestamp_cols)

        # keep only rows that were written inside the window, i.e. whose latest timestamp is inside the window
        keep = is_written_in_window(latest_timestamp, window_start, window_end)
        temp_table = temp_table[keep]
        latest_timestamp = latest_timestamp[keep]

        # "I" if the row was created inside the window, "U" if it was created earlier.
        # This needs order_created_at for order_items, so it comes before that column is dropped.
        op = derive_op(temp_table[CREATION_COLUMN_BY_TABLE[table]], window_start, window_end)

        if table == "order_items":
            # drop the working column, it is not part of the source schema
            temp_table = temp_table.drop(columns="order_created_at")

    return add_metadata_columns(temp_table, latest_timestamp, op)


def iter_batch_tables(
        database_connection: duckdb.DuckDBPyConnection,
        *,
        window_start: pd.Timestamp | None=None,
        window_end: pd.Timestamp,
        seed: int=42
) -> Iterator[tuple[str, pd.DataFrame]]:
    """Yield the seven tables of a batch one at a time, as (table_name, DataFrame) pairs.

    Same content as simulate_batch, but a table is only built when the caller asks for the next
    one. A caller that writes each table out and drops it before asking for the next never holds
    more than one table in memory, which matters for the biggest table (events).

    Args:
        database_connection: Open DuckDB connection to the snapshot.
        window_start: Inclusive start of the window (UTC), or None for no lower bound (batch 0).
        window_end: Exclusive end of the window (UTC).
        seed: Integer seed for the randomised quirks added in later phases.

    Yields:
        (table name, DataFrame), in the order of TABLE_KEYS.
    """
    for table in TABLE_KEYS:
        yield table, build_table(
            database_connection, table, window_start=window_start, window_end=window_end
        )


def simulate_batch(
        database_connection: duckdb.DuckDBPyConnection,
        *,
        window_start: pd.Timestamp | None=None,
        window_end: pd.Timestamp,
        seed: int=42
) -> dict[str, pd.DataFrame]:
    """Return one batch of the thelook source as a dictionary of DataFrames.
 
    Replays the frozen snapshot: every table comes back in the state it had at window_end, and
    only rows written inside the window are returned. Batch 0 is the same call with
    window_start=None: everything before window_end, plus the three static tables. In every
    other batch the static tables are empty, with the same columns and dtypes.
 
    Each DataFrame has the source columns unchanged (order_items keeps its own created_at),
    followed by three metadata columns:
        _op: "I" if this version is the row's insert, "U" for a later version. A row is an insert
            when its created_at is inside the window (or in batch 0); order items use their
            order's created_at. "D" (deletes) arrives in a later phase.
        _source_updated_at: when this version was written in the source, i.e. the latest lifecycle
            timestamp that had happened by window_end. Order items also count their order's
            created_at. Null only for the static tables (inventory_items, products,
            distribution_centers).
        _seq: row number within the table, starting at 1, rows ordered by key ascending.
            It is the final tiebreak between versions of the same key.
 
    orders and order_items get their status recomputed from the timestamps that exist at
    window_end (Cancelled is kept as it is). The loader adds _batch_id, _batch_date and
    _loaded_at. This function only reads; it writes nothing and keeps no state, so any batch
    can be built alone. All seven tables are held in memory at once; use iter_batch_tables to
    get them one at a time.
 
    Args:
        database_connection: Open DuckDB connection to the snapshot. The caller opens and
            closes it.
        window_start: Inclusive start of the window, timezone-aware (UTC). None means no lower
            bound (batch 0).
        window_end: Exclusive end of the window, timezone-aware (UTC). Anything timestamped at
            or after it counts as not yet happened.
        seed: Integer seed for the randomised quirks added in later phases.
 
    Returns:
        A dictionary mapping each of the seven table names to its DataFrame.
    """
    return dict(iter_batch_tables(
        database_connection, window_start=window_start, window_end=window_end, seed=seed
    ))
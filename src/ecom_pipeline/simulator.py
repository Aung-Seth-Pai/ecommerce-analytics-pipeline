import pandas as pd
import duckdb

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

def add_metadata_columns(input_dataframe: pd.DataFrame):
    """Return a copy of the table with _op, _source_updated_at and _seq appended.
        The input is not modified. _seq numbers the rows in their current order,
        so the caller must read them ordered by key.
    """
    output = input_dataframe.assign(_op="I")
    output['_source_updated_at'] =  pd.Series(pd.NaT, index=output.index, dtype=pd.DatetimeTZDtype(unit="us", tz="UTC"))
    output['_seq'] = range(1, len(output)+1, 1)
    return output

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
            Stage 0: null for every table. From stage 1: null only for tables with no timestamps (products, distribution_centers).
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
        data_tables[table] = add_metadata_columns(data_tables[table])

    # return the data object
    return data_tables
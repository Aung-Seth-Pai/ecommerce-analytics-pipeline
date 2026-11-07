import duckdb
from google.cloud import bigquery
import os
from tqdm import tqdm
import sys

from datetime import datetime, timezone
from pathlib import Path

MANIFEST_TABLE = "_snapshot_manifest"

def to_utc_naive(moment: datetime) -> datetime:
    """DuckDB TIMESTAMP columns hold no timezone, so every time is stored as UTC."""
    return moment.astimezone(timezone.utc).replace(tzinfo=None)

# this requires the google-cloud-bigquery library to be installed and configured with appropriate credentials.
def snapshot_bigquery_dataset_to_duckdb(
    gcp_project: str,
    bq_dataset_id: str,
    duckdb_path: str,
    tables: list[str] | None = None,
):
    """
    Copies every table of a BigQuery dataset into a local DuckDB file as an untouched
    snapshot, and writes a manifest table describing what was copied.
    :param gcp_project: GCP project that runs (and is billed for) the queries.
    :param bq_dataset_id: Fully qualified source dataset, e.g. project.dataset.
    :param duckdb_path: Local path of the DuckDB file.
    :param tables: Optional list of table names. If None, all tables are copied.
    """
    snapshot_taken_at = datetime.now(timezone.utc)

    print(f"Connecting to BigQuery using project '{gcp_project}'...")
    bq_client = bigquery.Client(project=gcp_project)
    try:
        query_job = bq_client.query("SELECT 1 AS conn_test")
        results = list(query_job.result())
        for row in results:
            print(f"Connection test successful: {row['conn_test']}")
    except Exception as e:
        print(f"Failed to connect to BigQuery: {e}")
        sys.exit(1)

    print(f"Connecting to DuckDB at '{duckdb_path}'...")
    try:
        duckdb_conn = duckdb.connect(duckdb_path)
        duckdb_conn.execute("SELECT 1 AS conn_test")
        print("Connection test successful.")
    except Exception as e:
        print(f"Failed to connect to DuckDB: {e}")
        sys.exit(1)

    try:
        duckdb_conn.execute(f"DROP TABLE IF EXISTS {MANIFEST_TABLE}")

        # if table is not specified, fetch all tables from the dataset
        if not tables:
            print(f"No specific tables provided. Fetching all tables from dataset '{bq_dataset_id}'...")
            dataset_ref = bigquery.DatasetReference.from_string(bq_dataset_id)
            tables = [table.table_id for table in bq_client.list_tables(dataset_ref)]
            print(f"Found {len(tables)} tables: {tables}")

        failed_tables = []
        manifest_rows = []
        pbar = tqdm(tables, desc="Syncing tables", unit="table")
        for table_name in pbar:
            pbar.set_postfix_str(f"Fetching `{table_name}`...")
            full_table_id = f"{bq_dataset_id}.{table_name}"
            try:
                source_modified_at = bq_client.get_table(full_table_id).modified

                # ingest the table from BigQuery into DuckDB using Arrow zero-copy streams
                query_job = bq_client.query(f"SELECT * FROM `{full_table_id}`")
                results = query_job.result()

                pbar.set_postfix_str(f"Writing `{table_name}` to DuckDB...")
                arrow_table = results.to_arrow()
                duckdb_conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM arrow_table")

                # verify row count in DuckDB matches BigQuery
                bq_row_count = results.total_rows
                duckdb_row_count = duckdb_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                if bq_row_count == duckdb_row_count:
                    pbar.write(f"'{table_name}' copied and verified: {duckdb_row_count}")
                    manifest_rows.append((
                        bq_dataset_id,
                        table_name,
                        duckdb_row_count,
                        to_utc_naive(source_modified_at),
                        to_utc_naive(snapshot_taken_at)
                    ))
                else:
                    pbar.write(f"[ERROR] Failed to process '{table_name}': {e}")
                    failed_tables.append(table_name)

            except Exception as e:
                pbar.write(f"[ERROR] Failed to process table '{table_name}': {e}")
                failed_tables.append(table_name)

        if failed_tables:
            print(f"Some tables failed to sync: {failed_tables}. No manifest written.")
            sys.exit(1)

        # create a manifest table in DuckDB
        print(f"Creating manifest table '{MANIFEST_TABLE}' in DuckDB...")
        duckdb_conn.execute(f"""
            CREATE TABLE {MANIFEST_TABLE} (
                source_dataset VARCHAR,
                table_name VARCHAR,
                row_count BIGINT,
                source_modified_at_utc TIMESTAMP,
                snapshot_taken_at_utc TIMESTAMP
            )
        """)
        duckdb_conn.executemany(
            f"INSERT INTO {MANIFEST_TABLE} VALUES (?,?,?,?,?)",
            manifest_rows,
        )
        print(f"Snapshot complete. Manifest written to '{MANIFEST_TABLE}'")

    finally:
        duckdb_conn.close()

if __name__ == "__main__":
    # Example usage
    settings = {
        "GCP_PROJECT":      os.getenv("GCP_PROJECT"),
        "BQ_DATASET_ID":    os.getenv("BQ_DATASET_ID"),
        "DUCKDB_PATH":      os.getenv("DUCKDB_PATH"),
    }
    missing = [name for name, value in settings.items() if not value]
    if missing:
        print(f"Missing environment variables: {missing}. Check your .env file")

    snapshot_bigquery_dataset_to_duckdb(
        gcp_project=settings['GCP_PROJECT'],
        bq_dataset_id=settings['BQ_DATASET_ID'],
        duckdb_path=settings['DUCKDB_PATH'],
        tables=None)
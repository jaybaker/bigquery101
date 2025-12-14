import io
import os
import logging

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
import pyarrow as pa
import polars as pl

CREDENTIALS_ENV_VAR = 'GOOGLE_APPLICATION_CREDENTIALS'


def get_bigquery_client(project_id: str) -> bigquery.Client:
    """Get Big Query client"""
    if CREDENTIALS_ENV_VAR in os.environ:
        logging.debug(
            f'Creating Bigquery client using service account file {os.environ.get(CREDENTIALS_ENV_VAR)}'
        )
    else:
        logging.debug('Creating Bigquery client with user credentials')

    return bigquery.Client(project=project_id)


def get_table(dataset: str, name: str, client: bigquery.Client):
    """Get the bigquery table. This is a convenient 'does table exist' function.

    Returns:
        table or None - If the table exists, return it
    """
    ds = client.dataset(dataset)
    table_ref = ds.table(name)

    try:
        # lookup the table
        table = client.get_table(table_ref)
        return table
    except NotFound:
        return


def get_bigquery_result(query_str: str, client: bigquery.Client) -> pa.Table:
    """Get query result from BigQuery and return a pyarrow table."""
    logging.debug(f'Running query:\n {query_str}')
    pa_tbl = client.query(query_str).to_arrow()
    return pa_tbl


def query_df(query_str: str, client: bigquery.Client) -> pl.DataFrame:
    """Execute query and return polars dataframe."""
    arrow_table = get_bigquery_result(query_str, client)
    return pl.DataFrame(arrow_table)  # same as pl.from_arrow() with known type


def dataframe_to_table(
    df: pl.DataFrame,
    dataset_id: str,
    table_id: str,
    client: bigquery.Client,
    append: bool = False,
    use_async: bool = False,
):
    """Write a DataFrame to BigQuery"""

    buffer = io.BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)  # Reset buffer's position to the beginning

    # Create table reference
    table_ref = f'{dataset_id}.{table_id}'

    # Configure load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,  # Infers schema from the Parquet file
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        if append
        else bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # Load DataFrame to BigQuery
    job = client.load_table_from_file(buffer, table_ref, job_config=job_config)

    if use_async:
        return job
    else:
        # Wait for job to complete
        job.result()

        logging.debug(f'Loaded {len(df)} rows to {table_ref}')
        return job


def append_table(
    source_table_ref: str,
    dest_dataset: str,
    dest_table: str,
    client: bigquery.Client,
    safe: bool = True,
    use_async: bool = False,
):
    """Append to a table.
    If safe is true, this will create the table if it does not exist.
    """
    query = f"""
    select *
    from `{source_table_ref}`
    """
    tbl = get_table(dest_dataset, dest_table, client)
    if tbl is not None or safe is False:
        job_config = bigquery.QueryJobConfig(
            destination=f'{dest_dataset}.{dest_table}',
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        job = client.query(query, job_config=job_config)
    else:  # add statement to create the table
        query = (
            f"""
        create table {dest_dataset}.{dest_table} as
        """
            + '\n'
            + query
        )
        job = client.query(query)

    if use_async:
        return job
    else:
        # Wait for job to complete
        job.result()


class CTX:
    """A context manager to support carrying the client and usage like:
    ```
    with bigquery101.CTX(project_id) as bq:
        df = bq.query_df('SELECT 1 as a')
    ```
    """

    def __init__(self, project_id: str):
        self.client = get_bigquery_client(project_id=project_id)

    def __enter__(self) -> 'CTX':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass

    def query(self, query: str):
        return get_bigquery_result(query, self.client)

    def query_df(self, query: str):
        return query_df(query, self.client)

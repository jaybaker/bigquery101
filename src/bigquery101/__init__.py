"""A small, opinionated convenience layer over Google BigQuery.

The query/load helpers are typed against the :class:`BigQueryClient` protocol
rather than the concrete ``google.cloud.bigquery.Client``. A real client
satisfies the protocol structurally, so production usage is unchanged, while
tests can pass lightweight fakes without mocking.
"""

import io
import logging
import os
import re
from typing import Any, Literal, Protocol, runtime_checkable

import polars as pl
import pyarrow as pa
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

__all__ = [
    'BigQueryClient',
    'CTX',
    'WriteDisposition',
    'append_table',
    'dataframe_to_table',
    'get_bigquery_client',
    'get_bigquery_result',
    'get_table',
    'query_df',
]

logger = logging.getLogger(__name__)

CREDENTIALS_ENV_VAR = 'GOOGLE_APPLICATION_CREDENTIALS'

#: Accepted values for ``dataframe_to_table(write_disposition=...)``.
WriteDisposition = Literal['append', 'truncate', 'empty']

_WRITE_DISPOSITIONS: dict[str, str] = {
    'append': bigquery.WriteDisposition.WRITE_APPEND,
    'truncate': bigquery.WriteDisposition.WRITE_TRUNCATE,
    'empty': bigquery.WriteDisposition.WRITE_EMPTY,
}

# A BigQuery table reference: backtick-quoted `project.dataset.table` parts.
# Identifiers cannot be parameterized, so we validate them before interpolation.
_IDENTIFIER_RE = re.compile(r'^[A-Za-z0-9_$.\-]+$')


class _QueryJob(Protocol):
    def result(self) -> Any: ...

    def to_arrow(self) -> pa.Table: ...


class _LoadJob(Protocol):
    def result(self) -> Any: ...


@runtime_checkable
class BigQueryClient(Protocol):
    """The subset of ``google.cloud.bigquery.Client`` this library relies on.

    Both the real client and test fakes satisfy this protocol structurally.
    """

    def query(self, query: str, *, job_config: Any = None) -> _QueryJob: ...

    def get_table(self, table: Any) -> Any: ...

    def load_table_from_file(
        self, file_obj: Any, destination: Any, *, job_config: Any = None
    ) -> _LoadJob: ...

    def close(self) -> None: ...


def _validate_identifier(identifier: str) -> str:
    """Reject table references that could break out of the SQL string.

    Identifiers cannot be passed as query parameters, so the best we can do is
    refuse anything that isn't a plain dotted/quoted identifier.
    """
    if not _IDENTIFIER_RE.match(identifier):
        raise ValueError(f'unsafe BigQuery identifier: {identifier!r}')
    return identifier


def get_bigquery_client(project_id: str) -> bigquery.Client:
    """Create a BigQuery client for ``project_id``.

    Uses the service-account file pointed to by
    ``GOOGLE_APPLICATION_CREDENTIALS`` when set, otherwise Application Default
    Credentials.
    """
    if CREDENTIALS_ENV_VAR in os.environ:
        logger.debug(
            'Creating BigQuery client using service account file %s',
            os.environ.get(CREDENTIALS_ENV_VAR),
        )
    else:
        logger.debug('Creating BigQuery client with user credentials')

    return bigquery.Client(project=project_id)


def get_table(dataset: str, name: str, client: BigQueryClient) -> bigquery.Table | None:
    """Look up a table; return it if it exists, otherwise ``None``.

    A convenient "does this table exist?" check.
    """
    try:
        return client.get_table(f'{dataset}.{name}')
    except NotFound:
        return None


def get_bigquery_result(query_str: str, client: BigQueryClient) -> pa.Table:
    """Run a query and return the result as a pyarrow table."""
    logger.debug('Running query:\n%s', query_str)
    return client.query(query_str).to_arrow()


def query_df(query_str: str, client: BigQueryClient) -> pl.DataFrame:
    """Run a query and return the result as a polars DataFrame."""
    arrow_table = get_bigquery_result(query_str, client)
    return pl.DataFrame(arrow_table)  # same as pl.from_arrow() with known type


def dataframe_to_table(
    df: pl.DataFrame,
    dataset_id: str,
    table_id: str,
    client: BigQueryClient,
    write_disposition: WriteDisposition = 'append',
    wait: bool = True,
) -> _LoadJob:
    """Load a polars DataFrame into a BigQuery table via Parquet.

    Args:
        write_disposition: How to treat an existing table.
            ``'append'`` (default) adds rows, ``'truncate'`` replaces all
            existing rows, and ``'empty'`` fails if the table is not empty.
        wait: Block until the load job finishes (default). Pass ``False`` to
            return the running job without waiting.

    Returns:
        The load job (completed when ``wait`` is ``True``).
    """
    try:
        disposition = _WRITE_DISPOSITIONS[write_disposition]
    except KeyError:
        raise ValueError(
            f'write_disposition must be one of {sorted(_WRITE_DISPOSITIONS)}, '
            f'got {write_disposition!r}'
        ) from None

    buffer = io.BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)  # rewind for reading

    table_ref = f'{dataset_id}.{table_id}'
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,  # infer schema from the Parquet file
        write_disposition=disposition,
    )

    job = client.load_table_from_file(buffer, table_ref, job_config=job_config)

    if wait:
        job.result()
        logger.debug('Loaded %d rows to %s', len(df), table_ref)
    return job


def append_table(
    source_table_ref: str,
    dest_dataset: str,
    dest_table: str,
    client: BigQueryClient,
    safe: bool = True,
    wait: bool = True,
) -> _QueryJob:
    """Append the rows of one table onto another.

    The source and destination schemas must be compatible (``insert ... select *``
    semantics).

    Args:
        safe: When ``True`` (default), create the destination table if it does
            not already exist instead of failing.
        wait: Block until the job finishes (default). Pass ``False`` to return
            the running job without waiting.

    Returns:
        The query job (completed when ``wait`` is ``True``).
    """
    _validate_identifier(source_table_ref)
    _validate_identifier(dest_dataset)
    _validate_identifier(dest_table)

    select = f'select * from `{source_table_ref}`'
    dest = f'{dest_dataset}.{dest_table}'

    if get_table(dest_dataset, dest_table, client) is not None or not safe:
        job = client.query(f'insert into `{dest}`\n{select}')
    else:  # create the destination table from the query
        job = client.query(f'create table `{dest}` as\n{select}')

    if wait:
        job.result()
    return job


class CTX:
    """Context manager that carries a client and closes it on exit.

    ```python
    with bigquery101.CTX(project_id) as bq:
        df = bq.query_df('SELECT 1 as a')
    ```
    """

    def __init__(self, project_id: str):
        self.client = get_bigquery_client(project_id=project_id)

    def __enter__(self) -> 'CTX':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.client.close()

    def query(self, query: str) -> pa.Table:
        return get_bigquery_result(query, self.client)

    def query_df(self, query: str) -> pl.DataFrame:
        return query_df(query, self.client)

    def dataframe_to_table(
        self,
        df: pl.DataFrame,
        dataset_id: str,
        table_id: str,
        write_disposition: WriteDisposition = 'append',
        wait: bool = True,
    ) -> _LoadJob:
        return dataframe_to_table(
            df, dataset_id, table_id, self.client, write_disposition, wait
        )

    def append_table(
        self,
        source_table_ref: str,
        dest_dataset: str,
        dest_table: str,
        safe: bool = True,
        wait: bool = True,
    ) -> _QueryJob:
        return append_table(
            source_table_ref, dest_dataset, dest_table, self.client, safe, wait
        )

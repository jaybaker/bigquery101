"""Unit tests that exercise the library's logic without touching real BigQuery.

Instead of mocking ``bigquery.Client``, we define fakes that structurally
satisfy the :class:`bigquery101.BigQueryClient` protocol and record the calls
made against them. The real ``google.cloud.bigquery.Client`` satisfies the same
protocol, so the production code is exercised exactly as it would be in anger.
"""

import pyarrow as pa
import polars as pl
import pytest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

import bigquery101


# --- Fakes implementing the BigQueryClient protocol -------------------------


class FakeQueryJob:
    def __init__(self, arrow_table: pa.Table | None = None):
        self._arrow = arrow_table
        self.result_called = False

    def result(self):
        self.result_called = True
        return None

    def to_arrow(self) -> pa.Table:
        assert self._arrow is not None
        return self._arrow


class FakeLoadJob:
    def __init__(self):
        self.result_called = False

    def result(self):
        self.result_called = True
        return None


class FakeClient:
    """Records calls; returns canned jobs. Satisfies BigQueryClient."""

    def __init__(self, table: object | None = None, arrow_table: pa.Table | None = None):
        self._table = table
        self._arrow = arrow_table
        self.closed = False
        self.queries: list[tuple[str, object]] = []
        self.loads: list[tuple[str, object]] = []
        self.last_query_job: FakeQueryJob | None = None
        self.last_load_job: FakeLoadJob | None = None

    def query(self, query, job_config=None):
        self.queries.append((query, job_config))
        self.last_query_job = FakeQueryJob(self._arrow)
        return self.last_query_job

    def get_table(self, table):
        if self._table is None:
            raise NotFound(f'table not found: {table}')
        return self._table

    def load_table_from_file(self, file_obj, destination, job_config=None):
        self.loads.append((destination, job_config))
        self.last_load_job = FakeLoadJob()
        return self.last_load_job

    def close(self):
        self.closed = True


# --- The protocol itself -----------------------------------------------------


def test_fake_client_satisfies_protocol():
    assert isinstance(FakeClient(), bigquery101.BigQueryClient)
    # a real client also satisfies it (structural typing)
    assert issubclass(bigquery.Client, bigquery101.BigQueryClient)


# --- get_table ---------------------------------------------------------------


def test_get_table_returns_table_when_present():
    sentinel = object()
    client = FakeClient(table=sentinel)
    assert bigquery101.get_table('ds', 'tbl', client) is sentinel


def test_get_table_returns_none_when_missing():
    client = FakeClient(table=None)
    assert bigquery101.get_table('ds', 'tbl', client) is None


# --- query helpers -----------------------------------------------------------


def test_get_bigquery_result_returns_arrow_table():
    arrow = pa.table({'a': [1], 'b': [2]})
    client = FakeClient(arrow_table=arrow)
    result = bigquery101.get_bigquery_result('select 1', client)
    assert result is arrow


def test_query_df_returns_polars_dataframe():
    arrow = pa.table({'x': [1, 2], 'y': [3, 4]})
    client = FakeClient(arrow_table=arrow)
    df = bigquery101.query_df('select 1', client)
    assert isinstance(df, pl.DataFrame)
    assert df.shape == (2, 2)
    assert df.columns == ['x', 'y']


# --- dataframe_to_table ------------------------------------------------------


def test_dataframe_to_table_append_uses_write_append():
    client = FakeClient()
    df = pl.DataFrame({'a': [1]})
    bigquery101.dataframe_to_table(df, 'ds', 'tbl', client, write_disposition='append')
    destination, job_config = client.loads[0]
    assert destination == 'ds.tbl'
    assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_APPEND


def test_dataframe_to_table_truncate_uses_write_truncate():
    client = FakeClient()
    df = pl.DataFrame({'a': [1]})
    bigquery101.dataframe_to_table(df, 'ds', 'tbl', client, write_disposition='truncate')
    _, job_config = client.loads[0]
    assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE


def test_dataframe_to_table_empty_uses_write_empty():
    client = FakeClient()
    df = pl.DataFrame({'a': [1]})
    bigquery101.dataframe_to_table(df, 'ds', 'tbl', client, write_disposition='empty')
    _, job_config = client.loads[0]
    assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_EMPTY


def test_dataframe_to_table_default_is_append():
    client = FakeClient()
    df = pl.DataFrame({'a': [1]})
    bigquery101.dataframe_to_table(df, 'ds', 'tbl', client)
    _, job_config = client.loads[0]
    assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_APPEND


def test_dataframe_to_table_rejects_unknown_disposition():
    client = FakeClient()
    df = pl.DataFrame({'a': [1]})
    with pytest.raises(ValueError):
        bigquery101.dataframe_to_table(df, 'ds', 'tbl', client, write_disposition='bogus')


def test_dataframe_to_table_wait_true_blocks():
    client = FakeClient()
    df = pl.DataFrame({'a': [1]})
    job = bigquery101.dataframe_to_table(df, 'ds', 'tbl', client, wait=True)
    assert job.result_called is True


def test_dataframe_to_table_wait_false_does_not_block():
    client = FakeClient()
    df = pl.DataFrame({'a': [1]})
    job = bigquery101.dataframe_to_table(df, 'ds', 'tbl', client, wait=False)
    assert job.result_called is False


# --- append_table ------------------------------------------------------------


def test_append_table_existing_table_appends():
    client = FakeClient(table=object())
    bigquery101.append_table('proj.ds.src', 'ds', 'dest', client)
    query, _ = client.queries[0]
    assert 'create table' not in query.lower()
    assert 'insert into' in query.lower()
    assert '`ds.dest`' in query


def test_append_table_safe_creates_when_missing():
    client = FakeClient(table=None)
    bigquery101.append_table('proj.ds.src', 'ds', 'dest', client, safe=True)
    query, _ = client.queries[0]
    assert 'create table' in query.lower()
    assert '`ds.dest`' in query


def test_append_table_unsafe_appends_even_when_missing():
    client = FakeClient(table=None)
    bigquery101.append_table('proj.ds.src', 'ds', 'dest', client, safe=False)
    query, _ = client.queries[0]
    assert 'create table' not in query.lower()
    assert 'insert into' in query.lower()


def test_append_table_wait_true_blocks():
    client = FakeClient(table=object())
    job = bigquery101.append_table('proj.ds.src', 'ds', 'dest', client, wait=True)
    assert job.result_called is True


def test_append_table_wait_false_does_not_block():
    client = FakeClient(table=object())
    job = bigquery101.append_table('proj.ds.src', 'ds', 'dest', client, wait=False)
    assert job.result_called is False


def test_append_table_rejects_bad_identifier():
    client = FakeClient(table=object())
    with pytest.raises(ValueError):
        bigquery101.append_table('proj.ds.src; drop table x', 'ds', 'dest', client)


# --- CTX ---------------------------------------------------------------------


def test_ctx_closes_client_on_exit():
    fake = FakeClient(arrow_table=pa.table({'a': [1]}))
    ctx = bigquery101.CTX.__new__(bigquery101.CTX)
    ctx.client = fake
    with ctx as bq:
        assert bq is ctx
    assert fake.closed is True


def test_ctx_query_df_returns_dataframe():
    fake = FakeClient(arrow_table=pa.table({'a': [1, 2]}))
    ctx = bigquery101.CTX.__new__(bigquery101.CTX)
    ctx.client = fake
    with ctx as bq:
        df = bq.query_df('select 1')
    assert isinstance(df, pl.DataFrame)
    assert df.shape[0] == 2

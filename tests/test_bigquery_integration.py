import pyarrow as pa
import pytest
from google.cloud import bigquery

import bigquery101
from conftest import requires_bigquery


@pytest.mark.integration
@requires_bigquery
def test_simple_query(bq_client: bigquery.Client):
    result = bigquery101.get_bigquery_result('select 1 as a, 2 as b', bq_client)

    assert isinstance(result, pa.Table)
    assert result.num_rows == 1
    assert result.column_names == ['a', 'b']
    assert result.column('a').to_numpy() == [1]
    assert result.column('b').to_numpy() == [2]


@pytest.mark.integration
@requires_bigquery
def test_dataframe(bq_client: bigquery.Client):
    df = bigquery101.query_df('select 1 as x, 2 as y', bq_client)
    assert df is not None and df.shape[0] == 1


@pytest.mark.integration
@requires_bigquery
def test_context_manager(gcp_project: str):
    with bigquery101.CTX(gcp_project) as bq:
        df = bq.query_df('select 1 as x, 2 as y')
        assert df is not None and df.shape[0] == 1

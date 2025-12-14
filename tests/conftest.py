import os

import pytest
from google.cloud import bigquery

import bigquery101

requires_bigquery = pytest.mark.skipif(
    'GCP_PROJECT' not in os.environ,
    reason='GCP_PROJECT environment variable not set',
)


@pytest.fixture
def gcp_project() -> str:
    return os.environ['GCP_PROJECT']


@pytest.fixture
def bq_client(gcp_project: str) -> bigquery.Client:
    return bigquery101.get_bigquery_client(gcp_project)

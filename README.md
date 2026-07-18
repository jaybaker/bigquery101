# BigQuery 101

A small, opinionated convenience layer over Google BigQuery that cuts the
boilerplate out of the common tasks: run a query into a
[polars](https://pola.rs/) DataFrame, and load a DataFrame back into a table.
Data transfer uses Arrow/Parquet under the hood.

## Install

```bash
uv add bigquery101
# or
pip install bigquery101
```

Requires Python ≥ 3.12.

## Authentication

`get_bigquery_client(project_id)` creates a client using whatever credentials
are available:

- If `GOOGLE_APPLICATION_CREDENTIALS` points at a service-account JSON file,
  that file is used.
- Otherwise [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
  are used (e.g. after `gcloud auth application-default login`).

## Usage

### Query into a DataFrame

```python
import bigquery101

client = bigquery101.get_bigquery_client('my-project')

df = bigquery101.query_df('select 1 as a, 2 as b', client)   # polars.DataFrame
arrow = bigquery101.get_bigquery_result('select 1 as a', client)  # pyarrow.Table
```

### Context manager

`CTX` carries the client and **closes it on exit**:

```python
with bigquery101.CTX('my-project') as bq:
    df = bq.query_df('select 1 as a')
    bq.dataframe_to_table(df, 'my_dataset', 'my_table')
```

### Load a DataFrame into a table

```python
bigquery101.dataframe_to_table(
    df,
    dataset_id='my_dataset',
    table_id='my_table',
    client=client,
    write_disposition='append',  # 'append' (default) | 'truncate' | 'empty'
    wait=True,                   # block until the load finishes
)
```

> **`write_disposition` controls existing data.** `'append'` (the default) adds
> rows, `'truncate'` **replaces all existing rows**, and `'empty'` fails if the
> table is not empty. Truncation is opt-in — the default never destroys data.

Pass `wait=False` to get the running job back without blocking.

### Append one table onto another

```python
bigquery101.append_table(
    source_table_ref='my-project.my_dataset.source',
    dest_dataset='my_dataset',
    dest_table='dest',
    client=client,
    safe=True,   # create the destination if it doesn't exist
)
```

Source and destination schemas must be compatible (`insert ... select *`).

## Testing without BigQuery

The query/load helpers are typed against the `BigQueryClient`
[`Protocol`](https://docs.python.org/3/library/typing.html#typing.Protocol)
rather than the concrete `google.cloud.bigquery.Client`. A real client satisfies
the protocol structurally, so you can pass a lightweight fake in tests — no
mocking required. See `tests/test_unit.py` for the pattern.

```bash
uv run pytest -m "not integration"   # fast, hermetic unit tests
uv run pytest -m integration         # hits real BigQuery; needs GCP_PROJECT
```

Integration tests are skipped unless `GCP_PROJECT` is set.

## License

MIT — see [LICENSE](LICENSE).

# BigQuery API Key Test Report

This test attempted to use an API key with `DataGetterBigQuery`.
The environment blocked outbound connections to `bigquery.googleapis.com` so the
client failed to run the query. The code now accepts an API key via the
`BIGQUERY_API_KEY` environment variable or constructor argument.

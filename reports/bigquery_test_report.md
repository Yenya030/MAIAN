# BigQuery Downloader Test Report

This short test attempted to run the `DataGetterBigQuery` helper to fetch
contract bytecode from Google BigQuery. The environment did not provide valid
GCP credentials via `GOOGLE_APPLICATION_CREDENTIALS`, so the query failed with a
`DefaultCredentialsError`.

Steps performed:

1. Installed the optional dependency `google-cloud-bigquery`.
2. Attempted to fetch contracts between blocks 17,000,000 and 17,000,005.
3. The BigQuery client reported `File <API_KEY> was not found`, indicating
   missing credentials.

No contract data could be downloaded without valid credentials.

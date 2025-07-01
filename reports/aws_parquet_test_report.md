# AWS Open Data Parquet Test Report

The new `DataGetterAWSParquet` class loads contract bytecode from Parquet files.
A small sample file was generated locally for testing. The getter returned the
expected dictionaries with ``Address``, ``ByteCode`` and ``BlockNumber`` fields
for the requested block range. Access to the
actual AWS Open-Data S3 buckets was blocked in this environment, so full-scale
retrieval could not be demonstrated.

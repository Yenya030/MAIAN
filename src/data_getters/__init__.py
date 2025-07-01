"""Data acquisition helpers."""

from .bigquery_getter import DataGetter, DataGetterBigQuery
from .aws_parquet_getter import DataGetterAWSParquet

__all__ = ["DataGetter", "DataGetterBigQuery", "DataGetterAWSParquet"]

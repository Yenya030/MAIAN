"""Data acquisition helpers."""

from .base import DataGetter
from .aws_parquet_getter import DataGetterAWSParquet
from .bigquery_getter import DataGetterBigQuery

__all__ = ["DataGetter", "DataGetterAWSParquet", "DataGetterBigQuery"]

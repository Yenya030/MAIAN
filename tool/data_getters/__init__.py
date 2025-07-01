"""Data acquisition helpers."""

from .base import DataGetter
from .aws_parquet_getter import DataGetterAWSParquet

__all__ = ["DataGetter", "DataGetterAWSParquet"]

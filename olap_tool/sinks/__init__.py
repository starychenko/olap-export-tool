"""Analytics sinks package."""
from .base import AnalyticsSink, sanitize_df
from .clickhouse import ClickHouseSink
from .duckdb import DuckDBSink
from .postgresql import PostgreSQLSink

__all__ = ["AnalyticsSink", "sanitize_df", "ClickHouseSink", "DuckDBSink", "PostgreSQLSink"]

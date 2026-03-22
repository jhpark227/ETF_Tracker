from __future__ import annotations

from pathlib import Path

import duckdb


def get_connection(db_path: str = "data/etf_analyzer.duckdb") -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection. Use ':memory:' for testing."""
    if db_path != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(db_path)

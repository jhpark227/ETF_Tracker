import duckdb
import pytest

from etf_analyzer.storage.db import get_connection
from etf_analyzer.storage.schema import create_tables


@pytest.fixture
def db():
    """In-memory DuckDB for testing."""
    conn = get_connection(":memory:")
    create_tables(conn)
    yield conn
    conn.close()

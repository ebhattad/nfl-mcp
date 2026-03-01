"""
DuckDB connection management for NFL MCP Server.
"""

import duckdb
from contextlib import contextmanager

from .config import get_duckdb_path


@contextmanager
def get_db_connection():
    """Context manager yielding a fresh read-only DuckDB connection.

    Each call opens a new connection so callers (including threaded timeouts)
    never share a connection across threads.
    """
    db_path = get_duckdb_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        yield conn
    finally:
        conn.close()

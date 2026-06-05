"""
DuckDB connection management for NFL MCP Server.
"""

import os
import duckdb
from contextlib import contextmanager

from .config import get_duckdb_path


def _apply_runtime_pragmas(conn: duckdb.DuckDBPyConnection) -> None:
    """Bound DuckDB resource usage for the serve process.

    In memory-constrained containers a heavy query can exceed the cgroup limit
    and OOM-kill the server (DuckDB otherwise sizes itself to host RAM). These
    env-driven caps keep queries within budget, spilling to disk when needed:
      NFL_MCP_DUCKDB_MEMORY_LIMIT  e.g. '512MB'
      NFL_MCP_DUCKDB_THREADS       e.g. '1'
      NFL_MCP_DUCKDB_TEMP_DIR      writable spill dir, e.g. '/tmp/.duckdb_spill'
    Unset vars leave DuckDB's defaults (local/dev use).
    """
    mem = os.getenv("NFL_MCP_DUCKDB_MEMORY_LIMIT")
    if mem:
        conn.execute(f"SET memory_limit='{mem}'")
    threads = os.getenv("NFL_MCP_DUCKDB_THREADS")
    if threads:
        conn.execute(f"SET threads={int(threads)}")
    spill = os.getenv("NFL_MCP_DUCKDB_TEMP_DIR")
    if spill:
        conn.execute(f"SET temp_directory='{spill}'")


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
        _apply_runtime_pragmas(conn)
        yield conn
    finally:
        conn.close()

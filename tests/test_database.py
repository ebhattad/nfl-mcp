"""Unit tests for the DuckDB connection layer — no network, temp DuckDB files."""

import duckdb
import pytest

from nfl_mcp.database import _apply_runtime_pragmas, get_db_connection


@pytest.fixture
def db_file(tmp_path):
    """A tiny on-disk DuckDB the runtime layer can open read-only."""
    path = tmp_path / "nflread.duckdb"
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE plays AS SELECT 1 AS x")
    con.close()
    return path


class TestApplyRuntimePragmas:
    def test_applies_all_env_caps(self, tmp_path, monkeypatch):
        monkeypatch.setenv("NFL_MCP_DUCKDB_MEMORY_LIMIT", "256MB")
        monkeypatch.setenv("NFL_MCP_DUCKDB_THREADS", "1")
        monkeypatch.setenv("NFL_MCP_DUCKDB_TEMP_DIR", str(tmp_path / "spill"))
        conn = duckdb.connect(":memory:")
        _apply_runtime_pragmas(conn)
        assert conn.execute("SELECT current_setting('threads')").fetchone()[0] == 1
        mem = conn.execute("SELECT current_setting('memory_limit')").fetchone()[0]
        assert "MiB" in mem or "MB" in mem
        spill = conn.execute("SELECT current_setting('temp_directory')").fetchone()[0]
        assert spill.endswith("spill")
        conn.close()

    def test_noop_when_env_unset(self, monkeypatch):
        for var in (
            "NFL_MCP_DUCKDB_MEMORY_LIMIT",
            "NFL_MCP_DUCKDB_THREADS",
            "NFL_MCP_DUCKDB_TEMP_DIR",
        ):
            monkeypatch.delenv(var, raising=False)
        conn = duckdb.connect(":memory:")
        default_threads = conn.execute("SELECT current_setting('threads')").fetchone()[0]
        _apply_runtime_pragmas(conn)
        assert conn.execute("SELECT current_setting('threads')").fetchone()[0] == default_threads
        conn.close()


class TestGetDbConnection:
    def test_yields_readonly_connection_with_pragmas(self, db_file, monkeypatch):
        monkeypatch.setenv("NFL_MCP_DB_PATH", str(db_file))
        monkeypatch.setenv("NFL_MCP_DUCKDB_MEMORY_LIMIT", "256MB")
        monkeypatch.setenv("NFL_MCP_DUCKDB_THREADS", "1")
        with get_db_connection() as conn:
            assert conn.execute("SELECT count(*) FROM plays").fetchone()[0] == 1
            assert conn.execute("SELECT current_setting('threads')").fetchone()[0] == 1
        with pytest.raises(Exception):
            conn.execute("SELECT 1")

    def test_read_only_rejects_writes(self, db_file, monkeypatch):
        monkeypatch.setenv("NFL_MCP_DB_PATH", str(db_file))
        with get_db_connection() as conn:
            with pytest.raises(Exception):
                conn.execute("CREATE TABLE t AS SELECT 1")

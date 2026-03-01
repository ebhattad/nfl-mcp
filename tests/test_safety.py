"""Tests for SQL safety guardrails in nfl_mcp.tools."""

import pytest
from nfl_mcp.tools import nfl_query, _FORBIDDEN


# ── Forbidden keyword detection ────────────────────────────────────────────────

class TestForbiddenRegex:
    """Ensure dangerous SQL keywords are blocked."""

    @pytest.mark.parametrize("keyword", [
        "INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "ALTER",
        "CREATE", "REPLACE", "UPSERT", "EXECUTE", "EXEC", "CALL",
        "COPY", "GRANT", "REVOKE", "VACUUM", "ANALYZE", "CLUSTER",
        "REINDEX", "COMMENT", "SECURITY", "OWNER", "TABLESPACE",
        "SCHEMA", "SET", "DO", "LISTEN", "NOTIFY", "PREPARE",
        "DEALLOCATE", "LOAD", "DISCARD", "RESET",
    ])
    def test_blocks_dangerous_keywords(self, keyword):
        assert _FORBIDDEN.search(keyword), f"Should block: {keyword}"
        assert _FORBIDDEN.search(keyword.lower()), f"Should block lowercase: {keyword.lower()}"

    @pytest.mark.parametrize("keyword", [
        "pg_read_file", "pg_read_binary_file", "pg_write_file",
        "pg_sleep", "lo_import", "lo_export", "dblink",
        "current_setting", "pg_terminate_backend", "pg_cancel_backend",
    ])
    def test_blocks_dangerous_functions(self, keyword):
        assert _FORBIDDEN.search(keyword), f"Should block function: {keyword}"

    def test_allows_select(self):
        assert _FORBIDDEN.search("SELECT") is None

    def test_allows_common_sql_words(self):
        for word in ["FROM", "WHERE", "GROUP", "ORDER", "HAVING", "JOIN",
                     "LEFT", "RIGHT", "INNER", "OUTER", "CASE", "WHEN",
                     "THEN", "ELSE", "END", "AS", "ON", "AND", "OR", "NOT",
                     "IN", "BETWEEN", "LIKE", "ILIKE", "IS", "NULL",
                     "COUNT", "SUM", "AVG", "MIN", "MAX", "ROUND",
                     "COALESCE", "NULLIF", "DISTINCT", "LIMIT", "OFFSET"]:
            assert _FORBIDDEN.search(word) is None, f"Should allow: {word}"


# ── nfl_query guardrails ──────────────────────────────────────────────────────

class TestNflQueryGuardrails:
    """Test that nfl_query blocks dangerous queries."""

    def test_rejects_non_select(self):
        result = nfl_query("INSERT INTO plays VALUES (1)")
        assert "error" in result
        assert "SELECT" in result["error"]

    def test_rejects_delete(self):
        result = nfl_query("DELETE FROM plays")
        assert "error" in result

    def test_rejects_drop(self):
        result = nfl_query("SELECT 1; DROP TABLE plays")
        assert "error" in result

    def test_rejects_multiple_statements(self):
        result = nfl_query("SELECT 1; SELECT 2")
        assert "error" in result
        assert "Multiple" in result["error"]

    def test_rejects_update_in_subquery(self):
        result = nfl_query("SELECT * FROM (UPDATE plays SET season=0) AS x")
        assert "error" in result

    def test_rejects_set_keyword(self):
        result = nfl_query("SELECT * FROM plays; SET statement_timeout = '0'")
        assert "error" in result

    def test_rejects_copy(self):
        result = nfl_query("SELECT 1; COPY plays TO '/tmp/dump'")
        assert "error" in result

    def test_rejects_pg_sleep(self):
        result = nfl_query("SELECT pg_sleep(100)")
        assert "error" in result
        assert "Forbidden" in result["error"]

    def test_rejects_pg_read_file(self):
        result = nfl_query("SELECT pg_read_file('/etc/passwd')")
        assert "error" in result

    def test_rejects_pg_read_binary_file(self):
        result = nfl_query("SELECT pg_read_binary_file('/etc/passwd')")
        assert "error" in result

    def test_allows_trailing_semicolon(self):
        # Single trailing semicolon should be stripped, not rejected
        result = nfl_query("SELECT 1;")
        # Should either succeed or fail on execution, not on validation
        assert "Multiple" not in result.get("error", "")

    def test_max_rows_clamped(self):
        result = nfl_query("SELECT 1", max_rows=9999)
        # Should not error on max_rows being too large — it gets clamped
        assert "error" not in result or "max_rows" not in result.get("error", "")


# ── nfl_query with live DuckDB ────────────────────────────────────────────────

class TestNflQueryLive:
    """Integration tests that require the DuckDB database to be loaded."""

    @pytest.fixture(autouse=True)
    def _check_db(self):
        """Skip if no data is loaded."""
        result = nfl_query("SELECT COUNT(*) AS n FROM plays")
        if "error" in result:
            pytest.skip("No DuckDB database loaded — run 'nfl-mcp ingest' first")
        if result["rows"][0]["n"] == 0:
            pytest.skip("plays table is empty")

    def test_basic_select(self):
        result = nfl_query("SELECT COUNT(*) AS n FROM plays")
        assert result["row_count"] == 1
        assert result["rows"][0]["n"] > 0

    def test_team_filter(self):
        result = nfl_query("SELECT COUNT(*) AS n FROM plays WHERE posteam = 'KC'")
        assert result["rows"][0]["n"] > 0

    def test_aggregate_table(self):
        result = nfl_query("SELECT * FROM team_offense_stats LIMIT 1")
        assert result["row_count"] == 1
        assert "team" in result["rows"][0]

    def test_max_rows_respected(self):
        result = nfl_query("SELECT * FROM plays", max_rows=5)
        assert result["row_count"] <= 5

    def test_truncation_flag(self):
        result = nfl_query("SELECT * FROM plays", max_rows=3)
        assert result["truncated"] is True

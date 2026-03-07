"""Unit tests for ingest helpers — no network calls, uses in-memory DuckDB."""

import datetime

import duckdb
import polars as pl
import pytest

from nfl_mcp.ingest import (
    _duckdb_type_for_polars,
    _ensure_metadata_table,
    _is_loaded,
    _record_loaded,
    _reconcile_schema,
    _safe_rename,
    _write_df_to_table,
)


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def conn():
    """Fresh in-memory DuckDB connection per test."""
    c = duckdb.connect(":memory:")
    _ensure_metadata_table(c)
    yield c
    c.close()


def _small_df(**cols) -> pl.DataFrame:
    """Build a tiny Polars DataFrame from keyword column-name→list pairs."""
    return pl.DataFrame(cols)


# ── _ensure_metadata_table ─────────────────────────────────────────────────────

class TestEnsureMetadataTable:
    def test_creates_table(self):
        c = duckdb.connect(":memory:")
        _ensure_metadata_table(c)
        tables = {r[0] for r in c.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'"
        ).fetchall()}
        assert "_ingest_metadata" in tables
        c.close()

    def test_idempotent(self):
        c = duckdb.connect(":memory:")
        _ensure_metadata_table(c)
        _ensure_metadata_table(c)   # should not raise
        c.close()

    def test_expected_columns(self):
        c = duckdb.connect(":memory:")
        _ensure_metadata_table(c)
        cols = {r[1] for r in c.execute("PRAGMA table_info('_ingest_metadata')").fetchall()}
        assert {"dataset_id", "table_name", "season", "row_count", "loaded_at", "loader_fn"} <= cols
        c.close()


# ── _is_loaded / _record_loaded ────────────────────────────────────────────────

class TestIsLoaded:
    def test_returns_false_initially(self, conn):
        assert not _is_loaded(conn, "schedules", 2024)

    def test_returns_false_for_static_initially(self, conn):
        assert not _is_loaded(conn, "players")

    def test_returns_true_after_record_seasonal(self, conn):
        _record_loaded(conn, "schedules", "schedules", "load_schedules", 285, season=2024)
        assert _is_loaded(conn, "schedules", 2024)

    def test_returns_true_after_record_static(self, conn):
        _record_loaded(conn, "players", "players", "load_players", 24000)
        assert _is_loaded(conn, "players")

    def test_different_seasons_are_independent(self, conn):
        _record_loaded(conn, "injuries", "injuries", "load_injuries", 5000, season=2023)
        assert _is_loaded(conn, "injuries", 2023)
        assert not _is_loaded(conn, "injuries", 2024)

    def test_different_datasets_are_independent(self, conn):
        _record_loaded(conn, "schedules", "schedules", "load_schedules", 285, season=2024)
        assert not _is_loaded(conn, "rosters", 2024)


class TestRecordLoaded:
    def test_inserts_record(self, conn):
        _record_loaded(conn, "schedules", "schedules", "load_schedules", 285, season=2024)
        rows = conn.execute(
            "SELECT dataset_id, table_name, season, row_count, loader_fn "
            "FROM _ingest_metadata"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0] == ("schedules", "schedules", 2024, 285, "load_schedules")

    def test_upserts_on_repeat(self, conn):
        _record_loaded(conn, "schedules", "schedules", "load_schedules", 285, season=2024)
        _record_loaded(conn, "schedules", "schedules", "load_schedules", 290, season=2024)
        rows = conn.execute(
            "SELECT row_count FROM _ingest_metadata WHERE dataset_id = 'schedules'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 290     # updated, not duplicated

    def test_null_season_for_static(self, conn):
        _record_loaded(conn, "teams", "teams", "load_teams", 36)
        row = conn.execute(
            "SELECT season FROM _ingest_metadata WHERE dataset_id = 'teams'"
        ).fetchone()
        assert row[0] is None

    def test_loaded_at_is_set(self, conn):
        _record_loaded(conn, "teams", "teams", "load_teams", 36)
        ts = conn.execute(
            "SELECT loaded_at FROM _ingest_metadata WHERE dataset_id = 'teams'"
        ).fetchone()[0]
        assert isinstance(ts, datetime.datetime)
        # Timestamp should be within the last 10 seconds
        age = abs((datetime.datetime.now() - ts).total_seconds())
        assert age < 10

    def test_multiple_seasons_stored_separately(self, conn):
        for season in [2022, 2023, 2024]:
            _record_loaded(conn, "rosters", "rosters", "load_rosters", 3000, season=season)
        count = conn.execute(
            "SELECT COUNT(*) FROM _ingest_metadata WHERE dataset_id = 'rosters'"
        ).fetchone()[0]
        assert count == 3


# ── _safe_rename ───────────────────────────────────────────────────────────────

class TestSafeRename:
    def test_replaces_dots(self):
        df = pl.DataFrame({"a.b": [1]})
        result = _safe_rename(df)
        assert "a_b" in result.columns

    def test_replaces_hyphens(self):
        df = pl.DataFrame({"a-b": [1]})
        result = _safe_rename(df)
        assert "a_b" in result.columns

    def test_replaces_spaces(self):
        df = pl.DataFrame({"a b": [1]})
        result = _safe_rename(df)
        assert "a_b" in result.columns

    def test_safe_names_unchanged(self):
        df = pl.DataFrame({"season": [1], "team_abbr": [2]})
        result = _safe_rename(df)
        assert result.columns == ["season", "team_abbr"]

    def test_multiple_replacements_in_one_name(self):
        df = pl.DataFrame({"a.b-c d": [1]})
        result = _safe_rename(df)
        assert "a_b_c_d" in result.columns


# ── _duckdb_type_for_polars ────────────────────────────────────────────────────

class TestDuckdbTypeForPolars:
    @pytest.mark.parametrize("dtype,expected", [
        (pl.Int8,    "BIGINT"),
        (pl.Int16,   "BIGINT"),
        (pl.Int32,   "BIGINT"),
        (pl.Int64,   "BIGINT"),
        (pl.UInt32,  "BIGINT"),
        (pl.Float32, "DOUBLE"),
        (pl.Float64, "DOUBLE"),
        (pl.Boolean, "BOOLEAN"),
        (pl.Date,    "DATE"),
        (pl.Utf8,    "VARCHAR"),
    ])
    def test_type_mapping(self, dtype, expected):
        assert _duckdb_type_for_polars(dtype) == expected

    def test_unknown_type_returns_varchar(self):
        # Any type not explicitly mapped falls back to VARCHAR
        assert _duckdb_type_for_polars(pl.List(pl.Int32)) == "VARCHAR"


# ── _write_df_to_table ─────────────────────────────────────────────────────────

class TestWriteDfToTable:
    def test_creates_new_table(self, conn):
        df = pl.DataFrame({"season": [2024], "team": ["KC"]})
        _write_df_to_table(conn, "test_tbl", df)
        count = conn.execute("SELECT COUNT(*) FROM test_tbl").fetchone()[0]
        assert count == 1

    def test_appends_to_existing(self, conn):
        df1 = pl.DataFrame({"season": [2023]})
        df2 = pl.DataFrame({"season": [2024]})
        _write_df_to_table(conn, "test_tbl", df1)
        _write_df_to_table(conn, "test_tbl", df2)
        count = conn.execute("SELECT COUNT(*) FROM test_tbl").fetchone()[0]
        assert count == 2

    def test_replace_drops_and_recreates(self, conn):
        df1 = pl.DataFrame({"season": [2023]})
        df2 = pl.DataFrame({"season": [2024]})
        _write_df_to_table(conn, "test_tbl", df1)
        _write_df_to_table(conn, "test_tbl", df2, replace=True)
        rows = conn.execute("SELECT season FROM test_tbl").fetchall()
        assert rows == [(2024,)]     # old row gone


# ── _reconcile_schema ──────────────────────────────────────────────────────────

class TestReconcileSchema:
    def test_adds_missing_columns(self, conn):
        conn.execute("CREATE TABLE tbl (season BIGINT)")
        df = pl.DataFrame({"season": [2024], "week": [1], "team": ["KC"]})
        _reconcile_schema(conn, "tbl", df)
        cols = {r[1] for r in conn.execute("PRAGMA table_info('tbl')").fetchall()}
        assert {"season", "week", "team"} <= cols

    def test_ignores_existing_columns(self, conn):
        conn.execute("CREATE TABLE tbl (season BIGINT, team VARCHAR)")
        df = pl.DataFrame({"season": [2024], "team": ["KC"]})
        _reconcile_schema(conn, "tbl", df)   # should not raise
        cols = {r[1] for r in conn.execute("PRAGMA table_info('tbl')").fetchall()}
        assert cols == {"season", "team"}


# ── run_ingest_datasets validation ────────────────────────────────────────────

class TestRunIngestValidation:
    def test_raises_for_start_greater_than_end(self):
        from nfl_mcp.ingest import run_ingest_datasets
        with pytest.raises(ValueError, match="start must be less than or equal to end"):
            run_ingest_datasets(["pbp"], start=2025, end=2020)

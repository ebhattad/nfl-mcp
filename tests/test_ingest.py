"""Unit tests for ingest helpers — no network calls, uses in-memory DuckDB."""

import datetime
import sys
import types

import duckdb
import polars as pl
import pytest

from nfl_mcp.registry import DatasetDef
from nfl_mcp.ingest import (
    _build_enhanced_description,
    _create_aggregate_tables,
    _create_indexes,
    _create_plays_table,
    _duckdb_type_for_polars,
    _ensure_metadata_table,
    _ingest_generic_dataset,
    _ingest_pbp_season,
    _is_loaded,
    _record_loaded,
    _reconcile_schema,
    _safe_rename,
    _str,
    _write_df_to_table,
    run_ingest,
    run_ingest_datasets,
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


def _install_fake_nflreadpy(monkeypatch, **loaders):
    """Install a lightweight fake nflreadpy module for import-time patching."""
    fake = types.ModuleType("nflreadpy")
    for name, fn in loaders.items():
        setattr(fake, name, fn)
    monkeypatch.setitem(sys.modules, "nflreadpy", fake)


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

    def test_returns_false_when_metadata_table_missing(self):
        c = duckdb.connect(":memory:")
        assert _is_loaded(c, "schedules", 2024) is False
        c.close()


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
        (pl.Time,    "TIME"),
        (pl.Datetime, "TIMESTAMP"),
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


# ── _str / description helpers ────────────────────────────────────────────────

class TestStringAndDescriptionHelpers:
    @pytest.mark.parametrize("value,expected", [
        (None, ""),
        ("None", ""),
        ("nan", ""),
        ("NaN", ""),
        ("", ""),
        (42, "42"),
        ("KC", "KC"),
    ])
    def test_str_normalization(self, value, expected):
        assert _str(value) == expected

    def test_build_enhanced_description_includes_context_and_tags(self):
        row = {
            "down": 3,
            "ydstogo": 8,
            "qtr": 4,
            "season": 2024,
            "week": 9,
            "season_type": "POST",
            "posteam": "KC",
            "defteam": "BAL",
            "play_type": "pass",
            "passer_player_name": "P.Mahomes",
            "rusher_player_name": None,
            "receiver_player_name": "T.Kelce",
            "desc": "Deep middle pass complete",
            "yardline_100": 12,
            "touchdown": 1,
            "interception": 1,
            "fumble_lost": 0,
            "yards_gained": 25,
            "pass_attempt": 1,
            "rush_attempt": 0,
        }
        text = _build_enhanced_description(row)
        assert "3 & 8" in text
        assert "Week 9 2024 (POST): KC vs BAL" in text
        assert "PASS" in text
        assert "QB: P.Mahomes" in text
        assert "Receiver: T.Kelce" in text
        assert "TOUCHDOWN" in text
        assert "TURNOVER" in text
        assert "EXPLOSIVE" in text
        assert "PLAYOFFS" in text

    def test_build_enhanced_description_handles_bad_numeric_values(self):
        row = {
            "down": "bad",
            "ydstogo": "bad",
            "qtr": "bad",
            "season": "2024",
            "week": "bad",
            "season_type": "REG",
            "posteam": "KC",
            "defteam": "BAL",
            "play_type": "run",
            "desc": "Simple run",
        }
        text = _build_enhanced_description(row)
        assert "2024 (REG): KC vs BAL" in text
        assert "RUN" in text
        assert "Simple run" in text

    def test_build_enhanced_description_handles_missing_week(self):
        row = {
            "season": 2024,
            "season_type": "REG",
            "posteam": "KC",
            "defteam": "BAL",
            "play_type": "run",
        }
        text = _build_enhanced_description(row)
        assert "2024 (REG): KC vs BAL" in text

    def test_build_enhanced_description_tolerates_bad_tag_inputs(self):
        row = {
            "season": 2024,
            "season_type": "REG",
            "posteam": "KC",
            "defteam": "BAL",
            "play_type": "pass",
            "yardline_100": "bad",
            "down": "bad",
            "yards_gained": "bad",
            "pass_attempt": 1,
            "rush_attempt": 0,
        }
        text = _build_enhanced_description(row)
        assert "KC vs BAL" in text


# ── create-table / pbp ingestion ───────────────────────────────────────────────

class TestPbpIngestionHelpers:
    def test_create_plays_table_creates_schema_and_enhanced_column(self, conn):
        sample = pl.DataFrame({"a.b": [1], "team name": ["KC"]})
        _create_plays_table(conn, sample, fresh=False)
        cols = {r[1] for r in conn.execute("PRAGMA table_info('plays')").fetchall()}
        assert "a_b" in cols
        assert "team_name" in cols
        assert "enhanced_description" in cols

    def test_create_plays_table_fresh_recreates_existing_table(self, conn):
        conn.execute("CREATE TABLE plays (season BIGINT)")
        conn.execute("INSERT INTO plays VALUES (2023)")
        sample = pl.DataFrame({"season": [2024], "posteam": ["KC"], "defteam": ["BAL"]})
        _create_plays_table(conn, sample, fresh=True)
        count = conn.execute("SELECT COUNT(*) FROM plays").fetchone()[0]
        assert count == 0

    def test_create_plays_table_when_already_exists_and_not_fresh(self, conn):
        conn.execute("CREATE TABLE plays (season BIGINT)")
        sample = pl.DataFrame({"season": [2024]})
        _create_plays_table(conn, sample, fresh=False)
        cols = {r[1] for r in conn.execute("PRAGMA table_info('plays')").fetchall()}
        assert "season" in cols

    def test_ingest_pbp_season_inserts_filtered_rows(self, conn, monkeypatch):
        df = pl.DataFrame({
            "season": [2024, 2024],
            "week": [1, 1],
            "season_type": ["REG", "REG"],
            "posteam": ["KC", None],
            "defteam": ["BAL", "KC"],
            "play_type": ["pass", "run"],
            "down": [3, 1],
            "ydstogo": [8, 10],
            "qtr": [4, 2],
            "yardline_100": [15, 50],
            "passer_player_name": ["P.Mahomes", None],
            "rusher_player_name": [None, "I.Pacheco"],
            "receiver_player_name": ["T.Kelce", None],
            "desc": ["Touchdown throw", "Run up middle"],
            "touchdown": [1, 0],
            "interception": [0, 0],
            "fumble_lost": [0, 0],
            "yards_gained": [24, 3],
            "pass_attempt": [1, 0],
            "rush_attempt": [0, 1],
            "game_id": ["g1", "g1"],
        })

        _create_plays_table(conn, df, fresh=False)
        _install_fake_nflreadpy(monkeypatch, load_pbp=lambda seasons: df)

        inserted = _ingest_pbp_season(conn, 2024)
        assert inserted == 1
        row = conn.execute("SELECT posteam, enhanced_description FROM plays").fetchone()
        assert row[0] == "KC"
        assert "TOUCHDOWN" in row[1]

    def test_ingest_pbp_season_returns_zero_on_loader_error(self, conn, monkeypatch):
        _install_fake_nflreadpy(monkeypatch, load_pbp=lambda seasons: (_ for _ in ()).throw(RuntimeError("boom")))
        assert _ingest_pbp_season(conn, 2024) == 0


# ── generic dataset ingestion ───────────────────────────────────────────────────

class TestGenericDatasetIngestion:
    def test_static_dataset_success(self, conn, monkeypatch):
        defn = DatasetDef(
            dataset_id="fake_static",
            loader_fn="load_fake_static",
            table_name="fake_static",
            seasonal=False,
            default=False,
            wave=1,
        )
        _install_fake_nflreadpy(
            monkeypatch,
            load_fake_static=lambda **kwargs: pl.DataFrame({"team": ["KC"]}),
        )
        total = _ingest_generic_dataset(conn, defn, seasons=[2024], fresh=False)
        assert total == 1
        assert _is_loaded(conn, "fake_static")
        assert conn.execute("SELECT COUNT(*) FROM fake_static").fetchone()[0] == 1

    def test_static_dataset_skips_when_already_loaded(self, conn, monkeypatch):
        defn = DatasetDef(
            dataset_id="fake_static",
            loader_fn="load_fake_static",
            table_name="fake_static",
            seasonal=False,
            default=False,
            wave=1,
        )
        _record_loaded(conn, "fake_static", "fake_static", "load_fake_static", 1)
        called = []
        _install_fake_nflreadpy(
            monkeypatch,
            load_fake_static=lambda **kwargs: called.append(True) or pl.DataFrame({"x": [1]}),
        )
        total = _ingest_generic_dataset(conn, defn, seasons=[2024], fresh=False)
        assert total == 0
        assert called == []

    def test_static_dataset_handles_loader_error_and_empty(self, conn, monkeypatch):
        defn = DatasetDef(
            dataset_id="fake_static",
            loader_fn="load_fake_static",
            table_name="fake_static",
            seasonal=False,
            default=False,
            wave=1,
        )
        _install_fake_nflreadpy(
            monkeypatch,
            load_fake_static=lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        assert _ingest_generic_dataset(conn, defn, seasons=[2024], fresh=False) == 0

        _install_fake_nflreadpy(monkeypatch, load_fake_static=lambda **kwargs: pl.DataFrame({"x": []}))
        assert _ingest_generic_dataset(conn, defn, seasons=[2024], fresh=False) == 0

    def test_seasonal_dataset_covers_skip_windows_and_partial_failures(self, conn, monkeypatch):
        defn = DatasetDef(
            dataset_id="fake_seasonal",
            loader_fn="load_fake_seasonal",
            table_name="fake_seasonal",
            seasonal=True,
            default=False,
            wave=1,
            min_season=2020,
            max_season=2024,
        )
        _record_loaded(conn, "fake_seasonal", "fake_seasonal", "load_fake_seasonal", 1, season=2020)

        def loader(seasons, **kwargs):
            season = seasons[0]
            if season == 2021:
                raise RuntimeError("transient loader failure")
            if season == 2023:
                return pl.DataFrame({"season": [], "value": []})
            return pl.DataFrame({"season": [season], "value": [season]})

        _install_fake_nflreadpy(monkeypatch, load_fake_seasonal=loader)
        total = _ingest_generic_dataset(
            conn,
            defn,
            seasons=[2019, 2020, 2021, 2023, 2024, 2025],
            fresh=False,
        )
        assert total == 1
        loaded_2024 = _is_loaded(conn, "fake_seasonal", 2024)
        loaded_2020 = _is_loaded(conn, "fake_seasonal", 2020)
        assert loaded_2024 is True
        assert loaded_2020 is True

    def test_seasonal_fresh_deletes_existing_rows_and_reinserts(self, conn, monkeypatch):
        defn = DatasetDef(
            dataset_id="fake_seasonal",
            loader_fn="load_fake_seasonal",
            table_name="fake_seasonal",
            seasonal=True,
            default=False,
            wave=1,
        )
        conn.execute("CREATE TABLE fake_seasonal (season BIGINT, value BIGINT)")
        conn.execute("INSERT INTO fake_seasonal VALUES (2024, 1), (2024, 2), (2023, 9)")

        _install_fake_nflreadpy(
            monkeypatch,
            load_fake_seasonal=lambda seasons, **kwargs: pl.DataFrame({"season": [2024], "value": [99]}),
        )
        total = _ingest_generic_dataset(conn, defn, seasons=[2024], fresh=True)
        assert total == 1
        rows = conn.execute(
            "SELECT season, value FROM fake_seasonal WHERE season = 2024 ORDER BY value"
        ).fetchall()
        assert rows == [(2024, 99)]

    def test_seasonal_fresh_ignores_missing_table_on_delete(self, conn, monkeypatch):
        defn = DatasetDef(
            dataset_id="fake_missing_table",
            loader_fn="load_fake_missing_table",
            table_name="fake_missing_table",
            seasonal=True,
            default=False,
            wave=1,
        )
        _install_fake_nflreadpy(
            monkeypatch,
            load_fake_missing_table=lambda seasons, **kwargs: pl.DataFrame({"season": [2024], "value": [1]}),
        )
        total = _ingest_generic_dataset(conn, defn, seasons=[2024], fresh=True)
        assert total == 1


# ── aggregate tables and indexes ────────────────────────────────────────────────

class TestAggregateAndIndexBuilders:
    def test_create_aggregate_tables_from_minimal_plays(self, conn):
        plays = pl.DataFrame({
            "posteam": ["KC", "KC", "KC", "KC", "KC", "BAL"],
            "defteam": ["BAL", "BAL", "BAL", "BAL", "BAL", "KC"],
            "season": [2024, 2024, 2024, 2024, 2024, 2024],
            "yards_gained": [5, 8, 12, 20, 25, 3],
            "rush_attempt": [1, 0, 1, 0, 0, 0],
            "pass_attempt": [0, 1, 0, 1, 1, 1],
            "touchdown": [0, 0, 0, 1, 0, 0],
            "interception": [0, 0, 0, 0, 0, 1],
            "fumble_lost": [0, 0, 0, 0, 0, 0],
            "down": [1, 3, 3, 4, 3, 3],
            "ydstogo": [10, 7, 2, 1, 8, 5],
            "yardline_100": [80, 45, 18, 10, 30, 40],
            "epa": [0.1, 0.2, 0.4, 1.1, 0.8, -0.6],
            "sack": [0, 0, 0, 1, 0, 0],
            "qtr": [1, 2, 4, 4, 4, 2],
            "time": ["10:00", "08:00", "01:30", "05:00", "00:45", "09:15"],
            "shotgun": [1, 1, 1, 1, 1, 0],
            "no_huddle": [0, 1, 0, 0, 1, 0],
            "play_type": ["run", "pass", "run", "pass", "pass", "pass"],
            "game_id": ["g1", "g1", "g1", "g1", "g1", "g1"],
        })
        _write_df_to_table(conn, "plays", plays, replace=True)

        _create_aggregate_tables(conn)
        created = {r[0] for r in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()}
        assert "team_offense_stats" in created
        assert "team_defense_stats" in created
        assert "situational_stats" in created
        assert "formation_effectiveness" in created

    def test_create_indexes_handles_missing_columns_gracefully(self, conn):
        conn.execute("CREATE TABLE plays (season BIGINT)")
        _create_indexes(conn)  # should not raise even though many indexes will fail

    def test_create_indexes_on_complete_schema(self, conn):
        conn.execute(
            "CREATE TABLE plays (season BIGINT, posteam VARCHAR, defteam VARCHAR, game_id VARCHAR, play_type VARCHAR)"
        )
        _create_indexes(conn)
        idx = conn.execute(
            "SELECT COUNT(*) FROM duckdb_indexes() WHERE table_name = 'plays'"
        ).fetchone()[0]
        assert idx > 0


# ── run_ingest_datasets orchestration ──────────────────────────────────────────

class TestRunIngestOrchestration:
    class _DummyResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    class _DummyConn:
        def __init__(self, loaded_rows):
            self.loaded_rows = loaded_rows
            self.closed = False
            self.queries = []

        def execute(self, sql):
            self.queries.append(sql)
            return TestRunIngestOrchestration._DummyResult(self.loaded_rows)

        def close(self):
            self.closed = True

    def test_run_ingest_datasets_orchestrates_pbp_and_other_datasets(self, monkeypatch, tmp_path):
        conn = self._DummyConn(loaded_rows=[(2023,)])
        calls = {"pbp": [], "record": [], "agg": 0, "idx": 0, "other": []}

        _install_fake_nflreadpy(
            monkeypatch,
            load_pbp=lambda seasons: pl.DataFrame({"season": [2024], "posteam": ["KC"], "defteam": ["BAL"]}),
        )
        monkeypatch.setattr("nfl_mcp.ingest.duckdb.connect", lambda path: conn)
        monkeypatch.setattr("nfl_mcp.ingest._ensure_metadata_table", lambda c: None)
        monkeypatch.setattr("nfl_mcp.ingest._create_plays_table", lambda c, df, fresh=False: None)
        monkeypatch.setattr("nfl_mcp.ingest._ingest_pbp_season", lambda c, season: calls["pbp"].append(season) or 11)
        monkeypatch.setattr(
            "nfl_mcp.ingest._record_loaded",
            lambda c, dataset_id, table_name, loader_fn, row_count, season=None: calls["record"].append(
                (dataset_id, table_name, loader_fn, row_count, season)
            ),
        )
        monkeypatch.setattr("nfl_mcp.ingest._create_indexes", lambda c: calls.__setitem__("idx", calls["idx"] + 1))
        monkeypatch.setattr(
            "nfl_mcp.ingest._create_aggregate_tables",
            lambda c: calls.__setitem__("agg", calls["agg"] + 1),
        )
        monkeypatch.setattr(
            "nfl_mcp.ingest._ingest_generic_dataset",
            lambda c, defn, seasons, fresh=False: calls["other"].append((defn.dataset_id, seasons, fresh)) or 5,
        )

        run_ingest_datasets(
            dataset_ids=["pbp", "unknown_dataset", "schedules"],
            start=2024,
            end=2024,
            fresh=False,
            skip_views=False,
            db_path=str(tmp_path / "nflread.duckdb"),
        )

        assert calls["pbp"] == [2024]
        assert calls["record"] == [("pbp", "plays", "load_pbp", 11, 2024)]
        assert calls["idx"] == 1
        assert calls["agg"] == 1
        assert calls["other"] and calls["other"][0][0] == "schedules"
        assert conn.closed is True

    def test_run_ingest_datasets_skips_pbp_when_already_loaded(self, monkeypatch, tmp_path):
        conn = self._DummyConn(loaded_rows=[(2024,)])
        pbp_calls = []
        agg_calls = []

        _install_fake_nflreadpy(
            monkeypatch,
            load_pbp=lambda seasons: pl.DataFrame({"season": [2024], "posteam": ["KC"], "defteam": ["BAL"]}),
        )
        monkeypatch.setattr("nfl_mcp.ingest.duckdb.connect", lambda path: conn)
        monkeypatch.setattr("nfl_mcp.ingest._ensure_metadata_table", lambda c: None)
        monkeypatch.setattr("nfl_mcp.ingest._create_plays_table", lambda c, df, fresh=False: None)
        monkeypatch.setattr("nfl_mcp.ingest._ingest_pbp_season", lambda c, season: pbp_calls.append(season) or 1)
        monkeypatch.setattr("nfl_mcp.ingest._create_indexes", lambda c: None)
        monkeypatch.setattr("nfl_mcp.ingest._create_aggregate_tables", lambda c: agg_calls.append(True))

        run_ingest_datasets(
            dataset_ids=["pbp"],
            start=2024,
            end=2024,
            fresh=False,
            skip_views=True,
            db_path=str(tmp_path / "nflread.duckdb"),
        )

        assert pbp_calls == []
        assert agg_calls == []
        assert conn.closed is True

    def test_run_ingest_datasets_without_pbp_only_runs_generic(self, monkeypatch, tmp_path):
        conn = self._DummyConn(loaded_rows=[])
        generic_calls = []

        _install_fake_nflreadpy(monkeypatch, load_pbp=lambda seasons: pl.DataFrame({"season": [2024]}))
        monkeypatch.setattr("nfl_mcp.ingest.duckdb.connect", lambda path: conn)
        monkeypatch.setattr("nfl_mcp.ingest._ensure_metadata_table", lambda c: None)
        monkeypatch.setattr(
            "nfl_mcp.ingest._ingest_generic_dataset",
            lambda c, defn, seasons, fresh=False: generic_calls.append(defn.dataset_id) or 1,
        )

        run_ingest_datasets(
            dataset_ids=["schedules"],
            start=2024,
            end=2024,
            db_path=str(tmp_path / "nflread.duckdb"),
        )
        assert generic_calls == ["schedules"]
        assert conn.closed is True

    def test_run_ingest_wrapper_calls_dataset_entrypoint(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            "nfl_mcp.ingest.run_ingest_datasets",
            lambda **kwargs: captured.update(kwargs),
        )
        run_ingest(start=2020, end=2021, fresh=True, skip_views=True, db_path="/tmp/custom.duckdb")
        assert captured["dataset_ids"] == ["pbp"]
        assert captured["start"] == 2020
        assert captured["end"] == 2021
        assert captured["fresh"] is True
        assert captured["skip_views"] is True
        assert captured["db_path"] == "/tmp/custom.duckdb"


# ── run_ingest_datasets validation ────────────────────────────────────────────

class TestRunIngestValidation:
    def test_raises_for_start_greater_than_end(self):
        with pytest.raises(ValueError, match="start must be less than or equal to end"):
            run_ingest_datasets(["pbp"], start=2025, end=2020)

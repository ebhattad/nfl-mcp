"""Unit tests for the nflverse freshness probe — no network required."""

import json
import urllib.error

import duckdb
import pytest

from nfl_mcp import freshness

# ── helpers ────────────────────────────────────────────────────────────────────

class _FakeResponse:
    """Stand-in for the object urllib hands back, usable as a context manager."""

    def __init__(self, payload):
        self._body = json.dumps(payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self, *args):
        return self._body


def _release(*asset_names, updated="2026-09-14T09:00:00Z"):
    return {"assets": [{"name": n, "updated_at": updated} for n in asset_names]}


def _patch_urlopen(monkeypatch, result):
    """Point urlopen at a payload, or at an exception to raise."""
    def fake_urlopen(request, timeout=None):
        if isinstance(result, Exception):
            raise result
        return _FakeResponse(result)

    monkeypatch.setattr(freshness.urllib.request, "urlopen", fake_urlopen)


def _seed_metadata(db_path, season=2026, stamp=None):
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE _ingest_metadata (
            dataset_id VARCHAR NOT NULL, table_name VARCHAR NOT NULL,
            season INTEGER, row_count BIGINT, loaded_at TIMESTAMP NOT NULL,
            loader_fn VARCHAR, source_updated_at VARCHAR
        )
    """)
    conn.execute(
        "INSERT INTO _ingest_metadata VALUES ('pbp','plays',?,10,now(),'load_pbp',?)",
        [season, stamp],
    )
    conn.close()


# ── pbp_updated_at ─────────────────────────────────────────────────────────────

class TestPbpUpdatedAt:
    def test_returns_timestamp_for_matching_asset(self, monkeypatch):
        _patch_urlopen(monkeypatch, _release("play_by_play_2026.parquet"))
        assert freshness.pbp_updated_at(2026) == "2026-09-14T09:00:00Z"

    def test_ignores_other_seasons(self, monkeypatch):
        _patch_urlopen(monkeypatch, _release("play_by_play_2025.parquet"))
        assert freshness.pbp_updated_at(2026) is None

    def test_returns_none_when_release_has_no_assets(self, monkeypatch):
        _patch_urlopen(monkeypatch, {})
        assert freshness.pbp_updated_at(2026) is None

    @pytest.mark.parametrize("error", [
        urllib.error.URLError("offline"),
        urllib.error.HTTPError("u", 403, "rate limited", {}, None),
        TimeoutError("slow"),
        OSError("socket blew up"),
    ])
    def test_network_failures_are_unknown_not_fatal(self, monkeypatch, error):
        _patch_urlopen(monkeypatch, error)
        assert freshness.pbp_updated_at(2026) is None

    def test_malformed_json_is_unknown(self, monkeypatch):
        class _Garbage:
            def __enter__(self): return self
            def __exit__(self, *exc): return False
            def read(self, *a): return b"not json"

        monkeypatch.setattr(freshness.urllib.request, "urlopen",
                            lambda request, timeout=None: _Garbage())
        assert freshness.pbp_updated_at(2026) is None

    def test_sends_a_user_agent(self, monkeypatch):
        seen = {}

        def fake_urlopen(request, timeout=None):
            seen["ua"] = request.get_header("User-agent")
            return _FakeResponse(_release("play_by_play_2026.parquet"))

        monkeypatch.setattr(freshness.urllib.request, "urlopen", fake_urlopen)
        freshness.pbp_updated_at(2026)
        assert "nfl-mcp" in seen["ua"]


# ── read / write stamp ─────────────────────────────────────────────────────────

class TestStampPersistence:
    def test_missing_database_reads_none(self, tmp_path):
        assert freshness.read_pbp_stamp(str(tmp_path / "nope.duckdb"), 2026) is None

    def test_unreadable_database_reads_none(self, tmp_path):
        junk = tmp_path / "junk.duckdb"
        junk.write_text("definitely not a duckdb file")
        assert freshness.read_pbp_stamp(str(junk), 2026) is None

    def test_database_without_metadata_table_reads_none(self, tmp_path):
        db = tmp_path / "bare.duckdb"
        duckdb.connect(str(db)).close()
        assert freshness.read_pbp_stamp(str(db), 2026) is None

    def test_reads_recorded_stamp(self, tmp_path):
        db = tmp_path / "m.duckdb"
        _seed_metadata(db, 2026, "2026-09-14T09:00:00Z")
        assert freshness.read_pbp_stamp(str(db), 2026) == "2026-09-14T09:00:00Z"

    def test_unknown_season_reads_none(self, tmp_path):
        db = tmp_path / "m.duckdb"
        _seed_metadata(db, 2026, "2026-09-14T09:00:00Z")
        assert freshness.read_pbp_stamp(str(db), 2013) is None

    def test_write_then_read_round_trips(self, tmp_path):
        db = tmp_path / "m.duckdb"
        _seed_metadata(db, 2026, None)
        freshness.write_pbp_stamp(str(db), 2026, "2026-09-21T09:00:00Z")
        assert freshness.read_pbp_stamp(str(db), 2026) == "2026-09-21T09:00:00Z"

    def test_write_adds_column_to_legacy_database(self, tmp_path):
        """Databases baked before this feature have no source_updated_at column."""
        db = tmp_path / "legacy.duckdb"
        conn = duckdb.connect(str(db))
        conn.execute("""
            CREATE TABLE _ingest_metadata (
                dataset_id VARCHAR NOT NULL, table_name VARCHAR NOT NULL,
                season INTEGER, row_count BIGINT, loaded_at TIMESTAMP NOT NULL,
                loader_fn VARCHAR
            )
        """)
        conn.execute("INSERT INTO _ingest_metadata VALUES ('pbp','plays',2026,10,now(),'load_pbp')")
        conn.close()

        freshness.write_pbp_stamp(str(db), 2026, "2026-09-14T09:00:00Z")
        assert freshness.read_pbp_stamp(str(db), 2026) == "2026-09-14T09:00:00Z"

    def test_write_is_a_noop_when_season_was_never_ingested(self, tmp_path):
        db = tmp_path / "m.duckdb"
        _seed_metadata(db, 2026, None)
        freshness.write_pbp_stamp(str(db), 2013, "2026-09-14T09:00:00Z")
        assert freshness.read_pbp_stamp(str(db), 2013) is None


# ── is_stale ───────────────────────────────────────────────────────────────────

class TestIsStale:
    def test_unknown_remote_is_not_stale(self, tmp_path, monkeypatch):
        monkeypatch.setattr(freshness, "pbp_updated_at", lambda s, timeout=15.0: None)
        assert freshness.is_stale(str(tmp_path / "x.duckdb"), 2026) == (False, None)

    def test_new_remote_stamp_is_stale(self, tmp_path, monkeypatch):
        db = tmp_path / "m.duckdb"
        _seed_metadata(db, 2026, "2026-09-14T09:00:00Z")
        monkeypatch.setattr(freshness, "pbp_updated_at",
                            lambda s, timeout=15.0: "2026-09-21T09:00:00Z")
        assert freshness.is_stale(str(db), 2026) == (True, "2026-09-21T09:00:00Z")

    def test_matching_stamp_is_not_stale(self, tmp_path, monkeypatch):
        db = tmp_path / "m.duckdb"
        _seed_metadata(db, 2026, "2026-09-14T09:00:00Z")
        monkeypatch.setattr(freshness, "pbp_updated_at",
                            lambda s, timeout=15.0: "2026-09-14T09:00:00Z")
        assert freshness.is_stale(str(db), 2026) == (False, "2026-09-14T09:00:00Z")

    def test_never_ingested_season_is_stale(self, tmp_path, monkeypatch):
        monkeypatch.setattr(freshness, "pbp_updated_at",
                            lambda s, timeout=15.0: "2026-09-14T09:00:00Z")
        stale, stamp = freshness.is_stale(str(tmp_path / "absent.duckdb"), 2026)
        assert (stale, stamp) == (True, "2026-09-14T09:00:00Z")

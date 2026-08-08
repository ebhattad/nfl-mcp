"""Unit tests for in-season updates — no database, network or server required."""

import threading

import pytest

from nfl_mcp import updater

# ── helpers ────────────────────────────────────────────────────────────────────

def _patch_env(monkeypatch, *, stale=True, stamp="2026-09-14T09:00:00Z", ingest=None):
    """Stub the freshness probe and the ingest so nothing touches the network."""
    calls = {"ingest": [], "stamped": []}

    monkeypatch.setattr("nfl_mcp.freshness.is_stale",
                        lambda path, season: (stale, stamp))
    monkeypatch.setattr("nfl_mcp.freshness.write_pbp_stamp",
                        lambda path, season, value: calls["stamped"].append((path, season, value)))

    def default_ingest(**kwargs):
        calls["ingest"].append(kwargs)

    monkeypatch.setattr("nfl_mcp.ingest.run_ingest_datasets", ingest or default_ingest)
    return calls


def _patch_defaults(monkeypatch, tmp_path, season=2026):
    monkeypatch.setattr("nfl_mcp.seasons.current_season", lambda: season)
    monkeypatch.setattr("nfl_mcp.config.get_duckdb_path", lambda: tmp_path / "live.duckdb")
    monkeypatch.setattr("nfl_mcp.registry.DEFAULT_DATASETS", ["pbp", "schedules"])


# ── parse_interval ─────────────────────────────────────────────────────────────

class TestParseInterval:
    @pytest.mark.parametrize("value,expected", [
        ("60", 60), ("90s", 90), ("30m", 1800), ("2h", 7200),
        ("  30M  ", 1800), (3600, 3600),
    ])
    def test_accepted_forms(self, value, expected):
        assert updater.parse_interval(value) == expected

    @pytest.mark.parametrize("value", ["", "soon", "5x", "m", "1.5h", "-30m", "0m"])
    def test_rejects_nonsense(self, value):
        with pytest.raises(ValueError, match="invalid interval"):
            updater.parse_interval(value)

    @pytest.mark.parametrize("value", ["1s", "59s", "30"])
    def test_rejects_below_minimum(self, value):
        with pytest.raises(ValueError, match="minimum"):
            updater.parse_interval(value)


# ── _resolve ───────────────────────────────────────────────────────────────────

class TestResolve:
    def test_fills_in_defaults(self, monkeypatch, tmp_path):
        _patch_defaults(monkeypatch, tmp_path)
        season, datasets, path = updater._resolve(None, None, None)
        assert season == 2026
        assert datasets == ["pbp", "schedules"]
        assert path == str(tmp_path / "live.duckdb")

    def test_explicit_values_win(self, monkeypatch, tmp_path):
        _patch_defaults(monkeypatch, tmp_path)
        season, datasets, path = updater._resolve(2024, ["injuries"], "/tmp/x.duckdb")
        assert (season, datasets, path) == (2024, ["injuries"], "/tmp/x.duckdb")


# ── run_update ─────────────────────────────────────────────────────────────────

class TestRunUpdate:
    def test_skips_when_nothing_new(self, monkeypatch, tmp_path, capsys):
        _patch_defaults(monkeypatch, tmp_path)
        calls = _patch_env(monkeypatch, stale=False, stamp=None)
        assert updater.run_update() is False
        assert calls["ingest"] == []
        assert "nothing new" in capsys.readouterr().out

    def test_ingests_when_stale(self, monkeypatch, tmp_path):
        _patch_defaults(monkeypatch, tmp_path)
        calls = _patch_env(monkeypatch)
        assert updater.run_update() is True
        kwargs = calls["ingest"][0]
        assert kwargs["refresh"] is True
        assert kwargs["start"] == kwargs["end"] == 2026
        assert kwargs["dataset_ids"] == ["pbp", "schedules"]

    def test_never_passes_fresh(self, monkeypatch, tmp_path):
        """--fresh drops the whole plays table; updates must not use it."""
        _patch_defaults(monkeypatch, tmp_path)
        calls = _patch_env(monkeypatch)
        updater.run_update()
        assert calls["ingest"][0].get("fresh") in (None, False)

    def test_force_ingests_even_when_fresh_data_absent(self, monkeypatch, tmp_path):
        _patch_defaults(monkeypatch, tmp_path)
        calls = _patch_env(monkeypatch, stale=False, stamp=None)
        assert updater.run_update(force=True) is True
        assert len(calls["ingest"]) == 1

    def test_records_stamp_after_success(self, monkeypatch, tmp_path):
        _patch_defaults(monkeypatch, tmp_path)
        calls = _patch_env(monkeypatch)
        updater.run_update()
        assert calls["stamped"] == [(str(tmp_path / "live.duckdb"), 2026, "2026-09-14T09:00:00Z")]

    def test_no_stamp_written_when_probe_had_none(self, monkeypatch, tmp_path):
        _patch_defaults(monkeypatch, tmp_path)
        calls = _patch_env(monkeypatch, stale=False, stamp=None)
        updater.run_update(force=True)
        assert calls["stamped"] == []


# ── run_update_swap ────────────────────────────────────────────────────────────

class TestRunUpdateSwap:
    def test_skips_without_copying_when_nothing_new(self, monkeypatch, tmp_path):
        _patch_defaults(monkeypatch, tmp_path)
        _patch_env(monkeypatch, stale=False, stamp=None)
        live = tmp_path / "live.duckdb"
        live.write_text("original")

        assert updater.run_update_swap() is False
        assert live.read_text() == "original"
        assert not (tmp_path / "live.duckdb.updating").exists()

    def test_ingests_into_staging_then_swaps(self, monkeypatch, tmp_path):
        _patch_defaults(monkeypatch, tmp_path)
        live = tmp_path / "live.duckdb"
        live.write_text("original")
        staging = tmp_path / "live.duckdb.updating"

        def ingest(**kwargs):
            # The ingest must be pointed at the copy, never the live file.
            assert kwargs["db_path"] == str(staging)
            assert staging.read_text() == "original"
            staging.write_text("refreshed")

        _patch_env(monkeypatch, ingest=ingest)

        assert updater.run_update_swap() is True
        assert live.read_text() == "refreshed"
        assert not staging.exists()

    def test_creates_database_when_none_exists_yet(self, monkeypatch, tmp_path):
        _patch_defaults(monkeypatch, tmp_path)
        staging = tmp_path / "live.duckdb.updating"
        _patch_env(monkeypatch, ingest=lambda **kw: staging.write_text("brand new"))

        assert updater.run_update_swap() is True
        assert (tmp_path / "live.duckdb").read_text() == "brand new"

    def test_failed_ingest_leaves_live_database_untouched(self, monkeypatch, tmp_path):
        _patch_defaults(monkeypatch, tmp_path)
        live = tmp_path / "live.duckdb"
        live.write_text("original")

        def boom(**kwargs):
            raise RuntimeError("nflverse fell over")

        _patch_env(monkeypatch, ingest=boom)

        with pytest.raises(RuntimeError, match="nflverse fell over"):
            updater.run_update_swap()
        assert live.read_text() == "original"
        assert not (tmp_path / "live.duckdb.updating").exists()

    def test_stale_staging_from_a_crash_is_discarded(self, monkeypatch, tmp_path):
        _patch_defaults(monkeypatch, tmp_path)
        live = tmp_path / "live.duckdb"
        live.write_text("original")
        staging = tmp_path / "live.duckdb.updating"
        staging.write_text("leftover junk")
        staging.with_name(staging.name + ".wal").write_text("leftover wal")

        _patch_env(monkeypatch, ingest=lambda **kw: None)
        updater.run_update_swap()
        # Copied fresh from live, not resumed from the crashed attempt.
        assert live.read_text() == "original"
        assert not staging.with_name(staging.name + ".wal").exists()


# ── _discard ───────────────────────────────────────────────────────────────────

class TestDiscard:
    def test_removes_database_and_wal(self, tmp_path):
        staging = tmp_path / "s.duckdb"
        staging.write_text("db")
        wal = tmp_path / "s.duckdb.wal"
        wal.write_text("wal")
        updater._discard(staging)
        assert not staging.exists() and not wal.exists()

    def test_tolerates_missing_files(self, tmp_path):
        updater._discard(tmp_path / "never-existed.duckdb")


# ── watch ──────────────────────────────────────────────────────────────────────

class TestWatch:
    def test_stops_when_event_is_set(self, monkeypatch, tmp_path):
        _patch_defaults(monkeypatch, tmp_path)
        stop = threading.Event()
        runs = []

        monkeypatch.setattr(updater, "run_update",
                            lambda **kw: runs.append(1) or stop.set())
        updater.watch(0.01, stop=stop)
        assert len(runs) == 1

    def test_uses_swap_variant_when_asked(self, monkeypatch, tmp_path):
        stop = threading.Event()
        used = []
        monkeypatch.setattr(updater, "run_update_swap",
                            lambda **kw: used.append("swap") or stop.set())
        updater.watch(0.01, swap=True, stop=stop)
        assert used == ["swap"]

    def test_failure_does_not_end_the_loop(self, monkeypatch, capsys):
        stop = threading.Event()
        attempts = []

        def flaky(**kwargs):
            attempts.append(1)
            if len(attempts) == 1:
                raise RuntimeError("transient")
            stop.set()

        monkeypatch.setattr(updater, "run_update", flaky)
        updater.watch(0.01, stop=stop)
        assert len(attempts) == 2
        assert "retrying next cycle" in capsys.readouterr().out

    def test_sleeps_between_cycles_without_a_stop_event(self, monkeypatch):
        runs = []

        def fake_sleep(seconds):
            raise KeyboardInterrupt

        monkeypatch.setattr(updater.time, "sleep", fake_sleep)
        monkeypatch.setattr(updater, "run_update", lambda **kw: runs.append(1))
        with pytest.raises(KeyboardInterrupt):
            updater.watch(1800)
        assert len(runs) == 1


# ── start_background_updater ───────────────────────────────────────────────────

class TestStartBackgroundUpdater:
    def test_starts_a_daemon_thread_and_can_be_stopped(self, monkeypatch):
        seen = []
        monkeypatch.setattr(updater, "run_update_swap", lambda **kw: seen.append(kw))

        thread, stop = updater.start_background_updater(60)
        try:
            assert thread.daemon is True
            assert thread.name == "nfl-mcp-updater"
        finally:
            stop.set()
            thread.join(timeout=5)
        assert not thread.is_alive()
        assert seen, "the updater should have run at least one cycle"

"""Unit tests for CLI commands — no database or network required."""

import pytest
from click.testing import CliRunner

from nfl_mcp.cli import main
from nfl_mcp.registry import ALL_DATASETS, DEFAULT_DATASETS, REGISTRY


@pytest.fixture
def runner():
    return CliRunner()


# ── ingest --list ──────────────────────────────────────────────────────────────

class TestIngestList:
    def test_list_exits_zero(self, runner):
        result = runner.invoke(main, ["ingest", "--list"])
        assert result.exit_code == 0

    def test_list_shows_all_dataset_ids(self, runner):
        result = runner.invoke(main, ["ingest", "--list"])
        for ds_id in ALL_DATASETS:
            assert ds_id in result.output

    def test_list_marks_default_datasets(self, runner):
        result = runner.invoke(main, ["ingest", "--list"])
        assert "[default]" in result.output

    def test_list_marks_wave_datasets(self, runner):
        result = runner.invoke(main, ["ingest", "--list"])
        assert "[wave 2]" in result.output
        assert "[wave 3]" in result.output

    def test_list_does_not_start_ingest(self, runner, monkeypatch):
        called = []
        monkeypatch.setattr("nfl_mcp.ingest.run_ingest_datasets",
                            lambda *a, **kw: called.append(True))
        runner.invoke(main, ["ingest", "--list"])
        assert not called


# ── ingest --dataset resolution ───────────────────────────────────────────────

class TestDatasetResolution:
    def test_unknown_dataset_exits_nonzero(self, runner):
        result = runner.invoke(main, ["ingest", "--dataset", "not_a_real_dataset",
                                      "--skip-views"])
        assert result.exit_code != 0

    def test_unknown_dataset_error_message(self, runner):
        result = runner.invoke(main, ["ingest", "--dataset", "not_a_real_dataset",
                                      "--skip-views"])
        assert "Unknown dataset" in result.output
        assert "not_a_real_dataset" in result.output

    def test_all_resolves_to_every_dataset(self, runner, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            "nfl_mcp.ingest.run_ingest_datasets",
            lambda dataset_ids, **kw: captured.update({"ids": dataset_ids}),
        )
        runner.invoke(main, ["ingest", "--dataset", "all"])
        assert set(captured["ids"]) == set(ALL_DATASETS)

    def test_default_resolves_to_default_datasets(self, runner, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            "nfl_mcp.ingest.run_ingest_datasets",
            lambda dataset_ids, **kw: captured.update({"ids": dataset_ids}),
        )
        runner.invoke(main, ["ingest", "--dataset", "default"])
        assert set(captured["ids"]) == set(DEFAULT_DATASETS)

    def test_specific_dataset_is_passed_through(self, runner, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            "nfl_mcp.ingest.run_ingest_datasets",
            lambda dataset_ids, **kw: captured.update({"ids": dataset_ids}),
        )
        runner.invoke(main, ["ingest", "--dataset", "schedules"])
        assert captured["ids"] == ["schedules"]

    def test_multiple_datasets_accumulate(self, runner, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            "nfl_mcp.ingest.run_ingest_datasets",
            lambda dataset_ids, **kw: captured.update({"ids": dataset_ids}),
        )
        runner.invoke(main, ["ingest", "--dataset", "pbp", "--dataset", "injuries"])
        assert set(captured["ids"]) == {"pbp", "injuries"}

    def test_deduplication(self, runner, monkeypatch):
        """Specifying the same dataset twice should not duplicate it."""
        captured = {}
        monkeypatch.setattr(
            "nfl_mcp.ingest.run_ingest_datasets",
            lambda dataset_ids, **kw: captured.update({"ids": dataset_ids}),
        )
        runner.invoke(main, ["ingest", "--dataset", "schedules", "--dataset", "schedules"])
        assert captured["ids"].count("schedules") == 1


# ── ingest season range validation ────────────────────────────────────────────

class TestSeasonRangeValidation:
    def test_start_greater_than_end_exits_nonzero(self, runner):
        result = runner.invoke(main, ["ingest", "--start", "2025", "--end", "2020"])
        assert result.exit_code != 0

    def test_start_greater_than_end_error_message(self, runner):
        result = runner.invoke(main, ["ingest", "--start", "2025", "--end", "2020"])
        assert "start" in result.output.lower() or "end" in result.output.lower()

    def test_equal_start_end_is_valid(self, runner, monkeypatch):
        monkeypatch.setattr(
            "nfl_mcp.ingest.run_ingest_datasets",
            lambda *a, **kw: None,
        )
        result = runner.invoke(main, ["ingest", "--dataset", "schedules",
                                      "--start", "2024", "--end", "2024"])
        assert result.exit_code == 0

    def test_no_start_end_passes_none(self, runner, monkeypatch):
        """Omitting --start/--end should pass start=None, end=None (bulk mode)."""
        captured = {}
        monkeypatch.setattr(
            "nfl_mcp.ingest.run_ingest_datasets",
            lambda dataset_ids, start, end, **kw: captured.update(
                {"start": start, "end": end}
            ),
        )
        runner.invoke(main, ["ingest", "--dataset", "schedules"])
        assert captured.get("start") is None
        assert captured.get("end") is None

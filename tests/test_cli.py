"""Unit tests for CLI commands — no database or network required."""

import json

import pytest
from click.testing import CliRunner

import nfl_mcp.cli as cli
from nfl_mcp.cli import main
from nfl_mcp.registry import ALL_DATASETS, DEFAULT_DATASETS, REGISTRY


@pytest.fixture
def runner():
    return CliRunner()


# ── serve ───────────────────────────────────────────────────────────────────────

class TestServe:
    def test_serve_starts_uvicorn(self, runner, monkeypatch):
        called = {}

        def fake_uvicorn_run(app, host, port):
            called["host"] = host
            called["port"] = port

        monkeypatch.setattr("nfl_mcp.cli.uvicorn.run", fake_uvicorn_run)

        result = runner.invoke(main, ["serve", "--host", "127.0.0.1", "--port", "9000"])
        assert result.exit_code == 0
        assert called.get("host") == "127.0.0.1"
        assert called.get("port") == 9000


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


# ── init / setup-client routing ────────────────────────────────────────────────

class TestInitAndSetupClient:
    def test_init_start_greater_than_end_exits_nonzero(self, runner):
        result = runner.invoke(main, ["init", "--start", "2025", "--end", "2024", "--skip-ingest"])
        assert result.exit_code != 0

    def test_init_skip_ingest_saves_config_and_skips_client_setup(self, runner, monkeypatch, tmp_path):
        saved = {}
        setup_calls = []

        monkeypatch.setattr("nfl_mcp.config.load_config", lambda: {})
        def _save_config(cfg):
            saved["config"] = cfg
            return tmp_path / "config.json"
        monkeypatch.setattr(
            "nfl_mcp.config.save_config",
            _save_config,
        )
        monkeypatch.setattr("nfl_mcp.cli._setup_client_interactive", lambda cfg: setup_calls.append(cfg))

        # Prompts: change DB path? -> no ; configure client? -> no
        result = runner.invoke(
            main,
            ["init", "--start", "2024", "--end", "2024", "--skip-ingest"],
            input="n\nn\n",
        )

        assert result.exit_code == 0
        assert "duckdb_path" in saved["config"]
        assert setup_calls == []

    def test_init_uses_custom_db_path_prompt_value(self, runner, monkeypatch, tmp_path):
        saved = {}
        monkeypatch.setattr("nfl_mcp.config.load_config", lambda: {})
        def _save_config(cfg):
            saved["config"] = cfg
            return tmp_path / "config.json"

        monkeypatch.setattr("nfl_mcp.config.save_config", _save_config)
        monkeypatch.setattr("nfl_mcp.cli._setup_client_interactive", lambda cfg: None)

        result = runner.invoke(
            main,
            ["init", "--skip-ingest"],
            input="y\n/tmp/custom-nfl.duckdb\nn\n",
        )

        assert result.exit_code == 0
        assert saved["config"]["duckdb_path"] == "/tmp/custom-nfl.duckdb"

    def test_init_runs_ingest_when_confirmed(self, runner, monkeypatch, tmp_path):
        captured = {}

        monkeypatch.setattr("nfl_mcp.config.load_config", lambda: {})
        monkeypatch.setattr("nfl_mcp.config.save_config", lambda cfg: tmp_path / "config.json")
        monkeypatch.setattr(
            "nfl_mcp.ingest.run_ingest_datasets",
            lambda dataset_ids, start, end, fresh, skip_views: captured.update(
                {
                    "dataset_ids": dataset_ids,
                    "start": start,
                    "end": end,
                    "fresh": fresh,
                    "skip_views": skip_views,
                }
            ),
        )
        monkeypatch.setattr("nfl_mcp.cli._setup_client_interactive", lambda cfg: None)

        # Prompts: change DB path? -> no ; ingest now? -> default yes ; configure client? -> no
        result = runner.invoke(
            main,
            ["init", "--start", "2024", "--end", "2024"],
            input="n\n\nn\n",
        )

        assert result.exit_code == 0
        assert captured["dataset_ids"] == DEFAULT_DATASETS
        assert captured["start"] == 2024
        assert captured["end"] == 2024
        assert captured["fresh"] is False
        assert captured["skip_views"] is False

    def test_init_skips_ingest_when_user_declines(self, runner, monkeypatch, tmp_path):
        ingest_calls = []
        monkeypatch.setattr("nfl_mcp.config.load_config", lambda: {})
        monkeypatch.setattr("nfl_mcp.config.save_config", lambda cfg: tmp_path / "config.json")
        monkeypatch.setattr("nfl_mcp.ingest.run_ingest_datasets", lambda *a, **k: ingest_calls.append(True))
        monkeypatch.setattr("nfl_mcp.cli._setup_client_interactive", lambda cfg: None)

        result = runner.invoke(
            main,
            ["init", "--start", "2024", "--end", "2024"],
            input="n\nn\nn\n",
        )

        assert result.exit_code == 0
        assert ingest_calls == []

    def test_init_configures_client_when_confirmed(self, runner, monkeypatch, tmp_path):
        setup_calls = []
        monkeypatch.setattr("nfl_mcp.config.load_config", lambda: {})
        monkeypatch.setattr("nfl_mcp.config.save_config", lambda cfg: tmp_path / "config.json")
        monkeypatch.setattr("nfl_mcp.cli._setup_client_interactive", lambda cfg: setup_calls.append(cfg))

        result = runner.invoke(
            main,
            ["init", "--skip-ingest"],
            input="n\ny\n",
        )

        assert result.exit_code == 0
        assert len(setup_calls) == 1

    def test_setup_client_routes_auto(self, runner, monkeypatch):
        called = []
        monkeypatch.setattr("nfl_mcp.config.load_config", lambda: {"duckdb_path": "/tmp/db.duckdb"})
        monkeypatch.setattr("nfl_mcp.cli._setup_client_interactive", lambda cfg: called.append(("auto", cfg)))
        result = runner.invoke(main, ["setup-client", "--client", "auto"])
        assert result.exit_code == 0
        assert called and called[0][0] == "auto"

    def test_setup_client_routes_claude_desktop(self, runner, monkeypatch):
        called = []
        monkeypatch.setattr("nfl_mcp.config.load_config", lambda: {"duckdb_path": "/tmp/db.duckdb"})
        monkeypatch.setattr("nfl_mcp.cli._configure_claude_desktop", lambda cfg: called.append(("claude", cfg)))
        result = runner.invoke(main, ["setup-client", "--client", "claude-desktop"])
        assert result.exit_code == 0
        assert called and called[0][0] == "claude"

    def test_setup_client_routes_vscode(self, runner, monkeypatch):
        called = []
        monkeypatch.setattr("nfl_mcp.config.load_config", lambda: {"duckdb_path": "/tmp/db.duckdb"})
        monkeypatch.setattr("nfl_mcp.cli._configure_vscode", lambda cfg: called.append(("vscode", cfg)))
        result = runner.invoke(main, ["setup-client", "--client", "vscode"])
        assert result.exit_code == 0
        assert called and called[0][0] == "vscode"


# ── CLI helpers ─────────────────────────────────────────────────────────────────

class TestCliHelpers:
    def test_setup_client_interactive_configures_selected_clients(self, monkeypatch, tmp_path):
        configured = []
        answers = iter([True, False])  # Claude yes, VS Code no

        monkeypatch.setattr("nfl_mcp.cli._claude_desktop_config_path", lambda: tmp_path / "claude_desktop_config.json")
        monkeypatch.setattr("nfl_mcp.cli.click.confirm", lambda *args, **kwargs: next(answers))
        monkeypatch.setattr("nfl_mcp.cli._configure_claude_desktop", lambda cfg: configured.append("claude"))
        monkeypatch.setattr("nfl_mcp.cli._configure_vscode", lambda cfg: configured.append("vscode"))

        cli._setup_client_interactive({"duckdb_path": "/tmp/db.duckdb"})
        assert configured == ["claude"]

    def test_setup_client_interactive_configures_vscode_branch(self, monkeypatch):
        configured = []
        monkeypatch.setattr("nfl_mcp.cli._claude_desktop_config_path", lambda: None)
        monkeypatch.setattr("nfl_mcp.cli.click.confirm", lambda *args, **kwargs: True)
        monkeypatch.setattr("nfl_mcp.cli._configure_vscode", lambda cfg: configured.append("vscode"))
        monkeypatch.setattr("nfl_mcp.cli._configure_claude_desktop", lambda cfg: configured.append("claude"))

        cli._setup_client_interactive({"duckdb_path": "/tmp/db.duckdb"})
        assert configured == ["vscode"]

    def test_resolve_server_command_prefers_uvx(self, monkeypatch):
        monkeypatch.setattr(
            "nfl_mcp.cli.shutil.which",
            lambda name: "/usr/local/bin/uvx" if name == "uvx" else None,
        )
        cmd, args = cli._resolve_server_command()
        assert cmd == "uvx"
        assert args == ["nfl-mcp", "serve"]

    def test_resolve_server_command_uses_nfl_mcp_binary_when_uvx_missing(self, monkeypatch):
        monkeypatch.setattr(
            "nfl_mcp.cli.shutil.which",
            lambda name: "/usr/local/bin/nfl-mcp" if name == "nfl-mcp" else None,
        )
        cmd, args = cli._resolve_server_command()
        assert cmd == "/usr/local/bin/nfl-mcp"
        assert args == ["serve"]

    def test_resolve_server_command_falls_back_to_python_module(self, monkeypatch):
        monkeypatch.setattr("nfl_mcp.cli.shutil.which", lambda name: None)
        cmd, args = cli._resolve_server_command()
        assert cmd == cli.sys.executable
        assert args == ["-m", "nfl_mcp.cli", "serve"]

    def test_build_server_config_uses_resolved_command(self, monkeypatch):
        monkeypatch.setattr("nfl_mcp.cli._resolve_server_command", lambda: ("uvx", ["nfl-mcp", "serve"]))
        result = cli._build_server_config({})
        assert result == {"command": "uvx", "args": ["nfl-mcp", "serve"]}

    @pytest.mark.parametrize("platform_name,env_var,expected_parts", [
        ("darwin", None, ("Library", "Application Support", "Claude", "claude_desktop_config.json")),
        ("linux", None, (".config", "claude", "claude_desktop_config.json")),
        ("win32", "APPDATA", ("Claude", "claude_desktop_config.json")),
    ])
    def test_claude_desktop_config_path_platforms(self, monkeypatch, tmp_path, platform_name, env_var, expected_parts):
        monkeypatch.setattr("nfl_mcp.cli.sys.platform", platform_name)
        monkeypatch.setattr("nfl_mcp.cli.Path.home", lambda: tmp_path)
        if env_var:
            appdata = tmp_path / "AppData" / "Roaming"
            monkeypatch.setenv(env_var, str(appdata))
            parent = appdata / expected_parts[0]
        else:
            parent = tmp_path.joinpath(*expected_parts[:-1])
        parent.mkdir(parents=True, exist_ok=True)

        path = cli._claude_desktop_config_path()
        assert path is not None
        assert path.name == expected_parts[-1]

    def test_claude_desktop_config_path_returns_none_if_parent_missing(self, monkeypatch, tmp_path):
        monkeypatch.setattr("nfl_mcp.cli.sys.platform", "linux")
        monkeypatch.setattr("nfl_mcp.cli.Path.home", lambda: tmp_path)
        assert cli._claude_desktop_config_path() is None

    def test_configure_claude_desktop_returns_when_path_missing(self, monkeypatch):
        monkeypatch.setattr("nfl_mcp.cli._claude_desktop_config_path", lambda: None)
        cli._configure_claude_desktop({})

    def test_configure_claude_desktop_merges_even_with_invalid_existing_json(self, monkeypatch, tmp_path):
        path = tmp_path / "claude_desktop_config.json"
        path.write_text("{invalid json")

        monkeypatch.setattr("nfl_mcp.cli._claude_desktop_config_path", lambda: path)
        monkeypatch.setattr("nfl_mcp.cli._build_server_config", lambda cfg: {"command": "nfl-mcp", "args": ["serve"]})

        cli._configure_claude_desktop({})
        saved = json.loads(path.read_text())
        assert saved["mcpServers"]["nfl"]["command"] == "nfl-mcp"

    def test_configure_vscode_merges_even_with_invalid_existing_json(self, monkeypatch, tmp_path):
        vscode_dir = tmp_path / ".vscode"
        vscode_dir.mkdir(parents=True, exist_ok=True)
        mcp_path = vscode_dir / "mcp.json"
        mcp_path.write_text("{invalid json")

        monkeypatch.setattr("nfl_mcp.cli.Path.cwd", lambda: tmp_path)
        monkeypatch.setattr("nfl_mcp.cli._build_server_config", lambda cfg: {"command": "nfl-mcp", "args": ["serve"]})

        cli._configure_vscode({})
        saved = json.loads(mcp_path.read_text())
        assert saved["servers"]["nfl"]["command"] == "nfl-mcp"


# ── doctor command ──────────────────────────────────────────────────────────────

class TestDoctorCommand:
    def test_doctor_exits_early_when_config_missing(self, runner, monkeypatch):
        monkeypatch.setattr("nfl_mcp.config.config_exists", lambda: False)
        result = runner.invoke(main, ["doctor"])
        assert result.exit_code == 0
        assert "No config file" in result.output

    def test_doctor_happy_path_reports_success(self, runner, monkeypatch, tmp_path):
        db_path = tmp_path / "nflread.duckdb"
        db_path.write_text("placeholder")

        claude_path = tmp_path / "claude_desktop_config.json"
        claude_path.write_text(json.dumps({"mcpServers": {"nfl": {"command": "nfl-mcp"}}}))

        vscode_dir = tmp_path / ".vscode"
        vscode_dir.mkdir(parents=True, exist_ok=True)
        (vscode_dir / "mcp.json").write_text(json.dumps({"servers": {"nfl": {"command": "nfl-mcp"}}}))

        class _FakeCursor:
            def __init__(self, row):
                self._row = row

            def fetchone(self):
                return self._row

        class _FakeConn:
            def execute(self, sql):
                if "COUNT(*) FROM plays" in sql:
                    return _FakeCursor((123456,))
                return _FakeCursor((2013, 2025))

            def close(self):
                return None

        monkeypatch.setattr("nfl_mcp.config.config_exists", lambda: True)
        monkeypatch.setattr("nfl_mcp.config.get_duckdb_path", lambda: db_path)
        monkeypatch.setattr("nfl_mcp.cli._claude_desktop_config_path", lambda: claude_path)
        monkeypatch.setattr("nfl_mcp.cli.Path.cwd", lambda: tmp_path)
        monkeypatch.setattr("duckdb.connect", lambda *args, **kwargs: _FakeConn())

        result = runner.invoke(main, ["doctor"])
        assert result.exit_code == 0
        assert "All checks passed!" in result.output

    def test_doctor_reports_db_error_and_failed_summary(self, runner, monkeypatch, tmp_path):
        db_path = tmp_path / "nflread.duckdb"
        db_path.write_text("placeholder")

        monkeypatch.setattr("nfl_mcp.config.config_exists", lambda: True)
        monkeypatch.setattr("nfl_mcp.config.get_duckdb_path", lambda: db_path)
        monkeypatch.setattr("nfl_mcp.cli._claude_desktop_config_path", lambda: None)
        monkeypatch.setattr("nfl_mcp.cli.Path.cwd", lambda: tmp_path)
        monkeypatch.setattr(
            "duckdb.connect",
            lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("cannot open db")),
        )

        result = runner.invoke(main, ["doctor"])
        assert result.exit_code == 0
        assert "Database error: cannot open db" in result.output
        assert "Claude Desktop not detected" in result.output
        assert "VS Code MCP config not found" in result.output
        assert "Some checks failed" in result.output

    def test_doctor_warns_when_server_entries_missing(self, runner, monkeypatch, tmp_path):
        db_path = tmp_path / "missing.duckdb"  # intentionally does not exist

        claude_path = tmp_path / "claude_desktop_config.json"
        claude_path.write_text(json.dumps({"mcpServers": {}}))

        vscode_dir = tmp_path / ".vscode"
        vscode_dir.mkdir(parents=True, exist_ok=True)
        (vscode_dir / "mcp.json").write_text(json.dumps({"servers": {}}))

        monkeypatch.setattr("nfl_mcp.config.config_exists", lambda: True)
        monkeypatch.setattr("nfl_mcp.config.get_duckdb_path", lambda: db_path)
        monkeypatch.setattr("nfl_mcp.cli._claude_desktop_config_path", lambda: claude_path)
        monkeypatch.setattr("nfl_mcp.cli.Path.cwd", lambda: tmp_path)

        result = runner.invoke(main, ["doctor"])
        assert result.exit_code == 0
        assert "DuckDB file not found" in result.output
        assert "Claude Desktop config exists but 'nfl' server not found" in result.output
        assert ".vscode/mcp.json exists but 'nfl' server not found" in result.output
        assert "Some checks failed" in result.output

    def test_doctor_warns_on_invalid_client_json(self, runner, monkeypatch, tmp_path):
        db_path = tmp_path / "missing.duckdb"  # intentionally does not exist

        claude_path = tmp_path / "claude_desktop_config.json"
        claude_path.write_text("{ not json")

        vscode_dir = tmp_path / ".vscode"
        vscode_dir.mkdir(parents=True, exist_ok=True)
        (vscode_dir / "mcp.json").write_text("{ definitely not json")

        monkeypatch.setattr("nfl_mcp.config.config_exists", lambda: True)
        monkeypatch.setattr("nfl_mcp.config.get_duckdb_path", lambda: db_path)
        monkeypatch.setattr("nfl_mcp.cli._claude_desktop_config_path", lambda: claude_path)
        monkeypatch.setattr("nfl_mcp.cli.Path.cwd", lambda: tmp_path)

        result = runner.invoke(main, ["doctor"])
        assert result.exit_code == 0
        assert "Claude Desktop config exists but is invalid JSON" in result.output
        assert ".vscode/mcp.json exists but is invalid JSON" in result.output

# AGENTS.md

Guidance for AI agents working on this codebase.

## What This Project Is

`nfl-mcp` is a local MCP server that ingests NFL data from nflreadpy into a DuckDB database and exposes it to AI clients (Claude Desktop, VS Code) via the Model Context Protocol. All data lives in a single `~/.nfl-mcp/nflread.duckdb` file. No network calls happen at query time — only at ingest time.

## Repository Layout

```
nfl_mcp/
  cli.py          # Click CLI: serve, ingest, init, setup-client, doctor
  server.py       # MCP server: TOOLS list + _DISPATCH + call_tool handler
  tools.py        # Tool implementations (nfl_schema, nfl_status, nfl_query, ...)
  schema_pbp.py   # PBP schema strings (_SCHEMA_SUMMARY, _SCHEMA_CATEGORIES) — pure data, no logic
  ingest.py       # Data ingestion: run_ingest_datasets, idempotency, schema reconciliation
  registry.py     # Declarative dataset definitions (REGISTRY, DatasetDef)
  database.py     # get_db_connection() context manager (read-only DuckDB)
  config.py       # ~/.nfl-mcp/config.json read/write, get_duckdb_path()

tests/
  test_registry.py   # Registry structure, coverage windows, loader functions
  test_ingest.py     # Ingest helpers using in-memory DuckDB (no network)
  test_cli.py        # CLI commands using click.testing.CliRunner
  test_tools.py      # Tool functions — unit + integration (require_db fixture)
  test_safety.py     # SQL injection / forbidden keyword guardrails
```

## Key Architectural Rules

**Adding a new MCP tool** requires changes in exactly two files:
1. `tools.py` — implement the function, add to `__all__`
2. `server.py` — add a `Tool(...)` entry to `TOOLS` and an entry to `_DISPATCH`

**Adding a new dataset** requires changes in exactly one file:
1. `registry.py` — add a `DatasetDef` entry to `REGISTRY`

The ingest loop in `ingest.py` is fully generic — it reads from the registry and handles everything else (idempotency, schema reconciliation, bulk vs ranged mode).

**Never hardcode table schemas** for non-pbp tables. Use `nfl_schema(table=<name>)` which queries `information_schema.columns` at runtime. PBP schema strings live in `schema_pbp.py` because they carry human-readable annotations — not just types.

## Development Workflow

```bash
# Install in editable mode
pip install -e ".[dev]"

# Run all tests (unit + integration if DB exists)
pytest

# Run only unit tests (no DB required)
pytest -m unit

# Run with coverage (configured in pyproject.toml, runs automatically)
pytest

# Check coverage for a specific module
pytest --cov=nfl_mcp.tools --cov-report=term-missing

# Ingest data (CI uses 2024 only; local development can use all seasons)
nfl-mcp ingest --dataset all --start 2024 --end 2024   # fast, CI-friendly
nfl-mcp ingest --dataset all                             # all seasons, slow

# After changing tool code, reinstall into Claude Desktop's venv
pip install --force-reinstall /Users/eshaanbhattad/Desktop/nfl-mcp/
# Then restart Claude Desktop
```

## Testing Strategy

- **Unit tests** (`test_registry`, `test_ingest`, `test_cli`): no DB, no network, always run in CI
- **Integration tests** (`test_tools.py` classes marked `@pytest.mark.integration`): require a loaded DuckDB; skipped automatically via `require_db` fixture if DB is absent
- **Safety tests** (`test_safety.py`): SQL injection guardrails, no DB needed
- CI runs `nfl-mcp ingest --dataset all --start 2024 --end 2024` before tests to populate the DB

When writing new tool tests, follow the existing pattern:
```python
@pytest.mark.integration
@pytest.mark.usefixtures("require_db")
class TestMyNewTool:
    def test_basic_filter(self):
        result = my_tool(team="KC", season=2024)
        assert "error" not in result
        assert result["count"] > 0
```

## Database

- **Path**: `~/.nfl-mcp/nflread.duckdb` (override with `NFL_MCP_DB_PATH` env var)
- **Access**: always read-only at query time via `get_db_connection()` in `database.py`
- **Metadata**: `_ingest_metadata` table tracks what datasets/seasons have been loaded
- **Tables**: 33 tables total — use `nfl_schema()` to list them or `nfl_schema(table=<name>)` for columns

## Dataset Registry

Each entry in `REGISTRY` is a `DatasetDef` with:
- `loader_fn`: the nflreadpy function name (must exist as `hasattr(nflreadpy, loader_fn)`)
- `seasonal`: whether it accepts a `seasons=` parameter
- `storage`: `"replace"` (static data) or `"append_by_season"` (time-series)
- `min_season`/`max_season`: clip the ingest range to avoid known 404s
- `wave`: 1 (default), 2, or 3 (opt-in heavy datasets)
- `default`: whether included in `nfl-mcp ingest` with no flags

## Ingest Idempotency

The ingest loop checks `_ingest_metadata` before every dataset+season. Re-running the same ingest command is safe — already-loaded combinations are skipped. Use `--fresh` to force re-ingest.

## Common Pitfalls

- **Claude Desktop can't access `~/Desktop`** due to macOS sandboxing. The Claude Desktop venv must be outside Desktop — currently `~/nfl-mcp-venv`. Any code changes require `pip install --force-reinstall` into that venv.
- **DuckDB write lock**: the server opens read-only connections. Ingest opens a write connection. Don't run ingest while the server is running against the same DB file.
- **Player names are abbreviated**: `'P.Mahomes'`, `'T.Hill'` — use `ILIKE '%name%'` for fuzzy matching.
- **Boolean columns are `DOUBLE PRECISION` 0/1** in the plays table — filter with `= 1`, not `IS TRUE`.
- **`nfl_query` is last resort** — prefer structured tools. The tool descriptions use "PREFERRED for..." language intentionally to steer the model away from raw SQL.

## Coverage Gaps (Known)

- `cli.py` (~29%): interactive wizard (`init`, `setup-client`) can't easily be unit tested
- `ingest.py` (~25%): bulk ingest and network-dependent paths not covered without live nflreadpy calls
- `tools.py` error paths: exception branches require DB failures to trigger

# nfl-mcp

MCP server for NFL data (2013–2025), powered by [nflreadpy](https://github.com/nflverse/nflreadpy) and DuckDB.
Query play-by-play, rosters, injuries, stats, and more using natural language in Claude Code, VS Code, or Claude Desktop.

Ask Claude questions like:
- *"Who had the best EPA per play in 2024?"*
- *"Show me Patrick Mahomes' completion % over expected by season"*
- *"Compare 4th quarter red zone efficiency for KC vs PHI in 2023"*
- *"Which defenses had the highest sack rate in 3rd & long situations?"*
- *"Who was on IR for the Eagles in Week 10, 2023?"*
- *"Show me snap count trends for the Chiefs receiving corps in 2024"*

## Quickstart

```bash
pip install nfl-mcp        # or: uvx nfl-mcp
nfl-mcp init               # configure + load default datasets
```

No database server to install. No credentials to manage. Data is stored locally in DuckDB.

## Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

## Setup

### 1. Initialize

```bash
nfl-mcp init
```

The wizard will:
1. Configure the local DuckDB database path
2. Download the default NFL datasets (play-by-play, rosters, stats, injuries, and more)
3. Auto-configure your IDE (Claude Desktop and/or VS Code)

Options:

```
--skip-ingest       Configure without loading data
```

### 2. Verify

```bash
nfl-mcp doctor
```

Checks database connectivity, loaded data, and IDE configuration.

### 3. Manual client configuration (optional)

If you skipped IDE setup during init, or need to reconfigure:

```bash
nfl-mcp setup-client                    # auto-detect clients
nfl-mcp setup-client --client vscode    # VS Code only
nfl-mcp setup-client --client claude-desktop
```

Or configure manually — add to `.vscode/mcp.json`:

```json
{
  "servers": {
    "nfl": {
      "command": "uvx",
      "args": ["nfl-mcp", "serve"]
    }
  }
}
```

## CLI Reference

```
nfl-mcp init               Interactive setup wizard
nfl-mcp serve              Start the MCP server (stdio)
nfl-mcp ingest             Load NFL data into the database
nfl-mcp setup-client       Configure IDE MCP clients
nfl-mcp doctor             Health check
```

### Ingestion options

```bash
nfl-mcp ingest                          # default datasets, all available seasons
nfl-mcp ingest --dataset all            # every dataset
nfl-mcp ingest --dataset schedules      # one specific dataset
nfl-mcp ingest --dataset pbp --dataset injuries   # multiple datasets
nfl-mcp ingest --start 2020 --end 2024  # limit to a season range
nfl-mcp ingest --fresh                  # re-ingest even if already loaded
nfl-mcp ingest --list                   # show all available dataset names
```

Ingest is **idempotent** — re-running skips datasets and seasons already in the database.

## Datasets

All data is sourced from [nflverse](https://github.com/nflverse/nflverse-data) via nflreadpy and stored locally in DuckDB.

| Table | Default | Coverage |
|---|:-:|---|
| `plays` | ✓ | 1999–present |
| `schedules` | ✓ | 1999–present |
| `rosters` | ✓ | 1920–present |
| `player_stats` | ✓ | 1999–present |
| `team_stats_raw` | ✓ | 1999–present |
| `injuries` | ✓ | 2009–present |
| `snap_counts` | ✓ | 2012–present |
| `teams` | ✓ | current |
| `players` | ✓ | all-time |
| `contracts` | ✓ | historical |
| `trades` | ✓ | historical |
| `depth_charts` | | 2001–present |
| `rosters_weekly` | | 2002–present |
| `ff_opportunity` | | 2006–present |
| `officials` | | 2015–present |
| `nextgen_stats_*` | | 2016–present |
| `participation` | | 2016–2024 |
| `pfr_advstats_*` | | 2018–present |
| `ftn_charting` | | 2022–present |
| `draft_picks` | | 1980–present |
| `combine` | | all-time |
| `ff_playerids` | | current |
| `ff_rankings_draft` | | current |
| `ff_rankings_week` | | current |

```bash
nfl-mcp ingest --dataset all   # load everything
nfl-mcp ingest --list          # see all dataset names
```

## MCP Tools

| Tool | Description |
|------|-------------|
| `nfl_schema` | Database schema reference — compact summary by default, pass `category` for detail |
| `nfl_status` | Database health: total plays, loaded seasons, available tables |
| `nfl_query` | Raw SQL SELECT for custom queries (500 row cap, 10s timeout) |
| `nfl_search_plays` | Find plays by player, team, season, season type, situation, touchdowns, etc. |
| `nfl_team_stats` | Pre-aggregated team offense, defense, and situational stats |
| `nfl_player_stats` | Player stats by season and season type — passing, rushing, or receiving |
| `nfl_compare` | Side-by-side comparison of two teams or two players |
| `nfl_schedule` | Game schedule and results — scores, spread, weather, coaches |
| `nfl_roster` | Team roster by season and position |
| `nfl_injuries` | Player injury report status by team, week, and designation |
| `nfl_snap_counts` | Offensive, defensive, and special teams snap counts per player |
| `nfl_fantasy_opportunity` | Target share, air yards share, carry share per player per week (2006–present, requires `ff_opportunity` dataset) |
| `nfl_catalog` | List all loaded tables with row counts and last refresh time |

## Key columns in `plays`

- `epa` — expected points added (the best single-play quality metric)
- `wpa` — win probability added
- `posteam` / `defteam` — offensive/defensive team abbreviations
- `passer_player_name` / `rusher_player_name` / `receiver_player_name`
- `play_type` — `'pass'` | `'run'` | `'field_goal'` | `'punt'` | `'kickoff'` | ...
- `desc` — raw play description (use `ILIKE` for text search)

## Local Development

```bash
git clone https://github.com/ebhattad/nfl-mcp
cd nfl-mcp
pip install -e ".[dev]"

nfl-mcp ingest --dataset all --start 2024 --end 2024
pytest
pytest -m unit     # unit tests
pytest -m integration  # integration tests (requires loaded DB)
```

## Troubleshooting

- `nfl-mcp doctor` is the fastest way to verify config, database, and client setup.
- If tools return database errors, run `nfl-mcp ingest` to ensure data is loaded.
- You can override the DB location with `NFL_MCP_DB_PATH=/path/to/nflread.duckdb`.
- Re-running `nfl-mcp ingest` is safe — it skips anything already loaded.

## License

MIT

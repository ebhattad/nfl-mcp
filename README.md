# nfl-mcp

MCP server for NFL play-by-play data (2013–2025), powered by [nflreadpy](https://github.com/nflverse/nflreadpy) and DuckDB.
Query 13 seasons of NFL play-by-play data using natural language in Claude Code, VS Code, or Claude Desktop.

Ask Claude questions like:
- *"Who had the best EPA per play in 2024?"*
- *"Show me Patrick Mahomes' completion % over expected by season"*
- *"Compare 4th quarter red zone efficiency for KC vs PHI in 2023"*
- *"Which defenses had the highest sack rate in 3rd & long situations?"*

## Quickstart

```bash
pip install nfl-mcp        # or: uvx nfl-mcp
nfl-mcp init               # defaults to all 13 seasons (2013–2025) + IDE setup
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
2. Download NFL play-by-play data for all 13 seasons by default (2013–2025)
3. Auto-configure your IDE (Claude Desktop and/or VS Code)

Options:

```
--start 2020        First season (default: 2013)
--end   2024        Last season  (default: 2025)
--skip-ingest       Configure without loading data
```

`--start` must be less than or equal to `--end`.
With no flags, `nfl-mcp init` loads all seasons from 2013 through 2025.

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
nfl-mcp ingest             Load/reload play-by-play data (default: 2013–2025)
nfl-mcp setup-client       Configure IDE MCP clients
nfl-mcp doctor             Health check
```

### Ingestion options

```
nfl-mcp ingest                                Load all 13 seasons (default)
nfl-mcp ingest --start 2020 --end 2024    Load specific seasons
nfl-mcp ingest --fresh                     Drop and reload all data
nfl-mcp ingest --skip-views                Skip aggregate table creation
```

## Tools

| Tool | Description |
|------|-------------|
| `nfl_schema` | Database schema reference — compact summary by default, pass `category` for detail |
| `nfl_status` | Database health: total plays, loaded seasons, available tables |
| `nfl_query` | Raw SQL SELECT for custom queries (500 row cap, 10s timeout) |
| `nfl_search_plays` | Find plays by player, team, season, season type, situation, touchdowns, etc. |
| `nfl_team_stats` | Pre-aggregated team offense, defense, and situational stats |
| `nfl_player_stats` | Player stats by season and season type — passing, rushing, or receiving |
| `nfl_compare` | Side-by-side comparison of two teams or two players |

## Database Schema

Hundreds of thousands of plays across 2013–2025, with 370+ nflreadpy columns preserved as-is.

**Key tables:**
- `plays` — every play, all columns
- `team_offense_stats` — pre-aggregated by team/season
- `team_defense_stats` — pre-aggregated by team/season
- `situational_stats` — by team/season/situation (Red Zone, 3rd & Long, etc.)
- `formation_effectiveness` — by team/season/formation

**Key columns:**
- `epa` — expected points added (the best single-play quality metric)
- `wpa` — win probability added
- `posteam` / `defteam` — offensive/defensive team abbreviations
- `passer_player_name` / `rusher_player_name` / `receiver_player_name`
- `play_type` — 'pass' | 'run' | 'field_goal' | 'punt' | 'kickoff' | ...
- `desc` — raw play description (use ILIKE for text search)

## Local Development

```bash
git clone https://github.com/ebhattad/nfl-mcp
cd nfl-mcp
pip install -e ".[dev]"

nfl-mcp init --start 2024 --end 2024
pytest
```

## Troubleshooting

- `nfl-mcp doctor` is the fastest way to verify config, database, and client setup.
- If tools return database errors, run `nfl-mcp ingest` (or rerun `nfl-mcp init`) to ensure `plays` is loaded.
- You can override the DB location with `NFL_MCP_DB_PATH=/path/to/nflread.duckdb`.

## Branch + PR Enforcement (GitHub)

To force branch-based contributions and test-gated merges on `main`, set these in **GitHub → Settings → Rules/Branches**:

- Require a pull request before merging
- Require approvals (at least 1)
- Require conversation resolution before merging
- Require status checks to pass before merging: select `CI / test`
- Require branches to be up to date before merging
- Restrict who can push to matching branches (or block direct pushes to `main` entirely)

## License

MIT

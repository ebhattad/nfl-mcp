# NFL MCP Multi-Dataset Expansion Roadmap

## Problem Statement
`nfl-mcp` currently ingests and serves only play-by-play (`load_pbp`) data plus a few derived aggregate tables. The goal is to expand ingestion, local DuckDB storage, MCP tooling, and UX to support the full nflreadpy dataset family (pbp, schedules, players, rosters, stats, injuries, contracts, combine, fantasy, etc.), then add a terminal-first TUI and CLI-agent workflows.

## Current State Analysis
- **Single-dataset ingestion path**: `nfl_mcp/ingest.py` only calls `nflreadpy.load_pbp(...)`, writes to `plays`, and builds pbp-centric aggregate tables (`team_offense_stats`, `team_defense_stats`, `situational_stats`, `formation_effectiveness`).
- **Single DB connection target**: `nfl_mcp/database.py` opens one DuckDB path from config/env (`NFL_MCP_DB_PATH`).
- **MCP surface is pbp-focused**: `nfl_mcp/server.py` registers tools for schema/status/query/search/team/player/compare tied to pbp and current aggregates.
- **CLI is pbp-centric**: `nfl_mcp/cli.py` exposes `ingest` with season range semantics that align to pbp flow.
- **Testing**: integration tests in `tests/test_tools.py` assume pbp-derived tables and season filters (notably 2024 in CI).

## Proposed Architecture (Target)
1. **Dataset catalog + ingestion registry**
   - Introduce a declarative dataset registry mapping:
     - dataset id (e.g., `pbp`, `player_stats`, `schedules`)
     - nflreadpy loader function (`load_*`)
     - supported filters (season/week/season_type)
     - storage strategy (replace, append, partitioned append)
     - primary keys and dedupe keys.
2. **Storage layers**
   - **Bronze (raw)**: canonical raw tables per dataset (`raw_pbp`, `raw_players`, ...).
   - **Silver (normalized)**: cleaned/typed joins and harmonized IDs.
   - **Gold (serving)**: MCP-facing aggregate/semantic tables optimized for tool queries.
3. **Tooling layers**
   - Keep existing pbp tools stable.
   - Add dataset-aware tools incrementally (status/schema/search/stats endpoints per domain).
4. **UX layers**
   - CLI upgrades for selective ingest and refresh workflows.
   - TUI module for discoverability, query building, and charting.
   - Agent-oriented CLI entrypoints for scriptable autonomous analysis.

## Dataset Roadmap (all requested nflreadpy loaders)
### Wave 1 (foundation + high-value joins)
- `load_pbp`, `load_schedules`, `load_players`, `load_rosters`, `load_rosters_weekly`
- `load_player_stats`, `load_team_stats`, `load_injuries`, `load_snap_counts`

### Wave 2 (analytics enrichment)
- `load_nextgen_stats`, `load_participation`, `load_depth_charts`, `load_officials`
- `load_draft_picks`, `load_combine`, `load_trades`

### Wave 3 (external/fantasy/commercial)
- `load_ftn_charting`, `load_contracts`
- `load_ff_playerids`, `load_ff_rankings`, `load_ff_opportunity`

## Implementation Plan (phased)
### Phase A — Data Platform Foundation
- Build dataset registry and generic ingest runner (`ingest dataset` / `ingest all`).
- Add table naming conventions and metadata table (ingest history, row counts, last refresh, source function).
- Implement idempotent load semantics and schema drift handling per dataset.

### Phase B — Multi-table DuckDB Modeling
- Create raw tables for each dataset with consistent column normalization.
- Build conformed dimensions (teams, players, games, seasons/weeks).
- Build serving marts used by MCP tools (player season summaries, team trends, injury timeline, roster snapshots, fantasy opportunity).

### Phase C — MCP API Expansion
- Add `nfl_catalog` tool (what datasets are loaded, freshness, row counts).
- Extend `nfl_schema` and `nfl_status` to include dataset/table health.
- Add domain tools:
  - schedules/games
  - player profile + transactions/injuries
  - roster/snap/depth evolution
  - advanced stats (nextgen/ftn)
  - fantasy rankings/opportunity.
- Keep `nfl_query` as last-resort read-only path.

### Phase D — CLI & Operations
- Add CLI commands/options:
  - `nfl-mcp ingest --dataset <name|all>`
  - `nfl-mcp ingest --season/--week` where supported
  - `nfl-mcp refresh --dataset ...`
  - `nfl-mcp status --dataset ...`
- Add health checks per dataset in `doctor`.
- Add CI job profile(s): no-db unit, pbp-e2e, full multi-dataset smoke.

### Phase E — TUI UX
- Build a curses/textual-based TUI prototype:
  - dataset browser + freshness/status
  - query presets by domain
  - table preview and filters
  - terminal charts (time series, rank bars, distribution histograms).
- Add export actions (CSV/JSON/chart snapshot).

### Phase F — CLI Agent Support
- Add agent-friendly command contracts:
  - stable JSON output mode
  - deterministic tool schemas
  - non-interactive ingest/query flows.
- Provide templates for local autonomous workflows (batch reports, scheduled refresh + insights).

### Phase G — Tool Routing Evals
Local-only eval harness (not run in CI, no API key required from contributors) for validating that Claude picks the right MCP tool for a given prompt.

**Motivation:** As more tools are added, it becomes easy for Claude to fall back to `nfl_query` instead of structured tools, or to pass malformed arguments (e.g. `team="Kansas City"` instead of `team="KC"`). Evals catch regressions before they reach users.

**Design:**
- Lives in `evals/` directory, gitignored API key via `.env` / `ANTHROPIC_API_KEY` env var
- Uses the Anthropic Python SDK — sends each prompt to Claude with the same `TOOLS` list from `server.py`
- Inspects the `tool_use` block(s) in the response; does not execute the tools
- Assertions cover:
  - **Tool selection** — correct tool was called (not `nfl_query` as a fallback)
  - **Argument shape** — required args are present and correctly formatted (team abbrevs, integer seasons, etc.)
  - **Anti-patterns** — `nfl_query` is not called when a structured tool exists for the question
- Eval cases defined as plain dicts (`prompt`, `expected_tool`, `expected_args`) — easy to add new ones
- Run manually with `pytest evals/ -m eval` before cutting a release or after changing tool descriptions

**What it won't cover (out of scope):**
- Answer correctness / response quality (would require LLM-as-judge or golden answers)
- Actual tool execution / data correctness (covered by integration tests against DuckDB)

**Estimated scope:** ~1 file, ~150 lines, ~30 seed eval cases covering all 12 tools.

## Technical Notes / Design Choices
- Preserve backward compatibility for existing pbp tools and CLI defaults.
- Prefer additive migrations; avoid breaking current `plays`-based queries.
- Introduce shared key strategy early (`game_id`, player IDs, team abbreviations) to prevent join fragmentation.
- Gate expensive datasets behind explicit flags for local footprint control.
- Add source/freshness metadata to every serving response where practical.

## Risks and Mitigations
- **Schema volatility across sources** → typed normalization + schema drift migration helpers.
- **Data volume/performance** → partitioning by season/week, selective indexes, bounded query defaults.
- **Cross-source key mismatch** → canonical ID mapping tables and reconciliation tests.
- **Tool explosion** → group tools by domain with consistent naming and shared filter schema.

## Decisions + Open Questions
- **Chosen baseline**: single DuckDB with namespaced tables.
- Open:
  1. Required freshness SLA by dataset (manual refresh vs scheduled/background refresh).
  2. Scope of first public TUI release (read-only dashboards vs full query builder + chart composer).

## Todo Backlog (execution order)
1. Architecture and dataset registry contract
2. Generic ingestion framework
3. Raw staging tables for all `load_*` datasets
4. Conformed dimensions and serving marts
5. MCP tool registry + domain tool rollout
6. CLI dataset-oriented commands and doctor/status updates
7. Observability and metadata/freshness reporting
8. Test matrix expansion (unit, db-e2e, multi-dataset smoke)
9. Documentation and migration guide
10. TUI MVP
11. CLI agent compatibility layer

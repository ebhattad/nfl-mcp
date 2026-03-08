# NFL MCP Multi-Dataset Expansion Roadmap

## Problem Statement
`nfl-mcp` is a local/remote MCP server ingesting NFL data from nflreadpy into DuckDB and exposing it to AI clients (Claude Desktop, VS Code) via the Model Context Protocol. The goal is to expand ingestion, local DuckDB storage, MCP tooling, and UX to support the full nflreadpy dataset family, then add CLI-agent workflows and a TUI.

## Current State (as of March 2026)
- **Transport**: Streamable HTTP (replaced stdio). Server runs as a persistent process via `nfl-mcp serve`. `init` wizard offers to start the server.
- **Multi-dataset ingestion**: Declarative registry in `registry.py` covering 33 tables across 3 waves. Generic ingest loop in `ingest.py` with idempotency, schema reconciliation, and `_ingest_metadata` tracking.
- **MCP tools**: 12 structured tools — `nfl_search_plays`, `nfl_team_stats`, `nfl_player_stats`, `nfl_compare`, `nfl_schedule`, `nfl_roster`, `nfl_injuries`, `nfl_snap_counts`, `nfl_schema`, `nfl_status`, `nfl_catalog`, `nfl_query` (last resort).
- **CLI**: `serve` (uvicorn, `--host`/`--port`), `ingest` (`--dataset`/`--start`/`--end`/`--fresh`), `init`, `setup-client`, `doctor`.
- **Spread/betting data**: `spread_line` and `total_line` are already columns in the `plays` PBP table. Accessible today via `nfl_search_plays` or `nfl_query`.
- **Fantasy data**: Wave 3 datasets (`ff_playerids`, `ff_rankings`, `ff_opportunity`, `ftn_charting`) are in the registry but opt-in (not ingested by default). No dedicated MCP tools yet.
- **Testing**: 331 tests, 100% coverage. Unit tests (`-m "not integration"`), integration tests (`-m integration`) require loaded DB.
- **Eval harness**: Private, lives at `~/nfl-mcp-evals/`. 29 cases, 29/29 passing on gpt-5.4 with prompt caching (~$0.035/run, 94% cache hit).

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
### ✅ Phase A — Data Platform Foundation
Dataset registry (`registry.py`), generic ingest runner, metadata table (`_ingest_metadata`), idempotent loads, schema drift handling.

### ✅ Phase B — Multi-table DuckDB Modeling
Raw tables for all Wave 1/2 datasets. Serving aggregates used by MCP tools.

### ✅ Phase C — MCP API Expansion
`nfl_catalog`, extended `nfl_schema`/`nfl_status`, 12 domain tools covering schedules, rosters, injuries, snap counts, player/team stats, compare. `nfl_query` as last resort.

### ✅ Phase D — CLI & Operations
`ingest --dataset`/`--start`/`--end`/`--fresh`, `serve --host`/`--port`, `doctor`, `setup-client`, `init` (offers to start server). Streamable HTTP transport replacing stdio.

### ✅ Phase G — Tool Routing Evals
Private eval harness at `~/nfl-mcp-evals/`. 29 cases, gpt-5.4, prompt caching (~$0.035/run). `seed=42 + temperature=0` for determinism, retry wrapper for transient errors.

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

### Phase H — Fantasy & Advanced Stats Tools *(next up)*
- Dedicated MCP tools for Wave 3 datasets once ingested:
  - `nfl_fantasy_rankings`, `nfl_fantasy_opportunity` (currently in registry, no tools)
  - `nfl_ftn_charting` for advanced snap/route/coverage charting
- Spread/betting data is already queryable via `plays` table (`spread_line`, `total_line`) — consider a `nfl_betting_lines` convenience tool surfacing game-level spread/total without raw SQL.

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
8. Test matrix expansion (unit, db-integration, multi-dataset smoke)
9. Documentation and migration guide
10. TUI MVP
11. CLI agent compatibility layer

# NFL MCP Multi-Dataset Expansion Roadmap

## Problem Statement
`nfl-mcp` is a local/remote MCP server ingesting NFL data from nflreadpy into DuckDB and exposing it to AI clients (Claude Desktop, VS Code) via the Model Context Protocol. The goal is to expand ingestion, local DuckDB storage, and MCP tooling to support the full nflreadpy dataset family.

## Current State (as of June 2026)
- **Transport**: Streamable HTTP (replaced stdio). Server runs as a persistent process via `nfl-mcp serve`. `init` wizard offers to start the server.
- **Multi-dataset ingestion**: Declarative registry in `registry.py` covering 33 tables. Generic ingest loop in `ingest.py` with idempotency, schema reconciliation, and `_ingest_metadata` tracking. **All 29 registry datasets are now ingested by default** — the full nflverse family is baked into the served image so clients always have whatever data they need.
- **MCP tools**: 21 structured tools — `nfl_search_plays`, `nfl_team_stats`, `nfl_player_stats`, `nfl_compare`, `nfl_schedule`, `nfl_roster`, `nfl_injuries`, `nfl_snap_counts`, `nfl_fantasy_opportunity`, `nfl_fantasy_rankings`, `nfl_ftn_charting`, `nfl_td_luck`, `nfl_role_trend`, `nfl_separation_opportunity`, `nfl_drop_rate`, `nfl_contract_value`, `nfl_injury_return`, `nfl_schema`, `nfl_status`, `nfl_catalog`, `nfl_query` (last resort).
- **CLI**: `serve` (uvicorn, `--host`/`--port`), `ingest` (`--dataset`/`--start`/`--end`/`--fresh`), `init`, `setup-client`, `doctor`.
- **Spread/betting data**: `spread_line` and `total_line` are already columns in the `plays` PBP table. Accessible today via `nfl_search_plays` or `nfl_query`.
- **Fantasy & charting data**: `ff_opportunity` (→ `nfl_fantasy_opportunity`), `ff_rankings_draft`/`ff_rankings_week` (→ `nfl_fantasy_rankings`), and `ftn_charting` (→ `nfl_ftn_charting`) all have dedicated tools and are ingested by default. Datasets without a dedicated tool yet (e.g. `ff_playerids`) remain queryable via `nfl_query`.
- **Fantasy derived tables**: six analytics tables are built at ingest time from already-loaded sources (no query-time compute) and exposed via dedicated tools — `player_td_luck` (→ `nfl_td_luck`), `player_role_trend` (→ `nfl_role_trend`), `player_separation_opportunity` (→ `nfl_separation_opportunity`), `player_drop_rate` (→ `nfl_drop_rate`), `player_contract_value` (→ `nfl_contract_value`), `injury_return_curve` (→ `nfl_injury_return`). All are recorded in `_ingest_metadata` so they surface in `nfl_catalog`/`nfl_status`.
- **Testing**: 417 tests, 100% coverage. Unit tests (`-m "not integration"`), integration tests (`-m integration`) require loaded DB.
- **CI**: `ci.yml` runs the test suite on a single-season ingest. `docker-build.yml` validates the container image on PRs touching Docker-relevant paths — a lightweight single-season bake (via the `INGEST_ARGS` build arg) plus a boot/`/mcp` smoke test, build-only (no push). The full nflverse bake + publish still runs on release/schedule via `docker.yml`.
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

## Dataset Roadmap (all requested nflreadpy loaders)
All datasets below are now ingested by default. Grouped by purpose:

### Foundation + high-value joins
- `load_pbp`, `load_schedules`, `load_players`, `load_rosters`, `load_rosters_weekly`
- `load_player_stats`, `load_team_stats`, `load_injuries`, `load_snap_counts`

### Analytics enrichment
- `load_nextgen_stats`, `load_participation`, `load_depth_charts`, `load_officials`
- `load_draft_picks`, `load_combine`, `load_trades`

### External / fantasy
- `load_ftn_charting`, `load_contracts`
- `load_ff_playerids`, `load_ff_rankings`, `load_ff_opportunity`

## Implementation Plan (phased)
### ✅ Phase A — Data Platform Foundation
Dataset registry (`registry.py`), generic ingest runner, metadata table (`_ingest_metadata`), idempotent loads, schema drift handling.

### ✅ Phase B — Multi-table DuckDB Modeling
Raw tables for all foundation/enrichment datasets. Serving aggregates used by MCP tools.

### ✅ Phase C — MCP API Expansion
`nfl_catalog`, extended `nfl_schema`/`nfl_status`, 12 domain tools covering schedules, rosters, injuries, snap counts, player/team stats, compare. `nfl_query` as last resort.

### ✅ Phase D — CLI & Operations
`ingest --dataset`/`--start`/`--end`/`--fresh`, `serve --host`/`--port`, `doctor`, `setup-client`, `init` (offers to start server). Streamable HTTP transport replacing stdio.

### ✅ Phase G — Tool Routing Evals
Private eval harness at `~/nfl-mcp-evals/`. 29 cases, gpt-5.4, prompt caching (~$0.035/run). `seed=42 + temperature=0` for determinism, retry wrapper for transient errors.

### Phase H — Fantasy & Advanced Stats Tools *(in progress)*
- Dedicated MCP tools for the fantasy datasets once ingested:
  - ✅ `nfl_fantasy_opportunity` — target/air-yards/carry share (ff_opportunity)
  - ✅ `nfl_fantasy_rankings` — expert consensus rankings, draft + weekly (ff_rankings_draft / ff_rankings_week)
  - ✅ `nfl_ftn_charting` — aggregated charting tendencies: play-action, RPO, screen, motion, box/blitz (ftn_charting)
- Fantasy analytics derived tables (built at ingest time, exposed via dedicated tools):
  - ✅ `nfl_td_luck` — actual vs expected TDs, regression candidates (player_td_luck ← ff_opportunity)
  - ✅ `nfl_role_trend` — rolling 3-week snap/target/carry/air-yards share + delta (player_role_trend ← ff_opportunity + snap_counts)
  - ✅ `nfl_separation_opportunity` — Next Gen separation vs production, regression flag, 2016+ (player_separation_opportunity ← ff_opportunity + nextgen_stats_receiving)
  - ✅ `nfl_drop_rate` — catchable-target drop rate, 2022+ (player_drop_rate ← ftn_charting + plays)
  - ✅ `nfl_contract_value` — fantasy points per $M APY (player_contract_value ← contracts + ff_opportunity)
  - ✅ `nfl_injury_return` — post-return snap-share recovery by injury type/position (injury_return_curve ← injuries + snap_counts)
- Spread/betting data is already queryable via `plays` table (`spread_line`, `total_line`) — consider a `nfl_betting_lines` convenience tool surfacing game-level spread/total without raw SQL.

## Technical Notes / Design Choices
- Preserve backward compatibility for existing pbp tools and CLI defaults.
- Prefer additive migrations; avoid breaking current `plays`-based queries.
- Introduce shared key strategy early (`game_id`, player IDs, team abbreviations) to prevent join fragmentation.
- Ingest the full nflverse family by default (every registry dataset) so the baked image always has whatever data a client might need; individual datasets remain selectable via `--dataset` for faster local/partial loads.
- Add source/freshness metadata to every serving response where practical.

## Risks and Mitigations
- **Schema volatility across sources** → typed normalization + schema drift migration helpers.
- **Data volume/performance** → partitioning by season/week, selective indexes, bounded query defaults.
- **Cross-source key mismatch** → canonical ID mapping tables and reconciliation tests.
- **Tool explosion** → group tools by domain with consistent naming and shared filter schema.

## Open Questions
1. Required freshness SLA by dataset (manual refresh vs scheduled/background refresh).
2. Whether `nfl_betting_lines` warrants a dedicated tool or if `nfl_schedule` (which already returns spread/total) is sufficient.

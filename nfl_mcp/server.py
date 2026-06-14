"""
NFL MCP Server
Tools: nfl_schema, nfl_status, nfl_query, nfl_search_plays, nfl_team_stats, nfl_player_stats, nfl_compare,
       nfl_catalog, nfl_roster, nfl_injuries, nfl_schedule, nfl_snap_counts
"""

import contextlib
import logging
from collections.abc import AsyncIterator
from typing import Any

from mcp.server import Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp.types import TextContent, Tool
from starlette.applications import Starlette
from starlette.routing import Route

from .tools import (
    nfl_schema, nfl_status, nfl_query, nfl_search_plays,
    nfl_team_stats, nfl_player_stats, nfl_compare,
    nfl_catalog, nfl_roster, nfl_injuries, nfl_schedule, nfl_snap_counts,
    nfl_fantasy_opportunity, nfl_fantasy_rankings, nfl_ftn_charting,
    nfl_td_luck, nfl_role_trend, nfl_separation_opportunity,
    nfl_drop_rate, nfl_contract_value, nfl_injury_return,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nfl-mcp")

_mcp_server = Server("nfl-mcp")


def _tool_error_payload(tool_name: str, exc: Exception) -> dict[str, Any]:
    if isinstance(exc, ValueError) and str(exc).startswith("Unknown tool:"):
        code = "UNKNOWN_TOOL"
    elif isinstance(exc, TimeoutError):
        code = "TIMEOUT"
    elif isinstance(exc, TypeError):
        code = "INVALID_ARGUMENTS"
    else:
        code = "TOOL_EXECUTION_ERROR"
    return {
        "ok": False,
        "error": {
            "code": code,
            "type": type(exc).__name__,
            "message": str(exc),
            "tool": tool_name,
        },
    }


TOOLS = [
    Tool(
        name="nfl_schema",
        description=(
            "Returns the NFL database schema. By default returns a compact summary with "
            "key columns and available categories. Pass category='<name>' for full column "
            "details on a specific section, or category='all' for everything. "
            "Pass table='<name>' to get the live column list for any non-pbp table."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "description": (
                        "Schema category to expand. Omit for summary. Use 'all' for full schema. "
                        "Options: game_context, teams, game_situation, play_details, timeouts, "
                        "score, boolean_outcomes, primary_players, special_teams_players, "
                        "defensive_players, fumble_players, penalties, probability_models, "
                        "epa, wpa, completion_probability, xyac, drive_data, "
                        "game_stadium_weather, vegas, aggregate_tables, query_tips"
                    ),
                },
                "table": {
                    "type": "string",
                    "description": "Table name to inspect (e.g. 'rosters', 'injuries', 'schedules'). Returns live column list from the database.",
                },
            },
        },
    ),
    Tool(
        name="nfl_status",
        description=(
            "Returns database health: play counts, loaded seasons, and a summary of all ingested datasets with row counts and last refresh time. Call this first to understand what data is available."
        ),
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="nfl_query",
        description=(
            "LAST RESORT: Execute raw SQL only when nfl_search_plays, nfl_team_stats, "
            "nfl_player_stats, and nfl_compare cannot answer the question. "
            "Requires calling nfl_schema first. Read-only SELECT only. "
            "500 row cap. 10s timeout."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "A SQL SELECT query.",
                },
                "max_rows": {
                    "type": "integer",
                    "description": "Max rows to return (default 100, max 500)",
                    "default": 100,
                    "maximum": 500,
                },
            },
            "required": ["sql"],
        },
    ),
    Tool(
        name="nfl_search_plays",
        description=(
            "PREFERRED for finding specific plays. Search by player, team, season, "
            "week, play type, situation, touchdowns, turnovers, or minimum yards. "
            "Returns plays sorted by impact (EPA). Use this instead of nfl_query "
            "whenever looking for plays."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "team": {
                    "type": "string",
                    "description": "Offensive team abbreviation (e.g. KC, PHI, BUF)",
                },
                "opponent": {
                    "type": "string",
                    "description": "Defensive team abbreviation",
                },
                "player": {
                    "type": "string",
                    "description": "Player name (partial match, e.g. 'Mahomes', 'J.Jefferson')",
                },
                "season": {
                    "type": "integer",
                    "description": "Exact season year (2013–2025). Use season_from/season_to for ranges.",
                },
                "season_from": {
                    "type": "integer",
                    "description": "Start of season range (inclusive), e.g. 2020 for 'since 2020'",
                },
                "season_to": {
                    "type": "integer",
                    "description": "End of season range (inclusive)",
                },
                "week": {
                    "type": "integer",
                    "description": "Week number (1–22)",
                },
                "season_type": {
                    "type": "string",
                    "enum": ["REG", "POST"],
                    "description": "Filter by season type: REG (regular season) or POST (playoffs)",
                },
                "play_type": {
                    "type": "string",
                    "enum": ["pass", "run", "field_goal", "punt", "kickoff"],
                    "description": "Type of play",
                },
                "situation": {
                    "type": "string",
                    "enum": ["red_zone", "third_down", "fourth_down", "two_minute"],
                    "description": "Game situation filter",
                },
                "is_touchdown": {
                    "type": "boolean",
                    "description": "Only touchdown plays",
                },
                "is_turnover": {
                    "type": "boolean",
                    "description": "Only turnover plays (INT or fumble lost)",
                },
                "min_yards": {
                    "type": "integer",
                    "description": "Minimum yards gained",
                },
                "max_rows": {
                    "type": "integer",
                    "description": "Max results (default 50, max 500)",
                    "default": 50,
                },
            },
        },
    ),
    Tool(
        name="nfl_team_stats",
        description=(
            "PREFERRED for any team-level question. Returns pre-aggregated offense, "
            "defense, and situational stats. Always use this instead of nfl_query "
            "for questions like 'how did [team] do', team rankings, or team efficiency."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "team": {
                    "type": "string",
                    "description": "Team abbreviation (e.g. KC, PHI, BUF, BAL)",
                },
                "season": {
                    "type": "integer",
                    "description": "Season year. Omit for all available seasons.",
                },
                "side": {
                    "type": "string",
                    "enum": ["offense", "defense", "situational", "both"],
                    "description": "Which stats to return (default: both offense + defense + situational)",
                    "default": "both",
                },
            },
            "required": ["team"],
        },
    ),
    Tool(
        name="nfl_player_stats",
        description=(
            "PREFERRED for any player stats question. Returns season-by-season "
            "aggregates for passing, rushing, or receiving. Always use this instead "
            "of nfl_query for questions about a player's performance, stats, or trends."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "player_name": {
                    "type": "string",
                    "description": "Player name (partial match, e.g. 'Mahomes', 'D.Henry', 'J.Jefferson')",
                },
                "season": {
                    "type": "integer",
                    "description": "Exact season year. Omit for all seasons.",
                },
                "season_from": {
                    "type": "integer",
                    "description": "Start of season range (inclusive), e.g. 2020 for 'since 2020'",
                },
                "season_to": {
                    "type": "integer",
                    "description": "End of season range (inclusive)",
                },
                "season_type": {
                    "type": "string",
                    "enum": ["REG", "POST"],
                    "description": "Filter by season type: REG (regular season) or POST (playoffs)",
                },
                "stat_type": {
                    "type": "string",
                    "enum": ["passing", "rushing", "receiving"],
                    "description": "Type of stats to return (default: passing)",
                    "default": "passing",
                },
            },
            "required": ["player_name"],
        },
    ),
    Tool(
        name="nfl_compare",
        description=(
            "PREFERRED for any comparison question. Side-by-side stats for two teams "
            "or two players in a single call. Always use this instead of calling "
            "nfl_team_stats or nfl_player_stats twice when the user asks to compare, "
            "rank, or contrast two teams or two players against each other."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "entity1": {
                    "type": "string",
                    "description": "First team abbreviation or player name",
                },
                "entity2": {
                    "type": "string",
                    "description": "Second team abbreviation or player name",
                },
                "compare_type": {
                    "type": "string",
                    "enum": ["team", "player"],
                    "description": "Whether comparing teams or players (default: team)",
                    "default": "team",
                },
                "season": {
                    "type": "integer",
                    "description": "Exact season year. Omit for all seasons.",
                },
                "season_from": {
                    "type": "integer",
                    "description": "Start of season range (inclusive), e.g. 2020 for 'since 2020'",
                },
                "season_to": {
                    "type": "integer",
                    "description": "End of season range (inclusive)",
                },
                "season_type": {
                    "type": "string",
                    "enum": ["REG", "POST"],
                    "description": "Filter by season type: REG (regular season) or POST (playoffs)",
                },
            },
            "required": ["entity1", "entity2"],
        },
    ),
    Tool(
        name="nfl_catalog",
        description=(
            "Returns a catalog of every dataset loaded into the local database: "
            "table name, row count, seasons available, and when it was last refreshed. "
            "Call this first to discover what data is available beyond play-by-play."
        ),
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="nfl_roster",
        description=(
            "PREFERRED for roster questions. Look up who was on a team's roster "
            "in a given season. Returns name, position, jersey number, experience, "
            "college, height, and weight. Filter by team, season, and/or position."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "team": {
                    "type": "string",
                    "description": "Team abbreviation (e.g. KC, PHI, BUF)",
                },
                "season": {
                    "type": "integer",
                    "description": "Season year (e.g. 2024). Omit for all seasons.",
                },
                "position": {
                    "type": "string",
                    "description": "Position filter (e.g. QB, WR, LB). Partial match supported.",
                },
            },
        },
    ),
    Tool(
        name="nfl_injuries",
        description=(
            "PREFERRED for injury report questions. Look up player injury status by "
            "team, season, week, player name, or report status (Out, Questionable, "
            "Doubtful, Full Participation). Returns primary/secondary injuries and "
            "both report and practice statuses."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "team": {
                    "type": "string",
                    "description": "Team abbreviation (e.g. KC, PHI)",
                },
                "season": {
                    "type": "integer",
                    "description": "Season year. Omit for all seasons.",
                },
                "week": {
                    "type": "integer",
                    "description": "Week number (1–22).",
                },
                "player": {
                    "type": "string",
                    "description": "Player name (partial match, e.g. 'Mahomes')",
                },
                "report_status": {
                    "type": "string",
                    "description": "Injury designation: 'Out', 'Questionable', 'Doubtful', 'Full Participation In Practice'",
                },
            },
        },
    ),
    Tool(
        name="nfl_schedule",
        description=(
            "PREFERRED for schedule and game result questions. Look up games by team, "
            "season, week, or season type. Returns scores, spread/total lines, "
            "weather, stadium, coaches, and referee."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "team": {
                    "type": "string",
                    "description": "Team abbreviation — returns all games where team is home or away.",
                },
                "season": {
                    "type": "integer",
                    "description": "Season year. Omit for all seasons.",
                },
                "week": {
                    "type": "integer",
                    "description": "Week number (1–22).",
                },
                "season_type": {
                    "type": "string",
                    "enum": ["REG", "POST", "SB"],
                    "description": "Season type filter: REG, POST, or SB (Super Bowl).",
                },
            },
        },
    ),
    Tool(
        name="nfl_snap_counts",
        description=(
            "PREFERRED for snap count questions. Look up how many offensive, defensive, "
            "and special teams snaps a player or team unit played. Filter by player, "
            "team, season, week, or position."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "player": {
                    "type": "string",
                    "description": "Player name (partial match, e.g. 'Kelce')",
                },
                "team": {
                    "type": "string",
                    "description": "Team abbreviation (e.g. KC, PHI)",
                },
                "season": {
                    "type": "integer",
                    "description": "Season year. Omit for all seasons.",
                },
                "week": {
                    "type": "integer",
                    "description": "Week number (1–22).",
                },
                "position": {
                    "type": "string",
                    "description": "Position filter (e.g. TE, WR, CB).",
                },
            },
        },
    ),
    Tool(
        name="nfl_fantasy_opportunity",
        description=(
            "PREFERRED for fantasy football opportunity questions. Look up target share, "
            "air yards share, carry share, and opportunity scores per player per week. "
            "Requires ff_opportunity dataset. Available 2006–present. Filter by player, "
            "team, season, week, or position."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "player": {
                    "type": "string",
                    "description": "Player name (partial match, e.g. 'Jefferson')",
                },
                "team": {
                    "type": "string",
                    "description": "Team abbreviation (e.g. KC, PHI)",
                },
                "season": {
                    "type": "integer",
                    "description": "Season year. Omit for all seasons.",
                },
                "week": {
                    "type": "integer",
                    "description": "Week number (1–22). Omit for full season.",
                },
                "position": {
                    "type": "string",
                    "description": "Position filter (e.g. WR, RB, TE).",
                },
            },
        },
    ),
    Tool(
        name="nfl_fantasy_rankings",
        description=(
            "PREFERRED for fantasy football ranking questions (expert consensus "
            "rankings / ECR, start-sit, draft and dynasty value). Two snapshots: "
            "scope='draft' for preseason/dynasty/best-ball draft rankings, scope='week' "
            "for the current week's start/sit rankings. Lower ECR = ranked higher. "
            "These are the latest scrape, not historical. Requires ff_rankings_draft / "
            "ff_rankings_week datasets. Filter by player, position, team, or ranking_set."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "player": {
                    "type": "string",
                    "description": "Player name (partial match, e.g. 'Jefferson')",
                },
                "position": {
                    "type": "string",
                    "description": "Position filter (e.g. QB, RB, WR, TE, K, DST).",
                },
                "team": {
                    "type": "string",
                    "description": "Team abbreviation (e.g. KC, PHI).",
                },
                "scope": {
                    "type": "string",
                    "enum": ["draft", "week"],
                    "description": "draft = preseason/dynasty/best-ball rankings; week = current-week start/sit rankings (default: draft).",
                    "default": "draft",
                },
                "ranking_set": {
                    "type": "string",
                    "description": "Filter the ranking list/format (partial match). For draft: 'redraft', 'dynasty', 'best', or position-specific like 'redraft-rb'. For week: 'ppr', 'qb', etc.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results (default 50, max 500).",
                    "default": 50,
                },
            },
        },
    ),
    Tool(
        name="nfl_ftn_charting",
        description=(
            "PREFERRED for play-style / scheme tendency questions (2022–present). "
            "Returns aggregated FTN manual charting rates over offensive scrimmage plays "
            "(pass + run): play-action, RPO, screen, no-huddle, pre-snap motion, and "
            "trick-play usage, plus average defenders in the box (per scrimmage play) and "
            "pass rushers / blitzers (per dropback). Use for questions like 'how often "
            "does KC run play action' or 'what motion rate did Mahomes see'. Offensive "
            "perspective (team = offense). Requires ftn_charting dataset."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "team": {
                    "type": "string",
                    "description": "Offensive team abbreviation (e.g. KC, PHI).",
                },
                "opponent": {
                    "type": "string",
                    "description": "Defensive team abbreviation.",
                },
                "player": {
                    "type": "string",
                    "description": "Player name (partial match) — matches passer, rusher, or receiver.",
                },
                "season": {
                    "type": "integer",
                    "description": "Exact season year (2022–present). Use season_from/season_to for ranges.",
                },
                "season_from": {
                    "type": "integer",
                    "description": "Start of season range (inclusive).",
                },
                "season_to": {
                    "type": "integer",
                    "description": "End of season range (inclusive).",
                },
                "week": {
                    "type": "integer",
                    "description": "Week number (1–22).",
                },
                "season_type": {
                    "type": "string",
                    "enum": ["REG", "POST"],
                    "description": "Filter by season type: REG or POST.",
                },
            },
        },
    ),
    Tool(
        name="nfl_td_luck",
        description=(
            "PREFERRED for touchdown luck / TD regression questions. Returns actual vs "
            "expected receiving and rushing touchdowns per player-season. Negative "
            "total_td_luck_score = scored fewer TDs than expected (positive-regression / "
            "'unlucky' bounce-back candidate); positive = over-performed (sell-high). Use "
            "for 'who was unlucky on TDs in 2024' or 'is player X due for TD regression'. "
            "Default sort is most unlucky first."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "player": {"type": "string", "description": "Player full name (partial match)."},
                "position": {"type": "string", "description": "Position filter (e.g. WR, RB, TE)."},
                "team": {"type": "string", "description": "Team abbreviation (e.g. KC)."},
                "season": {"type": "integer", "description": "Exact season year."},
                "limit": {"type": "integer", "description": "Max rows (default 50, max 500)."},
            },
        },
    ),
    Tool(
        name="nfl_role_trend",
        description=(
            "PREFERRED for usage-trend / breakout / fade questions. Returns weekly snap %, "
            "target share, carry share, and air-yards share with a trailing 3-week average "
            "and the current-week delta vs that average. Use for 'whose role is trending up' "
            "or 'is player X's snap share rising'. Default sort is biggest snap-share gain "
            "first (snap_pct_delta DESC)."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "player": {"type": "string", "description": "Player full name (partial match)."},
                "position": {"type": "string", "description": "Position filter (e.g. WR, RB)."},
                "team": {"type": "string", "description": "Team abbreviation."},
                "season": {"type": "integer", "description": "Exact season year."},
                "week": {"type": "integer", "description": "Week number."},
                "min_snap_pct": {"type": "number", "description": "Minimum snap % (0–100) for the week."},
                "limit": {"type": "integer", "description": "Max rows (default 50, max 500)."},
            },
        },
    ),
    Tool(
        name="nfl_separation_opportunity",
        description=(
            "PREFERRED for receiver regression / 'getting open but not producing' questions "
            "(2016–present). Joins Next Gen Stats separation and YAC-above-expected to fantasy "
            "opportunity per player-season. regression_candidate=true flags receivers creating "
            "separation (>2.5 yds) who under-produced (fp_diff_per_game < -1.5, td_luck < -1.0). "
            "Default sort is most under-producing first (fp_diff_per_game ASC)."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "player": {"type": "string", "description": "Player full name (partial match)."},
                "position": {"type": "string", "description": "Position filter (e.g. WR, TE)."},
                "team": {"type": "string", "description": "Team abbreviation."},
                "season": {"type": "integer", "description": "Exact season year (2016+)."},
                "regression_candidate": {
                    "type": "boolean",
                    "description": "If true, only return flagged positive-regression candidates.",
                },
                "limit": {"type": "integer", "description": "Max rows (default 50, max 500)."},
            },
        },
    ),
    Tool(
        name="nfl_drop_rate",
        description=(
            "PREFERRED for drops / catchable-target reliability questions (2022–present). "
            "Returns share of catchable targets dropped per receiver-season from FTN charting, "
            "plus contested targets and created receptions. Use for 'who drops the most passes' "
            "or 'what is player X's drop rate'. Player names are short form ('J.Jefferson'). "
            "Default sort is highest drop rate first."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "player": {"type": "string", "description": "Player name (partial match, short form)."},
                "team": {"type": "string", "description": "Team abbreviation."},
                "season": {"type": "integer", "description": "Exact season year (2022+)."},
                "min_targets": {"type": "integer", "description": "Minimum catchable targets to qualify."},
                "limit": {"type": "integer", "description": "Max rows (default 50, max 500)."},
            },
        },
    ),
    Tool(
        name="nfl_contract_value",
        description=(
            "PREFERRED for contract value / cost-efficiency questions. Returns fantasy points "
            "per $M of average per year (APY) using each player's active contract joined to "
            "seasonal fantasy production. Use for 'best fantasy value per dollar' or 'is player "
            "X worth his contract'. apy and cap_pct reflect the current active contract. Default "
            "sort is best value first (fp_per_million DESC)."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "player": {"type": "string", "description": "Player full name (partial match)."},
                "position": {"type": "string", "description": "Position filter (e.g. RB, WR)."},
                "team": {"type": "string", "description": "Team abbreviation."},
                "season": {"type": "integer", "description": "Exact season year."},
                "min_apy": {"type": "number", "description": "Minimum APY in $millions."},
                "max_apy": {"type": "number", "description": "Maximum APY in $millions."},
                "limit": {"type": "integer", "description": "Max rows (default 50, max 500)."},
            },
        },
    ),
    Tool(
        name="nfl_injury_return",
        description=(
            "PREFERRED for injury recovery / return-to-form questions. Returns post-return "
            "snap-share recovery as a percent of pre-injury baseline at +1..+8 weeks after an "
            "'Out' spell, bucketed by normalized injury type (hamstring, knee, ankle, …) and "
            "position. 100 = fully back to baseline usage. Use for 'how long until players "
            "recover snaps after a hamstring injury'. Default sort is by weeks since return."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "injury_type": {
                    "type": "string",
                    "description": (
                        "Normalized injury bucket (partial match). Valid buckets: hamstring, "
                        "knee, ankle, shoulder, concussion, groin, foot, calf, hip, back, "
                        "quadriceps, achilles, wrist, hand, elbow, toe, thigh, neck, ribs, "
                        "pectoral, other. Specific diagnoses are mapped to these buckets "
                        "(e.g. 'ACL' -> knee, 'hammy' -> hamstring); anything unmatched -> other."
                    ),
                },
                "position": {"type": "string", "description": "Position filter (e.g. WR, RB)."},
                "week_post_return": {
                    "type": "integer",
                    "description": "Specific week offset after return (1–8).",
                },
                "limit": {"type": "integer", "description": "Max rows (default 50, max 500)."},
            },
        },
    ),
]
async def list_tools() -> list[Tool]:
    return TOOLS


@_mcp_server.call_tool()
async def call_tool(name: str, arguments: Any) -> list[TextContent]:
    import json

    _DISPATCH = {
        "nfl_schema":       nfl_schema,
        "nfl_status":       nfl_status,
        "nfl_query":        nfl_query,
        "nfl_search_plays": nfl_search_plays,
        "nfl_team_stats":   nfl_team_stats,
        "nfl_player_stats": nfl_player_stats,
        "nfl_compare":      nfl_compare,
        "nfl_catalog":      nfl_catalog,
        "nfl_roster":       nfl_roster,
        "nfl_injuries":     nfl_injuries,
        "nfl_schedule":     nfl_schedule,
        "nfl_snap_counts":          nfl_snap_counts,
        "nfl_fantasy_opportunity":  nfl_fantasy_opportunity,
        "nfl_fantasy_rankings":     nfl_fantasy_rankings,
        "nfl_ftn_charting":         nfl_ftn_charting,
        "nfl_td_luck":              nfl_td_luck,
        "nfl_role_trend":           nfl_role_trend,
        "nfl_separation_opportunity": nfl_separation_opportunity,
        "nfl_drop_rate":            nfl_drop_rate,
        "nfl_contract_value":       nfl_contract_value,
        "nfl_injury_return":        nfl_injury_return,
    }

    try:
        fn = _DISPATCH.get(name)
        if fn is None:
            raise ValueError(f"Unknown tool: {name}")
        result = fn(**arguments) if arguments else fn()
        return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

    except Exception as e:
        logger.error(f"Error in {name}: {e}", exc_info=True)
        return [TextContent(type="text", text=json.dumps(_tool_error_payload(name, e), indent=2))]


def create_app() -> Starlette:
    """Return a Starlette ASGI app that serves the MCP server over Streamable HTTP."""
    session_manager = StreamableHTTPSessionManager(
        app=_mcp_server,
        json_response=False,
        stateless=False,
    )

    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        logger.info("Starting NFL MCP server (Streamable HTTP)")
        async with session_manager.run():
            yield
        logger.info("NFL MCP server stopped")

    from mcp.server.fastmcp.server import StreamableHTTPASGIApp

    return Starlette(
        lifespan=lifespan,
        routes=[Route("/mcp", endpoint=StreamableHTTPASGIApp(session_manager))],
    )

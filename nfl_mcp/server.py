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
            "or two players. Always use this instead of nfl_query when the user asks "
            "to compare, rank, or contrast two teams or players."
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
]


@_mcp_server.list_tools()
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
        "nfl_snap_counts":  nfl_snap_counts,
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

"""
NFL Play-by-Play MCP Server
Tools: nfl_schema, nfl_status, nfl_query, nfl_search_plays, nfl_team_stats, nfl_player_stats, nfl_compare
"""

import asyncio
import logging
import os
from typing import Any

import mcp.server.stdio
from mcp.server import Server
from mcp.types import TextContent, Tool

from .tools import (
    nfl_schema, nfl_status, nfl_query, nfl_search_plays,
    nfl_team_stats, nfl_player_stats, nfl_compare,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nfl-mcp")

server = Server("nfl-playbyplay")


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
            "details on a specific section, or category='all' for everything."
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
            },
        },
    ),
    Tool(
        name="nfl_status",
        description=(
            "Returns database health info: total plays, loaded seasons (with season types), "
            "and available tables. Call this to check what data is available before querying."
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
]


@server.list_tools()
async def list_tools() -> list[Tool]:
    return TOOLS


@server.call_tool()
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


async def run():
    logger.info("Starting NFL MCP Server")
    logger.info(f"DB: {os.getenv('DB_NAME', 'nflread')} @ {os.getenv('DB_HOST', 'localhost')}")
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())

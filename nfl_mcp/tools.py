"""
NFL MCP Server tools:
  nfl_schema        → returns column reference so the model knows what to query
  nfl_status        → database health: row counts, loaded seasons, tables
  nfl_query         → read-only SELECT with safety guardrails
  nfl_search_plays  → structured play search
  nfl_team_stats    → pre-aggregated team stats
  nfl_player_stats  → player stats by season
  nfl_compare       → side-by-side comparison
"""

import logging
import re
import threading
from typing import Any, Dict, Sequence

import duckdb

from .database import get_db_connection
from .schema_pbp import _SCHEMA_SUMMARY, _SCHEMA_CATEGORIES

logger = logging.getLogger("nfl-mcp")


# ── Safety guardrails ──────────────────────────────────────────────────────────

_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|REPLACE|UPSERT"
    r"|EXECUTE|EXEC|CALL|COPY|GRANT|REVOKE|VACUUM|ANALYZE|CLUSTER"
    r"|REINDEX|COMMENT|SECURITY|OWNER|TABLESPACE|SCHEMA"
    r"|SET|DO|LISTEN|NOTIFY|PREPARE|DEALLOCATE|LOAD|DISCARD|RESET"
    r"|pg_read_file|pg_read_binary_file|pg_write_file|pg_sleep"
    r"|lo_import|lo_export|dblink|current_setting"
    r"|pg_terminate_backend|pg_cancel_backend)\b",
    re.IGNORECASE,
)

_MAX_ROWS = 500
_QUERY_TIMEOUT_SECONDS = 10


def nfl_schema(category: str | None = None, table: str | None = None) -> Dict[str, Any]:
    """Return schema reference. Summary by default, or a specific category for detail.
    Pass table='<name>' to get the live column list for any non-pbp table."""
    if table is not None:
        try:
            rows = _execute(
                "SELECT column_name, data_type "
                "FROM information_schema.columns "
                "WHERE table_name = ? AND table_schema = 'main' "
                "ORDER BY ordinal_position",
                [table],
            )
            if rows:
                return {"table": table, "columns": rows}
            available = _execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            )
            return {
                "error": f"Table '{table}' not found in schema 'main'",
                "available_tables": [r["table_name"] for r in available],
            }
        except (duckdb.Error, ValueError, TimeoutError) as e:
            logger.error("Tool error: %s", e, exc_info=True)
            return {"error": str(e)}
    if category is None:
        try:
            tables = _execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_name NOT LIKE '\\_%' ESCAPE '\\' "
                "ORDER BY table_name"
            )
            table_names = [r["table_name"] for r in tables]
        except Exception:
            table_names = []
        return {
            "schema": _SCHEMA_SUMMARY,
            "available_tables": table_names,
            "hint": "Pass category='<name>' for full PBP column details. Pass table='<name>' for any other table's columns.",
        }
    cat = category.lower().strip()
    if cat == "all":
        full = "\n".join(v.strip() for v in _SCHEMA_CATEGORIES.values())
        return {"schema": full}
    if cat in _SCHEMA_CATEGORIES:
        return {"category": cat, "schema": _SCHEMA_CATEGORIES[cat].strip()}
    return {"error": f"Unknown category '{cat}'", "available": list(_SCHEMA_CATEGORIES.keys())}


def nfl_status() -> Dict[str, Any]:
    """Return database health: play counts, loaded seasons, and a summary of all ingested datasets."""
    try:
        total = _execute("SELECT COUNT(*) AS total_plays FROM plays")
        seasons = _execute(
            "SELECT season, season_type, COUNT(*) AS plays "
            "FROM plays GROUP BY season, season_type ORDER BY season, season_type"
        )
        min_max = _execute(
            "SELECT MIN(season) AS first_season, MAX(season) AS last_season, "
            "COUNT(DISTINCT season) AS num_seasons "
            "FROM plays"
        )
        datasets = _execute(
            "SELECT dataset_id, table_name, SUM(row_count) as total_rows "
            "FROM _ingest_metadata "
            "GROUP BY dataset_id, table_name "
            "ORDER BY dataset_id"
        )
        last_refreshed = _execute(
            "SELECT MAX(loaded_at) as last_refreshed FROM _ingest_metadata"
        )
        return {
            "plays": {
                "total_plays": total[0]["total_plays"] if total else 0,
                "season_range": min_max[0] if min_max else {},
                "seasons": seasons,
            },
            "datasets": {
                "total_loaded": len(datasets),
                "last_refreshed": last_refreshed[0]["last_refreshed"] if last_refreshed else None,
                "loaded": datasets,
            },
        }
    except (duckdb.Error, ValueError, TimeoutError) as e:
        logger.error("Tool error: %s", e, exc_info=True)
        return {"error": str(e)}


def nfl_query(sql: str, max_rows: int = 100) -> Dict[str, Any]:
    """
    Execute a read-only SQL SELECT against the nflread database.

    Safety rules:
      - Only SELECT statements allowed
      - Forbidden mutation/system keywords are blocked
      - Multiple statements (semicolons) blocked
      - Results capped at max_rows (hard max 500)
      - 10-second statement timeout
    """
    sql = sql.strip()

    if not re.match(r"^\s*SELECT\b", sql, re.IGNORECASE):
        return {"error": "Only SELECT queries are allowed."}

    match = _FORBIDDEN.search(sql)
    if match:
        return {"error": f"Forbidden keyword: '{match.group()}'"}

    sql_no_trailing = sql.rstrip(";")
    if ";" in sql_no_trailing:
        return {"error": "Multiple statements are not allowed."}

    max_rows = min(max_rows, _MAX_ROWS)
    safe_sql = f"SELECT * FROM ({sql_no_trailing}) AS _q LIMIT {max_rows + 1}"

    try:
        rows = _execute(safe_sql)

        truncated = len(rows) > max_rows
        rows = rows[:max_rows]

        return {
            "rows":      rows,
            "row_count": len(rows),
            "truncated": truncated,
        }

    except (duckdb.Error, ValueError, TimeoutError) as e:
        logger.error("Tool error: %s", e, exc_info=True)
        return {"error": str(e)}


def nfl_search_plays(
    team: str | None = None,
    opponent: str | None = None,
    player: str | None = None,
    season: int | None = None,
    season_from: int | None = None,
    season_to: int | None = None,
    week: int | None = None,
    season_type: str | None = None,
    play_type: str | None = None,
    situation: str | None = None,
    is_touchdown: bool = False,
    is_turnover: bool = False,
    min_yards: int | None = None,
    max_rows: int = 50,
) -> Dict[str, Any]:
    """Search for plays using structured filters instead of raw SQL."""
    conditions = []
    params: list[Any] = []
    if team:
        conditions.append("posteam = ?")
        params.append(team)
    if opponent:
        conditions.append("defteam = ?")
        params.append(opponent)
    if player:
        conditions.append(
            "(passer_player_name ILIKE ? "
            "OR rusher_player_name ILIKE ? "
            "OR receiver_player_name ILIKE ?)"
        )
        pattern = f"%{player}%"
        params.extend([pattern, pattern, pattern])
    if season:
        conditions.append("season = ?")
        params.append(int(season))
    if season_from:
        conditions.append("season >= ?")
        params.append(int(season_from))
    if season_to:
        conditions.append("season <= ?")
        params.append(int(season_to))
    if week:
        conditions.append("week = ?")
        params.append(int(week))
    if season_type:
        conditions.append("season_type = ?")
        params.append(season_type)
    if play_type:
        conditions.append("play_type = ?")
        params.append(play_type)
    if situation == "red_zone":
        conditions.append("yardline_100 <= 20")
    elif situation == "third_down":
        conditions.append("down = 3")
    elif situation == "fourth_down":
        conditions.append("down = 4")
    elif situation == "two_minute":
        conditions.append("qtr = 4 AND half_seconds_remaining <= 120")
    if is_touchdown:
        conditions.append("touchdown = 1")
    if is_turnover:
        conditions.append("(interception = 1 OR fumble_lost = 1)")
    if min_yards is not None:
        conditions.append("yards_gained >= ?")
        params.append(int(min_yards))

    where = " AND ".join(conditions) if conditions else "1=1"
    max_rows = min(max_rows, _MAX_ROWS)

    sql = (
        f"SELECT season, week, posteam, defteam, down, ydstogo, play_type, "
        f"yards_gained, epa, \"desc\", passer_player_name, rusher_player_name, "
        f"receiver_player_name, touchdown, interception "
        f"FROM plays WHERE {where} "
        f"ORDER BY ABS(epa) DESC LIMIT {max_rows}"
    )

    try:
        rows = _execute(sql, params)
        return {"rows": rows, "row_count": len(rows)}
    except (duckdb.Error, ValueError, TimeoutError) as e:
        logger.error("Tool error: %s", e, exc_info=True)
        return {"error": str(e)}


def nfl_team_stats(
    team: str,
    season: int | None = None,
    side: str = "both",
) -> Dict[str, Any]:
    """Get pre-aggregated team stats (offense, defense, or both)."""
    team = team.upper()
    results = {}
    season_clause = ""
    season_params: list[Any] = []
    if season:
        season_clause = " AND season_year = ?"
        season_params.append(int(season))

    try:
        if side in ("offense", "both"):
            sql = f"SELECT * FROM team_offense_stats WHERE team = ?{season_clause} ORDER BY season_year"
            results["offense"] = _execute(sql, [team, *season_params])

        if side in ("defense", "both"):
            sql = f"SELECT * FROM team_defense_stats WHERE team = ?{season_clause} ORDER BY season_year"
            results["defense"] = _execute(sql, [team, *season_params])

        if side in ("situational", "both"):
            sql = f"SELECT * FROM situational_stats WHERE team = ?{season_clause} ORDER BY season_year, situation"
            results["situational"] = _execute(sql, [team, *season_params])

        return results
    except (duckdb.Error, ValueError, TimeoutError) as e:
        logger.error("Tool error: %s", e, exc_info=True)
        return {"error": str(e)}


def nfl_player_stats(
    player_name: str,
    season: int | None = None,
    season_from: int | None = None,
    season_to: int | None = None,
    season_type: str | None = None,
    stat_type: str = "passing",
) -> Dict[str, Any]:
    """Aggregate player stats by season."""
    params: list[Any] = []

    if stat_type == "passing":
        sql = """
            SELECT season, season_type, COUNT(*) AS attempts,
                SUM(CASE WHEN complete_pass = 1 THEN 1 ELSE 0 END) AS completions,
                ROUND(100.0 * SUM(CASE WHEN complete_pass = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS comp_pct,
                SUM(COALESCE(passing_yards, 0)) AS yards,
                SUM(CASE WHEN pass_touchdown = 1 THEN 1 ELSE 0 END) AS touchdowns,
                SUM(CASE WHEN interception = 1 THEN 1 ELSE 0 END) AS interceptions,
                ROUND(AVG(epa), 3) AS avg_epa,
                ROUND(AVG(cpoe), 3) AS avg_cpoe,
                SUM(COALESCE(air_yards, 0)) AS air_yards,
                ROUND(AVG(air_yards), 1) AS avg_air_yards
            FROM plays
            WHERE passer_player_name ILIKE ? AND play_type = ?
        """
        params.extend([f"%{player_name}%", "pass"])
    elif stat_type == "rushing":
        sql = """
            SELECT season, season_type, COUNT(*) AS carries,
                SUM(COALESCE(rushing_yards, 0)) AS yards,
                ROUND(AVG(yards_gained), 1) AS yards_per_carry,
                SUM(CASE WHEN rush_touchdown = 1 THEN 1 ELSE 0 END) AS touchdowns,
                SUM(CASE WHEN fumble_lost = 1 THEN 1 ELSE 0 END) AS fumbles_lost,
                ROUND(AVG(epa), 3) AS avg_epa,
                SUM(CASE WHEN yards_gained >= 10 THEN 1 ELSE 0 END) AS explosive_runs
            FROM plays
            WHERE rusher_player_name ILIKE ? AND play_type = ?
        """
        params.extend([f"%{player_name}%", "run"])
    elif stat_type == "receiving":
        sql = """
            SELECT season, season_type, COUNT(*) AS targets,
                SUM(CASE WHEN complete_pass = 1 THEN 1 ELSE 0 END) AS receptions,
                SUM(COALESCE(receiving_yards, 0)) AS yards,
                ROUND(AVG(yards_after_catch), 1) AS avg_yac,
                SUM(CASE WHEN pass_touchdown = 1 THEN 1 ELSE 0 END) AS touchdowns,
                ROUND(AVG(epa), 3) AS avg_epa,
                SUM(CASE WHEN yards_gained >= 20 THEN 1 ELSE 0 END) AS explosive_plays
            FROM plays
            WHERE receiver_player_name ILIKE ? AND play_type = ?
        """
        params.extend([f"%{player_name}%", "pass"])
    else:
        return {"error": f"Unknown stat_type: {stat_type}. Use 'passing', 'rushing', or 'receiving'."}

    if season:
        sql += " AND season = ?"
        params.append(int(season))
    if season_from:
        sql += " AND season >= ?"
        params.append(int(season_from))
    if season_to:
        sql += " AND season <= ?"
        params.append(int(season_to))
    if season_type:
        sql += " AND season_type = ?"
        params.append(season_type)
    sql += " GROUP BY season, season_type ORDER BY season, season_type"

    try:
        rows = _execute(sql, params)
        return {"player": player_name, "stat_type": stat_type, "seasons": rows}
    except (duckdb.Error, ValueError, TimeoutError) as e:
        logger.error("Tool error: %s", e, exc_info=True)
        return {"error": str(e)}


def nfl_compare(
    entity1: str,
    entity2: str,
    compare_type: str = "team",
    season: int | None = None,
    season_from: int | None = None,
    season_to: int | None = None,
    season_type: str | None = None,
) -> Dict[str, Any]:
    """Side-by-side comparison of two teams or players."""
    e1, e2 = entity1, entity2

    try:
        if compare_type == "team":
            t1, t2 = e1.upper(), e2.upper()
            season_filter = ""
            season_params: list[Any] = []
            if season:
                season_filter += " AND season_year = ?"
                season_params.append(int(season))
            if season_from:
                season_filter += " AND season_year >= ?"
                season_params.append(int(season_from))
            if season_to:
                season_filter += " AND season_year <= ?"
                season_params.append(int(season_to))

            off1 = _execute(
                f"SELECT * FROM team_offense_stats WHERE team = ?{season_filter} ORDER BY season_year",
                [t1, *season_params],
            )
            off2 = _execute(
                f"SELECT * FROM team_offense_stats WHERE team = ?{season_filter} ORDER BY season_year",
                [t2, *season_params],
            )
            def1 = _execute(
                f"SELECT * FROM team_defense_stats WHERE team = ?{season_filter} ORDER BY season_year",
                [t1, *season_params],
            )
            def2 = _execute(
                f"SELECT * FROM team_defense_stats WHERE team = ?{season_filter} ORDER BY season_year",
                [t2, *season_params],
            )

            return {
                entity1: {"offense": off1, "defense": def1},
                entity2: {"offense": off2, "defense": def2},
            }

        elif compare_type == "player":
            season_filter = ""
            season_params: list[Any] = []
            if season:
                season_filter += " AND season = ?"
                season_params.append(int(season))
            if season_from:
                season_filter += " AND season >= ?"
                season_params.append(int(season_from))
            if season_to:
                season_filter += " AND season <= ?"
                season_params.append(int(season_to))
            if season_type:
                season_filter += " AND season_type = ?"
                season_params.append(season_type)

            result = {}
            for p, label in [(e1, entity1), (e2, entity2)]:
                stats = {}
                for stype, col, ptype in [
                    ("passing", "passer_player_name", "pass"),
                    ("rushing", "rusher_player_name", "run"),
                    ("receiving", "receiver_player_name", "pass"),
                ]:
                    count_sql = (
                        f"SELECT COUNT(*) AS n FROM plays "
                        f"WHERE {col} ILIKE ? AND play_type = ?{season_filter}"
                    )
                    count_row = _execute(count_sql, [f"%{p}%", ptype, *season_params])
                    if count_row and count_row[0].get("n", 0) > 0:
                        player_result = nfl_player_stats(
                            label, season=season, season_from=season_from,
                            season_to=season_to, season_type=season_type,
                            stat_type=stype,
                        )
                        if "seasons" in player_result:
                            stats[stype] = player_result["seasons"]
                result[label] = stats

            return result
        else:
            return {"error": "compare_type must be 'team' or 'player'"}

    except (duckdb.Error, ValueError, TimeoutError) as e:
        logger.error("Tool error: %s", e, exc_info=True)
        return {"error": str(e)}


def _execute(sql: str, params: Sequence[Any] | None = None) -> list[dict]:
    """Execute SQL on DuckDB with timeout and cancellation.

    Runs in a worker thread with a dedicated connection, and interrupts
    the active query if it exceeds the timeout.
    """
    result = [None]
    error = [None]
    thread_conn = [None]

    def _run():
        try:
            with get_db_connection() as conn:
                thread_conn[0] = conn
                if params is None:
                    rel = conn.execute(sql)
                else:
                    rel = conn.execute(sql, params)
                columns = [desc[0] for desc in rel.description]
                result[0] = [dict(zip(columns, row)) for row in rel.fetchall()]
        except Exception as e:
            error[0] = e

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=_QUERY_TIMEOUT_SECONDS)
    if t.is_alive():
        conn = thread_conn[0]
        if conn is not None:
            try:
                conn.interrupt()
            except Exception:
                logger.warning("Failed to interrupt timed out DuckDB query", exc_info=True)
        t.join(timeout=1)
        raise TimeoutError(f"Query exceeded {_QUERY_TIMEOUT_SECONDS} second timeout")
    if error[0]:
        raise error[0]
    return result[0]


def nfl_catalog() -> Dict[str, Any]:
    """List every loaded dataset with row counts and last-refresh timestamp."""
    try:
        rows = _execute("""
            SELECT
                dataset_id,
                table_name,
                SUM(row_count)   AS total_rows,
                COUNT(*)         AS seasons_loaded,
                MIN(season)      AS min_season,
                MAX(season)      AS max_season,
                MAX(loaded_at)   AS last_loaded
            FROM _ingest_metadata
            GROUP BY dataset_id, table_name
            ORDER BY dataset_id
        """)
        return {"datasets": rows, "total_datasets": len(rows)}
    except (duckdb.Error, ValueError, TimeoutError) as e:
        logger.error("Tool error: %s", e, exc_info=True)
        return {"error": str(e)}


def nfl_roster(
    team: str | None = None,
    season: int | None = None,
    position: str | None = None,
) -> Dict[str, Any]:
    """
    Look up roster data — who was on a team's roster in a given season.
    Returns name, position, status, jersey number, experience, and measurables.
    """
    conditions, params = [], []
    if team:
        conditions.append("team = ?")
        params.append(team.upper())
    if season:
        conditions.append("season = ?")
        params.append(int(season))
    if position:
        conditions.append("position ILIKE ?")
        params.append(position.upper())

    where = " AND ".join(conditions) if conditions else "1=1"
    sql = f"""
        SELECT full_name, position, depth_chart_position, team, season,
               jersey_number, status, years_exp, college, height, weight
        FROM rosters
        WHERE {where}
        ORDER BY position, full_name
        LIMIT 500
    """
    try:
        rows = _execute(sql, params if params else None)
        return {"players": rows, "count": len(rows)}
    except (duckdb.Error, ValueError, TimeoutError) as e:
        logger.error("Tool error: %s", e, exc_info=True)
        return {"error": str(e)}


def nfl_injuries(
    team: str | None = None,
    season: int | None = None,
    week: int | None = None,
    player: str | None = None,
    report_status: str | None = None,
) -> Dict[str, Any]:
    """
    Look up injury report data. Filter by team, season, week, player name, or
    report status (e.g. 'Out', 'Questionable', 'Doubtful', 'Full Participation').
    """
    conditions, params = [], []
    if team:
        conditions.append("team = ?")
        params.append(team.upper())
    if season:
        conditions.append("season = ?")
        params.append(int(season))
    if week:
        conditions.append("week = ?")
        params.append(int(week))
    if player:
        conditions.append("full_name ILIKE ?")
        params.append(f"%{player}%")
    if report_status:
        conditions.append("report_status ILIKE ?")
        params.append(f"%{report_status}%")

    where = " AND ".join(conditions) if conditions else "1=1"
    sql = f"""
        SELECT full_name, position, team, season, week,
               report_primary_injury, report_secondary_injury, report_status,
               practice_primary_injury, practice_secondary_injury, practice_status
        FROM injuries
        WHERE {where}
        ORDER BY season, week, team, full_name
        LIMIT 500
    """
    try:
        rows = _execute(sql, params if params else None)
        return {"injuries": rows, "count": len(rows)}
    except (duckdb.Error, ValueError, TimeoutError) as e:
        logger.error("Tool error: %s", e, exc_info=True)
        return {"error": str(e)}


def nfl_schedule(
    team: str | None = None,
    season: int | None = None,
    week: int | None = None,
    season_type: str | None = None,
) -> Dict[str, Any]:
    """
    Look up game schedules and results. Filter by team, season, week, or season type.
    Returns scores, spread lines, weather, stadium, and coaching info.
    """
    conditions, params = [], []
    if team:
        t = team.upper()
        conditions.append("(home_team = ? OR away_team = ?)")
        params.extend([t, t])
    if season:
        conditions.append("season = ?")
        params.append(int(season))
    if week:
        conditions.append("week = ?")
        params.append(int(week))
    if season_type:
        conditions.append("game_type = ?")
        params.append(season_type.upper())

    where = " AND ".join(conditions) if conditions else "1=1"
    sql = f"""
        SELECT game_id, season, week, game_type, gameday, gametime,
               away_team, home_team, away_score, home_score, result,
               overtime, spread_line, total_line, div_game,
               roof, surface, temp, wind,
               away_coach, home_coach, referee, stadium
        FROM schedules
        WHERE {where}
        ORDER BY season, week
        LIMIT 500
    """
    try:
        rows = _execute(sql, params if params else None)
        return {"games": rows, "count": len(rows)}
    except (duckdb.Error, ValueError, TimeoutError) as e:
        logger.error("Tool error: %s", e, exc_info=True)
        return {"error": str(e)}


def nfl_snap_counts(
    player: str | None = None,
    team: str | None = None,
    season: int | None = None,
    week: int | None = None,
    position: str | None = None,
) -> Dict[str, Any]:
    """
    Look up snap count data — how many snaps a player or team unit played.
    Returns offense, defense, and special teams snap counts and percentages.
    """
    conditions, params = [], []
    if player:
        conditions.append("player ILIKE ?")
        params.append(f"%{player}%")
    if team:
        conditions.append("team = ?")
        params.append(team.upper())
    if season:
        conditions.append("season = ?")
        params.append(int(season))
    if week:
        conditions.append("week = ?")
        params.append(int(week))
    if position:
        conditions.append("position ILIKE ?")
        params.append(position.upper())

    where = " AND ".join(conditions) if conditions else "1=1"
    sql = f"""
        SELECT player, position, team, opponent, season, week, game_type,
               offense_snaps, offense_pct, defense_snaps, defense_pct,
               st_snaps, st_pct
        FROM snap_counts
        WHERE {where}
        ORDER BY season, week, team, offense_snaps DESC NULLS LAST
        LIMIT 500
    """
    try:
        rows = _execute(sql, params if params else None)
        return {"snap_counts": rows, "count": len(rows)}
    except (duckdb.Error, ValueError, TimeoutError) as e:
        logger.error("Tool error: %s", e, exc_info=True)
        return {"error": str(e)}


def nfl_fantasy_opportunity(
    player: str | None = None,
    team: str | None = None,
    season: int | None = None,
    week: int | None = None,
    position: str | None = None,
) -> Dict[str, Any]:
    """
    Look up fantasy football opportunity data — target share, air yards share,
    carry share, and opportunity scores per player per week. Available 2006–present.
    """
    conditions, params = [], []
    if player:
        conditions.append("full_name ILIKE ?")
        params.append(f"%{player}%")
    if team:
        conditions.append("posteam = ?")
        params.append(team.upper())
    if season:
        conditions.append("season = ?")
        params.append(int(season))
    if week:
        conditions.append("week = ?")
        params.append(int(week))
    if position:
        conditions.append("position ILIKE ?")
        params.append(position.upper())

    where = " AND ".join(conditions) if conditions else "1=1"
    sql = f"""
        SELECT season, week, posteam, full_name, position, player_id,
               rec_attempt, rush_attempt, rec_air_yards,
               receptions, receptions_exp, rec_yards_gained, rush_yards_gained,
               rec_touchdown, rush_touchdown,
               total_fantasy_points, total_fantasy_points_exp, total_fantasy_points_diff,
               rec_attempt_team, rush_attempt_team
        FROM ff_opportunity
        WHERE {where}
        ORDER BY season DESC, week DESC, total_fantasy_points_exp DESC NULLS LAST
        LIMIT 500
    """
    try:
        rows = _execute(sql, params if params else None)
        return {"fantasy_opportunity": rows, "count": len(rows)}
    except (duckdb.Error, ValueError, TimeoutError) as e:
        logger.error("Tool error: %s", e, exc_info=True)
        return {"error": str(e)}


__all__ = [
    "nfl_schema", "nfl_status", "nfl_query",
    "nfl_search_plays", "nfl_team_stats", "nfl_player_stats", "nfl_compare",
    "nfl_catalog", "nfl_roster", "nfl_injuries", "nfl_schedule", "nfl_snap_counts",
    "nfl_fantasy_opportunity",
]

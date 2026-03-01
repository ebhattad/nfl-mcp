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

logger = logging.getLogger("nfl-mcp")

# ── Schema description ─────────────────────────────────────────────────────────
# Split by category so models can request only what they need.

_SCHEMA_SUMMARY = """
Database: nflread (DuckDB)
Table: plays (~595K rows, 2013–2025, 372 columns)

Key columns for common queries:
  season (INT), week (INT), season_type ('REG'|'POST'), game_id, game_date
  posteam, defteam, play_type ('pass'|'run'|'field_goal'|'punt'|'kickoff')
  down, ydstogo, yardline_100, yards_gained, epa, wpa, success
  passer_player_name, rusher_player_name, receiver_player_name
  passing_yards, rushing_yards, receiving_yards, air_yards, yards_after_catch
  touchdown, interception, fumble_lost, sack, complete_pass, first_down
  cp, cpoe, xpass, pass_oe

Player names are abbreviated: 'P.Mahomes', 'J.Allen', 'D.Henry'
Team abbreviations: KC, BUF, BAL, PHI, SF, DAL, DET, etc.

Aggregate tables (faster for team-level queries):
  team_offense_stats  (team, season_year, total_plays, total_yards, avg_epa, ...)
  team_defense_stats  (team, season_year, yards_allowed, sacks, turnovers_forced, ...)
  situational_stats   (team, season_year, situation, plays, avg_epa, conversion_pct)
  formation_effectiveness (team, season_year, formation, play_type, avg_epa, ...)

Categories available (pass category='<name>' for full detail):
  game_context, teams, game_situation, play_details, timeouts, score,
  boolean_outcomes, primary_players, special_teams_players, defensive_players,
  fumble_players, penalties, probability_models, epa, wpa, completion_probability,
  xyac, drive_data, game_stadium_weather, vegas, aggregate_tables, query_tips
""".strip()

_SCHEMA_CATEGORIES = {
    "game_context": """
GAME CONTEXT
  play_id          DOUBLE PRECISION  nflreadpy play identifier
  game_id          TEXT              e.g. '2024_01_KC_BAL'
  old_game_id      TEXT              legacy ESPN/NFL game ID
  nfl_api_id       TEXT              NFL API identifier
  season           INTEGER           2013–2025
  week             INTEGER           1–22 (regular + post)
  season_type      TEXT              'REG' | 'POST'
  game_date        TEXT              'YYYY-MM-DD'
  home_team        TEXT              home team abbreviation
  away_team        TEXT              away team abbreviation
  home_score       INTEGER           final home score
  away_score       INTEGER           final away score
  result           INTEGER           home_score - away_score (from home team perspective)
  total            INTEGER           home_score + away_score
  location         TEXT              'Home' | 'Neutral'
  start_time       TEXT              kickoff time
  time_of_day      TEXT              time of day string
  order_sequence   DOUBLE PRECISION  play ordering within game
  play_clock       TEXT              play clock at snap
  play_deleted     DOUBLE PRECISION  1 = play was deleted/reversed
  play_type_nfl    TEXT              NFL's own play type label
  aborted_play     DOUBLE PRECISION  1 = aborted play
""",
    "teams": """
TEAMS
  posteam          TEXT   offensive team abbreviation
  defteam          TEXT   defensive team abbreviation
  posteam_type     TEXT   'home' | 'away'
  side_of_field    TEXT   which team's side of field the ball is on

  Team abbreviations (use these exactly):
    AFC: KC  BUF  NE  MIA  NYJ  PIT  BAL  CLE  CIN  HOU  TEN  IND  JAX
         DEN  OAK (pre-2020) / LV (2020+)  LAC (SD pre-2017)
    NFC: PHI  DAL  WAS  NYG  ATL  NO  CAR  TB  CHI  GB  MIN  DET
         SEA  LAR (LA)  SF  ARI
""",
    "game_situation": """
GAME SITUATION
  qtr                        DOUBLE PRECISION  1–5 (5 = OT)
  game_half                  TEXT              'Half1' | 'Half2' | 'Overtime'
  quarter_end                DOUBLE PRECISION  1 = last play of quarter
  time                       TEXT              'MM:SS' remaining in quarter
  quarter_seconds_remaining  DOUBLE PRECISION  seconds left in quarter
  half_seconds_remaining     DOUBLE PRECISION  seconds left in half
  game_seconds_remaining     DOUBLE PRECISION  seconds left in game
  down                       DOUBLE PRECISION  1–4
  ydstogo                    DOUBLE PRECISION  yards needed for first down
  ydsnet                     DOUBLE PRECISION  net yards on drive so far
  yardline_100               DOUBLE PRECISION  yards from opponent end zone (1=goal line, 99=own 1)
  yrdln                      TEXT              field position string e.g. 'KC 35'
  goal_to_go                 INTEGER           1 = goal-to-go situation
  drive                      DOUBLE PRECISION  drive number within game
  fixed_drive                DOUBLE PRECISION  corrected drive number
  fixed_drive_result         TEXT              drive outcome: 'Touchdown','Field goal','Punt', etc.
  series                     DOUBLE PRECISION  series number within drive
  series_success             DOUBLE PRECISION  1 = series resulted in first down or TD
  series_result              TEXT              'First down','Touchdown','Punt','End of half', etc.
  sp                         DOUBLE PRECISION  1 = scoring play
  special_teams_play         DOUBLE PRECISION  1 = special teams play
  st_play_type               TEXT              special teams play type label
""",
    "play_details": """
PLAY DETAILS
  play_type        TEXT    'pass' | 'run' | 'field_goal' | 'punt' | 'kickoff'
                           | 'extra_point' | 'no_play' | 'qb_spike' | 'qb_kneel'
  desc             TEXT    raw play description — use ILIKE for text search
  yards_gained     DOUBLE PRECISION  net yards gained on play
  end_clock_time   TEXT    clock time at end of play
  end_yard_line    TEXT    yard line at end of play

  PASS PLAYS
  shotgun          DOUBLE PRECISION  1 = shotgun formation
  no_huddle        DOUBLE PRECISION  1 = no huddle
  qb_dropback      DOUBLE PRECISION  1 = QB dropped back (pass or scramble)
  qb_scramble      DOUBLE PRECISION  1 = QB scrambled
  pass_length      TEXT    'short' | 'deep'
  pass_location    TEXT    'left' | 'middle' | 'right'
  air_yards        DOUBLE PRECISION  yards thrown beyond line of scrimmage
  yards_after_catch DOUBLE PRECISION yards gained after catch

  RUN PLAYS
  run_location     TEXT    'left' | 'middle' | 'right'
  run_gap          TEXT    'tackle' | 'guard' | 'end'

  KICKS
  field_goal_result    TEXT    'made' | 'missed' | 'blocked'
  kick_distance        DOUBLE PRECISION  distance of kick in yards
  extra_point_result   TEXT    'good' | 'failed' | 'blocked' | 'aborted'
  two_point_conv_result TEXT   'success' | 'failure'

  SPECIAL FLAGS
  qb_kneel         DOUBLE PRECISION  1 = QB kneel
  qb_spike         DOUBLE PRECISION  1 = QB spike
""",
    "timeouts": """
TIMEOUTS
  timeout                   DOUBLE PRECISION  1 = timeout called
  timeout_team              TEXT              team that called timeout
  posteam_timeouts_remaining DOUBLE PRECISION 0–3
  defteam_timeouts_remaining DOUBLE PRECISION 0–3
  home_timeouts_remaining   DOUBLE PRECISION  0–3
  away_timeouts_remaining   DOUBLE PRECISION  0–3
""",
    "score": """
SCORE / GAME STATE
  posteam_score         DOUBLE PRECISION  offensive team score before play
  defteam_score         DOUBLE PRECISION  defensive team score before play
  score_differential    DOUBLE PRECISION  posteam minus defteam before play
  posteam_score_post    DOUBLE PRECISION  offensive team score after play
  defteam_score_post    DOUBLE PRECISION  defensive team score after play
  score_differential_post DOUBLE PRECISION posteam minus defteam after play
  total_home_score      DOUBLE PRECISION  cumulative home score at play
  total_away_score      DOUBLE PRECISION  cumulative away score at play
  td_team               TEXT              team that scored touchdown
  td_player_name        TEXT              player who scored TD
  td_player_id          TEXT
""",
    "boolean_outcomes": """
BOOLEAN OUTCOMES  (DOUBLE PRECISION 0/1 — filter with `= 1`)
  SCORING
    touchdown              pass_touchdown         rush_touchdown
    return_touchdown       extra_point_attempt    two_point_attempt
    field_goal_attempt     safety

  BALL MOVEMENT
    rush_attempt           pass_attempt           complete_pass
    incomplete_pass        first_down             first_down_rush
    first_down_pass        first_down_penalty     success
    pass                   rush                   play

  TURNOVERS
    interception           fumble                 fumble_lost
    fumble_forced          fumble_not_forced      fumble_out_of_bounds

  DEFENSIVE
    sack                   qb_hit                 tackled_for_loss
    solo_tackle            assist_tackle          tackle_with_assist

  CONVERSION ATTEMPTS
    third_down_converted   third_down_failed      fourth_down_converted
    fourth_down_failed     defensive_two_point_attempt  defensive_two_point_conv
    defensive_extra_point_attempt  defensive_extra_point_conv

  KICKS
    punt_blocked           punt_inside_twenty     punt_in_endzone
    punt_out_of_bounds     punt_downed            punt_fair_catch
    punt_attempt           kickoff_attempt        kickoff_inside_twenty
    kickoff_in_endzone     kickoff_out_of_bounds  kickoff_downed
    kickoff_fair_catch

  OTHER
    penalty                touchback              lateral_reception
    lateral_rush           lateral_return         lateral_recovery
    own_kickoff_recovery   own_kickoff_recovery_td replay_or_challenge
    out_of_bounds          home_opening_kickoff   special
""",
    "primary_players": """
PRIMARY PLAYERS
  Names use abbreviated format: 'P.Mahomes', 'T.Hill', 'D.Henry'

  passer_player_name    TEXT    quarterback who threw
  passer_player_id      TEXT
  passing_yards         DOUBLE PRECISION
  passer                TEXT    alternate passer name field
  passer_jersey_number  INTEGER
  passer_id             TEXT

  receiver_player_name  TEXT    intended receiver
  receiver_player_id    TEXT
  receiving_yards       DOUBLE PRECISION
  receiver              TEXT    alternate receiver name field
  receiver_jersey_number INTEGER
  receiver_id           TEXT

  rusher_player_name    TEXT    ball carrier on run
  rusher_player_id      TEXT
  rushing_yards         DOUBLE PRECISION
  rusher                TEXT    alternate rusher name field
  rusher_jersey_number  INTEGER
  rusher_id             TEXT

  name                  TEXT    primary player name (context-dependent)
  jersey_number         INTEGER primary player jersey number
  id                    TEXT    primary player ID

  fantasy_player_name   TEXT    fantasy-relevant player name
  fantasy_player_id     TEXT
  fantasy               TEXT    fantasy player name (alternate)
  fantasy_id            TEXT
""",
    "special_teams_players": """
SPECIAL TEAMS PLAYERS
  kicker_player_name             punter_player_name
  kicker_player_id               punter_player_id
  kickoff_returner_player_name   punt_returner_player_name
  kickoff_returner_player_id     punt_returner_player_id
  blocked_player_name            blocked_player_id
  own_kickoff_recovery_player_name  own_kickoff_recovery_player_id
  return_team                    return_yards
  safety_player_name             safety_player_id

  LATERAL PLAYS
  lateral_receiver_player_name   lateral_receiver_player_id   lateral_receiving_yards
  lateral_rusher_player_name     lateral_rusher_player_id     lateral_rushing_yards
  lateral_sack_player_name       lateral_sack_player_id
  lateral_interception_player_name  lateral_interception_player_id
  lateral_punt_returner_player_name lateral_punt_returner_player_id
  lateral_kickoff_returner_player_name lateral_kickoff_returner_player_id
""",
    "defensive_players": """
DEFENSIVE PLAYERS
  SACKS
  sack_player_name / sack_player_id
  half_sack_1_player_name / half_sack_1_player_id
  half_sack_2_player_name / half_sack_2_player_id

  TACKLES
  solo_tackle_1_player_name / solo_tackle_1_player_id / solo_tackle_1_team
  solo_tackle_2_player_name / solo_tackle_2_player_id / solo_tackle_2_team
  assist_tackle_1_player_name / assist_tackle_1_player_id / assist_tackle_1_team
  assist_tackle_2_player_name / assist_tackle_2_player_id / assist_tackle_2_team
  assist_tackle_3_player_name / assist_tackle_3_player_id / assist_tackle_3_team
  assist_tackle_4_player_name / assist_tackle_4_player_id / assist_tackle_4_team
  tackle_with_assist_1_player_name / tackle_with_assist_1_player_id / tackle_with_assist_1_team
  tackle_with_assist_2_player_name / tackle_with_assist_2_player_id / tackle_with_assist_2_team
  tackle_for_loss_1_player_name / tackle_for_loss_1_player_id
  tackle_for_loss_2_player_name / tackle_for_loss_2_player_id

  QB PRESSURE
  qb_hit_1_player_name / qb_hit_1_player_id
  qb_hit_2_player_name / qb_hit_2_player_id

  PASS DEFENSE
  pass_defense_1_player_name / pass_defense_1_player_id
  pass_defense_2_player_name / pass_defense_2_player_id

  INTERCEPTIONS
  interception_player_name / interception_player_id
  lateral_interception_player_name / lateral_interception_player_id
""",
    "fumble_players": """
FUMBLE PLAYERS
  fumbled_1_player_name / fumbled_1_player_id / fumbled_1_team
  fumbled_2_player_name / fumbled_2_player_id / fumbled_2_team
  forced_fumble_player_1_player_name / forced_fumble_player_1_player_id / forced_fumble_player_1_team
  forced_fumble_player_2_player_name / forced_fumble_player_2_player_id / forced_fumble_player_2_team
  fumble_recovery_1_player_name / fumble_recovery_1_player_id / fumble_recovery_1_team / fumble_recovery_1_yards
  fumble_recovery_2_player_name / fumble_recovery_2_player_id / fumble_recovery_2_team / fumble_recovery_2_yards
""",
    "penalties": """
PENALTIES
  penalty_team          TEXT    team penalized
  penalty_player_name   TEXT    player who committed penalty
  penalty_player_id     TEXT
  penalty_yards         DOUBLE PRECISION  yards assessed
  penalty_type          TEXT    e.g. 'Offensive Holding', 'Pass Interference'
  replay_or_challenge         DOUBLE PRECISION  1 = replay/challenge on this play
  replay_or_challenge_result  TEXT              'upheld' | 'reversed' | 'denied'
""",
    "probability_models": """
PROBABILITY MODELS  (pre-play estimates)
  ep               DOUBLE PRECISION  expected points before play
  no_score_prob    DOUBLE PRECISION  P(no score this drive)
  opp_fg_prob      DOUBLE PRECISION  P(opponent field goal)
  opp_safety_prob  DOUBLE PRECISION  P(opponent safety)
  opp_td_prob      DOUBLE PRECISION  P(opponent touchdown)
  fg_prob          DOUBLE PRECISION  P(posteam field goal)
  safety_prob      DOUBLE PRECISION  P(posteam safety)
  td_prob          DOUBLE PRECISION  P(posteam touchdown)
  extra_point_prob DOUBLE PRECISION  P(extra point success)
  two_point_conversion_prob DOUBLE PRECISION  P(2pt conversion success)
""",
    "epa": """
EPA — EXPECTED POINTS ADDED  (positive = good for offense)
  epa              DOUBLE PRECISION  total EPA on play
  qb_epa           DOUBLE PRECISION  EPA credited to QB (pass + scramble)

  CUMULATIVE (running totals within game at time of play)
  total_home_epa   total_away_epa
  total_home_rush_epa   total_away_rush_epa
  total_home_pass_epa   total_away_pass_epa

  PASS DECOMPOSITION
  air_epa          EPA from air yards component
  yac_epa          EPA from yards-after-catch component
  comp_air_epa     air_epa on completed passes only
  comp_yac_epa     yac_epa on completed passes only
  total_home_comp_air_epa   total_away_comp_air_epa
  total_home_comp_yac_epa   total_away_comp_yac_epa
  total_home_raw_air_epa    total_away_raw_air_epa   (all targets, not just completions)
  total_home_raw_yac_epa    total_away_raw_yac_epa
""",
    "wpa": """
WPA — WIN PROBABILITY ADDED
  wp               DOUBLE PRECISION  win probability for posteam before play
  def_wp           DOUBLE PRECISION  win probability for defteam before play
  home_wp          DOUBLE PRECISION  home team win probability before play
  away_wp          DOUBLE PRECISION  away team win probability before play
  home_wp_post     DOUBLE PRECISION  home team win probability after play
  away_wp_post     DOUBLE PRECISION  away team win probability after play
  wpa              DOUBLE PRECISION  win probability added (posteam perspective)
  vegas_wp         DOUBLE PRECISION  Vegas-adjusted win probability (posteam)
  vegas_home_wp    DOUBLE PRECISION  Vegas-adjusted win probability (home)
  vegas_wpa        DOUBLE PRECISION  Vegas-adjusted WPA (posteam)
  vegas_home_wpa   DOUBLE PRECISION  Vegas-adjusted WPA (home)

  CUMULATIVE WPA
  total_home_rush_wpa   total_away_rush_wpa
  total_home_pass_wpa   total_away_pass_wpa
  air_wpa / yac_wpa     comp_air_wpa / comp_yac_wpa
  total_home_comp_air_wpa  total_away_comp_air_wpa
  total_home_comp_yac_wpa  total_away_comp_yac_wpa
  total_home_raw_air_wpa   total_away_raw_air_wpa
  total_home_raw_yac_wpa   total_away_raw_yac_wpa
""",
    "completion_probability": """
COMPLETION PROBABILITY
  cp       DOUBLE PRECISION  completion probability (model estimate)
  cpoe     DOUBLE PRECISION  completion % over expected (cp - actual completion rate)
  xpass    DOUBLE PRECISION  expected pass rate given game situation
  pass_oe  DOUBLE PRECISION  pass rate over expected (actual - xpass)
""",
    "xyac": """
EXPECTED YARDS AFTER CATCH (xYAC)
  xyac_epa           DOUBLE PRECISION  EPA from expected YAC
  xyac_mean_yardage  DOUBLE PRECISION  mean expected YAC
  xyac_median_yardage INTEGER          median expected YAC
  xyac_success       DOUBLE PRECISION  P(YAC results in success)
  xyac_fd            DOUBLE PRECISION  P(YAC results in first down)
""",
    "drive_data": """
DRIVE DATA
  drive_play_count         DOUBLE PRECISION  plays on this drive
  drive_time_of_possession TEXT              drive TOP e.g. '4:32'
  drive_first_downs        DOUBLE PRECISION  first downs on drive
  drive_inside20           DOUBLE PRECISION  1 = drive reached red zone
  drive_ended_with_score   DOUBLE PRECISION  1 = drive resulted in score
  drive_quarter_start      DOUBLE PRECISION  quarter drive began
  drive_quarter_end        DOUBLE PRECISION  quarter drive ended
  drive_yards_penalized    DOUBLE PRECISION  penalty yards on drive
  drive_start_transition   TEXT              how drive started: 'KICKOFF','PUNT','INTERCEPTION', etc.
  drive_end_transition     TEXT              how drive ended: 'TOUCHDOWN','FIELD_GOAL','PUNT', etc.
  drive_game_clock_start   TEXT              game clock when drive started
  drive_game_clock_end     TEXT              game clock when drive ended
  drive_start_yard_line    TEXT              yard line where drive started
  drive_end_yard_line      TEXT              yard line where drive ended
  drive_play_id_started    DOUBLE PRECISION  play_id of first play in drive
  drive_play_id_ended      DOUBLE PRECISION  play_id of last play in drive
  drive_real_start_time    TEXT              wall-clock time drive started
""",
    "game_stadium_weather": """
GAME / STADIUM / WEATHER
  stadium       TEXT     stadium name
  game_stadium  TEXT     game-level stadium name
  stadium_id    TEXT     stadium identifier
  roof          TEXT     'outdoors' | 'dome' | 'open' | 'closed'
  surface       TEXT     'grass' | 'fieldturf' | 'astroturf' | 'dessograss', etc.
  temp          INTEGER  temperature in °F (NULL for dome/indoor)
  wind          INTEGER  wind speed in mph (NULL for dome/indoor)
  weather       TEXT     raw weather string description
  div_game      INTEGER  1 = divisional game
  home_coach    TEXT     head coach of home team
  away_coach    TEXT     head coach of away team
""",
    "vegas": """
VEGAS / BETTING
  spread_line   DOUBLE PRECISION  point spread (negative = home favored)
  total_line    DOUBLE PRECISION  over/under total
""",
    "aggregate_tables": """
AGGREGATE TABLES  (pre-aggregated, much faster for team/season queries)
  team_offense_stats
    (team, season_year, total_plays, total_yards, yards_per_play,
     rush_plays, pass_plays, rush_yards, pass_yards, yards_per_rush, yards_per_pass,
     touchdowns, turnovers, third_down_pct, red_zone_td_pct, explosive_plays,
     avg_epa, pass_epa, rush_epa)

  team_defense_stats
    (team, season_year, plays_against, yards_allowed, yards_per_play_allowed,
     sacks, interceptions, turnovers_forced, third_down_stop_pct, avg_epa_allowed)

  situational_stats
    (team, season_year, situation, plays, avg_yards, touchdowns, conversion_pct, avg_epa)
    -- situation values: '3rd & Long','3rd & Short','Red Zone','4th Down',
    --                   'Two Minute Drill','Standard'

  formation_effectiveness
    (team, season_year, formation, play_type, plays, avg_yards, touchdowns, turnovers, avg_epa)
    -- formation values: 'SHOTGUN','UNDER CENTER','NO HUDDLE','SHOTGUN NO HUDDLE'
""",
    "query_tips": """
QUERY TIPS
  • Boolean outcome columns are DOUBLE PRECISION 0/1 — filter with `= 1` not `IS TRUE`
  • play_type is lowercase: 'pass', 'run', 'field_goal' — not uppercase
  • Player names are abbreviated: 'P.Mahomes', 'T.Hill', 'D.Henry', 'J.Jefferson'
  • Use aggregate tables for season-level team stats (much faster than raw plays)
  • Use plays table directly for play-level filtering or custom aggregations
  • Use ILIKE '%keyword%' for text search in the desc column
  • Exclude non-play rows: WHERE play_type NOT IN ('no_play','qb_spike','qb_kneel')
    or WHERE rush_attempt = 1 OR pass_attempt = 1
  • Exclude special teams: WHERE special_teams_play = 0 OR special_teams_play IS NULL
  • For REG season only: WHERE season_type = 'REG'
  • game_date is TEXT in 'YYYY-MM-DD' format — cast if needed: game_date::date
  • `success` = 1 means play met the success threshold (50%+ of yards on 1st, 70%+ on 2nd, 100% on 3rd/4th)
  • OT is qtr = 5
""",
}

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


def nfl_schema(category: str | None = None) -> Dict[str, Any]:
    """Return schema reference. Summary by default, or a specific category for detail."""
    if category is None:
        return {"schema": _SCHEMA_SUMMARY, "hint": "Pass category='<name>' for full column details on a specific section."}
    cat = category.lower().strip()
    if cat == "all":
        full = "\n".join(v.strip() for v in _SCHEMA_CATEGORIES.values())
        return {"schema": full}
    if cat in _SCHEMA_CATEGORIES:
        return {"category": cat, "schema": _SCHEMA_CATEGORIES[cat].strip()}
    return {"error": f"Unknown category '{cat}'", "available": list(_SCHEMA_CATEGORIES.keys())}


def nfl_status() -> Dict[str, Any]:
    """Return database health: row counts, loaded seasons, season types, and table list."""
    try:
        total = _execute("SELECT COUNT(*) AS total_plays FROM plays")
        seasons = _execute(
            "SELECT season, season_type, COUNT(*) AS plays "
            "FROM plays GROUP BY season, season_type ORDER BY season, season_type"
        )
        tables = _execute(
            "SELECT table_name, estimated_size "
            "FROM duckdb_tables() ORDER BY table_name"
        )
        min_max = _execute(
            "SELECT MIN(season) AS first_season, MAX(season) AS last_season, "
            "COUNT(DISTINCT season) AS num_seasons "
            "FROM plays"
        )
        return {
            "total_plays": total[0]["total_plays"] if total else 0,
            "season_range": min_max[0] if min_max else {},
            "seasons": seasons,
            "tables": tables,
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


__all__ = ["nfl_schema", "nfl_query", "nfl_search_plays", "nfl_team_stats", "nfl_player_stats", "nfl_compare"]

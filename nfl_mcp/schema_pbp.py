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

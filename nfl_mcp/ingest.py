"""
NFL data ingestion into DuckDB.

Supports all nflreadpy datasets via a declarative registry.
PBP ingestion preserves its existing enhanced_description logic.

Called by: nfl-mcp init / nfl-mcp ingest
"""

import datetime
import os
import duckdb
import polars as pl
from pathlib import Path
from tqdm import tqdm

ALL_SEASONS = list(range(2013, 2026))


def _apply_duckdb_pragmas(conn: duckdb.DuckDBPyConnection, db_path: str) -> None:
    """Bound DuckDB memory so large ingests spill to disk instead of being OOM-killed.

    Driven by env vars so containers/CI can cap memory (e.g. when baking the
    database into a Docker image) while local runs keep DuckDB's defaults:
      NFL_MCP_DUCKDB_MEMORY_LIMIT  e.g. '5GB'
      NFL_MCP_DUCKDB_THREADS       e.g. '2'
    """
    mem = os.getenv("NFL_MCP_DUCKDB_MEMORY_LIMIT")
    if mem:
        conn.execute(f"SET memory_limit='{mem}'")
        spill = str(Path(db_path).parent / ".duckdb_spill")
        conn.execute(f"SET temp_directory='{spill}'")
    threads = os.getenv("NFL_MCP_DUCKDB_THREADS")
    if threads:
        conn.execute(f"SET threads={int(threads)}")


# ── Helpers ────────────────────────────────────────────────────────────────────

def _safe_col(name: str) -> str:
    return name.replace(".", "_").replace(" ", "_").replace("-", "_")


def _safe_rename(df: pl.DataFrame) -> pl.DataFrame:
    return df.rename({c: _safe_col(c) for c in df.columns})


def _str(val) -> str:
    if val is None:
        return ""
    s = str(val)
    return "" if s in ("None", "nan", "NaN", "") else s


def _duckdb_type_for_polars(dtype: pl.DataType) -> str:
    if dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64,
                 pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
        return "BIGINT"
    if dtype in (pl.Float32, pl.Float64):
        return "DOUBLE"
    if dtype == pl.Boolean:
        return "BOOLEAN"
    if dtype == pl.Date:
        return "DATE"
    if dtype == pl.Time:
        return "TIME"
    if dtype == pl.Datetime:
        return "TIMESTAMP"
    return "VARCHAR"


# ── Metadata table ─────────────────────────────────────────────────────────────

def _ensure_metadata_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create _ingest_metadata if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _ingest_metadata (
            dataset_id  VARCHAR NOT NULL,
            table_name  VARCHAR NOT NULL,
            season      INTEGER,          -- NULL for static (non-seasonal) datasets
            row_count   BIGINT,
            loaded_at   TIMESTAMP NOT NULL,
            loader_fn   VARCHAR
        )
    """)


def _is_loaded(conn: duckdb.DuckDBPyConnection, dataset_id: str, season: int | None = None) -> bool:
    """Return True if this dataset (+ season) is already recorded in metadata."""
    try:
        if season is None:
            result = conn.execute(
                "SELECT COUNT(*) FROM _ingest_metadata WHERE dataset_id = ? AND season IS NULL",
                [dataset_id],
            ).fetchone()
        else:
            result = conn.execute(
                "SELECT COUNT(*) FROM _ingest_metadata WHERE dataset_id = ? AND season = ?",
                [dataset_id, season],
            ).fetchone()
        return (result[0] or 0) > 0
    except duckdb.CatalogException:
        return False


def _record_loaded(
    conn: duckdb.DuckDBPyConnection,
    dataset_id: str,
    table_name: str,
    loader_fn: str,
    row_count: int,
    season: int | None = None,
) -> None:
    """Upsert a completion record into _ingest_metadata."""
    # Delete any prior record for this dataset+season before inserting fresh
    if season is None:
        conn.execute(
            "DELETE FROM _ingest_metadata WHERE dataset_id = ? AND season IS NULL",
            [dataset_id],
        )
    else:
        conn.execute(
            "DELETE FROM _ingest_metadata WHERE dataset_id = ? AND season = ?",
            [dataset_id, season],
        )
    conn.execute(
        """
        INSERT INTO _ingest_metadata (dataset_id, table_name, season, row_count, loaded_at, loader_fn)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [dataset_id, table_name, season, row_count, datetime.datetime.now(datetime.timezone.utc), loader_fn],
    )


# ── PBP-specific helpers (preserved from original) ────────────────────────────

def _build_enhanced_description(row: dict) -> str:
    parts = []
    try:
        d, ytg = row.get("down"), row.get("ydstogo")
        if d and _str(d):
            parts.append(f"{int(float(d))} & {int(float(ytg))}")
    except (TypeError, ValueError):
        pass

    try:
        qtr = row.get("qtr")
        if qtr and _str(qtr):
            parts.append(f"Q{int(float(qtr))}")
    except (TypeError, ValueError):
        pass

    season  = _str(row.get("season"))
    week    = _str(row.get("week"))
    stype   = _str(row.get("season_type")) or "REG"
    posteam = _str(row.get("posteam"))
    defteam = _str(row.get("defteam"))

    if week:
        try:
            parts.append(f"Week {int(float(week))} {season} ({stype}): {posteam} vs {defteam}")
        except (TypeError, ValueError):
            parts.append(f"{season} ({stype}): {posteam} vs {defteam}")
    else:
        parts.append(f"{season} ({stype}): {posteam} vs {defteam}")

    pt = _str(row.get("play_type"))
    if pt:
        parts.append(pt.upper())

    for label, key in [("QB", "passer_player_name"),
                        ("RB", "rusher_player_name"),
                        ("Receiver", "receiver_player_name")]:
        name = _str(row.get(key))
        if name:
            parts.append(f"{label}: {name}")

    desc = _str(row.get("desc"))
    if desc:
        parts.append(desc)

    tags = []
    try:
        yl = row.get("yardline_100")
        if yl is not None and float(yl) <= 20:
            tags.append("Red Zone")
    except (TypeError, ValueError):
        pass
    try:
        dv = row.get("down")
        if dv is not None:
            d = int(float(dv))
            if d == 3: tags.append("3rd Down")
            if d == 4: tags.append("4th Down")
    except (TypeError, ValueError):
        pass
    if row.get("touchdown") in (1, 1.0, True):
        tags.append("TOUCHDOWN")
    if row.get("interception") in (1, 1.0, True) or \
       row.get("fumble_lost") in (1, 1.0, True):
        tags.append("TURNOVER")
    try:
        yg = float(row.get("yards_gained") or 0)
        if (row.get("pass_attempt") in (1, 1.0) and yg >= 20) or \
           (row.get("rush_attempt") in (1, 1.0) and yg >= 10):
            tags.append("EXPLOSIVE")
    except (TypeError, ValueError):
        pass
    if stype == "POST":
        tags.append("PLAYOFFS")

    if tags:
        parts.append(f"[{', '.join(tags)}]")

    return " — ".join(parts)


def _reconcile_schema(conn: duckdb.DuckDBPyConnection, table: str, df: pl.DataFrame) -> None:
    """Add any new incoming columns to an existing table before insert-by-name."""
    existing_rows = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
    existing_cols = {row[1] for row in existing_rows}

    added = []
    for col_name, dtype in df.schema.items():
        if col_name in existing_cols:
            continue
        duck_type = _duckdb_type_for_polars(dtype)
        conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col_name}" {duck_type}')
        added.append(f"{col_name} ({duck_type})")

    if added:
        print(f"    Schema drift: added {', '.join(added)}")


# ── PBP ingestion (season-by-season with enhanced_description) ─────────────────

def _create_plays_table(conn: duckdb.DuckDBPyConnection, sample_df: pl.DataFrame, fresh: bool = False):
    if fresh:
        conn.execute("DROP TABLE IF EXISTS plays")
        print("  Dropped existing plays table")

    renamed = _safe_rename(sample_df.head(0))
    try:
        conn.execute("SELECT 1 FROM plays LIMIT 0")
        print("  plays table already exists")
    except duckdb.CatalogException:
        conn.register("_schema_df", renamed.to_arrow())
        conn.execute(
            "CREATE TABLE plays AS "
            "SELECT *, '' AS enhanced_description FROM _schema_df"
        )
        conn.unregister("_schema_df")
        print(f"  plays table created — {len(sample_df.columns) + 1} columns")


def _ingest_pbp_season(conn: duckdb.DuckDBPyConnection, season: int) -> int:
    import nflreadpy
    print(f"\n  {season}")
    try:
        df = nflreadpy.load_pbp([season])
    except Exception as e:
        print(f"    Failed to load: {e}")
        return 0

    print(f"    {len(df):,} plays, {len(df.columns)} columns")

    before = len(df)
    if "posteam" in df.columns and "defteam" in df.columns:
        df = df.filter(
            pl.col("posteam").is_not_null() & pl.col("defteam").is_not_null()
        )
    dropped = before - len(df)
    if dropped:
        print(f"    Filtered {dropped:,} rows (null posteam/defteam)")

    df = _safe_rename(df)
    total = len(df)
    descriptions = [
        _build_enhanced_description(row)
        for row in tqdm(df.iter_rows(named=True), total=total, desc=f"    {season} descriptions")
    ]
    df = df.with_columns(pl.Series("enhanced_description", descriptions))

    _reconcile_schema(conn, "plays", df)
    conn.register("_ingest_df", df.to_arrow())
    conn.execute("INSERT INTO plays BY NAME SELECT * FROM _ingest_df")
    conn.unregister("_ingest_df")
    print(f"    {total:,} rows inserted")
    return total


# ── Generic dataset ingestion ──────────────────────────────────────────────────

def _write_df_to_table(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    df: pl.DataFrame,
    replace: bool = False,
) -> None:
    """Create or append df into a DuckDB table."""
    if replace:
        conn.execute(f"DROP TABLE IF EXISTS {table}")

    try:
        conn.execute(f"SELECT 1 FROM {table} LIMIT 0")
        _reconcile_schema(conn, table, df)
        conn.register("_load_df", df.to_arrow())
        conn.execute(f"INSERT INTO {table} BY NAME SELECT * FROM _load_df")
        conn.unregister("_load_df")
    except duckdb.CatalogException:
        conn.register("_load_df", df.to_arrow())
        conn.execute(f"CREATE TABLE {table} AS SELECT * FROM _load_df")
        conn.unregister("_load_df")


def _ingest_generic_dataset(
    conn: duckdb.DuckDBPyConnection,
    dataset_def,                        # DatasetDef from registry
    seasons: list[int] | None,          # None = load all via seasons=True
    fresh: bool = False,
) -> int:
    """Load a single dataset (non-pbp) into DuckDB. Returns total rows inserted."""
    import nflreadpy

    loader = getattr(nflreadpy, dataset_def.loader_fn)
    table = dataset_def.table_name
    total_rows = 0
    bulk_mode = seasons is None  # pass True to loader instead of per-season list

    if not dataset_def.seasonal:
        # Static dataset — one load, replace table
        print(f"\n  {dataset_def.dataset_id} (static)")

        if _is_loaded(conn, dataset_def.dataset_id) and not fresh:
            print(f"    Already loaded, skipping")
            return 0

        try:
            df = loader(**dataset_def.extra_params)
        except Exception as e:
            print(f"    Failed to load: {e}")
            return 0

        if df is None or len(df) == 0:
            print(f"    No data returned")
            return 0

        df = _safe_rename(df)
        print(f"    {len(df):,} rows, {len(df.columns)} columns")
        _write_df_to_table(conn, table, df, replace=True)
        _record_loaded(conn, dataset_def.dataset_id, table, dataset_def.loader_fn, len(df))
        total_rows = len(df)

    else:
        # Seasonal dataset — iterate per season (bulk mode uses ALL_SEASONS)
        season_list = ALL_SEASONS if bulk_mode else seasons
        for season in season_list:
            # Skip seasons outside the dataset's known coverage window
            if dataset_def.min_season and season < dataset_def.min_season:
                continue
            if dataset_def.max_season and season > dataset_def.max_season:
                continue

            if _is_loaded(conn, dataset_def.dataset_id, season) and not fresh:
                print(f"    {dataset_def.dataset_id} {season}: already loaded, skipping")
                continue

            print(f"\n  {dataset_def.dataset_id} {season}")
            try:
                df = loader([season], **dataset_def.extra_params)
            except Exception as e:
                print(f"    Failed to load: {e}")
                continue

            if df is None or len(df) == 0:
                print(f"    No data returned")
                continue

            df = _safe_rename(df)
            print(f"    {len(df):,} rows, {len(df.columns)} columns")

            if fresh:
                # Remove stale rows for this season before re-inserting
                try:
                    conn.execute(f"DELETE FROM {table} WHERE season = {season}")
                except Exception:
                    pass

            _write_df_to_table(conn, table, df)
            _record_loaded(conn, dataset_def.dataset_id, table, dataset_def.loader_fn, len(df), season)
            total_rows += len(df)
            print(f"    {len(df):,} rows inserted")

    return total_rows


# ── Aggregate tables (pbp-derived) ────────────────────────────────────────────

_SITUATION_EXPR = """
    CASE
        WHEN down = 4                             THEN '4th Down'
        WHEN down = 3 AND ydstogo >= 7            THEN '3rd & Long'
        WHEN down = 3 AND ydstogo <= 3            THEN '3rd & Short'
        WHEN yardline_100 <= 20                   THEN 'Red Zone'
        WHEN qtr = 4
             AND "time" IS NOT NULL
             AND regexp_matches("time", '^[0-9]+:[0-9]+')
             AND CAST(string_split("time", ':')[1] AS INT) < 2
                                                  THEN 'Two Minute Drill'
        ELSE 'Standard'
    END
""".strip()

_FORMATION_EXPR = """
    CASE
        WHEN shotgun = 1 AND no_huddle = 1 THEN 'SHOTGUN NO HUDDLE'
        WHEN shotgun = 1                   THEN 'SHOTGUN'
        WHEN no_huddle = 1                 THEN 'NO HUDDLE'
        ELSE 'UNDER CENTER'
    END
""".strip()


def _create_aggregate_tables(conn: duckdb.DuckDBPyConnection):
    print("\n  Creating aggregate tables…")

    for table in ["team_offense_stats", "team_defense_stats",
                  "situational_stats", "formation_effectiveness"]:
        conn.execute(f"DROP TABLE IF EXISTS {table}")

    conn.execute("""
        CREATE TABLE team_offense_stats AS
        SELECT
            posteam AS team, season AS season_year,
            COUNT(*) AS total_plays,
            SUM(COALESCE(yards_gained,0)) AS total_yards,
            ROUND(AVG(yards_gained), 2) AS yards_per_play,
            SUM(CASE WHEN rush_attempt=1 THEN 1 ELSE 0 END) AS rush_plays,
            SUM(CASE WHEN pass_attempt=1 THEN 1 ELSE 0 END) AS pass_plays,
            SUM(CASE WHEN rush_attempt=1 THEN COALESCE(yards_gained,0) ELSE 0 END) AS rush_yards,
            SUM(CASE WHEN pass_attempt=1 THEN COALESCE(yards_gained,0) ELSE 0 END) AS pass_yards,
            ROUND(AVG(CASE WHEN rush_attempt=1 THEN yards_gained END), 2) AS yards_per_rush,
            ROUND(AVG(CASE WHEN pass_attempt=1 THEN yards_gained END), 2) AS yards_per_pass,
            SUM(CASE WHEN touchdown=1 THEN 1 ELSE 0 END) AS touchdowns,
            SUM(CASE WHEN interception=1 OR fumble_lost=1 THEN 1 ELSE 0 END) AS turnovers,
            ROUND(100.0*SUM(CASE WHEN down=3 AND yards_gained>=ydstogo THEN 1 ELSE 0 END)
                /NULLIF(SUM(CASE WHEN down=3 THEN 1 ELSE 0 END),0), 1) AS third_down_pct,
            ROUND(100.0*SUM(CASE WHEN yardline_100<=20 AND touchdown=1 THEN 1 ELSE 0 END)
                /NULLIF(SUM(CASE WHEN yardline_100<=20 THEN 1 ELSE 0 END),0), 1) AS red_zone_td_pct,
            SUM(CASE WHEN (pass_attempt=1 AND yards_gained>=20)
                       OR (rush_attempt=1  AND yards_gained>=10) THEN 1 ELSE 0 END) AS explosive_plays,
            ROUND(AVG(epa), 3) AS avg_epa,
            ROUND(AVG(CASE WHEN pass_attempt=1 THEN epa END), 3) AS pass_epa,
            ROUND(AVG(CASE WHEN rush_attempt=1  THEN epa END), 3) AS rush_epa
        FROM plays
        WHERE posteam IS NOT NULL
          AND play_type IN ('pass','run','field_goal','extra_point',
                            'punt','kickoff','qb_spike','qb_kneel','no_play')
        GROUP BY posteam, season
    """)
    print("    team_offense_stats ✓")

    conn.execute("""
        CREATE TABLE team_defense_stats AS
        SELECT
            defteam AS team, season AS season_year,
            COUNT(*) AS plays_against,
            SUM(COALESCE(yards_gained,0)) AS yards_allowed,
            ROUND(AVG(yards_gained), 2) AS yards_per_play_allowed,
            SUM(CASE WHEN sack=1         THEN 1 ELSE 0 END) AS sacks,
            SUM(CASE WHEN interception=1 THEN 1 ELSE 0 END) AS interceptions,
            SUM(CASE WHEN interception=1 OR fumble_lost=1 THEN 1 ELSE 0 END) AS turnovers_forced,
            ROUND(100.0*SUM(CASE WHEN down=3 AND yards_gained<ydstogo THEN 1 ELSE 0 END)
                /NULLIF(SUM(CASE WHEN down=3 THEN 1 ELSE 0 END),0), 1) AS third_down_stop_pct,
            ROUND(AVG(epa), 3) AS avg_epa_allowed
        FROM plays
        WHERE defteam IS NOT NULL
          AND play_type IN ('pass','run','field_goal','extra_point',
                            'punt','kickoff','qb_spike','qb_kneel','no_play')
        GROUP BY defteam, season
    """)
    print("    team_defense_stats ✓")

    conn.execute(f"""
        CREATE TABLE situational_stats AS
        WITH base AS (
            SELECT *, {_SITUATION_EXPR} AS situation_label
            FROM plays WHERE posteam IS NOT NULL AND down IS NOT NULL
        )
        SELECT
            posteam AS team, season AS season_year,
            situation_label AS situation,
            COUNT(*) AS plays,
            ROUND(AVG(yards_gained), 2) AS avg_yards,
            SUM(CASE WHEN touchdown=1 THEN 1 ELSE 0 END) AS touchdowns,
            ROUND(100.0*SUM(CASE WHEN yards_gained>=ydstogo THEN 1 ELSE 0 END)
                /NULLIF(COUNT(*),0), 1) AS conversion_pct,
            ROUND(AVG(epa), 3) AS avg_epa
        FROM base
        GROUP BY posteam, season, situation_label
    """)
    print("    situational_stats ✓")

    conn.execute(f"""
        CREATE TABLE formation_effectiveness AS
        SELECT
            posteam AS team, season AS season_year,
            {_FORMATION_EXPR} AS formation,
            play_type,
            COUNT(*) AS plays,
            ROUND(AVG(yards_gained), 2) AS avg_yards,
            SUM(CASE WHEN touchdown=1 THEN 1 ELSE 0 END) AS touchdowns,
            SUM(CASE WHEN interception=1 OR fumble_lost=1 THEN 1 ELSE 0 END) AS turnovers,
            ROUND(AVG(epa), 3) AS avg_epa
        FROM plays
        WHERE posteam IS NOT NULL AND play_type IN ('pass','run') AND shotgun IS NOT NULL
        GROUP BY posteam, season, {_FORMATION_EXPR}, play_type
        HAVING COUNT(*) >= 5
    """)
    print("    formation_effectiveness ✓")

    print("  Aggregate tables done")


def _table_exists(conn: duckdb.DuckDBPyConnection, name: str) -> bool:
    """Return True if a base table named `name` exists in the main schema."""
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_name = ?",
        [name],
    ).fetchone()
    return (row[0] or 0) > 0


# Each fantasy derived table: (table_name, [required source tables], CREATE-AS SELECT body).
# All are built at ingest time so the MCP tools never compute at query time.
_FANTASY_TABLES: list[tuple[str, list[str], str]] = [
    # 1. TD luck — actual vs expected touchdowns per player-season (most "unlucky" first).
    ("player_td_luck", ["ff_opportunity"], """
        SELECT
            player_id,
            ANY_VALUE(full_name)            AS full_name,
            ANY_VALUE(position)             AS position,
            ANY_VALUE(posteam)              AS team,
            CAST(season AS INTEGER)         AS season,
            SUM(rec_touchdown)              AS rec_td,
            SUM(rec_touchdown_exp)          AS rec_td_exp,
            SUM(rec_touchdown_diff)         AS rec_td_luck,
            SUM(rush_touchdown)             AS rush_td,
            SUM(rush_touchdown_exp)         AS rush_td_exp,
            SUM(rush_touchdown_diff)        AS rush_td_luck,
            ROUND(SUM(rec_touchdown_diff) + SUM(rush_touchdown_diff), 3) AS total_td_luck_score
        FROM ff_opportunity
        WHERE player_id IS NOT NULL
        GROUP BY player_id, CAST(season AS INTEGER)
    """),

    # 2. Rolling 3-week role trend — snap / target / carry / air-yards share with
    #    a trailing 3-week average and the current-week delta vs that average.
    ("player_role_trend", ["ff_opportunity", "snap_counts"], """
        WITH base AS (
            SELECT
                o.player_id,
                o.full_name,
                o.position,
                o.posteam                                   AS team,
                CAST(o.season AS INTEGER)                   AS season,
                CAST(o.week AS INTEGER)                      AS week,
                ROUND(100.0 * s.offense_pct, 2)             AS snap_pct,
                ROUND(100.0 * o.rec_attempt  / NULLIF(o.rec_attempt_team, 0), 2)   AS target_share_pct,
                ROUND(100.0 * o.rush_attempt / NULLIF(o.rush_attempt_team, 0), 2)  AS carry_share_pct,
                ROUND(100.0 * o.rec_air_yards / NULLIF(o.rec_air_yards_team, 0), 2) AS air_yards_share_pct
            FROM ff_opportunity o
            LEFT JOIN snap_counts s
              ON s.player  = o.full_name
             AND s.team    = o.posteam
             AND s.season  = CAST(o.season AS INTEGER)
             AND s.week    = CAST(o.week AS INTEGER)
            WHERE o.player_id IS NOT NULL
        )
        SELECT
            player_id, full_name, position, team, season, week,
            snap_pct, target_share_pct, carry_share_pct, air_yards_share_pct,
            ROUND(AVG(snap_pct)            OVER w, 2) AS snap_pct_3wk,
            ROUND(AVG(target_share_pct)    OVER w, 2) AS target_share_pct_3wk,
            ROUND(AVG(carry_share_pct)     OVER w, 2) AS carry_share_pct_3wk,
            ROUND(AVG(air_yards_share_pct) OVER w, 2) AS air_yards_share_pct_3wk,
            ROUND(snap_pct            - AVG(snap_pct)            OVER w, 2) AS snap_pct_delta,
            ROUND(target_share_pct    - AVG(target_share_pct)    OVER w, 2) AS target_share_pct_delta,
            ROUND(carry_share_pct     - AVG(carry_share_pct)     OVER w, 2) AS carry_share_pct_delta,
            ROUND(air_yards_share_pct - AVG(air_yards_share_pct) OVER w, 2) AS air_yards_share_pct_delta
        FROM base
        WINDOW w AS (PARTITION BY player_id, season ORDER BY week ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
    """),

    # 3. Separation-adjusted opportunity — joins Next Gen Stats receiving (2016+) to
    #    fantasy opportunity to flag separation-creators who underproduced (regression up).
    ("player_separation_opportunity", ["ff_opportunity", "nextgen_stats_receiving"], """
        WITH ng AS (
            SELECT
                player_gsis_id,
                season,
                AVG(avg_separation)              AS avg_separation,
                AVG(avg_yac_above_expectation)   AS avg_yac_above_expectation,
                AVG(catch_percentage)            AS catch_percentage
            FROM nextgen_stats_receiving
            WHERE week > 0 AND player_gsis_id IS NOT NULL
            GROUP BY player_gsis_id, season
        ),
        opp AS (
            SELECT
                player_id,
                ANY_VALUE(full_name)   AS full_name,
                ANY_VALUE(position)    AS position,
                ANY_VALUE(posteam)     AS team,
                CAST(season AS INTEGER) AS season,
                COUNT(DISTINCT week)   AS games,
                SUM(total_fantasy_points_diff) AS fp_diff,
                ROUND(SUM(total_fantasy_points_diff) / NULLIF(COUNT(DISTINCT week), 0), 3) AS fp_diff_per_game,
                ROUND(SUM(rec_touchdown_diff), 3) AS td_luck,
                ROUND(SUM(receptions_diff), 3)    AS catch_luck,
                ROUND(100.0 * SUM(rec_attempt)  / NULLIF(SUM(rec_attempt_team), 0), 2)  AS target_share_pct,
                ROUND(100.0 * SUM(rec_air_yards) / NULLIF(SUM(rec_air_yards_team), 0), 2) AS air_yards_share_pct
            FROM ff_opportunity
            WHERE player_id IS NOT NULL
            GROUP BY player_id, CAST(season AS INTEGER)
        )
        SELECT
            opp.player_id, opp.full_name, opp.position, opp.team, opp.season,
            opp.games, opp.fp_diff, opp.fp_diff_per_game, opp.td_luck, opp.catch_luck,
            opp.target_share_pct, opp.air_yards_share_pct,
            ROUND(ng.avg_separation, 3)            AS avg_separation,
            ROUND(ng.avg_yac_above_expectation, 3) AS avg_yac_above_expectation,
            ROUND(ng.catch_percentage, 2)          AS catch_percentage,
            (ng.avg_separation > 2.5 AND opp.fp_diff_per_game < -1.5 AND opp.td_luck < -1.0) AS regression_candidate
        FROM opp
        JOIN ng ON ng.player_gsis_id = opp.player_id AND ng.season = opp.season
    """),

    # 4. Drop rate — catchable-target drop rate from FTN charting (2022+) joined to plays.
    ("player_drop_rate", ["ftn_charting", "plays"], """
        SELECT
            p.receiver_player_id                AS player_id,
            ANY_VALUE(p.receiver_player_name)   AS player,
            p.posteam                           AS team,
            p.season                            AS season,
            COUNT(*)                            AS catchable_targets,
            SUM(CASE WHEN f.is_drop THEN 1 ELSE 0 END)              AS drops,
            ROUND(100.0 * SUM(CASE WHEN f.is_drop THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0), 2)       AS drop_rate_pct,
            SUM(CASE WHEN f.is_contested_ball THEN 1 ELSE 0 END)    AS contested_targets,
            SUM(CASE WHEN f.is_created_reception THEN 1 ELSE 0 END) AS created_receptions
        FROM ftn_charting f
        JOIN plays p
          ON f.nflverse_game_id = p.game_id
         AND f.nflverse_play_id = p.play_id
        WHERE f.is_catchable_ball = TRUE
          AND p.receiver_player_id IS NOT NULL
        GROUP BY p.receiver_player_id, p.posteam, p.season
    """),

    # 5. Contract value efficiency — fantasy points per $M of average per year (APY),
    #    using each player's active contract joined to seasonal fantasy production.
    ("player_contract_value", ["contracts", "ff_opportunity"], """
        WITH active AS (
            SELECT
                gsis_id,
                ANY_VALUE(position)   AS position,
                ANY_VALUE(team)       AS team,
                MAX(apy)              AS apy,
                MAX(apy_cap_pct)      AS cap_pct
            FROM contracts
            WHERE is_active = TRUE AND gsis_id IS NOT NULL
            GROUP BY gsis_id
        ),
        opp AS (
            SELECT
                player_id,
                ANY_VALUE(full_name)   AS full_name,
                ANY_VALUE(position)    AS position,
                ANY_VALUE(posteam)     AS team,
                CAST(season AS INTEGER) AS season,
                SUM(total_fantasy_points)     AS total_fp,
                SUM(total_fantasy_points_exp) AS total_fp_exp
            FROM ff_opportunity
            WHERE player_id IS NOT NULL
            GROUP BY player_id, CAST(season AS INTEGER)
        )
        SELECT
            opp.player_id,
            opp.full_name,
            COALESCE(opp.position, active.position) AS position,
            opp.team,
            opp.season,
            ROUND(active.apy, 4)       AS apy,
            ROUND(active.cap_pct, 4)   AS cap_pct,
            ROUND(opp.total_fp, 2)     AS total_fp,
            ROUND(opp.total_fp_exp, 2) AS total_fp_exp,
            ROUND(opp.total_fp / NULLIF(active.apy, 0), 2) AS fp_per_million
        FROM opp
        JOIN active ON active.gsis_id = opp.player_id
    """),

    # 6. Injury return curve — post-return snap-share recovery (as % of pre-injury
    #    baseline) at +1..+8 weeks, bucketed by normalized injury type and position.
    ("injury_return_curve", ["injuries", "snap_counts"], """
        WITH outs AS (
            SELECT
                gsis_id,
                full_name,
                position,
                CAST(season AS INTEGER) AS season,
                CAST(week AS INTEGER)   AS week,
                LOWER(TRIM(COALESCE(report_primary_injury, practice_primary_injury))) AS injury_raw
            FROM injuries
            WHERE report_status = 'Out'
              AND gsis_id IS NOT NULL
              AND COALESCE(report_primary_injury, practice_primary_injury) IS NOT NULL
        ),
        typed AS (
            SELECT *,
                CASE
                    WHEN injury_raw LIKE '%hamstring%'  THEN 'hamstring'
                    WHEN injury_raw LIKE '%knee%'       THEN 'knee'
                    WHEN injury_raw LIKE '%ankle%'      THEN 'ankle'
                    WHEN injury_raw LIKE '%shoulder%'   THEN 'shoulder'
                    WHEN injury_raw LIKE '%concussion%' THEN 'concussion'
                    WHEN injury_raw LIKE '%groin%'      THEN 'groin'
                    WHEN injury_raw LIKE '%foot%'       THEN 'foot'
                    WHEN injury_raw LIKE '%calf%'       THEN 'calf'
                    WHEN injury_raw LIKE '%hip%'        THEN 'hip'
                    WHEN injury_raw LIKE '%back%'       THEN 'back'
                    WHEN injury_raw LIKE '%quad%'       THEN 'quadriceps'
                    WHEN injury_raw LIKE '%achilles%'   THEN 'achilles'
                    WHEN injury_raw LIKE '%wrist%'      THEN 'wrist'
                    WHEN injury_raw LIKE '%hand%'       THEN 'hand'
                    WHEN injury_raw LIKE '%elbow%'      THEN 'elbow'
                    WHEN injury_raw LIKE '%toe%'        THEN 'toe'
                    WHEN injury_raw LIKE '%thigh%'      THEN 'thigh'
                    WHEN injury_raw LIKE '%neck%'       THEN 'neck'
                    WHEN injury_raw LIKE '%rib%'        THEN 'ribs'
                    WHEN injury_raw LIKE '%pectoral%'   THEN 'pectoral'
                    ELSE 'other'
                END AS injury_type
            FROM outs
        ),
        islands AS (
            SELECT *,
                week - ROW_NUMBER() OVER (PARTITION BY gsis_id, season ORDER BY week) AS island
            FROM typed
        ),
        spells AS (
            SELECT
                gsis_id,
                ANY_VALUE(full_name) AS full_name,
                ANY_VALUE(position)  AS position,
                season,
                MODE(injury_type)    AS injury_type,
                MIN(week)            AS first_out_week,
                MAX(week)            AS last_out_week
            FROM islands
            GROUP BY gsis_id, season, island
        ),
        baseline AS (
            SELECT
                sp.gsis_id, sp.full_name, sp.position, sp.season,
                sp.injury_type, sp.first_out_week, sp.last_out_week,
                AVG(sn.offense_pct) AS baseline_pct
            FROM spells sp
            JOIN snap_counts sn
              ON sn.player = sp.full_name
             AND sn.season = sp.season
             AND sn.week   < sp.first_out_week
            GROUP BY sp.gsis_id, sp.full_name, sp.position, sp.season,
                     sp.injury_type, sp.first_out_week, sp.last_out_week
        ),
        curve AS (
            SELECT
                b.injury_type,
                b.position,
                (sn.week - b.last_out_week) AS week_post_return,
                100.0 * sn.offense_pct / NULLIF(b.baseline_pct, 0) AS recovery_pct
            FROM baseline b
            JOIN snap_counts sn
              ON sn.player = b.full_name
             AND sn.season = b.season
             AND sn.week   > b.last_out_week
             AND sn.week  <= b.last_out_week + 8
            WHERE b.baseline_pct > 0
        )
        SELECT
            injury_type,
            position,
            week_post_return,
            ROUND(AVG(recovery_pct), 1)    AS avg_snap_pct_recovery,
            ROUND(MEDIAN(recovery_pct), 1) AS median_snap_pct_recovery,
            COUNT(*)                       AS sample_size
        FROM curve
        GROUP BY injury_type, position, week_post_return
    """),
]


def _create_fantasy_tables(conn: duckdb.DuckDBPyConnection):
    """Build the fantasy-analytics derived tables from already-ingested source data.

    Each table is fully recomputed (DROP + CREATE TABLE AS) and recorded in
    _ingest_metadata as a 'derived' dataset so it surfaces in nfl_catalog / nfl_status.
    Tables whose source data isn't present yet are skipped gracefully.
    """
    print("\n  Creating fantasy derived tables…")
    for table_name, sources, body in _FANTASY_TABLES:
        missing = [s for s in sources if not _table_exists(conn, s)]
        if missing:
            print(f"    {table_name} — skipped (missing sources: {', '.join(missing)})")
            continue
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.execute(f"CREATE TABLE {table_name} AS {body}")
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        _record_loaded(conn, table_name, table_name, "derived", row_count, season=None)
        print(f"    {table_name} ✓ ({row_count:,} rows)")
    print("  Fantasy derived tables done")


def _create_indexes(conn: duckdb.DuckDBPyConnection):
    print("\n  Creating indexes…")
    for idx_sql in [
        'CREATE INDEX IF NOT EXISTS idx_plays_season      ON plays(season)',
        'CREATE INDEX IF NOT EXISTS idx_plays_posteam     ON plays(posteam)',
        'CREATE INDEX IF NOT EXISTS idx_plays_defteam     ON plays(defteam)',
        'CREATE INDEX IF NOT EXISTS idx_plays_team_season ON plays(posteam, season)',
        'CREATE INDEX IF NOT EXISTS idx_plays_game_id     ON plays(game_id)',
        'CREATE INDEX IF NOT EXISTS idx_plays_play_type   ON plays(play_type)',
    ]:
        try:
            conn.execute(idx_sql)
        except Exception as e:
            print(f"    Warning: {e}")
    print("  Indexes done")


# ── Public entry points ────────────────────────────────────────────────────────

def run_ingest_datasets(
    dataset_ids: list[str],
    start: int | None = None,
    end: int | None = None,
    fresh: bool = False,
    skip_views: bool = False,
    db_path: str | None = None,
) -> None:
    """
    Ingest one or more datasets into DuckDB.

    dataset_ids: list of keys from REGISTRY (e.g. ["pbp", "schedules"])
    start/end:   season range; if both are None, loads all available seasons.
    """
    import nflreadpy
    from .config import get_duckdb_path
    from .registry import REGISTRY

    bulk_mode = start is None and end is None
    if not bulk_mode:
        start = start or 2013
        end = end or 2025
        if start > end:
            raise ValueError("start must be less than or equal to end")

    path = db_path or str(get_duckdb_path())
    seasons: list[int] | None = None if bulk_mode else [s for s in ALL_SEASONS if start <= s <= end]

    print("=" * 60)
    print("NFL MCP — Multi-Dataset Ingest")
    print("=" * 60)
    print(f"Datasets : {', '.join(dataset_ids)}")
    print(f"Seasons  : {'all available' if bulk_mode else f'{seasons[0]}–{seasons[-1]}'}")
    print(f"Database : {path}")

    from pathlib import Path
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(path)
    _apply_duckdb_pragmas(conn, path)

    _ensure_metadata_table(conn)

    pbp_ingested = 0
    has_pbp = "pbp" in dataset_ids

    # ── PBP (special path — enhanced_description, null filtering) ──────────
    if has_pbp:
        print("\n── Play-by-play ──")

        pbp_seasons = ALL_SEASONS if bulk_mode else seasons
        schema_season = pbp_seasons[-1]
        print(f"  Discovering schema from {schema_season}…")
        sample_df = nflreadpy.load_pbp([schema_season])
        print(f"  {len(sample_df.columns)} columns · {len(sample_df):,} rows")

        _create_plays_table(conn, sample_df, fresh=fresh)

        loaded_pbp = {
            row[0] for row in conn.execute(
                "SELECT DISTINCT season FROM plays WHERE season IS NOT NULL"
            ).fetchall()
        } if not fresh else set()

        to_ingest = [s for s in pbp_seasons if s not in loaded_pbp]
        skipped   = [s for s in pbp_seasons if s in loaded_pbp]

        if skipped:
            print(f"  Already loaded, skipping: {skipped}")
        if not to_ingest:
            print("  All PBP seasons already loaded.")
        else:
            print(f"  Will ingest: {to_ingest}")
            for season in to_ingest:
                rows = _ingest_pbp_season(conn, season)
                pbp_ingested += rows
                if rows:
                    _record_loaded(conn, "pbp", "plays", "load_pbp", rows, season)

        _create_indexes(conn)

        if not skip_views:
            _create_aggregate_tables(conn)

    # ── All other datasets ─────────────────────────────────────────────────
    other_ids = [d for d in dataset_ids if d != "pbp"]
    if other_ids:
        print("\n── Additional datasets ──")
        for dataset_id in other_ids:
            defn = REGISTRY.get(dataset_id)
            if defn is None:
                print(f"\n  WARNING: unknown dataset '{dataset_id}', skipping")
                continue
            _ingest_generic_dataset(conn, defn, seasons, fresh=fresh)

    # ── Fantasy derived tables (built from already-ingested sources) ────────
    if not skip_views:
        _create_fantasy_tables(conn)

    conn.close()
    print("\n" + "=" * 60)
    print("Ingestion complete!")
    if has_pbp and pbp_ingested:
        print(f"  PBP plays ingested this run: {pbp_ingested:,}")
    season_label = "all available" if bulk_mode else f"{seasons[0]}–{seasons[-1]}"
    print(f"  Seasons: {season_label}")
    print(f"  DB: {path}")
    print("=" * 60)


def run_ingest(
    start: int = 2013,
    end: int = 2025,
    fresh: bool = False,
    skip_views: bool = False,
    db_path: str | None = None,
) -> None:
    """Backward-compatible entry point — ingests PBP only."""
    run_ingest_datasets(
        dataset_ids=["pbp"],
        start=start,
        end=end,
        fresh=fresh,
        skip_views=skip_views,
        db_path=db_path,
    )

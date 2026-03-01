"""
NFL play-by-play ingestion into DuckDB.

Uses native Polars → DuckDB path for fast bulk loading.
Called by: nfl-mcp init / nfl-mcp ingest (when backend=duckdb)
"""

import duckdb
import polars as pl
from tqdm import tqdm

ALL_SEASONS = list(range(2013, 2026))


# ── Helpers ────────────────────────────────────────────────────────────────────

def _safe_col(name: str) -> str:
    return name.replace(".", "_").replace(" ", "_").replace("-", "_")


def _str(val) -> str:
    if val is None:
        return ""
    s = str(val)
    return "" if s in ("None", "nan", "NaN", "") else s


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


def _duckdb_type_for_polars(dtype: pl.DataType) -> str:
    if dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
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


def _reconcile_plays_schema(conn: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> None:
    """Add any new incoming columns to plays before insert-by-name."""
    existing_rows = conn.execute("PRAGMA table_info('plays')").fetchall()
    existing_cols = {row[1] for row in existing_rows}

    added_cols = []
    for col_name, dtype in df.schema.items():
        if col_name in existing_cols:
            continue
        duck_type = _duckdb_type_for_polars(dtype)
        conn.execute(f'ALTER TABLE plays ADD COLUMN "{col_name}" {duck_type}')
        added_cols.append(f"{col_name} ({duck_type})")

    if added_cols:
        print(f"    Added new columns: {', '.join(added_cols)}")


# ── Table creation ─────────────────────────────────────────────────────────────

def _get_loaded_seasons(conn: duckdb.DuckDBPyConnection) -> set[int]:
    try:
        result = conn.execute(
            "SELECT DISTINCT season FROM plays WHERE season IS NOT NULL"
        ).fetchall()
        return {row[0] for row in result}
    except duckdb.CatalogException:
        return set()


def _create_table(conn: duckdb.DuckDBPyConnection, sample_df: pl.DataFrame, fresh: bool = False):
    if fresh:
        conn.execute("DROP TABLE IF EXISTS plays")
        print("  Dropped existing plays table")

    # Rename columns to safe names in the DataFrame
    safe_cols = {c: _safe_col(c) for c in sample_df.columns}
    renamed = sample_df.head(0).rename(safe_cols)

    # Create table from schema of the renamed DataFrame + extra columns
    try:
        conn.execute("SELECT 1 FROM plays LIMIT 0")
        print("  plays table already exists")
    except duckdb.CatalogException:
        # Register the empty frame so DuckDB can see it, then CREATE TABLE from it
        conn.register("_schema_df", renamed.to_arrow())
        conn.execute(
            "CREATE TABLE plays AS "
            "SELECT *, '' AS enhanced_description FROM _schema_df"
        )
        conn.unregister("_schema_df")
        print(f"  plays table created — {len(sample_df.columns) + 1} columns")


# ── Season ingestion ───────────────────────────────────────────────────────────

def _ingest_season(conn: duckdb.DuckDBPyConnection, season: int) -> int:
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

    # Rename columns to safe names
    safe_cols = {c: _safe_col(c) for c in df.columns}
    df = df.rename(safe_cols)

    # Build enhanced descriptions without materializing full row dicts.
    total = len(df)
    all_descriptions = [
        _build_enhanced_description(row)
        for row in tqdm(df.iter_rows(named=True), total=total, desc=f"    {season} descriptions")
    ]

    df = df.with_columns(pl.Series("enhanced_description", all_descriptions))
    _reconcile_plays_schema(conn, df)

    # Insert into DuckDB via Arrow registration
    conn.register("_ingest_df", df.to_arrow())
    conn.execute("INSERT INTO plays BY NAME SELECT * FROM _ingest_df")
    conn.unregister("_ingest_df")
    print(f"    {total:,} rows inserted")
    return total


# ── Aggregate tables (replacement for PostgreSQL materialized views) ───────────

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


# ── Indexes ────────────────────────────────────────────────────────────────────

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


# ── Public entry point ─────────────────────────────────────────────────────────

def run_ingest(
    start: int = 2013,
    end: int = 2025,
    fresh: bool = False,
    skip_views: bool = False,
    db_path: str | None = None,
):
    import nflreadpy
    from .config import get_duckdb_path

    if start > end:
        raise ValueError("start must be less than or equal to end")

    path = db_path or str(get_duckdb_path())
    seasons = [s for s in ALL_SEASONS if start <= s <= end]

    print("=" * 60)
    print("NFL nflreadpy → DuckDB")
    print("=" * 60)
    print(f"Seasons requested: {seasons[0]}–{seasons[-1]}")
    print(f"Database: {path}")

    from pathlib import Path
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(path)

    schema_season = seasons[-1]
    print(f"\nDiscovering schema from {schema_season} season…")
    sample_df = nflreadpy.load_pbp([schema_season])
    print(f"  {len(sample_df.columns)} columns · {len(sample_df):,} rows")

    _create_table(conn, sample_df, fresh=fresh)

    loaded = _get_loaded_seasons(conn)
    to_ingest = [s for s in seasons if s not in loaded]
    skipped = [s for s in seasons if s in loaded]

    if skipped:
        print(f"\nAlready loaded, skipping: {skipped}")
    if not to_ingest:
        print("All requested seasons already loaded.")
        conn.close()
        return

    print(f"Will ingest: {to_ingest}")

    total = 0
    for season in to_ingest:
        total += _ingest_season(conn, season)

    print(f"\nPlays ingested this run: {total:,}")

    _create_indexes(conn)

    if not skip_views:
        _create_aggregate_tables(conn)

    conn.close()
    print("\n" + "=" * 60)
    print("Ingestion complete!")
    print(f"  DB:      {path}")
    print(f"  Plays:   {total:,}")
    print(f"  Seasons: {seasons[0]}–{seasons[-1]}")
    print("=" * 60)

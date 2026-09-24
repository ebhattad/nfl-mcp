"""
Deciding whether nflverse has published anything new.

nflverse rewrites a season's file in place at a stable URL as games finish, so
the cheap way to detect new data is to ask GitHub when the release asset was
last written rather than re-downloading a ~20 MB parquet on a timer. One
unauthenticated request covers every season at once, and GitHub allows 60/hour
unauthenticated — a 30-minute poll spends two of them.

The last-seen timestamp is persisted alongside the ingest record in
`_ingest_metadata.source_updated_at`, so a restart doesn't trigger a
re-download of data already loaded.
"""

import json
import urllib.error
import urllib.request
from pathlib import Path

RELEASE_API = "https://api.github.com/repos/nflverse/nflverse-data/releases/tags/{tag}"
PBP_TAG = "pbp"
PBP_ASSET = "play_by_play_{season}.parquet"
USER_AGENT = "nfl-mcp (+https://github.com/ebhattad/nfl-mcp)"


def pbp_updated_at(season: int, timeout: float = 15.0) -> str | None:
    """When nflverse last republished `season`'s play-by-play, ISO-8601.

    Returns None when the question can't be answered — the season hasn't
    kicked off so the asset doesn't exist yet, or GitHub is unreachable or
    rate-limiting us. Callers treat None as "don't know" and skip the update
    rather than re-ingesting blindly; an outage shouldn't cost a full reload
    on every poll.
    """
    url = RELEASE_API.format(tag=PBP_TAG)
    request = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "application/vnd.github+json",
    })
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.load(response)
    except (urllib.error.URLError, TimeoutError, ValueError, OSError):
        return None

    wanted = PBP_ASSET.format(season=season)
    for asset in payload.get("assets", []):
        if asset.get("name") == wanted:
            return asset.get("updated_at")
    return None


# ── Persistence ────────────────────────────────────────────────────────────────

def read_pbp_stamp(db_path: str, season: int) -> str | None:
    """Last `source_updated_at` recorded for this season's play-by-play."""
    import duckdb

    if not Path(db_path).exists():
        return None
    try:
        conn = duckdb.connect(db_path, read_only=True)
    except Exception:  # noqa: BLE001 — unopenable means unknown, not fatal
        return None
    try:
        row = conn.execute(
            "SELECT source_updated_at FROM _ingest_metadata "
            "WHERE dataset_id = 'pbp' AND season = ?",
            [season],
        ).fetchone()
        return row[0] if row else None
    except Exception:  # noqa: BLE001 — no metadata table, or a DB predating the column
        return None
    finally:
        conn.close()


def write_pbp_stamp(db_path: str, season: int, stamp: str) -> None:
    """Record the asset timestamp this season's play-by-play was ingested from.

    A no-op when the ingest wrote no metadata row (i.e. it loaded nothing), so
    a failed load can't mark the season as up to date.
    """
    import duckdb

    from .ingest import _ensure_metadata_table

    conn = duckdb.connect(db_path)
    try:
        _ensure_metadata_table(conn)
        conn.execute(
            "UPDATE _ingest_metadata SET source_updated_at = ? "
            "WHERE dataset_id = 'pbp' AND season = ?",
            [stamp, season],
        )
    finally:
        conn.close()


def is_stale(db_path: str, season: int) -> tuple[bool, str | None]:
    """Whether `season` is worth re-ingesting, and the stamp to record if so.

    Returns (False, None) both when nflverse has published nothing new and when
    the probe couldn't reach GitHub — see `pbp_updated_at` for why unknown maps
    to "leave it alone".
    """
    remote = pbp_updated_at(season)
    if remote is None:
        return False, None
    return remote != read_pbp_stamp(db_path, season), remote

"""
In-season updates — refreshing the current season as nflverse publishes games.

Two entry points, differing only in whether a server is reading the database:

  `run_update`      writes straight to the database. For cron/`--watch`, where
                    nothing else holds the file.
  `run_update_swap` ingests into a copy and atomically renames it over the
                    live file. For `serve --auto-update`.

The copy exists because DuckDB permits exactly one writer and no concurrent
readers: a process cannot take the write lock while the server holds the file
open, and the server cannot open it while a writer holds it. Since
`get_db_connection()` opens a fresh connection per request and re-resolves the
path each time, `os.replace` lets new requests pick up the new file while
in-flight ones finish reading the old inode. No downtime, and the serve path
keeps its read-only connections.

Both paths probe nflverse before doing any work, so a poll that finds nothing
new costs one small HTTP request — not a download, and not a file copy.
"""

import os
import shutil
import time
from pathlib import Path

MIN_INTERVAL_SECONDS = 60

_UNITS = {"s": 1, "m": 60, "h": 3600}


def parse_interval(value) -> int:
    """Parse `900`, `900s`, `30m` or `2h` into seconds.

    Floored at a minute: the probe hits GitHub's unauthenticated API, which
    allows 60 requests an hour.
    """
    text = str(value).strip().lower()
    unit = _UNITS.get(text[-1:] or " ")
    try:
        amount = int(text[:-1] if unit else text)
    except ValueError:
        raise ValueError(
            f"invalid interval {value!r}; use forms like 900s, 30m, 2h"
        ) from None
    if amount <= 0:
        raise ValueError(f"invalid interval {value!r}; must be positive")
    seconds = amount * (unit or 1)
    if seconds < MIN_INTERVAL_SECONDS:
        raise ValueError(
            f"interval {value!r} is below the {MIN_INTERVAL_SECONDS}s minimum"
        )
    return seconds


def _resolve(season, dataset_ids, db_path):
    """Fill in the current season, the default dataset list and the DB path."""
    from .config import get_duckdb_path
    from .registry import DEFAULT_DATASETS
    from .seasons import current_season

    return (
        current_season() if season is None else season,
        list(DEFAULT_DATASETS) if not dataset_ids else list(dataset_ids),
        str(get_duckdb_path()) if db_path is None else db_path,
    )


def run_update(
    season: int | None = None,
    dataset_ids: list[str] | None = None,
    force: bool = False,
    skip_views: bool = False,
    db_path: str | None = None,
) -> bool:
    """Refresh one season in place. Returns True if anything was re-ingested."""
    from .freshness import is_stale, write_pbp_stamp
    from .ingest import run_ingest_datasets

    season, dataset_ids, path = _resolve(season, dataset_ids, db_path)

    stale, stamp = is_stale(path, season)
    if not (stale or force):
        print(f"  {season}: nflverse has published nothing new — skipping")
        return False

    run_ingest_datasets(
        dataset_ids=dataset_ids,
        start=season,
        end=season,
        refresh=True,
        skip_views=skip_views,
        db_path=path,
    )
    if stamp:
        write_pbp_stamp(path, season, stamp)
    return True


def run_update_swap(
    season: int | None = None,
    dataset_ids: list[str] | None = None,
    force: bool = False,
    skip_views: bool = False,
    db_path: str | None = None,
) -> bool:
    """Refresh one season without disturbing a server reading the database.

    Ingests into a sibling `.updating` file and renames it over the live one.
    The copy is only made once the probe says there is new data, so an idle
    poll never touches the disk.
    """
    from .freshness import is_stale, write_pbp_stamp
    from .ingest import run_ingest_datasets

    season, dataset_ids, path = _resolve(season, dataset_ids, db_path)
    live = Path(path)

    stale, stamp = is_stale(path, season)
    if not (stale or force):
        print(f"  {season}: nflverse has published nothing new — skipping")
        return False

    staging = live.with_name(live.name + ".updating")
    _discard(staging)
    if live.exists():
        shutil.copy2(live, staging)

    try:
        run_ingest_datasets(
            dataset_ids=dataset_ids,
            start=season,
            end=season,
            refresh=True,
            skip_views=skip_views,
            db_path=str(staging),
        )
        if stamp:
            write_pbp_stamp(str(staging), season, stamp)
        os.replace(staging, live)
    except BaseException:
        # Leave the live database exactly as it was.
        _discard(staging)
        raise

    print(f"  {season}: swapped in refreshed database")
    return True


def _discard(staging: Path) -> None:
    """Remove a staging database and any write-ahead log beside it."""
    for path in (staging, staging.with_name(staging.name + ".wal")):
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def watch(
    interval_seconds: int,
    swap: bool = False,
    stop=None,
    **kwargs,
) -> None:
    """Poll nflverse forever, refreshing whenever new data appears.

    A failing cycle is logged and retried on the next tick rather than killing
    the loop — a transient nflverse or network fault shouldn't end the watch.
    `stop` is an optional `threading.Event` used to shut the loop down.
    """
    update = run_update_swap if swap else run_update
    while stop is None or not stop.is_set():
        try:
            update(**kwargs)
        except Exception as exc:                       # noqa: BLE001 — keep polling
            print(f"  update failed, retrying next cycle: {exc}")
        if stop is None:
            time.sleep(interval_seconds)
        elif stop.wait(interval_seconds):
            break


def start_background_updater(interval_seconds: int, **kwargs):
    """Run `watch` in a daemon thread, swapping so serving keeps working.

    Returns (thread, stop_event); the event is only needed by tests, since the
    thread is a daemon and dies with the process.
    """
    import threading

    stop = threading.Event()
    thread = threading.Thread(
        target=watch,
        args=(interval_seconds,),
        kwargs={"swap": True, "stop": stop, **kwargs},
        name="nfl-mcp-updater",
        daemon=True,
    )
    thread.start()
    return thread, stop

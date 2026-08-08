"""
Season boundaries for ingestion.

nflverse only publishes play-by-play for a season once that season has begun,
rolling over on the Thursday following Labor Day — the rule
``nflreadpy.get_current_season`` implements. Deriving the upper bound from that
function instead of a literal keeps this package from needing a code change
every September, and guarantees it can never disagree with the loader about
which seasons exist.
"""

FIRST_SEASON = 2013


def current_season() -> int:
    """Latest season nflverse serves play-by-play for."""
    from nflreadpy import get_current_season

    return get_current_season()


def all_seasons() -> list[int]:
    """Every season this package ingests, oldest first."""
    return list(range(FIRST_SEASON, current_season() + 1))

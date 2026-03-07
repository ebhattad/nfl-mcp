"""Tests for the structured MCP tools."""

import pytest
from nfl_mcp.tools import (
    nfl_schema, nfl_status, nfl_search_plays, nfl_team_stats,
    nfl_player_stats, nfl_compare, nfl_query,
    nfl_catalog, nfl_roster, nfl_injuries, nfl_schedule, nfl_snap_counts,
)


@pytest.fixture
def require_db():
    """Skip integration tests if no data is loaded."""
    result = nfl_query("SELECT COUNT(*) AS n FROM plays")
    if "error" in result:
        pytest.skip("No DuckDB database loaded")
    if result["rows"][0]["n"] == 0:
        pytest.skip("plays table is empty")


class TestNflSchema:
    def test_returns_summary_by_default(self):
        result = nfl_schema()
        assert "schema" in result
        assert "hint" in result
        assert len(result["schema"]) > 200

    def test_default_includes_available_tables_key(self):
        result = nfl_schema()
        assert "available_tables" in result
        assert isinstance(result["available_tables"], list)

    def test_summary_contains_key_columns(self):
        schema = nfl_schema()["schema"]
        for col in ["epa", "posteam", "defteam", "play_type", "passer_player_name"]:
            assert col in schema

    def test_category_returns_detail(self):
        result = nfl_schema(category="epa")
        assert "schema" in result
        assert "qb_epa" in result["schema"]

    def test_category_all_returns_full(self):
        result = nfl_schema(category="all")
        assert len(result["schema"]) > 5000

    def test_unknown_category_returns_error(self):
        result = nfl_schema(category="nonexistent")
        assert "error" in result
        assert "available" in result


@pytest.mark.integration
@pytest.mark.usefixtures("require_db")
class TestNflSchemaIntegration:
    def test_default_lists_known_tables(self):
        result = nfl_schema()
        tables = result["available_tables"]
        assert len(tables) > 0
        for expected in ("plays", "rosters", "injuries", "schedules", "snap_counts"):
            assert expected in tables

    def test_default_excludes_internal_tables(self):
        result = nfl_schema()
        for t in result["available_tables"]:
            assert not t.startswith("_"), f"Internal table '{t}' should be excluded"

    def test_table_lookup_returns_columns(self):
        result = nfl_schema(table="rosters")
        assert "error" not in result
        assert result["table"] == "rosters"
        col_names = [c["column_name"] for c in result["columns"]]
        assert "full_name" in col_names
        assert "position" in col_names
        assert "team" in col_names
        assert "season" in col_names

    def test_table_lookup_injuries(self):
        result = nfl_schema(table="injuries")
        assert "error" not in result
        col_names = [c["column_name"] for c in result["columns"]]
        assert "report_status" in col_names

    def test_table_lookup_schedules(self):
        result = nfl_schema(table="schedules")
        assert "error" not in result
        col_names = [c["column_name"] for c in result["columns"]]
        for col in ("home_team", "away_team", "home_score", "away_score"):
            assert col in col_names

    def test_unknown_table_returns_error_with_list(self):
        result = nfl_schema(table="definitely_not_a_table")
        assert "error" in result
        assert "available_tables" in result
        assert len(result["available_tables"]) > 0


@pytest.mark.integration
@pytest.mark.usefixtures("require_db")
class TestNflSearchPlays:
    def test_search_by_team(self):
        result = nfl_search_plays(team="KC", season=2024, max_rows=5)
        assert "error" not in result
        assert result["row_count"] > 0
        assert all(r["posteam"] == "KC" for r in result["rows"])

    def test_search_by_player(self):
        result = nfl_search_plays(player="Mahomes", season=2024, max_rows=5)
        assert "error" not in result
        assert result["row_count"] > 0

    def test_search_touchdowns(self):
        result = nfl_search_plays(is_touchdown=True, season=2024, max_rows=5)
        assert "error" not in result
        assert all(r["touchdown"] == 1 for r in result["rows"])

    def test_search_turnovers(self):
        result = nfl_search_plays(is_turnover=True, season=2024, max_rows=5)
        assert "error" not in result
        assert result["row_count"] > 0

    def test_search_situation_red_zone(self):
        result = nfl_search_plays(situation="red_zone", team="KC", season=2024, max_rows=5)
        assert "error" not in result

    def test_search_min_yards(self):
        result = nfl_search_plays(min_yards=30, season=2024, max_rows=5)
        assert "error" not in result
        assert all(r["yards_gained"] >= 30 for r in result["rows"])

    def test_empty_search(self):
        result = nfl_search_plays(max_rows=10)
        assert "error" not in result
        assert result["row_count"] > 0


@pytest.mark.integration
@pytest.mark.usefixtures("require_db")
class TestNflTeamStats:
    def test_returns_offense_and_defense(self):
        result = nfl_team_stats(team="KC", season=2024)
        assert "error" not in result
        assert "offense" in result
        assert "defense" in result
        assert len(result["offense"]) == 1

    def test_offense_only(self):
        result = nfl_team_stats(team="BAL", season=2024, side="offense")
        assert "offense" in result
        assert "defense" not in result

    def test_defense_only(self):
        result = nfl_team_stats(team="BAL", season=2024, side="defense")
        assert "defense" in result
        assert "offense" not in result

    def test_includes_situational(self):
        result = nfl_team_stats(team="KC", season=2024)
        assert "situational" in result
        assert len(result["situational"]) > 0

    def test_invalid_team_returns_empty(self):
        result = nfl_team_stats(team="ZZZ", season=2024)
        assert "error" not in result
        assert len(result["offense"]) == 0


@pytest.mark.integration
@pytest.mark.usefixtures("require_db")
class TestNflPlayerStats:
    def test_passing_stats(self):
        result = nfl_player_stats(player_name="P.Mahomes", stat_type="passing")
        assert "error" not in result
        assert len(result["seasons"]) > 0
        season = result["seasons"][0]
        assert "attempts" in season
        assert "completions" in season
        assert "avg_epa" in season

    def test_rushing_stats(self):
        result = nfl_player_stats(player_name="D.Henry", stat_type="rushing", season=2024)
        assert "error" not in result
        assert len(result["seasons"]) > 0
        assert "carries" in result["seasons"][0]

    def test_receiving_stats(self):
        result = nfl_player_stats(player_name="J.Jefferson", stat_type="receiving")
        assert "error" not in result

    def test_unknown_player_returns_empty(self):
        result = nfl_player_stats(player_name="ZZZZNOTAPLAYER", stat_type="passing")
        assert "error" not in result
        assert len(result["seasons"]) == 0


@pytest.mark.integration
@pytest.mark.usefixtures("require_db")
class TestNflCompare:
    def test_compare_teams(self):
        result = nfl_compare(entity1="KC", entity2="BAL", compare_type="team", season=2024)
        assert "error" not in result
        assert "KC" in result
        assert "BAL" in result
        assert "offense" in result["KC"]
        assert "defense" in result["BAL"]

    def test_compare_players(self):
        result = nfl_compare(
            entity1="P.Mahomes", entity2="L.Jackson",
            compare_type="player", season=2024
        )
        assert "error" not in result
        assert "P.Mahomes" in result
        assert "L.Jackson" in result

@pytest.mark.integration
@pytest.mark.usefixtures("require_db")
class TestNflStatus:
    def test_returns_plays_key(self):
        result = nfl_status()
        assert "error" not in result
        assert "plays" in result

    def test_plays_has_expected_fields(self):
        plays = nfl_status()["plays"]
        assert "total_plays" in plays
        assert "season_range" in plays
        assert plays["total_plays"] > 0

    def test_returns_datasets_key(self):
        result = nfl_status()
        assert "datasets" in result

    def test_datasets_has_expected_fields(self):
        datasets = nfl_status()["datasets"]
        assert "total_loaded" in datasets
        assert "last_refreshed" in datasets
        assert "loaded" in datasets
        assert datasets["total_loaded"] > 0
        assert isinstance(datasets["loaded"], list)

    def test_datasets_loaded_entries_have_fields(self):
        loaded = nfl_status()["datasets"]["loaded"]
        assert len(loaded) > 0
        for entry in loaded:
            assert "dataset_id" in entry
            assert "table_name" in entry
            assert "total_rows" in entry


@pytest.mark.integration
@pytest.mark.usefixtures("require_db")
class TestNflCatalog:
    def test_returns_datasets_list(self):
        result = nfl_catalog()
        assert "error" not in result
        assert "datasets" in result
        assert len(result["datasets"]) > 0

    def test_includes_row_counts(self):
        result = nfl_catalog()
        for ds in result["datasets"]:
            assert "dataset_id" in ds
            assert "total_rows" in ds
            assert ds["total_rows"] > 0

    def test_total_datasets_matches_list(self):
        result = nfl_catalog()
        assert result["total_datasets"] == len(result["datasets"])


@pytest.mark.integration
@pytest.mark.usefixtures("require_db")
class TestNflRoster:
    def test_filter_by_team_and_season(self):
        result = nfl_roster(team="KC", season=2024)
        assert "error" not in result
        assert result["count"] > 0
        assert all(r["team"] == "KC" for r in result["players"])
        assert all(r["season"] == 2024 for r in result["players"])

    def test_filter_by_position(self):
        result = nfl_roster(team="KC", season=2024, position="QB")
        assert "error" not in result
        assert result["count"] > 0

    def test_returns_expected_fields(self):
        result = nfl_roster(team="KC", season=2024)
        row = result["players"][0]
        for field in ("full_name", "position", "team", "season"):
            assert field in row

    def test_unknown_team_returns_empty(self):
        result = nfl_roster(team="ZZZ", season=2024)
        assert "error" not in result
        assert result["count"] == 0


@pytest.mark.integration
@pytest.mark.usefixtures("require_db")
class TestNflInjuries:
    def test_filter_by_team_and_season(self):
        result = nfl_injuries(team="KC", season=2024)
        assert "error" not in result
        assert result["count"] > 0

    def test_filter_by_week(self):
        result = nfl_injuries(team="KC", season=2024, week=10)
        assert "error" not in result
        assert all(r["week"] == 10 for r in result["injuries"])

    def test_filter_by_player(self):
        result = nfl_injuries(player="Mahomes", season=2024)
        assert "error" not in result

    def test_returns_expected_fields(self):
        result = nfl_injuries(team="KC", season=2024, week=1)
        if result["count"] > 0:
            row = result["injuries"][0]
            for field in ("full_name", "position", "team", "season", "week", "report_status"):
                assert field in row


@pytest.mark.integration
@pytest.mark.usefixtures("require_db")
class TestNflSchedule:
    def test_filter_by_team(self):
        result = nfl_schedule(team="KC", season=2024)
        assert "error" not in result
        assert result["count"] > 0
        assert all(
            r["home_team"] == "KC" or r["away_team"] == "KC"
            for r in result["games"]
        )

    def test_filter_by_week(self):
        result = nfl_schedule(season=2024, week=1)
        assert "error" not in result
        assert result["count"] > 0
        assert all(r["week"] == 1 for r in result["games"])

    def test_returns_expected_fields(self):
        result = nfl_schedule(team="KC", season=2024)
        row = result["games"][0]
        for field in ("game_id", "season", "week", "home_team", "away_team", "home_score", "away_score"):
            assert field in row

    def test_season_type_filter(self):
        result = nfl_schedule(team="KC", season=2024, season_type="REG")
        assert "error" not in result
        assert all(r["game_type"] == "REG" for r in result["games"])


@pytest.mark.integration
@pytest.mark.usefixtures("require_db")
class TestNflSnapCounts:
    def test_filter_by_team_and_season(self):
        result = nfl_snap_counts(team="KC", season=2024)
        assert "error" not in result
        assert result["count"] > 0

    def test_filter_by_player(self):
        result = nfl_snap_counts(player="Kelce", season=2024)
        assert "error" not in result
        assert result["count"] > 0

    def test_filter_by_week(self):
        result = nfl_snap_counts(team="KC", season=2024, week=1)
        assert "error" not in result
        assert all(r["week"] == 1 for r in result["snap_counts"])

    def test_returns_expected_fields(self):
        result = nfl_snap_counts(team="KC", season=2024, week=1)
        if result["count"] > 0:
            row = result["snap_counts"][0]
            for field in ("player", "position", "team", "season", "week", "offense_snaps", "offense_pct"):
                assert field in row


class TestInputValidation:
    def test_invalid_stat_type(self):
        result = nfl_player_stats(player_name="Mahomes", stat_type="kicking")
        assert "error" in result

    def test_invalid_compare_type(self):
        result = nfl_compare(entity1="KC", entity2="BAL", compare_type="coach")
        assert "error" in result

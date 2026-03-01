"""Tests for the structured MCP tools."""

import pytest
from nfl_mcp.tools import (
    nfl_schema, nfl_search_plays, nfl_team_stats,
    nfl_player_stats, nfl_compare, nfl_query,
)


@pytest.fixture(autouse=True)
def _check_db():
    """Skip all tests in this file if no data is loaded."""
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

    def test_invalid_stat_type(self):
        result = nfl_player_stats(player_name="Mahomes", stat_type="kicking")
        assert "error" in result

    def test_unknown_player_returns_empty(self):
        result = nfl_player_stats(player_name="ZZZZNOTAPLAYER", stat_type="passing")
        assert "error" not in result
        assert len(result["seasons"]) == 0


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

    def test_invalid_compare_type(self):
        result = nfl_compare(entity1="KC", entity2="BAL", compare_type="coach")
        assert "error" in result

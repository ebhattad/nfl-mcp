"""DB-independent unit tests for query construction and validation."""

import nfl_mcp.tools as tools


def test_search_plays_uses_parameterized_filters(monkeypatch):
    captured = {}

    def fake_execute(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(tools, "_execute", fake_execute)
    result = tools.nfl_search_plays(team="KC", player="Mahomes", season=2024, max_rows=5)

    assert result["row_count"] == 0
    assert "posteam = ?" in captured["sql"]
    assert "ILIKE ?" in captured["sql"]
    assert "LIMIT 5" in captured["sql"]
    assert captured["params"][0] == "KC"
    assert "%Mahomes%" in captured["params"]
    assert 2024 in captured["params"]


def test_team_stats_uses_parameterized_team_and_season(monkeypatch):
    calls = []

    def fake_execute(sql, params=None):
        calls.append((sql, params))
        return []

    monkeypatch.setattr(tools, "_execute", fake_execute)
    tools.nfl_team_stats(team="kc", season=2024, side="both")

    assert len(calls) == 3
    assert all("team = ?" in sql for sql, _ in calls)
    assert all(params[0] == "KC" for _, params in calls)
    assert all(2024 in params for _, params in calls)


def test_player_stats_uses_parameterized_player_name(monkeypatch):
    captured = {}

    def fake_execute(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(tools, "_execute", fake_execute)
    tools.nfl_player_stats(player_name="P.Mahomes", stat_type="passing", season_type="REG")

    assert "ILIKE ?" in captured["sql"]
    assert captured["params"][0] == "%P.Mahomes%"
    assert captured["params"][1] == "pass"
    assert captured["params"][-1] == "REG"


def test_compare_team_does_not_interpolate_user_input(monkeypatch):
    calls = []

    def fake_execute(sql, params=None):
        calls.append((sql, params))
        return []

    monkeypatch.setattr(tools, "_execute", fake_execute)
    tools.nfl_compare(entity1="KC'; DROP TABLE plays; --", entity2="BAL", compare_type="team", season=2024)

    assert len(calls) == 4
    assert all("DROP TABLE" not in sql for sql, _ in calls)
    assert calls[0][1][0] == "KC'; DROP TABLE PLAYS; --"

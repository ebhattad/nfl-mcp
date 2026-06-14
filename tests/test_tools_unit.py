"""DB-independent unit tests for query construction and validation."""

import time
from contextlib import contextmanager

import duckdb
import pytest

import nfl_mcp.tools as tools


def test_normalize_player_name_handles_full_short_and_last_name_inputs():
    assert tools._normalize_player_name("Mahomes") == "Mahomes"
    assert tools._normalize_player_name("J.Jefferson") == "J.Jefferson"
    assert tools._normalize_player_name("Justin Jefferson") == "J.Jefferson"
    assert tools._normalize_player_name("J. Jefferson") == "J. Jefferson"


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


def test_player_stats_normalizes_full_player_name(monkeypatch):
    captured = {}

    def fake_execute(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(tools, "_execute", fake_execute)
    tools.nfl_player_stats(player_name="Justin Jefferson", stat_type="passing")

    assert captured["params"][0] == "%J.Jefferson%"


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


def test_schema_table_returns_columns(monkeypatch):
    monkeypatch.setattr(
        tools,
        "_execute",
        lambda sql, params=None: [{"column_name": "full_name", "data_type": "VARCHAR"}],
    )
    result = tools.nfl_schema(table="rosters")
    assert result["table"] == "rosters"
    assert result["columns"][0]["column_name"] == "full_name"


def test_schema_table_not_found_returns_available_tables(monkeypatch):
    def fake_execute(sql, params=None):
        if "information_schema.columns" in sql:
            return []
        return [{"table_name": "plays"}, {"table_name": "rosters"}]

    monkeypatch.setattr(tools, "_execute", fake_execute)
    result = tools.nfl_schema(table="missing_table")
    assert "not found" in result["error"]
    assert result["available_tables"] == ["plays", "rosters"]


def test_schema_table_returns_error_when_execute_raises(monkeypatch):
    monkeypatch.setattr(tools, "_execute", lambda *a, **k: (_ for _ in ()).throw(ValueError("schema boom")))
    result = tools.nfl_schema(table="rosters")
    assert result["error"] == "schema boom"


def test_schema_summary_handles_table_listing_error(monkeypatch):
    monkeypatch.setattr(tools, "_execute", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    result = tools.nfl_schema()
    assert "schema" in result
    assert result["available_tables"] == []


def test_schema_category_all_and_unknown():
    all_result = tools.nfl_schema(category="all")
    assert "schema" in all_result and isinstance(all_result["schema"], str)

    unknown = tools.nfl_schema(category="not-real")
    assert "error" in unknown
    assert "available" in unknown


def test_nfl_status_happy_path(monkeypatch):
    responses = iter([
        [{"total_plays": 123}],
        [{"season": 2024, "season_type": "REG", "plays": 123}],
        [{"first_season": 2024, "last_season": 2024, "num_seasons": 1}],
        [{"dataset_id": "pbp", "table_name": "plays", "total_rows": 123}],
        [{"last_refreshed": "2026-03-01"}],
    ])
    monkeypatch.setattr(tools, "_execute", lambda sql, params=None: next(responses))

    result = tools.nfl_status()
    assert result["plays"]["total_plays"] == 123
    assert result["plays"]["season_range"]["first_season"] == 2024
    assert result["datasets"]["total_loaded"] == 1
    assert result["datasets"]["last_refreshed"] == "2026-03-01"


def test_nfl_status_returns_error_on_timeout(monkeypatch):
    monkeypatch.setattr(tools, "_execute", lambda *a, **k: (_ for _ in ()).throw(TimeoutError("timeout")))
    result = tools.nfl_status()
    assert "error" in result


def test_nfl_query_reports_truncation(monkeypatch):
    monkeypatch.setattr(
        tools,
        "_execute",
        lambda sql, params=None: [{"x": 1}, {"x": 2}, {"x": 3}],
    )
    result = tools.nfl_query("SELECT 1 AS x", max_rows=2)
    assert result["row_count"] == 2
    assert result["truncated"] is True


def test_nfl_query_returns_error_when_execute_fails(monkeypatch):
    monkeypatch.setattr(tools, "_execute", lambda *a, **k: (_ for _ in ()).throw(ValueError("bad sql")))
    result = tools.nfl_query("SELECT 1")
    assert result["error"] == "bad sql"


@pytest.mark.parametrize(
    "situation,expected_sql",
    [
        ("red_zone", "yardline_100 <= 20"),
        ("third_down", "down = 3"),
        ("fourth_down", "down = 4"),
        ("two_minute", "qtr = 4 AND half_seconds_remaining <= 120"),
    ],
)
def test_search_plays_situations(monkeypatch, situation, expected_sql):
    captured = {}

    def fake_execute(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(tools, "_execute", fake_execute)
    tools.nfl_search_plays(situation=situation, max_rows=10)
    assert expected_sql in captured["sql"]


def test_search_plays_applies_all_numeric_and_boolean_filters(monkeypatch):
    captured = {}

    def fake_execute(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(tools, "_execute", fake_execute)
    result = tools.nfl_search_plays(
        opponent="BAL",
        season_from=2020,
        season_to=2021,
        week=1,
        season_type="REG",
        play_type="pass",
        is_touchdown=True,
        is_turnover=True,
        min_yards=7,
        max_rows=999,
    )

    assert result["row_count"] == 0
    assert "defteam = ?" in captured["sql"]
    assert "season >= ?" in captured["sql"]
    assert "season <= ?" in captured["sql"]
    assert "week = ?" in captured["sql"]
    assert "season_type = ?" in captured["sql"]
    assert "play_type = ?" in captured["sql"]
    assert "touchdown = 1" in captured["sql"]
    assert "(interception = 1 OR fumble_lost = 1)" in captured["sql"]
    assert "yards_gained >= ?" in captured["sql"]
    assert "LIMIT 500" in captured["sql"]
    assert captured["params"] == ["BAL", 2020, 2021, 1, "REG", "pass", 7]


def test_search_plays_returns_error_on_timeout(monkeypatch):
    monkeypatch.setattr(tools, "_execute", lambda *a, **k: (_ for _ in ()).throw(TimeoutError("boom")))
    result = tools.nfl_search_plays(team="KC")
    assert "error" in result


def test_team_stats_offense_only_without_season_clause(monkeypatch):
    calls = []

    def fake_execute(sql, params=None):
        calls.append((sql, params))
        return []

    monkeypatch.setattr(tools, "_execute", fake_execute)
    result = tools.nfl_team_stats(team="kc", side="offense")
    assert "offense" in result
    assert "defense" not in result
    assert "season_year = ?" not in calls[0][0]
    assert calls[0][1] == ["KC"]


def test_team_stats_returns_error_on_timeout(monkeypatch):
    monkeypatch.setattr(tools, "_execute", lambda *a, **k: (_ for _ in ()).throw(TimeoutError("timeout")))
    result = tools.nfl_team_stats(team="KC")
    assert "error" in result


@pytest.mark.parametrize(
    "stat_type,expected_play_type",
    [("rushing", "run"), ("receiving", "pass")],
)
def test_player_stats_other_stat_types(monkeypatch, stat_type, expected_play_type):
    captured = {}

    def fake_execute(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return [{"season": 2024, "season_type": "REG"}]

    monkeypatch.setattr(tools, "_execute", fake_execute)
    result = tools.nfl_player_stats(
        player_name="P.Mahomes",
        stat_type=stat_type,
        season_from=2020,
        season_to=2024,
    )
    assert result["stat_type"] == stat_type
    assert captured["params"][1] == expected_play_type
    assert "season >=" in captured["sql"]
    assert "season <=" in captured["sql"]


def test_player_stats_rejects_unknown_stat_type():
    result = tools.nfl_player_stats(player_name="P.Mahomes", stat_type="kicking")
    assert "Unknown stat_type" in result["error"]


def test_player_stats_returns_error_on_timeout(monkeypatch):
    monkeypatch.setattr(tools, "_execute", lambda *a, **k: (_ for _ in ()).throw(TimeoutError("timeout")))
    result = tools.nfl_player_stats(player_name="P.Mahomes", stat_type="passing")
    assert "error" in result


def test_compare_player_path_only_includes_stats_with_attempts(monkeypatch):
    calls = []

    def fake_execute(sql, params=None):
        calls.append((sql, params))
        if "COUNT(*) AS n" in sql and "passer_player_name" in sql:
            return [{"n": 1}]
        if "COUNT(*) AS n" in sql and "receiver_player_name" in sql:
            return [{"n": 1}]
        if "COUNT(*) AS n" in sql and "rusher_player_name" in sql:
            return [{"n": 0}]
        return []

    monkeypatch.setattr(tools, "_execute", fake_execute)
    monkeypatch.setattr(
        tools,
        "nfl_player_stats",
        lambda player_name, **kwargs: {"seasons": [{"player": player_name, **kwargs}]},
    )
    result = tools.nfl_compare(
        entity1="P.Mahomes",
        entity2="J.Allen",
        compare_type="player",
        season_from=2020,
        season_to=2024,
        season_type="REG",
    )
    assert "passing" in result["P.Mahomes"]
    assert "receiving" in result["P.Mahomes"]
    assert "rushing" not in result["P.Mahomes"]
    assert "season_type = ?" in calls[0][0]


def test_compare_rejects_unknown_type():
    result = tools.nfl_compare(entity1="KC", entity2="BAL", compare_type="invalid")
    assert "compare_type must be" in result["error"]


def test_compare_team_applies_season_range_filters(monkeypatch):
    calls = []

    def fake_execute(sql, params=None):
        calls.append((sql, params))
        return []

    monkeypatch.setattr(tools, "_execute", fake_execute)
    result = tools.nfl_compare(
        entity1="KC",
        entity2="BAL",
        compare_type="team",
        season_from=2020,
        season_to=2021,
    )
    assert "error" not in result
    assert len(calls) == 4
    assert "season_year >= ?" in calls[0][0]
    assert "season_year <= ?" in calls[0][0]
    assert calls[0][1] == ["KC", 2020, 2021]


def test_compare_returns_error_on_timeout(monkeypatch):
    monkeypatch.setattr(tools, "_execute", lambda *a, **k: (_ for _ in ()).throw(TimeoutError("timeout")))
    result = tools.nfl_compare(entity1="KC", entity2="BAL", compare_type="team", season=2024)
    assert "error" in result


def test_catalog_returns_dataset_summary(monkeypatch):
    monkeypatch.setattr(
        tools,
        "_execute",
        lambda sql, params=None: [{"dataset_id": "pbp", "table_name": "plays", "total_rows": 123}],
    )
    result = tools.nfl_catalog()
    assert result["total_datasets"] == 1
    assert result["datasets"][0]["dataset_id"] == "pbp"


def test_catalog_returns_error_on_timeout(monkeypatch):
    monkeypatch.setattr(tools, "_execute", lambda *a, **k: (_ for _ in ()).throw(TimeoutError("timeout")))
    result = tools.nfl_catalog()
    assert "error" in result


def test_roster_builds_filters_and_uppercases_inputs(monkeypatch):
    captured = {}

    def fake_execute(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return [{"full_name": "A Player"}]

    monkeypatch.setattr(tools, "_execute", fake_execute)
    result = tools.nfl_roster(team="kc", season=2024, position="wr")
    assert result["count"] == 1
    assert "team = ?" in captured["sql"]
    assert "season = ?" in captured["sql"]
    assert "position ILIKE ?" in captured["sql"]
    assert captured["params"] == ["KC", 2024, "WR"]


def test_roster_uses_none_params_when_no_filters(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        tools,
        "_execute",
        lambda sql, params=None: captured.setdefault("params", params) or [],
    )
    tools.nfl_roster()
    assert captured["params"] is None


def test_roster_returns_error_on_timeout(monkeypatch):
    monkeypatch.setattr(tools, "_execute", lambda *a, **k: (_ for _ in ()).throw(TimeoutError("roster timeout")))
    result = tools.nfl_roster(team="KC")
    assert result["error"] == "roster timeout"


def test_injuries_builds_filters_and_wildcards(monkeypatch):
    captured = {}

    def fake_execute(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return [{"full_name": "P.Mahomes"}]

    monkeypatch.setattr(tools, "_execute", fake_execute)
    result = tools.nfl_injuries(
        team="kc",
        season=2024,
        week=1,
        player="Mahomes",
        report_status="Out",
    )
    assert result["count"] == 1
    assert "team = ?" in captured["sql"]
    assert "report_status ILIKE ?" in captured["sql"]
    assert captured["params"] == ["KC", 2024, 1, "%Mahomes%", "%Out%"]


def test_injuries_returns_error_on_timeout(monkeypatch):
    monkeypatch.setattr(tools, "_execute", lambda *a, **k: (_ for _ in ()).throw(TimeoutError("injuries timeout")))
    result = tools.nfl_injuries(team="KC")
    assert result["error"] == "injuries timeout"


def test_schedule_builds_team_pair_filter_and_uppercases_season_type(monkeypatch):
    captured = {}

    def fake_execute(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return [{"game_id": "2024_01_KC_BAL"}]

    monkeypatch.setattr(tools, "_execute", fake_execute)
    result = tools.nfl_schedule(team="kc", season=2024, week=1, season_type="reg")
    assert result["count"] == 1
    assert "(home_team = ? OR away_team = ?)" in captured["sql"]
    assert "game_type = ?" in captured["sql"]
    assert captured["params"] == ["KC", "KC", 2024, 1, "REG"]


def test_schedule_returns_error_on_timeout(monkeypatch):
    monkeypatch.setattr(tools, "_execute", lambda *a, **k: (_ for _ in ()).throw(TimeoutError("schedule timeout")))
    result = tools.nfl_schedule(team="KC")
    assert result["error"] == "schedule timeout"


def test_snap_counts_builds_filters(monkeypatch):
    captured = {}

    def fake_execute(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return [{"player": "T.Kelce"}]

    monkeypatch.setattr(tools, "_execute", fake_execute)
    result = tools.nfl_snap_counts(
        player="Kelce",
        team="kc",
        season=2024,
        week=1,
        position="te",
    )
    assert result["count"] == 1
    assert "player ILIKE ?" in captured["sql"]
    assert "team = ?" in captured["sql"]
    assert "position ILIKE ?" in captured["sql"]
    assert captured["params"] == ["%Kelce%", "KC", 2024, 1, "TE"]


def test_snap_counts_returns_error_on_timeout(monkeypatch):
    monkeypatch.setattr(tools, "_execute", lambda *a, **k: (_ for _ in ()).throw(TimeoutError("snap timeout")))
    result = tools.nfl_snap_counts(team="KC")
    assert result["error"] == "snap timeout"


def test_execute_raises_worker_error(monkeypatch):
    class _BadConn:
        def execute(self, *_args, **_kwargs):
            raise ValueError("execute failed")

    @contextmanager
    def fake_get_db_connection():
        yield _BadConn()

    monkeypatch.setattr(tools, "get_db_connection", fake_get_db_connection)
    with pytest.raises(ValueError, match="execute failed"):
        tools._execute("SELECT 1")


def test_execute_returns_rows_with_and_without_params(monkeypatch):
    class _Rel:
        description = [("col_a",), ("col_b",)]

        def fetchall(self):
            return [(1, "x")]

    class _GoodConn:
        def __init__(self):
            self.calls = []

        def execute(self, sql, params=None):
            self.calls.append((sql, params))
            return _Rel()

    conn = _GoodConn()

    @contextmanager
    def fake_get_db_connection():
        yield conn

    monkeypatch.setattr(tools, "get_db_connection", fake_get_db_connection)

    rows_without_params = tools._execute("SELECT 1")
    rows_with_params = tools._execute("SELECT ? AS col_a, ? AS col_b", [1, "x"])

    assert rows_without_params == [{"col_a": 1, "col_b": "x"}]
    assert rows_with_params == [{"col_a": 1, "col_b": "x"}]
    assert conn.calls[0][1] is None
    assert conn.calls[1][1] == [1, "x"]


def test_execute_timeout_interrupt_warning_path(monkeypatch):
    class _Rel:
        description = [("x",)]

        def fetchall(self):
            return [(1,)]

    class _SlowConn:
        def execute(self, *_args, **_kwargs):
            time.sleep(0.25)
            return _Rel()

        def interrupt(self):
            raise RuntimeError("interrupt failed")

    @contextmanager
    def fake_get_db_connection():
        yield _SlowConn()

    warnings = []
    monkeypatch.setattr(tools, "get_db_connection", fake_get_db_connection)
    monkeypatch.setattr(tools, "_QUERY_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(tools.logger, "warning", lambda *a, **k: warnings.append((a, k)))

    with pytest.raises(TimeoutError, match="Query exceeded"):
        tools._execute("SELECT 1")
    assert warnings


def test_ftn_charting_builds_all_filters(monkeypatch):
    captured = {}

    def fake_execute(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return [{
            "total_plays": 10, "play_action": 4, "rpo": 1, "screen_pass": 2,
            "no_huddle": 0, "motion": 5, "trick_play": 0,
            "avg_defenders_in_box": 6.5, "avg_pass_rushers": 4.2, "avg_blitzers": 1.1,
        }]

    monkeypatch.setattr(tools, "_execute", fake_execute)
    result = tools.nfl_ftn_charting(
        team="kc", opponent="phi", player="Mahomes",
        season_from=2022, season_to=2024, week=3, season_type="reg",
    )
    sql, params = captured["sql"], captured["params"]
    assert "p.posteam = ?" in sql and "p.defteam = ?" in sql
    assert "p.passer_player_name ILIKE ?" in sql
    assert "p.season >= ?" in sql and "p.season <= ?" in sql
    assert "p.week = ?" in sql and "p.season_type = ?" in sql
    assert params[:2] == ["KC", "PHI"]
    assert params[2:5] == ["%Mahomes%"] * 3
    assert params[5:7] == [2022, 2024]
    assert params[-2:] == [3, "REG"]
    assert result["charting"]["play_action"]["pct"] == 40.0
    assert result["charting"]["motion"]["plays"] == 5


def test_ftn_charting_exact_season_filter(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        tools, "_execute",
        lambda sql, params=None: captured.update(sql=sql, params=params) or [{"total_plays": 0}],
    )
    result = tools.nfl_ftn_charting(season=2024)
    assert "p.season = ?" in captured["sql"]
    assert captured["params"] == [2024]
    assert result == {"charting": {}, "total_plays": 0}


def test_ftn_charting_handles_null_averages(monkeypatch):
    monkeypatch.setattr(
        tools, "_execute",
        lambda sql, params=None: [{
            "total_plays": 3, "play_action": None, "rpo": None, "screen_pass": None,
            "no_huddle": None, "motion": None, "trick_play": None,
            "avg_defenders_in_box": None, "avg_pass_rushers": None, "avg_blitzers": None,
        }],
    )
    result = tools.nfl_ftn_charting(team="KC")
    c = result["charting"]
    assert c["play_action"]["pct"] == 0.0
    assert c["play_action"]["plays"] == 0
    assert c["avg_defenders_in_box"] is None


def test_ftn_charting_empty_rows(monkeypatch):
    monkeypatch.setattr(tools, "_execute", lambda sql, params=None: [])
    result = tools.nfl_ftn_charting(team="KC")
    assert result == {"charting": {}, "total_plays": 0}


def test_ftn_charting_returns_error_on_db_error(monkeypatch):
    monkeypatch.setattr(tools, "_execute", lambda *a, **k: (_ for _ in ()).throw(duckdb.Error("ftn boom")))
    result = tools.nfl_ftn_charting(team="KC")
    assert result["error"] == "ftn boom"


# ── Fantasy derived-table tools ────────────────────────────────────────────────

def _capture_execute(monkeypatch, rows):
    captured = {}

    def fake_execute(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return rows

    monkeypatch.setattr(tools, "_execute", fake_execute)
    return captured


def test_td_luck_builds_filters(monkeypatch):
    captured = _capture_execute(monkeypatch, [{"full_name": "X"}])
    result = tools.nfl_td_luck(player="Jefferson", position="wr", team="min", season=2024, limit=10)
    assert result["count"] == 1
    assert "full_name ILIKE ?" in captured["sql"]
    assert "position ILIKE ?" in captured["sql"]
    assert "team = ?" in captured["sql"]
    assert "total_td_luck_score ASC" in captured["sql"]
    assert captured["params"] == ["%Jefferson%", "WR", "MIN", 2024]


def test_td_luck_invalid_limit_falls_back(monkeypatch):
    captured = _capture_execute(monkeypatch, [])
    tools.nfl_td_luck(limit="oops")
    assert "LIMIT 50" in captured["sql"]
    assert captured["params"] is None


def test_td_luck_returns_error_on_timeout(monkeypatch):
    monkeypatch.setattr(tools, "_execute", lambda *a, **k: (_ for _ in ()).throw(TimeoutError("td timeout")))
    assert tools.nfl_td_luck()["error"] == "td timeout"


def test_role_trend_builds_filters(monkeypatch):
    captured = _capture_execute(monkeypatch, [{"full_name": "X"}])
    tools.nfl_role_trend(player="Kelce", position="te", team="kc", season=2024, week=8, min_snap_pct=50)
    assert "full_name ILIKE ?" in captured["sql"]
    assert "snap_pct >= ?" in captured["sql"]
    assert "snap_pct_delta DESC" in captured["sql"]
    assert captured["params"] == ["%Kelce%", "TE", "KC", 2024, 8, 50.0]


def test_role_trend_invalid_limit_falls_back(monkeypatch):
    captured = _capture_execute(monkeypatch, [])
    tools.nfl_role_trend(limit="oops")
    assert "LIMIT 50" in captured["sql"]


def test_role_trend_returns_error_on_timeout(monkeypatch):
    monkeypatch.setattr(tools, "_execute", lambda *a, **k: (_ for _ in ()).throw(TimeoutError("role timeout")))
    assert tools.nfl_role_trend()["error"] == "role timeout"


def test_separation_opportunity_builds_filters(monkeypatch):
    captured = _capture_execute(monkeypatch, [{"full_name": "X"}])
    tools.nfl_separation_opportunity(player="Aiyuk", position="wr", team="sf", season=2024, regression_candidate=True)
    assert "regression_candidate = ?" in captured["sql"]
    assert "fp_diff_per_game ASC" in captured["sql"]
    assert captured["params"] == ["%Aiyuk%", "WR", "SF", 2024, True]


def test_separation_opportunity_guards_pre_2016():
    result = tools.nfl_separation_opportunity(season=2015)
    assert "2016" in result["error"]


def test_separation_opportunity_invalid_limit_falls_back(monkeypatch):
    captured = _capture_execute(monkeypatch, [])
    tools.nfl_separation_opportunity(limit="oops")
    assert "LIMIT 50" in captured["sql"]


def test_separation_opportunity_returns_error_on_timeout(monkeypatch):
    monkeypatch.setattr(tools, "_execute", lambda *a, **k: (_ for _ in ()).throw(TimeoutError("sep timeout")))
    assert tools.nfl_separation_opportunity()["error"] == "sep timeout"


def test_drop_rate_builds_filters(monkeypatch):
    captured = _capture_execute(monkeypatch, [{"player": "T.Pollard"}])
    tools.nfl_drop_rate(player="Pollard", team="ten", season=2024, min_targets=30)
    assert "player ILIKE ?" in captured["sql"]
    assert "catchable_targets >= ?" in captured["sql"]
    assert "drop_rate_pct DESC" in captured["sql"]
    assert captured["params"] == ["%Pollard%", "TEN", 2024, 30]


def test_drop_rate_guards_pre_2022():
    result = tools.nfl_drop_rate(season=2021)
    assert "2022" in result["error"]


def test_drop_rate_invalid_limit_falls_back(monkeypatch):
    captured = _capture_execute(monkeypatch, [])
    tools.nfl_drop_rate(limit="oops")
    assert "LIMIT 50" in captured["sql"]


def test_drop_rate_returns_error_on_timeout(monkeypatch):
    monkeypatch.setattr(tools, "_execute", lambda *a, **k: (_ for _ in ()).throw(TimeoutError("drop timeout")))
    assert tools.nfl_drop_rate()["error"] == "drop timeout"


def test_contract_value_builds_filters(monkeypatch):
    captured = _capture_execute(monkeypatch, [{"full_name": "X"}])
    tools.nfl_contract_value(player="Gibbs", position="rb", team="det", season=2024, min_apy=2, max_apy=10)
    assert "apy >= ?" in captured["sql"]
    assert "apy <= ?" in captured["sql"]
    assert "fp_per_million DESC" in captured["sql"]
    assert captured["params"] == ["%Gibbs%", "RB", "DET", 2024, 2.0, 10.0]


def test_contract_value_invalid_limit_falls_back(monkeypatch):
    captured = _capture_execute(monkeypatch, [])
    tools.nfl_contract_value(limit="oops")
    assert "LIMIT 50" in captured["sql"]


def test_contract_value_returns_error_on_timeout(monkeypatch):
    monkeypatch.setattr(tools, "_execute", lambda *a, **k: (_ for _ in ()).throw(TimeoutError("contract timeout")))
    assert tools.nfl_contract_value()["error"] == "contract timeout"


def test_injury_return_builds_filters(monkeypatch):
    captured = _capture_execute(monkeypatch, [{"injury_type": "hamstring"}])
    tools.nfl_injury_return(injury_type="hamstring", position="wr", week_post_return=2)
    assert "injury_type ILIKE ?" in captured["sql"]
    assert "position = ?" in captured["sql"]
    assert "week_post_return = ?" in captured["sql"]
    assert "week_post_return ASC" in captured["sql"]
    assert captured["params"] == ["%hamstring%", "WR", 2]


def test_injury_return_invalid_limit_falls_back(monkeypatch):
    captured = _capture_execute(monkeypatch, [])
    tools.nfl_injury_return(limit="oops")
    assert "LIMIT 50" in captured["sql"]
    assert captured["params"] is None


def test_injury_return_returns_error_on_timeout(monkeypatch):
    monkeypatch.setattr(tools, "_execute", lambda *a, **k: (_ for _ in ()).throw(TimeoutError("injury timeout")))
    assert tools.nfl_injury_return()["error"] == "injury timeout"

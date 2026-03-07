"""Tests for the dataset registry — structure, completeness, and loader validity."""

import nflreadpy
import pytest

from nfl_mcp.registry import (
    ALL_DATASETS,
    DEFAULT_DATASETS,
    REGISTRY,
    DatasetDef,
)


# ── Structure ──────────────────────────────────────────────────────────────────

class TestRegistryStructure:
    def test_registry_is_not_empty(self):
        assert len(REGISTRY) > 0

    def test_all_datasets_matches_registry_keys(self):
        assert set(ALL_DATASETS) == set(REGISTRY.keys())

    def test_no_duplicate_dataset_ids(self):
        assert len(ALL_DATASETS) == len(set(ALL_DATASETS))

    def test_no_duplicate_table_names(self):
        table_names = [d.table_name for d in REGISTRY.values()]
        assert len(table_names) == len(set(table_names)), \
            "Multiple datasets share the same table_name"

    def test_default_datasets_is_subset_of_all(self):
        assert set(DEFAULT_DATASETS) <= set(ALL_DATASETS)

    def test_default_datasets_not_empty(self):
        assert len(DEFAULT_DATASETS) > 0

    def test_all_wave_values_are_valid(self):
        for d in REGISTRY.values():
            assert d.wave in (1, 2, 3), \
                f"{d.dataset_id} has invalid wave={d.wave}"

    def test_all_entries_are_datasetdef(self):
        for d in REGISTRY.values():
            assert isinstance(d, DatasetDef)

    def test_storage_values_are_valid(self):
        valid = {"replace", "append_by_season"}
        for d in REGISTRY.values():
            assert d.storage in valid, \
                f"{d.dataset_id} has invalid storage={d.storage!r}"

    def test_descriptions_are_non_empty(self):
        for d in REGISTRY.values():
            assert d.description, f"{d.dataset_id} has empty description"


# ── Season coverage windows ────────────────────────────────────────────────────

class TestSeasonCoverageWindows:
    def test_min_max_ordering(self):
        for d in REGISTRY.values():
            if d.min_season and d.max_season:
                assert d.min_season <= d.max_season, \
                    f"{d.dataset_id}: min_season {d.min_season} > max_season {d.max_season}"

    def test_non_seasonal_datasets_have_no_coverage_window(self):
        for d in REGISTRY.values():
            if not d.seasonal:
                assert d.min_season is None, \
                    f"{d.dataset_id} is non-seasonal but has min_season"
                assert d.max_season is None, \
                    f"{d.dataset_id} is non-seasonal but has max_season"

    def test_season_values_are_plausible(self):
        for d in REGISTRY.values():
            if d.min_season:
                assert 1900 <= d.min_season <= 2030, \
                    f"{d.dataset_id} min_season={d.min_season} is implausible"
            if d.max_season:
                assert 1900 <= d.max_season <= 2030, \
                    f"{d.dataset_id} max_season={d.max_season} is implausible"

    @pytest.mark.parametrize("dataset_id,expected_min", [
        ("officials",             2015),
        ("participation",         2016),
        ("nextgen_stats_passing", 2016),
        ("pfr_advstats_pass",     2018),
        ("ftn_charting",          2022),
        ("snap_counts",           2012),
        ("injuries",              2009),
    ])
    def test_known_min_seasons(self, dataset_id, expected_min):
        assert REGISTRY[dataset_id].min_season == expected_min

    def test_participation_has_max_season(self):
        assert REGISTRY["participation"].max_season == 2024


# ── Loader functions ───────────────────────────────────────────────────────────

class TestLoaderFunctions:
    def test_all_loader_fns_exist_in_nflreadpy(self):
        missing = [
            d.dataset_id
            for d in REGISTRY.values()
            if not hasattr(nflreadpy, d.loader_fn)
        ]
        assert not missing, f"loader_fn not found in nflreadpy: {missing}"

    def test_extra_params_are_dicts(self):
        for d in REGISTRY.values():
            assert isinstance(d.extra_params, dict), \
                f"{d.dataset_id} extra_params is not a dict"

    def test_pbp_loader_is_load_pbp(self):
        assert REGISTRY["pbp"].loader_fn == "load_pbp"
        assert REGISTRY["pbp"].table_name == "plays"

    def test_non_seasonal_datasets_have_no_seasons_param(self):
        # Spot-check: these are known static datasets
        for ds_id in ("teams", "players", "contracts", "trades", "combine"):
            assert not REGISTRY[ds_id].seasonal, \
                f"{ds_id} should be non-seasonal"


# ── Default dataset completeness ───────────────────────────────────────────────

class TestDefaultDatasets:
    @pytest.mark.parametrize("dataset_id", [
        "pbp", "schedules", "rosters", "player_stats",
        "team_stats_raw", "injuries", "snap_counts",
        "teams", "players", "contracts", "trades",
    ])
    def test_core_datasets_are_default(self, dataset_id):
        assert REGISTRY[dataset_id].default, \
            f"{dataset_id} should be a default dataset"

    @pytest.mark.parametrize("dataset_id", [
        "participation", "nextgen_stats_passing", "ftn_charting",
        "rosters_weekly", "pfr_advstats_pass",
    ])
    def test_heavy_datasets_are_not_default(self, dataset_id):
        assert not REGISTRY[dataset_id].default, \
            f"{dataset_id} should be opt-in, not default"

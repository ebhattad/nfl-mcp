"""Tests for configuration loading behavior."""

import warnings

import nfl_mcp.config as config


def test_load_config_recovers_from_malformed_json(tmp_path, monkeypatch):
    config_dir = tmp_path / "nfl-mcp"
    config_dir.mkdir()
    config_file = config_dir / "config.json"
    config_file.write_text("{ this is not valid json ")

    monkeypatch.setattr(config, "CONFIG_DIR", config_dir)
    monkeypatch.setattr(config, "CONFIG_FILE", config_file)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        loaded = config.load_config()

    assert loaded == {**config._DEFAULT_CONFIG}
    assert (config_dir / "config.json.broken").exists()
    assert any("Malformed config" in str(w.message) for w in caught)

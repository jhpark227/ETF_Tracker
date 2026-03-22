from pathlib import Path
from etf_analyzer.config import load_config, AppConfig


def test_load_config_defaults():
    """Config without file should return defaults."""
    config = load_config(config_path=Path("/nonexistent/config.toml"))
    assert config.database.path == "data/etf_analyzer.duckdb"
    assert config.collector.request_delay_min == 2.0
    assert config.collector.request_delay_max == 4.0
    assert config.analyzer.default_window == 5
    assert config.analyzer.flow_weight == 0.4
    assert config.analyzer.conviction_weight == 0.6
    assert config.analyzer.active_etf_multiplier == 2.0
    assert len(config.etf_universe) == 25


def test_load_config_from_file(tmp_path):
    """Config from file should override defaults."""
    config_file = tmp_path / "config.toml"
    config_file.write_text("""
[database]
path = "custom/path.duckdb"

[analyzer]
flow_weight = 0.5
conviction_weight = 0.5
""")
    config = load_config(config_path=config_file)
    assert config.database.path == "custom/path.duckdb"
    assert config.analyzer.flow_weight == 0.5
    assert config.analyzer.conviction_weight == 0.5
    # Non-overridden values keep defaults
    assert config.analyzer.default_window == 5


def test_etf_universe_have_required_fields():
    """Each ETF entry must have code, name, type, tier, and sector."""
    config = load_config(config_path=Path("/nonexistent/config.toml"))
    for etf in config.etf_universe:
        assert etf.code
        assert etf.name
        assert etf.type in ("passive", "active")
        assert etf.tier in ("market", "sector", "strategy", "active")
        assert etf.sector

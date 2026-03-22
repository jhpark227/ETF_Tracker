from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class DatabaseConfig:
    path: str = "data/etf_analyzer.duckdb"


@dataclass
class CollectorConfig:
    request_delay_min: float = 2.0
    request_delay_max: float = 4.0
    backfill_batch_size: int = 5
    backfill_batch_delay: float = 10.0


@dataclass
class AnalyzerConfig:
    default_window: int = 5
    flow_weight: float = 0.4
    conviction_weight: float = 0.6
    active_etf_multiplier: float = 2.0


@dataclass
class EtfEntry:
    code: str
    name: str
    type: str  # "passive" or "active"
    tier: str = ""  # "market", "sector", "strategy", "active"
    sector: str = ""


DEFAULT_ETF_UNIVERSE = [
    # Tier 1: 광역 시장
    EtfEntry(code="069500", name="KODEX 200", type="passive", tier="market", sector="코스피200"),
    EtfEntry(code="229200", name="KODEX 코스닥150", type="passive", tier="market", sector="코스닥150"),
    EtfEntry(code="310970", name="TIGER MSCI Korea TR", type="passive", tier="market", sector="MSCI Korea"),
    EtfEntry(code="292150", name="TIGER 코리아TOP10", type="passive", tier="market", sector="대형주"),
    EtfEntry(code="495050", name="RISE 코리아밸류업", type="passive", tier="market", sector="밸류업"),
    # Tier 2: 업종/테마
    EtfEntry(code="396500", name="TIGER 반도체TOP10", type="passive", tier="sector", sector="반도체"),
    EtfEntry(code="395160", name="KODEX AI반도체", type="passive", tier="sector", sector="AI반도체"),
    EtfEntry(code="305720", name="KODEX 2차전지산업", type="passive", tier="sector", sector="2차전지"),
    EtfEntry(code="449450", name="PLUS K방산", type="passive", tier="sector", sector="방산"),
    EtfEntry(code="466920", name="SOL 조선TOP3플러스", type="passive", tier="sector", sector="조선"),
    EtfEntry(code="487240", name="KODEX AI전력핵심설비", type="passive", tier="sector", sector="AI전력"),
    EtfEntry(code="434730", name="HANARO 원자력iSelect", type="passive", tier="sector", sector="원자력"),
    EtfEntry(code="102970", name="KODEX 증권", type="passive", tier="sector", sector="금융"),
    EtfEntry(code="091180", name="KODEX 자동차", type="passive", tier="sector", sector="자동차"),
    EtfEntry(code="228790", name="TIGER 화장품", type="passive", tier="sector", sector="소비재"),
    # Tier 3: 전략/스타일
    EtfEntry(code="161510", name="PLUS 고배당주", type="passive", tier="strategy", sector="배당"),
    EtfEntry(code="102780", name="KODEX 삼성그룹", type="passive", tier="strategy", sector="삼성그룹"),
    EtfEntry(code="138540", name="TIGER 현대차그룹플러스", type="passive", tier="strategy", sector="현대차그룹"),
    EtfEntry(code="466940", name="TIGER 은행고배당플러스TOP10", type="passive", tier="strategy", sector="은행배당"),
    # Tier 4: 액티브
    EtfEntry(code="0163Y0", name="KoAct 코스닥액티브", type="active", tier="active", sector="코스닥"),
    EtfEntry(code="445290", name="KODEX 로봇액티브", type="active", tier="active", sector="로봇"),
    EtfEntry(code="444200", name="SOL 코리아메가테크액티브", type="active", tier="active", sector="메가테크"),
    EtfEntry(code="441800", name="TIME Korea플러스배당액티브", type="active", tier="active", sector="배당성장"),
    EtfEntry(code="462900", name="KoAct 바이오헬스케어액티브", type="active", tier="active", sector="바이오"),
    EtfEntry(code="494890", name="KODEX 200액티브", type="active", tier="active", sector="시장대표"),
]


@dataclass
class AppConfig:
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    collector: CollectorConfig = field(default_factory=CollectorConfig)
    analyzer: AnalyzerConfig = field(default_factory=AnalyzerConfig)
    etf_universe: list[EtfEntry] = field(default_factory=lambda: list(DEFAULT_ETF_UNIVERSE))


def load_config(config_path: Path | None = None) -> AppConfig:
    """Load configuration from TOML file, falling back to defaults."""
    if config_path is None:
        config_path = Path("config.toml")

    config = AppConfig()

    if not config_path.exists():
        return config

    with open(config_path, "rb") as f:
        data = tomllib.load(f)

    if "database" in data:
        for key, value in data["database"].items():
            setattr(config.database, key, value)

    if "collector" in data:
        for key, value in data["collector"].items():
            setattr(config.collector, key, value)

    if "analyzer" in data:
        for key, value in data["analyzer"].items():
            setattr(config.analyzer, key, value)

    if "etf_universe" in data and "etfs" in data["etf_universe"]:
        config.etf_universe = [
            EtfEntry(
                code=e["code"],
                name=e["name"],
                type=e["type"],
                tier=e.get("tier", ""),
                sector=e.get("sector", ""),
            )
            for e in data["etf_universe"]["etfs"]
        ]

    return config

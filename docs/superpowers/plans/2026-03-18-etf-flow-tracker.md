# ETF Flow Tracker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Korean ETF flow tracking system that collects daily ETF portfolio data from KRX, computes fund-flow and manager-conviction scores per stock, and ranks stocks by combined score via CLI.

**Architecture:** Layered module architecture with 4 independent modules (collector, storage, analyzer, cli) communicating via function calls. Data flows: collector → storage ← analyzer, with cli orchestrating both. DuckDB as embedded analytical database.

**Tech Stack:** Python 3.14, httpx, duckdb, pydantic, typer, rich, pytest

**Spec:** `docs/superpowers/specs/2026-03-18-etf-flow-tracker-design.md`

---

## File Map

| File | Responsibility |
|------|---------------|
| `pyproject.toml` | Dependencies, script entry point |
| `config.toml` | User-facing configuration (ETF list, weights, DB path) |
| `main.py` | Thin entry point delegating to CLI |
| `etf_analyzer/__init__.py` | Package marker |
| `etf_analyzer/config.py` | Load config.toml with defaults |
| `etf_analyzer/collector/__init__.py` | Package marker |
| `etf_analyzer/collector/models.py` | Pydantic models for KRX API responses |
| `etf_analyzer/collector/krx.py` | KRX HTTP client with rate limiting |
| `etf_analyzer/storage/__init__.py` | Package marker |
| `etf_analyzer/storage/db.py` | DuckDB connection manager |
| `etf_analyzer/storage/schema.py` | CREATE TABLE DDL statements |
| `etf_analyzer/storage/repository.py` | UPSERT/query functions |
| `etf_analyzer/analyzer/__init__.py` | Package marker |
| `etf_analyzer/analyzer/flow.py` | Flow score calculation |
| `etf_analyzer/analyzer/conviction.py` | Conviction score calculation |
| `etf_analyzer/analyzer/ranking.py` | Combined ranking and drill-down |
| `etf_analyzer/cli.py` | Typer app with all subcommands |
| `tests/conftest.py` | Shared fixtures (in-memory DuckDB, sample data) |
| `tests/fixtures/krx_holdings.json` | Sample KRX holdings API response |
| `tests/fixtures/krx_flow.json` | Sample KRX flow API response |
| `tests/test_config.py` | Config loading tests |
| `tests/test_storage/test_schema.py` | Schema creation tests |
| `tests/test_storage/test_repository.py` | Repository CRUD tests |
| `tests/test_collector/test_models.py` | Pydantic model parsing tests |
| `tests/test_collector/test_krx.py` | KRX client tests (mocked HTTP) |
| `tests/test_analyzer/test_flow.py` | Flow score tests |
| `tests/test_analyzer/test_conviction.py` | Conviction score tests |
| `tests/test_analyzer/test_ranking.py` | Combined ranking tests |
| `tests/test_integration.py` | End-to-end pipeline test |

---

### Task 1: Project Scaffolding

**Files:**
- Modify: `pyproject.toml`
- Modify: `main.py`
- Create: `config.toml`
- Create: `etf_analyzer/__init__.py`
- Create: `etf_analyzer/collector/__init__.py`
- Create: `etf_analyzer/storage/__init__.py`
- Create: `etf_analyzer/analyzer/__init__.py`
- Create: `etf_analyzer/web/__init__.py`
- Create: `tests/__init__.py`
- Create: `tests/test_collector/__init__.py`
- Create: `tests/test_storage/__init__.py`
- Create: `tests/test_analyzer/__init__.py`

- [ ] **Step 1: Update pyproject.toml with dependencies and script entry point**

```toml
[project]
name = "etf-analyzer"
version = "0.1.0"
description = "Korean ETF flow tracking system"
readme = "README.md"
requires-python = ">=3.14"
dependencies = [
    "httpx",
    "duckdb",
    "pydantic",
    "typer",
    "rich",
]

[project.scripts]
etf-analyzer = "etf_analyzer.cli:app"

[dependency-groups]
dev = [
    "pytest",
]
```

- [ ] **Step 2: Create config.toml with default settings**

```toml
[database]
path = "data/etf_analyzer.duckdb"

[collector]
request_delay_min = 2.0
request_delay_max = 4.0
backfill_batch_size = 5
backfill_batch_delay = 10.0

[analyzer]
default_window = 5
flow_weight = 0.4
conviction_weight = 0.6
active_etf_multiplier = 2.0

[pilot_etfs]
etfs = [
    { code = "069500", name = "KODEX 200", type = "passive" },
    { code = "102110", name = "TIGER 200", type = "passive" },
    { code = "305720", name = "KODEX 2차전지산업", type = "passive" },
    { code = "441800", name = "TIMEFOLIO Korea플러스배당액티브", type = "active" },
    { code = "161510", name = "PLUS 고배당주", type = "active" },
]
```

- [ ] **Step 3: Create package directories and __init__.py files**

Create empty `__init__.py` in:
- `etf_analyzer/__init__.py`
- `etf_analyzer/collector/__init__.py`
- `etf_analyzer/storage/__init__.py`
- `etf_analyzer/analyzer/__init__.py`
- `etf_analyzer/web/__init__.py`
- `tests/__init__.py`
- `tests/test_collector/__init__.py`
- `tests/test_storage/__init__.py`
- `tests/test_analyzer/__init__.py`

- [ ] **Step 4: Update main.py**

```python
from etf_analyzer.cli import app

if __name__ == "__main__":
    app()
```

- [ ] **Step 5: Create minimal cli.py so import works**

```python
import typer

app = typer.Typer(name="etf-analyzer", help="Korean ETF flow tracking system")


@app.callback()
def callback() -> None:
    """Korean ETF flow tracking system."""
```

- [ ] **Step 6: Install dependencies and verify**

Run: `uv sync`
Expected: Dependencies installed successfully.

Run: `uv run etf-analyzer --help`
Expected: Shows help text with "Korean ETF flow tracking system".

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml uv.lock config.toml main.py etf_analyzer/ tests/
git commit -m "feat: scaffold project structure with dependencies"
```

---

### Task 2: Configuration Module

**Files:**
- Create: `etf_analyzer/config.py`
- Create: `tests/test_config.py`

- [ ] **Step 1: Write the failing test**

`tests/test_config.py`:

```python
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
    assert len(config.pilot_etfs) == 5


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


def test_pilot_etfs_have_required_fields():
    """Each pilot ETF must have code, name, and type."""
    config = load_config(config_path=Path("/nonexistent/config.toml"))
    for etf in config.pilot_etfs:
        assert etf.code
        assert etf.name
        assert etf.type in ("passive", "active")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'etf_analyzer.config'`

- [ ] **Step 3: Implement config.py**

`etf_analyzer/config.py`:

```python
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


DEFAULT_PILOT_ETFS = [
    EtfEntry(code="069500", name="KODEX 200", type="passive"),
    EtfEntry(code="102110", name="TIGER 200", type="passive"),
    EtfEntry(code="305720", name="KODEX 2차전지산업", type="passive"),
    EtfEntry(code="441800", name="TIMEFOLIO Korea플러스배당액티브", type="active"),
    EtfEntry(code="161510", name="PLUS 고배당주", type="active"),
]


@dataclass
class AppConfig:
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    collector: CollectorConfig = field(default_factory=CollectorConfig)
    analyzer: AnalyzerConfig = field(default_factory=AnalyzerConfig)
    pilot_etfs: list[EtfEntry] = field(default_factory=lambda: list(DEFAULT_PILOT_ETFS))


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

    if "pilot_etfs" in data and "etfs" in data["pilot_etfs"]:
        config.pilot_etfs = [
            EtfEntry(code=e["code"], name=e["name"], type=e["type"])
            for e in data["pilot_etfs"]["etfs"]
        ]

    return config
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_config.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add etf_analyzer/config.py tests/test_config.py
git commit -m "feat: add configuration module with TOML loading and defaults"
```

---

### Task 3: Storage — DB Connection & Schema

**Files:**
- Create: `etf_analyzer/storage/db.py`
- Create: `etf_analyzer/storage/schema.py`
- Create: `tests/conftest.py`
- Create: `tests/test_storage/test_schema.py`

- [ ] **Step 1: Write the failing test**

`tests/conftest.py`:

```python
import duckdb
import pytest

from etf_analyzer.storage.db import get_connection
from etf_analyzer.storage.schema import create_tables


@pytest.fixture
def db():
    """In-memory DuckDB for testing."""
    conn = get_connection(":memory:")
    create_tables(conn)
    yield conn
    conn.close()
```

`tests/test_storage/test_schema.py`:

```python
def test_create_tables(db):
    """All 4 tables should exist after schema creation."""
    tables = db.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()
    table_names = {row[0] for row in tables}
    assert table_names == {"etf_master", "etf_holdings", "etf_flow", "stock_master"}


def test_create_tables_idempotent(db):
    """Calling create_tables twice should not error."""
    from etf_analyzer.storage.schema import create_tables
    create_tables(db)  # second call
    tables = db.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()
    assert len(tables) == 4


def test_etf_holdings_primary_key(db):
    """etf_holdings should reject duplicate (date, etf_code, stock_code)."""
    db.execute("""
        INSERT INTO etf_holdings VALUES ('2026-03-18', '069500', '005930', '삼성전자', 25.3, 1000)
    """)
    db.execute("""
        INSERT OR REPLACE INTO etf_holdings VALUES ('2026-03-18', '069500', '005930', '삼성전자', 26.0, 1100)
    """)
    result = db.execute(
        "SELECT weight FROM etf_holdings WHERE stock_code = '005930'"
    ).fetchone()
    assert result[0] == 26.0  # replaced, not duplicated
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_storage/test_schema.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'etf_analyzer.storage.db'`

- [ ] **Step 3: Implement db.py and schema.py**

`etf_analyzer/storage/db.py`:

```python
from __future__ import annotations

from pathlib import Path

import duckdb


def get_connection(db_path: str = "data/etf_analyzer.duckdb") -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection. Use ':memory:' for testing."""
    if db_path != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(db_path)
```

`etf_analyzer/storage/schema.py`:

```python
from __future__ import annotations

import duckdb


def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all tables if they don't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS etf_master (
            etf_code VARCHAR PRIMARY KEY,
            etf_name VARCHAR NOT NULL,
            manager VARCHAR,
            etf_type VARCHAR NOT NULL,
            benchmark VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS etf_holdings (
            date DATE NOT NULL,
            etf_code VARCHAR NOT NULL,
            stock_code VARCHAR NOT NULL,
            stock_name VARCHAR NOT NULL,
            weight DOUBLE NOT NULL,
            shares BIGINT NOT NULL,
            PRIMARY KEY (date, etf_code, stock_code)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS etf_flow (
            date DATE NOT NULL,
            etf_code VARCHAR NOT NULL,
            creation_units BIGINT NOT NULL,
            redemption_units BIGINT NOT NULL,
            net_units BIGINT NOT NULL,
            nav DOUBLE NOT NULL,
            PRIMARY KEY (date, etf_code)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_master (
            stock_code VARCHAR PRIMARY KEY,
            stock_name VARCHAR NOT NULL,
            market VARCHAR
        )
    """)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_storage/test_schema.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add etf_analyzer/storage/db.py etf_analyzer/storage/schema.py tests/conftest.py tests/test_storage/test_schema.py
git commit -m "feat: add DuckDB connection manager and schema creation"
```

---

### Task 4: Storage — Repository

**Files:**
- Create: `etf_analyzer/storage/repository.py`
- Create: `tests/test_storage/test_repository.py`

- [ ] **Step 1: Write the failing test**

`tests/test_storage/test_repository.py`:

```python
from datetime import date
from etf_analyzer.storage.repository import (
    upsert_etf_master,
    upsert_holdings,
    upsert_flow,
    upsert_stock_master,
    get_holdings,
    get_flow,
    get_collection_status,
)


def test_upsert_etf_master(db):
    upsert_etf_master(db, "069500", "KODEX 200", "삼성자산운용", "passive", "KOSPI 200")
    result = db.execute("SELECT * FROM etf_master WHERE etf_code = '069500'").fetchone()
    assert result[1] == "KODEX 200"
    assert result[3] == "passive"


def test_upsert_etf_master_update(db):
    """Upsert should update existing records."""
    upsert_etf_master(db, "069500", "KODEX 200", "삼성자산운용", "passive", "KOSPI 200")
    upsert_etf_master(db, "069500", "KODEX 200 Updated", "삼성자산운용", "passive", "KOSPI 200")
    result = db.execute("SELECT etf_name FROM etf_master WHERE etf_code = '069500'").fetchone()
    assert result[0] == "KODEX 200 Updated"


def test_upsert_holdings(db):
    rows = [
        {"date": date(2026, 3, 18), "etf_code": "069500", "stock_code": "005930",
         "stock_name": "삼성전자", "weight": 25.3, "shares": 1000},
        {"date": date(2026, 3, 18), "etf_code": "069500", "stock_code": "000660",
         "stock_name": "SK하이닉스", "weight": 10.5, "shares": 500},
    ]
    upsert_holdings(db, rows)
    result = db.execute("SELECT COUNT(*) FROM etf_holdings").fetchone()
    assert result[0] == 2


def test_upsert_flow(db):
    rows = [
        {"date": date(2026, 3, 18), "etf_code": "069500",
         "creation_units": 100, "redemption_units": 50, "net_units": 50, "nav": 35000.0},
    ]
    upsert_flow(db, rows)
    result = db.execute("SELECT net_units, nav FROM etf_flow WHERE etf_code = '069500'").fetchone()
    assert result[0] == 50
    assert result[1] == 35000.0


def test_upsert_stock_master(db):
    upsert_stock_master(db, "005930", "삼성전자", "KOSPI")
    result = db.execute("SELECT * FROM stock_master WHERE stock_code = '005930'").fetchone()
    assert result[1] == "삼성전자"
    assert result[2] == "KOSPI"


def test_get_holdings(db):
    rows = [
        {"date": date(2026, 3, 17), "etf_code": "069500", "stock_code": "005930",
         "stock_name": "삼성전자", "weight": 24.0, "shares": 900},
        {"date": date(2026, 3, 18), "etf_code": "069500", "stock_code": "005930",
         "stock_name": "삼성전자", "weight": 25.3, "shares": 1000},
    ]
    upsert_holdings(db, rows)
    result = get_holdings(db, date(2026, 3, 18))
    assert len(result) == 1
    assert result[0]["weight"] == 25.3


def test_get_flow(db):
    rows = [
        {"date": date(2026, 3, 18), "etf_code": "069500",
         "creation_units": 100, "redemption_units": 50, "net_units": 50, "nav": 35000.0},
    ]
    upsert_flow(db, rows)
    result = get_flow(db, date(2026, 3, 17), date(2026, 3, 18))
    assert len(result) == 1


def test_get_collection_status(db):
    rows = [
        {"date": date(2026, 3, 18), "etf_code": "069500", "stock_code": "005930",
         "stock_name": "삼성전자", "weight": 25.3, "shares": 1000},
    ]
    upsert_holdings(db, rows)
    status = get_collection_status(db)
    assert status["total_days"] == 1
    assert status["last_date"] == date(2026, 3, 18)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_storage/test_repository.py -v`
Expected: FAIL — `ImportError`

- [ ] **Step 3: Implement repository.py**

`etf_analyzer/storage/repository.py`:

```python
from __future__ import annotations

from datetime import date

import duckdb


def upsert_etf_master(
    conn: duckdb.DuckDBPyConnection,
    etf_code: str,
    etf_name: str,
    manager: str | None,
    etf_type: str,
    benchmark: str | None = None,
) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO etf_master VALUES (?, ?, ?, ?, ?)",
        [etf_code, etf_name, manager, etf_type, benchmark],
    )


def upsert_holdings(conn: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    for row in rows:
        conn.execute(
            "INSERT OR REPLACE INTO etf_holdings VALUES (?, ?, ?, ?, ?, ?)",
            [row["date"], row["etf_code"], row["stock_code"],
             row["stock_name"], row["weight"], row["shares"]],
        )


def upsert_flow(conn: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    for row in rows:
        conn.execute(
            "INSERT OR REPLACE INTO etf_flow VALUES (?, ?, ?, ?, ?, ?)",
            [row["date"], row["etf_code"], row["creation_units"],
             row["redemption_units"], row["net_units"], row["nav"]],
        )


def upsert_stock_master(
    conn: duckdb.DuckDBPyConnection,
    stock_code: str,
    stock_name: str,
    market: str | None = None,
) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO stock_master VALUES (?, ?, ?)",
        [stock_code, stock_name, market],
    )


def get_holdings(
    conn: duckdb.DuckDBPyConnection,
    target_date: date,
    etf_code: str | None = None,
) -> list[dict]:
    query = "SELECT * FROM etf_holdings WHERE date = ?"
    params: list = [target_date]
    if etf_code:
        query += " AND etf_code = ?"
        params.append(etf_code)
    rows = conn.execute(query, params).fetchall()
    columns = ["date", "etf_code", "stock_code", "stock_name", "weight", "shares"]
    return [dict(zip(columns, row)) for row in rows]


def get_flow(
    conn: duckdb.DuckDBPyConnection,
    from_date: date,
    to_date: date,
    etf_code: str | None = None,
) -> list[dict]:
    query = "SELECT * FROM etf_flow WHERE date >= ? AND date <= ?"
    params: list = [from_date, to_date]
    if etf_code:
        query += " AND etf_code = ?"
        params.append(etf_code)
    rows = conn.execute(query, params).fetchall()
    columns = ["date", "etf_code", "creation_units", "redemption_units", "net_units", "nav"]
    return [dict(zip(columns, row)) for row in rows]


def get_collection_status(conn: duckdb.DuckDBPyConnection) -> dict:
    result = conn.execute("""
        SELECT
            COUNT(DISTINCT date) as total_days,
            MIN(date) as first_date,
            MAX(date) as last_date,
            COUNT(DISTINCT etf_code) as etf_count
        FROM etf_holdings
    """).fetchone()
    return {
        "total_days": result[0],
        "first_date": result[1],
        "last_date": result[2],
        "etf_count": result[3],
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_storage/test_repository.py -v`
Expected: 8 passed

- [ ] **Step 5: Commit**

```bash
git add etf_analyzer/storage/repository.py tests/test_storage/test_repository.py
git commit -m "feat: add storage repository with UPSERT and query functions"
```

---

### Task 5: Collector — Pydantic Models

**Files:**
- Create: `etf_analyzer/collector/models.py`
- Create: `tests/fixtures/krx_holdings.json`
- Create: `tests/fixtures/krx_flow.json`
- Create: `tests/test_collector/test_models.py`

- [ ] **Step 1: Create KRX API response fixture files**

These are sample responses matching the actual KRX JSON API format. The exact field names need to be verified against real KRX responses during the collector implementation (Task 6), but this gives the Pydantic models a concrete structure to validate against.

`tests/fixtures/krx_holdings.json`:

```json
{
  "OutBlock_1": [
    {
      "ISU_SRT_CD": "005930",
      "ISU_NM": "삼성전자",
      "COMPST_RTO": "25.30",
      "SHRS": "1,000"
    },
    {
      "ISU_SRT_CD": "000660",
      "ISU_NM": "SK하이닉스",
      "COMPST_RTO": "10.50",
      "SHRS": "500"
    }
  ]
}
```

`tests/fixtures/krx_flow.json`:

```json
{
  "OutBlock_1": [
    {
      "TRD_DD": "2026/03/18",
      "CRETRT_NAV": "35,000.00",
      "SU_CNT": "100",
      "REDMPT_CNT": "50"
    }
  ]
}
```

- [ ] **Step 2: Write the failing test**

`tests/test_collector/test_models.py`:

```python
import json
from pathlib import Path

from etf_analyzer.collector.models import (
    parse_holdings_response,
    parse_flow_response,
    HoldingRecord,
    FlowRecord,
)

FIXTURES = Path(__file__).parent.parent / "fixtures"


def test_parse_holdings_response():
    raw = json.loads((FIXTURES / "krx_holdings.json").read_text())
    records = parse_holdings_response(raw, etf_code="069500", date_str="20260318")
    assert len(records) == 2
    assert isinstance(records[0], HoldingRecord)
    assert records[0].stock_code == "005930"
    assert records[0].stock_name == "삼성전자"
    assert records[0].weight == 25.3
    assert records[0].shares == 1000


def test_parse_flow_response():
    raw = json.loads((FIXTURES / "krx_flow.json").read_text())
    records = parse_flow_response(raw, etf_code="069500")
    assert len(records) == 1
    assert isinstance(records[0], FlowRecord)
    assert records[0].nav == 35000.0
    assert records[0].creation_units == 100
    assert records[0].redemption_units == 50
    assert records[0].net_units == 50


def test_parse_holdings_empty_response():
    raw = {"OutBlock_1": []}
    records = parse_holdings_response(raw, etf_code="069500", date_str="20260318")
    assert records == []


def test_parse_number_with_commas():
    """Korean number format: '1,000' -> 1000."""
    raw = {
        "OutBlock_1": [
            {"ISU_SRT_CD": "005930", "ISU_NM": "삼성전자",
             "COMPST_RTO": "1,234.56", "SHRS": "10,000"}
        ]
    }
    records = parse_holdings_response(raw, etf_code="069500", date_str="20260318")
    assert records[0].weight == 1234.56
    assert records[0].shares == 10000
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/test_collector/test_models.py -v`
Expected: FAIL — `ImportError`

- [ ] **Step 4: Implement models.py**

`etf_analyzer/collector/models.py`:

```python
from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel


def _parse_int(value: str) -> int:
    """Parse Korean-formatted integer: '1,000' -> 1000."""
    return int(value.replace(",", ""))


def _parse_float(value: str) -> float:
    """Parse Korean-formatted float: '1,234.56' -> 1234.56."""
    return float(value.replace(",", ""))


class HoldingRecord(BaseModel):
    date: date
    etf_code: str
    stock_code: str
    stock_name: str
    weight: float
    shares: int


class FlowRecord(BaseModel):
    date: date
    etf_code: str
    creation_units: int
    redemption_units: int
    net_units: int
    nav: float


def parse_holdings_response(
    raw: dict, etf_code: str, date_str: str
) -> list[HoldingRecord]:
    """Parse KRX holdings API response into HoldingRecord list."""
    items = raw.get("OutBlock_1", [])
    if not items:
        return []

    parsed_date = datetime.strptime(date_str, "%Y%m%d").date()
    records = []
    for item in items:
        records.append(
            HoldingRecord(
                date=parsed_date,
                etf_code=etf_code,
                stock_code=item["ISU_SRT_CD"],
                stock_name=item["ISU_NM"],
                weight=_parse_float(item["COMPST_RTO"]),
                shares=_parse_int(item["SHRS"]),
            )
        )
    return records


def parse_flow_response(raw: dict, etf_code: str) -> list[FlowRecord]:
    """Parse KRX flow API response into FlowRecord list."""
    items = raw.get("OutBlock_1", [])
    if not items:
        return []

    records = []
    for item in items:
        date_str = item["TRD_DD"].replace("/", "")
        parsed_date = datetime.strptime(date_str, "%Y%m%d").date()
        creation = _parse_int(item["SU_CNT"])
        redemption = _parse_int(item["REDMPT_CNT"])
        records.append(
            FlowRecord(
                date=parsed_date,
                etf_code=etf_code,
                creation_units=creation,
                redemption_units=redemption,
                net_units=creation - redemption,
                nav=_parse_float(item["CRETRT_NAV"]),
            )
        )
    return records
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_collector/test_models.py -v`
Expected: 4 passed

- [ ] **Step 6: Commit**

```bash
git add etf_analyzer/collector/models.py tests/test_collector/test_models.py tests/fixtures/
git commit -m "feat: add Pydantic models for KRX API response parsing"
```

---

### Task 6: Collector — KRX Client

**Files:**
- Create: `etf_analyzer/collector/krx.py`
- Create: `tests/test_collector/test_krx.py`

- [ ] **Step 1: Write the failing test**

`tests/test_collector/test_krx.py`:

```python
import json
from datetime import date
from pathlib import Path
from unittest.mock import patch, MagicMock

from etf_analyzer.collector.krx import KRXClient
from etf_analyzer.config import CollectorConfig

FIXTURES = Path(__file__).parent.parent / "fixtures"


def _mock_response(fixture_name: str) -> MagicMock:
    """Create a mock httpx response from a fixture file."""
    data = json.loads((FIXTURES / fixture_name).read_text())
    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = data
    mock.raise_for_status = MagicMock()
    return mock


def test_fetch_holdings():
    config = CollectorConfig(request_delay_min=0, request_delay_max=0)
    client = KRXClient(config)

    with patch.object(client, "_session") as mock_session:
        mock_session.post.return_value = _mock_response("krx_holdings.json")
        records = client.fetch_holdings("069500", date(2026, 3, 18))

    assert len(records) == 2
    assert records[0].stock_code == "005930"


def test_fetch_flow():
    config = CollectorConfig(request_delay_min=0, request_delay_max=0)
    client = KRXClient(config)

    with patch.object(client, "_session") as mock_session:
        mock_session.post.return_value = _mock_response("krx_flow.json")
        records = client.fetch_flow("069500", date(2026, 3, 18))

    assert len(records) == 1
    assert records[0].net_units == 50


def test_fetch_holdings_blocked():
    """HTTP 403 should raise and not silently continue."""
    config = CollectorConfig(request_delay_min=0, request_delay_max=0)
    client = KRXClient(config)

    mock_resp = MagicMock()
    mock_resp.status_code = 403
    mock_resp.raise_for_status.side_effect = Exception("403 Forbidden")

    with patch.object(client, "_session") as mock_session:
        mock_session.post.return_value = mock_resp
        try:
            client.fetch_holdings("069500", date(2026, 3, 18))
            assert False, "Should have raised"
        except Exception as e:
            assert "403" in str(e)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_collector/test_krx.py -v`
Expected: FAIL — `ImportError`

- [ ] **Step 3: Implement krx.py**

`etf_analyzer/collector/krx.py`:

```python
from __future__ import annotations

import logging
import random
import time
from datetime import date

import httpx

from etf_analyzer.collector.models import (
    FlowRecord,
    HoldingRecord,
    parse_flow_response,
    parse_holdings_response,
)
from etf_analyzer.config import CollectorConfig

logger = logging.getLogger(__name__)

KRX_BASE_URL = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd",
}


class KRXClient:
    def __init__(self, config: CollectorConfig | None = None) -> None:
        self._config = config or CollectorConfig()
        self._session = httpx.Client(headers=HEADERS, timeout=30.0)

    def _delay(self) -> None:
        delay = random.uniform(
            self._config.request_delay_min,
            self._config.request_delay_max,
        )
        if delay > 0:
            time.sleep(delay)

    def fetch_holdings(
        self, etf_code: str, target_date: date
    ) -> list[HoldingRecord]:
        """Fetch ETF portfolio holdings for a given date."""
        date_str = target_date.strftime("%Y%m%d")
        logger.info("Fetching holdings for %s on %s", etf_code, target_date)

        self._delay()
        response = self._session.post(
            KRX_BASE_URL,
            data={
                "bld": "dbms/MDC/STAT/standard/MDCSTAT04601",
                "tboxisuCd_finder_secuprodisu1_0": etf_code,
                "isuCd": etf_code,
                "isuCd2": etf_code,
                "codeNmisuCd_finder_secuprodisu1_0": "",
                "param1isuCd_finder_secuprodisu1_0": "",
                "trdDd": date_str,
                "share": "1",
                "money": "1",
                "csvxls_is498": "false",
            },
        )
        response.raise_for_status()
        raw = response.json()
        return parse_holdings_response(raw, etf_code, date_str)

    def fetch_flow(
        self, etf_code: str, target_date: date
    ) -> list[FlowRecord]:
        """Fetch ETF creation/redemption flow for a given date."""
        date_str = target_date.strftime("%Y%m%d")
        logger.info("Fetching flow for %s on %s", etf_code, target_date)

        self._delay()
        response = self._session.post(
            KRX_BASE_URL,
            data={
                "bld": "dbms/MDC/STAT/standard/MDCSTAT04501",
                "tboxisuCd_finder_secuprodisu1_0": etf_code,
                "isuCd": etf_code,
                "isuCd2": etf_code,
                "codeNmisuCd_finder_secuprodisu1_0": "",
                "param1isuCd_finder_secuprodisu1_0": "",
                "strtDd": date_str,
                "endDd": date_str,
                "share": "1",
                "money": "1",
                "csvxls_isNo": "false",
            },
        )
        response.raise_for_status()
        raw = response.json()
        return parse_flow_response(raw, etf_code)

    def close(self) -> None:
        self._session.close()
```

> **Note:** The exact KRX POST parameter names (`tboxisuCd_finder_secuprodisu1_0`, `bld`, etc.) must be verified against live KRX requests using browser DevTools. The parameter names above are based on known KRX patterns but may need adjustment during live testing. Mark this with `@pytest.mark.slow` test for manual verification.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_collector/test_krx.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add etf_analyzer/collector/krx.py tests/test_collector/test_krx.py
git commit -m "feat: add KRX HTTP client with rate limiting and error handling"
```

---

### Task 7: Analyzer — Flow Score

**Files:**
- Create: `etf_analyzer/analyzer/flow.py`
- Create: `tests/test_analyzer/__init__.py` (already created in Task 1)
- Create: `tests/test_analyzer/test_flow.py`

- [ ] **Step 1: Write the failing test**

`tests/test_analyzer/test_flow.py`:

```python
from datetime import date

from etf_analyzer.storage.repository import upsert_holdings, upsert_flow
from etf_analyzer.analyzer.flow import calculate_flow_scores


def _seed_data(db):
    """Seed test data: 2 ETFs, 2 stocks, 2 days of holdings + flow."""
    # Day 1 holdings
    upsert_holdings(db, [
        {"date": date(2026, 3, 17), "etf_code": "069500", "stock_code": "005930",
         "stock_name": "삼성전자", "weight": 25.0, "shares": 1000},
        {"date": date(2026, 3, 17), "etf_code": "069500", "stock_code": "000660",
         "stock_name": "SK하이닉스", "weight": 10.0, "shares": 500},
        {"date": date(2026, 3, 17), "etf_code": "102110", "stock_code": "005930",
         "stock_name": "삼성전자", "weight": 24.0, "shares": 900},
    ])
    # Day 2 holdings
    upsert_holdings(db, [
        {"date": date(2026, 3, 18), "etf_code": "069500", "stock_code": "005930",
         "stock_name": "삼성전자", "weight": 26.0, "shares": 1100},
        {"date": date(2026, 3, 18), "etf_code": "069500", "stock_code": "000660",
         "stock_name": "SK하이닉스", "weight": 10.0, "shares": 500},
        {"date": date(2026, 3, 18), "etf_code": "102110", "stock_code": "005930",
         "stock_name": "삼성전자", "weight": 25.0, "shares": 950},
    ])
    # Flow data
    upsert_flow(db, [
        {"date": date(2026, 3, 17), "etf_code": "069500",
         "creation_units": 100, "redemption_units": 50, "net_units": 50, "nav": 35000.0},
        {"date": date(2026, 3, 18), "etf_code": "069500",
         "creation_units": 200, "redemption_units": 80, "net_units": 120, "nav": 35500.0},
        {"date": date(2026, 3, 17), "etf_code": "102110",
         "creation_units": 80, "redemption_units": 80, "net_units": 0, "nav": 34000.0},
        {"date": date(2026, 3, 18), "etf_code": "102110",
         "creation_units": 60, "redemption_units": 30, "net_units": 30, "nav": 34500.0},
    ])


def test_flow_score_basic(db):
    _seed_data(db)
    scores = calculate_flow_scores(db, window=1, base_date=date(2026, 3, 18))
    # 삼성전자 should have flow from both ETFs
    samsung = next(s for s in scores if s["stock_code"] == "005930")
    assert samsung["flow_score"] > 0
    # SK하이닉스 flow only from 069500
    sk = next(s for s in scores if s["stock_code"] == "000660")
    assert sk["flow_score"] > 0


def test_flow_score_zero_net(db):
    """ETF with net_units=0 contributes zero flow."""
    _seed_data(db)
    scores = calculate_flow_scores(db, window=1, base_date=date(2026, 3, 17))
    # On day 17, 102110 has net_units=0, so only 069500 contributes to 삼성전자
    samsung = next(s for s in scores if s["stock_code"] == "005930")
    # flow = 069500: 50 * 35000 * 0.25 + 102110: 0 * 34000 * 0.24
    expected_069500 = 50 * 35000.0 * (25.0 / 100)
    assert abs(samsung["flow_score"] - expected_069500) < 1.0


def test_flow_score_returns_sorted(db):
    """Results should be sorted by flow_score descending."""
    _seed_data(db)
    scores = calculate_flow_scores(db, window=1, base_date=date(2026, 3, 18))
    flow_values = [s["flow_score"] for s in scores]
    assert flow_values == sorted(flow_values, reverse=True)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_analyzer/test_flow.py -v`
Expected: FAIL — `ImportError`

- [ ] **Step 3: Implement flow.py**

`etf_analyzer/analyzer/flow.py`:

```python
from __future__ import annotations

from datetime import date, timedelta

import duckdb


def calculate_flow_scores(
    conn: duckdb.DuckDBPyConnection,
    window: int,
    base_date: date | None = None,
) -> list[dict]:
    """
    Calculate flow scores for all stocks.

    flow_score(stock) = Σ over ETFs [
        ETF's cumulative net_amount over window × stock's avg weight in that ETF
    ]

    net_amount = net_units × nav
    """
    if base_date is None:
        result = conn.execute("SELECT MAX(date) FROM etf_holdings").fetchone()
        if result[0] is None:
            return []
        base_date = result[0]

    start_date = base_date - timedelta(days=window)

    rows = conn.execute("""
        WITH flow_amounts AS (
            SELECT
                etf_code,
                SUM(net_units * nav) AS total_net_amount
            FROM etf_flow
            WHERE date > ? AND date <= ?
            GROUP BY etf_code
        ),
        avg_weights AS (
            SELECT
                etf_code,
                stock_code,
                stock_name,
                AVG(weight) AS avg_weight
            FROM etf_holdings
            WHERE date > ? AND date <= ?
            GROUP BY etf_code, stock_code, stock_name
        )
        SELECT
            w.stock_code,
            w.stock_name,
            SUM(f.total_net_amount * w.avg_weight / 100.0) AS flow_score
        FROM avg_weights w
        JOIN flow_amounts f ON w.etf_code = f.etf_code
        GROUP BY w.stock_code, w.stock_name
        ORDER BY flow_score DESC
    """, [start_date, base_date, start_date, base_date]).fetchall()

    return [
        {"stock_code": row[0], "stock_name": row[1], "flow_score": row[2]}
        for row in rows
    ]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_analyzer/test_flow.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add etf_analyzer/analyzer/flow.py tests/test_analyzer/test_flow.py
git commit -m "feat: add flow score calculation engine"
```

---

### Task 8: Analyzer — Conviction Score

**Files:**
- Create: `etf_analyzer/analyzer/conviction.py`
- Create: `tests/test_analyzer/test_conviction.py`

- [ ] **Step 1: Write the failing test**

`tests/test_analyzer/test_conviction.py`:

```python
from datetime import date

from etf_analyzer.storage.repository import upsert_holdings, upsert_etf_master
from etf_analyzer.analyzer.conviction import calculate_conviction_scores


def _seed_data(db):
    """Seed: 1 active + 2 passive ETFs, weight changes over 2 days."""
    # Register ETF types
    upsert_etf_master(db, "069500", "KODEX 200", "삼성", "passive", "KOSPI200")
    upsert_etf_master(db, "102110", "TIGER 200", "미래에셋", "passive", "KOSPI200")
    upsert_etf_master(db, "441800", "TIMEFOLIO", "타임폴리오", "active", None)

    # Day 1: baseline weights
    upsert_holdings(db, [
        {"date": date(2026, 3, 13), "etf_code": "069500", "stock_code": "005930",
         "stock_name": "삼성전자", "weight": 25.0, "shares": 1000},
        {"date": date(2026, 3, 13), "etf_code": "102110", "stock_code": "005930",
         "stock_name": "삼성전자", "weight": 24.0, "shares": 900},
        {"date": date(2026, 3, 13), "etf_code": "441800", "stock_code": "005930",
         "stock_name": "삼성전자", "weight": 5.0, "shares": 200},
        {"date": date(2026, 3, 13), "etf_code": "069500", "stock_code": "000660",
         "stock_name": "SK하이닉스", "weight": 10.0, "shares": 500},
    ])
    # Day 2: 069500 increased, 102110 decreased, 441800 (active) increased
    upsert_holdings(db, [
        {"date": date(2026, 3, 18), "etf_code": "069500", "stock_code": "005930",
         "stock_name": "삼성전자", "weight": 27.0, "shares": 1100},
        {"date": date(2026, 3, 18), "etf_code": "102110", "stock_code": "005930",
         "stock_name": "삼성전자", "weight": 23.0, "shares": 850},
        {"date": date(2026, 3, 18), "etf_code": "441800", "stock_code": "005930",
         "stock_name": "삼성전자", "weight": 8.0, "shares": 350},
        {"date": date(2026, 3, 18), "etf_code": "069500", "stock_code": "000660",
         "stock_name": "SK하이닉스", "weight": 10.0, "shares": 500},
    ])


def test_conviction_breadth(db):
    """breadth = ETFs that increased / total ETFs holding stock."""
    _seed_data(db)
    scores = calculate_conviction_scores(
        db, window=5, base_date=date(2026, 3, 18), active_multiplier=2.0
    )
    samsung = next(s for s in scores if s["stock_code"] == "005930")
    # 069500 increased (+2), 102110 decreased (-1), 441800 increased (+3)
    # breadth = 2/3
    assert abs(samsung["breadth"] - 2 / 3) < 0.01


def test_conviction_depth_active_weighted(db):
    """depth should weight active ETFs more heavily."""
    _seed_data(db)
    scores = calculate_conviction_scores(
        db, window=5, base_date=date(2026, 3, 18), active_multiplier=2.0
    )
    samsung = next(s for s in scores if s["stock_code"] == "005930")
    # Only ETFs that increased: 069500 (+2.0, passive, mult=1.0), 441800 (+3.0, active, mult=2.0)
    # depth = (2.0*1.0 + 3.0*2.0) / (1.0 + 2.0) = 8.0/3.0 ≈ 2.667
    expected_depth = (2.0 * 1.0 + 3.0 * 2.0) / (1.0 + 2.0)
    assert abs(samsung["depth"] - expected_depth) < 0.01


def test_conviction_no_change_stock(db):
    """Stock with no weight change should have conviction_score = 0."""
    _seed_data(db)
    scores = calculate_conviction_scores(
        db, window=5, base_date=date(2026, 3, 18), active_multiplier=2.0
    )
    sk = next((s for s in scores if s["stock_code"] == "000660"), None)
    # SK하이닉스: weight unchanged (10.0 → 10.0), breadth = 0/1 = 0
    if sk:
        assert sk["conviction_score"] == 0.0


def test_conviction_new_stock_treated_as_zero(db):
    """Stock not held in start_date should use 0% as prior weight."""
    # Add a stock only on day 2 (new entry)
    upsert_holdings(db, [
        {"date": date(2026, 3, 18), "etf_code": "441800", "stock_code": "035720",
         "stock_name": "카카오", "weight": 3.0, "shares": 100},
    ])
    _seed_data(db)
    scores = calculate_conviction_scores(
        db, window=5, base_date=date(2026, 3, 18), active_multiplier=2.0
    )
    kakao = next((s for s in scores if s["stock_code"] == "035720"), None)
    assert kakao is not None
    assert kakao["breadth"] == 1.0  # 1/1 ETF increased
    assert kakao["depth"] == 3.0   # +3.0%p, active multiplier in weighted avg = 3.0*2.0/2.0 = 3.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_analyzer/test_conviction.py -v`
Expected: FAIL — `ImportError`

- [ ] **Step 3: Implement conviction.py**

`etf_analyzer/analyzer/conviction.py`:

```python
from __future__ import annotations

from datetime import date, timedelta

import duckdb


def calculate_conviction_scores(
    conn: duckdb.DuckDBPyConnection,
    window: int,
    base_date: date | None = None,
    active_multiplier: float = 2.0,
) -> list[dict]:
    """
    Calculate conviction scores for all stocks.

    breadth = ETFs that increased weight / total ETFs holding stock
    depth = weighted avg of weight increases (active ETFs get higher multiplier)
    conviction_score = breadth × depth
    """
    if base_date is None:
        result = conn.execute("SELECT MAX(date) FROM etf_holdings").fetchone()
        if result[0] is None:
            return []
        base_date = result[0]

    start_date = base_date - timedelta(days=window)

    rows = conn.execute("""
        WITH end_weights AS (
            SELECT etf_code, stock_code, stock_name, weight
            FROM etf_holdings
            WHERE date = ?
        ),
        start_weights AS (
            SELECT etf_code, stock_code, weight
            FROM etf_holdings
            WHERE date = (
                SELECT MAX(date) FROM etf_holdings WHERE date <= ?
            )
        ),
        weight_changes AS (
            SELECT
                e.etf_code,
                e.stock_code,
                e.stock_name,
                e.weight - COALESCE(s.weight, 0) AS weight_change,
                CASE
                    WHEN m.etf_type = 'active' THEN ?
                    ELSE 1.0
                END AS multiplier
            FROM end_weights e
            LEFT JOIN start_weights s
                ON e.etf_code = s.etf_code AND e.stock_code = s.stock_code
            LEFT JOIN etf_master m ON e.etf_code = m.etf_code
        )
        SELECT
            stock_code,
            stock_name,
            COUNT(*) AS total_etfs,
            SUM(CASE WHEN weight_change > 0 THEN 1 ELSE 0 END) AS increased_etfs,
            SUM(CASE WHEN weight_change > 0 THEN weight_change * multiplier ELSE 0 END) AS weighted_increase_sum,
            SUM(CASE WHEN weight_change > 0 THEN multiplier ELSE 0 END) AS multiplier_sum
        FROM weight_changes
        GROUP BY stock_code, stock_name
    """, [base_date, start_date, active_multiplier]).fetchall()

    results = []
    for row in rows:
        stock_code, stock_name, total_etfs, increased_etfs, weighted_sum, mult_sum = row
        breadth = increased_etfs / total_etfs if total_etfs > 0 else 0.0
        depth = weighted_sum / mult_sum if mult_sum > 0 else 0.0
        conviction_score = breadth * depth
        results.append({
            "stock_code": stock_code,
            "stock_name": stock_name,
            "breadth": breadth,
            "depth": depth,
            "conviction_score": conviction_score,
        })

    results.sort(key=lambda x: x["conviction_score"], reverse=True)
    return results
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_analyzer/test_conviction.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add etf_analyzer/analyzer/conviction.py tests/test_analyzer/test_conviction.py
git commit -m "feat: add conviction score calculation with active ETF weighting"
```

---

### Task 9: Analyzer — Combined Ranking

**Files:**
- Create: `etf_analyzer/analyzer/ranking.py`
- Create: `tests/test_analyzer/test_ranking.py`

- [ ] **Step 1: Write the failing test**

`tests/test_analyzer/test_ranking.py`:

```python
from datetime import date

from etf_analyzer.storage.repository import (
    upsert_holdings, upsert_flow, upsert_etf_master,
)
from etf_analyzer.analyzer.ranking import (
    calculate_combined_ranking,
    get_stock_detail,
    percentile_rank,
)


def test_percentile_rank():
    values = [10.0, 20.0, 30.0, 40.0, 50.0]
    ranked = percentile_rank(values)
    assert ranked[0] == 0.0    # smallest
    assert ranked[4] == 100.0  # largest
    assert ranked[2] == 50.0   # median


def test_percentile_rank_single():
    assert percentile_rank([42.0]) == [50.0]


def test_percentile_rank_empty():
    assert percentile_rank([]) == []


def _seed_full_data(db):
    """Seed comprehensive data for ranking tests."""
    upsert_etf_master(db, "069500", "KODEX 200", "삼성", "passive", "KOSPI200")
    upsert_etf_master(db, "441800", "TIMEFOLIO", "타임폴리오", "active", None)

    for d, w1, w2, w3 in [
        (date(2026, 3, 13), 25.0, 10.0, 5.0),
        (date(2026, 3, 18), 27.0, 10.0, 8.0),
    ]:
        upsert_holdings(db, [
            {"date": d, "etf_code": "069500", "stock_code": "005930",
             "stock_name": "삼성전자", "weight": w1, "shares": 1000},
            {"date": d, "etf_code": "069500", "stock_code": "000660",
             "stock_name": "SK하이닉스", "weight": w2, "shares": 500},
            {"date": d, "etf_code": "441800", "stock_code": "005930",
             "stock_name": "삼성전자", "weight": w3, "shares": 200},
        ])

    upsert_flow(db, [
        {"date": date(2026, 3, 18), "etf_code": "069500",
         "creation_units": 100, "redemption_units": 50, "net_units": 50, "nav": 35000.0},
        {"date": date(2026, 3, 18), "etf_code": "441800",
         "creation_units": 30, "redemption_units": 10, "net_units": 20, "nav": 15000.0},
    ])


def test_combined_ranking(db):
    _seed_full_data(db)
    ranking = calculate_combined_ranking(
        db, window=5, base_date=date(2026, 3, 18),
        flow_weight=0.4, conviction_weight=0.6, active_multiplier=2.0,
    )
    assert len(ranking) >= 2
    # 삼성전자 should rank higher (both flow and conviction signals)
    assert ranking[0]["stock_code"] == "005930"
    assert "combined_score" in ranking[0]
    assert "flow_score" in ranking[0]
    assert "conviction_score" in ranking[0]


def test_get_stock_detail(db):
    _seed_full_data(db)
    detail = get_stock_detail(
        db, stock_code="005930", window=5, base_date=date(2026, 3, 18),
        active_multiplier=2.0,
    )
    assert detail["stock_code"] == "005930"
    assert detail["stock_name"] == "삼성전자"
    assert "etf_breakdown" in detail
    assert len(detail["etf_breakdown"]) > 0
    # Each ETF breakdown should have flow and weight change info
    for etf in detail["etf_breakdown"]:
        assert "etf_code" in etf
        assert "weight_change" in etf
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_analyzer/test_ranking.py -v`
Expected: FAIL — `ImportError`

- [ ] **Step 3: Implement ranking.py**

`etf_analyzer/analyzer/ranking.py`:

```python
from __future__ import annotations

from datetime import date, timedelta

import duckdb

from etf_analyzer.analyzer.conviction import calculate_conviction_scores
from etf_analyzer.analyzer.flow import calculate_flow_scores


def percentile_rank(values: list[float]) -> list[float]:
    """Convert raw values to percentile ranks (0-100)."""
    n = len(values)
    if n == 0:
        return []
    if n == 1:
        return [50.0]

    indexed = sorted(enumerate(values), key=lambda x: x[1])
    ranks = [0.0] * n
    for rank_pos, (orig_idx, _) in enumerate(indexed):
        ranks[orig_idx] = (rank_pos / (n - 1)) * 100.0
    return ranks


def calculate_combined_ranking(
    conn: duckdb.DuckDBPyConnection,
    window: int,
    base_date: date | None = None,
    flow_weight: float = 0.4,
    conviction_weight: float = 0.6,
    active_multiplier: float = 2.0,
) -> list[dict]:
    """
    Calculate combined ranking: flow_weight × percentile(flow) + conviction_weight × percentile(conviction).
    """
    flow_scores = calculate_flow_scores(conn, window, base_date)
    conviction_scores = calculate_conviction_scores(conn, window, base_date, active_multiplier)

    if not flow_scores and not conviction_scores:
        return []

    # Build lookup maps
    flow_map = {s["stock_code"]: s for s in flow_scores}
    conv_map = {s["stock_code"]: s for s in conviction_scores}

    # Union of all stock codes
    all_codes = set(flow_map.keys()) | set(conv_map.keys())

    # Collect raw scores
    stocks = []
    raw_flows = []
    raw_convictions = []
    for code in all_codes:
        flow_entry = flow_map.get(code, {})
        conv_entry = conv_map.get(code, {})
        stock_name = flow_entry.get("stock_name") or conv_entry.get("stock_name", "")
        raw_flow = flow_entry.get("flow_score", 0.0)
        raw_conv = conv_entry.get("conviction_score", 0.0)
        stocks.append({
            "stock_code": code,
            "stock_name": stock_name,
            "flow_score": raw_flow,
            "conviction_score": raw_conv,
            "breadth": conv_entry.get("breadth", 0.0),
            "depth": conv_entry.get("depth", 0.0),
        })
        raw_flows.append(raw_flow)
        raw_convictions.append(raw_conv)

    # Percentile normalize
    flow_pcts = percentile_rank(raw_flows)
    conv_pcts = percentile_rank(raw_convictions)

    for i, stock in enumerate(stocks):
        stock["flow_percentile"] = flow_pcts[i]
        stock["conviction_percentile"] = conv_pcts[i]
        stock["combined_score"] = (
            flow_weight * flow_pcts[i] + conviction_weight * conv_pcts[i]
        )

    stocks.sort(key=lambda x: x["combined_score"], reverse=True)
    return stocks


def get_stock_detail(
    conn: duckdb.DuckDBPyConnection,
    stock_code: str,
    window: int,
    base_date: date | None = None,
    active_multiplier: float = 2.0,
) -> dict:
    """Get detailed drill-down for a specific stock."""
    if base_date is None:
        result = conn.execute("SELECT MAX(date) FROM etf_holdings").fetchone()
        if result[0] is None:
            return {}
        base_date = result[0]

    start_date = base_date - timedelta(days=window)

    # Get stock name
    name_row = conn.execute(
        "SELECT stock_name FROM etf_holdings WHERE stock_code = ? LIMIT 1",
        [stock_code],
    ).fetchone()
    stock_name = name_row[0] if name_row else stock_code

    # Per-ETF breakdown
    etf_rows = conn.execute("""
        WITH end_w AS (
            SELECT etf_code, weight
            FROM etf_holdings WHERE date = ? AND stock_code = ?
        ),
        start_w AS (
            SELECT etf_code, weight
            FROM etf_holdings
            WHERE date = (SELECT MAX(date) FROM etf_holdings WHERE date <= ?)
              AND stock_code = ?
        ),
        flow_data AS (
            SELECT etf_code, SUM(net_units * nav) AS net_amount
            FROM etf_flow WHERE date > ? AND date <= ?
            GROUP BY etf_code
        )
        SELECT
            e.etf_code,
            m.etf_name,
            m.etf_type,
            e.weight AS current_weight,
            e.weight - COALESCE(s.weight, 0) AS weight_change,
            COALESCE(f.net_amount, 0) * e.weight / 100.0 AS flow_contribution
        FROM end_w e
        LEFT JOIN start_w s ON e.etf_code = s.etf_code
        LEFT JOIN flow_data f ON e.etf_code = f.etf_code
        LEFT JOIN etf_master m ON e.etf_code = m.etf_code
        ORDER BY flow_contribution DESC
    """, [base_date, stock_code, start_date, stock_code,
          start_date, base_date]).fetchall()

    etf_breakdown = [
        {
            "etf_code": row[0],
            "etf_name": row[1] or row[0],
            "etf_type": row[2] or "passive",
            "current_weight": row[3],
            "weight_change": row[4],
            "flow_contribution": row[5],
        }
        for row in etf_rows
    ]

    # Weight trend over window
    trend_rows = conn.execute("""
        SELECT date, AVG(weight) AS avg_weight
        FROM etf_holdings
        WHERE stock_code = ? AND date > ? AND date <= ?
        GROUP BY date ORDER BY date
    """, [stock_code, start_date, base_date]).fetchall()

    weight_trend = [{"date": row[0], "weight": row[1]} for row in trend_rows]

    return {
        "stock_code": stock_code,
        "stock_name": stock_name,
        "window": window,
        "base_date": base_date,
        "etf_breakdown": etf_breakdown,
        "weight_trend": weight_trend,
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_analyzer/test_ranking.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add etf_analyzer/analyzer/ranking.py tests/test_analyzer/test_ranking.py
git commit -m "feat: add combined ranking with percentile normalization and drill-down"
```

---

### Task 10: CLI — init, status, collect Commands

**Files:**
- Modify: `etf_analyzer/cli.py`

- [ ] **Step 1: Implement CLI commands**

`etf_analyzer/cli.py`:

```python
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table
from rich.tree import Tree

from etf_analyzer.config import load_config

app = typer.Typer(name="etf-analyzer", help="Korean ETF flow tracking system")
console = Console()


@app.callback()
def callback() -> None:
    """Korean ETF flow tracking system."""


@app.command()
def init(
    config_path: Optional[Path] = typer.Option(None, "--config", help="Config file path"),
) -> None:
    """Initialize database and register pilot ETFs."""
    from etf_analyzer.storage.db import get_connection
    from etf_analyzer.storage.schema import create_tables
    from etf_analyzer.storage.repository import upsert_etf_master

    config = load_config(config_path)
    conn = get_connection(config.database.path)
    create_tables(conn)

    for etf in config.pilot_etfs:
        upsert_etf_master(conn, etf.code, etf.name, None, etf.type, None)

    conn.close()
    console.print(f"[green]DB initialized at {config.database.path}[/green]")
    console.print(f"[green]Registered {len(config.pilot_etfs)} pilot ETFs[/green]")


@app.command()
def status(
    config_path: Optional[Path] = typer.Option(None, "--config", help="Config file path"),
) -> None:
    """Show collection status."""
    from etf_analyzer.storage.db import get_connection
    from etf_analyzer.storage.repository import get_collection_status

    config = load_config(config_path)
    conn = get_connection(config.database.path)
    s = get_collection_status(conn)
    conn.close()

    if s["total_days"] == 0:
        console.print("[yellow]No data collected yet. Run 'collect' first.[/yellow]")
        return

    table = Table(title="Collection Status")
    table.add_column("Metric", style="bold")
    table.add_column("Value")
    table.add_row("Total days", str(s["total_days"]))
    table.add_row("First date", str(s["first_date"]))
    table.add_row("Last date", str(s["last_date"]))
    table.add_row("ETF count", str(s["etf_count"]))
    console.print(table)


@app.command()
def collect(
    today: bool = typer.Option(False, "--today", help="Collect today's data"),
    date_str: Optional[str] = typer.Option(None, "--date", help="Date to collect (YYYY-MM-DD)"),
    from_str: Optional[str] = typer.Option(None, "--from", help="Start date for backfill"),
    to_str: Optional[str] = typer.Option(None, "--to", help="End date for backfill"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
    config_path: Optional[Path] = typer.Option(None, "--config", help="Config file path"),
) -> None:
    """Collect ETF data from KRX."""
    import time
    from etf_analyzer.collector.krx import KRXClient
    from etf_analyzer.storage.db import get_connection
    from etf_analyzer.storage.schema import create_tables
    from etf_analyzer.storage.repository import upsert_holdings, upsert_flow, upsert_stock_master

    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    config = load_config(config_path)
    conn = get_connection(config.database.path)
    create_tables(conn)

    # Determine dates to collect
    if today:
        dates = [date.today()]
    elif date_str:
        dates = [datetime.strptime(date_str, "%Y-%m-%d").date()]
    elif from_str and to_str:
        start = datetime.strptime(from_str, "%Y-%m-%d").date()
        end = datetime.strptime(to_str, "%Y-%m-%d").date()
        dates = []
        current = start
        while current <= end:
            if current.weekday() < 5:  # Skip weekends
                dates.append(current)
            current += timedelta(days=1)
    else:
        console.print("[red]Specify --today, --date, or --from/--to[/red]")
        raise typer.Exit(1)

    client = KRXClient(config.collector)
    successes = []
    failures = []

    # Batch processing for backfill
    batch_size = config.collector.backfill_batch_size
    for batch_start in range(0, len(dates), batch_size):
        batch = dates[batch_start:batch_start + batch_size]

        for target_date in batch:
            console.print(f"\n[bold]Collecting {target_date}...[/bold]")

            for etf in config.pilot_etfs:
                try:
                    # Fetch holdings
                    holdings = client.fetch_holdings(etf.code, target_date)
                    if holdings:
                        upsert_holdings(
                            conn,
                            [h.model_dump() for h in holdings],
                        )
                        # Auto-populate stock_master
                        for h in holdings:
                            upsert_stock_master(conn, h.stock_code, h.stock_name)
                        console.print(f"  [green]{etf.name}: {len(holdings)} holdings[/green]")
                    else:
                        console.print(f"  [yellow]{etf.name}: no holdings data[/yellow]")

                    # Fetch flow
                    flows = client.fetch_flow(etf.code, target_date)
                    if flows:
                        upsert_flow(conn, [f.model_dump() for f in flows])

                    successes.append((target_date, etf.code))

                except Exception as e:
                    console.print(f"  [red]{etf.name}: FAILED - {e}[/red]")
                    failures.append((target_date, etf.code, str(e)))

                    if "403" in str(e) or "429" in str(e):
                        console.print("[red]Blocked by KRX. Stopping immediately.[/red]")
                        client.close()
                        conn.close()
                        raise typer.Exit(1)

        # Batch delay for backfill
        if batch_start + batch_size < len(dates):
            console.print(f"[dim]Batch delay: {config.collector.backfill_batch_delay}s...[/dim]")
            time.sleep(config.collector.backfill_batch_delay)

    client.close()
    conn.close()

    # Summary
    console.print(f"\n[bold]Done:[/bold] {len(successes)} OK, {len(failures)} failed")
    if failures:
        for d, code, err in failures:
            console.print(f"  [red]FAIL: {d} {code} — {err}[/red]")
```

- [ ] **Step 2: Verify CLI commands work**

Run: `uv run etf-analyzer init`
Expected: "DB initialized at data/etf_analyzer.duckdb" + "Registered 5 pilot ETFs"

Run: `uv run etf-analyzer status`
Expected: "No data collected yet. Run 'collect' first."

Run: `uv run etf-analyzer --help`
Expected: Shows all commands (init, status, collect)

- [ ] **Step 3: Commit**

```bash
git add etf_analyzer/cli.py
git commit -m "feat: add CLI commands for init, status, and collect"
```

---

### Task 11: CLI — rank, detail, etf Commands

**Files:**
- Modify: `etf_analyzer/cli.py`

- [ ] **Step 1: Add rank, detail, and etf commands to cli.py**

Append to `etf_analyzer/cli.py`:

```python
@app.command()
def rank(
    window: str = typer.Option("5", "--window", "-w", help="Analysis window in days (comma-separated for multiple)"),
    top: int = typer.Option(20, "--top", "-n", help="Number of top stocks to show"),
    config_path: Optional[Path] = typer.Option(None, "--config", help="Config file path"),
) -> None:
    """Show stock rankings by combined score."""
    from etf_analyzer.analyzer.ranking import calculate_combined_ranking
    from etf_analyzer.storage.db import get_connection

    config = load_config(config_path)
    conn = get_connection(config.database.path)

    windows = [int(w.strip()) for w in window.split(",")]

    for w in windows:
        ranking = calculate_combined_ranking(
            conn, window=w,
            flow_weight=config.analyzer.flow_weight,
            conviction_weight=config.analyzer.conviction_weight,
            active_multiplier=config.analyzer.active_etf_multiplier,
        )

        if not ranking:
            console.print(f"[yellow]No data for {w}-day window.[/yellow]")
            continue

        table = Table(title=f"Top {top} Stocks ({w}-day window)")
        table.add_column("#", style="dim", width=4)
        table.add_column("Code", style="bold")
        table.add_column("Name")
        table.add_column("Combined", justify="right")
        table.add_column("Flow ₩", justify="right")
        table.add_column("Conviction", justify="right")
        table.add_column("Breadth", justify="right")

        for i, stock in enumerate(ranking[:top], 1):
            flow_str = f"{stock['flow_score']:,.0f}"
            table.add_row(
                str(i),
                stock["stock_code"],
                stock["stock_name"],
                f"{stock['combined_score']:.1f}",
                flow_str,
                f"{stock['conviction_score']:.3f}",
                f"{stock['breadth']:.0%}",
            )

        console.print(table)
        console.print()

    conn.close()


@app.command()
def detail(
    stock_code: str = typer.Argument(help="Stock code to analyze"),
    window: str = typer.Option("5", "--window", "-w", help="Analysis window (comma-separated)"),
    config_path: Optional[Path] = typer.Option(None, "--config", help="Config file path"),
) -> None:
    """Show detailed analysis for a stock."""
    from etf_analyzer.analyzer.ranking import get_stock_detail
    from etf_analyzer.storage.db import get_connection

    config = load_config(config_path)
    conn = get_connection(config.database.path)

    windows = [int(w.strip()) for w in window.split(",")]

    for w in windows:
        d = get_stock_detail(
            conn, stock_code=stock_code, window=w,
            active_multiplier=config.analyzer.active_etf_multiplier,
        )

        if not d:
            console.print(f"[yellow]No data for {stock_code}.[/yellow]")
            continue

        tree = Tree(f"[bold]{d['stock_name']} ({d['stock_code']})[/bold] — {w}일 기준")

        # Flow breakdown
        total_flow = sum(e["flow_contribution"] for e in d["etf_breakdown"])
        flow_branch = tree.add(f"자금유입: {total_flow / 1e8:+,.1f}억원")
        for etf in d["etf_breakdown"]:
            marker = " ★" if etf["etf_type"] == "active" else ""
            flow_branch.add(
                f"{etf['etf_name']}: {etf['flow_contribution'] / 1e8:+,.1f}억 "
                f"(비중 {etf['current_weight']:.1f}%){marker}"
            )

        # Conviction breakdown
        conv_branch = tree.add("확신도")
        for etf in d["etf_breakdown"]:
            direction = "+" if etf["weight_change"] > 0 else ""
            marker = " ★ (액티브)" if etf["etf_type"] == "active" else ""
            conv_branch.add(
                f"{etf['etf_name']}: {direction}{etf['weight_change']:.1f}%p{marker}"
            )

        # Weight trend
        if d["weight_trend"]:
            trend_str = " → ".join(f"{t['weight']:.1f}%" for t in d["weight_trend"])
            tree.add(f"비중 추이: {trend_str}")

        console.print(tree)
        console.print()

    conn.close()


@app.command()
def etf(
    etf_code: str = typer.Argument(help="ETF code to analyze"),
    window: str = typer.Option("5", "--window", "-w", help="Analysis window"),
    top: int = typer.Option(10, "--top", "-n", help="Number of top holdings to show"),
    config_path: Optional[Path] = typer.Option(None, "--config", help="Config file path"),
) -> None:
    """Show ETF flow summary and top holdings changes."""
    from etf_analyzer.storage.db import get_connection
    from etf_analyzer.storage.repository import get_flow

    config = load_config(config_path)
    conn = get_connection(config.database.path)
    w = int(window)

    # ETF info
    etf_info = conn.execute(
        "SELECT etf_name, etf_type FROM etf_master WHERE etf_code = ?", [etf_code]
    ).fetchone()
    if not etf_info:
        console.print(f"[red]ETF {etf_code} not found.[/red]")
        conn.close()
        raise typer.Exit(1)

    console.print(f"\n[bold]{etf_info[0]} ({etf_code})[/bold] — {etf_info[1]}")

    # Flow summary
    base_date = conn.execute("SELECT MAX(date) FROM etf_holdings").fetchone()[0]
    if base_date is None:
        console.print("[yellow]No data.[/yellow]")
        conn.close()
        return

    from datetime import timedelta
    start_date = base_date - timedelta(days=w)
    flow_data = get_flow(conn, start_date, base_date, etf_code)
    total_net = sum(f["net_units"] * f["nav"] for f in flow_data)
    console.print(f"순자금유입 ({w}일): [bold]{total_net / 1e8:+,.1f}억원[/bold]")

    # Top weight changes
    changes = conn.execute("""
        WITH end_w AS (
            SELECT stock_code, stock_name, weight
            FROM etf_holdings WHERE date = ? AND etf_code = ?
        ),
        start_w AS (
            SELECT stock_code, weight
            FROM etf_holdings
            WHERE date = (SELECT MAX(date) FROM etf_holdings WHERE date <= ? AND etf_code = ?)
              AND etf_code = ?
        )
        SELECT e.stock_code, e.stock_name, e.weight,
               e.weight - COALESCE(s.weight, 0) AS weight_change
        FROM end_w e LEFT JOIN start_w s ON e.stock_code = s.stock_code
        ORDER BY ABS(e.weight - COALESCE(s.weight, 0)) DESC
        LIMIT ?
    """, [base_date, etf_code, start_date, etf_code, etf_code, top]).fetchall()

    table = Table(title=f"Top {top} Weight Changes ({w}d)")
    table.add_column("Code")
    table.add_column("Name")
    table.add_column("Weight", justify="right")
    table.add_column("Change", justify="right")

    for row in changes:
        change_str = f"{row[3]:+.2f}%p"
        style = "green" if row[3] > 0 else "red" if row[3] < 0 else "dim"
        table.add_row(row[0], row[1], f"{row[2]:.2f}%", f"[{style}]{change_str}[/{style}]")

    console.print(table)
    conn.close()
```

- [ ] **Step 2: Verify CLI commands are registered**

Run: `uv run etf-analyzer --help`
Expected: Shows all commands: collect, detail, etf, init, rank, status

- [ ] **Step 3: Commit**

```bash
git add etf_analyzer/cli.py
git commit -m "feat: add rank, detail, and etf CLI commands"
```

---

### Task 12: Integration Test

**Files:**
- Create: `tests/test_integration.py`

- [ ] **Step 1: Write the integration test**

`tests/test_integration.py`:

```python
"""End-to-end pipeline test: seed data → analyze → rank → drill-down."""
from datetime import date

from etf_analyzer.storage.repository import (
    upsert_etf_master, upsert_holdings, upsert_flow,
)
from etf_analyzer.analyzer.ranking import calculate_combined_ranking, get_stock_detail


def _seed_realistic_data(db):
    """Seed 5 days of data for 2 ETFs with 3 stocks."""
    upsert_etf_master(db, "069500", "KODEX 200", "삼성", "passive", "KOSPI200")
    upsert_etf_master(db, "441800", "TIMEFOLIO", "타임폴리오", "active", None)

    # 5 days of holdings data
    days_data = [
        # (date, etf, stock, name, weight, shares)
        # KODEX 200 — 삼성전자 weight increasing
        (date(2026, 3, 14), "069500", "005930", "삼성전자", 24.0, 1000),
        (date(2026, 3, 15), "069500", "005930", "삼성전자", 24.5, 1020),
        (date(2026, 3, 16), "069500", "005930", "삼성전자", 25.0, 1040),
        (date(2026, 3, 17), "069500", "005930", "삼성전자", 25.5, 1060),
        (date(2026, 3, 18), "069500", "005930", "삼성전자", 26.0, 1080),
        # KODEX 200 — SK하이닉스 weight stable
        (date(2026, 3, 14), "069500", "000660", "SK하이닉스", 10.0, 500),
        (date(2026, 3, 15), "069500", "000660", "SK하이닉스", 10.0, 500),
        (date(2026, 3, 16), "069500", "000660", "SK하이닉스", 10.1, 505),
        (date(2026, 3, 17), "069500", "000660", "SK하이닉스", 10.0, 500),
        (date(2026, 3, 18), "069500", "000660", "SK하이닉스", 10.0, 500),
        # TIMEFOLIO — 삼성전자 weight increasing strongly (active signal)
        (date(2026, 3, 14), "441800", "005930", "삼성전자", 4.0, 150),
        (date(2026, 3, 15), "441800", "005930", "삼성전자", 5.0, 180),
        (date(2026, 3, 16), "441800", "005930", "삼성전자", 6.0, 220),
        (date(2026, 3, 17), "441800", "005930", "삼성전자", 7.0, 260),
        (date(2026, 3, 18), "441800", "005930", "삼성전자", 8.0, 300),
        # TIMEFOLIO — 카카오 newly added on day 3
        (date(2026, 3, 16), "441800", "035720", "카카오", 2.0, 50),
        (date(2026, 3, 17), "441800", "035720", "카카오", 3.0, 80),
        (date(2026, 3, 18), "441800", "035720", "카카오", 4.0, 110),
    ]
    upsert_holdings(db, [
        {"date": d, "etf_code": e, "stock_code": s, "stock_name": n, "weight": w, "shares": sh}
        for d, e, s, n, w, sh in days_data
    ])

    # Flow data: KODEX 200 has positive inflow, TIMEFOLIO has mixed
    flow_data = [
        (date(2026, 3, 14), "069500", 100, 50, 50, 35000.0),
        (date(2026, 3, 15), "069500", 120, 40, 80, 35200.0),
        (date(2026, 3, 16), "069500", 90, 60, 30, 35100.0),
        (date(2026, 3, 17), "069500", 150, 70, 80, 35400.0),
        (date(2026, 3, 18), "069500", 200, 50, 150, 35600.0),
        (date(2026, 3, 14), "441800", 30, 20, 10, 15000.0),
        (date(2026, 3, 15), "441800", 20, 25, -5, 15200.0),
        (date(2026, 3, 16), "441800", 40, 10, 30, 15100.0),
        (date(2026, 3, 17), "441800", 15, 15, 0, 15300.0),
        (date(2026, 3, 18), "441800", 50, 20, 30, 15500.0),
    ]
    upsert_flow(db, [
        {"date": d, "etf_code": e, "creation_units": c, "redemption_units": r,
         "net_units": n, "nav": nav}
        for d, e, c, r, n, nav in flow_data
    ])


def test_full_pipeline_5day(db):
    """Full pipeline: 5-day window ranking should produce valid results."""
    _seed_realistic_data(db)

    ranking = calculate_combined_ranking(
        db, window=5, base_date=date(2026, 3, 18),
        flow_weight=0.4, conviction_weight=0.6, active_multiplier=2.0,
    )

    assert len(ranking) >= 3  # 삼성전자, SK하이닉스, 카카오

    # 삼성전자 should be #1: both flow and conviction signals strong
    assert ranking[0]["stock_code"] == "005930"
    assert ranking[0]["combined_score"] > 0

    # All scores should be in valid ranges
    for stock in ranking:
        assert 0 <= stock["combined_score"] <= 100
        assert 0 <= stock["flow_percentile"] <= 100
        assert 0 <= stock["conviction_percentile"] <= 100


def test_drill_down_detail(db):
    """Drill-down should show per-ETF breakdown."""
    _seed_realistic_data(db)

    detail = get_stock_detail(
        db, stock_code="005930", window=5, base_date=date(2026, 3, 18),
        active_multiplier=2.0,
    )

    assert detail["stock_name"] == "삼성전자"
    assert len(detail["etf_breakdown"]) == 2  # KODEX 200 + TIMEFOLIO

    # TIMEFOLIO (active) should show larger weight change
    timefolio = next(e for e in detail["etf_breakdown"] if e["etf_code"] == "441800")
    assert timefolio["weight_change"] > 0
    assert timefolio["etf_type"] == "active"

    # Weight trend should have 5 data points
    assert len(detail["weight_trend"]) == 5


def test_1day_window(db):
    """1-day window should capture only the latest day's changes."""
    _seed_realistic_data(db)

    ranking = calculate_combined_ranking(
        db, window=1, base_date=date(2026, 3, 18),
        flow_weight=0.4, conviction_weight=0.6, active_multiplier=2.0,
    )

    assert len(ranking) >= 1
    # All stocks should have valid scores
    for stock in ranking:
        assert "combined_score" in stock
```

- [ ] **Step 2: Run integration tests**

Run: `uv run pytest tests/test_integration.py -v`
Expected: 3 passed

- [ ] **Step 3: Run all tests to ensure nothing is broken**

Run: `uv run pytest -v`
Expected: All tests pass (approx 25+ tests)

- [ ] **Step 4: Commit**

```bash
git add tests/test_integration.py
git commit -m "feat: add integration tests for full analysis pipeline"
```

---

### Task 13: Live KRX Verification

**Files:**
- Modify: `etf_analyzer/collector/krx.py` (if API format differs)
- Modify: `etf_analyzer/collector/models.py` (if field names differ)
- Modify: `tests/fixtures/krx_holdings.json` (update to real format)
- Modify: `tests/fixtures/krx_flow.json` (update to real format)

> **This task requires live network access to KRX.** It verifies the collector works against the real API and adjusts field names if needed.

- [ ] **Step 1: Run init to set up DB**

Run: `uv run etf-analyzer init`
Expected: DB created, 5 ETFs registered

- [ ] **Step 2: Test collecting a single recent date**

Run: `uv run etf-analyzer collect --date 2026-03-18 -v`

Watch the output:
- If holdings data appears → field names are correct
- If Pydantic validation errors appear → note the actual KRX field names from the debug log

- [ ] **Step 3: If field names differ, update models.py and fixtures**

Open browser DevTools on `data.krx.co.kr`, navigate to ETF portfolio page, inspect the actual JSON response field names. Update `models.py` parse functions and fixture files to match.

- [ ] **Step 4: Verify collect + rank work end-to-end**

Run: `uv run etf-analyzer collect --date 2026-03-17`
Run: `uv run etf-analyzer collect --date 2026-03-18`
Run: `uv run etf-analyzer rank --window 1`
Expected: Ranking table with real stock data

Run: `uv run etf-analyzer detail 005930 --window 1`
Expected: Drill-down tree for 삼성전자

- [ ] **Step 5: Update fixtures and re-run tests**

Run: `uv run pytest -v`
Expected: All tests pass with updated fixtures

- [ ] **Step 6: Commit**

```bash
git add -u
git commit -m "fix: align KRX API field names with live responses"
```

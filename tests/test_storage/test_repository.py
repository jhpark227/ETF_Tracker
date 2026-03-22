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
    upsert_stock_master(db, "005930", "삼성전자")
    result = db.execute("SELECT * FROM stock_master WHERE stock_code = '005930'").fetchone()
    assert result[1] == "삼성전자"


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

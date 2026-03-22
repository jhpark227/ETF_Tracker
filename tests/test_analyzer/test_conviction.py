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
    """breadth = weighted ETFs that increased / total weighted ETFs."""
    _seed_data(db)
    scores = calculate_conviction_scores(
        db, window=5, base_date=date(2026, 3, 18), active_multiplier=2.0
    )
    samsung = next(s for s in scores if s["stock_code"] == "005930")
    # 069500 increased (+2, passive, mult=1.0), 102110 decreased (-1, passive, mult=1.0),
    # 441800 increased (+3, active, mult=2.0)
    # increased_weighted = 1.0 + 2.0 = 3.0, total_weighted = 1.0 + 1.0 + 2.0 = 4.0
    # breadth = 3.0 / 4.0 = 0.75
    assert abs(samsung["breadth"] - 0.75) < 0.01


def test_conviction_depth_active_weighted(db):
    """depth is sum of weight_change × multiplier (not average)."""
    _seed_data(db)
    scores = calculate_conviction_scores(
        db, window=5, base_date=date(2026, 3, 18), active_multiplier=2.0
    )
    samsung = next(s for s in scores if s["stock_code"] == "005930")
    # Only ETFs that increased: 069500 (+2.0, passive, mult=1.0), 441800 (+3.0, active, mult=2.0)
    # depth = 2.0*1.0 + 3.0*2.0 = 8.0
    assert abs(samsung["depth"] - 8.0) < 0.01


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
    # +3.0%p, active mult=2.0, new entry bonus=3.0 → mult=2.0*3.0=6.0
    # depth = 3.0 * 6.0 = 18.0
    assert abs(kakao["depth"] - 18.0) < 0.01
    assert kakao["new_entries"] == 1  # detected as new entry

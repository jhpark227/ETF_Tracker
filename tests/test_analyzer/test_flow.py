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

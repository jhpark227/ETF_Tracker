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

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
    )
    assert detail["stock_code"] == "005930"
    assert detail["stock_name"] == "삼성전자"
    assert "etf_breakdown" in detail
    assert len(detail["etf_breakdown"]) > 0
    # Each ETF breakdown should have flow and weight change info
    for etf in detail["etf_breakdown"]:
        assert "etf_code" in etf
        assert "weight_change" in etf

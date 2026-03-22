import json
from pathlib import Path

from etf_analyzer.collector.models import (
    parse_holdings_response,
    parse_flow_from_price_history,
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


def test_parse_flow_from_price_history():
    raw = json.loads((FIXTURES / "krx_flow.json").read_text())
    records = parse_flow_from_price_history(raw["output"], etf_code="069500")
    assert len(records) == 1
    assert isinstance(records[0], FlowRecord)
    assert records[0].nav == 35200.0
    # 19,850,000 - 19,800,000 = 50,000 net shares created
    assert records[0].net_units == 50000
    assert records[0].creation_units == 50000
    assert records[0].redemption_units == 0


def test_parse_flow_negative():
    """Net redemption should produce negative net_units."""
    items = [
        {"TRD_DD": "2026/03/17", "LIST_SHRS": "20,000,000", "LST_NAV": "35,000.00"},
        {"TRD_DD": "2026/03/18", "LIST_SHRS": "19,800,000", "LST_NAV": "35,200.00"},
    ]
    records = parse_flow_from_price_history(items, etf_code="069500")
    assert len(records) == 1
    assert records[0].net_units == -200000
    assert records[0].creation_units == 0
    assert records[0].redemption_units == 200000


def test_parse_flow_single_day():
    """Single day of data means no flow can be computed."""
    items = [
        {"TRD_DD": "2026/03/18", "LIST_SHRS": "19,850,000", "LST_NAV": "35,200.00"},
    ]
    records = parse_flow_from_price_history(items, etf_code="069500")
    assert records == []


def test_parse_holdings_empty_response():
    raw = {"output": []}
    records = parse_holdings_response(raw, etf_code="069500", date_str="20260318")
    assert records == []


def test_parse_number_with_commas():
    """Korean number format: '1,000' -> 1000."""
    raw = {
        "output": [
            {"COMPST_ISU_CD": "005930", "COMPST_ISU_NM": "삼성전자",
             "COMPST_RTO": "1,234.56", "COMPST_ISU_CU1_SHRS": "10,000"}
        ]
    }
    records = parse_holdings_response(raw, etf_code="069500", date_str="20260318")
    assert records[0].weight == 1234.56
    assert records[0].shares == 10000


def test_parse_shares_with_decimals():
    """KRX may return shares with decimals like '8,175.00'."""
    raw = {
        "output": [
            {"COMPST_ISU_CD": "005930", "COMPST_ISU_NM": "삼성전자",
             "COMPST_RTO": "16.53", "COMPST_ISU_CU1_SHRS": "8,175.00"}
        ]
    }
    records = parse_holdings_response(raw, etf_code="069500", date_str="20260318")
    assert records[0].shares == 8175

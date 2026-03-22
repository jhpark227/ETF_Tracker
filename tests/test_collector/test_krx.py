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
        records = client.fetch_holdings("KR7069500007", "069500", date(2026, 3, 18))

    assert len(records) == 2
    assert records[0].stock_code == "005930"

    # Verify correct bld was used
    call_args = mock_session.post.call_args
    assert call_args[1]["data"]["bld"] == "dbms/MDC/STAT/standard/MDCSTAT05001"
    assert call_args[1]["data"]["isuCd"] == "KR7069500007"


def test_fetch_flow():
    config = CollectorConfig(request_delay_min=0, request_delay_max=0)
    client = KRXClient(config)

    with patch.object(client, "_session") as mock_session:
        mock_session.post.return_value = _mock_response("krx_flow.json")
        records = client.fetch_flow("KR7069500007", "069500", date(2026, 3, 18))

    assert len(records) == 1
    assert records[0].net_units == 50000  # 19,850,000 - 19,800,000

    # Verify correct bld was used
    call_args = mock_session.post.call_args
    assert call_args[1]["data"]["bld"] == "dbms/MDC/STAT/standard/MDCSTAT04501"
    assert call_args[1]["data"]["isuCd"] == "KR7069500007"


def test_fetch_isin_map():
    config = CollectorConfig(request_delay_min=0, request_delay_max=0)
    client = KRXClient(config)

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "output": [
            {"ISU_SRT_CD": "069500", "ISU_CD": "KR7069500007", "ISU_ABBRV": "KODEX 200"},
            {"ISU_SRT_CD": "102110", "ISU_CD": "KR7102110004", "ISU_ABBRV": "TIGER 200"},
        ]
    }
    mock_resp.raise_for_status = MagicMock()

    with patch.object(client, "_session") as mock_session:
        mock_session.post.return_value = mock_resp
        isin_map = client.fetch_isin_map()

    assert isin_map["069500"] == "KR7069500007"
    assert isin_map["102110"] == "KR7102110004"


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
            client.fetch_holdings("KR7069500007", "069500", date(2026, 3, 18))
            assert False, "Should have raised"
        except Exception as e:
            assert "403" in str(e)

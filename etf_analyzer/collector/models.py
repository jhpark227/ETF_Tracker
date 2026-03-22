from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel


def _parse_int(value: str) -> int:
    """Parse Korean-formatted integer: '1,000' -> 1000, '8,175.00' -> 8175."""
    return int(float(value.replace(",", "")))


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
    """Parse KRX PDF (holdings) API response into HoldingRecord list."""
    items = raw.get("output", [])
    if not items:
        return []

    parsed_date = datetime.strptime(date_str, "%Y%m%d").date()
    records = []
    for item in items:
        records.append(
            HoldingRecord(
                date=parsed_date,
                etf_code=etf_code,
                stock_code=item["COMPST_ISU_CD"],
                stock_name=item["COMPST_ISU_NM"],
                weight=_parse_float(item["COMPST_RTO"]),
                shares=_parse_int(item["COMPST_ISU_CU1_SHRS"]),
            )
        )
    return records


def parse_flow_from_price_history(
    items: list[dict], etf_code: str
) -> list[FlowRecord]:
    """Derive flow records from consecutive daily listed shares changes.

    KRX doesn't expose creation/redemption directly.
    We compute net flow from changes in LIST_SHRS (total outstanding shares)
    between consecutive trading days.
    """
    if len(items) < 2:
        return []

    # Sort by date ascending (KRX may return reverse chronological)
    sorted_items = sorted(items, key=lambda x: x["TRD_DD"])

    records = []
    for i in range(1, len(sorted_items)):
        today = sorted_items[i]
        yesterday = sorted_items[i - 1]

        today_shares = _parse_int(today["LIST_SHRS"])
        yesterday_shares = _parse_int(yesterday["LIST_SHRS"])
        net_units = today_shares - yesterday_shares

        nav = _parse_float(today["LST_NAV"])
        date_str = today["TRD_DD"].replace("/", "")
        parsed_date = datetime.strptime(date_str, "%Y%m%d").date()

        creation = max(0, net_units)
        redemption = abs(min(0, net_units))

        records.append(
            FlowRecord(
                date=parsed_date,
                etf_code=etf_code,
                creation_units=creation,
                redemption_units=redemption,
                net_units=net_units,
                nav=nav,
            )
        )
    return records

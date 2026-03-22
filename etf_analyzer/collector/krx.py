from __future__ import annotations

import logging
import random
import time
from datetime import date, timedelta

import httpx

from etf_analyzer.collector.models import (
    FlowRecord,
    HoldingRecord,
    parse_flow_from_price_history,
    parse_holdings_response,
)
from etf_analyzer.config import CollectorConfig

logger = logging.getLogger(__name__)

KRX_BASE_URL = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Connection": "keep-alive",
    "Content-Type": "application/x-www-form-urlencoded",
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

    def fetch_isin_map(self) -> dict[str, str]:
        """Fetch all ETF basic info, returning short_code -> ISIN mapping."""
        logger.info("Fetching ETF ISIN map from KRX")
        self._delay()
        response = self._session.post(
            KRX_BASE_URL,
            data={"bld": "dbms/MDC/STAT/standard/MDCSTAT04601"},
        )
        response.raise_for_status()
        raw = response.json()
        mapping: dict[str, str] = {}
        for item in raw.get("output", []):
            mapping[item["ISU_SRT_CD"]] = item["ISU_CD"]
        return mapping

    def fetch_holdings(
        self, isin: str, etf_code: str, target_date: date
    ) -> list[HoldingRecord]:
        """Fetch ETF portfolio holdings (PDF) for a given date."""
        date_str = target_date.strftime("%Y%m%d")
        logger.info("Fetching holdings for %s (%s) on %s", etf_code, isin, target_date)

        self._delay()
        response = self._session.post(
            KRX_BASE_URL,
            data={
                "bld": "dbms/MDC/STAT/standard/MDCSTAT05001",
                "trdDd": date_str,
                "isuCd": isin,
            },
        )
        response.raise_for_status()
        raw = response.json()
        return parse_holdings_response(raw, etf_code, date_str)

    def fetch_flow(
        self, isin: str, etf_code: str, target_date: date
    ) -> list[FlowRecord]:
        """Derive ETF flow from listed shares changes in price history."""
        logger.info("Fetching flow for %s (%s) on %s", etf_code, isin, target_date)

        # Fetch ~10 days back to find the previous trading day
        from_date = target_date - timedelta(days=10)
        self._delay()
        response = self._session.post(
            KRX_BASE_URL,
            data={
                "bld": "dbms/MDC/STAT/standard/MDCSTAT04501",
                "isuCd": isin,
                "strtDd": from_date.strftime("%Y%m%d"),
                "endDd": target_date.strftime("%Y%m%d"),
            },
        )
        response.raise_for_status()
        raw = response.json()
        all_flows = parse_flow_from_price_history(
            raw.get("output", []), etf_code
        )
        # Return only the target date's flow
        return [f for f in all_flows if f.date == target_date]

    def close(self) -> None:
        self._session.close()

"""한국투자증권 OpenAPI client."""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path

import httpx

from etf_analyzer.collector.models import FlowRecord, HoldingRecord

logger = logging.getLogger(__name__)

REAL_BASE = "https://openapi.koreainvestment.com:9443"
PAPER_BASE = "https://openapivts.koreainvestment.com:29443"
TOKEN_CACHE = Path("data/.kis_token.json")


class KISClient:
    """Korea Investment & Securities OpenAPI client with 1-day token caching."""

    def __init__(
        self,
        app_key: str | None = None,
        app_secret: str | None = None,
        *,
        is_paper: bool = False,
    ) -> None:
        self.app_key = app_key or os.environ.get("KIS_APP_KEY", "")
        self.app_secret = app_secret or os.environ.get("KIS_APP_SECRET", "")
        if not self.app_key or not self.app_secret:
            raise ValueError("KIS_APP_KEY / KIS_APP_SECRET 필요 (.env 또는 환경변수)")

        self.base_url = PAPER_BASE if is_paper else REAL_BASE
        self._token: str | None = None
        self._token_expires: datetime | None = None
        self._session = httpx.Client(timeout=30.0)
        self._load_cached_token()

    # ── token management ──────────────────────────────────────

    def _load_cached_token(self) -> None:
        if not TOKEN_CACHE.exists():
            return
        try:
            data = json.loads(TOKEN_CACHE.read_text())
            expires = datetime.fromisoformat(data["expires_at"])
            if expires > datetime.now() + timedelta(minutes=5):
                self._token = data["access_token"]
                self._token_expires = expires
                logger.info("Cached KIS token loaded (expires %s)", expires)
        except (json.JSONDecodeError, KeyError):
            pass

    def _save_token(self, token: str, expires_at: datetime) -> None:
        TOKEN_CACHE.parent.mkdir(parents=True, exist_ok=True)
        TOKEN_CACHE.write_text(
            json.dumps({"access_token": token, "expires_at": expires_at.isoformat()})
        )

    def ensure_token(self) -> str:
        """Return valid access token, requesting a new one if needed."""
        if self._token and self._token_expires and self._token_expires > datetime.now():
            return self._token

        logger.info("Requesting new KIS access token...")
        resp = self._session.post(
            f"{self.base_url}/oauth2/tokenP",
            json={
                "grant_type": "client_credentials",
                "appkey": self.app_key,
                "appsecret": self.app_secret,
            },
        )
        resp.raise_for_status()
        body = resp.json()

        self._token = body["access_token"]
        expires_in = body.get("expires_in", 86400)
        self._token_expires = datetime.now() + timedelta(seconds=expires_in)
        self._save_token(self._token, self._token_expires)
        logger.info("New KIS token acquired (expires in %ds)", expires_in)
        return self._token

    def _headers(self, tr_id: str) -> dict[str, str]:
        token = self.ensure_token()
        return {
            "authorization": f"Bearer {token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
            "content-type": "application/json; charset=utf-8",
        }

    def _get(self, path: str, tr_id: str, params: dict) -> dict:
        """GET request with auth headers. Raises on API-level errors."""
        resp = self._session.get(
            f"{self.base_url}{path}",
            headers=self._headers(tr_id),
            params=params,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("rt_cd") != "0":
            msg = data.get("msg1", "unknown error")
            raise RuntimeError(f"KIS API error [{data.get('rt_cd')}]: {msg}")
        return data

    # ── ETF 구성종목 (holdings) ───────────────────────────────

    def fetch_holdings(
        self, etf_code: str, target_date: date
    ) -> list[HoldingRecord]:
        """ETF 구성종목 시세 조회 (FHKST121600C0)."""
        logger.info("Fetching holdings for %s on %s", etf_code, target_date)

        data = self._get(
            "/uapi/etfetn/v1/quotations/inquire-component-stock-price",
            tr_id="FHKST121600C0",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": etf_code,
                "FID_COND_SCR_DIV_CODE": "11216",
            },
        )

        records: list[HoldingRecord] = []
        for item in data.get("output2", []):
            stock_code = item.get("stck_shrn_iscd", "").strip()
            if not stock_code:
                continue
            weight = float(item.get("etf_cnfg_issu_rlim", "0") or "0")
            shares = int(item.get("etf_cu_unit_scrt_cnt", "0") or "0")
            records.append(
                HoldingRecord(
                    date=target_date,
                    etf_code=etf_code,
                    stock_code=stock_code,
                    stock_name=item.get("hts_kor_isnm", "").strip(),
                    weight=weight,
                    shares=shares,
                )
            )
        return records

    def close(self) -> None:
        self._session.close()


KRX_ETF_URL = "https://data-dbg.krx.co.kr/svc/apis/etp/etf_bydd_trd"


class KRXFlowClient:
    """KRX OpenAPI client for ETF flow data (LIST_SHRS 변화)."""

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key or os.environ.get("KRX_API_KEY", "")
        if not self.api_key:
            raise ValueError("KRX_API_KEY 필요 (.env 또는 환경변수)")
        self._session = httpx.Client(timeout=30.0)

    def _fetch_all_etf(self, date_str: str) -> dict[str, dict]:
        """Fetch all ETF daily trading data, return {ISU_CD: row}."""
        resp = self._session.get(
            KRX_ETF_URL,
            params={"AUTH_KEY": self.api_key, "basDd": date_str},
            follow_redirects=True,
        )
        resp.raise_for_status()
        items = resp.json().get("OutBlock_1", [])
        return {item["ISU_CD"]: item for item in items}

    def fetch_flow(
        self, etf_code: str, target_date: date
    ) -> list[FlowRecord]:
        """두 영업일의 LIST_SHRS 차이로 flow 계산."""
        logger.info("Fetching KRX flow for %s on %s", etf_code, target_date)

        today_str = target_date.strftime("%Y%m%d")
        today_map = self._fetch_all_etf(today_str)

        if etf_code not in today_map:
            logger.warning("%s not found in KRX data for %s", etf_code, target_date)
            return []

        # Find previous trading day (skip weekends)
        prev = target_date - timedelta(days=1)
        for _ in range(5):
            if prev.weekday() < 5:
                break
            prev -= timedelta(days=1)

        prev_map = self._fetch_all_etf(prev.strftime("%Y%m%d"))
        if etf_code not in prev_map:
            logger.warning("%s not found in KRX data for %s", etf_code, prev)
            return []

        today_shares = int(today_map[etf_code]["LIST_SHRS"])
        prev_shares = int(prev_map[etf_code]["LIST_SHRS"])
        nav = float(today_map[etf_code]["NAV"])
        net_units = today_shares - prev_shares

        return [
            FlowRecord(
                date=target_date,
                etf_code=etf_code,
                creation_units=max(0, net_units),
                redemption_units=abs(min(0, net_units)),
                net_units=net_units,
                nav=nav,
            )
        ]

    def fetch_flow_batch(
        self, etf_codes: list[str], target_date: date
    ) -> dict[str, list[FlowRecord]]:
        """여러 ETF flow를 2회 API 호출로 일괄 계산."""
        logger.info("Fetching KRX flow batch for %s on %s", etf_codes, target_date)

        today_map = self._fetch_all_etf(target_date.strftime("%Y%m%d"))

        prev = target_date - timedelta(days=1)
        for _ in range(5):
            if prev.weekday() < 5:
                break
            prev -= timedelta(days=1)
        prev_map = self._fetch_all_etf(prev.strftime("%Y%m%d"))

        result: dict[str, list[FlowRecord]] = {}
        for code in etf_codes:
            if code not in today_map or code not in prev_map:
                logger.warning("%s not found in KRX data", code)
                result[code] = []
                continue

            today_shares = int(today_map[code]["LIST_SHRS"])
            prev_shares = int(prev_map[code]["LIST_SHRS"])
            nav = float(today_map[code]["NAV"])
            net_units = today_shares - prev_shares

            result[code] = [
                FlowRecord(
                    date=target_date,
                    etf_code=code,
                    creation_units=max(0, net_units),
                    redemption_units=abs(min(0, net_units)),
                    net_units=net_units,
                    nav=nav,
                )
            ]
        return result

    def close(self) -> None:
        self._session.close()

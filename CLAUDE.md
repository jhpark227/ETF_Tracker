# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ETF Analyzer — Korean ETF 자금 흐름을 추적·분석하는 Python 3.14 CLI + 텔레그램 브리핑 도구. 패키지 관리는 `uv`, DB는 DuckDB 사용.

## Setup

```bash
# 의존성 설치 (macOS 버그 워크어라운드 포함)
make sync

# macOS + Python 3.14 + uv 버그:
# uv sync 후 .pth 파일에 UF_HIDDEN 플래그가 설정되어 CLI가 동작 안 할 수 있음
# make sync가 scripts/fix_sitecustomize.py를 실행해 자동으로 수정함

# 테스트
uv run pytest

# 패키지 추가
uv add <package>
uv add --dev <package>
```

## CLI Commands

```bash
# 목업 데이터로 전체 파이프라인 즉시 확인 (KRX API 없이)
uv run etf-analyzer seed [--days 10]

# DB 초기화 (ETF 마스터 등록)
uv run etf-analyzer init

# KRX에서 실제 데이터 수집 (KRX 로그인 세션 필요)
uv run etf-analyzer collect --date 2026-03-18
uv run etf-analyzer collect --today
uv run etf-analyzer collect --from 2026-03-01 --to 2026-03-18

# 수집 상태 확인
uv run etf-analyzer status

# 종목 랭킹 (flow + conviction 복합 점수)
uv run etf-analyzer rank [--window 5] [--top 20]
uv run etf-analyzer rank --window 1,3,5   # 복수 윈도우 비교

# 종목 상세 분석 (ETF별 자금유입·비중변화)
uv run etf-analyzer detail <종목코드>
uv run etf-analyzer detail 005930

# ETF별 분석 (순자금유입, 비중변화 Top N)
uv run etf-analyzer etf <ETF코드>
uv run etf-analyzer etf 441800
```

## Telegram Briefing

매일 오전 09:30 (월~금) macOS launchd로 자동 실행.

```bash
# 수동 실행
bash scripts/daily_collect.sh

# launchd 관리
launchctl load ~/Library/LaunchAgents/com.etf-analyzer.daily-collect.plist
launchctl start com.etf-analyzer.daily-collect
```

핵심 함수: `etf_analyzer/notifier.py` → `build_cross_window_report()`
- 멀티윈도우(3d/5d/10d) 교차 분석 → 5단 투자 브리핑 생성
- 섹터 그룹핑 적용 (배당/은행배당/배당성장 → 배당)
- 환경변수: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` (.env)

## Architecture

```
etf_analyzer/
├── cli.py              — Typer CLI (seed, init, collect, status, rank, detail, etf)
├── config.py           — AppConfig dataclass, config.toml 로더
├── notifier.py         — 텔레그램 알림 (교차 분석 5단 브리핑, 섹터 그룹핑)
├── collector/
│   ├── krx.py          — KRX HTTP 클라이언트 (fetch_isin_map, fetch_holdings, fetch_flow)
│   └── models.py       — Pydantic 모델 + KRX 응답 파서
├── storage/
│   ├── db.py           — DuckDB 연결 관리
│   ├── schema.py       — CREATE TABLE DDL (etf_master, etf_holdings, etf_flow, stock_master)
│   └── repository.py   — UPSERT / 쿼리 함수
└── analyzer/
    ├── flow.py         — Flow score = Σ(net_units × NAV × stock_weight)
    ├── conviction.py   — Conviction score (breadth × depth, 액티브 ETF 가중)
    └── ranking.py      — 퍼센타일 정규화 복합 랭킹 + 드릴다운
config.toml             — ETF 유니버스 25종, DB 경로, 파라미터
scripts/
├── daily_collect.sh    — launchd 자동 수집 + 텔레그램 전송 스크립트
└── fix_sitecustomize.py — macOS uv 버그 워크어라운드
```

## Multi-Window Cross Analysis

교차 점수 = Σ(윈도우별 combined_score × 가중치)
- 3일: 0.20 | 5일: 0.35 | 10일: 0.45
- 전 구간 유입 보너스 ×1.15, 패시브+액티브 동시 편입 ×1.10
- 추세 신호: ▲ 강화 (가속), ▲ 유입 (안정), ▼ 약화 (감속), ─ (혼조)

## KRX API 현황

- `data.krx.co.kr` — 현재 로그인 필수로 변경됨. 직접 스크래핑 불가.
- **개발/테스트**: `seed` 명령어로 목업 데이터 생성 후 전체 파이프라인 확인
- **향후 실데이터**: KRX OpenAPI (`openapi.krx.co.kr`) API 키 발급 예정
- Holdings: `MDCSTAT05001` (PDF), Flow: `MDCSTAT04501` (LIST_SHRS 변화로 계산)

## ETF Universe (25종)

| Tier | 코드 | 이름 | 유형 | 섹터 |
|------|------|------|------|------|
| market | 069500 | KODEX 200 | passive | 코스피200 |
| market | 229200 | KODEX 코스닥150 | passive | 코스닥150 |
| market | 310970 | TIGER MSCI Korea TR | passive | MSCI Korea |
| market | 292150 | TIGER 코리아TOP10 | passive | 대형주 |
| market | 495050 | RISE 코리아밸류업 | passive | 밸류업 |
| sector | 396500 | TIGER 반도체TOP10 | passive | 반도체 |
| sector | 395160 | KODEX AI반도체 | passive | AI반도체 |
| sector | 305720 | KODEX 2차전지산업 | passive | 2차전지 |
| sector | 449450 | PLUS K방산 | passive | 방산 |
| sector | 466920 | SOL 조선TOP3플러스 | passive | 조선 |
| sector | 487240 | KODEX AI전력핵심설비 | passive | AI전력 |
| sector | 434730 | HANARO 원자력iSelect | passive | 원자력 |
| sector | 102970 | KODEX 증권 | passive | 금융 |
| sector | 091180 | KODEX 자동차 | passive | 자동차 |
| sector | 228790 | TIGER 화장품 | passive | 소비재 |
| strategy | 161510 | PLUS 고배당주 | passive | 배당 |
| strategy | 102780 | KODEX 삼성그룹 | passive | 삼성그룹 |
| strategy | 138540 | TIGER 현대차그룹플러스 | passive | 현대차그룹 |
| strategy | 466940 | TIGER 은행고배당플러스TOP10 | passive | 은행배당 |
| active | 0163Y0 | KoAct 코스닥액티브 | active | 코스닥 |
| active | 445290 | KODEX 로봇액티브 | active | 로봇 |
| active | 444200 | SOL 코리아메가테크액티브 | active | 메가테크 |
| active | 441800 | TIME Korea플러스배당액티브 | active | 배당성장 |
| active | 462900 | KoAct 바이오헬스케어액티브 | active | 바이오 |
| active | 494890 | KODEX 200액티브 | active | 시장대표 |

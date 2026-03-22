# ETF Flow Tracker — 설계 문서

## 1. 프로젝트 개요

한국 ETF 시장에서 자금 흐름(Flow)을 추적하여, 수급이 쏠리는 "좋은 종목"을 찾는 분석 시스템.

### 핵심 가치

1. **자금유입 신호** — ETF 설정/해지를 통해 실제 자금이 유입되는 종목 포착
2. **운용역 확신 신호** — 여러 ETF 운용역이 독립적으로 비중을 올린 종목 포착
3. **두 신호의 결합** — 종합 스코어링으로 "좋은 종목" 랭킹 산출

### 파일럿 범위

전체 ETF 대신 5개 ETF로 시작하여 시스템을 검증한 뒤 확장한다.

| ETF | 코드 | 유형 | 운용사 |
|-----|------|------|--------|
| KODEX 200 | 069500 | 패시브 (대형) | 삼성자산운용 |
| TIGER 200 | 102110 | 패시브 (대형) | 미래에셋자산운용 |
| KODEX 2차전지산업 | 305720 | 패시브 (테마) | 삼성자산운용 |
| TIMEFOLIO Korea플러스배당액티브 | 441800 | 액티브 | 타임폴리오자산운용 |
| PLUS 고배당주 | 161510 | 액티브 | 한화자산운용 |

## 2. 아키텍처

레이어드 모듈 아키텍처를 채택한다. 각 모듈은 명확한 책임을 갖고, 모듈 간 함수 호출로 통신한다.

```
etf-analyzer/
├── main.py                    # 진입점
├── pyproject.toml
├── config.toml                # 설정 파일 (ETF 목록, 가중치 등)
├── etf_analyzer/
│   ├── __init__.py
│   ├── cli.py                 # CLI 서브커맨드 정의 (Typer)
│   ├── config.py              # 설정 파일 로드 & 기본값 관리
│   ├── collector/
│   │   ├── __init__.py
│   │   ├── krx.py             # KRX 데이터 수집
│   │   └── models.py          # 수집 데이터 Pydantic 모델
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── db.py              # DuckDB 연결 관리
│   │   ├── schema.py          # 테이블 DDL 정의
│   │   └── repository.py      # 데이터 CRUD 함수
│   ├── analyzer/
│   │   ├── __init__.py
│   │   ├── flow.py            # 자금유입 점수 계산
│   │   ├── conviction.py      # 운용역 확신도 점수 계산
│   │   └── ranking.py         # 종합 점수 & 랭킹 산출
│   └── web/                   # 향후 구현 (기술 스택 미정)
│       └── __init__.py
└── tests/
    ├── conftest.py            # 공통 픽스처 (테스트 DB, KRX 응답 mock)
    ├── test_collector/
    ├── test_storage/
    └── test_analyzer/
```

### 모듈 간 의존 방향

```
cli → collector → storage
 │                  ↑
 └──→ analyzer ─────┘
                    ↓
                  DuckDB
```

- `collector`는 `storage`에 데이터를 쓴다
- `analyzer`는 `storage`에서 데이터를 읽는다
- `cli`는 `collector`와 `analyzer`를 모두 호출한다
- 모듈 간 순환 의존 없음

## 3. 설정 관리 (Configuration)

프로젝트 루트의 `config.toml` 파일로 관리한다. `etf_analyzer/config.py`에서 로드하며, 파일이 없으면 기본값을 사용한다.

```toml
[database]
path = "data/etf_analyzer.duckdb"     # DuckDB 파일 경로

[collector]
request_delay_min = 2.0               # 요청 간 최소 딜레이 (초)
request_delay_max = 4.0               # 요청 간 최대 딜레이 (초)
backfill_batch_size = 5               # 백필 시 한 번에 처리할 일수
backfill_batch_delay = 10.0           # 백필 배치 간 대기 (초)

[analyzer]
default_window = 5                    # 기본 분석 윈도우 (일)
flow_weight = 0.4                     # 자금유입 점수 가중치
conviction_weight = 0.6               # 확신도 점수 가중치
active_etf_multiplier = 2.0           # 액티브 ETF 가중 배수

[pilot_etfs]
etfs = [
    { code = "069500", name = "KODEX 200", type = "passive" },
    { code = "102110", name = "TIGER 200", type = "passive" },
    { code = "305720", name = "KODEX 2차전지산업", type = "passive" },
    { code = "441800", name = "TIMEFOLIO Korea플러스배당액티브", type = "active" },
    { code = "161510", name = "PLUS 고배당주", type = "active" },
]
```

전체 ETF 확장 시 `[pilot_etfs]` 섹션만 변경하면 된다.

## 4. 데이터 모델

### 4-1. ETF 마스터 (etf_master)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| etf_code | VARCHAR PK | ETF 종목코드 (예: '069500') |
| etf_name | VARCHAR | ETF 명칭 |
| manager | VARCHAR | 운용사명 |
| etf_type | VARCHAR | 'passive' 또는 'active' |
| benchmark | VARCHAR | 추종 지수명 (패시브인 경우) |

### 4-2. ETF 구성종목 (etf_holdings)

일별 PDF 스냅샷. 핵심 테이블.

| 컬럼 | 타입 | 설명 |
|------|------|------|
| date | DATE | 기준일 |
| etf_code | VARCHAR | ETF 종목코드 |
| stock_code | VARCHAR | 구성종목 코드 |
| stock_name | VARCHAR | 구성종목명 |
| weight | DOUBLE | 편입비중 (%) |
| shares | BIGINT | 보유 주수 |

PK: (date, etf_code, stock_code)

### 4-3. ETF 설정/해지 (etf_flow)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| date | DATE | 기준일 |
| etf_code | VARCHAR | ETF 종목코드 |
| creation_units | BIGINT | 설정 수량 (CU) |
| redemption_units | BIGINT | 해지 수량 (CU) |
| net_units | BIGINT | 순설정 (설정 - 해지) |
| nav | DOUBLE | 기준가 (NAV) |

PK: (date, etf_code)

`net_amount`(순설정금액)는 저장하지 않고 조회 시 `net_units × nav`로 산출한다 (파생 컬럼). 이렇게 하면 NAV 보정 시 재계산이 불필요하다.

### 4-4. 종목 마스터 (stock_master)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| stock_code | VARCHAR PK | 종목코드 |
| stock_name | VARCHAR | 종목명 |
| market | VARCHAR | 'KOSPI' 또는 'KOSDAQ' |

`stock_master`는 `etf_holdings` 수집 시 자동으로 채워진다. 새로운 종목코드가 등장하면 INSERT, 기존 종목명이 변경되면 UPDATE한다. 별도 API 호출 없이 ETF 구성종목 데이터에서 파생한다.

### 설계 원칙

- 비중 변화량은 별도 테이블 없이 DuckDB 윈도우 함수(`LAG`)로 즉시 산출
- UPSERT는 DuckDB의 `INSERT OR REPLACE INTO` 구문 사용 (PK 제약 기반)
- DuckDB 파일 경로: `data/etf_analyzer.duckdb` (config.toml에서 변경 가능)
- `init` 명령 시 DB가 이미 존재하면 기존 데이터 유지, 누락된 테이블만 `CREATE TABLE IF NOT EXISTS`로 생성

## 5. 데이터 수집 (Collector)

### 수집 소스

KRX 정보데이터시스템 (data.krx.co.kr)의 내부 JSON API를 직접 호출한다.

| 데이터 | API 엔드포인트 |
|--------|---------------|
| ETF 구성종목 (PDF) | `bld=dbms/MDC/STAT/standard/MDCSTAT04601` |
| ETF 설정/해지 | `bld=dbms/MDC/STAT/standard/MDCSTAT04501` |

> **주의:** 이 API는 KRX의 비공식 내부 API이며 사전 통보 없이 변경될 수 있다. 응답 구조가 예상과 다를 경우 Pydantic 검증에서 즉시 감지되도록 설계한다.

### 수집 흐름

1. ETF 코드 + 조회일자로 POST 요청
2. JSON 응답 수신
3. Pydantic 모델로 검증 & 변환 (예상 스키마와 불일치 시 즉시 에러)
4. DuckDB에 UPSERT
5. 수집한 종목 정보로 `stock_master` 자동 갱신

### 에러 처리 전략

| 상황 | 처리 |
|------|------|
| HTTP 429/403 (차단) | 즉시 중단, 이미 수집된 데이터는 유지, 경고 메시지 출력 |
| HTTP 200이지만 빈/비정상 JSON | Pydantic 검증 실패 → 해당 ETF 건너뛰기, 에러 로그 |
| 네트워크 타임아웃 | 1회 재시도 (5초 대기 후), 실패 시 건너뛰기 |
| 5개 ETF 중 일부 실패 | 성공한 ETF는 정상 저장, 실패 목록을 최종 요약에 출력 |

로깅: Python 표준 `logging` 모듈 사용. 기본 레벨 INFO, `--verbose` 옵션으로 DEBUG 전환.

### KRX 차단 방지 전략

| 항목 | 설정 |
|------|------|
| 요청 간격 | 2~4초 랜덤 딜레이 |
| User-Agent | 일반 브라우저 헤더 |
| Referer | KRX 페이지 URL 포함 |
| 세션 관리 | httpx.Client 세션 유지 |
| 일일 요청 상한 | 파일럿 기준 10건/일 |
| 백필 배치 | 한 번에 최대 5일치, 배치 간 10초 대기 |
| 차단 감지 | HTTP 429/403 시 즉시 중단 + 경고 |

### 수집 방식

- 초기: 수동 실행 (`etf-analyzer collect` 명령)
- 안정화 후: cron 스케줄러로 매일 장 마감 후 자동 수집
- 과거 데이터 백필: `--from`, `--to` 옵션으로 기간 지정

### 동기/비동기 결정

수집은 **동기(sync) httpx.Client**를 사용한다. 이유:
- 파일럿 5개 ETF 기준 요청 수가 적어 비동기의 이점이 없음
- KRX 차단 방지를 위해 요청 간 딜레이가 필수이므로 동시 요청 불가
- Typer는 async를 네이티브 지원하지 않아 불필요한 복잡도 증가
- 전체 ETF 확장 시 필요하면 그때 async로 전환 (httpx는 동일 인터페이스 제공)

## 6. 분석 엔진 (Analyzer)

### 6-1. 자금유입 점수 (Flow Score)

```
flow_score(stock, window) = Σ over ETFs [
    해당 ETF의 window 기간 누적 순설정금액 × 해당 ETF 내 stock의 window 기간 평균 편입비중
]

순설정금액 = net_units × nav  (etf_flow 테이블에서 조회 시 산출)
```

- 패시브/액티브 구분 없이 동일 적용 (실제 자금 흐름이므로)
- 결과 단위: 원(₩)
- CU(Creation Unit) 수량과 NAV는 KRX API 응답에 포함되어 있어 별도 변환 불필요

### 6-2. 운용역 확신도 점수 (Conviction Score)

```
conviction_score(stock, window) = breadth × depth

breadth = 비중을 늘린 ETF 수 / stock을 보유한 전체 ETF 수

depth = Σ(weight_change_i × multiplier_i) / Σ(multiplier_i)
        (비중을 늘린 ETF만 대상)
        multiplier: active ETF = 2.0, passive ETF = 1.0

weight_change = weight(today) - weight(today - window)
```

- **breadth**: 합의도 — 얼마나 많은 운용역이 동의하는가 (0.0 ~ 1.0)
- **depth**: 강도 — 비중을 올린 ETF들의 가중 평균 증가폭 (%p)
- 비중을 **줄이거나 유지한 ETF는 depth 계산에서 제외** (breadth에서만 분모에 반영)
- 윈도우 기간의 시작일에 해당 종목을 보유하지 않았던 ETF가 새로 편입한 경우: 이전 비중을 0%로 간주

### 6-3. 종합 랭킹 (Combined Ranking)

```
combined_score = 0.4 × normalize(flow_score) + 0.6 × normalize(conviction_score)
```

- 확신도에 더 높은 가중치(0.6) — 프로젝트 핵심 목표가 "운용역이 좋다고 판단한 종목" 발굴
- normalize: **percentile rank** (0~100). 이유: min-max는 극단값에 민감하지만, percentile은 분포에 강건함
- 가중치는 `config.toml`에서 조정 가능

### 6-4. 분석 윈도우

- 기본 제공: 1일, 5일, 20일
- 사용자 지정 기간 지원
- 기본값: 5일

### 6-5. 드릴다운 상세

종목 선택 시 제공하는 정보:

```
삼성전자 (005930) — 종합 78.5점 (상위 3위)
├─ 자금유입: +523억원 (5일 기준)
│  ├─ KODEX 200:  +312억 (비중 25.3%)
│  ├─ TIGER 200:  +198억 (비중 24.8%)
│  └─ TIMEFOLIO:  +13억 (비중 5.2%)
├─ 확신도: 0.72
│  ├─ breadth: 3/4 ETF가 비중 증가
│  ├─ depth: 평균 +1.2%p
│  └─ TIMEFOLIO Korea플러스배당액티브: +3.1%p ★ (액티브)
└─ 5일간 비중 변화 추이: 22.1% → 23.5% → 24.0% → 24.8% → 25.3%
```

## 7. CLI 인터페이스

Typer 기반. 서브커맨드 구조.

```bash
# 데이터 수집
etf-analyzer collect --date 2026-03-18
etf-analyzer collect --from 2026-03-01 --to 2026-03-18
etf-analyzer collect --today

# 분석 & 랭킹
etf-analyzer rank --window 5 --top 20
etf-analyzer rank --window 1,5,20 --top 10

# 드릴다운
etf-analyzer detail 005930 --window 5
etf-analyzer detail 005930 --window 1,5,20

# ETF 단위 조회 — 해당 ETF의 자금흐름 요약 + 구성종목 비중 변화 상위 종목
etf-analyzer etf 069500 --window 5

# 유틸리티
etf-analyzer init      # DB 초기화 & 파일럿 ETF 등록
etf-analyzer status    # 수집 현황 (마지막 수집일, 누적일수, ETF별 상태)
```

- 기본 윈도우는 5일
- `rank --from/--to`는 제거: 윈도우 기반 분석과 혼동을 방지. 분석 기준일은 가장 최근 수집일을 자동 사용
- `web` 커맨드는 대시보드 구현 시 추가 (현재 미구현)
- 파일럿 단계에서는 모든 파일럿 ETF를 대상으로 수집/분석. ETF 필터링은 전체 확장 시 추가

## 8. 기술 스택

| 패키지 | 용도 |
|--------|------|
| httpx | KRX 데이터 수집 (동기 모드) |
| duckdb | 분석용 임베디드 DB |
| pydantic | 데이터 검증 & 모델 |
| typer | CLI 프레임워크 |
| rich | CLI 출력 포맷팅 (테이블, 트리) |
| pytest | 테스트 (dev) |

### 의도적으로 제외

- **pandas** — DuckDB SQL + Pydantic으로 충분
- **beautifulsoup4** — KRX JSON API 직접 호출이므로 HTML 파싱 불필요
- **selenium/playwright** — KRX JSON API 직접 호출로 불필요
- **pytest-asyncio** — 수집이 동기 방식이므로 불필요
- **web 프레임워크** — 대시보드 기술 스택은 향후 결정

### pyproject.toml 스크립트 진입점

```toml
[project.scripts]
etf-analyzer = "etf_analyzer.cli:app"
```

## 9. 테스트 전략

| 레벨 | 대상 | 방법 |
|------|------|------|
| 단위 테스트 | analyzer (flow, conviction, ranking) | DuckDB 인메모리 DB에 테스트 데이터 삽입 후 검증 |
| 단위 테스트 | collector models (Pydantic) | 실제 KRX 응답 샘플을 fixture로 저장, 파싱 검증 |
| 통합 테스트 | collector → storage → analyzer 전체 흐름 | 인메모리 DuckDB, KRX 응답은 fixture (네트워크 호출 없음) |

- `tests/conftest.py`에 공통 픽스처 정의: 인메모리 DuckDB 인스턴스, KRX API 응답 샘플
- KRX API 응답 샘플은 `tests/fixtures/` 디렉토리에 JSON 파일로 저장
- 실제 KRX 호출 테스트는 수동으로만 실행 (`@pytest.mark.slow` 마커)

## 10. 향후 확장 계획

1. **전체 ETF 확장** — 파일럿 검증 후 `config.toml`의 ETF 목록 변경
2. **웹 대시보드** — 핵심 로직 완성 후 기술 스택 결정, fancy UI + 제어 기능
3. **자동 스케줄링** — cron 연동으로 매일 자동 수집
4. **변동 알림** — 랭킹 급등/급락 종목 감지 (알림 인프라 필요)
5. **추가 지표 결합** — 거래량, 외국인/기관 수급 등과 결합하여 선행 매매 신호 도출
6. **async 전환** — 전체 ETF 수집 시 성능 필요 시 httpx async 모드로 전환

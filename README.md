# ETF Analyzer

한국 ETF의 자금 흐름과 펀드매니저 확신도를 분석해 **유망 종목을 찾는** CLI 도구.

패시브 ETF는 시장 수급을, 액티브 ETF는 매니저 판단을 반영한다는 아이디어에서 출발합니다.

---

## 설치

```bash
git clone <repo>
cd ETF-Analyzer
make sync          # uv sync + macOS 버그 워크어라운드
```

Python 3.14, [uv](https://docs.astral.sh/uv/) 필요.

---

## 빠른 시작 (목업 데이터)

KRX API 없이 전체 파이프라인을 바로 확인할 수 있습니다.

```bash
uv run etf-analyzer seed        # 10일치 목업 데이터 생성
uv run etf-analyzer rank        # 종목 랭킹
uv run etf-analyzer detail 005930   # 삼성전자 상세
uv run etf-analyzer etf 441800      # TIMEFOLIO ETF 분석
```

---

## CLI 명령어

| 명령어 | 설명 |
|--------|------|
| `seed [--days N]` | 목업 데이터 생성 (KRX API 없이 테스트) |
| `init` | DB 초기화, 파일럿 ETF 등록 |
| `collect --date YYYY-MM-DD` | KRX에서 특정 날짜 데이터 수집 |
| `collect --today` | 오늘 데이터 수집 |
| `collect --from DATE --to DATE` | 기간 백필 |
| `status` | 수집 현황 확인 |
| `rank [--window N] [--top N]` | 종목 랭킹 |
| `detail <종목코드>` | 종목별 ETF 드릴다운 |
| `etf <ETF코드>` | ETF별 순유입·비중변화 |

---

## 결과 해석 가이드

### 랭킹 컬럼 의미

| 컬럼 | 의미 | 범위 |
|------|------|------|
| **Combined** | Flow + Conviction 복합 점수 | 0~100 |
| **Flow ₩** | 해당 종목으로 귀속된 ETF 순유입 추정액 | 원화 |
| **Conviction** | 펀드매니저 확신도 (Breadth × Depth) | 소수 |
| **Breadth** | 비중을 늘린 ETF 비율 | 0%~100% |

**Flow ₩ 계산 방식**
```
Flow ₩ = Σ(ETF별 순유입액 × 해당 종목 비중)
순유입액 = net_units × NAV
```

**Conviction 계산 방식**
```
Conviction = Breadth × Depth
Breadth = 비중 증가 ETF 수 / 전체 ETF 수
Depth   = 비중 증가 ETF들의 가중 평균 증가폭
          (액티브 ETF는 2배 가중)
```

---

### 우선순위별 해석 방법

**1단계 — Combined 점수로 1차 필터**
- 70 이상: 강한 시그널 후보
- 50~70: 주목할 만한 종목
- 50 미만: 약한 시그널

**2단계 — Breadth로 신뢰도 확인**
- **Breadth ≥ 67%**: 여러 ETF에서 동시 매수 → 시그널 신뢰도 높음
- **Breadth ≤ 33%**: 한 ETF만 매수 → 단일 펀드 베팅일 수 있어 주의

**3단계 — Flow ₩ vs Conviction 방향 확인**

| 상황 | 해석 |
|------|------|
| Flow ↑, Conviction ↑ | 자금 유입 + 비중 확대 → 가장 강한 신호 |
| Flow ↑, Conviction ↓ | 자금은 들어왔지만 비중은 줄이는 중 → 유의 |
| Flow ↓, Conviction ↑ | 자금 유출에도 비중 확대 → 역발상 포지션 가능성 |
| Flow ↓, Conviction ↓ | 양방향 매도 → 매도 시그널 |

**4단계 — `detail` 명령어로 ETF별 드릴다운**
- 어떤 ETF가 비중을 늘렸는지 확인
- **액티브 ETF(★)**의 비중 변화가 더 의미 있음 — 패시브는 인덱스를 따르지만 액티브는 매니저 판단
- 비중 추이(weight_trend)로 추세 방향 확인

**액티브 ETF 신호를 더 신뢰해야 하는 이유**
- 패시브 ETF의 비중 변화 = 종목 시총 변화를 따라가는 것 (실질적 판단 아님)
- 액티브 ETF의 비중 변화 = 펀드매니저가 의도적으로 늘린/줄인 것

---

## Pilot ETFs

| 코드 | 이름 | 유형 |
|------|------|------|
| 069500 | KODEX 200 | passive |
| 102110 | TIGER 200 | passive |
| 305720 | KODEX 2차전지산업 | passive |
| 441800 | TIMEFOLIO Korea플러스배당액티브 | active |
| 161510 | PLUS 고배당주 | active |

---

## 구조

```
etf_analyzer/
├── cli.py              CLI 진입점 (Typer + Rich)
├── config.py           설정 로더
├── collector/
│   ├── krx.py          KRX HTTP 클라이언트
│   └── models.py       응답 파서 (Pydantic)
├── storage/
│   ├── db.py           DuckDB 연결
│   ├── schema.py       테이블 DDL
│   └── repository.py   UPSERT / 쿼리
└── analyzer/
    ├── flow.py         Flow score 계산
    ├── conviction.py   Conviction score 계산
    └── ranking.py      복합 랭킹 + 드릴다운
config.toml             ETF 목록, 가중치 설정
```

---

## KRX 데이터 수집 현황

`data.krx.co.kr`은 현재 로그인 없이 API 접근이 차단된 상태입니다.

- **현재**: `seed` 명령어로 목업 데이터 사용
- **향후**: KRX OpenAPI (`openapi.krx.co.kr`) API 키 연동 예정

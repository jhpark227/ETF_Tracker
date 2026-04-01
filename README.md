# ETF Analyzer

한국 ETF의 자금 흐름과 펀드매니저 확신도를 분석해 **유망 종목을 찾는** CLI + 텔레그램 브리핑 도구.

패시브 ETF는 시장 수급을, 액티브 ETF는 매니저 판단을 반영한다는 아이디어에서 출발합니다.

---

## 설치

```bash
git clone https://github.com/jhpark227/ETF_Tracker.git
cd ETF-Tracker
make sync          # uv sync + macOS 버그 워크어라운드
cp .env.example .env   # 텔레그램 봇 토큰 등 설정
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
| `init` | DB 초기화, ETF 마스터 등록 |
| `collect --date YYYY-MM-DD` | KRX에서 특정 날짜 데이터 수집 |
| `collect --today` | 오늘 데이터 수집 |
| `collect --from DATE --to DATE` | 기간 백필 |
| `status` | 수집 현황 확인 |
| `rank [--window N] [--top N]` | 종목 랭킹 (복수 윈도우: `--window 1,3,5`) |
| `detail <종목코드>` | 종목별 ETF 드릴다운 |
| `etf <ETF코드>` | ETF별 순유입·비중변화 |

---

## 텔레그램 자동 브리핑

매일 오전 09:30 (월~금) macOS launchd를 통해 자동 실행됩니다.

**브리핑 5단 구성:**
1. **시장 성격** — risk-on/off, 업종 순환, 베타 추종 등 국면 판단
2. **주도 테마** — 유입 상위 섹터 3개 + 대표 종목/ETF
3. **Top 10 랭킹** — 교차 점수, 자금흐름, 추세 신호, ETF 편입 현황
4. **주목 시그널** — 추세 강화 종목, 단기 유출 전환 섹터
5. **한줄 액션** — 비중 유지/축소 추천

**설정:**
```bash
# .env에 다음 변수 설정
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```

**수동 실행:**
```bash
bash scripts/daily_collect.sh
```

---

## 멀티윈도우 교차 분석

단일 윈도우(예: 5일)만으로는 노이즈를 걸러낼 수 없습니다. 3일/5일/10일 교차 분석으로 신뢰도 높은 시그널을 추출합니다.

### 교차 점수 계산

```
cross_score = Σ(윈도우별 combined_score × 가중치)
  3일: 0.20  |  5일: 0.35  |  10일: 0.45

보너스:
  × 1.15  전 구간(3d/5d/10d) 모두 자금 유입
  × 1.10  패시브 + 액티브 ETF 동시 편입
```

### 추세 신호

| 신호 | 의미 |
|------|------|
| ▲ 강화 | 전 구간 유입 + 단기→장기 점수 상승 (자금 가속) |
| ▲ 유입 | 전 구간 유입 (안정적 유입, 가속은 아님) |
| ▼ 약화 | 단기→장기 점수 하락 (모멘텀 감소) |
| ─ | 혼조 |

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
Breadth = 비중 증가 ETF 수(가중) / 전체 ETF 수(가중)
Depth   = Σ(비중 변화 × multiplier)

Multiplier 계층 (누적):
  sector ETF: ×2.0  |  strategy ETF: ×1.5
  active ETF: ×2.0  |  신규 편입: ×3.0
```

### 해석 흐름

1. **Combined 점수로 1차 필터** — 70↑ 강한 시그널, 50~70 주목, 50↓ 약함
2. **Breadth로 신뢰도 확인** — 67%↑ 다수 ETF 동시 매수, 33%↓ 단일 펀드 베팅 주의
3. **Flow vs Conviction 방향 확인**

| 상황 | 해석 |
|------|------|
| Flow ↑, Conviction ↑ | 자금 유입 + 비중 확대 → 가장 강한 신호 |
| Flow ↑, Conviction ↓ | 자금 유입, 비중은 축소 → 유의 |
| Flow ↓, Conviction ↑ | 자금 유출에도 비중 확대 → 역발상 가능성 |
| Flow ↓, Conviction ↓ | 양방향 매도 → 매도 시그널 |

4. **`detail` 명령어로 ETF별 드릴다운** — 액티브 ETF(★)의 비중 변화가 더 의미 있음

---

## ETF 유니버스 (25종)

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

---

## 구조

```
etf_analyzer/
├── cli.py              CLI 진입점 (Typer + Rich)
├── config.py           설정 로더
├── notifier.py         텔레그램 알림 (교차 분석 브리핑)
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
scripts/
├── daily_collect.sh    launchd 자동 수집 + 텔레그램 전송
└── fix_sitecustomize.py  macOS uv 버그 워크어라운드
```

---

## 자동 실행 설정 (macOS launchd)

```bash
# 에이전트 로드 (최초 1회)
launchctl load ~/Library/LaunchAgents/com.etf-analyzer.daily-collect.plist

# 수동 테스트
launchctl start com.etf-analyzer.daily-collect

# 로그 확인
tail -f data/launchd.log
```

> macOS에서 `/bin/bash`에 전체 디스크 접근 권한이 필요합니다.
> 시스템 설정 → 개인정보 보호 및 보안 → 전체 디스크 접근 권한 → `/bin/bash` 추가

---

## KRX 데이터 수집 현황

`data.krx.co.kr`은 현재 로그인 없이 API 접근이 차단된 상태입니다.

- **현재**: `seed` 명령어로 목업 데이터 사용
- **향후**: KRX OpenAPI (`openapi.krx.co.kr`) API 키 연동 예정

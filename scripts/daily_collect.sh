#!/bin/bash
# ETF 데이터 자동 수집 + 멀티윈도우 랭킹 텔레그램 알림 (cron용)
# 매일 오전 9:30 실행: 전 영업일 데이터 수집 → 교차 분석 → 텔레그램 전송

cd "/Users/jhpark/Documents/Claude Code/ETF-Tracker"

# .env 로드
set -a
source .env
set +a

# 전 영업일 계산 (주말 건너뜀)
YESTERDAY=$(/opt/homebrew/bin/uv run python -c "
from datetime import date, timedelta
d = date.today() - timedelta(days=1)
while d.weekday() >= 5:
    d -= timedelta(days=1)
print(d.strftime('%Y-%m-%d'))
")

echo "--- $(date) collecting $YESTERDAY ---" >> data/collect.log

# 1) 데이터 수집
/opt/homebrew/bin/uv run etf-analyzer collect --date "$YESTERDAY" \
  >> data/collect.log 2>&1

# 2) 멀티윈도우 교차 분석 → 텔레그램 전송
/opt/homebrew/bin/uv run python -c "
from dotenv import load_dotenv
load_dotenv()

from etf_analyzer.config import load_config
from etf_analyzer.storage.db import get_connection
from etf_analyzer.notifier import build_cross_window_report, send_telegram

config = load_config()
conn = get_connection(config.database.path)

text = build_cross_window_report(conn, config, windows=[3, 5, 10], top=10)
ok = send_telegram(text)
print(f'Cross-window telegram: {\"OK\" if ok else \"FAIL\"}')

conn.close()
" >> data/collect.log 2>&1

echo "--- $(date) done ---" >> data/collect.log

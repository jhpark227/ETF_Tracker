#!/bin/bash
# ETF 데이터 자동 수집 스크립트 (cron용)
# 매일 오전 8:30 실행: 전 영업일 데이터 수집 (KRX D+1 반영)

cd "/Users/jhpark/Documents/Claude Code/ETF-Tracker"

# 전 영업일 계산 (주말 건너뜀)
YESTERDAY=$(/opt/homebrew/bin/uv run python -c "
from datetime import date, timedelta
d = date.today() - timedelta(days=1)
while d.weekday() >= 5:
    d -= timedelta(days=1)
print(d.strftime('%Y-%m-%d'))
")

/opt/homebrew/bin/uv run etf-analyzer collect --date "$YESTERDAY" \
  >> data/collect.log 2>&1

echo "--- $(date) collected $YESTERDAY ---" >> data/collect.log

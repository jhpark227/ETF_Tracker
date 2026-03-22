from __future__ import annotations

from datetime import date, timedelta

import duckdb


def calculate_conviction_scores(
    conn: duckdb.DuckDBPyConnection,
    window: int,
    base_date: date | None = None,
    active_multiplier: float = 2.0,
) -> list[dict]:
    """
    Calculate conviction scores for all stocks.

    Multiplier hierarchy (stacks):
      - sector/strategy ETF: ×2 (업종 ETF 비중 변화는 더 의미있는 시그널)
      - active ETF: ×active_multiplier (펀드매니저 재량)
      - 신규 편입 (start weight=0): ×3 bonus (새로 담은 건 강한 확신)

    breadth = weighted count of ETFs that increased weight / total weighted ETF count
    depth  = sum of (weight_change × multiplier) — 합산이라 큰 변화가 제대로 반영됨
    conviction_score = breadth × depth
    """
    if base_date is None:
        result = conn.execute("SELECT MAX(date) FROM etf_holdings").fetchone()
        if result[0] is None:
            return []
        base_date = result[0]

    start_date = base_date - timedelta(days=window)

    rows = conn.execute("""
        WITH end_weights AS (
            SELECT etf_code, stock_code, stock_name, weight
            FROM etf_holdings
            WHERE date = ?
        ),
        start_weights AS (
            SELECT etf_code, stock_code, weight
            FROM etf_holdings
            WHERE date = (
                SELECT MAX(date) FROM etf_holdings WHERE date <= ?
            )
        ),
        weight_changes AS (
            SELECT
                e.etf_code,
                e.stock_code,
                e.stock_name,
                e.weight AS end_weight,
                COALESCE(s.weight, 0) AS start_weight,
                e.weight - COALESCE(s.weight, 0) AS weight_change,
                -- tier multiplier: 업종/테마 ETF 비중 변화가 시장 전체 ETF보다 의미있음
                CASE WHEN m.tier = 'sector' THEN 2.0
                     WHEN m.tier = 'strategy' THEN 1.5
                     ELSE 1.0
                END
                -- active multiplier
                * CASE WHEN m.etf_type = 'active' THEN ? ELSE 1.0 END
                -- 신규 편입 bonus: 이전에 없던 종목을 새로 담았으면 강한 시그널
                * CASE WHEN s.weight IS NULL THEN 3.0 ELSE 1.0 END
                AS multiplier
            FROM end_weights e
            LEFT JOIN start_weights s
                ON e.etf_code = s.etf_code AND e.stock_code = s.stock_code
            LEFT JOIN etf_master m ON e.etf_code = m.etf_code
        )
        SELECT
            stock_code,
            stock_name,
            COUNT(*) AS total_etfs,
            SUM(CASE WHEN weight_change > 0 THEN multiplier ELSE 0 END) AS increased_weighted,
            SUM(multiplier) AS total_weighted,
            SUM(CASE WHEN weight_change > 0 THEN weight_change * multiplier ELSE 0 END) AS depth_sum,
            SUM(CASE WHEN start_weight = 0 AND end_weight > 0 THEN 1 ELSE 0 END) AS new_entries
        FROM weight_changes
        GROUP BY stock_code, stock_name
    """, [base_date, start_date, active_multiplier]).fetchall()

    results = []
    for row in rows:
        stock_code, stock_name, total_etfs, increased_weighted, total_weighted, depth_sum, new_entries = row
        breadth = increased_weighted / total_weighted if total_weighted > 0 else 0.0
        depth = depth_sum  # 합산 — 큰 변화 한 건이 제대로 반영됨
        conviction_score = breadth * depth
        results.append({
            "stock_code": stock_code,
            "stock_name": stock_name,
            "breadth": breadth,
            "depth": depth,
            "conviction_score": conviction_score,
            "new_entries": new_entries,
        })

    results.sort(key=lambda x: x["conviction_score"], reverse=True)
    return results

from __future__ import annotations

from datetime import date, timedelta

import duckdb


def calculate_flow_scores(
    conn: duckdb.DuckDBPyConnection,
    window: int,
    base_date: date | None = None,
) -> list[dict]:
    """
    Calculate flow scores for all stocks.

    flow_score(stock) = Σ over ETFs [
        ETF's cumulative net_amount over window × stock's avg weight in that ETF
    ]

    net_amount = net_units × nav
    """
    if base_date is None:
        result = conn.execute("SELECT MAX(date) FROM etf_holdings").fetchone()
        if result[0] is None:
            return []
        base_date = result[0]

    start_date = base_date - timedelta(days=window)

    rows = conn.execute("""
        WITH flow_amounts AS (
            SELECT
                etf_code,
                SUM(net_units * nav) AS total_net_amount
            FROM etf_flow
            WHERE date > ? AND date <= ?
            GROUP BY etf_code
        ),
        avg_weights AS (
            SELECT
                etf_code,
                stock_code,
                stock_name,
                AVG(weight) AS avg_weight
            FROM etf_holdings
            WHERE date > ? AND date <= ?
            GROUP BY etf_code, stock_code, stock_name
        )
        SELECT
            w.stock_code,
            w.stock_name,
            SUM(f.total_net_amount * w.avg_weight / 100.0) AS flow_score
        FROM avg_weights w
        JOIN flow_amounts f ON w.etf_code = f.etf_code
        GROUP BY w.stock_code, w.stock_name
        ORDER BY flow_score DESC
    """, [start_date, base_date, start_date, base_date]).fetchall()

    return [
        {"stock_code": row[0], "stock_name": row[1], "flow_score": row[2]}
        for row in rows
    ]

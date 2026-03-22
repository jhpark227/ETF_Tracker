from __future__ import annotations

from datetime import date, timedelta

import duckdb

from etf_analyzer.analyzer.conviction import calculate_conviction_scores
from etf_analyzer.analyzer.flow import calculate_flow_scores


def percentile_rank(values: list[float]) -> list[float]:
    """Convert raw values to percentile ranks (0-100)."""
    n = len(values)
    if n == 0:
        return []
    if n == 1:
        return [50.0]

    indexed = sorted(enumerate(values), key=lambda x: x[1])
    ranks = [0.0] * n
    for rank_pos, (orig_idx, _) in enumerate(indexed):
        ranks[orig_idx] = (rank_pos / (n - 1)) * 100.0
    return ranks


def calculate_combined_ranking(
    conn: duckdb.DuckDBPyConnection,
    window: int,
    base_date: date | None = None,
    flow_weight: float = 0.4,
    conviction_weight: float = 0.6,
    active_multiplier: float = 2.0,
) -> list[dict]:
    """
    Calculate combined ranking: flow_weight × percentile(flow) + conviction_weight × percentile(conviction).
    """
    flow_scores = calculate_flow_scores(conn, window, base_date)
    conviction_scores = calculate_conviction_scores(
        conn, window, base_date, active_multiplier,
    )

    if not flow_scores and not conviction_scores:
        return []

    # Build lookup maps
    flow_map = {s["stock_code"]: s for s in flow_scores}
    conv_map = {s["stock_code"]: s for s in conviction_scores}

    # Union of all stock codes
    all_codes = set(flow_map.keys()) | set(conv_map.keys())

    # Collect raw scores
    stocks = []
    raw_flows = []
    raw_convictions = []
    for code in all_codes:
        flow_entry = flow_map.get(code, {})
        conv_entry = conv_map.get(code, {})
        stock_name = flow_entry.get("stock_name") or conv_entry.get("stock_name", "")
        raw_flow = flow_entry.get("flow_score", 0.0)
        raw_conv = conv_entry.get("conviction_score", 0.0)
        stocks.append({
            "stock_code": code,
            "stock_name": stock_name,
            "flow_score": raw_flow,
            "conviction_score": raw_conv,
            "breadth": conv_entry.get("breadth", 0.0),
            "depth": conv_entry.get("depth", 0.0),
        })
        raw_flows.append(raw_flow)
        raw_convictions.append(raw_conv)

    # Percentile normalize
    flow_pcts = percentile_rank(raw_flows)
    conv_pcts = percentile_rank(raw_convictions)

    for i, stock in enumerate(stocks):
        stock["flow_percentile"] = flow_pcts[i]
        stock["conviction_percentile"] = conv_pcts[i]
        stock["combined_score"] = (
            flow_weight * flow_pcts[i] + conviction_weight * conv_pcts[i]
        )

    stocks.sort(key=lambda x: x["combined_score"], reverse=True)
    return stocks


def summarize_ranking(
    conn: duckdb.DuckDBPyConnection,
    ranking: list[dict],
    window: int,
    top: int,
    etf_universe: list | None = None,
) -> list[str]:
    """Generate market insight sentences by analyzing sector-level fund flows."""
    if not ranking:
        return []

    lines: list[str] = []
    top_stocks = ranking[:top]
    top_codes = {s["stock_code"] for s in top_stocks}

    # --- 1) ETF 섹터별 순자금유입 집계 ---
    if etf_universe:
        etf_sector_map = {e.code: e.sector for e in etf_universe}
        etf_tier_map = {e.code: e.tier for e in etf_universe}
    else:
        etf_sector_map = {}
        etf_tier_map = {}

    base_date_row = conn.execute("SELECT MAX(date) FROM etf_flow").fetchone()
    if base_date_row[0] is None:
        return []
    base_date = base_date_row[0]
    start_date = base_date - timedelta(days=window)

    # ETF별 순자금유입
    etf_flows = conn.execute("""
        SELECT etf_code, SUM(net_units * nav) AS net_amount
        FROM etf_flow WHERE date > ? AND date <= ?
        GROUP BY etf_code
    """, [start_date, base_date]).fetchall()

    sector_flow: dict[str, float] = {}
    for etf_code, net_amount in etf_flows:
        sector = etf_sector_map.get(etf_code, "")
        if sector:
            sector_flow[sector] = sector_flow.get(sector, 0) + net_amount

    # 섹터별 정렬
    sorted_sectors = sorted(sector_flow.items(), key=lambda x: x[1], reverse=True)
    inflow_sectors = [(s, v) for s, v in sorted_sectors if v > 0]
    outflow_sectors = [(s, v) for s, v in sorted_sectors if v < 0]

    # --- 2) 자금 유입 업종 ---
    if inflow_sectors:
        top_inflows = inflow_sectors[:3]
        sector_strs = [f"{s}({v / 1e8:+,.0f}억)" for s, v in top_inflows]
        lines.append(f"[bold]▸ 자금 유입 업종:[/bold] {', '.join(sector_strs)}")

    # --- 3) 자금 유출 업종 ---
    if outflow_sectors:
        bottom = outflow_sectors[-3:]
        sector_strs = [f"{s}({v / 1e8:+,.0f}억)" for s, v in bottom]
        lines.append(f"[bold]▸ 자금 유출 업종:[/bold] {', '.join(sector_strs)}")

    # --- 4) 상위 종목의 업종 분포 분석 ---
    # 상위 종목들이 주로 어떤 ETF(섹터)에 담겨있는지
    stock_sectors = conn.execute("""
        SELECT DISTINCT h.stock_code, m.etf_code
        FROM etf_holdings h
        JOIN etf_master m ON h.etf_code = m.etf_code
        WHERE h.date = ? AND h.stock_code IN (
            SELECT UNNEST(?::VARCHAR[])
        )
    """, [base_date, list(top_codes)]).fetchall()

    sector_stock_count: dict[str, int] = {}
    for stock_code, etf_code in stock_sectors:
        sector = etf_sector_map.get(etf_code, "")
        if sector and etf_tier_map.get(etf_code) == "sector":
            sector_stock_count[sector] = sector_stock_count.get(sector, 0) + 1

    if sector_stock_count:
        dominant = sorted(sector_stock_count.items(), key=lambda x: x[1], reverse=True)
        top_themes = [f"{s}({c}종목)" for s, c in dominant[:3]]
        lines.append(f"[bold]▸ 상위 종목 테마:[/bold] {', '.join(top_themes)}")

    # --- 5) 액티브 ETF 시그널 ---
    active_etf_codes = [code for code, tier in etf_tier_map.items() if tier == "active"]
    active_inflow = sum(
        net for code, net in etf_flows if code in active_etf_codes and net > 0
    )
    active_outflow = sum(
        net for code, net in etf_flows if code in active_etf_codes and net < 0
    )
    if active_inflow > abs(active_outflow) * 1.5 and active_inflow > 0:
        lines.append(
            f"[bold]▸ 액티브 ETF:[/bold] 순유입 우세 ({active_inflow / 1e8:+,.0f}억) "
            f"— 펀드매니저들이 적극적으로 포지션 구축 중"
        )
    elif abs(active_outflow) > active_inflow * 1.5 and active_outflow < 0:
        lines.append(
            f"[bold]▸ 액티브 ETF:[/bold] 순유출 ({active_outflow / 1e8:+,.0f}억) "
            f"— 펀드매니저 차익실현 또는 리밸런싱 시그널"
        )

    # --- 6) 시장 vs 섹터 흐름 비교 ---
    market_sectors = {"코스피200", "코스닥150", "MSCI Korea", "대형주", "밸류업", "시장대표"}
    market_flow = sum(v for s, v in sector_flow.items() if s in market_sectors)
    theme_flow = sum(v for s, v in sector_flow.items() if s not in market_sectors)
    if theme_flow > 0 and market_flow < 0:
        lines.append("[dim]▸ 테마/업종 ETF로 자금 이동, 시장 전체 ETF는 유출 — 업종 순환 장세[/dim]")
    elif market_flow > 0 and theme_flow < 0:
        lines.append("[dim]▸ 시장 전체 ETF로 자금 유입, 테마 ETF는 유출 — 지수 추종 흐름[/dim]")
    elif market_flow > 0 and theme_flow > 0:
        lines.append("[dim]▸ 시장·테마 ETF 동반 유입 — 전반적 위험선호(risk-on) 흐름[/dim]")

    return lines


def get_stock_detail(
    conn: duckdb.DuckDBPyConnection,
    stock_code: str,
    window: int,
    base_date: date | None = None,
) -> dict:
    """Get detailed drill-down for a specific stock."""
    if base_date is None:
        result = conn.execute("SELECT MAX(date) FROM etf_holdings").fetchone()
        if result[0] is None:
            return {}
        base_date = result[0]

    start_date = base_date - timedelta(days=window)

    # Get stock name
    name_row = conn.execute(
        "SELECT stock_name FROM etf_holdings WHERE stock_code = ? LIMIT 1",
        [stock_code],
    ).fetchone()
    stock_name = name_row[0] if name_row else stock_code

    # Per-ETF breakdown
    etf_rows = conn.execute("""
        WITH end_w AS (
            SELECT etf_code, weight
            FROM etf_holdings WHERE date = ? AND stock_code = ?
        ),
        start_w AS (
            SELECT etf_code, weight
            FROM etf_holdings
            WHERE date = (SELECT MAX(date) FROM etf_holdings WHERE date <= ?)
              AND stock_code = ?
        ),
        flow_data AS (
            SELECT etf_code, SUM(net_units * nav) AS net_amount
            FROM etf_flow WHERE date > ? AND date <= ?
            GROUP BY etf_code
        )
        SELECT
            e.etf_code,
            m.etf_name,
            m.etf_type,
            e.weight AS current_weight,
            e.weight - COALESCE(s.weight, 0) AS weight_change,
            COALESCE(f.net_amount, 0) * e.weight / 100.0 AS flow_contribution
        FROM end_w e
        LEFT JOIN start_w s ON e.etf_code = s.etf_code
        LEFT JOIN flow_data f ON e.etf_code = f.etf_code
        LEFT JOIN etf_master m ON e.etf_code = m.etf_code
        ORDER BY flow_contribution DESC
    """, [base_date, stock_code, start_date, stock_code, start_date, base_date]).fetchall()

    etf_breakdown = [
        {
            "etf_code": row[0],
            "etf_name": row[1] or row[0],
            "etf_type": row[2] or "passive",
            "current_weight": row[3],
            "weight_change": row[4],
            "flow_contribution": row[5],
        }
        for row in etf_rows
    ]

    # Weight trend over window
    trend_rows = conn.execute("""
        SELECT date, AVG(weight) AS avg_weight
        FROM etf_holdings
        WHERE stock_code = ? AND date > ? AND date <= ?
        GROUP BY date ORDER BY date
    """, [stock_code, start_date, base_date]).fetchall()

    weight_trend = [{"date": row[0], "weight": row[1]} for row in trend_rows]

    return {
        "stock_code": stock_code,
        "stock_name": stock_name,
        "window": window,
        "base_date": base_date,
        "etf_breakdown": etf_breakdown,
        "weight_trend": weight_trend,
    }

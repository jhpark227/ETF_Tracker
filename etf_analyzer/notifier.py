"""Telegram notification for ETF Analyzer."""
from __future__ import annotations

import os

import httpx


def send_telegram(message: str, parse_mode: str = "HTML") -> bool:
    """Send a message via Telegram Bot API to all configured chat IDs. Returns True if all succeed."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    if not token:
        return False

    chat_ids = [
        cid for key in ("TELEGRAM_CHAT_ID", "TELEGRAM_CHAT_ID_2")
        if (cid := os.environ.get(key, ""))
    ]
    if not chat_ids:
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    results = []
    for chat_id in chat_ids:
        resp = httpx.post(url, json={
            "chat_id": chat_id,
            "text": message,
            "parse_mode": parse_mode,
        }, timeout=10)
        results.append(resp.status_code == 200)
    return all(results)


def format_rank_text(
    ranking: list[dict],
    window: int,
    top: int = 20,
    summary_lines: list[str] | None = None,
    base_date: str = "",
) -> str:
    """Format ranking data for Telegram (mobile-friendly)."""
    from datetime import date as _date

    date_str = base_date or _date.today().strftime("%m/%d")
    lines = [f"📊 <b>ETF 자금흐름 Top {top}</b>  ({date_str} 기준, {window}일)", ""]

    # Medal + ranked list
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    for i, s in enumerate(ranking[:top], 1):
        flow_val = s["flow_score"] / 1e8
        breadth_pct = s.get("breadth", 0) * 100

        prefix = medals.get(i, f"<b>{i:>2}.</b>")
        flow_sign = "+" if flow_val >= 0 else ""
        flow_arrow = "🔴" if flow_val < 0 else "🟢"

        lines.append(
            f"{prefix} <b>{s['stock_name']}</b> ({s['stock_code']})\n"
            f"     점수 {s['combined_score']:.0f}  |  "
            f"{flow_arrow} {flow_sign}{flow_val:.0f}억  |  "
            f"확신 {s['conviction_score']:.2f}  |  "
            f"편입 {breadth_pct:.0f}%"
        )

    # Summary section
    if summary_lines:
        lines.append("")
        lines.append("━━━━━━━━━━━━━━━━━━━━")
        lines.append("<b>💡 시장 인사이트</b>")
        for line in summary_lines:
            clean = _strip_rich_tags(line)
            # ▸ 접두어 유지, 볼드는 HTML로 변환
            lines.append(clean)

    return "\n".join(lines)


def build_cross_window_report(
    conn,
    config,
    windows: list[int] = [3, 5, 10],
    top: int = 10,
) -> str:
    """
    멀티윈도우 교차 분석 → 투자 적합 종목 랭킹.

    scoring:
    - 각 윈도우별 combined_score 퍼센타일 합산 (장기 가중)
    - 전 구간 유입 종목에 보너스 ×1.15
    - 패시브+액티브 동시 편입 종목에 보너스 ×1.10

    포맷 (3섹션):
    - ① 시장 국면: 국면 한 줄 + 섹터별 자금 흐름 바
    - ② Top 10: 종목별 매수 성격 태그 + 핵심 근거 한 줄
    - ③ 주목 시그널 + 액션 포인트
    """
    from datetime import timedelta
    from etf_analyzer.analyzer.ranking import calculate_combined_ranking

    # 윈도우별 랭킹 계산
    rankings_by_window: dict[int, dict[str, dict]] = {}
    for w in windows:
        ranking = calculate_combined_ranking(
            conn, window=w,
            flow_weight=config.analyzer.flow_weight,
            conviction_weight=config.analyzer.conviction_weight,
            active_multiplier=config.analyzer.active_etf_multiplier,
        )
        rankings_by_window[w] = {s["stock_code"]: s for s in ranking}

    all_codes: set[str] = set()
    for wmap in rankings_by_window.values():
        all_codes |= wmap.keys()

    # 섹터 그룹핑 (유사 섹터 병합)
    SECTOR_GROUPS = {
        "배당": "배당",
        "은행배당": "배당",
        "배당성장": "배당",
    }

    def _group_sector(sector: str) -> str:
        return SECTOR_GROUPS.get(sector, sector)

    # ETF 분류
    # 494890 (KODEX 200액티브)은 코스피200 추종으로 사실상 패시브 — 매수 신호 판단에서 제외
    PASSIVE_LIKE_ACTIVE = {"494890"}
    active_etf_codes = {e.code for e in config.etf_universe if e.type == "active"}
    signal_active_etf_codes = active_etf_codes - PASSIVE_LIKE_ACTIVE
    passive_etf_codes = {e.code for e in config.etf_universe if e.type == "passive"}
    etf_sector_map = {e.code: _group_sector(e.sector) for e in config.etf_universe}
    etf_tier_map = {e.code: e.tier for e in config.etf_universe}


    base_date = conn.execute("SELECT MAX(date) FROM etf_holdings").fetchone()[0]
    if base_date is None:
        return "데이터 없음"

    # 현재 보유 ETF 맵 (etf_code → weight)
    etf_holdings_map: dict[str, list[tuple[str, float]]] = {}
    rows = conn.execute(
        "SELECT stock_code, etf_code, weight FROM etf_holdings WHERE date = ?",
        [base_date],
    ).fetchall()
    for stock_code, etf_code, weight in rows:
        etf_holdings_map.setdefault(stock_code, []).append((etf_code, weight))

    # 가장 긴 윈도우 기준 신규 편입 조회
    longest_w = max(windows)
    short_w = min(windows)
    start_date = base_date - timedelta(days=longest_w)
    prev_date_row = conn.execute(
        "SELECT MAX(date) FROM etf_holdings WHERE date <= ?", [start_date]
    ).fetchone()
    prev_date = prev_date_row[0] if prev_date_row and prev_date_row[0] else start_date

    prev_holdings_map: dict[str, set[str]] = {}
    prev_rows = conn.execute(
        "SELECT stock_code, etf_code FROM etf_holdings WHERE date = ?",
        [prev_date],
    ).fetchall()
    for stock_code, etf_code in prev_rows:
        prev_holdings_map.setdefault(stock_code, set()).add(etf_code)

    # 비중 변화 맵: stock_code → {etf_code: weight_change}
    weight_change_map: dict[str, dict[str, float]] = {}
    wc_rows = conn.execute("""
        WITH cur AS (
            SELECT stock_code, etf_code, weight FROM etf_holdings WHERE date = ?
        ),
        prv AS (
            SELECT stock_code, etf_code, weight FROM etf_holdings WHERE date = ?
        )
        SELECT c.stock_code, c.etf_code,
               c.weight - COALESCE(p.weight, 0) AS weight_change
        FROM cur c LEFT JOIN prv p
          ON c.stock_code = p.stock_code AND c.etf_code = p.etf_code
    """, [base_date, prev_date]).fetchall()
    for stock_code, etf_code, wc in wc_rows:
        weight_change_map.setdefault(stock_code, {})[etf_code] = wc

    # ETF별 순자금유입 (longest_w 기준)
    etf_flows_rows = conn.execute("""
        SELECT etf_code, SUM(net_units * nav) AS net_amount
        FROM etf_flow WHERE date > ? AND date <= ?
        GROUP BY etf_code
    """, [start_date, base_date]).fetchall()
    etf_flow_map = {code: net for code, net in etf_flows_rows}

    # 단기(short_w) ETF별 순자금유입
    short_start = base_date - timedelta(days=short_w)
    short_flows_rows = conn.execute("""
        SELECT etf_code, SUM(net_units * nav) AS net_amount
        FROM etf_flow WHERE date > ? AND date <= ?
        GROUP BY etf_code
    """, [short_start, base_date]).fetchall()
    short_etf_flow_map = {code: net for code, net in short_flows_rows}

    # 교차 점수 계산
    scored = []
    for code in all_codes:
        window_scores = {}
        window_flows = {}
        stock_name = ""
        breadth_val = 0.0
        depth_val = 0.0
        flow_pct_val = 0.0
        conv_pct_val = 0.0
        for w in windows:
            entry = rankings_by_window[w].get(code)
            if entry:
                window_scores[w] = entry["combined_score"]
                window_flows[w] = entry["flow_score"]
                if not stock_name:
                    stock_name = entry["stock_name"]
                if w == longest_w:
                    breadth_val = entry.get("breadth", 0.0)
                    depth_val = entry.get("depth", 0.0)
                    flow_pct_val = entry.get("flow_percentile", 0.0)
                    conv_pct_val = entry.get("conviction_percentile", 0.0)
            else:
                window_scores[w] = 0.0
                window_flows[w] = 0.0

        # 기본 점수 (장기 가중)
        weight_map = {3: 0.2, 5: 0.35, 10: 0.45}
        cross_score = sum(
            window_scores.get(w, 0) * weight_map.get(w, 0.33) for w in windows
        )

        # 보너스 1: 전 구간 유입
        all_inflow = all(window_flows.get(w, 0) > 0 for w in windows)
        if all_inflow:
            cross_score *= 1.15

        # 보너스 2: 패시브+액티브 동시
        holdings = etf_holdings_map.get(code, [])
        in_passive = any(ec in passive_etf_codes for ec, _ in holdings)
        in_active = any(ec in active_etf_codes for ec, _ in holdings)
        both_types = in_passive and in_active
        if both_types:
            cross_score *= 1.10

        # 추세 판단 (단기→장기 점수 상승)
        scores_list = [window_scores.get(w, 0) for w in sorted(windows)]
        if len(scores_list) >= 2:
            trend_up = all(
                scores_list[i] <= scores_list[i + 1]
                for i in range(len(scores_list) - 1)
            )
            trend_down = all(
                scores_list[i] >= scores_list[i + 1]
                for i in range(len(scores_list) - 1)
            )
        else:
            trend_up = trend_down = False

        # 유입 가속도: 단기 flow / 장기 flow (기간 정규화)
        long_flow = window_flows.get(longest_w, 0)
        short_flow = window_flows.get(short_w, 0)
        if long_flow != 0:
            acceleration = (short_flow / short_w) / (long_flow / longest_w)
        else:
            acceleration = 1.0

        # 비중 변화 합산 (액티브 ETF 비중 변화를 별도 집계)
        wc_by_etf = weight_change_map.get(code, {})
        active_wc_sum = sum(
            wc for ec, wc in wc_by_etf.items()
            if ec in active_etf_codes and wc > 0
        )
        passive_wc_sum = sum(
            wc for ec, wc in wc_by_etf.items()
            if ec in passive_etf_codes and wc > 0
        )
        total_wc_sum = sum(wc for wc in wc_by_etf.values() if wc > 0)

        # 신규 편입 ETF 분류
        prev_etfs = prev_holdings_map.get(code, set())
        passive_count = sum(1 for ec, _ in holdings if ec in passive_etf_codes)
        active_count = sum(1 for ec, _ in holdings if ec in active_etf_codes)
        new_passive_etfs = [
            ec for ec, _ in holdings
            if ec in passive_etf_codes and ec not in prev_etfs
        ]
        new_active_etfs = [
            ec for ec, _ in holdings
            if ec in signal_active_etf_codes and ec not in prev_etfs
        ]

        # 업종 태그 (해당 종목이 속한 섹터 ETF 기준)
        stock_sectors = set()
        for ec, _ in holdings:
            if etf_tier_map.get(ec) == "sector":
                stock_sectors.add(etf_sector_map.get(ec, ""))
        stock_sectors.discard("")

        # ── 매수 성격 태그 결정 ──
        # 🎯 액티브 매수: 펀드매니저 의도적 편입 (신규 or 비중 확대)
        # 💰 자금 유입: 전구간 꾸준한 유입 (패시브+액티브 혼재)
        # 🔄 수급 유입: 자금은 들어오나 패시브 리밸런싱 추정 (지속성 불확실)
        # 🔻 약화: 모멘텀 소멸 or 유출
        pct_gap = flow_pct_val - conv_pct_val  # 양수면 자금 > 확신

        if new_active_etfs or (conv_pct_val >= 70 and active_wc_sum > 0):
            buy_tag = "◆ 액티브 매수"
        elif all_inflow and (flow_pct_val >= 60 or conv_pct_val >= 60):
            buy_tag = "▲ 자금 유입"
        elif pct_gap > 30 and flow_pct_val >= 60:
            buy_tag = "△ 수급 유입"
        elif trend_down or (not all_inflow and long_flow < 0):
            buy_tag = "▼ 약화"
        else:
            buy_tag = ""

        scored.append({
            "stock_code": code,
            "stock_name": stock_name,
            "cross_score": cross_score,
            "window_scores": window_scores,
            "window_flows": window_flows,
            "all_inflow": all_inflow,
            "both_types": both_types,
            "trend_up": trend_up,
            "trend_down": trend_down,
            "acceleration": acceleration,
            "flow_pct": flow_pct_val,
            "conv_pct": conv_pct_val,
            "breadth": breadth_val,
            "depth": depth_val,
            "active_wc_sum": active_wc_sum,
            "passive_wc_sum": passive_wc_sum,
            "total_wc_sum": total_wc_sum,
            "passive_count": passive_count,
            "active_count": active_count,
            "new_passive_etfs": new_passive_etfs,
            "new_active_etfs": new_active_etfs,
            "sectors": stock_sectors,
            "buy_tag": buy_tag,
        })

    scored.sort(key=lambda x: x["cross_score"], reverse=True)

    # ═══ 집계 ═══
    sector_flow: dict[str, float] = {}
    for etf_code, net in etf_flow_map.items():
        sector = etf_sector_map.get(etf_code, "")
        if sector:
            sector_flow[sector] = sector_flow.get(sector, 0) + net

    short_sector: dict[str, float] = {}
    for etf_code, net in short_etf_flow_map.items():
        sec = etf_sector_map.get(etf_code, "")
        if sec:
            short_sector[sec] = short_sector.get(sec, 0) + net

    sorted_sectors = sorted(sector_flow.items(), key=lambda x: x[1], reverse=True)
    inflow_sectors = [(s, v) for s, v in sorted_sectors if v > 0]
    outflow_sectors = [(s, v) for s, v in sorted_sectors if v < 0]

    market_sectors = {"코스피200", "코스닥150", "MSCI Korea", "대형주", "밸류업", "시장대표"}
    market_flow_total = sum(v for s, v in sector_flow.items() if s in market_sectors)
    theme_flow_total = sum(v for s, v in sector_flow.items() if s not in market_sectors)

    passive_total = sum(net for ec, net in etf_flow_map.items() if ec in passive_etf_codes)
    active_total = sum(net for ec, net in etf_flow_map.items() if ec in active_etf_codes)

    top_scored = scored[:top]

    # 단기 유출 전환 섹터
    turning_out = [
        s for s in short_sector
        if short_sector.get(s, 0) < 0 and sector_flow.get(s, 0) > 0
        and s not in market_sectors
    ]

    # ═══ 포맷팅 ═══
    date_str = base_date.strftime("%m/%d")
    lines: list[str] = []

    # ── ① 헤더 + 시장 국면 ──
    lines.append(f"📊 <b>ETF 브리핑</b>  {date_str}")
    lines.append("")

    # 시장 국면: 비율 기반 판단
    total_flow = abs(market_flow_total) + abs(theme_flow_total)
    if total_flow > 0:
        market_ratio = market_flow_total / total_flow   # -1 ~ +1
        theme_ratio = theme_flow_total / total_flow
    else:
        market_ratio = theme_ratio = 0.0

    if market_flow_total > 0 and theme_flow_total > 0:
        if theme_ratio > 0.6:
            regime = "테마 주도 유입 — 업종 순환"
        elif market_ratio > 0.6:
            regime = "지수 중심 유입 — 방어적 베타"
        else:
            regime = "시장·테마 동반 유입 — risk-on"
    elif theme_flow_total > 0 > market_flow_total:
        regime = "테마 ETF 집중 유입 — 업종 순환"
    elif market_flow_total > 0 > theme_flow_total:
        regime = "지수 ETF 집중 유입 — 방어적 베타"
    else:
        regime = "시장·테마 동반 유출 — risk-off"

    lines.append(f"▪ {regime}")
    lines.append(f"▪ 패시브 {passive_total / 1e8:+,.0f}억  액티브 {active_total / 1e8:+,.0f}억")

    # ── ② 섹터 자금 흐름 (인라인) ──
    theme_sectors = [(s, v) for s, v in sorted_sectors if s not in market_sectors]
    if theme_sectors:
        lines.append("")
        lines.append("☑️ <b>섹터</b>")
        parts = []
        for sec_name, sec_val in theme_sectors[:6]:
            sign = "+" if sec_val >= 0 else ""
            short_val = short_sector.get(sec_name, 0)
            accel_mark = ""
            if sec_val > 0 and short_val > 0 and (short_val / sec_val) > 0.5:
                accel_mark = "↑"
            elif sec_name in turning_out:
                accel_mark = "⚠"
            parts.append(f"{sec_name} {sign}{sec_val / 1e8:.0f}억{accel_mark}")
        lines.append("  " + "  |  ".join(parts[:3]))
        if len(parts) > 3:
            lines.append("  " + "  |  ".join(parts[3:6]))

    # ── ③ Top N: 태그 + 10d 누적 + 가속도 ──
    main_scored = [s for s in top_scored if s["buy_tag"] != "△ 수급 유입"]
    passive_scored = [s for s in top_scored if s["buy_tag"] == "△ 수급 유입"]

    lines.append("")
    lines.append(f"🏆 <b>Top {top}</b>")

    for i, s in enumerate(main_scored[:top], 1):
        long_flow_val = s["window_flows"].get(longest_w, 0) / 1e8
        tag = f"  {s['buy_tag']}" if s["buy_tag"] else ""
        accel_str = f"  ×{s['acceleration']:.1f}↑" if s["acceleration"] >= 1.5 and s["all_inflow"] else ""
        sign = "+" if long_flow_val >= 0 else ""
        lines.append(
            f"{i:>2}. <b>{s['stock_name']}</b>{tag}  {sign}{long_flow_val:.0f}억{accel_str}"
        )

    # ── ④ 수급 참고 (패시브 리밸런싱) ──
    if passive_scored:
        lines.append("")
        lines.append("💡 <b>수급 참고</b>  패시브 리밸런싱 추정")
        names_flows = []
        for s in passive_scored[:4]:
            lf = s["window_flows"].get(longest_w, 0) / 1e8
            sign = "+" if lf >= 0 else ""
            names_flows.append(f"{s['stock_name']} {sign}{lf:.0f}억")
        lines.append("  " + "  ·  ".join(names_flows))

    # ── ⑤ 시그널 ──
    signal_lines = []

    active_new_stocks = [s for s in top_scored if s["new_active_etfs"]]
    if active_new_stocks:
        names = [s["stock_name"] for s in active_new_stocks[:3]]
        signal_lines.append(f"액티브 신규 편입: {', '.join(names)}")

    accel_stocks = [s for s in top_scored if s["acceleration"] >= 1.5 and s["all_inflow"]]
    if accel_stocks:
        names = [s["stock_name"] for s in accel_stocks[:3]]
        signal_lines.append(f"매수 가속: {', '.join(names)}")

    if turning_out:
        signal_lines.append(
            f"단기 유출 전환: {', '.join(turning_out[:3])}  ({longest_w}d 유입→{short_w}d 유출)"
        )

    if signal_lines:
        lines.append("")
        lines.append("⚡ <b>시그널</b>")
        for sl in signal_lines:
            lines.append(f"  ▪ {sl}")

    # ── ⑥ 액션 포인트 ──
    hold_themes = [s for s, _ in inflow_sectors[:2] if s not in market_sectors]
    reduce_themes = [s for s, _ in outflow_sectors[-2:] if s not in market_sectors]
    hold_str = "·".join(hold_themes) if hold_themes else "유입 업종"
    reduce_str = "·".join(reduce_themes) if reduce_themes else "유출 업종"

    lines.append("")
    lines.append(f"▪ 비중 유지: {hold_str}  /  축소 검토: {reduce_str}")

    return "\n".join(lines)


def _strip_rich_tags(text: str) -> str:
    """Remove Rich markup tags like [bold], [dim], [/bold] etc."""
    import re
    return re.sub(r"\[/?[a-z]+\]", "", text)

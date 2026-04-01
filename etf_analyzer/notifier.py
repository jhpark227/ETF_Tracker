"""Telegram notification for ETF Analyzer."""
from __future__ import annotations

import os

import httpx


def send_telegram(message: str, parse_mode: str = "HTML") -> bool:
    """Send a message via Telegram Bot API. Returns True on success."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    resp = httpx.post(url, json={
        "chat_id": chat_id,
        "text": message,
        "parse_mode": parse_mode,
    }, timeout=10)
    return resp.status_code == 200


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
    active_etf_codes = {e.code for e in config.etf_universe if e.type == "active"}
    passive_etf_codes = {e.code for e in config.etf_universe if e.type == "passive"}
    etf_sector_map = {e.code: _group_sector(e.sector) for e in config.etf_universe}
    etf_tier_map = {e.code: e.tier for e in config.etf_universe}
    etf_name_map = {e.code: e.name for e in config.etf_universe}

    base_date = conn.execute("SELECT MAX(date) FROM etf_holdings").fetchone()[0]
    if base_date is None:
        return "데이터 없음"

    # 현재 보유 ETF 맵
    etf_holdings_map: dict[str, list[tuple[str, float]]] = {}
    rows = conn.execute(
        "SELECT stock_code, etf_code, weight FROM etf_holdings WHERE date = ?",
        [base_date],
    ).fetchall()
    for stock_code, etf_code, weight in rows:
        etf_holdings_map.setdefault(stock_code, []).append((etf_code, weight))

    # 가장 긴 윈도우 기준 신규 편입 조회
    longest_w = max(windows)
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

    # ETF별 순자금유입 (인사이트용)
    etf_flows_rows = conn.execute("""
        SELECT etf_code, SUM(net_units * nav) AS net_amount
        FROM etf_flow WHERE date > ? AND date <= ?
        GROUP BY etf_code
    """, [start_date, base_date]).fetchall()
    etf_flow_map = {code: net for code, net in etf_flows_rows}

    # 교차 점수 계산
    scored = []
    for code in all_codes:
        window_scores = {}
        window_flows = {}
        stock_name = ""
        breadth_val = 0.0
        for w in windows:
            entry = rankings_by_window[w].get(code)
            if entry:
                window_scores[w] = entry["combined_score"]
                window_flows[w] = entry["flow_score"]
                if not stock_name:
                    stock_name = entry["stock_name"]
                if w == longest_w:
                    breadth_val = entry.get("breadth", 0.0)
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

        # 추세 판단 (단기→장기)
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

        # 패시브/액티브 ETF 수 + 신규 편입
        prev_etfs = prev_holdings_map.get(code, set())
        passive_count = sum(1 for ec, _ in holdings if ec in passive_etf_codes)
        active_count = sum(1 for ec, _ in holdings if ec in active_etf_codes)
        new_passive = sum(
            1 for ec, _ in holdings
            if ec in passive_etf_codes and ec not in prev_etfs
        )
        new_active = sum(
            1 for ec, _ in holdings
            if ec in active_etf_codes and ec not in prev_etfs
        )

        # 편입 ETF 이름 (상위 비중 3개)
        top_etfs = sorted(holdings, key=lambda x: x[1], reverse=True)[:3]
        top_etf_names = [
            etf_name_map.get(ec, ec) for ec, _ in top_etfs
        ]

        # 업종 태그 (해당 종목이 속한 섹터 ETF 기준)
        stock_sectors = set()
        for ec, _ in holdings:
            if etf_tier_map.get(ec) == "sector":
                stock_sectors.add(etf_sector_map.get(ec, ""))
        stock_sectors.discard("")

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
            "passive_count": passive_count,
            "active_count": active_count,
            "new_passive": new_passive,
            "new_active": new_active,
            "breadth": breadth_val,
            "top_etf_names": top_etf_names,
            "sectors": stock_sectors,
        })

    scored.sort(key=lambda x: x["cross_score"], reverse=True)

    # ═══ 분석에 필요한 집계 ═══
    sector_flow: dict[str, float] = {}
    for etf_code, net in etf_flow_map.items():
        sector = etf_sector_map.get(etf_code, "")
        if sector:
            sector_flow[sector] = sector_flow.get(sector, 0) + net

    sorted_sectors = sorted(sector_flow.items(), key=lambda x: x[1], reverse=True)
    inflow_sectors = [(s, v) for s, v in sorted_sectors if v > 0]
    outflow_sectors = [(s, v) for s, v in sorted_sectors if v < 0]

    market_sectors = {"코스피200", "코스닥150", "MSCI Korea", "대형주", "밸류업", "시장대표"}
    market_flow_total = sum(v for s, v in sector_flow.items() if s in market_sectors)
    theme_flow_total = sum(v for s, v in sector_flow.items() if s not in market_sectors)

    passive_total = sum(
        net for ec, net in etf_flow_map.items() if ec in passive_etf_codes
    )
    active_total = sum(
        net for ec, net in etf_flow_map.items() if ec in active_etf_codes
    )

    top_scored = scored[:top]

    # 3d vs 10d 섹터 로테이션
    short_w = min(windows)
    short_start = base_date - timedelta(days=short_w)
    short_flows = conn.execute("""
        SELECT etf_code, SUM(net_units * nav) AS net_amount
        FROM etf_flow WHERE date > ? AND date <= ?
        GROUP BY etf_code
    """, [short_start, base_date]).fetchall()
    short_sector: dict[str, float] = {}
    for ec, net in short_flows:
        sec = etf_sector_map.get(ec, "")
        if sec:
            short_sector[sec] = short_sector.get(sec, 0) + net

    turning_out = [
        s for s in short_sector
        if short_sector.get(s, 0) < 0 and sector_flow.get(s, 0) > 0
    ]

    # 상위 종목 공통 섹터 분석
    top_sector_count: dict[str, int] = {}
    for s in top_scored:
        for sec in s["sectors"]:
            top_sector_count[sec] = top_sector_count.get(sec, 0) + 1
    # 섹터 없는 종목의 ETF tier로 스타일 추론
    top_style_count: dict[str, int] = {}
    for s in top_scored:
        for ec, _ in etf_holdings_map.get(s["stock_code"], []):
            tier = etf_tier_map.get(ec, "")
            sec = etf_sector_map.get(ec, "")
            if tier == "strategy":
                top_style_count[sec] = top_style_count.get(sec, 0) + 1

    # ═══ 포맷팅: 5단 구성 ═══
    date_str = base_date.strftime("%m/%d")
    lines: list[str] = []

    # ── ① 오늘의 시장 성격 ──
    lines.append(f"📊 <b>ETF 자금흐름 브리핑</b>  {date_str}")
    lines.append("")

    # 시장 국면 판단
    if market_flow_total > 0 and theme_flow_total > 0:
        regime = "시장·테마 동반 유입, 위험선호(risk-on) 국면"
    elif theme_flow_total > 0 > market_flow_total:
        regime = "테마 ETF로 자금 쏠림, 업종 순환 장세"
    elif market_flow_total > 0 > theme_flow_total:
        regime = "지수 ETF 중심 유입, 방어적 베타 추종"
    else:
        regime = "시장·테마 동반 유출, 위험회피(risk-off) 국면"

    # 주도 스타일 판단
    top_inflow_names = [s for s, _ in inflow_sectors[:2]]
    pa_desc = ""
    if passive_total > 0 and active_total > 0:
        pa_desc = "패시브·액티브 동반 유입"
    elif active_total > 0 > passive_total:
        pa_desc = "액티브만 유입 (선별적 장세)"
    elif passive_total > 0 > active_total:
        pa_desc = "패시브 중심 유입 (펀드매니저 신중)"
    else:
        pa_desc = "양쪽 모두 유출"

    lines.append(f"{regime}")
    if top_inflow_names:
        lines.append(
            f"자금 집중: {', '.join(top_inflow_names)} | {pa_desc}"
        )
    lines.append(
        f"패시브 {passive_total / 1e8:+,.0f}억 · 액티브 {active_total / 1e8:+,.0f}억"
    )

    # ── ② 이번 주 주도 테마 ──
    lines.append("")
    lines.append("🎯 <b>주도 테마</b>")

    # 유입 상위 섹터 3개에서 대표 종목 + 대표 ETF 매칭
    theme_count = 0
    used_sectors: set[str] = set()
    for sec_name, sec_flow_val in inflow_sectors:
        if theme_count >= 3:
            break
        if sec_name in market_sectors:
            continue  # 시장 전체 섹터는 테마가 아님
        used_sectors.add(sec_name)

        # 대표 종목: 해당 섹터 ETF에 편입된 상위 랭킹 종목
        rep_stock = ""
        for s in top_scored:
            if sec_name in s["sectors"]:
                rep_stock = s["stock_name"]
                break
        if not rep_stock:
            # 전체 scored에서 탐색
            for s in scored[:30]:
                if sec_name in s["sectors"]:
                    rep_stock = s["stock_name"]
                    break

        # 대표 ETF: 해당 섹터 ETF 중 유입 최대
        rep_etf = ""
        sec_etfs = [
            (ec, etf_flow_map.get(ec, 0))
            for ec in etf_sector_map
            if etf_sector_map[ec] == sec_name
        ]
        sec_etfs.sort(key=lambda x: x[1], reverse=True)
        if sec_etfs:
            rep_etf = etf_name_map.get(sec_etfs[0][0], "")

        # 이유 생성
        flow_billions = sec_flow_val / 1e8
        reason = f"{flow_billions:+,.0f}억 유입"
        # 단기 가속 여부
        short_val = short_sector.get(sec_name, 0)
        if short_val > 0 and sec_flow_val > 0:
            short_ratio = short_val / sec_flow_val if sec_flow_val else 0
            if short_ratio > 0.5:
                reason += ", 단기 가속 중"

        stock_part = rep_stock if rep_stock else "—"
        etf_part = rep_etf if rep_etf else "—"
        lines.append(f"• <b>{sec_name}</b>: {stock_part} / {etf_part} — {reason}")
        theme_count += 1

    # 테마가 3개 미만이면 전략 스타일에서 보충
    if theme_count < 3 and top_style_count:
        for style, cnt in sorted(top_style_count.items(), key=lambda x: -x[1]):
            if theme_count >= 3 or style in used_sectors:
                continue
            style_flow = sector_flow.get(style, 0)
            if style_flow <= 0:
                continue
            rep_stock = ""
            for s in top_scored:
                holdings = etf_holdings_map.get(s["stock_code"], [])
                for ec, _ in holdings:
                    if etf_sector_map.get(ec) == style:
                        rep_stock = s["stock_name"]
                        break
                if rep_stock:
                    break
            rep_etf = ""
            style_etfs = [
                (ec, etf_flow_map.get(ec, 0))
                for ec in etf_sector_map if etf_sector_map[ec] == style
            ]
            style_etfs.sort(key=lambda x: x[1], reverse=True)
            if style_etfs:
                rep_etf = etf_name_map.get(style_etfs[0][0], "")
            stock_part = rep_stock if rep_stock else "—"
            etf_part = rep_etf if rep_etf else "—"
            lines.append(
                f"• <b>{style}</b>: {stock_part} / {etf_part} "
                f"— {style_flow / 1e8:+,.0f}억, 상위권 {cnt}종목 편입"
            )
            theme_count += 1

    # ── ③ Top 10 랭킹 ──
    lines.append("")
    lines.append("🏆 <b>Top 10</b>")

    for i, s in enumerate(top_scored, 1):
        flow_val = s["window_flows"].get(longest_w, 0) / 1e8

        # 추세 신호
        if s["all_inflow"] and s["trend_up"]:
            signal = "▲ 강화"
        elif s["all_inflow"]:
            signal = "▲ 유입"
        elif s["trend_down"]:
            signal = "▼ 약화"
        else:
            signal = "─"

        # ETF 편입 (신규 포함)
        p_new = f"(+{s['new_passive']})" if s["new_passive"] else ""
        a_new = f"(+{s['new_active']})" if s["new_active"] else ""
        etf_str = f"P:{s['passive_count']}{p_new} A:{s['active_count']}{a_new}"

        lines.append(
            f"{i:>2}. <b>{s['stock_name']}</b> | "
            f"{s['cross_score']:.0f}점 | "
            f"{flow_val:+,.0f}억 | "
            f"{signal} | {etf_str}"
        )

    # 공통 특징 한 줄
    all_inflow_count = sum(1 for s in top_scored if s["all_inflow"])
    both_count = sum(1 for s in top_scored if s["both_types"])
    dominant_sectors = sorted(top_sector_count.items(), key=lambda x: -x[1])
    dominant_styles = sorted(top_style_count.items(), key=lambda x: -x[1])

    common_parts = []
    if dominant_sectors:
        common_parts.append(dominant_sectors[0][0])
    if dominant_styles:
        common_parts.append(dominant_styles[0][0])
    common_theme = "+".join(common_parts) if common_parts else "다양한 업종"

    lines.append(
        f"→ 상위권 {common_theme} 중심, "
        f"전구간유입 {all_inflow_count}/{top} · P+A {both_count}/{top}"
    )

    # ── ④ 주목할 시그널 ──
    trend_up_stocks = [s for s in top_scored if s["trend_up"]]
    if trend_up_stocks or turning_out:
        lines.append("")
        lines.append("⚡ <b>주목 시그널</b>")

        if trend_up_stocks:
            names = [s["stock_name"] for s in trend_up_stocks[:3]]
            lines.append(
                f"추세 강화: {', '.join(names)} "
                f"— 단기→장기 점수 상승, 자금 유입 가속 구간"
            )

        if turning_out:
            lines.append(
                f"⚠️ 단기 유출 전환: {', '.join(turning_out[:3])} "
                f"— {longest_w}d 유입이었으나 {short_w}d 유출, "
                f"차익실현 또는 로테이션 가능성"
            )

    # ── ⑤ 한줄 액션 포인트 ──
    lines.append("")

    # 유지 추천: 유입 상위 2개 테마
    hold_themes = [s for s, _ in inflow_sectors[:2] if s not in market_sectors]
    # 축소 고려: 유출 하위 2개
    reduce_themes = [s for s, _ in outflow_sectors[-2:] if s not in market_sectors]

    hold_str = "·".join(hold_themes) if hold_themes else "상위 유입 업종"
    reduce_str = "·".join(reduce_themes) if reduce_themes else "유출 업종"

    lines.append(
        f"🎯 {hold_str} 비중 유지, {reduce_str} 단기 비중 축소 고려"
    )

    return "\n".join(lines)


def _strip_rich_tags(text: str) -> str:
    """Remove Rich markup tags like [bold], [dim], [/bold] etc."""
    import re
    return re.sub(r"\[/?[a-z]+\]", "", text)

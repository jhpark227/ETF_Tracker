from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table
from rich.tree import Tree

from etf_analyzer.config import load_config

app = typer.Typer(name="etf-analyzer", help="Korean ETF flow tracking system")
console = Console()


@app.callback()
def callback() -> None:
    """Korean ETF flow tracking system."""


@app.command()
def seed(
    days: int = typer.Option(10, "--days", "-d", help="Number of trading days to generate"),
    config_path: Optional[Path] = typer.Option(None, "--config", help="Config file path"),
) -> None:
    """Seed database with realistic mock data for testing."""
    import random
    from etf_analyzer.storage.db import get_connection
    from etf_analyzer.storage.schema import create_tables
    from etf_analyzer.storage.repository import (
        upsert_etf_master, upsert_holdings, upsert_flow, upsert_stock_master,
    )

    config = load_config(config_path)

    # Seed uses a separate temporary DB to avoid overwriting real data
    db_path = Path(config.database.path)
    seed_db_path = str(db_path.parent / f"{db_path.stem}_seed{db_path.suffix}")
    conn = get_connection(seed_db_path)
    create_tables(conn)

    console.print(f"[dim]Seed DB: {seed_db_path}[/dim]")

    # Register ETF universe
    for etf in config.etf_universe:
        upsert_etf_master(conn, etf.code, etf.name, None, etf.type, tier=etf.tier)

    # Mock stock universe (Korean blue chips)
    stocks = [
        ("005930", "삼성전자"),
        ("000660", "SK하이닉스"),
        ("035420", "NAVER"),
        ("005380", "현대차"),
        ("051910", "LG화학"),
        ("006400", "삼성SDI"),
        ("035720", "카카오"),
        ("207940", "삼성바이오로직스"),
        ("068270", "셀트리온"),
        ("105560", "KB금융"),
        ("055550", "신한지주"),
        ("096770", "SK이노베이션"),
        ("034730", "SK"),
        ("028260", "삼성물산"),
        ("030200", "KT"),
        ("009150", "삼성전기"),
        ("066570", "LG전자"),
        ("003670", "포스코퓨처엠"),
        ("373220", "LG에너지솔루션"),
        ("012330", "현대모비스"),
        ("010130", "고려아연"),
        ("086790", "하나금융지주"),
        ("316140", "우리금융지주"),
        ("032830", "삼성생명"),
        ("047050", "포스코인터내셔널"),
        ("042700", "한미반도체"),
        ("402340", "SK스퀘어"),
        ("011200", "HMM"),
        ("329180", "HD현대중공업"),
        ("267250", "HD현대"),
        ("042660", "한화오션"),
        ("009540", "HD한국조선해양"),
        ("272210", "한화시스템"),
        ("012450", "한화에어로스페이스"),
        ("298040", "효성중공업"),
        ("267260", "HD현대일렉트릭"),
        ("003490", "대한항공"),
        ("090430", "아모레퍼시픽"),
        ("285130", "SK케미칼"),
        ("196170", "알테오젠"),
        ("326030", "SK바이오팜"),
        ("003850", "보령"),
        ("377300", "카카오페이"),
        ("259960", "크래프톤"),
        ("058470", "리노공업"),
    ]

    stock_dict = dict(stocks)
    for code, name in stocks:
        upsert_stock_master(conn, code, name)

    # ETF composition weights
    etf_bases = {
        # --- Tier 1: 광역 시장 ---
        "069500": {  # KODEX 200
            "005930": 26.0, "000660": 9.0, "035420": 4.5, "005380": 3.8,
            "051910": 3.2, "006400": 2.8, "207940": 2.5, "068270": 2.0,
            "105560": 1.8, "055550": 1.6, "034730": 1.4, "028260": 1.2,
        },
        "229200": {  # KODEX 코스닥150
            "035420": 8.0, "035720": 7.5, "068270": 6.0, "196170": 5.5,
            "326030": 4.0, "259960": 3.5, "377300": 3.0, "058470": 2.5,
        },
        "310970": {  # TIGER MSCI Korea TR
            "005930": 24.0, "000660": 8.5, "035420": 4.0, "005380": 3.5,
            "207940": 2.8, "051910": 2.5, "068270": 2.0, "105560": 1.8,
        },
        "292150": {  # TIGER 코리아TOP10
            "005930": 30.0, "000660": 15.0, "207940": 8.0, "005380": 7.0,
            "035420": 6.0, "068270": 5.0, "051910": 4.5, "006400": 4.0,
        },
        "495050": {  # RISE 코리아밸류업
            "005930": 18.0, "000660": 7.0, "105560": 5.0, "055550": 4.5,
            "086790": 4.0, "005380": 3.5, "316140": 3.0, "028260": 2.5,
        },
        # --- Tier 2: 업종/테마 ---
        "396500": {  # TIGER 반도체TOP10
            "005930": 28.0, "000660": 25.0, "042700": 10.0, "009150": 8.0,
            "402340": 6.0, "058470": 5.0,
        },
        "395160": {  # KODEX AI반도체
            "000660": 22.0, "005930": 18.0, "042700": 12.0, "058470": 8.0,
            "009150": 7.0, "402340": 5.0,
        },
        "305720": {  # KODEX 2차전지산업
            "006400": 22.0, "051910": 18.0, "373220": 15.0, "096770": 10.0,
            "003670": 8.0, "285130": 5.0,
        },
        "449450": {  # PLUS K방산
            "012450": 25.0, "272210": 15.0, "042660": 12.0, "003490": 10.0,
            "047050": 8.0, "267250": 6.0,
        },
        "466920": {  # SOL 조선TOP3플러스
            "329180": 30.0, "009540": 25.0, "042660": 20.0,
            "011200": 8.0, "267250": 5.0,
        },
        "487240": {  # KODEX AI전력핵심설비
            "298040": 20.0, "267260": 18.0, "066570": 12.0,
            "009150": 8.0, "005930": 6.0, "000660": 5.0,
        },
        "434730": {  # HANARO 원자력iSelect
            "298040": 22.0, "267260": 18.0, "066570": 10.0,
            "034730": 8.0, "005930": 5.0,
        },
        "102970": {  # KODEX 증권
            "105560": 15.0, "055550": 14.0, "086790": 12.0,
            "316140": 10.0, "032830": 8.0,
        },
        "091180": {  # KODEX 자동차
            "005380": 25.0, "012330": 18.0, "066570": 12.0,
            "009150": 8.0, "010130": 6.0,
        },
        "228790": {  # TIGER 화장품
            "090430": 25.0, "068270": 15.0, "003850": 10.0,
            "285130": 8.0, "035720": 5.0,
        },
        # --- Tier 3: 전략/스타일 ---
        "161510": {  # PLUS 고배당주
            "105560": 12.0, "055550": 11.0, "005380": 9.0, "030200": 8.0,
            "005930": 7.0, "028260": 6.0, "086790": 5.0,
        },
        "102780": {  # KODEX 삼성그룹
            "005930": 35.0, "009150": 8.0, "032830": 6.0, "028260": 5.0,
            "006400": 4.5, "207940": 4.0,
        },
        "138540": {  # TIGER 현대차그룹플러스
            "005380": 30.0, "012330": 20.0, "267250": 10.0,
            "329180": 8.0, "009540": 6.0,
        },
        "466940": {  # TIGER 은행고배당플러스TOP10
            "105560": 18.0, "055550": 16.0, "086790": 14.0,
            "316140": 12.0, "032830": 10.0,
        },
        # --- Tier 4: 액티브 ---
        "0163Y0": {  # KoAct 코스닥액티브
            "196170": 8.0, "035720": 7.0, "259960": 6.0, "326030": 5.5,
            "377300": 5.0, "058470": 4.5, "068270": 4.0,
        },
        "445290": {  # KODEX 로봇액티브
            "042700": 12.0, "298040": 10.0, "267260": 9.0,
            "066570": 8.0, "272210": 7.0, "009150": 6.0,
        },
        "444200": {  # SOL 코리아메가테크액티브
            "005930": 15.0, "000660": 12.0, "035420": 8.0,
            "259960": 7.0, "207940": 6.0, "068270": 5.0,
        },
        "441800": {  # TIME Korea플러스배당액티브
            "005930": 12.0, "035420": 10.0, "105560": 8.0, "055550": 7.0,
            "000660": 6.0, "005380": 5.0, "086790": 4.0,
        },
        "462900": {  # KoAct 바이오헬스케어액티브
            "207940": 15.0, "068270": 12.0, "196170": 10.0,
            "326030": 8.0, "003850": 6.0, "285130": 5.0,
        },
        "494890": {  # KODEX 200액티브
            "005930": 25.0, "000660": 10.0, "035420": 5.0, "005380": 4.0,
            "207940": 3.5, "051910": 3.0, "068270": 2.5, "105560": 2.0,
        },
    }

    # NAV per ETF (approximate)
    etf_nav = {
        "069500": 35000.0, "229200": 7500.0, "310970": 15000.0,
        "292150": 14000.0, "495050": 12000.0,
        "396500": 22000.0, "395160": 18000.0, "305720": 14000.0,
        "449450": 16000.0, "466920": 20000.0, "487240": 13000.0,
        "434730": 11000.0, "102970": 8000.0, "091180": 10000.0,
        "228790": 9000.0,
        "161510": 9000.0, "102780": 8500.0, "138540": 12000.0,
        "466940": 10000.0,
        "0163Y0": 11000.0, "445290": 15000.0, "444200": 13000.0,
        "441800": 11000.0, "462900": 12000.0, "494890": 14000.0,
    }
    # Listed shares per ETF (approximate)
    etf_shares = {
        "069500": 19_800_000, "229200": 10_000_000, "310970": 8_000_000,
        "292150": 5_000_000, "495050": 3_000_000,
        "396500": 12_000_000, "395160": 4_000_000, "305720": 8_000_000,
        "449450": 3_500_000, "466920": 3_000_000, "487240": 4_500_000,
        "434730": 2_500_000, "102970": 4_000_000, "091180": 2_000_000,
        "228790": 1_500_000,
        "161510": 5_000_000, "102780": 6_000_000, "138540": 2_000_000,
        "466940": 3_000_000,
        "0163Y0": 3_000_000, "445290": 2_500_000, "444200": 2_000_000,
        "441800": 2_500_000, "462900": 2_000_000, "494890": 2_500_000,
    }

    # Generate trading days (skip weekends)
    base = date(2026, 3, 18)
    trading_days = []
    d = base - timedelta(days=days * 2)
    while len(trading_days) < days:
        if d.weekday() < 5:
            trading_days.append(d)
        d += timedelta(days=1)

    random.seed(42)
    holdings_rows = []
    flow_rows = []

    for i, tdate in enumerate(trading_days):
        for etf_code, base_weights in etf_bases.items():
            etf_type = next(e.type for e in config.etf_universe if e.code == etf_code)
            # Active ETFs drift weights more
            drift = 0.3 if etf_type == "active" else 0.1

            for stock_code, base_w in base_weights.items():
                # Trending drift: slight upward trend for first 2 stocks in each ETF
                trend = 0.05 * i if list(base_weights).index(stock_code) == 0 else 0.0
                w = max(0.5, base_w + trend + random.uniform(-drift, drift))
                shares = int(w * 100)
                holdings_rows.append({
                    "date": tdate,
                    "etf_code": etf_code,
                    "stock_code": stock_code,
                    "stock_name": stock_dict[stock_code],
                    "weight": round(w, 2),
                    "shares": shares,
                })

            # Flow: net units from listed shares change
            base_sh = etf_shares[etf_code]
            # Simulate net inflow trend (positive bias)
            delta = int(random.gauss(10000, 30000))
            prev_shares = base_sh + int(i * 5000)
            today_shares = prev_shares + delta
            net_units = delta
            creation = max(0, net_units)
            redemption = abs(min(0, net_units))
            nav = etf_nav[etf_code] * (1 + 0.001 * random.gauss(0, 1))

            flow_rows.append({
                "date": tdate,
                "etf_code": etf_code,
                "creation_units": creation,
                "redemption_units": redemption,
                "net_units": net_units,
                "nav": round(nav, 2),
            })

    upsert_holdings(conn, holdings_rows)
    upsert_flow(conn, flow_rows)
    conn.close()

    console.print(f"[green]Seeded {days} days × {len(etf_bases)} ETFs[/green]")
    console.print(f"[green]{len(holdings_rows)} holding rows, {len(flow_rows)} flow rows[/green]")
    console.print(f"[dim]DB: {seed_db_path}[/dim]")


@app.command()
def init(
    config_path: Optional[Path] = typer.Option(None, "--config", help="Config file path"),
) -> None:
    """Initialize database and register ETF universe."""
    from etf_analyzer.storage.db import get_connection
    from etf_analyzer.storage.schema import create_tables
    from etf_analyzer.storage.repository import upsert_etf_master

    config = load_config(config_path)
    conn = get_connection(config.database.path)
    create_tables(conn)

    for etf in config.etf_universe:
        upsert_etf_master(conn, etf.code, etf.name, None, etf.type, tier=etf.tier)

    conn.close()
    console.print(f"[green]DB initialized at {config.database.path}[/green]")
    console.print(f"[green]Registered {len(config.etf_universe)} ETFs[/green]")


@app.command()
def status(
    config_path: Optional[Path] = typer.Option(None, "--config", help="Config file path"),
) -> None:
    """Show collection status."""
    from etf_analyzer.storage.db import get_connection
    from etf_analyzer.storage.repository import get_collection_status

    config = load_config(config_path)
    conn = get_connection(config.database.path)
    s = get_collection_status(conn)
    conn.close()

    if s["total_days"] == 0:
        console.print("[yellow]No data collected yet. Run 'collect' first.[/yellow]")
        return

    table = Table(title="Collection Status")
    table.add_column("Metric", style="bold")
    table.add_column("Value")
    table.add_row("Total days", str(s["total_days"]))
    table.add_row("First date", str(s["first_date"]))
    table.add_row("Last date", str(s["last_date"]))
    table.add_row("ETF count", str(s["etf_count"]))
    console.print(table)


@app.command()
def collect(
    today: bool = typer.Option(False, "--today", help="Collect today's data"),
    date_str: Optional[str] = typer.Option(None, "--date", help="Date to collect (YYYY-MM-DD)"),
    from_str: Optional[str] = typer.Option(None, "--from", help="Start date for backfill"),
    to_str: Optional[str] = typer.Option(None, "--to", help="End date for backfill"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
    config_path: Optional[Path] = typer.Option(None, "--config", help="Config file path"),
) -> None:
    """Collect ETF data: holdings from KIS, flow from KRX OpenAPI."""
    from dotenv import load_dotenv
    from etf_analyzer.collector.kis import KISClient, KRXFlowClient
    from etf_analyzer.storage.db import get_connection
    from etf_analyzer.storage.schema import create_tables
    from etf_analyzer.storage.repository import upsert_holdings, upsert_flow, upsert_stock_master

    load_dotenv()

    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    config = load_config(config_path)
    conn = get_connection(config.database.path)
    create_tables(conn)

    # Determine dates to collect
    if today:
        dates = [date.today()]
    elif date_str:
        dates = [datetime.strptime(date_str, "%Y-%m-%d").date()]
    elif from_str and to_str:
        start = datetime.strptime(from_str, "%Y-%m-%d").date()
        end = datetime.strptime(to_str, "%Y-%m-%d").date()
        dates = []
        current = start
        while current <= end:
            if current.weekday() < 5:
                dates.append(current)
            current += timedelta(days=1)
    else:
        console.print("[red]Specify --today, --date, or --from/--to[/red]")
        raise typer.Exit(1)

    # Init clients
    try:
        kis = KISClient()
        console.print("[green]KIS 토큰 OK[/green]")
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        conn.close()
        raise typer.Exit(1)

    try:
        krx = KRXFlowClient()
        console.print("[green]KRX API OK[/green]")
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        kis.close()
        conn.close()
        raise typer.Exit(1)

    target_etfs = config.etf_universe
    etf_codes = [etf.code for etf in target_etfs]

    console.print(f"[bold]Target: {len(target_etfs)} ETFs[/bold]")

    successes = []
    failures = []

    for target_date in dates:
        console.print(f"\n[bold]Collecting {target_date}...[/bold]")

        # Holdings from KIS (per ETF)
        for etf in target_etfs:
            try:
                holdings = kis.fetch_holdings(etf.code, target_date)
                if holdings:
                    upsert_holdings(conn, [h.model_dump() for h in holdings])
                    for h in holdings:
                        upsert_stock_master(conn, h.stock_code, h.stock_name)
                    console.print(f"  [green]{etf.name}: {len(holdings)} holdings[/green]")
                else:
                    console.print(f"  [yellow]{etf.name}: no holdings data[/yellow]")
            except Exception as e:
                console.print(f"  [red]{etf.name} holdings: {e}[/red]")
                failures.append((target_date, etf.code, str(e)))

        # Flow from KRX (batch — 2 API calls for all ETFs)
        try:
            flow_batch = krx.fetch_flow_batch(etf_codes, target_date)
            for etf in target_etfs:
                flows = flow_batch.get(etf.code, [])
                if flows:
                    upsert_flow(conn, [f.model_dump() for f in flows])
                    f = flows[0]
                    console.print(
                        f"  [green]{etf.name}: flow {f.net_units:+,} units, NAV {f.nav:,.0f}[/green]"
                    )
                    successes.append((target_date, etf.code))
                else:
                    console.print(f"  [yellow]{etf.name}: no flow data[/yellow]")
        except Exception as e:
            console.print(f"  [red]Flow fetch failed: {e}[/red]")
            for etf in target_etfs:
                failures.append((target_date, etf.code, str(e)))

    kis.close()
    krx.close()
    conn.close()

    console.print(f"\n[bold]Done:[/bold] {len(successes)} OK, {len(failures)} failed")
    if failures:
        for d, code, err in failures:
            console.print(f"  [red]FAIL: {d} {code} — {err}[/red]")


@app.command()
def rank(
    window: str = typer.Option("5", "--window", "-w", help="Analysis window in days (comma-separated for multiple)"),
    top: int = typer.Option(20, "--top", "-n", help="Number of top stocks to show"),
    config_path: Optional[Path] = typer.Option(None, "--config", help="Config file path"),
) -> None:
    """Show stock rankings by combined score."""
    from etf_analyzer.analyzer.ranking import calculate_combined_ranking, summarize_ranking
    from etf_analyzer.storage.db import get_connection

    config = load_config(config_path)
    conn = get_connection(config.database.path)

    windows = [int(w.strip()) for w in window.split(",")]

    for w in windows:
        ranking = calculate_combined_ranking(
            conn, window=w,
            flow_weight=config.analyzer.flow_weight,
            conviction_weight=config.analyzer.conviction_weight,
            active_multiplier=config.analyzer.active_etf_multiplier,
        )

        if not ranking:
            console.print(f"[yellow]No data for {w}-day window.[/yellow]")
            continue

        table = Table(title=f"Top {top} Stocks — {w}d window")
        table.add_column("#", style="dim", width=4)
        table.add_column("Code", style="bold")
        table.add_column("Name")
        table.add_column("Combined", justify="right")
        table.add_column("Flow ₩", justify="right")
        table.add_column("Conviction", justify="right")
        table.add_column("Breadth", justify="right")

        for i, stock in enumerate(ranking[:top], 1):
            flow_str = f"{stock['flow_score']:,.0f}"
            table.add_row(
                str(i),
                stock["stock_code"],
                stock["stock_name"],
                f"{stock['combined_score']:.1f}",
                flow_str,
                f"{stock['conviction_score']:.3f}",
                f"{stock['breadth']:.0%}",
            )

        console.print(table)

        # 결과 해석
        summary = summarize_ranking(conn, ranking, window=w, top=top, etf_universe=config.etf_universe)
        if summary:
            console.print()
            for line in summary:
                console.print(line)
        console.print()

    conn.close()


@app.command()
def detail(
    stock_code: str = typer.Argument(help="Stock code to analyze"),
    window: str = typer.Option("5", "--window", "-w", help="Analysis window (comma-separated)"),
    config_path: Optional[Path] = typer.Option(None, "--config", help="Config file path"),
) -> None:
    """Show detailed analysis for a stock."""
    from etf_analyzer.analyzer.ranking import get_stock_detail
    from etf_analyzer.storage.db import get_connection

    config = load_config(config_path)
    conn = get_connection(config.database.path)

    windows = [int(w.strip()) for w in window.split(",")]

    for w in windows:
        d = get_stock_detail(conn, stock_code=stock_code, window=w)

        if not d:
            console.print(f"[yellow]No data for {stock_code}.[/yellow]")
            continue

        tree = Tree(f"[bold]{d['stock_name']} ({d['stock_code']})[/bold] — {w}일")

        # Flow breakdown
        total_flow = sum(e["flow_contribution"] for e in d["etf_breakdown"])
        flow_branch = tree.add(f"자금유입: {total_flow / 1e8:+,.1f}억원")
        for etf in d["etf_breakdown"]:
            marker = " ★" if etf["etf_type"] == "active" else ""
            flow_branch.add(
                f"{etf['etf_name']}: {etf['flow_contribution'] / 1e8:+,.1f}억 "
                f"(비중 {etf['current_weight']:.1f}%){marker}"
            )

        # Conviction breakdown
        conv_branch = tree.add("확신도")
        for etf in d["etf_breakdown"]:
            direction = "+" if etf["weight_change"] > 0 else ""
            marker = " ★ (액티브)" if etf["etf_type"] == "active" else ""
            conv_branch.add(
                f"{etf['etf_name']}: {direction}{etf['weight_change']:.1f}%p{marker}"
            )

        # Weight trend
        if d["weight_trend"]:
            trend_str = " → ".join(f"{t['weight']:.1f}%" for t in d["weight_trend"])
            tree.add(f"비중 추이: {trend_str}")

        console.print(tree)
        console.print()

    conn.close()


@app.command()
def etf(
    etf_code: str = typer.Argument(help="ETF code to analyze"),
    window: str = typer.Option("5", "--window", "-w", help="Analysis window"),
    top: int = typer.Option(10, "--top", "-n", help="Number of top holdings to show"),
    config_path: Optional[Path] = typer.Option(None, "--config", help="Config file path"),
) -> None:
    """Show ETF flow summary and top holdings changes."""
    from etf_analyzer.storage.db import get_connection
    from etf_analyzer.storage.repository import get_flow

    config = load_config(config_path)
    conn = get_connection(config.database.path)
    w = int(window)

    # ETF info
    etf_info = conn.execute(
        "SELECT etf_name, etf_type FROM etf_master WHERE etf_code = ?", [etf_code]
    ).fetchone()

    if not etf_info:
        console.print(f"[red]ETF {etf_code} not found.[/red]")
        conn.close()
        raise typer.Exit(1)

    console.print(f"\n[bold]{etf_info[0]} ({etf_code})[/bold] — {etf_info[1]}")

    # Flow summary
    base_date = conn.execute("SELECT MAX(date) FROM etf_holdings").fetchone()[0]
    if base_date is None:
        console.print("[yellow]No data.[/yellow]")
        conn.close()
        return

    from datetime import timedelta
    start_date = base_date - timedelta(days=w)
    flow_data = get_flow(conn, start_date, base_date, etf_code)
    total_net = sum(f["net_units"] * f["nav"] for f in flow_data)
    console.print(f"순자금유입 ({w}일): [bold]{total_net / 1e8:+,.1f}억원[/bold]")

    # Top weight changes
    changes = conn.execute("""
        WITH end_w AS (
            SELECT stock_code, stock_name, weight
            FROM etf_holdings WHERE date = ? AND etf_code = ?
        ),
        start_w AS (
            SELECT stock_code, weight
            FROM etf_holdings
            WHERE date = (SELECT MAX(date) FROM etf_holdings WHERE date <= ? AND etf_code = ?)
              AND etf_code = ?
        )
        SELECT e.stock_code, e.stock_name, e.weight,
               e.weight - COALESCE(s.weight, 0) AS weight_change
        FROM end_w e LEFT JOIN start_w s ON e.stock_code = s.stock_code
        ORDER BY ABS(e.weight - COALESCE(s.weight, 0)) DESC
        LIMIT ?
    """, [base_date, etf_code, start_date, etf_code, etf_code, top]).fetchall()

    table = Table(title=f"Top {top} Weight Changes ({w}d)")
    table.add_column("Code")
    table.add_column("Name")
    table.add_column("Weight", justify="right")
    table.add_column("Change", justify="right")

    for row in changes:
        change_str = f"{row[3]:+.2f}%p"
        style = "green" if row[3] > 0 else "red" if row[3] < 0 else "dim"
        table.add_row(row[0], row[1], f"{row[2]:.2f}%", f"[{style}]{change_str}[/{style}]")

    console.print(table)
    conn.close()

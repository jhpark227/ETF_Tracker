from __future__ import annotations

from datetime import date

import duckdb


def upsert_etf_master(
    conn: duckdb.DuckDBPyConnection,
    etf_code: str,
    etf_name: str,
    manager: str | None,
    etf_type: str,
    benchmark: str | None = None,
    tier: str | None = None,
) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO etf_master VALUES (?, ?, ?, ?, ?, ?)",
        [etf_code, etf_name, manager, etf_type, benchmark, tier],
    )


def upsert_holdings(conn: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    for row in rows:
        conn.execute(
            "INSERT OR REPLACE INTO etf_holdings VALUES (?, ?, ?, ?, ?, ?)",
            [row["date"], row["etf_code"], row["stock_code"],
             row["stock_name"], row["weight"], row["shares"]],
        )


def upsert_flow(conn: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    for row in rows:
        conn.execute(
            "INSERT OR REPLACE INTO etf_flow VALUES (?, ?, ?, ?, ?, ?)",
            [row["date"], row["etf_code"], row["creation_units"],
             row["redemption_units"], row["net_units"], row["nav"]],
        )


def upsert_stock_master(
    conn: duckdb.DuckDBPyConnection,
    stock_code: str,
    stock_name: str,
) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO stock_master VALUES (?, ?)",
        [stock_code, stock_name],
    )


def get_holdings(
    conn: duckdb.DuckDBPyConnection,
    target_date: date,
    etf_code: str | None = None,
) -> list[dict]:
    query = "SELECT * FROM etf_holdings WHERE date = ?"
    params: list = [target_date]
    if etf_code:
        query += " AND etf_code = ?"
        params.append(etf_code)
    rows = conn.execute(query, params).fetchall()
    columns = ["date", "etf_code", "stock_code", "stock_name", "weight", "shares"]
    return [dict(zip(columns, row)) for row in rows]


def get_flow(
    conn: duckdb.DuckDBPyConnection,
    from_date: date,
    to_date: date,
    etf_code: str | None = None,
) -> list[dict]:
    query = "SELECT * FROM etf_flow WHERE date >= ? AND date <= ?"
    params: list = [from_date, to_date]
    if etf_code:
        query += " AND etf_code = ?"
        params.append(etf_code)
    rows = conn.execute(query, params).fetchall()
    columns = ["date", "etf_code", "creation_units", "redemption_units", "net_units", "nav"]
    return [dict(zip(columns, row)) for row in rows]


def get_collection_status(
    conn: duckdb.DuckDBPyConnection,
) -> dict:
    result = conn.execute("""
        SELECT
            COUNT(DISTINCT date) as total_days,
            MIN(date) as first_date,
            MAX(date) as last_date,
            COUNT(DISTINCT etf_code) as etf_count
        FROM etf_holdings
    """).fetchone()
    return {
        "total_days": result[0],
        "first_date": result[1],
        "last_date": result[2],
        "etf_count": result[3],
    }

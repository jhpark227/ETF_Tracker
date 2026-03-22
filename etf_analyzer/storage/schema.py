from __future__ import annotations

import duckdb


def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all tables if they don't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS etf_master (
            etf_code VARCHAR PRIMARY KEY,
            etf_name VARCHAR NOT NULL,
            manager VARCHAR,
            etf_type VARCHAR NOT NULL,
            benchmark VARCHAR,
            tier VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS etf_holdings (
            date DATE NOT NULL,
            etf_code VARCHAR NOT NULL,
            stock_code VARCHAR NOT NULL,
            stock_name VARCHAR NOT NULL,
            weight DOUBLE NOT NULL,
            shares BIGINT NOT NULL,
            PRIMARY KEY (date, etf_code, stock_code)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS etf_flow (
            date DATE NOT NULL,
            etf_code VARCHAR NOT NULL,
            creation_units BIGINT NOT NULL,
            redemption_units BIGINT NOT NULL,
            net_units BIGINT NOT NULL,
            nav DOUBLE NOT NULL,
            PRIMARY KEY (date, etf_code)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_master (
            stock_code VARCHAR PRIMARY KEY,
            stock_name VARCHAR NOT NULL
        )
    """)

def test_create_tables(db):
    """All 4 tables should exist after schema creation."""
    tables = db.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()
    table_names = {row[0] for row in tables}
    assert table_names == {"etf_master", "etf_holdings", "etf_flow", "stock_master"}


def test_create_tables_idempotent(db):
    """Calling create_tables twice should not error."""
    from etf_analyzer.storage.schema import create_tables
    create_tables(db)  # second call
    tables = db.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()
    assert len(tables) == 4


def test_etf_holdings_primary_key(db):
    """etf_holdings should reject duplicate (date, etf_code, stock_code)."""
    db.execute("""
        INSERT INTO etf_holdings VALUES ('2026-03-18', '069500', '005930', '삼성전자', 25.3, 1000)
    """)
    db.execute("""
        INSERT OR REPLACE INTO etf_holdings VALUES ('2026-03-18', '069500', '005930', '삼성전자', 26.0, 1100)
    """)
    result = db.execute(
        "SELECT weight FROM etf_holdings WHERE stock_code = '005930'"
    ).fetchone()
    assert result[0] == 26.0  # replaced, not duplicated

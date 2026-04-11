"""Shared test fixtures for Thread."""

from pathlib import Path

import duckdb
import pytest


def load_schema(conn):
    """Load schema.sql into a DuckDB connection."""
    schema_path = Path(__file__).parent.parent / "thread" / "schema.sql"
    sql = schema_path.read_text()
    # Remove SQL comments before splitting on semicolons
    lines = [l for l in sql.splitlines() if not l.strip().startswith("--")]
    cleaned = "\n".join(lines)
    for stmt in cleaned.split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)


@pytest.fixture
def duckdb_conn():
    """In-memory DuckDB connection with schema loaded."""
    conn = duckdb.connect(":memory:")
    load_schema(conn)
    yield conn
    conn.close()


@pytest.fixture
def sample_beads_dir():
    """Path to the sample-data/.beads/ directory."""
    p = Path(__file__).parent.parent / "sample-data" / ".beads"
    if not p.is_dir():
        pytest.skip("sample-data/.beads/ not found")
    return str(p)

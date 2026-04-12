"""Unit tests for thread.cli — CLI commands."""

from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pytest
from click.testing import CliRunner

from thread.cli import cli


# ============================================================
# Helper — fabricated DB (same pattern as test_prime/test_report)
# ============================================================

def _load_schema(conn):
    schema_path = Path(__file__).parent.parent / "thread" / "schema.sql"
    sql = schema_path.read_text()
    lines = [l for l in sql.splitlines() if not l.strip().startswith("--")]
    cleaned = "\n".join(lines)
    for stmt in cleaned.split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)


def _build_test_db(tmp_path, n_beads=10, n_closed=8):
    db_path = str(tmp_path / "thread.duckdb")
    conn = duckdb.connect(db_path)
    _load_schema(conn)

    t0 = datetime(2026, 4, 5, 15, 0, 0)

    for i in range(n_beads):
        iid = f"proj-{i:03d}"
        created_at = t0 + timedelta(minutes=i * 3)
        closed_at = created_at + timedelta(minutes=2) if i < n_closed else None
        claimed_at = created_at + timedelta(seconds=10) if i < n_closed else None

        conn.execute(
            "INSERT INTO dim_bead VALUES "
            "(?, ?, 'task', 2, 'user', NULL, NULL, NULL, "
            "true, false, false, NULL, NULL, NULL, false)",
            [iid, f"Bead {i}"],
        )
        conn.execute(
            "INSERT INTO dim_hierarchy VALUES (?, NULL, ?, 0, true, ?)",
            [iid, iid, iid],
        )
        active_secs = 120 if i < n_closed else 0
        elapsed_secs = 130 if i < n_closed else None
        time_to_start = 10 if claimed_at else None
        conn.execute(
            "INSERT INTO fact_bead_lifecycle VALUES "
            "(?, ?, ?, NULL, ?, ?, ?, ?, 0, 1, 0, NULL, 0, 0, 0, "
            "NULL, NULL, false, 'user', 'user')",
            [iid, created_at, claimed_at, closed_at,
             time_to_start, active_secs, elapsed_secs],
        )

    conn.execute(
        "INSERT INTO dim_actor VALUES "
        "('user', 'user', NULL, NULL, NULL, NULL, 'agent', 'heuristic')"
    )
    conn.execute(
        "INSERT INTO dim_session VALUES "
        "('s-001', ?, ?, ?, ?, ?, ?, 0, 'user', 'task')",
        [t0, t0 + timedelta(minutes=n_beads * 3),
         n_beads * 3 * 60, n_beads, n_closed, n_beads - n_closed],
    )
    for i in range(n_beads):
        conn.execute(
            "INSERT INTO bridge_session_bead VALUES ('s-001', ?)",
            [f"proj-{i:03d}"],
        )

    conn.close()
    return db_path


# ============================================================
# Tests
# ============================================================

class TestQueryCommand:
    def test_basic_query(self, tmp_path, monkeypatch):
        db_path = _build_test_db(tmp_path)
        beads_dir = tmp_path
        (tmp_path / "thread.duckdb").rename(tmp_path / "thread.duckdb")  # noop, already there
        monkeypatch.setattr("thread.cli.find_beads_dir", lambda x=None: tmp_path)

        runner = CliRunner()
        result = runner.invoke(cli, ["query", "SELECT COUNT(*) AS n FROM dim_bead"])
        assert result.exit_code == 0
        assert "10" in result.output

    def test_query_csv(self, tmp_path, monkeypatch):
        db_path = _build_test_db(tmp_path)
        monkeypatch.setattr("thread.cli.find_beads_dir", lambda x=None: tmp_path)

        runner = CliRunner()
        result = runner.invoke(cli, [
            "query", "SELECT issue_id FROM dim_bead", "--csv", "--limit", "2"
        ])
        assert result.exit_code == 0
        assert "issue_id" in result.output
        lines = result.output.strip().split("\n")
        assert len(lines) == 3  # header + 2 rows

    def test_query_limit(self, tmp_path, monkeypatch):
        db_path = _build_test_db(tmp_path)
        monkeypatch.setattr("thread.cli.find_beads_dir", lambda x=None: tmp_path)

        runner = CliRunner()
        result = runner.invoke(cli, [
            "query", "SELECT issue_id FROM dim_bead", "--limit", "3"
        ])
        assert result.exit_code == 0
        data_lines = [l for l in result.output.strip().split("\n") if l and not l.startswith("-")]
        assert len(data_lines) == 4  # header + 3 rows

    def test_query_truncation(self, tmp_path, monkeypatch):
        db_path = _build_test_db(tmp_path)
        # Insert a bead with a very long title
        conn = duckdb.connect(db_path)
        conn.execute(
            "INSERT INTO dim_bead VALUES "
            "('proj-long', ?, 'task', 2, 'user', NULL, NULL, NULL, "
            "true, false, false, NULL, NULL, NULL, false)",
            ["A" * 100],
        )
        conn.close()
        monkeypatch.setattr("thread.cli.find_beads_dir", lambda x=None: tmp_path)

        runner = CliRunner()
        result = runner.invoke(cli, [
            "query", "SELECT title FROM dim_bead WHERE issue_id='proj-long'"
        ])
        assert result.exit_code == 0
        # Title should be truncated to 60 chars with ...
        assert "..." in result.output
        assert "A" * 100 not in result.output


class TestSessionsCommand:
    def test_sessions_table(self, tmp_path, monkeypatch):
        db_path = _build_test_db(tmp_path)
        monkeypatch.setattr("thread.cli.find_beads_dir", lambda x=None: tmp_path)

        runner = CliRunner()
        result = runner.invoke(cli, ["sessions"])
        assert result.exit_code == 0
        assert "s-001" in result.output
        assert "When" in result.output

    def test_sessions_json(self, tmp_path, monkeypatch):
        db_path = _build_test_db(tmp_path)
        monkeypatch.setattr("thread.cli.find_beads_dir", lambda x=None: tmp_path)

        runner = CliRunner()
        result = runner.invoke(cli, ["sessions", "--json"])
        assert result.exit_code == 0
        import json
        parsed = json.loads(result.output)
        assert len(parsed) >= 1
        assert parsed[0]["session_id"] == "s-001"
        assert "compliance" in parsed[0]

    def test_sessions_limit(self, tmp_path, monkeypatch):
        db_path = _build_test_db(tmp_path)
        monkeypatch.setattr("thread.cli.find_beads_dir", lambda x=None: tmp_path)

        runner = CliRunner()
        result = runner.invoke(cli, ["sessions", "--limit", "1"])
        assert result.exit_code == 0
        assert "s-001" in result.output


class TestInteractionsCommand:
    def test_interactions_empty(self, tmp_path, monkeypatch):
        db_path = _build_test_db(tmp_path)
        monkeypatch.setattr("thread.cli.find_beads_dir", lambda x=None: tmp_path)

        runner = CliRunner()
        result = runner.invoke(cli, ["interactions"])
        assert result.exit_code == 0
        assert "No interactions" in result.output or "Audit trail" in result.output

    def test_interactions_json(self, tmp_path, monkeypatch):
        db_path = _build_test_db(tmp_path)
        monkeypatch.setattr("thread.cli.find_beads_dir", lambda x=None: tmp_path)

        runner = CliRunner()
        result = runner.invoke(cli, ["interactions", "--json"])
        assert result.exit_code == 0
        import json
        parsed = json.loads(result.output)
        assert "status" in parsed
        assert "total" in parsed

    def test_interactions_tools_empty(self, tmp_path, monkeypatch):
        db_path = _build_test_db(tmp_path)
        monkeypatch.setattr("thread.cli.find_beads_dir", lambda x=None: tmp_path)

        runner = CliRunner()
        result = runner.invoke(cli, ["interactions", "--tools"])
        assert result.exit_code == 0
        assert "No tool call data" in result.output

    def test_interactions_models_empty(self, tmp_path, monkeypatch):
        db_path = _build_test_db(tmp_path)
        monkeypatch.setattr("thread.cli.find_beads_dir", lambda x=None: tmp_path)

        runner = CliRunner()
        result = runner.invoke(cli, ["interactions", "--models"])
        assert result.exit_code == 0
        assert "No LLM call data" in result.output

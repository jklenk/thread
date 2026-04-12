"""Unit tests for thread.report — HTML report generation (v2)."""

import json
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from thread.report import generate_report


# ============================================================
# Helper — reuse the same fabricated DB builder from test_prime
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

class TestGenerateReport:
    def test_generates_html_file(self, tmp_path):
        db_path = _build_test_db(tmp_path)
        out = tmp_path / "report.html"
        path = generate_report(db_path, str(out))
        assert Path(path).exists()
        content = Path(path).read_text()
        assert "<!DOCTYPE html>" in content
        assert "Thread report" in content

    def test_six_headline_cards(self, tmp_path):
        db_path = _build_test_db(tmp_path)
        out = tmp_path / "report.html"
        generate_report(db_path, str(out))
        content = Path(out).read_text()
        assert content.count('class="card"') == 6

    def test_verdict_colored_borders(self, tmp_path):
        db_path = _build_test_db(tmp_path)
        out = tmp_path / "report.html"
        generate_report(db_path, str(out))
        content = Path(out).read_text()
        # At least one verdict border color should be present
        assert "#22c55e" in content or "#eab308" in content or "#ef4444" in content

    def test_session_table_present(self, tmp_path):
        db_path = _build_test_db(tmp_path)
        out = tmp_path / "report.html"
        generate_report(db_path, str(out))
        content = Path(out).read_text()
        assert "Sessions" in content
        assert "session-row" in content

    def test_compliance_section_present(self, tmp_path):
        db_path = _build_test_db(tmp_path)
        out = tmp_path / "report.html"
        generate_report(db_path, str(out))
        content = Path(out).read_text()
        assert "Compliance" in content
        assert "Claim compliance" in content
        assert "Close documentation" in content
        assert "Dependency order" in content
        assert "Scope stability" in content

    def test_chart_js_present(self, tmp_path):
        db_path = _build_test_db(tmp_path)
        out = tmp_path / "report.html"
        generate_report(db_path, str(out))
        content = Path(out).read_text()
        assert "chart.js" in content.lower() or "Chart" in content
        assert "histChart" in content
        assert "tpChart" in content

    def test_drill_down_panel_present(self, tmp_path):
        db_path = _build_test_db(tmp_path)
        out = tmp_path / "report.html"
        generate_report(db_path, str(out))
        content = Path(out).read_text()
        assert "drilldown" in content
        assert "MEDIAN_CYCLE_SECS" in content

    def test_no_dollar_amounts(self, tmp_path):
        db_path = _build_test_db(tmp_path)
        out = tmp_path / "report.html"
        generate_report(db_path, str(out))
        content = Path(out).read_text()
        assert "$" not in content

    def test_cost_footer_present(self, tmp_path):
        db_path = _build_test_db(tmp_path)
        out = tmp_path / "report.html"
        generate_report(db_path, str(out))
        content = Path(out).read_text()
        assert "multiples of project median cycle time" in content

    def test_agent_knowledge_section(self, tmp_path):
        db_path = _build_test_db(tmp_path)
        out = tmp_path / "report.html"
        generate_report(db_path, str(out))
        content = Path(out).read_text()
        assert "Agent knowledge base" in content

    def test_insights_section(self, tmp_path):
        db_path = _build_test_db(tmp_path)
        out = tmp_path / "report.html"
        generate_report(db_path, str(out))
        content = Path(out).read_text()
        assert "Insights" in content

    def test_bead_data_embedded_for_drilldown(self, tmp_path):
        db_path = _build_test_db(tmp_path)
        out = tmp_path / "report.html"
        generate_report(db_path, str(out))
        content = Path(out).read_text()
        assert "beadData" in content
        assert "proj-000" in content  # bead IDs in the embedded JSON

    def test_no_fidelity_placeholders(self, tmp_path):
        """Old v1 placeholders should not appear."""
        db_path = _build_test_db(tmp_path)
        out = tmp_path / "report.html"
        generate_report(db_path, str(out))
        content = Path(out).read_text()
        assert "{fidelity}" not in content
        assert "{effort}" not in content
        assert "Fidelity score" not in content
        assert "Rework cost" not in content

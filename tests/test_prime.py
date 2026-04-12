"""Unit tests for thread.prime — v2 metrics, verdicts, and signals."""

from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import json
import pytest

from thread.prime import (
    compute_prime,
    format_human,
    format_json,
    _fmt_duration,
    _verdict,
    _VERDICT_ICON,
)


# ============================================================
# Helpers
# ============================================================

def _load_schema(conn):
    """Load schema.sql into a DuckDB connection."""
    schema_path = Path(__file__).parent.parent / "thread" / "schema.sql"
    sql = schema_path.read_text()
    lines = [l for l in sql.splitlines() if not l.strip().startswith("--")]
    cleaned = "\n".join(lines)
    for stmt in cleaned.split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)


def _build_test_db(tmp_path, n_beads=10, n_closed=8, session_gap_mins=30):
    """Create a minimal thread.duckdb with fabricated data for testing."""
    db_path = str(tmp_path / "thread.duckdb")
    conn = duckdb.connect(db_path)
    _load_schema(conn)

    t0 = datetime(2026, 4, 5, 15, 0, 0)

    for i in range(n_beads):
        iid = f"bd-{i:03d}"
        created_at = t0 + timedelta(minutes=i * 3)
        closed_at = created_at + timedelta(minutes=2) if i < n_closed else None
        claimed_at = created_at + timedelta(seconds=10) if i < n_closed else None

        # dim_bead
        conn.execute(
            "INSERT INTO dim_bead VALUES "
            "(?, ?, 'task', 2, 'user', NULL, NULL, NULL, "
            "true, false, false, NULL, NULL, NULL, false)",
            [iid, f"Bead {i}"],
        )

        # dim_hierarchy (flat)
        conn.execute(
            "INSERT INTO dim_hierarchy VALUES (?, NULL, ?, 0, true, ?)",
            [iid, iid, iid],
        )

        # fact_bead_lifecycle
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

    # dim_actor
    conn.execute(
        "INSERT INTO dim_actor VALUES "
        "('user', 'user', NULL, NULL, NULL, NULL, 'agent', 'heuristic')"
    )

    # dim_session + bridge (one session for all beads)
    conn.execute(
        "INSERT INTO dim_session VALUES "
        "('s-001', ?, ?, ?, ?, ?, ?, 0, 'user', 'task')",
        [t0, t0 + timedelta(minutes=n_beads * 3),
         n_beads * 3 * 60, n_beads, n_closed, n_beads - n_closed],
    )
    for i in range(n_beads):
        conn.execute(
            "INSERT INTO bridge_session_bead VALUES ('s-001', ?)",
            [f"bd-{i:03d}"],
        )

    conn.close()
    return db_path


# ============================================================
# Time formatting
# ============================================================

class TestFmtDuration:
    def test_seconds(self):
        assert _fmt_duration(45) == "45s"

    def test_minutes(self):
        assert _fmt_duration(150) == "2m 30s"

    def test_hours(self):
        assert _fmt_duration(7200) == "2h"
        assert _fmt_duration(7320) == "2h 2m"

    def test_none(self):
        assert _fmt_duration(None) == "—"


# ============================================================
# Verdict logic
# ============================================================

class TestVerdict:
    def test_good(self):
        assert _verdict(True, True) == "good"

    def test_watch(self):
        assert _verdict(False, True) == "watch"

    def test_concern(self):
        assert _verdict(False, False) == "concern"

    def test_icons(self):
        assert _VERDICT_ICON["good"] == "[+]"
        assert _VERDICT_ICON["watch"] == "[~]"
        assert _VERDICT_ICON["concern"] == "[!]"


# ============================================================
# compute_prime — full pipeline
# ============================================================

class TestComputePrime:
    def test_returns_all_required_keys(self, tmp_path):
        db_path = _build_test_db(tmp_path)
        data = compute_prime(db_path)

        required = {
            "workflow_type", "total_beads", "completion_rate", "completion_verdict",
            "cycle_time_p50_secs", "cycle_time_verdict",
            "throughput_beads_per_day", "throughput_verdict",
            "cost_p90_multiple", "cost_verdict",
            "agent_closure_rate", "agent_closure_verdict",
            "scope_stability_rate", "scope_stability_verdict",
            "skip_claim_rate", "skip_claim_verdict",
            "documentation_rate", "documentation_verdict",
            "dep_order_violations", "dep_order_verdict",
            "queue_wait_p50_secs", "queue_wait_verdict",
            "interactions", "agent_knowledge", "trend",
            "recent_sessions",
        }
        assert required.issubset(data.keys())

    def test_completion_rate_correct(self, tmp_path):
        db_path = _build_test_db(tmp_path, n_beads=10, n_closed=8)
        data = compute_prime(db_path)
        assert data["completion_rate"] == 0.8
        assert data["total_beads"] == 10
        assert data["closed_count"] == 8
        assert data["open_count"] == 2

    def test_cycle_time_populated(self, tmp_path):
        db_path = _build_test_db(tmp_path)
        data = compute_prime(db_path)
        # All closed beads have active_time_secs=120
        assert data["cycle_time_p50_secs"] == 120
        assert data["cycle_time_p90_secs"] == 120

    def test_all_verdicts_are_valid(self, tmp_path):
        db_path = _build_test_db(tmp_path)
        data = compute_prime(db_path)
        for key, val in data.items():
            if key.endswith("_verdict"):
                assert val in ("good", "watch", "concern"), f"{key} = {val}"

    def test_interactions_missing_when_empty(self, tmp_path):
        db_path = _build_test_db(tmp_path)
        data = compute_prime(db_path)
        assert data["interactions"]["total"] == 0
        assert data["interactions"]["status"] == "missing"
        assert data["interactions"]["models_used"] == []
        assert data["interactions"]["tools_used"] == []
        assert data["interactions"]["tool_success_rate"] is None

    def test_agent_knowledge_zero_when_empty(self, tmp_path):
        db_path = _build_test_db(tmp_path)
        data = compute_prime(db_path)
        assert data["agent_knowledge"]["count"] == 0
        assert data["agent_knowledge"]["memories"] == []

    def test_sessions_populated(self, tmp_path):
        db_path = _build_test_db(tmp_path)
        data = compute_prime(db_path)
        sessions = data["recent_sessions"]
        assert len(sessions) == 1
        assert sessions[0]["session_id"] == "s-001"
        assert sessions[0]["bead_count"] == 10
        assert "verdict" in sessions[0]
        assert "assessment" in sessions[0]

    def test_no_dollar_amounts_in_output(self, tmp_path):
        """Cost is always relative — never dollars."""
        db_path = _build_test_db(tmp_path)
        data = compute_prime(db_path)
        json_str = json.dumps(data, default=str)
        assert "$" not in json_str

    def test_signals_are_plain_language(self, tmp_path):
        db_path = _build_test_db(tmp_path)
        data = compute_prime(db_path)
        technical_terms = [
            "fidelity_score", "effort_score", "reopen_count",
            "revision_requested", "active_time_secs", "compaction_level",
            "dim_bead", "fact_bead", "v_bead_scores",
        ]

        def _check(obj, path=""):
            if isinstance(obj, str):
                for term in technical_terms:
                    assert term not in obj, f"{path} contains '{term}'"
            elif isinstance(obj, dict):
                for k, v in obj.items():
                    if k.endswith("_signal") or k in ("signal", "assessment", "message"):
                        _check(v, f"{path}.{k}")
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    _check(item, f"{path}[{i}]")

        _check(data)


# ============================================================
# format_human
# ============================================================

class TestFormatHuman:
    def test_output_contains_key_sections(self, tmp_path):
        db_path = _build_test_db(tmp_path)
        data = compute_prime(db_path)
        out = format_human(data)
        assert "Thread — project health" in out
        assert "completed" in out
        assert "Compliance:" in out
        assert "[+]" in out or "[~]" in out or "[!]" in out

    def test_no_dollar_amounts(self, tmp_path):
        db_path = _build_test_db(tmp_path)
        data = compute_prime(db_path)
        out = format_human(data)
        assert "$" not in out


# ============================================================
# format_json
# ============================================================

class TestFormatJson:
    def test_valid_json(self, tmp_path):
        db_path = _build_test_db(tmp_path)
        data = compute_prime(db_path)
        out = format_json(data)
        parsed = json.loads(out)
        assert parsed["completion_rate"] == data["completion_rate"]

    def test_interactions_top_level_object(self, tmp_path):
        db_path = _build_test_db(tmp_path)
        data = compute_prime(db_path)
        out = format_json(data)
        parsed = json.loads(out)
        assert "interactions" in parsed
        assert isinstance(parsed["interactions"], dict)
        assert "status" in parsed["interactions"]

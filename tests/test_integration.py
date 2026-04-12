"""Integration tests — run thread refresh against real Dolt sample data.

Requires: dolt binary on PATH, and a user-provided sample-data/.beads/
directory at the repo root (gitignored). Drop a real Beads project there
(or symlink one) to exercise the full pipeline. Auto-skips when absent.

Run with: uv run pytest -m integration
Skip by default in normal runs: uv run pytest -m "not integration"

NOTE: test_counts_match_sample_data_shape hardcodes counts (34 beads) from
the original sample dump. When swapping in your own sample-data, expect
that test and a few other shape assertions to need updating.
"""

import json
from pathlib import Path

import duckdb
import pytest

from thread.extractor import refresh
from thread.prime import compute_prime, format_human, format_json
from thread.report import generate_report


pytestmark = pytest.mark.integration


@pytest.fixture
def refreshed_db(sample_beads_dir, tmp_path):
    """Run a real refresh against sample-data into a tmp duckdb."""
    db_path = tmp_path / "thread.duckdb"
    counts = refresh(beads_dir=sample_beads_dir, output_path=str(db_path))
    return str(db_path), counts


class TestRefreshAgainstSampleData:
    def test_all_tables_populated(self, refreshed_db):
        db_path, counts = refreshed_db
        # Every Dolt-sourced table should have at least one row. Skip private
        # status keys (prefixed with _) and tables that can legitimately be
        # empty when their source isn't populated in sample-data
        # (fact_interactions depends on interactions.jsonl; dim_agent_memory
        # depends on kv.memory.* keys in Dolt config).
        optional_empty = {"fact_interactions", "dim_agent_memory"}
        for table, n in counts.items():
            if table.startswith("_"):
                continue
            if table in optional_empty:
                continue
            assert n > 0, f"{table} is empty after refresh"

    def test_counts_match_sample_data_shape(self, refreshed_db):
        _, counts = refreshed_db
        # Sample data is the data_eng_summary project: 34 beads
        assert counts["dim_bead"] == 34
        assert counts["dim_hierarchy"] == 34
        assert counts["fact_bead_lifecycle"] == 34
        assert counts["fact_bead_events"] > counts["dim_bead"]  # multiple events per bead
        assert counts["dim_actor"] >= 1
        # Session detection should cluster the 34 beads into at least one session
        assert counts["dim_session"] >= 1
        # interactions status is reported even when the file is absent
        assert counts["_interactions_status"] in ("missing", "empty", "populated")

    def test_every_bead_has_lifecycle_row(self, refreshed_db):
        db_path, _ = refreshed_db
        conn = duckdb.connect(db_path, read_only=True)
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM dim_bead b "
                "LEFT JOIN fact_bead_lifecycle f ON f.issue_id = b.issue_id "
                "WHERE f.issue_id IS NULL"
            ).fetchone()
            assert row[0] == 0, "every bead must have a lifecycle row"
        finally:
            conn.close()

    def test_every_bead_has_hierarchy_row(self, refreshed_db):
        db_path, _ = refreshed_db
        conn = duckdb.connect(db_path, read_only=True)
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM dim_bead b "
                "LEFT JOIN dim_hierarchy h ON h.issue_id = b.issue_id "
                "WHERE h.issue_id IS NULL"
            ).fetchone()
            assert row[0] == 0
        finally:
            conn.close()

    def test_actor_keys_populated(self, refreshed_db):
        """Every closed bead should have a closer_actor_key."""
        db_path, _ = refreshed_db
        conn = duckdb.connect(db_path, read_only=True)
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM fact_bead_lifecycle "
                "WHERE final_closed_at IS NOT NULL AND closer_actor_key IS NULL"
            ).fetchone()
            assert row[0] == 0, "closed beads must have closer_actor_key"

            row = conn.execute(
                "SELECT COUNT(*) FROM fact_bead_lifecycle "
                "WHERE creator_actor_key IS NULL"
            ).fetchone()
            assert row[0] == 0, "every bead must have creator_actor_key"
        finally:
            conn.close()

    def test_closer_is_not_dolt_root(self, refreshed_db):
        """closer_actor_key must come from events, not dolt commit author ('root')."""
        db_path, _ = refreshed_db
        conn = duckdb.connect(db_path, read_only=True)
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM fact_bead_lifecycle "
                "WHERE closer_actor_key = 'root'"
            ).fetchone()
            assert row[0] == 0, (
                "closer_actor_key should come from events table, "
                "not Dolt commit author which is always 'root' in embedded mode"
            )
        finally:
            conn.close()

    def test_dim_actor_has_classification(self, refreshed_db):
        db_path, _ = refreshed_db
        conn = duckdb.connect(db_path, read_only=True)
        try:
            rows = conn.execute(
                "SELECT actor_class, classification_source FROM dim_actor"
            ).fetchall()
            assert len(rows) >= 1
            for cls, src in rows:
                assert cls in ("agent", "human", "unknown")
                assert src in (
                    "hop_uri", "role_type", "session",
                    "agent_state", "heuristic", "unknown",
                )
        finally:
            conn.close()

    def test_views_return_rows(self, refreshed_db):
        db_path, _ = refreshed_db
        conn = duckdb.connect(db_path, read_only=True)
        try:
            for view in ("v_bead_scores", "v_bead_dep_activity",
                         "mart_epic_summary", "v_weekly_trends"):
                n = conn.execute(f"SELECT COUNT(*) FROM {view}").fetchone()[0]
                assert n >= 0  # at minimum the query must succeed

            # v_bead_scores should be non-empty for closed beads
            n = conn.execute("SELECT COUNT(*) FROM v_bead_scores").fetchone()[0]
            assert n == 34
        finally:
            conn.close()


class TestPrimeAgainstSampleData:
    def test_compute_prime_returns_sane_output(self, refreshed_db):
        db_path, _ = refreshed_db
        data = compute_prime(db_path)

        # Required v2 keys present
        required = {
            "workflow_type", "total_beads", "epic_count", "singleton_bead_count",
            "completion_rate", "completion_signal", "completion_verdict",
            "cycle_time_p50_secs", "cycle_time_p90_secs", "cycle_time_verdict",
            "throughput_beads_per_day", "throughput_verdict",
            "cost_p90_multiple", "cost_verdict",
            "parallelism_ratio", "parallelism_verdict",
            "agent_closure_rate", "agent_closure_signal", "agent_closure_verdict",
            "scope_stability_rate", "scope_stability_verdict",
            "skip_claim_rate", "skip_claim_verdict",
            "documentation_rate", "documentation_verdict",
            "dep_order_violations", "dep_order_verdict",
            "queue_wait_p50_secs", "queue_wait_verdict",
            "interactions", "agent_knowledge", "trend",
            "recent_sessions",
        }
        assert required.issubset(data.keys())

        assert data["workflow_type"] in ("epic", "flat", "mixed", "empty")
        assert data["completion_rate"] >= 0.7
        assert data["agent_closure_rate"] == 1.0

        for key, val in data.items():
            if key.endswith("_verdict"):
                assert val in ("good", "watch", "concern"), f"{key} = {val}"

    def test_format_human_is_string(self, refreshed_db):
        db_path, _ = refreshed_db
        data = compute_prime(db_path)
        out = format_human(data)
        assert isinstance(out, str)
        assert "Thread" in out
        assert "completed" in out

    def test_format_json_is_valid_json(self, refreshed_db):
        db_path, _ = refreshed_db
        data = compute_prime(db_path)
        out = format_json(data)
        parsed = json.loads(out)
        assert parsed["completion_rate"] == data["completion_rate"]
        assert "interactions" in parsed
        assert "status" in parsed["interactions"]

    def test_signals_are_plain_language(self, refreshed_db):
        """No technical terms in user-facing signal strings."""
        db_path, _ = refreshed_db
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
                    if k.endswith("_signal") or k in ("signal", "assessment"):
                        _check(v, f"{path}.{k}")
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    _check(item, f"{path}[{i}]")

        _check(data)


class TestReportAgainstSampleData:
    def test_report_generates_html(self, refreshed_db, tmp_path):
        db_path, _ = refreshed_db
        out = tmp_path / "report.html"
        path = generate_report(db_path, str(out))
        assert Path(path).exists()
        content = Path(path).read_text()
        assert "<!DOCTYPE html>" in content
        assert "Thread report" in content
        assert "Chart" in content  # chart.js reference
        # Headline values should be interpolated, not left as placeholders
        assert "{fidelity}" not in content
        assert "{effort}" not in content

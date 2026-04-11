"""Tests for schema.sql — verify all tables and views are created correctly."""

import duckdb
import pytest


class TestSchemaCreation:
    def test_all_tables_created(self, duckdb_conn):
        tables = duckdb_conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_type = 'BASE TABLE' "
            "ORDER BY table_name"
        ).fetchall()
        table_names = [t[0] for t in tables]
        assert table_names == [
            "dim_actor",
            "dim_bead",
            "dim_hierarchy",
            "fact_bead_events",
            "fact_bead_lifecycle",
            "fact_dep_activity",
        ]

    def test_all_views_created(self, duckdb_conn):
        views = duckdb_conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_type = 'VIEW' "
            "ORDER BY table_name"
        ).fetchall()
        view_names = [t[0] for t in views]
        assert view_names == [
            "mart_epic_summary",
            "mart_project_summary",
            "v_bead_dep_activity",
            "v_bead_scores",
            "v_weekly_trends",
        ]

    def test_dim_bead_columns(self, duckdb_conn):
        cols = duckdb_conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'dim_bead' ORDER BY ordinal_position"
        ).fetchall()
        col_names = [c[0] for c in cols]
        assert "issue_id" in col_names
        assert "quality_score" in col_names
        assert "crystallizes" in col_names
        assert "is_template" in col_names

    def test_fact_bead_lifecycle_has_actor_keys(self, duckdb_conn):
        cols = duckdb_conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'fact_bead_lifecycle' ORDER BY ordinal_position"
        ).fetchall()
        col_names = [c[0] for c in cols]
        assert "creator_actor_key" in col_names
        assert "closer_actor_key" in col_names

    def test_views_work_on_empty_tables(self, duckdb_conn):
        """Views should return 0 rows on empty tables without error."""
        for view in ["v_bead_scores", "v_bead_dep_activity",
                      "mart_epic_summary", "v_weekly_trends"]:
            result = duckdb_conn.execute(f"SELECT * FROM {view}").fetchall()
            assert result == []

    def test_mart_epic_summary_uses_left_join(self, duckdb_conn):
        """Orphan hierarchy rows (no matching dim_bead) should still appear."""
        duckdb_conn.execute(
            "INSERT INTO dim_hierarchy VALUES "
            "('orphan-1', NULL, 'orphan-1', 0, true, 'orphan-1')"
        )
        duckdb_conn.execute(
            "INSERT INTO fact_bead_lifecycle VALUES "
            "('orphan-1', '2026-01-01', NULL, NULL, NULL, NULL, 0, NULL, "
            "0, 0, 0, NULL, 0, 0, 0, NULL, NULL, false, NULL, NULL)"
        )
        result = duckdb_conn.execute(
            "SELECT epic_id FROM mart_epic_summary"
        ).fetchall()
        assert len(result) == 1
        assert result[0][0] == "orphan-1"

"""Tests for extractor functions — mocked Dolt, real DuckDB."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from thread.extractor import (
    extract_dim_bead,
    extract_dim_hierarchy,
    extract_fact_bead_events,
    extract_fact_bead_lifecycle,
    extract_fact_dep_activity,
    dep_category,
    _parse_hierarchy_from_id,
    _walk_to_root,
    _build_path,
)


class MockCursor:
    """Mock pymysql cursor that returns predetermined rows."""

    def __init__(self, rows):
        self._rows = rows
        self._executed = []

    def execute(self, sql, args=None):
        self._executed.append(sql)

    def fetchall(self):
        return self._rows

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class MultiQueryCursor:
    """Mock cursor that returns different rows based on query index."""

    def __init__(self, query_results: list):
        self._results = query_results
        self._call_idx = -1

    def execute(self, sql, args=None):
        self._call_idx += 1

    def fetchall(self):
        if self._call_idx < len(self._results):
            return self._results[self._call_idx]
        return []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class MockConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


# ============================================================
# dep_category mapping
# ============================================================

class TestDepCategory:
    def test_workflow_types(self):
        for t in ["blocks", "parent-child", "waits-for", "conditional-blocks"]:
            assert dep_category(t) == "workflow"

    def test_association_types(self):
        for t in ["related", "discovered-from", "replies-to", "relates-to",
                   "duplicates", "supersedes"]:
            assert dep_category(t) == "association"

    def test_hop_types(self):
        for t in ["authored-by", "assigned-to", "approved-by", "attests", "validates"]:
            assert dep_category(t) == "hop"

    def test_unknown_type_is_reference(self):
        assert dep_category("something-new") == "reference"
        assert dep_category("custom") == "reference"


# ============================================================
# dim_bead
# ============================================================

class TestExtractDimBead:
    def test_extracts_issues(self, duckdb_conn):
        rows = [
            {
                "id": "bd-001", "title": "Test bead", "issue_type": "task",
                "priority": 2, "created_by": "user1", "owner": "user1@test.com",
                "assignee": "user1", "estimated_minutes": 60,
                "description": "A test", "acceptance_criteria": "",
                "design": "some design", "quality_score": None,
                "crystallizes": None, "source_system": "", "is_template": 0,
            },
        ]
        conn = MockConn(MockCursor(rows))
        count = extract_dim_bead(conn, duckdb_conn)
        assert count == 1

        result = duckdb_conn.execute("SELECT * FROM dim_bead").fetchall()
        assert len(result) == 1
        assert result[0][0] == "bd-001"  # issue_id
        assert result[0][8] == True  # has_description
        assert result[0][9] == False  # has_acceptance_criteria
        assert result[0][10] == True  # has_design

    def test_handles_missing_nullable_columns(self, duckdb_conn):
        """Missing quality_score and crystallizes should be NULL."""
        rows = [
            {
                "id": "bd-002", "title": "Minimal", "issue_type": "bug",
                "priority": 1, "created_by": "u", "owner": "",
                "assignee": None, "estimated_minutes": None,
                "description": "", "acceptance_criteria": "",
                "design": "", "quality_score": None,
                "crystallizes": None, "source_system": "",
                "is_template": None,
            },
        ]
        conn = MockConn(MockCursor(rows))
        count = extract_dim_bead(conn, duckdb_conn)
        assert count == 1

        result = duckdb_conn.execute(
            "SELECT quality_score, crystallizes, is_template FROM dim_bead"
        ).fetchone()
        assert result[0] is None
        assert result[1] is None
        assert result[2] is None


# ============================================================
# dim_hierarchy
# ============================================================

class TestHierarchyHelpers:
    def test_parse_flat_id(self):
        parent, depth = _parse_hierarchy_from_id("bd-abc", {"bd-abc"})
        assert parent is None
        assert depth == 0

    def test_parse_dotted_id_depth_1(self):
        all_ids = {"bd-abc", "bd-abc.1"}
        parent, depth = _parse_hierarchy_from_id("bd-abc.1", all_ids)
        assert parent == "bd-abc"
        assert depth == 1

    def test_parse_dotted_id_depth_2(self):
        all_ids = {"bd-abc", "bd-abc.1", "bd-abc.1.2"}
        parent, depth = _parse_hierarchy_from_id("bd-abc.1.2", all_ids)
        assert parent == "bd-abc.1"
        assert depth == 2

    def test_parse_dotted_id_no_parent_match(self):
        """If prefix doesn't match a known ID, treat as root."""
        parent, depth = _parse_hierarchy_from_id("bd-abc.1", {"bd-abc.1"})
        assert parent is None
        assert depth == 0

    def test_walk_to_root(self):
        parents = {"c": "b", "b": "a", "a": None}
        assert _walk_to_root("c", parents) == "a"
        assert _walk_to_root("a", parents) == "a"

    def test_build_path(self):
        parents = {"c": "b", "b": "a", "a": None}
        assert _build_path("c", parents) == "a/b/c"
        assert _build_path("a", parents) == "a"


class TestExtractDimHierarchy:
    def test_flat_ids_all_roots(self, duckdb_conn):
        """Flat IDs with no parent-child deps = all depth 0."""
        issues = [{"id": "bd-aaa"}, {"id": "bd-bbb"}, {"id": "bd-ccc"}]
        deps = []
        cursor = MultiQueryCursor([issues, deps])
        conn = MockConn(cursor)

        count = extract_dim_hierarchy(conn, duckdb_conn)
        assert count == 3

        rows = duckdb_conn.execute(
            "SELECT issue_id, depth, is_root FROM dim_hierarchy ORDER BY issue_id"
        ).fetchall()
        for row in rows:
            assert row[1] == 0  # depth
            assert row[2] == True  # is_root

    def test_hierarchical_ids(self, duckdb_conn):
        """Dotted IDs should produce correct hierarchy."""
        issues = [
            {"id": "bd-epic"},
            {"id": "bd-epic.1"},
            {"id": "bd-epic.1.1"},
        ]
        deps = []
        cursor = MultiQueryCursor([issues, deps])
        conn = MockConn(cursor)

        count = extract_dim_hierarchy(conn, duckdb_conn)
        assert count == 3

        rows = duckdb_conn.execute(
            "SELECT issue_id, parent_id, root_id, depth, path "
            "FROM dim_hierarchy ORDER BY depth"
        ).fetchall()

        # Epic
        assert rows[0][0] == "bd-epic"
        assert rows[0][1] is None  # parent
        assert rows[0][2] == "bd-epic"  # root
        assert rows[0][3] == 0  # depth

        # Task
        assert rows[1][0] == "bd-epic.1"
        assert rows[1][1] == "bd-epic"
        assert rows[1][2] == "bd-epic"
        assert rows[1][3] == 1
        assert rows[1][4] == "bd-epic/bd-epic.1"

        # Subtask
        assert rows[2][0] == "bd-epic.1.1"
        assert rows[2][1] == "bd-epic.1"
        assert rows[2][2] == "bd-epic"
        assert rows[2][3] == 2
        assert rows[2][4] == "bd-epic/bd-epic.1/bd-epic.1.1"

    def test_parent_child_deps_override(self, duckdb_conn):
        """Parent-child dep edges override ID-based parsing."""
        issues = [{"id": "flat-a"}, {"id": "flat-b"}, {"id": "flat-c"}]
        deps = [
            {"issue_id": "flat-b", "depends_on_id": "flat-a"},
            {"issue_id": "flat-c", "depends_on_id": "flat-b"},
        ]
        cursor = MultiQueryCursor([issues, deps])
        conn = MockConn(cursor)

        count = extract_dim_hierarchy(conn, duckdb_conn)
        assert count == 3

        rows = {
            r[0]: r
            for r in duckdb_conn.execute(
                "SELECT issue_id, parent_id, root_id, depth, is_root "
                "FROM dim_hierarchy"
            ).fetchall()
        }

        assert rows["flat-a"][1] is None  # no parent
        assert rows["flat-a"][4] == True  # is_root
        assert rows["flat-b"][1] == "flat-a"  # parent from dep
        assert rows["flat-b"][3] == 1  # depth
        assert rows["flat-c"][1] == "flat-b"
        assert rows["flat-c"][2] == "flat-a"  # root
        assert rows["flat-c"][3] == 2

    def test_parent_child_depth_order_independent(self, duckdb_conn):
        """Depth must be correct regardless of the order deps are returned.

        Regression: walking parents mid-mutation meant a grandchild processed
        before its parent's edge got a truncated chain and depth=1.
        """
        issues = [{"id": "root"}, {"id": "mid"}, {"id": "leaf"}]
        # Deliberately return grandchild edge BEFORE parent edge
        deps = [
            {"issue_id": "leaf", "depends_on_id": "mid"},
            {"issue_id": "mid", "depends_on_id": "root"},
        ]
        cursor = MultiQueryCursor([issues, deps])
        conn = MockConn(cursor)

        extract_dim_hierarchy(conn, duckdb_conn)

        rows = {
            r[0]: r
            for r in duckdb_conn.execute(
                "SELECT issue_id, parent_id, root_id, depth, path "
                "FROM dim_hierarchy"
            ).fetchall()
        }
        assert rows["root"][3] == 0
        assert rows["mid"][3] == 1
        assert rows["leaf"][3] == 2
        assert rows["leaf"][2] == "root"
        assert rows["leaf"][4] == "root/mid/leaf"


# ============================================================
# fact_bead_events
# ============================================================

class TestExtractFactBeadEvents:
    def test_extracts_events(self, duckdb_conn):
        t = datetime(2026, 4, 5, 15, 0, 0)
        rows = [
            {"issue_id": "bd-1", "event_type": "created", "actor": "user",
             "old_value": None, "new_value": None, "created_at": t},
            {"issue_id": "bd-1", "event_type": "closed", "actor": "user",
             "old_value": None, "new_value": None, "created_at": t + timedelta(minutes=5)},
        ]
        conn = MockConn(MockCursor(rows))
        count = extract_fact_bead_events(conn, duckdb_conn)
        assert count == 2

        result = duckdb_conn.execute("SELECT COUNT(*) FROM fact_bead_events").fetchone()
        assert result[0] == 2


# ============================================================
# fact_bead_lifecycle
# ============================================================

class TestExtractFactBeadLifecycle:
    def test_basic_lifecycle(self, duckdb_conn):
        t0 = datetime(2026, 4, 5, 15, 0, 0)
        t1 = t0 + timedelta(minutes=2)
        t2 = t0 + timedelta(minutes=5)

        issues = [
            {"id": "bd-1", "created_at": t0, "closed_at": t2,
             "compaction_level": 0, "compacted_at": None,
             "quality_score": None, "crystallizes": None,
             "created_by": "testuser"},
        ]
        diffs = [
            {"to_id": "bd-1", "to_status": "in_progress", "from_status": "open",
             "to_commit_date": t1},
            {"to_id": "bd-1", "to_status": "closed", "from_status": "in_progress",
             "to_commit_date": t2},
        ]
        events = [
            {"issue_id": "bd-1", "event_type": "created", "actor": "testuser",
             "created_at": t0, "old_value": None, "new_value": None},
            {"issue_id": "bd-1", "event_type": "claimed", "actor": "testuser",
             "created_at": t1, "old_value": None, "new_value": None},
            {"issue_id": "bd-1", "event_type": "closed", "actor": "testuser",
             "created_at": t2, "old_value": None, "new_value": None},
        ]

        cursor = MultiQueryCursor([issues, diffs, events])
        conn = MockConn(cursor)

        # Need dim_actor populated for FK
        duckdb_conn.execute(
            "INSERT INTO dim_actor VALUES "
            "('testuser', 'testuser', NULL, NULL, NULL, NULL, 'unknown', 'unknown')"
        )

        count = extract_fact_bead_lifecycle(conn, duckdb_conn)
        assert count == 1

        row = duckdb_conn.execute(
            "SELECT issue_id, creator_actor_key, closer_actor_key, "
            "reopen_count, active_time_secs "
            "FROM fact_bead_lifecycle"
        ).fetchone()
        assert row[0] == "bd-1"
        assert row[1] == "testuser"  # creator
        assert row[2] == "testuser"  # closer
        assert row[3] == 0  # no reopens
        assert row[4] == 180  # 3 minutes in_progress


# ============================================================
# fact_dep_activity
# ============================================================

class TestExtractFactBeadLifecycleEdgeCases:
    def test_reopen_count(self, duckdb_conn):
        """closed → in_progress transitions count as reopens."""
        t0 = datetime(2026, 4, 5, 15, 0, 0)
        t1 = t0 + timedelta(minutes=2)
        t2 = t0 + timedelta(minutes=5)
        t3 = t0 + timedelta(minutes=8)
        t4 = t0 + timedelta(minutes=10)

        issues = [
            {"id": "bd-1", "created_at": t0, "closed_at": t4,
             "compaction_level": 0, "compacted_at": None,
             "quality_score": None, "crystallizes": None,
             "created_by": "u"},
        ]
        diffs = [
            {"to_id": "bd-1", "to_status": "in_progress", "from_status": "open",
             "to_commit_date": t1},
            {"to_id": "bd-1", "to_status": "closed", "from_status": "in_progress",
             "to_commit_date": t2},
            {"to_id": "bd-1", "to_status": "in_progress", "from_status": "closed",
             "to_commit_date": t3},
            {"to_id": "bd-1", "to_status": "closed", "from_status": "in_progress",
             "to_commit_date": t4},
        ]
        events = [
            {"issue_id": "bd-1", "event_type": "closed", "actor": "u",
             "created_at": t2, "old_value": None, "new_value": None},
        ]
        cursor = MultiQueryCursor([issues, diffs, events])
        conn = MockConn(cursor)
        extract_fact_bead_lifecycle(conn, duckdb_conn)

        row = duckdb_conn.execute(
            "SELECT reopen_count, active_time_secs FROM fact_bead_lifecycle"
        ).fetchone()
        assert row[0] == 1  # one reopen (closed → in_progress)
        # active = (t2-t1) + (t4-t3) = 180 + 120 = 300
        assert row[1] == 300

    def test_closed_without_claim(self, duckdb_conn):
        """Issue closed without ever being claimed."""
        t0 = datetime(2026, 4, 5, 15, 0, 0)
        t1 = t0 + timedelta(minutes=1)
        issues = [
            {"id": "bd-1", "created_at": t0, "closed_at": t1,
             "compaction_level": 0, "compacted_at": None,
             "quality_score": None, "crystallizes": None,
             "created_by": "u"},
        ]
        diffs = [
            {"to_id": "bd-1", "to_status": "closed", "from_status": "open",
             "to_commit_date": t1},
        ]
        events = []
        cursor = MultiQueryCursor([issues, diffs, events])
        conn = MockConn(cursor)
        extract_fact_bead_lifecycle(conn, duckdb_conn)

        row = duckdb_conn.execute(
            "SELECT first_claimed_at, first_closed_at, active_time_secs "
            "FROM fact_bead_lifecycle"
        ).fetchone()
        assert row[0] is None  # never claimed
        assert row[1] is not None  # was closed
        assert row[2] == 0  # no active time


class TestExtractFactDepActivity:
    def test_basic_dep(self, duckdb_conn):
        t0 = datetime(2026, 4, 5, 15, 0, 0)
        dep_diffs = [
            {
                "to_issue_id": "bd-1", "to_depends_on_id": "bd-2",
                "to_type": "blocks", "to_created_at": t0,
                "to_created_by": "user", "diff_type": "added",
                "from_issue_id": None, "from_depends_on_id": None,
                "from_type": None, "from_created_at": None,
                "from_created_by": None,
            },
        ]

        # Populate lifecycle for after_first_claim check
        duckdb_conn.execute(
            "INSERT INTO fact_bead_lifecycle VALUES "
            "('bd-1', '2026-04-05 14:00:00', '2026-04-05 14:30:00', "
            "'2026-04-05 15:00:00', '2026-04-05 15:00:00', "
            "1800, 1800, 3600, 0, 1, 0, NULL, 0, 0, 0, NULL, NULL, false, 'user', 'user')"
        )

        cursor = MultiQueryCursor([dep_diffs])
        conn = MockConn(cursor)

        count = extract_fact_dep_activity(conn, duckdb_conn)
        assert count == 1

        row = duckdb_conn.execute(
            "SELECT dep_category, dep_event, after_first_claim FROM fact_dep_activity"
        ).fetchone()
        assert row[0] == "workflow"  # blocks -> workflow
        assert row[1] == "added"
        assert row[2] == True  # created after first_claimed_at

    def test_replan_detection(self, duckdb_conn):
        """remove + add same pair within 60s = replan."""
        t0 = datetime(2026, 4, 5, 15, 0, 0)
        dep_diffs = [
            {
                "to_issue_id": None, "to_depends_on_id": None,
                "to_type": None, "to_created_at": None,
                "to_created_by": None, "diff_type": "removed",
                "from_issue_id": "bd-1", "from_depends_on_id": "bd-2",
                "from_type": "blocks", "from_created_at": t0,
                "from_created_by": "user",
            },
            {
                "to_issue_id": "bd-1", "to_depends_on_id": "bd-2",
                "to_type": "blocks", "to_created_at": t0 + timedelta(seconds=30),
                "to_created_by": "user", "diff_type": "added",
                "from_issue_id": None, "from_depends_on_id": None,
                "from_type": None, "from_created_at": None,
                "from_created_by": None,
            },
        ]
        cursor = MultiQueryCursor([dep_diffs])
        conn = MockConn(cursor)
        count = extract_fact_dep_activity(conn, duckdb_conn)
        assert count == 2

        replan_rows = duckdb_conn.execute(
            "SELECT COUNT(*) FROM fact_dep_activity WHERE is_replan = true"
        ).fetchone()
        assert replan_rows[0] == 2  # both rows for the pair marked

    def test_non_workflow_dep(self, duckdb_conn):
        """A 'related' dep should map to 'association', not 'workflow'."""
        t0 = datetime(2026, 4, 5, 15, 0, 0)
        dep_diffs = [
            {
                "to_issue_id": "bd-1", "to_depends_on_id": "bd-2",
                "to_type": "related", "to_created_at": t0,
                "to_created_by": "u", "diff_type": "added",
                "from_issue_id": None, "from_depends_on_id": None,
                "from_type": None, "from_created_at": None,
                "from_created_by": None,
            },
        ]
        cursor = MultiQueryCursor([dep_diffs])
        conn = MockConn(cursor)
        extract_fact_dep_activity(conn, duckdb_conn)

        row = duckdb_conn.execute(
            "SELECT dep_category FROM fact_dep_activity"
        ).fetchone()
        assert row[0] == "association"

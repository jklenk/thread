"""Tests for DuckDB views — v_bead_scores, v_bead_dep_activity, mart_epic_summary, v_weekly_trends."""

from decimal import Decimal

import pytest


def _insert_dim_bead(conn, issue_id="bd-001", title="Test", issue_type="task",
                     priority=2, crystallizes=None, is_template=False,
                     estimated_minutes=None):
    conn.execute(
        "INSERT INTO dim_bead VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [issue_id, title, issue_type, priority, "user", "user@test.com",
         "user", estimated_minutes, True, False, False, None,
         crystallizes, "", is_template],
    )


def _insert_hierarchy(conn, issue_id="bd-001", parent_id=None, root_id=None,
                      depth=0):
    root = root_id or issue_id
    conn.execute(
        "INSERT INTO dim_hierarchy VALUES (?,?,?,?,?,?)",
        [issue_id, parent_id, root, depth, parent_id is None,
         f"{root}/{issue_id}" if parent_id else issue_id],
    )


def _insert_lifecycle(conn, issue_id="bd-001", reopen_count=0,
                      revision_requested_count=0, rejected_count=0,
                      active_time_secs=3600, compaction_level=0,
                      agent_actor_count=1, creator="user", closer="user",
                      created_at="2026-04-05 15:00:00",
                      final_closed_at="2026-04-05 16:00:00"):
    conn.execute(
        "INSERT INTO fact_bead_lifecycle VALUES "
        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [issue_id, created_at, "2026-04-05 15:02:00",
         final_closed_at, final_closed_at,
         120, active_time_secs, 3600,
         reopen_count, agent_actor_count, compaction_level, None,
         0, revision_requested_count, rejected_count,
         None, None, True, creator, closer],
    )


def _insert_actor(conn, actor_key="user", actor_class="agent",
                  source="heuristic"):
    conn.execute(
        "INSERT INTO dim_actor VALUES (?,?,?,?,?,?,?,?)",
        [actor_key, actor_key, None, None, None, None, actor_class, source],
    )


def _insert_dep_activity(conn, issue_id="bd-001", depends_on="bd-002",
                          dep_type="blocks", after_first_claim=True,
                          is_replan=False):
    conn.execute(
        "INSERT INTO fact_dep_activity VALUES (?,?,?,?,?,?,?,?,?)",
        [issue_id, depends_on, dep_type, "workflow", "added",
         "2026-04-05 15:30:00", "user", after_first_claim, is_replan],
    )


class TestVBeadScores:
    def test_fidelity_score_perfect(self, duckdb_conn):
        """Claimed bead with no reopens/rejections = 1.0 fidelity."""
        _insert_lifecycle(duckdb_conn, reopen_count=0, rejected_count=0)
        row = duckdb_conn.execute(
            "SELECT fidelity_score FROM v_bead_scores"
        ).fetchone()
        assert float(row[0]) == 1.0

    def test_fidelity_score_with_reopens(self, duckdb_conn):
        """1 reopen = 1.0 - (1*0.3) = 0.7."""
        _insert_lifecycle(duckdb_conn, reopen_count=1)
        row = duckdb_conn.execute(
            "SELECT fidelity_score FROM v_bead_scores"
        ).fetchone()
        assert float(row[0]) == 0.7

    def test_fidelity_score_skip_claim_penalty(self, duckdb_conn):
        """Closed without claiming incurs 0.4 penalty."""
        # Insert directly with NULL first_claimed_at
        duckdb_conn.execute(
            "INSERT INTO fact_bead_lifecycle VALUES "
            "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ["bd-skip", "2026-04-05 15:00:00", None,
             "2026-04-05 16:00:00", "2026-04-05 16:00:00",
             None, 3600, 3600,
             0, 1, 0, None, 0, 0, 0,
             None, None, True, "user", "user"],
        )
        row = duckdb_conn.execute(
            "SELECT fidelity_score FROM v_bead_scores WHERE issue_id = 'bd-skip'"
        ).fetchone()
        assert float(row[0]) == 0.6

    def test_effort_score_is_cost_multiple(self, duckdb_conn):
        """effort_score is now a relative cost multiple (active_time / median)."""
        _insert_lifecycle(duckdb_conn, active_time_secs=3600)
        row = duckdb_conn.execute(
            "SELECT effort_score, base_cost_hours FROM v_bead_scores"
        ).fetchone()
        # Single bead: active_time / median(active_time) = 1.0
        assert float(row[0]) == 1.0
        assert float(row[1]) == 1.0

    def test_effort_score_relative_to_median(self, duckdb_conn):
        """A bead 2x the median active time should have effort_score ~2.0."""
        # Insert two beads: one at 1000s, one at 2000s. Median = 1500.
        _insert_lifecycle(duckdb_conn, issue_id="bd-fast",
                         active_time_secs=1000)
        _insert_dim_bead(duckdb_conn, issue_id="bd-fast")
        _insert_lifecycle(duckdb_conn, issue_id="bd-slow",
                         active_time_secs=3000)
        _insert_dim_bead(duckdb_conn, issue_id="bd-slow")
        rows = duckdb_conn.execute(
            "SELECT issue_id, effort_score FROM v_bead_scores ORDER BY issue_id"
        ).fetchall()
        scores = {r[0]: float(r[1]) for r in rows}
        # median of [1000, 3000] = 2000. fast=0.5, slow=1.5
        assert scores["bd-fast"] == 0.5
        assert scores["bd-slow"] == 1.5

    def test_has_actor_keys(self, duckdb_conn):
        _insert_lifecycle(duckdb_conn, creator="alice", closer="bob")
        row = duckdb_conn.execute(
            "SELECT creator_actor_key, closer_actor_key FROM v_bead_scores"
        ).fetchone()
        assert row[0] == "alice"
        assert row[1] == "bob"


class TestVBeadDepActivity:
    def test_counts_workflow_only(self, duckdb_conn):
        """v_bead_dep_activity only counts workflow deps."""
        _insert_dep_activity(duckdb_conn, dep_type="blocks",
                            after_first_claim=True, is_replan=False)
        # Add a non-workflow dep that should be excluded
        duckdb_conn.execute(
            "INSERT INTO fact_dep_activity VALUES "
            "('bd-001','bd-003','related','association','added',"
            "'2026-04-05 15:30:00','user',true,false)"
        )
        row = duckdb_conn.execute(
            "SELECT total_dep_events, post_claim_dep_events FROM v_bead_dep_activity"
        ).fetchone()
        assert row[0] == 1  # only the workflow dep
        assert row[1] == 1

    def test_replan_counted(self, duckdb_conn):
        _insert_dep_activity(duckdb_conn, is_replan=True)
        row = duckdb_conn.execute(
            "SELECT replan_events FROM v_bead_dep_activity"
        ).fetchone()
        assert row[0] == 1


class TestMartEpicSummary:
    def test_aggregates_by_root(self, duckdb_conn):
        """Epic with two children should aggregate correctly."""
        _insert_dim_bead(duckdb_conn, "epic-1", "My Epic", "epic", 1)
        _insert_dim_bead(duckdb_conn, "task-1", "Task A", "task", 2)
        _insert_dim_bead(duckdb_conn, "task-2", "Task B", "task", 2)

        _insert_hierarchy(duckdb_conn, "epic-1", depth=0)
        _insert_hierarchy(duckdb_conn, "task-1", parent_id="epic-1",
                         root_id="epic-1", depth=1)
        _insert_hierarchy(duckdb_conn, "task-2", parent_id="epic-1",
                         root_id="epic-1", depth=1)

        _insert_actor(duckdb_conn, "user", "agent", "heuristic")

        _insert_lifecycle(duckdb_conn, "epic-1", closer="user")
        _insert_lifecycle(duckdb_conn, "task-1", reopen_count=1, closer="user")
        _insert_lifecycle(duckdb_conn, "task-2", closer="user")

        row = duckdb_conn.execute(
            "SELECT epic_title, bead_count, task_count, total_reopens, "
            "agent_closure_rate FROM mart_epic_summary"
        ).fetchone()
        assert row[0] == "My Epic"
        assert row[1] == 3  # epic + 2 tasks
        assert row[2] == 2  # 2 tasks at depth 1
        assert row[3] == 1  # 1 reopen from task-1
        assert float(row[4]) == 1.0  # all closed by agent

    def test_agent_closure_rate_mixed(self, duckdb_conn):
        """Mix of agent and human closers."""
        _insert_dim_bead(duckdb_conn, "e-1", "Epic", "epic", 1)
        _insert_hierarchy(duckdb_conn, "e-1")

        _insert_dim_bead(duckdb_conn, "t-1", "Task", "task", 2)
        _insert_hierarchy(duckdb_conn, "t-1", parent_id="e-1",
                         root_id="e-1", depth=1)

        _insert_actor(duckdb_conn, "agent-u", "agent", "heuristic")
        _insert_actor(duckdb_conn, "human-u", "human", "heuristic")

        _insert_lifecycle(duckdb_conn, "e-1", closer="human-u")
        _insert_lifecycle(duckdb_conn, "t-1", closer="agent-u")

        row = duckdb_conn.execute(
            "SELECT agent_closure_rate FROM mart_epic_summary"
        ).fetchone()
        assert float(row[0]) == 0.5  # 1 of 2 closed by agent

    def test_orphan_bead_counted(self, duckdb_conn):
        """Orphan (root_id not in dim_bead) should still appear."""
        _insert_hierarchy(duckdb_conn, "orphan-1")
        _insert_lifecycle(duckdb_conn, "orphan-1")
        row = duckdb_conn.execute(
            "SELECT orphan_bead_count FROM mart_epic_summary"
        ).fetchone()
        assert row[0] == 1

    def test_is_template_column_present(self, duckdb_conn):
        """mart_epic_summary should expose is_template for headline query filter."""
        _insert_dim_bead(duckdb_conn, "e-1", "Epic", "epic", 1, is_template=False)
        _insert_hierarchy(duckdb_conn, "e-1")
        _insert_lifecycle(duckdb_conn, "e-1")
        row = duckdb_conn.execute(
            "SELECT epic_is_template FROM mart_epic_summary"
        ).fetchone()
        assert row[0] == False

    def test_total_effort_penalty_fidelity_failures_only(self, duckdb_conn):
        """total_effort_penalty counts reopens/revisions/rejections only.

        Regression: old total_fidelity_penalty included compaction + agent floor,
        which degenerated to bead_count * 0.5 on clean agentic projects.
        """
        _insert_dim_bead(duckdb_conn, "e-1", "Epic", "epic", 1)
        _insert_hierarchy(duckdb_conn, "e-1")
        _insert_lifecycle(duckdb_conn, "e-1",
                         reopen_count=1, revision_requested_count=1,
                         rejected_count=1, compaction_level=5,
                         agent_actor_count=10)
        row = duckdb_conn.execute(
            "SELECT total_effort_penalty FROM mart_epic_summary"
        ).fetchone()
        # Only fidelity failures: 1*2.0 + 1*1.5 + 1*1.0 = 4.5
        # Compaction and agent count are NOT counted.
        assert float(row[0]) == 4.5

    def test_total_effort_penalty_zero_on_clean_project(self, duckdb_conn):
        """Clean agentic project should have zero penalty (bug regression)."""
        _insert_dim_bead(duckdb_conn, "e-1", "Epic", "epic", 1)
        _insert_hierarchy(duckdb_conn, "e-1")
        _insert_lifecycle(duckdb_conn, "e-1", agent_actor_count=1)
        row = duckdb_conn.execute(
            "SELECT total_effort_penalty FROM mart_epic_summary"
        ).fetchone()
        assert float(row[0]) == 0.0

    def test_elapsed_vs_estimate_ratio(self, duckdb_conn):
        """Epic took 60 minutes, estimated 30 → ratio 2.0."""
        _insert_dim_bead(duckdb_conn, "e-1", "Epic", "epic", 1,
                        estimated_minutes=30)
        _insert_hierarchy(duckdb_conn, "e-1")
        _insert_lifecycle(duckdb_conn, "e-1",
                         created_at="2026-04-05 15:00:00",
                         final_closed_at="2026-04-05 16:00:00")
        row = duckdb_conn.execute(
            "SELECT epic_elapsed_minutes, elapsed_vs_estimate_ratio "
            "FROM mart_epic_summary"
        ).fetchone()
        assert float(row[0]) == 60.0
        assert float(row[1]) == 2.0

    def test_elapsed_vs_estimate_null_when_no_estimate(self, duckdb_conn):
        """No estimate → ratio is NULL, not 0."""
        _insert_dim_bead(duckdb_conn, "e-1", "Epic", "epic", 1,
                        estimated_minutes=None)
        _insert_hierarchy(duckdb_conn, "e-1")
        _insert_lifecycle(duckdb_conn, "e-1")
        row = duckdb_conn.execute(
            "SELECT elapsed_vs_estimate_ratio FROM mart_epic_summary"
        ).fetchone()
        assert row[0] is None


class TestMartProjectSummary:
    def test_flat_workflow_detected(self, duckdb_conn):
        """Three singleton beads, no epics → epic_count=0, singletons=3."""
        for i in range(1, 4):
            _insert_dim_bead(duckdb_conn, f"b-{i}", f"Bead {i}", "task", 2)
            _insert_hierarchy(duckdb_conn, f"b-{i}")
            _insert_lifecycle(duckdb_conn, f"b-{i}")
        _insert_actor(duckdb_conn, "user", "agent", "heuristic")

        row = duckdb_conn.execute(
            "SELECT total_beads, singleton_bead_count, epic_count, "
            "agent_closure_rate FROM mart_project_summary"
        ).fetchone()
        assert row[0] == 3  # total_beads
        assert row[1] == 3  # singleton_bead_count
        assert row[2] == 0  # epic_count
        assert float(row[3]) == 1.0

    def test_epic_workflow_detected(self, duckdb_conn):
        """Epic with children → epic_count=1, singletons=0."""
        _insert_dim_bead(duckdb_conn, "e-1", "Epic", "epic", 1)
        _insert_hierarchy(duckdb_conn, "e-1")
        _insert_dim_bead(duckdb_conn, "t-1", "Task", "task", 2)
        _insert_hierarchy(duckdb_conn, "t-1", parent_id="e-1",
                         root_id="e-1", depth=1)
        _insert_lifecycle(duckdb_conn, "e-1")
        _insert_lifecycle(duckdb_conn, "t-1")
        _insert_actor(duckdb_conn, "user", "agent", "heuristic")

        row = duckdb_conn.execute(
            "SELECT total_beads, singleton_bead_count, epic_count "
            "FROM mart_project_summary"
        ).fetchone()
        assert row[0] == 2
        assert row[1] == 0  # e-1 has a child → not a singleton
        assert row[2] == 1

    def test_mixed_workflow_detected(self, duckdb_conn):
        """Epic with child + one singleton → epic=1, singleton=1."""
        _insert_dim_bead(duckdb_conn, "e-1", "Epic", "epic", 1)
        _insert_hierarchy(duckdb_conn, "e-1")
        _insert_dim_bead(duckdb_conn, "t-1", "Task", "task", 2)
        _insert_hierarchy(duckdb_conn, "t-1", parent_id="e-1",
                         root_id="e-1", depth=1)
        _insert_dim_bead(duckdb_conn, "s-1", "Solo", "task", 2)
        _insert_hierarchy(duckdb_conn, "s-1")
        for bid in ("e-1", "t-1", "s-1"):
            _insert_lifecycle(duckdb_conn, bid)
        _insert_actor(duckdb_conn, "user", "agent", "heuristic")

        row = duckdb_conn.execute(
            "SELECT epic_count, singleton_bead_count FROM mart_project_summary"
        ).fetchone()
        assert row[0] == 1
        assert row[1] == 1

    def test_templates_excluded(self, duckdb_conn):
        """Template beads should not appear in project summary."""
        _insert_dim_bead(duckdb_conn, "t-1", "Template", "task", 2,
                        is_template=True)
        _insert_hierarchy(duckdb_conn, "t-1")
        _insert_lifecycle(duckdb_conn, "t-1")
        row = duckdb_conn.execute(
            "SELECT total_beads FROM mart_project_summary"
        ).fetchone()
        assert row[0] == 0


class TestVWeeklyTrends:
    def test_weekly_grouping(self, duckdb_conn):
        """Beads closing in same week group together."""
        _insert_actor(duckdb_conn, "u", "agent", "heuristic")
        _insert_lifecycle(duckdb_conn, "b-1", closer="u",
                         final_closed_at="2026-04-06 10:00:00")
        _insert_lifecycle(duckdb_conn, "b-2", closer="u",
                         final_closed_at="2026-04-07 10:00:00")

        rows = duckdb_conn.execute(
            "SELECT beads_closed, agent_closure_rate FROM v_weekly_trends"
        ).fetchall()
        assert len(rows) == 1  # same week
        assert rows[0][0] == 2
        assert float(rows[0][1]) == 1.0  # all agent

    def test_excludes_unclosed(self, duckdb_conn):
        """Beads without final_closed_at should not appear."""
        duckdb_conn.execute(
            "INSERT INTO fact_bead_lifecycle VALUES "
            "('b-1','2026-04-05',NULL,NULL,NULL,NULL,0,NULL,"
            "0,0,0,NULL,0,0,0,NULL,NULL,false,NULL,NULL)"
        )
        rows = duckdb_conn.execute(
            "SELECT * FROM v_weekly_trends"
        ).fetchall()
        assert len(rows) == 0

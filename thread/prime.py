"""Thread prime — project health summary for humans and agents.

Generates both human-readable and JSON output from thread.duckdb.
All signal strings use plain outcome language — no technical terms.
"""

import json

import duckdb


# ============================================================
# Signal language — plain outcome, never technical
# ============================================================

def _fidelity_signal(score: float | None) -> str:
    if score is None:
        return "not enough completed work to measure scope faithfulness yet"
    if score >= 0.9:
        return "work is staying close to its original scope"
    if score >= 0.7:
        return "some work is drifting from original scope — worth a look"
    if score >= 0.5:
        return "agents are frequently not completing work as originally scoped"
    return "something structural is wrong with how work is being defined"


def _effort_signal(avg_effort: float | None) -> str:
    if avg_effort is None:
        return "not enough completed work to measure rework cost yet"
    # <= 0.5 covers the pure agent_actor_count floor with zero fidelity failures
    if avg_effort <= 0.5:
        return "work is completing cleanly on the first pass"
    if avg_effort < 2.0:
        return "a small amount of rework is normal discovery"
    if avg_effort < 5.0:
        return "rework is noticeable — worth a look at which beads are being reopened or revised"
    return "rework is significant — something structural is driving beads back into progress"


def _elapsed_signal(ratio: float | None) -> str:
    if ratio is None:
        return "no estimates recorded — cannot compare elapsed time to scope"
    if ratio <= 1.2:
        return "epics are completing close to their estimates — scoping is working well"
    if ratio <= 2.0:
        return "epics are running slightly over estimate — normal discovery"
    if ratio <= 3.0:
        return "epics are taking about 2-3x longer than estimated — consider breaking work into smaller pieces upfront"
    return "epics are taking much longer than estimated — scope was likely unclear at the start"


def _singleton_signal(count: int | None) -> str | None:
    if not count:
        return None
    noun = "bead was" if count == 1 else "beads were"
    return (
        f"{count} {noun} worked outside of any epic — grouping related "
        "work helps track whether efforts are staying on scope"
    )


def _quality_signal(avg_quality: float | None) -> str:
    if avg_quality is None:
        return "no review data available for this project"
    if avg_quality >= 0.8:
        return "reviews are largely positive"
    if avg_quality >= 0.6:
        return "reviews are mixed"
    return "reviews are flagging concerns"


def _agent_closure_signal(rate: float | None) -> str:
    if rate is None:
        return "no completed work yet"
    pct = int(round(rate * 100))
    if rate >= 0.75:
        return f"{pct}% of completed work was closed by an agent — your agentic workflow is well established"
    if rate >= 0.25:
        return f"{pct}% of completed work was closed by an agent — a mix of agent and human execution"
    if rate > 0:
        return f"{pct}% of completed work was closed by an agent — most work is still being done by humans"
    return "no agent-closed work detected"


def _dep_activity_signal(rate: float | None) -> str:
    if rate is None or rate == 0:
        return "little dependency churn — scope is stable"
    if rate < 1.0:
        return "occasional dependency changes — normal discovery"
    if rate < 3.0:
        return "dependency changes are frequent — review closed beads to assess if discovery was expected or scope was unclear"
    return "dependency churn is high — scope may be unclear at the start of work"


def _orphan_signal(rate: float | None) -> str:
    if rate is None or rate == 0:
        return "no orphaned work detected"
    pct = int(round(rate * 100))
    if rate < 0.05:
        return f"{pct}% of work is not linked to any larger effort — normal for quick fixes"
    if rate < 0.15:
        return f"{pct}% of bead activity involves work that was cleaned up or temporary — your agents are moving fast but some context may be lost"
    return f"{pct}% of work is not connected to larger efforts — you may be losing context across sessions"


def _actor_classification_note(sources: set[str]) -> str | None:
    """Plain explanation when heuristic classification is the primary path."""
    if "hop_uri" in sources or "role_type" in sources:
        return None  # Gas Town provides explicit attribution
    if sources == {"heuristic"} or sources == {"heuristic", "unknown"}:
        return (
            "Actor classification is based on behavior patterns (timing, batch "
            "closes, close reason specificity) because no explicit agent "
            "attribution is recorded. Treat agent/human labels as informed "
            "inference, not certainty."
        )
    return None


# ============================================================
# Main prime computation
# ============================================================

def _detect_workflow_type(epic_count: int, singleton_count: int) -> str:
    if epic_count > 0 and singleton_count == 0:
        return "epic"
    if epic_count == 0 and singleton_count > 0:
        return "flat"
    if epic_count == 0 and singleton_count == 0:
        return "empty"
    return "mixed"


def compute_prime(db_path: str) -> dict:
    """Query thread.duckdb and return the prime output dict.

    Adapts headline metrics to the workflow type found in the data:
      - epic: metrics from mart_epic_summary
      - flat: metrics from mart_project_summary directly
      - mixed: project-level from mart_project_summary, elapsed from mart_epic_summary
    """
    conn = duckdb.connect(db_path, read_only=True)

    try:
        # Workflow detection + project-level headline metrics in one query
        row = conn.execute(
            "SELECT total_beads, singleton_bead_count, epic_count, "
            "  avg_fidelity_score, avg_effort_score, agent_closure_rate "
            "FROM mart_project_summary"
        ).fetchone()
        total_beads = row[0] if row and row[0] else 0
        singleton_count = row[1] if row and row[1] else 0
        epic_count = row[2] if row and row[2] else 0
        fidelity = float(row[3]) if row and row[3] is not None else None
        effort = float(row[4]) if row and row[4] is not None else None
        agent_closure_rate = float(row[5]) if row and row[5] is not None else None

        workflow_type = _detect_workflow_type(epic_count, singleton_count)

        # Average quality across closed beads (not in mart_project_summary)
        row = conn.execute(
            "SELECT AVG(s.quality_score) FROM v_bead_scores s "
            "JOIN fact_bead_lifecycle f ON f.issue_id = s.issue_id "
            "WHERE f.final_closed_at IS NOT NULL"
        ).fetchone()
        quality = float(row[0]) if row[0] is not None else None

        # Elapsed vs estimate — only meaningful when epics exist
        elapsed_ratio = None
        if workflow_type in ("epic", "mixed"):
            row = conn.execute(
                "SELECT AVG(elapsed_vs_estimate_ratio) FROM mart_epic_summary "
                "WHERE (epic_is_template = false OR epic_is_template IS NULL) "
                "  AND elapsed_vs_estimate_ratio IS NOT NULL"
            ).fetchone()
            elapsed_ratio = float(row[0]) if row[0] is not None else None

        # Dep activity rate (events per bead)
        row = conn.execute(
            "SELECT COUNT(*) * 1.0 / NULLIF("
            "  (SELECT COUNT(*) FROM fact_bead_lifecycle), 0"
            ") FROM fact_dep_activity WHERE dep_category = 'workflow'"
        ).fetchone()
        dep_activity_rate = float(row[0]) if row[0] is not None else None

        # Orphan rate: beads where hierarchy root has no matching dim_bead
        row = conn.execute(
            "SELECT "
            "  COUNT(CASE WHEN epic.issue_id IS NULL THEN 1 END) * 1.0 "
            "  / NULLIF(COUNT(*), 0) "
            "FROM dim_hierarchy h "
            "LEFT JOIN dim_bead epic ON epic.issue_id = h.root_id"
        ).fetchone()
        orphan_rate = float(row[0]) if row[0] is not None else None

        # Actor classification sources in use
        sources_rows = conn.execute(
            "SELECT DISTINCT classification_source FROM dim_actor"
        ).fetchall()
        sources = {r[0] for r in sources_rows if r[0]}

    finally:
        conn.close()

    return {
        "workflow_type": workflow_type,
        "total_beads": int(total_beads),
        "epic_count": int(epic_count),
        "singleton_bead_count": int(singleton_count),
        "singleton_signal": _singleton_signal(singleton_count),

        "project_fidelity_score": round(fidelity, 2) if fidelity is not None else None,
        "fidelity_signal": _fidelity_signal(fidelity),

        "avg_effort_score": round(effort, 2) if effort is not None else None,
        "effort_signal": _effort_signal(effort),

        "avg_elapsed_vs_estimate": round(elapsed_ratio, 2) if elapsed_ratio is not None else None,
        "elapsed_signal": _elapsed_signal(elapsed_ratio),

        "avg_quality_score": round(quality, 2) if quality is not None else None,
        "quality_signal": _quality_signal(quality),
        "quality_score_note": None,

        "agent_closure_rate": round(agent_closure_rate, 2) if agent_closure_rate is not None else None,
        "agent_closure_signal": _agent_closure_signal(agent_closure_rate),

        "dep_activity_rate": round(dep_activity_rate, 2) if dep_activity_rate is not None else None,
        "dep_activity_signal": _dep_activity_signal(dep_activity_rate),

        "orphan_bead_rate": round(orphan_rate, 2) if orphan_rate is not None else None,
        "orphan_signal": _orphan_signal(orphan_rate),

        "actor_classification_note": _actor_classification_note(sources),
    }


_WORKFLOW_LABELS = {
    "epic": "epic-driven (all beads belong to an epic)",
    "flat": "flat (beads are tracked individually without epics)",
    "mixed": "mixed (some beads belong to epics, others don't)",
    "empty": "no beads yet",
}


def format_human(data: dict) -> str:
    """Format prime output as human-readable text."""
    lines = []
    lines.append("Thread — project health")
    lines.append("=" * 60)
    lines.append("")

    # Workflow shape
    wf = data.get("workflow_type", "empty")
    lines.append(f"  Workflow: {_WORKFLOW_LABELS.get(wf, wf)}")
    lines.append(
        f"    {data.get('total_beads', 0)} beads · "
        f"{data.get('epic_count', 0)} epics · "
        f"{data.get('singleton_bead_count', 0)} singletons"
    )
    if data.get("singleton_signal"):
        lines.append(f"    {data['singleton_signal']}")
    lines.append("")

    def fmt_metric(label, value, signal):
        v = "—" if value is None else str(value)
        lines.append(f"  {label}: {v}")
        lines.append(f"    {signal}")
        lines.append("")

    fmt_metric("Fidelity score", data["project_fidelity_score"],
               data["fidelity_signal"])
    fmt_metric("Rework cost (avg)", data["avg_effort_score"],
               data["effort_signal"])

    # Elapsed vs estimate is only meaningful for epic/mixed workflows
    if wf in ("epic", "mixed"):
        fmt_metric("Elapsed vs estimate", data["avg_elapsed_vs_estimate"],
                   data["elapsed_signal"])

    fmt_metric("Review quality (avg)", data["avg_quality_score"],
               data["quality_signal"])
    fmt_metric("Agent closure rate", data["agent_closure_rate"],
               data["agent_closure_signal"])
    fmt_metric("Dependency activity", data["dep_activity_rate"],
               data["dep_activity_signal"])
    fmt_metric("Orphan rate", data["orphan_bead_rate"],
               data["orphan_signal"])

    if data.get("actor_classification_note"):
        lines.append("Note on actor classification:")
        lines.append(f"  {data['actor_classification_note']}")
        lines.append("")

    return "\n".join(lines)


def format_json(data: dict) -> str:
    return json.dumps(data, indent=2)

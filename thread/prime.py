"""Thread prime — project health summary for humans and agents.

Thread v2: replaces flatlined fidelity/effort/elapsed-vs-estimate metrics
with session-based, cycle-time, throughput, relative cost, compliance,
and behavioral signals. Every metric gets a verdict (good/watch/concern).

All signal strings use plain outcome language — no technical terms.
Cost is expressed as multiples of the project median cycle time, NEVER dollars.
"""

import json
from pathlib import Path

import duckdb


# ============================================================
# Verdicts: good / watch / concern
# ============================================================

def _verdict(good_test: bool, watch_test: bool) -> str:
    if good_test:
        return "good"
    if watch_test:
        return "watch"
    return "concern"

_VERDICT_ICON = {"good": "[+]", "watch": "[~]", "concern": "[!]"}


# ============================================================
# Time formatting helpers
# ============================================================

def _fmt_duration(secs) -> str:
    """Format seconds as human-readable duration."""
    if secs is None:
        return "—"
    secs = int(secs)
    if secs < 60:
        return f"{secs}s"
    if secs < 3600:
        m, s = divmod(secs, 60)
        return f"{m}m {s}s" if s else f"{m}m"
    h, rem = divmod(secs, 3600)
    m = rem // 60
    return f"{h}h {m}m" if m else f"{h}h"


# ============================================================
# Workflow type detection (unchanged)
# ============================================================

def _detect_workflow_type(epic_count: int, singleton_count: int) -> str:
    if epic_count > 0 and singleton_count == 0:
        return "epic"
    if epic_count == 0 and singleton_count > 0:
        return "flat"
    if epic_count == 0 and singleton_count == 0:
        return "empty"
    return "mixed"


# ============================================================
# Signal functions — plain outcome language, never jargon
# ============================================================

def _completion_signal(rate: float | None, open_count: int) -> str:
    if rate is None:
        return "no beads recorded yet"
    pct = int(round(rate * 100))
    if open_count > 0:
        suffix = f" — {open_count} beads are still open"
    else:
        suffix = ""
    if rate >= 0.9:
        return f"{pct}% of beads reached completion{suffix}"
    if rate >= 0.7:
        return f"{pct}% of beads completed — check if the rest are blocked or abandoned{suffix}"
    return f"only {pct}% of beads completed — significant backlog{suffix}"


def _cycle_time_signal(p50_secs, p90_secs) -> str:
    if p50_secs is None:
        return "no completed beads to measure cycle time"
    p50 = _fmt_duration(p50_secs)
    p90 = _fmt_duration(p90_secs)
    if p90_secs and p50_secs and p50_secs > 0 and p90_secs / p50_secs > 5:
        return f"90% of work completes in under {p90} but the spread is wide — investigate the outliers"
    if (p50_secs or 0) <= 300:
        return f"90% of work completes in under {p90} — your agents are fast"
    if (p50_secs or 0) <= 900:
        return f"median cycle time is {p50} — reasonable pace"
    return f"median cycle time is {p50} — some beads are taking a while"


def _throughput_signal(beads_per_day: float | None, active_days: int) -> str:
    if beads_per_day is None or active_days == 0:
        return "not enough data to measure throughput"
    bpd = round(beads_per_day, 1)
    return f"{bpd} beads/day across {active_days} active days"


def _cost_spread_signal(p90_p50_ratio: float | None) -> str:
    if p90_p50_ratio is None:
        return "not enough data to measure cost spread"
    r = round(p90_p50_ratio, 1)
    if r <= 2.0:
        return "cost is consistent across beads"
    if r <= 5.0:
        return f"p90 costs {r}x the median — some beads are notably more expensive"
    return f"p90 costs {r}x the median — investigate the expensive tail"


def _parallelism_signal(ratio: float | None) -> str:
    if ratio is None:
        return "not enough session data to measure parallelism"
    if ratio > 1.5:
        return f"agents are running {ratio:.1f}x parallel work — good utilization"
    if ratio > 0.8:
        return "work is mostly sequential with some overlap"
    return "work is sequential"


def _scope_stability_signal(rate: float | None) -> str:
    if rate is None or rate == 0:
        return "scope held steady during execution"
    if rate <= 0.05:
        return "scope held steady during execution"
    if rate <= 0.15:
        return f"{rate:.2f} scope changes per bead after claiming — some discovery in flight"
    return f"{rate:.2f} scope changes per bead after claiming — scope was unclear upfront"


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
        return "dependency changes are frequent — review if discovery was expected or scope was unclear"
    return "dependency churn is high — scope may be unclear at the start of work"


def _singleton_signal(count: int | None, workflow_type: str) -> str | None:
    if not count:
        return None
    noun = "bead" if count == 1 else "beads"
    if workflow_type == "flat":
        return f"{count} {noun} tracked individually — normal for this workflow"
    return f"{count} {noun} worked outside of any epic"


def _queue_wait_signal(p50_secs, p90_secs, claimed_count: int, skip_count: int) -> str:
    if p50_secs is None:
        return "no claimed beads to measure queue wait"
    p50 = _fmt_duration(p50_secs)
    suffix = ""
    if skip_count > 0:
        suffix = f" ({skip_count} beads skipped claim entirely)"
    if (p50_secs or 0) <= 60:
        return f"beads are picked up almost immediately{suffix}"
    if (p50_secs or 0) <= 300:
        return f"median wait of {p50} — beads sit briefly before pickup{suffix}"
    return f"median wait of {p50} — agents may be overloaded or not watching{suffix}"


def _skip_claim_signal(rate: float | None, count: int, total: int) -> str:
    if total == 0:
        return "no completed beads to check"
    pct = int(round((rate or 0) * 100))
    if (rate or 0) <= 0.10:
        return f"{count} of {total} beads closed without claiming — agents are following the claim workflow"
    if (rate or 0) <= 0.30:
        return f"{count} of {total} beads closed without claiming — agents are sometimes skipping assignment"
    return f"{count} of {total} beads closed without claiming — agents are skipping the claim step"


def _documentation_signal(rate: float | None) -> str:
    if rate is None:
        return "no completed beads to check"
    pct = int(round(rate * 100))
    if rate >= 0.80:
        return f"{pct}% of closes have a descriptive reason — good context for review"
    if rate >= 0.50:
        return f"{pct}% of closes have a descriptive reason — consider requiring close reasons"
    return f"only {pct}% of closes have context — most closes lack explanation"


def _dep_order_signal(violation_count: int) -> str:
    if violation_count == 0:
        return "all blockers resolved before dependent work was closed"
    if violation_count <= 2:
        return f"{violation_count} beads closed before their blockers — minor ordering issue"
    return f"{violation_count} beads closed before their blockers were resolved — check dependency definitions"


def _trend_signal(direction: str, change_pct: float | None, first_p50, second_p50) -> str:
    if direction == "insufficient":
        return "not enough data points to compute a trend"
    f = _fmt_duration(first_p50)
    s = _fmt_duration(second_p50)
    pct = abs(int(round(change_pct or 0)))
    if direction == "improving":
        return f"cycle time improved {pct}% ({f} to {s} median) — agents are getting faster"
    if direction == "regressing":
        return f"cycle time increased {pct}% ({f} to {s} median) — check recent sessions for outliers"
    return f"cycle time is stable ({f} to {s} median)"


def _interactions_signal(total: int, by_kind: dict) -> str:
    if total == 0:
        return "no interaction data recorded"
    parts = [f"{v} {k}" for k, v in sorted(by_kind.items(), key=lambda x: -x[1])]
    kind_str = ", ".join(parts)
    llm_count = by_kind.get("llm_call", 0)
    tool_count = by_kind.get("tool_call", 0)
    if llm_count == 0 and tool_count == 0:
        return f"all interactions are status changes — no LLM or tool calls recorded yet"
    return f"{total} interactions ({kind_str})"


def _actor_classification_note(sources: set[str]) -> str | None:
    if "hop_uri" in sources or "role_type" in sources:
        return None
    if sources == {"heuristic"} or sources == {"heuristic", "unknown"}:
        return (
            "Actor classification is based on behavior patterns (timing, batch "
            "closes, close reason specificity) because no explicit agent "
            "attribution is recorded. Treat agent/human labels as informed "
            "inference, not certainty."
        )
    return None


def _session_assessment(session: dict, baseline_cycle_secs: float | None) -> str:
    """One-line plain-language assessment of a session vs project baseline."""
    beads = session.get("bead_count", 0)
    closed = session.get("beads_closed", 0)
    open_b = session.get("beads_open", 0)
    epics = session.get("epics_touched", 0)

    # Planning-only session: beads created but none closed
    if closed == 0 and beads > 0:
        return "planning only — beads created but not started"

    parts = []

    # Compare to baseline
    avg_cycle = session.get("avg_cycle_time_secs")
    if baseline_cycle_secs and avg_cycle and baseline_cycle_secs > 0:
        ratio = avg_cycle / baseline_cycle_secs
        if ratio > 3.0:
            parts.append(f"{ratio:.0f}x slower than baseline")
        elif ratio > 1.5:
            parts.append(f"{ratio:.1f}x slower than baseline")
        elif ratio < 0.6:
            parts.append("faster than usual")
        else:
            parts.append("normal pace")
    else:
        parts.append("normal pace")

    if open_b > 0:
        parts.append(f"{open_b} unfinished")
    elif closed == beads:
        parts.append("all work completed")

    if epics and epics > 0:
        parts.append(f"{epics} epic{'s' if epics > 1 else ''}")

    return " — ".join(parts)


def _session_verdict(session: dict, compliance: dict | None,
                     baseline_cycle_secs: float | None) -> str:
    """Verdict for a single session based on completion + cost + compliance."""
    closed = session.get("beads_closed", 0)
    beads = session.get("bead_count", 0)
    avg_cycle = session.get("avg_cycle_time_secs")

    # Compliance-driven
    if compliance:
        if compliance.get("dep_violations", 0) > 0:
            return "concern"
        skip = compliance.get("skip_claim_count", 0)
        if beads > 0 and skip / beads > 0.5:
            return "concern"
        if skip > 0 or (compliance.get("documented_closes", 0) == 0 and closed > 0):
            comp_v = "watch"
        else:
            comp_v = "good"
    else:
        comp_v = "good"

    # Cost-driven
    if baseline_cycle_secs and avg_cycle and baseline_cycle_secs > 0:
        ratio = avg_cycle / baseline_cycle_secs
        if ratio > 5.0:
            return "concern"
        if ratio > 2.0:
            return max("watch", comp_v)

    # Completion-driven
    if beads > 0 and closed == 0:
        return max("watch", comp_v)

    return comp_v


# ============================================================
# Main prime computation
# ============================================================

def compute_prime(db_path: str, beads_dir: str | None = None) -> dict:
    """Query thread.duckdb and return the prime output dict.

    Every metric includes a numeric value, a plain-language signal, and a
    good/watch/concern verdict. Cost is expressed as multiples of the
    project median cycle time — never dollars.
    """
    conn = duckdb.connect(db_path, read_only=True)

    try:
        data = {}

        # -- Project shape --
        row = conn.execute(
            "SELECT total_beads, singleton_bead_count, epic_count, "
            "  agent_closure_rate "
            "FROM mart_project_summary"
        ).fetchone()
        total_beads = row[0] if row and row[0] else 0
        singleton_count = row[1] if row and row[1] else 0
        epic_count = row[2] if row and row[2] else 0
        agent_closure_rate = float(row[3]) if row and row[3] is not None else None
        workflow_type = _detect_workflow_type(epic_count, singleton_count)

        data["workflow_type"] = workflow_type
        data["total_beads"] = int(total_beads)
        data["epic_count"] = int(epic_count)
        data["singleton_bead_count"] = int(singleton_count)
        data["singleton_signal"] = _singleton_signal(singleton_count, workflow_type)

        # -- Completion --
        row = conn.execute(
            "SELECT "
            "  COUNT(CASE WHEN final_closed_at IS NOT NULL THEN 1 END), "
            "  COUNT(CASE WHEN final_closed_at IS NULL THEN 1 END), "
            "  COUNT(CASE WHEN final_closed_at IS NULL AND first_claimed_at IS NULL THEN 1 END) "
            "FROM fact_bead_lifecycle"
        ).fetchone()
        closed_count = row[0] if row else 0
        open_count = row[1] if row else 0
        unclaimed_open = row[2] if row else 0
        completion_rate = closed_count / total_beads if total_beads > 0 else None
        completion_verdict = _verdict(
            (completion_rate or 0) >= 0.9,
            (completion_rate or 0) >= 0.7,
        )

        data["closed_count"] = closed_count
        data["open_count"] = open_count
        data["unclaimed_open_count"] = unclaimed_open
        data["completion_rate"] = round(completion_rate, 2) if completion_rate is not None else None
        data["completion_signal"] = _completion_signal(completion_rate, open_count)
        data["completion_verdict"] = completion_verdict

        # -- Cycle time (from closed beads' active_time_secs) --
        row = conn.execute(
            "SELECT "
            "  ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY active_time_secs), 0), "
            "  ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY active_time_secs), 0), "
            "  MAX(active_time_secs) "
            "FROM fact_bead_lifecycle "
            "WHERE final_closed_at IS NOT NULL AND active_time_secs IS NOT NULL"
        ).fetchone()
        ct_p50 = int(row[0]) if row and row[0] is not None else None
        ct_p90 = int(row[1]) if row and row[1] is not None else None
        ct_max = int(row[2]) if row and row[2] is not None else None
        ct_verdict = _verdict(
            (ct_p50 or 999999) <= 300,
            (ct_p50 or 999999) <= 900,
        )
        # Additional concern if p90 > 5x p50
        if ct_p50 and ct_p90 and ct_p50 > 0 and ct_p90 / ct_p50 > 5:
            ct_verdict = "concern"

        data["cycle_time_p50_secs"] = ct_p50
        data["cycle_time_p90_secs"] = ct_p90
        data["cycle_time_max_secs"] = ct_max
        data["cycle_time_signal"] = _cycle_time_signal(ct_p50, ct_p90)
        data["cycle_time_verdict"] = ct_verdict

        # -- Throughput --
        daily_rows = conn.execute(
            "SELECT day, beads_closed, median_cycle_time_secs "
            "FROM v_daily_trends ORDER BY day"
        ).fetchall()
        active_days = len(daily_rows)
        total_closed_daily = sum(r[1] for r in daily_rows) if daily_rows else 0
        beads_per_day = total_closed_daily / active_days if active_days > 0 else None
        tp_verdict = _verdict(
            (beads_per_day or 0) >= 10,
            (beads_per_day or 0) >= 3,
        )

        data["throughput_beads_per_day"] = round(beads_per_day, 1) if beads_per_day is not None else None
        data["throughput_active_days"] = active_days
        data["throughput_signal"] = _throughput_signal(beads_per_day, active_days)
        data["throughput_verdict"] = tp_verdict

        # -- Relative cost (p90/p50 multiple) --
        # Uses active_time_secs, NOT total_elapsed_secs (which includes queue wait)
        cost_p90_multiple = round(ct_p90 / ct_p50, 1) if ct_p50 and ct_p90 and ct_p50 > 0 else None
        total_compute_row = conn.execute(
            "SELECT SUM(active_time_secs) FROM fact_bead_lifecycle "
            "WHERE final_closed_at IS NOT NULL"
        ).fetchone()
        total_compute_secs = int(total_compute_row[0]) if total_compute_row and total_compute_row[0] else 0
        cost_verdict = _verdict(
            (cost_p90_multiple or 0) <= 2.0,
            (cost_p90_multiple or 0) <= 5.0,
        )

        data["cost_p90_multiple"] = cost_p90_multiple
        data["cost_distribution_signal"] = _cost_spread_signal(cost_p90_multiple)
        data["cost_verdict"] = cost_verdict
        data["total_compute_time_secs"] = total_compute_secs
        data["total_compute_time_human"] = _fmt_duration(total_compute_secs)
        data["median_cycle_secs"] = ct_p50  # project baseline for per-session cost

        # -- Parallelism --
        par_row = conn.execute(
            "SELECT ROUND(AVG(parallelism_ratio), 2) "
            "FROM mart_session_summary WHERE beads_closed > 0"
        ).fetchone()
        parallelism = float(par_row[0]) if par_row and par_row[0] is not None else None
        par_verdict = _verdict(
            (parallelism or 0) > 1.0,
            (parallelism or 0) > 0.5,
        )

        data["parallelism_ratio"] = parallelism
        data["parallelism_signal"] = _parallelism_signal(parallelism)
        data["parallelism_verdict"] = par_verdict

        # -- Agent closure rate --
        acr_verdict = _verdict(
            (agent_closure_rate or 0) >= 0.75,
            (agent_closure_rate or 0) >= 0.25,
        )

        data["agent_closure_rate"] = round(agent_closure_rate, 2) if agent_closure_rate is not None else None
        data["agent_closure_signal"] = _agent_closure_signal(agent_closure_rate)
        data["agent_closure_verdict"] = acr_verdict

        # -- Scope stability --
        scope_row = conn.execute(
            "SELECT COALESCE(SUM(post_claim_dep_events), 0) "
            "FROM v_bead_dep_activity"
        ).fetchone()
        post_claim_total = scope_row[0] if scope_row else 0
        scope_rate = post_claim_total / total_beads if total_beads > 0 else 0.0
        scope_verdict = _verdict(scope_rate <= 0.05, scope_rate <= 0.15)

        data["scope_stability_rate"] = round(scope_rate, 3)
        data["scope_stability_signal"] = _scope_stability_signal(scope_rate)
        data["scope_stability_verdict"] = scope_verdict

        # -- Dependency churn --
        dep_row = conn.execute(
            "SELECT COUNT(*) * 1.0 / NULLIF("
            "  (SELECT COUNT(*) FROM fact_bead_lifecycle), 0"
            ") FROM fact_dep_activity WHERE dep_category = 'workflow'"
        ).fetchone()
        dep_rate = float(dep_row[0]) if dep_row and dep_row[0] is not None else None

        data["dep_activity_rate"] = round(dep_rate, 2) if dep_rate is not None else None
        data["dep_activity_signal"] = _dep_activity_signal(dep_rate)

        # -- Compliance: skip-claim --
        skip_row = conn.execute(
            "SELECT "
            "  COUNT(CASE WHEN first_claimed_at IS NULL AND final_closed_at IS NOT NULL THEN 1 END), "
            "  COUNT(CASE WHEN final_closed_at IS NOT NULL THEN 1 END) "
            "FROM fact_bead_lifecycle"
        ).fetchone()
        skip_claim_count = skip_row[0] if skip_row else 0
        total_closed_for_compliance = skip_row[1] if skip_row else 0
        skip_claim_rate = (
            skip_claim_count / total_closed_for_compliance
            if total_closed_for_compliance > 0 else None
        )
        skip_verdict = _verdict(
            (skip_claim_rate or 0) <= 0.10,
            (skip_claim_rate or 0) <= 0.30,
        )

        data["skip_claim_count"] = skip_claim_count
        data["skip_claim_rate"] = round(skip_claim_rate, 2) if skip_claim_rate is not None else None
        data["skip_claim_signal"] = _skip_claim_signal(
            skip_claim_rate, skip_claim_count, total_closed_for_compliance,
        )
        data["skip_claim_verdict"] = skip_verdict

        # -- Compliance: documentation rate --
        doc_row = conn.execute(
            "SELECT COUNT(DISTINCT issue_id) FROM v_close_reasons"
        ).fetchone()
        documented_closes = doc_row[0] if doc_row else 0
        doc_rate = (
            documented_closes / total_closed_for_compliance
            if total_closed_for_compliance > 0 else None
        )
        doc_verdict = _verdict(
            (doc_rate or 0) >= 0.80,
            (doc_rate or 0) >= 0.50,
        )

        data["documentation_rate"] = round(doc_rate, 2) if doc_rate is not None else None
        data["documentation_signal"] = _documentation_signal(doc_rate)
        data["documentation_verdict"] = doc_verdict

        # -- Compliance: dependency order violations --
        viol_row = conn.execute(
            "SELECT COUNT(*) FROM v_dep_order_violations"
        ).fetchone()
        dep_violations = viol_row[0] if viol_row else 0
        dep_viol_verdict = _verdict(dep_violations == 0, dep_violations <= 2)

        data["dep_order_violations"] = dep_violations
        data["dep_order_signal"] = _dep_order_signal(dep_violations)
        data["dep_order_verdict"] = dep_viol_verdict

        # -- Queue wait --
        qw_row = conn.execute(
            "SELECT "
            "  ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY time_to_start_secs), 0), "
            "  ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY time_to_start_secs), 0), "
            "  COUNT(*) "
            "FROM v_queue_wait"
        ).fetchone()
        qw_p50 = int(qw_row[0]) if qw_row and qw_row[0] is not None else None
        qw_p90 = int(qw_row[1]) if qw_row and qw_row[1] is not None else None
        qw_claimed_count = qw_row[2] if qw_row else 0
        qw_verdict = _verdict(
            (qw_p50 or 999999) <= 60,
            (qw_p50 or 999999) <= 300,
        )

        data["queue_wait_p50_secs"] = qw_p50
        data["queue_wait_p90_secs"] = qw_p90
        data["queue_wait_signal"] = _queue_wait_signal(
            qw_p50, qw_p90, qw_claimed_count, skip_claim_count,
        )
        data["queue_wait_verdict"] = qw_verdict

        # -- By type --
        type_rows = conn.execute(
            "SELECT issue_type, bead_count, completed, median_cycle_secs "
            "FROM v_type_performance WHERE issue_type IS NOT NULL"
        ).fetchall()
        type_perf = [
            {
                "type": r[0],
                "count": int(r[1]),
                "completed": int(r[2]) if r[2] is not None else 0,
                "median_cycle_secs": int(r[3]) if r[3] is not None else None,
            }
            for r in type_rows
        ]
        data["type_performance"] = type_perf

        # -- By priority --
        prio_rows = conn.execute(
            "SELECT priority, bead_count, completed, median_cycle_secs, avg_wait_secs "
            "FROM v_priority_performance WHERE priority IS NOT NULL"
        ).fetchall()
        prio_perf = [
            {
                "priority": int(r[0]),
                "count": int(r[1]),
                "completed": int(r[2]) if r[2] is not None else 0,
                "median_cycle_secs": int(r[3]) if r[3] is not None else None,
                "avg_wait_secs": int(r[4]) if r[4] is not None else None,
            }
            for r in prio_rows
        ]
        data["priority_performance"] = prio_perf

        # -- Spec quality --
        spec_rows = conn.execute(
            "SELECT spec_level, bead_count, completed, median_cycle_secs "
            "FROM v_spec_quality_correlation"
        ).fetchall()
        spec_perf = [
            {
                "level": r[0],
                "count": int(r[1]),
                "median_cycle_secs": int(r[2]) if r[2] is not None else None,
                "completion_rate": round(
                    (int(r[2]) if r[2] else 0) / int(r[1]), 2
                ) if int(r[1]) > 0 else None,
            }
            for r in spec_rows
        ]
        # Fix: completion_rate should use completed (r[2]), not median_cycle_secs
        spec_perf = [
            {
                "level": r[0],
                "count": int(r[1]),
                "median_cycle_secs": int(r[3]) if r[3] is not None else None,
                "completion_rate": round(
                    (int(r[2]) if r[2] else 0) / int(r[1]), 2
                ) if int(r[1]) > 0 else None,
            }
            for r in spec_rows
        ]
        data["spec_quality"] = spec_perf

        # -- Improvement trend --
        if len(daily_rows) >= 4:
            midpoint = len(daily_rows) // 2
            first_half_medians = [
                r[2] for r in daily_rows[:midpoint] if r[2] is not None
            ]
            second_half_medians = [
                r[2] for r in daily_rows[midpoint:] if r[2] is not None
            ]
            if first_half_medians and second_half_medians:
                first_p50 = sorted(first_half_medians)[len(first_half_medians) // 2]
                second_p50 = sorted(second_half_medians)[len(second_half_medians) // 2]
                if first_p50 > 0:
                    change_pct = (first_p50 - second_p50) / first_p50 * 100
                    if change_pct > 10:
                        direction = "improving"
                    elif change_pct < -10:
                        direction = "regressing"
                    else:
                        direction = "stable"
                else:
                    change_pct = 0
                    direction = "stable"
                data["trend"] = {
                    "direction": direction,
                    "cycle_time_change_pct": round(change_pct, 1),
                    "first_half_p50_secs": int(first_p50),
                    "second_half_p50_secs": int(second_p50),
                    "signal": _trend_signal(direction, change_pct, first_p50, second_p50),
                }
            else:
                data["trend"] = {
                    "direction": "insufficient",
                    "signal": "not enough data points to compute a trend",
                }
        else:
            data["trend"] = {
                "direction": "insufficient",
                "signal": "not enough data points to compute a trend",
            }

        # -- Interactions --
        int_rows = conn.execute(
            "SELECT kind, count FROM v_interaction_summary"
        ).fetchall()
        total_interactions = sum(r[1] for r in int_rows) if int_rows else 0
        by_kind = {r[0]: int(r[1]) for r in int_rows} if int_rows else {}

        # Check if interactions data is actually present or missing
        if total_interactions == 0:
            # Determine if the file was missing or empty by checking the table
            fi_count = conn.execute(
                "SELECT COUNT(*) FROM fact_interactions"
            ).fetchone()[0]
            if fi_count == 0:
                int_status = "missing"
                int_message = (
                    "interactions.jsonl not found or empty — audit trail unavailable. "
                    "Run bd compact --audit or enable agent hooks to populate."
                )
            else:
                int_status = "empty"
                int_message = "interaction records exist but contain no categorized data"
        else:
            int_status = "populated"
            int_message = _interactions_signal(total_interactions, by_kind)

        # Models and tools from interactions
        model_rows = conn.execute(
            "SELECT model, calls FROM v_model_usage ORDER BY calls DESC"
        ).fetchall()
        models_used = [{"model": r[0], "count": int(r[1])} for r in model_rows] if model_rows else []

        tool_rows = conn.execute(
            "SELECT tool_name, calls, successes, failures, success_rate "
            "FROM v_tool_usage ORDER BY calls DESC"
        ).fetchall()
        tools_used = [
            {"tool": r[0], "calls": int(r[1]), "success_rate": float(r[4]) if r[4] else None}
            for r in tool_rows
        ] if tool_rows else []

        tool_success_rate = None
        if tools_used:
            total_calls = sum(t["calls"] for t in tools_used)
            success_calls = sum(int(r[2]) for r in tool_rows)
            tool_success_rate = round(success_calls / total_calls, 3) if total_calls > 0 else None

        data["interactions"] = {
            "status": int_status,
            "total": total_interactions,
            "by_kind": by_kind,
            "models_used": models_used,
            "tools_used": tools_used,
            "tool_success_rate": tool_success_rate,
            "signal": int_message if int_status != "populated" else _interactions_signal(total_interactions, by_kind),
            "message": int_message,
        }

        # -- Agent knowledge --
        mem_row = conn.execute(
            "SELECT COUNT(*) FROM dim_agent_memory"
        ).fetchone()
        mem_count = mem_row[0] if mem_row else 0

        mem_entries = conn.execute(
            "SELECT memory_key, memory_value FROM dim_agent_memory ORDER BY memory_key"
        ).fetchall()
        memories = [{"key": r[0], "value": r[1]} for r in mem_entries] if mem_entries else []

        data["agent_knowledge"] = {
            "count": mem_count,
            "memories": memories,
            "signal": (
                f"agents have built institutional context across sessions"
                if mem_count > 0
                else "no persistent memories stored yet"
            ),
        }

        # -- Recent sessions --
        session_rows = conn.execute(
            "SELECT session_id, started_at, ended_at, duration_secs, "
            "  bead_count, beads_closed, beads_open, epics_touched, "
            "  parallelism_ratio, avg_cycle_time_secs, median_cycle_time_secs "
            "FROM mart_session_summary "
            "ORDER BY started_at DESC LIMIT 5"
        ).fetchall()

        # Project baseline: median of session median cycle times
        baseline_rows = conn.execute(
            "SELECT median_cycle_time_secs FROM mart_session_summary "
            "WHERE beads_closed > 0 AND median_cycle_time_secs IS NOT NULL"
        ).fetchall()
        baseline_vals = sorted([r[0] for r in baseline_rows])
        baseline_cycle = (
            baseline_vals[len(baseline_vals) // 2] if baseline_vals else None
        )

        # Session compliance
        compliance_rows = conn.execute(
            "SELECT session_id, beads_in_session, skip_claim_count, "
            "  documented_closes, dep_violations "
            "FROM v_session_compliance"
        ).fetchall()
        compliance_map = {
            r[0]: {
                "beads_in_session": r[1],
                "skip_claim_count": r[2],
                "documented_closes": r[3],
                "dep_violations": r[4],
            }
            for r in compliance_rows
        }

        recent_sessions = []
        for r in session_rows:
            sid = r[0]
            sess = {
                "session_id": sid,
                "started_at": str(r[1]) if r[1] else None,
                "ended_at": str(r[2]) if r[2] else None,
                "duration_secs": int(r[3]) if r[3] is not None else 0,
                "bead_count": int(r[4]) if r[4] is not None else 0,
                "beads_closed": int(r[5]) if r[5] is not None else 0,
                "beads_open": int(r[6]) if r[6] is not None else 0,
                "epics_touched": int(r[7]) if r[7] is not None else 0,
                "parallelism_ratio": float(r[8]) if r[8] is not None else None,
                "avg_cycle_time_secs": int(r[9]) if r[9] is not None else None,
                "median_cycle_time_secs": int(r[10]) if r[10] is not None else None,
            }
            # Cost multiple
            if ct_p50 and sess["avg_cycle_time_secs"] and ct_p50 > 0:
                sess["cost_multiple"] = round(sess["avg_cycle_time_secs"] / ct_p50, 1)
            else:
                sess["cost_multiple"] = None

            comp = compliance_map.get(sid)
            sess["compliance"] = comp
            if comp:
                # Compliance verdict per session
                c_beads = comp.get("beads_in_session", 0)
                c_skip = comp.get("skip_claim_count", 0)
                c_doc = comp.get("documented_closes", 0)
                c_viol = comp.get("dep_violations", 0)
                if c_viol > 0 or (c_beads > 0 and c_skip / c_beads > 0.5):
                    sess["compliance_verdict"] = "concern"
                elif c_skip > 0 or (c_doc == 0 and sess["beads_closed"] > 0):
                    sess["compliance_verdict"] = "watch"
                else:
                    sess["compliance_verdict"] = "good"
            else:
                sess["compliance_verdict"] = "good"

            sess["assessment"] = _session_assessment(sess, baseline_cycle)
            sess["verdict"] = _session_verdict(sess, comp, baseline_cycle)

            recent_sessions.append(sess)

        data["recent_sessions"] = recent_sessions

        # -- Actor classification --
        sources_rows = conn.execute(
            "SELECT DISTINCT classification_source FROM dim_actor"
        ).fetchall()
        sources = {r[0] for r in sources_rows if r[0]}
        data["actor_classification_note"] = _actor_classification_note(sources)

    finally:
        conn.close()

    return data


# ============================================================
# Human-readable output
# ============================================================

_WORKFLOW_LABELS = {
    "epic": "epic-driven (all beads belong to an epic)",
    "flat": "flat (beads are tracked individually without epics)",
    "mixed": "mixed (some beads belong to epics, others don't)",
    "empty": "no beads yet",
}


def format_human(data: dict) -> str:
    """Format prime output as human-readable text per Thread v2 spec section 4."""
    lines = []
    lines.append("Thread — project health")
    lines.append("=" * 60)
    lines.append("")

    icon = _VERDICT_ICON

    # Headline
    total = data.get("total_beads", 0)
    closed = data.get("closed_count", 0)
    open_c = data.get("open_count", 0)
    rate = data.get("completion_rate")
    pct = f"{int(round(rate * 100))}%" if rate is not None else "—"
    v = data.get("completion_verdict", "good")
    queue_str = f" · {open_c} queued" if open_c else ""
    lines.append(f"  {icon[v]} {closed} of {total} beads completed ({pct}){queue_str}")

    # Cycle time
    ct_p50 = data.get("cycle_time_p50_secs")
    ct_p90 = data.get("cycle_time_p90_secs")
    v = data.get("cycle_time_verdict", "good")
    if ct_p50 is not None:
        lines.append(
            f"  {icon[v]} Median cycle time: {_fmt_duration(ct_p50)} "
            f"(p90: {_fmt_duration(ct_p90)})"
        )
    else:
        lines.append(f"  {icon[v]} Cycle time: no completed beads to measure")

    # Throughput
    bpd = data.get("throughput_beads_per_day")
    ad = data.get("throughput_active_days", 0)
    v = data.get("throughput_verdict", "good")
    if bpd is not None:
        lines.append(f"  {icon[v]} Throughput: {bpd} beads/day across {ad} active days")
    else:
        lines.append(f"  {icon[v]} Throughput: not enough data")

    lines.append("")

    # Cost spread
    cost_mult = data.get("cost_p90_multiple")
    v = data.get("cost_verdict", "good")
    compute = data.get("total_compute_time_human", "—")
    if cost_mult is not None:
        lines.append(f"  {icon[v]} Cost spread: p90 is {cost_mult}x median · {compute} total compute")
        lines.append(f"      {data.get('cost_distribution_signal', '')}")
    else:
        lines.append(f"  {icon[v]} Cost spread: not enough data")
    lines.append("")

    # Recent sessions
    sessions = data.get("recent_sessions", [])
    if sessions:
        lines.append("  Last sessions:")
        for s in sessions:
            sid_v = icon.get(s.get("verdict", "good"), "[+]")
            started = s.get("started_at", "")[:16] if s.get("started_at") else "?"
            bc = s.get("bead_count", 0)
            bclose = s.get("beads_closed", 0)
            dur = _fmt_duration(s.get("duration_secs"))
            cost = s.get("cost_multiple")
            cost_str = f"cost: {cost}x" if cost is not None else "cost: —"
            comp_v = s.get("compliance_verdict", "good")
            lines.append(
                f"    {sid_v} {started}  {bclose}/{bc} in {dur}  "
                f"{cost_str}  compliance: {comp_v}"
            )
            lines.append(f"        {s.get('assessment', '')}")
        lines.append("")

    # Agent closure rate
    acr = data.get("agent_closure_rate")
    v = data.get("agent_closure_verdict", "good")
    if acr is not None:
        lines.append(f"  {icon[v]} Agent closure rate: {int(round(acr * 100))}%")
        lines.append(f"      {data.get('agent_closure_signal', '')}")
        lines.append("")

    # Parallelism
    par = data.get("parallelism_ratio")
    v = data.get("parallelism_verdict", "good")
    if par is not None:
        lines.append(f"  {icon[v]} Parallelism: {par}x")
        lines.append(f"      {data.get('parallelism_signal', '')}")
        lines.append("")

    # Scope stability
    scope = data.get("scope_stability_rate")
    v = data.get("scope_stability_verdict", "good")
    lines.append(f"  {icon[v]} Scope stability: {scope} changes/bead")
    lines.append(f"      {data.get('scope_stability_signal', '')}")
    lines.append("")

    # Dependency churn
    dep = data.get("dep_activity_rate")
    if dep is not None:
        lines.append(f"  Dependency churn: {dep} events/bead")
        lines.append(f"      {data.get('dep_activity_signal', '')}")
        lines.append("")

    # Compliance
    lines.append("  Compliance:")
    for label, key_val, key_sig, key_v in [
        ("Skip-claim", "skip_claim_signal", "skip_claim_signal", "skip_claim_verdict"),
        ("Documentation", "documentation_signal", "documentation_signal", "documentation_verdict"),
        ("Dependency order", "dep_order_signal", "dep_order_signal", "dep_order_verdict"),
    ]:
        v = data.get(key_v, "good")
        sig = data.get(key_sig, "")
        lines.append(f"    {icon[v]} {label}: {sig}")
    lines.append("")

    # Queue wait
    qw_p50 = data.get("queue_wait_p50_secs")
    qw_p90 = data.get("queue_wait_p90_secs")
    v = data.get("queue_wait_verdict", "good")
    if qw_p50 is not None:
        lines.append(
            f"  {icon[v]} Queue wait: median {_fmt_duration(qw_p50)} "
            f"(p90: {_fmt_duration(qw_p90)})"
        )
        lines.append(f"      {data.get('queue_wait_signal', '')}")
        lines.append("")

    # By type
    type_perf = data.get("type_performance", [])
    if type_perf:
        parts = [
            f"{t['type']}s {_fmt_duration(t.get('median_cycle_secs'))} ({t['count']})"
            for t in type_perf
            if t.get("median_cycle_secs") is not None
        ]
        if parts:
            lines.append(f"  By type: {', '.join(parts)}")
            lines.append("")

    # By priority
    prio_perf = data.get("priority_performance", [])
    if prio_perf:
        parts = []
        for p in prio_perf:
            mc = p.get("median_cycle_secs")
            if mc is not None:
                parts.append(f"P{p['priority']} {_fmt_duration(mc)} ({p['count']} beads)")
            elif p.get("completed", 0) == 0 and p.get("count", 0) > 0:
                parts.append(f"P{p['priority']} all open ({p['count']})")
        if parts:
            lines.append(f"  Priority split: {', '.join(parts)}")
            lines.append("")

    # Trend
    trend = data.get("trend", {})
    if trend.get("direction") != "insufficient":
        lines.append(f"  Trend: {trend.get('signal', '')}")
        lines.append("")

    # Interactions
    interactions = data.get("interactions", {})
    total_int = interactions.get("total", 0)
    if total_int > 0:
        by_kind = interactions.get("by_kind", {})
        kind_str = ", ".join(f"{v} {k}" for k, v in sorted(by_kind.items(), key=lambda x: -x[1]))
        lines.append(f"  Audit trail: {total_int} interactions ({kind_str})")
    else:
        lines.append(f"  Audit trail: {interactions.get('message', 'no data')}")
    lines.append(f"      {interactions.get('signal', '')}")
    lines.append("")

    # Agent knowledge
    ak = data.get("agent_knowledge", {})
    ak_count = ak.get("count", 0)
    if ak_count > 0:
        lines.append(f"  Agent knowledge: {ak_count} persistent memories stored")
        lines.append(f"      {ak.get('signal', '')}")
        lines.append("")

    # Open work summary
    open_c = data.get("open_count", 0)
    unclaimed = data.get("unclaimed_open_count", 0)
    if open_c > 0:
        unclaimed_str = f" — {unclaimed} not yet claimed" if unclaimed else ""
        lines.append(f"  {open_c} open beads{unclaimed_str}")
        lines.append("")

    # Actor classification note
    note = data.get("actor_classification_note")
    if note:
        lines.append(f"  Note: {note}")
        lines.append("")

    return "\n".join(lines)


def format_json(data: dict) -> str:
    return json.dumps(data, indent=2, default=str)

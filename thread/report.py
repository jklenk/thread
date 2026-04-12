"""Thread report — self-contained HTML report generator (v2).

Single HTML file with inline Chart.js from CDN. Six verdict-colored headline
cards, session timeline, compliance scorecard, audit trail, interactive cycle
time histogram with drill-down, throughput bar chart, insights section, agent
knowledge base, and cost footer.

All cost is expressed as multiples of the project's median cycle time — never
dollars. Verdict borders: green (good), amber (watch), red (concern).
"""

import json
from datetime import datetime
from pathlib import Path

import duckdb

from thread.prime import compute_prime, _fmt_duration


# ============================================================
# Verdict → color mapping
# ============================================================

_VERDICT_BORDER = {
    "good": "#22c55e",     # green
    "watch": "#eab308",    # amber
    "concern": "#ef4444",  # red
}

_VERDICT_DOT = {
    "good": "&#x1F7E2;",   # green circle
    "watch": "&#x1F7E1;",  # yellow circle
    "concern": "&#x1F534;", # red circle
}


def _esc(val):
    """HTML-escape a value."""
    if val is None:
        return "&mdash;"
    return str(val).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _pct(val, digits=0):
    if val is None:
        return "&mdash;"
    return f"{val * 100:.{digits}f}%"


def _cost_mult(val):
    if val is None:
        return "&mdash;"
    return f"{val:.1f}x"


# ============================================================
# HTML template pieces
# ============================================================

def _headline_card(label, value, sublabel, verdict):
    color = _VERDICT_BORDER.get(verdict, "#999")
    return (
        f'<div class="card" style="border-top:4px solid {color}">'
        f'<div class="card-value">{value}</div>'
        f'<div class="card-label">{_esc(label)}</div>'
        f'<div class="card-sub">{sublabel}</div>'
        f'</div>'
    )


def _section(title, html, section_id=""):
    id_attr = f' id="{section_id}"' if section_id else ""
    return f'<section{id_attr}><h2>{_esc(title)}</h2>\n{html}\n</section>'


# ============================================================
# generate_report — the main entry point
# ============================================================

def generate_report(db_path: str, output_path: str) -> str:
    """Generate thread-report.html at output_path. Returns the path."""

    # Get all prime metrics
    data = compute_prime(db_path)

    # Query DuckDB for detail data needed by the report
    conn = duckdb.connect(db_path, read_only=True)
    try:
        detail = _query_report_details(conn, data)
    finally:
        conn.close()

    html = _render_html(data, detail)
    out = Path(output_path)
    out.write_text(html)
    return str(out)


def _query_report_details(conn, data):
    """Query DuckDB for detail-level data the report needs beyond prime."""
    d = {}

    # Bead-level data for cycle time histogram drill-down
    d["beads"] = conn.execute(
        "SELECT b.issue_id, b.title, b.issue_type, "
        "  f.active_time_secs, f.total_elapsed_secs "
        "FROM dim_bead b "
        "JOIN fact_bead_lifecycle f ON f.issue_id = b.issue_id "
        "WHERE f.final_closed_at IS NOT NULL AND f.active_time_secs IS NOT NULL "
        "ORDER BY f.active_time_secs DESC"
    ).fetchall()

    # All sessions (not just recent 5)
    d["sessions"] = conn.execute(
        "SELECT m.session_id, m.started_at, m.ended_at, m.duration_secs, "
        "  m.bead_count, m.beads_closed, m.beads_open, "
        "  m.parallelism_ratio, m.avg_cycle_time_secs, m.median_cycle_time_secs, "
        "  c.skip_claim_count, c.documented_closes, c.dep_violations "
        "FROM mart_session_summary m "
        "LEFT JOIN v_session_compliance c ON c.session_id = m.session_id "
        "ORDER BY m.started_at DESC"
    ).fetchall()

    # Daily throughput for bar chart
    d["daily"] = conn.execute(
        "SELECT day, beads_closed, median_cycle_time_secs "
        "FROM v_daily_trends ORDER BY day"
    ).fetchall()

    # Interactions detail
    d["interaction_summary"] = conn.execute(
        "SELECT kind, count, beads_touched FROM v_interaction_summary ORDER BY count DESC"
    ).fetchall()

    d["hourly"] = conn.execute(
        "SELECT day_of_week, hour_of_day, interactions FROM v_interaction_hourly"
    ).fetchall()

    d["daily_activity"] = conn.execute(
        "SELECT day, first_activity, last_activity, span_secs, "
        "  interactions, beads_touched "
        "FROM v_daily_activity ORDER BY day"
    ).fetchall()

    d["transitions"] = conn.execute(
        "SELECT from_status, to_status, count, beads FROM v_status_transitions"
    ).fetchall()

    d["close_velocity"] = conn.execute(
        "SELECT gap_secs FROM v_close_velocity WHERE gap_secs IS NOT NULL ORDER BY gap_secs"
    ).fetchall()

    d["close_reasons"] = conn.execute(
        "SELECT issue_id, bead_title, close_reason, created_at FROM v_close_reasons"
    ).fetchall()

    d["model_usage"] = conn.execute(
        "SELECT model, calls, avg_prompt_chars, avg_response_chars, errors, beads_touched "
        "FROM v_model_usage ORDER BY calls DESC"
    ).fetchall()

    d["tool_usage"] = conn.execute(
        "SELECT tool_name, calls, successes, failures, success_rate "
        "FROM v_tool_usage ORDER BY calls DESC"
    ).fetchall()

    # Compliance detail tables
    d["title_reasons"] = conn.execute(
        "SELECT issue_id, title, close_reason, created_at FROM v_title_reason_pairs"
    ).fetchall()

    d["dep_violations"] = conn.execute(
        "SELECT blocked_bead, blocked_title, blocker_bead, blocker_title, "
        "  blocked_closed_at, blocker_closed_at "
        "FROM v_dep_order_violations"
    ).fetchall()

    # Insights
    d["priority_perf"] = conn.execute(
        "SELECT priority, bead_count, completed, median_cycle_secs, avg_wait_secs "
        "FROM v_priority_performance WHERE priority IS NOT NULL ORDER BY priority"
    ).fetchall()

    d["type_perf"] = conn.execute(
        "SELECT issue_type, bead_count, completed, median_cycle_secs, p90_cycle_secs "
        "FROM v_type_performance WHERE issue_type IS NOT NULL ORDER BY median_cycle_secs"
    ).fetchall()

    d["spec_quality"] = conn.execute(
        "SELECT spec_level, bead_count, completed, median_cycle_secs "
        "FROM v_spec_quality_correlation ORDER BY median_cycle_secs"
    ).fetchall()

    # Epic table — mart_epic_summary doesn't have median_cycle or scope changes,
    # so we compute them with a subquery joining the raw facts
    d["epics"] = conn.execute(
        "SELECT e.epic_title, e.bead_count, "
        "  ROUND(e.avg_fidelity_score, 2), "
        "  sub.median_cycle_secs, "
        "  sub.scope_changes "
        "FROM mart_epic_summary e "
        "LEFT JOIN ("
        "  SELECT h.root_id, "
        "    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.active_time_secs), 0) AS median_cycle_secs, "
        "    COALESCE(SUM(da.post_claim_dep_events), 0) AS scope_changes "
        "  FROM dim_hierarchy h "
        "  JOIN fact_bead_lifecycle f ON f.issue_id = h.issue_id "
        "  LEFT JOIN v_bead_dep_activity da ON da.issue_id = h.issue_id "
        "  WHERE f.final_closed_at IS NOT NULL "
        "  GROUP BY h.root_id"
        ") sub ON sub.root_id = e.epic_id "
        "WHERE (e.epic_is_template = false OR e.epic_is_template IS NULL) "
        "  AND e.bead_count > 1 "
        "ORDER BY e.bead_count DESC LIMIT 20"
    ).fetchall()

    # Agent memories
    d["memories"] = conn.execute(
        "SELECT memory_key, memory_value FROM dim_agent_memory ORDER BY memory_key"
    ).fetchall()

    # Date range
    d["date_range"] = conn.execute(
        "SELECT MIN(created_at), MAX(created_at) FROM fact_bead_lifecycle"
    ).fetchone()

    # Project name from bead IDs
    d["project_name"] = conn.execute(
        "SELECT issue_id FROM dim_bead LIMIT 1"
    ).fetchone()

    return d


def _render_html(data, detail):
    """Render the full HTML report."""
    parts = []

    # Derive project name from the most common bead ID prefix.
    # Bead IDs look like "data-eng-summary-izl" — the last segment is the
    # unique hash, everything before it is the project prefix. Only use the
    # prefix if it has multiple segments (e.g., "data-eng-summary"), otherwise
    # it's just the repo directory name and "Thread" is a better default.
    project_name = "Thread"
    if detail["project_name"] and detail["project_name"][0]:
        bid = detail["project_name"][0]
        prefix = bid.rsplit("-", 1)[0] if "-" in bid else bid
        # Only use prefix if it contains a hyphen itself (multi-segment),
        # indicating a real project name like "data-eng-summary"
        if "-" in prefix:
            project_name = prefix

    # Date range
    dr = detail["date_range"]
    date_range_str = ""
    if dr and dr[0] and dr[1]:
        d0 = str(dr[0])[:10]
        d1 = str(dr[1])[:10]
        date_range_str = f"{d0} to {d1}"

    workflow_type = data.get("workflow_type", "unknown")

    parts.append(_render_head(project_name))
    parts.append(f'<body>\n<div class="container">')
    parts.append(
        f'<header>'
        f'<h1>{_esc(project_name)} <span class="tag">{workflow_type}</span></h1>'
        f'<p class="subtitle">{date_range_str} &middot; '
        f'{data.get("total_beads", 0)} beads &middot; '
        f'{data.get("epic_count", 0)} epics</p>'
        f'</header>'
    )

    # --- 6 headline cards ---
    parts.append(_render_headlines(data))

    # --- Session timeline ---
    parts.append(_render_sessions(data, detail))

    # --- Compliance scorecard ---
    parts.append(_render_compliance(data, detail))

    # --- Audit trail ---
    parts.append(_render_audit_trail(data, detail))

    # --- Charts (cycle time histogram + throughput) ---
    parts.append(_render_charts(data, detail))

    # --- Insights ---
    parts.append(_render_insights(data, detail))

    # --- Epic table ---
    if detail["epics"]:
        parts.append(_render_epics(data, detail))

    # --- Agent knowledge ---
    parts.append(_render_agent_knowledge(detail))

    # --- Footer ---
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    parts.append(
        f'<footer>'
        f'<p>Generated by Thread &middot; {now}</p>'
        f'<p>Cost expressed as multiples of project median cycle time (1.0x = baseline)</p>'
        f'</footer>'
    )

    parts.append('</div>')

    # --- JavaScript ---
    parts.append(_render_scripts(data, detail))

    parts.append('</body>\n</html>')
    return "\n".join(parts)


def _render_head(project_name):
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Thread report &mdash; {_esc(project_name)}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
:root {{
  --green: #22c55e; --amber: #eab308; --red: #ef4444;
  --bg: #fafafa; --card-bg: #fff; --border: #e5e7eb;
  --text: #1f2937; --muted: #6b7280; --font: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
}}
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family: var(--font); background: var(--bg); color: var(--text); line-height:1.6; }}
.container {{ max-width:1200px; margin:0 auto; padding:1.5rem; }}
header {{ margin-bottom:1.5rem; }}
h1 {{ font-size:1.75rem; border-bottom:2px solid #333; padding-bottom:0.3em; }}
h2 {{ font-size:1.25rem; margin-bottom:0.75rem; color:#333; }}
.tag {{ display:inline-block; padding:0.15rem 0.5rem; background:#eef; border-radius:4px; font-size:0.8rem; color:#335; vertical-align:middle; }}
.subtitle {{ color:var(--muted); font-size:0.9rem; margin-top:0.25rem; }}
section {{ margin-bottom:2rem; }}
.cards {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(160px,1fr)); gap:1rem; margin-bottom:2rem; }}
.card {{ background:var(--card-bg); border-radius:8px; padding:1.25rem 1rem; text-align:center; box-shadow:0 1px 3px rgba(0,0,0,0.08); }}
.card-value {{ font-size:2rem; font-weight:700; line-height:1.2; }}
.card-label {{ font-size:0.8rem; text-transform:uppercase; color:var(--muted); letter-spacing:0.03em; margin-top:0.25rem; }}
.card-sub {{ font-size:0.75rem; color:var(--muted); margin-top:0.15rem; }}
table {{ width:100%; border-collapse:collapse; font-size:0.85rem; }}
th, td {{ padding:0.5rem 0.6rem; text-align:left; border-bottom:1px solid var(--border); }}
th {{ background:#f8f8f8; font-weight:600; font-size:0.78rem; text-transform:uppercase; color:var(--muted); letter-spacing:0.03em; }}
tr.session-row {{ cursor:default; }}
.compliance-dot {{ font-size:0.85rem; cursor:help; }}
.chart-wrap {{ margin:1rem 0; position:relative; }}
canvas {{ max-width:100%; }}
#drilldown {{ display:none; margin-top:1rem; background:var(--card-bg); border-radius:8px; padding:1rem; box-shadow:0 1px 3px rgba(0,0,0,0.08); }}
#drilldown h3 {{ font-size:1rem; margin-bottom:0.5rem; }}
.scorecard {{ display:grid; grid-template-columns:1fr; gap:0.5rem; margin-bottom:1.5rem; }}
.sc-row {{ display:grid; grid-template-columns:2rem 1fr 5rem auto; align-items:center; gap:0.75rem; padding:0.5rem 0.75rem; background:var(--card-bg); border-radius:6px; box-shadow:0 1px 2px rgba(0,0,0,0.05); }}
.sc-icon {{ font-size:1rem; text-align:center; }}
.sc-label {{ font-weight:600; font-size:0.85rem; }}
.sc-label small {{ font-weight:400; color:var(--muted); display:block; font-size:0.78rem; }}
.sc-value {{ font-weight:600; text-align:right; font-size:0.85rem; }}
.heatmap-wrap {{ overflow-x:auto; margin:1rem 0; }}
.heatmap {{ display:grid; grid-template-columns: 50px repeat(24, 1fr); gap:2px; font-size:0.7rem; }}
.heatmap-cell {{ aspect-ratio:1; display:flex; align-items:center; justify-content:center; border-radius:3px; color:#fff; font-weight:600; min-width:18px; }}
.heatmap-label {{ display:flex; align-items:center; font-size:0.7rem; color:var(--muted); }}
details {{ margin:1rem 0; }}
details summary {{ cursor:pointer; font-weight:600; color:#555; }}
details summary:hover {{ color:#222; }}
.stat-cards {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(140px,1fr)); gap:0.75rem; margin:1rem 0; }}
.stat-card {{ background:var(--card-bg); border-radius:6px; padding:0.75rem; text-align:center; box-shadow:0 1px 2px rgba(0,0,0,0.06); }}
.stat-card .val {{ font-size:1.5rem; font-weight:700; }}
.stat-card .lbl {{ font-size:0.72rem; text-transform:uppercase; color:var(--muted); }}
footer {{ color:var(--muted); font-size:0.8rem; margin-top:2rem; padding-top:1rem; border-top:1px solid var(--border); }}
footer p {{ margin:0.15rem 0; }}
</style>
</head>"""


def _render_headlines(data):
    """Render 6 verdict-colored headline cards."""
    median = data.get("median_cycle_secs")

    cards = [
        _headline_card(
            "Completion",
            _pct(data.get("completion_rate")),
            f'{data.get("closed_count", 0)} of {data.get("total_beads", 0)} beads',
            data.get("completion_verdict", "watch"),
        ),
        _headline_card(
            "Cycle time (p50)",
            _fmt_duration(data.get("cycle_time_p50_secs")),
            f'p90: {_fmt_duration(data.get("cycle_time_p90_secs"))}',
            data.get("cycle_time_verdict", "watch"),
        ),
        _headline_card(
            "Beads / day",
            str(data.get("throughput_beads_per_day") or "&mdash;"),
            f'across {data.get("throughput_active_days", 0)} active days',
            data.get("throughput_verdict", "watch"),
        ),
        _headline_card(
            "Cost spread",
            _cost_mult(data.get("cost_p90_multiple")),
            f'{_fmt_duration(data.get("total_compute_time_secs"))} total compute',
            data.get("cost_verdict", "watch"),
        ),
        _headline_card(
            "Parallelism",
            _cost_mult(data.get("parallelism_ratio")),
            data.get("parallelism_signal", ""),
            data.get("parallelism_verdict", "watch"),
        ),
        _headline_card(
            "Queue wait (p50)",
            _fmt_duration(data.get("queue_wait_p50_secs")),
            f'p90: {_fmt_duration(data.get("queue_wait_p90_secs"))}',
            data.get("queue_wait_verdict", "watch"),
        ),
    ]

    # Trend arrow on cycle time card
    trend = data.get("trend", {})
    direction = trend.get("direction", "")
    if direction == "improving":
        arrow = f' <span style="color:var(--green)" title="{_esc(trend.get("signal",""))}">&#x25BC; {abs(trend.get("cycle_time_change_pct", 0)):.0f}%</span>'
        cards[1] = cards[1].replace("</div>\n<div", f"{arrow}</div>\n<div", 1)
    elif direction == "regressing":
        arrow = f' <span style="color:var(--red)" title="{_esc(trend.get("signal",""))}">&#x25B2; {abs(trend.get("cycle_time_change_pct", 0)):.0f}%</span>'
        cards[1] = cards[1].replace("</div>\n<div", f"{arrow}</div>\n<div", 1)

    return f'<div class="cards">{"".join(cards)}</div>'


def _render_sessions(data, detail):
    """Session timeline table with verdict left borders."""
    sessions = detail["sessions"]
    if not sessions:
        return _section("Sessions", "<p>No sessions detected.</p>")

    median_cycle = data.get("median_cycle_secs")

    rows = []
    for s in sessions:
        sid, started, ended, dur, total, closed, opened, par, avg_ct, med_ct, \
            skip_claim, doc_closes, dep_viols = s

        # Session verdict (simple: all closed + no violations = good)
        if opened == 0 and (dep_viols or 0) == 0 and (skip_claim or 0) == 0:
            sv = "good"
        elif (dep_viols or 0) > 0 or (skip_claim or 0) > total * 0.5:
            sv = "concern"
        else:
            sv = "watch"

        border_color = _VERDICT_BORDER.get(sv, "#999")

        # Cost multiple
        cost = None
        if avg_ct and median_cycle and median_cycle > 0:
            cost = avg_ct / median_cycle

        # Compliance dot
        comp_verdict = "good"
        if (dep_viols or 0) > 0 or (skip_claim or 0) > (total or 1) * 0.5:
            comp_verdict = "concern"
        elif (skip_claim or 0) > 0 or (doc_closes or 0) < (closed or 1) * 0.5:
            comp_verdict = "watch"

        comp_detail = (
            f"{skip_claim or 0} skip-claims, "
            f"{doc_closes or 0}/{closed or 0} documented, "
            f"{dep_viols or 0} dep violations"
        )

        # Assessment
        assessment = _session_assessment_html(
            closed, total, avg_ct, median_cycle, opened
        )

        when = str(started)[:16] if started else "&mdash;"

        rows.append(
            f'<tr class="session-row" style="border-left:4px solid {border_color}">'
            f'<td>{when}</td>'
            f'<td>{_fmt_duration(dur)}</td>'
            f'<td>{total}</td>'
            f'<td>{closed}/{total}</td>'
            f'<td>{_fmt_duration(avg_ct)}</td>'
            f'<td>{f"{par:.1f}x" if par else "&mdash;"}</td>'
            f'<td>{_cost_mult(cost)}</td>'
            f'<td><span class="compliance-dot" title="{comp_detail}">'
            f'{_VERDICT_DOT.get(comp_verdict, "")}</span></td>'
            f'<td>{assessment}</td>'
            f'</tr>'
        )

    table = (
        '<table><thead><tr>'
        '<th>When</th><th>Duration</th><th>Beads</th><th>Completed</th>'
        '<th>Avg cycle</th><th>Parallelism</th><th>Cost</th>'
        '<th>Comp.</th><th>Assessment</th>'
        '</tr></thead><tbody>'
        + "\n".join(rows)
        + '</tbody></table>'
    )
    return _section("Sessions", table, "sessions")


def _session_assessment_html(closed, total, avg_ct, median_cycle, opened):
    """One-line plain-language session assessment."""
    if not closed or closed == 0:
        if total and total > 0:
            return "planning only &mdash; beads created but not started"
        return "&mdash;"

    parts = []
    if median_cycle and avg_ct and median_cycle > 0:
        ratio = avg_ct / median_cycle
        if ratio > 2:
            parts.append(f"{ratio:.0f}x slower than baseline")
        elif ratio < 0.5:
            parts.append("faster than usual")
        else:
            parts.append("normal pace")
    else:
        parts.append(f"{closed} completed")

    if opened and opened > 0:
        parts.append(f"{opened} unfinished")
    else:
        parts.append("all work completed")

    return " &mdash; ".join(parts)


def _render_compliance(data, detail):
    """Compliance scorecard + detail tables."""
    parts = []

    # 7-check scorecard
    checks = [
        (
            "Claim compliance",
            f'{_pct(1.0 - (data.get("skip_claim_rate") or 0))} claimed before close',
            _pct(1.0 - (data.get("skip_claim_rate") or 0)),
            data.get("skip_claim_verdict", "watch"),
            data.get("skip_claim_signal", ""),
        ),
        (
            "Close documentation",
            f'{_pct(data.get("documentation_rate"))} with descriptive reason',
            _pct(data.get("documentation_rate")),
            data.get("documentation_verdict", "watch"),
            data.get("documentation_signal", ""),
        ),
        (
            "Dependency order",
            f'{data.get("dep_order_violations", 0)} violations',
            str(data.get("dep_order_violations", 0)),
            data.get("dep_order_verdict", "watch"),
            data.get("dep_order_signal", ""),
        ),
        (
            "Scope stability",
            f'{data.get("scope_stability_rate", 0):.2f} changes/bead',
            f'{data.get("scope_stability_rate", 0):.2f}',
            data.get("scope_stability_verdict", "watch"),
            data.get("scope_stability_signal", ""),
        ),
        (
            "Late-add beads",
            f'{data.get("late_add_bead_count", 0)} reactive beads',
            str(data.get("late_add_bead_count", 0)),
            data.get("late_add_bead_verdict", "good"),
            data.get("late_add_bead_signal", ""),
        ),
        (
            "Late-add blockers",
            f'{data.get("late_add_blocker_count", 0)} surprise blockers',
            str(data.get("late_add_blocker_count", 0)),
            data.get("late_add_blocker_verdict", "good"),
            data.get("late_add_blocker_signal", ""),
        ),
        (
            "Title/reason alignment",
            f'{data.get("title_reason_mismatch_count", 0)} potential mismatches',
            str(data.get("title_reason_mismatch_count", 0)),
            data.get("title_reason_mismatch_verdict", "good"),
            data.get("title_reason_mismatch_signal", ""),
        ),
    ]

    sc_rows = []
    for name, desc, value, verdict, signal in checks:
        dot = _VERDICT_DOT.get(verdict, "")
        sc_rows.append(
            f'<div class="sc-row">'
            f'<div class="sc-icon">{dot}</div>'
            f'<div class="sc-label">{_esc(name)}<small>{_esc(signal)}</small></div>'
            f'<div class="sc-value">{value}</div>'
            f'</div>'
        )

    parts.append('<div class="scorecard">' + "".join(sc_rows) + '</div>')

    # Title/reason mismatch table (low word-overlap pairs)
    mismatches = data.get("title_reason_mismatches", [])
    if mismatches:
        mm_rows = []
        for m in mismatches:
            score_pct = f"{int(m['overlap_score'] * 100)}%"
            mm_rows.append(
                f'<tr><td>{_esc(m["issue_id"])}</td><td>{_esc(m["title"])}</td>'
                f'<td>{_esc(m["close_reason"])}</td>'
                f'<td style="color:var(--red)">{score_pct}</td></tr>'
            )
        parts.append(
            '<details open><summary>Title / reason alignment issues</summary>'
            '<p style="color:var(--muted);font-size:0.82rem;margin-bottom:0.5rem">'
            'Low word-overlap between bead title and close reason — agent may have done different work than scoped.</p>'
            '<table><thead><tr><th>Bead</th><th>Title</th><th>Close reason</th><th>Overlap</th></tr></thead>'
            '<tbody>' + "\n".join(mm_rows) + '</tbody></table></details>'
        )

    # Title-reason pairs (all documented closes, for human review)
    if detail["title_reasons"]:
        tr_rows = []
        for r in detail["title_reasons"]:
            tr_rows.append(
                f'<tr><td>{_esc(r[0])}</td><td>{_esc(r[1])}</td>'
                f'<td>{_esc(r[2])}</td><td>{str(r[3])[:16] if r[3] else "&mdash;"}</td></tr>'
            )
        parts.append(
            '<details><summary>All title vs close reason pairs</summary>'
            '<table><thead><tr><th>Bead ID</th><th>Title</th><th>Close reason</th><th>Closed</th></tr></thead>'
            '<tbody>' + "\n".join(tr_rows) + '</tbody></table></details>'
        )

    # Late-add beads table
    late_beads = data.get("late_add_beads", [])
    if late_beads:
        lb_rows = []
        for b in late_beads:
            lb_rows.append(
                f'<tr><td>{_esc(b["issue_id"])}</td><td>{_esc(b["title"])}</td>'
                f'<td>{_fmt_duration(b["time_to_start_secs"])}</td>'
                f'<td>{b["prior_closes_in_session"]}</td>'
                f'<td>{_esc(b["session_id"])}</td></tr>'
            )
        parts.append(
            '<details open><summary>Late-add beads (created mid-session)</summary>'
            '<p style="color:var(--muted);font-size:0.82rem;margin-bottom:0.5rem">'
            'Created and claimed within 5s while work was already underway — unplanned reactive additions.</p>'
            '<table><thead><tr><th>Bead</th><th>Title</th><th>Claim lag</th>'
            '<th>Prior closes</th><th>Session</th></tr></thead>'
            '<tbody>' + "\n".join(lb_rows) + '</tbody></table></details>'
        )

    # Late-add blockers table
    late_blockers = data.get("late_add_blockers", [])
    if late_blockers:
        lab_rows = []
        for b in late_blockers:
            lab_rows.append(
                f'<tr><td>{_esc(b["blocked_bead"])}</td><td>{_esc(b["blocked_title"])}</td>'
                f'<td>{_esc(b["blocker_bead"])}</td><td>{_esc(b["blocker_title"])}</td>'
                f'<td>{str(b["added_at"])[:16] if b["added_at"] else "&mdash;"}</td></tr>'
            )
        parts.append(
            '<details open><summary>Late-add blockers (added after claim)</summary>'
            '<p style="color:var(--muted);font-size:0.82rem;margin-bottom:0.5rem">'
            'Blocking dependency added after the dependent bead was already claimed — unexpected constraint.</p>'
            '<table><thead><tr><th>Blocked bead</th><th>Title</th><th>Blocker</th>'
            '<th>Blocker title</th><th>Added at</th></tr></thead>'
            '<tbody>' + "\n".join(lab_rows) + '</tbody></table></details>'
        )

    # Dependency violations table
    if detail["dep_violations"]:
        dv_rows = []
        for r in detail["dep_violations"]:
            blocker_closed = str(r[5])[:16] if r[5] else "still open"
            dv_rows.append(
                f'<tr><td>{_esc(r[0])}</td><td>{_esc(r[1])}</td>'
                f'<td>{_esc(r[2])}</td><td>{_esc(r[3])}</td>'
                f'<td>{str(r[4])[:16] if r[4] else "&mdash;"}</td>'
                f'<td>{_esc(blocker_closed)}</td></tr>'
            )
        parts.append(
            '<details open><summary>Dependency order violations</summary>'
            '<table><thead><tr><th>Blocked</th><th>Title</th><th>Blocker</th>'
            '<th>Blocker title</th><th>Blocked closed</th><th>Blocker closed</th></tr></thead>'
            '<tbody>' + "\n".join(dv_rows) + '</tbody></table></details>'
        )

    return _section("Compliance", "\n".join(parts), "compliance")


def _render_audit_trail(data, detail):
    """Audit trail section: interaction stats, heatmap, tables."""
    interactions = data.get("interactions", {})
    if interactions.get("status") == "missing":
        return _section(
            "Audit trail",
            '<p class="muted">interactions.jsonl not found &mdash; '
            'run <code>bd compact --audit</code> or enable agent hooks to populate.</p>',
            "audit",
        )

    parts = []

    # Stat cards
    total = interactions.get("total", 0)
    by_kind = interactions.get("by_kind", {})
    kind_str = ", ".join(f'{v} {k}' for k, v in sorted(by_kind.items(), key=lambda x: -x[1]))

    # Median cadence (from close velocity)
    gaps = [r[0] for r in detail["close_velocity"] if r[0] is not None]
    median_cadence = sorted(gaps)[len(gaps) // 2] if gaps else None

    # Batch close count (gaps < 10s)
    batch_count = sum(1 for g in gaps if g < 10) if gaps else 0

    skip_rate = data.get("skip_claim_rate")

    stat_cards = (
        f'<div class="stat-cards">'
        f'<div class="stat-card"><div class="val">{total}</div><div class="lbl">interactions</div>'
        f'<div class="card-sub">{kind_str or "none"}</div></div>'
        f'<div class="stat-card"><div class="val">{_fmt_duration(median_cadence)}</div>'
        f'<div class="lbl">median cadence</div></div>'
        f'<div class="stat-card"><div class="val">{batch_count}</div>'
        f'<div class="lbl">batch closes</div><div class="card-sub">&lt;10s gaps</div></div>'
        f'<div class="stat-card"><div class="val">{_pct(skip_rate)}</div>'
        f'<div class="lbl">skip-claim rate</div></div>'
        f'</div>'
    )
    parts.append(stat_cards)

    # Activity heatmap
    if detail["hourly"]:
        parts.append(_render_heatmap(detail["hourly"]))

    # Daily activity table
    if detail["daily_activity"]:
        da_rows = []
        for r in detail["daily_activity"]:
            da_rows.append(
                f'<tr><td>{str(r[0])[:10]}</td><td>{str(r[1])[11:16] if r[1] else "&mdash;"}</td>'
                f'<td>{str(r[2])[11:16] if r[2] else "&mdash;"}</td>'
                f'<td>{_fmt_duration(r[3])}</td><td>{r[4]}</td><td>{r[5]}</td></tr>'
            )
        parts.append(
            '<details><summary>Daily activity</summary>'
            '<table><thead><tr><th>Day</th><th>First</th><th>Last</th>'
            '<th>Span</th><th>Interactions</th><th>Beads</th></tr></thead>'
            '<tbody>' + "\n".join(da_rows) + '</tbody></table></details>'
        )

    # Status transitions
    if detail["transitions"]:
        t_total = sum(r[2] for r in detail["transitions"])
        t_rows = []
        for r in detail["transitions"]:
            share = f"{r[2] / t_total * 100:.0f}%" if t_total > 0 else "&mdash;"
            t_rows.append(
                f'<tr><td>{_esc(r[0] or "—")}</td><td>{_esc(r[1] or "—")}</td>'
                f'<td>{r[2]}</td><td>{share}</td></tr>'
            )
        parts.append(
            '<details><summary>Status transitions</summary>'
            '<table><thead><tr><th>From</th><th>To</th><th>Count</th><th>Share</th></tr></thead>'
            '<tbody>' + "\n".join(t_rows) + '</tbody></table></details>'
        )

    # Close cadence buckets
    if gaps:
        buckets = {"< 5s (batch)": 0, "5-30s (rapid)": 0, "30s-5m (working)": 0, "> 5m (break)": 0}
        for g in gaps:
            if g < 5:
                buckets["< 5s (batch)"] += 1
            elif g < 30:
                buckets["5-30s (rapid)"] += 1
            elif g < 300:
                buckets["30s-5m (working)"] += 1
            else:
                buckets["> 5m (break)"] += 1
        b_rows = "".join(
            f'<tr><td>{k}</td><td>{v}</td></tr>' for k, v in buckets.items()
        )
        parts.append(
            '<details><summary>Close cadence</summary>'
            '<table><thead><tr><th>Bucket</th><th>Count</th></tr></thead>'
            f'<tbody>{b_rows}</tbody></table></details>'
        )

    # Close reasons
    if detail["close_reasons"]:
        cr_rows = []
        for r in detail["close_reasons"]:
            cr_rows.append(
                f'<tr><td>{_esc(r[0])}</td><td>{_esc(r[1])}</td>'
                f'<td>{_esc(r[2])}</td></tr>'
            )
        parts.append(
            '<details><summary>Close reasons</summary>'
            '<table><thead><tr><th>Bead</th><th>Title</th><th>Reason</th></tr></thead>'
            '<tbody>' + "\n".join(cr_rows) + '</tbody></table></details>'
        )

    # Model usage
    if detail["model_usage"]:
        m_rows = []
        m_total = sum(r[1] for r in detail["model_usage"])
        for r in detail["model_usage"]:
            share = f"{r[1] / m_total * 100:.0f}%" if m_total > 0 else "&mdash;"
            m_rows.append(
                f'<tr><td>{_esc(r[0])}</td><td>{r[1]}</td><td>{share}</td>'
                f'<td>{int(r[2]) if r[2] else "&mdash;"}</td>'
                f'<td>{int(r[3]) if r[3] else "&mdash;"}</td>'
                f'<td>{r[4]}</td><td>{r[5]}</td></tr>'
            )
        parts.append(
            '<details><summary>Model usage</summary>'
            '<table><thead><tr><th>Model</th><th>Calls</th><th>Share</th>'
            '<th>Avg prompt</th><th>Avg response</th><th>Errors</th><th>Beads</th></tr></thead>'
            '<tbody>' + "\n".join(m_rows) + '</tbody></table></details>'
        )

    # Tool usage
    if detail["tool_usage"]:
        t_rows = []
        for r in detail["tool_usage"]:
            t_rows.append(
                f'<tr><td>{_esc(r[0])}</td><td>{r[1]}</td><td>{r[2]}</td>'
                f'<td>{r[3]}</td><td>{r[4] or "&mdash;"}%</td></tr>'
            )
        parts.append(
            '<details><summary>Tool usage</summary>'
            '<table><thead><tr><th>Tool</th><th>Calls</th><th>Successes</th>'
            '<th>Failures</th><th>Success rate</th></tr></thead>'
            '<tbody>' + "\n".join(t_rows) + '</tbody></table></details>'
        )

    # Missing data guidance
    if not detail["model_usage"] and not detail["tool_usage"]:
        parts.append(
            '<p style="color:var(--muted);font-size:0.85rem;margin-top:0.5rem">'
            'LLM and tool call tracking not yet active &mdash; '
            'run <code>bd compact --audit</code> or use agent hooks to enable model/tool attribution.</p>'
        )

    return _section("Audit trail", "\n".join(parts), "audit")


def _render_heatmap(hourly_data):
    """Hour-of-day x day-of-week activity heatmap."""
    grid = {}
    max_val = 1
    for dow, hour, count in hourly_data:
        grid[(int(dow), int(hour))] = int(count)
        if int(count) > max_val:
            max_val = int(count)

    days = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]

    rows = []
    # Header
    header = '<div class="heatmap-label"></div>'
    for h in range(24):
        header += f'<div class="heatmap-label" style="justify-content:center">{h}</div>'
    rows.append(header)

    for d_idx, d_name in enumerate(days):
        row = f'<div class="heatmap-label">{d_name}</div>'
        for h in range(24):
            val = grid.get((d_idx, h), 0)
            intensity = val / max_val if max_val > 0 else 0
            bg = f"rgba(37,99,235,{intensity:.2f})" if val > 0 else "#f0f0f0"
            txt = str(val) if val > 0 else ""
            row += f'<div class="heatmap-cell" style="background:{bg};color:{"#fff" if intensity > 0.5 else "#666"}">{txt}</div>'
        rows.append(row)

    return (
        '<div class="heatmap-wrap"><h3 style="font-size:0.9rem;margin-bottom:0.5rem">Activity heatmap</h3>'
        '<div class="heatmap">' + "\n".join(rows) + '</div></div>'
    )


def _render_charts(data, detail):
    """Chart containers — JS fills them."""
    return (
        '<section id="charts">'
        '<h2>Cycle time distribution</h2>'
        '<div class="chart-wrap"><canvas id="histChart" height="250"></canvas></div>'
        '<div id="drilldown"><h3 id="dd-title"></h3><table id="dd-table"></table></div>'
        '<h2>Throughput over time</h2>'
        '<div class="chart-wrap"><canvas id="tpChart" height="200"></canvas></div>'
        '</section>'
    )


def _render_insights(data, detail):
    """Priority, type, and spec quality tables."""
    parts = []

    # Trend signal banner
    trend = data.get("trend", {})
    if trend.get("direction") in ("improving", "regressing", "stable"):
        sig = _esc(trend.get("signal", ""))
        parts.append(f'<p style="font-size:0.9rem;margin-bottom:1rem"><strong>Trend:</strong> {sig}</p>')

    # Priority performance
    if detail["priority_perf"]:
        p_rows = []
        for r in detail["priority_perf"]:
            comp_rate = f"{r[2] / r[1] * 100:.0f}%" if r[1] > 0 else "&mdash;"
            p_rows.append(
                f'<tr><td>P{r[0]}</td><td>{r[1]}</td><td>{comp_rate}</td>'
                f'<td>{_fmt_duration(r[3])}</td><td>{_fmt_duration(r[4])}</td></tr>'
            )
        parts.append(
            '<details open><summary>Performance by priority</summary>'
            '<table><thead><tr><th>Priority</th><th>Count</th><th>Completion</th>'
            '<th>Median cycle</th><th>Avg wait</th></tr></thead>'
            '<tbody>' + "\n".join(p_rows) + '</tbody></table></details>'
        )

    # Type performance
    if detail["type_perf"]:
        t_rows = []
        for r in detail["type_perf"]:
            comp_rate = f"{r[2] / r[1] * 100:.0f}%" if r[1] > 0 else "&mdash;"
            t_rows.append(
                f'<tr><td>{_esc(r[0])}</td><td>{r[1]}</td><td>{comp_rate}</td>'
                f'<td>{_fmt_duration(r[3])}</td><td>{_fmt_duration(r[4])}</td></tr>'
            )
        parts.append(
            '<details open><summary>Performance by type</summary>'
            '<table><thead><tr><th>Type</th><th>Count</th><th>Completion</th>'
            '<th>Median cycle</th><th>p90 cycle</th></tr></thead>'
            '<tbody>' + "\n".join(t_rows) + '</tbody></table></details>'
        )

    # Spec quality correlation
    if detail["spec_quality"]:
        s_rows = []
        for r in detail["spec_quality"]:
            comp_rate = f"{r[2] / r[1] * 100:.0f}%" if r[1] > 0 else "&mdash;"
            s_rows.append(
                f'<tr><td>{_esc(r[0])}</td><td>{r[1]}</td><td>{comp_rate}</td>'
                f'<td>{_fmt_duration(r[3])}</td></tr>'
            )
        parts.append(
            '<details open><summary>Spec quality vs cycle time</summary>'
            '<table><thead><tr><th>Spec level</th><th>Count</th><th>Completion</th>'
            '<th>Median cycle</th></tr></thead>'
            '<tbody>' + "\n".join(s_rows) + '</tbody></table></details>'
        )

    if not parts:
        return ""
    return _section("Insights", "\n".join(parts), "insights")


def _render_epics(data, detail):
    """Epic detail table with cost multiples."""
    median_cycle = data.get("median_cycle_secs")
    rows = []
    for r in detail["epics"]:
        title, count, _fidelity, med_ct, scope = r
        cost = None
        if med_ct and median_cycle and median_cycle > 0:
            cost = med_ct / median_cycle
        rows.append(
            f'<tr><td>{_esc(title)}</td><td>{count}</td>'
            f'<td>{_fmt_duration(med_ct)}</td>'
            f'<td>{_cost_mult(cost)}</td><td>{scope or 0}</td></tr>'
        )

    table = (
        '<table><thead><tr><th>Epic</th><th>Beads</th>'
        '<th>Median cycle</th><th>Cost</th><th>Scope changes</th></tr></thead>'
        '<tbody>' + "\n".join(rows) + '</tbody></table>'
    )
    return _section("Epics", table, "epics")


def _render_agent_knowledge(detail):
    """Memories summary + collapsible agent knowledge base section."""
    memories = detail["memories"]
    count = len(memories)

    if count == 0:
        return (
            '<section id="knowledge">'
            '<details><summary>Agent knowledge base (0 memories)</summary>'
            '<p style="color:var(--muted)">No persistent memories stored yet. '
            'Use <code>bd remember "insight"</code> to build institutional knowledge.</p>'
            '</details></section>'
        )

    # Memories summary — extract key topics from memory keys
    keys = []
    for r in memories:
        key = r[0]
        if key.startswith("kv.memory."):
            key = key[len("kv.memory."):]
        # Use the first 3-4 words of the key as the topic label
        label = key.replace("-", " ").replace("_", " ")
        # Trim to ~40 chars
        if len(label) > 40:
            label = label[:37] + "..."
        keys.append(label)

    topics_str = "; ".join(keys[:5])
    if len(keys) > 5:
        topics_str += f" and {len(keys) - 5} more"

    summary_html = (
        f'<div style="background:#f0fdf4;border-left:4px solid var(--green);'
        f'padding:0.75rem 1rem;border-radius:0 6px 6px 0;margin-bottom:1rem">'
        f'<strong>{count} persistent memories</strong> — '
        f'<span style="color:var(--muted)">{_esc(topics_str)}</span>'
        f'</div>'
    )

    rows = []
    for r in memories:
        key = r[0]
        if key.startswith("kv.memory."):
            key = key[len("kv.memory."):]
        rows.append(f'<tr><td><strong>{_esc(key)}</strong></td><td>{_esc(r[1])}</td></tr>')

    return (
        '<section id="knowledge">'
        f'<h2>Agent knowledge base</h2>'
        + summary_html +
        f'<details><summary>Show all {count} memories</summary>'
        '<table><thead><tr><th style="width:25%">Key</th><th>Knowledge</th></tr></thead>'
        '<tbody>' + "\n".join(rows) + '</tbody></table></details></section>'
    )


def _render_scripts(data, detail):
    """All JavaScript for Chart.js charts and drill-down interactivity."""
    median_cycle = data.get("median_cycle_secs") or 1

    # Bead data for histogram drill-down
    bead_data = [
        {
            "id": r[0],
            "title": r[1] or "",
            "type": r[2] or "",
            "cycle_secs": int(r[3]) if r[3] is not None else 0,
        }
        for r in detail["beads"]
    ]

    # Daily throughput
    daily_data = [
        {
            "day": str(r[0])[:10] if r[0] else "",
            "closed": int(r[1]) if r[1] else 0,
            "median_ct": int(r[2]) if r[2] is not None else None,
        }
        for r in detail["daily"]
    ]

    return f"""<script>
const MEDIAN_CYCLE_SECS = {median_cycle};
const beadData = {json.dumps(bead_data)};
const dailyData = {json.dumps(daily_data)};

// --- Cycle time histogram ---
const buckets = [
  {{label: '<1m', min: 0, max: 60, color: '#22c55e'}},
  {{label: '1-2m', min: 60, max: 120, color: '#22c55e'}},
  {{label: '2-5m', min: 120, max: 300, color: '#22c55e'}},
  {{label: '5-10m', min: 300, max: 600, color: '#eab308'}},
  {{label: '10-30m', min: 600, max: 1800, color: '#ef4444'}},
  {{label: '>30m', min: 1800, max: Infinity, color: '#ef4444'}}
];

const counts = buckets.map(b =>
  beadData.filter(d => d.cycle_secs >= b.min && d.cycle_secs < b.max).length
);

const histCtx = document.getElementById('histChart');
if (histCtx) {{
  const histChart = new Chart(histCtx, {{
    type: 'bar',
    data: {{
      labels: buckets.map(b => b.label),
      datasets: [{{
        label: 'Beads',
        data: counts,
        backgroundColor: buckets.map(b => b.color),
      }}]
    }},
    options: {{
      plugins: {{
        tooltip: {{
          callbacks: {{
            afterLabel: function(ctx) {{
              const b = buckets[ctx.dataIndex];
              const inBucket = beadData.filter(d => d.cycle_secs >= b.min && d.cycle_secs < b.max);
              if (inBucket.length === 0) return '';
              const avgCost = inBucket.reduce((s,d) => s + d.cycle_secs, 0) / inBucket.length / MEDIAN_CYCLE_SECS;
              return 'Avg cost: ' + avgCost.toFixed(1) + 'x median';
            }}
          }}
        }}
      }},
      onClick: function(e, elements) {{
        if (!elements.length) return;
        const idx = elements[0].index;
        const b = buckets[idx];
        const inBucket = beadData.filter(d => d.cycle_secs >= b.min && d.cycle_secs < b.max)
          .sort((a,b) => b.cycle_secs - a.cycle_secs);
        const dd = document.getElementById('drilldown');
        const title = document.getElementById('dd-title');
        const table = document.getElementById('dd-table');
        if (inBucket.length === 0) {{ dd.style.display = 'none'; return; }}
        title.textContent = b.label + ' — ' + inBucket.length + ' beads';
        const fmtDur = s => {{
          if (s < 60) return s + 's';
          const m = Math.floor(s/60), sec = s % 60;
          return sec ? m+'m '+sec+'s' : m+'m';
        }};
        let html = '<thead><tr><th>ID</th><th>Title</th><th>Type</th><th>Cycle time</th><th>Cost</th></tr></thead><tbody>';
        inBucket.forEach(d => {{
          const cost = (d.cycle_secs / MEDIAN_CYCLE_SECS).toFixed(1) + 'x';
          html += '<tr><td>'+d.id+'</td><td>'+d.title+'</td><td>'+d.type+'</td><td>'+fmtDur(d.cycle_secs)+'</td><td>'+cost+'</td></tr>';
        }});
        html += '</tbody>';
        table.innerHTML = html;
        dd.style.display = 'block';
        dd.scrollIntoView({{behavior:'smooth', block:'nearest'}});
      }}
    }}
  }});
}}

// --- Throughput bar chart with cycle time trend overlay ---
const tpCtx = document.getElementById('tpChart');
if (tpCtx) {{
  new Chart(tpCtx, {{
    type: 'bar',
    data: {{
      labels: dailyData.map(d => d.day),
      datasets: [
        {{
          label: 'Beads closed',
          data: dailyData.map(d => d.closed),
          backgroundColor: '#6366f1',
          yAxisID: 'y',
        }},
        {{
          label: 'p50 cycle time (s)',
          data: dailyData.map(d => d.median_ct),
          type: 'line',
          borderColor: '#ef4444',
          backgroundColor: 'transparent',
          tension: 0.3,
          pointRadius: 3,
          yAxisID: 'y1',
        }}
      ]
    }},
    options: {{
      scales: {{
        y: {{ beginAtZero: true, position: 'left', title: {{ display: true, text: 'Beads closed' }} }},
        y1: {{ beginAtZero: true, position: 'right', grid: {{ drawOnChartArea: false }}, title: {{ display: true, text: 'Cycle time (s)' }} }}
      }}
    }}
  }});
}}
</script>"""

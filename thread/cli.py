"""Thread CLI entrypoint."""

import csv
import io
import json
import sys
from pathlib import Path

import click
import duckdb

from thread.extractor import refresh as _refresh
from thread.prime import compute_prime, format_human, format_json, _fmt_duration
from thread.report import generate_report
from thread.dolt import find_beads_dir


def _default_db_path() -> str:
    """Default thread.duckdb location inside .beads/."""
    bd = find_beads_dir()
    return str(bd / "thread.duckdb")


@click.group()
def cli():
    """Thread — forensics layer for your Beads history."""
    pass


@cli.command()
@click.option("--beads-dir", default=None,
              help="Path to .beads directory (defaults to BEADS_DIR env or ./.beads)")
def refresh(beads_dir):
    """Extract from Dolt, rebuild thread.duckdb."""
    click.echo("Refreshing Thread database...")
    counts = _refresh(beads_dir=beads_dir)
    click.echo("Extraction complete:")
    for table, n in counts.items():
        click.echo(f"  {table}: {n}")


@cli.command()
@click.option("--beads-dir", default=None,
              help="Path to .beads directory")
@click.option("--json", "as_json", is_flag=True,
              help="Output as agent-consumable JSON")
def prime(beads_dir, as_json):
    """Print project health summary."""
    bd = find_beads_dir(beads_dir)
    db_path = str(bd / "thread.duckdb")

    if not Path(db_path).exists():
        click.echo(
            "thread.duckdb not found. Run 'thread refresh' first.",
            err=True,
        )
        sys.exit(1)

    data = compute_prime(db_path)
    if as_json:
        click.echo(format_json(data))
    else:
        click.echo(format_human(data))


@cli.command()
@click.option("--beads-dir", default=None,
              help="Path to .beads directory")
@click.option("--output", default="thread-report.html",
              help="Output path for HTML report")
def report(beads_dir, output):
    """Generate thread-report.html."""
    bd = find_beads_dir(beads_dir)
    db_path = str(bd / "thread.duckdb")

    if not Path(db_path).exists():
        click.echo(
            "thread.duckdb not found. Run 'thread refresh' first.",
            err=True,
        )
        sys.exit(1)

    path = generate_report(db_path, output)
    click.echo(f"Report written to {path}")


@cli.command()
@click.argument("sql")
@click.option("--beads-dir", default=None,
              help="Path to .beads directory")
@click.option("--csv", "as_csv", is_flag=True,
              help="Output as CSV (for piping to other tools)")
@click.option("--limit", "row_limit", type=int, default=None,
              help="Limit rows returned (appends LIMIT if not already present)")
def query(sql, beads_dir, as_csv, row_limit):
    """Run ad-hoc SQL against thread.duckdb."""
    bd = find_beads_dir(beads_dir)
    db_path = str(bd / "thread.duckdb")

    if not Path(db_path).exists():
        click.echo(
            "thread.duckdb not found. Run 'thread refresh' first.",
            err=True,
        )
        sys.exit(1)

    # Append LIMIT if requested and not already present
    if row_limit is not None and "limit" not in sql.lower().split()[-2:]:
        sql = f"{sql.rstrip().rstrip(';')} LIMIT {row_limit}"

    conn = duckdb.connect(db_path, read_only=True)
    try:
        result = conn.execute(sql)
        columns = [d[0] for d in result.description] if result.description else []
        rows = result.fetchall()

        if not rows:
            click.echo("(no results)")
            return

        if as_csv:
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(columns)
            writer.writerows(rows)
            click.echo(buf.getvalue().rstrip())
        else:
            _print_table(columns, rows)
    finally:
        conn.close()


def _truncate(val, max_len=60):
    """Truncate long cell values to max_len chars with ellipsis."""
    s = str(val)
    if len(s) > max_len:
        return s[:max_len - 3] + "..."
    return s


def _print_table(columns, rows, max_cell=60):
    """Print a formatted table with truncation."""
    str_rows = [[_truncate(v, max_cell) for v in row] for row in rows]
    widths = [len(c) for c in columns]
    for row in str_rows:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(val))

    header = "  ".join(c.ljust(widths[i]) for i, c in enumerate(columns))
    click.echo(header)
    click.echo("  ".join("-" * w for w in widths))
    for row in str_rows:
        click.echo("  ".join(val.ljust(widths[i]) for i, val in enumerate(row)))


@cli.command()
@click.option("--beads-dir", default=None,
              help="Path to .beads directory")
@click.option("--json", "as_json", is_flag=True,
              help="Output as JSON")
@click.option("--limit", "row_limit", type=int, default=10,
              help="Number of sessions to show (default: 10)")
@click.option("--detail", is_flag=True,
              help="Show full session detail including bead lists")
def sessions(beads_dir, as_json, row_limit, detail):
    """Show recent work sessions with stats."""
    bd = find_beads_dir(beads_dir)
    db_path = str(bd / "thread.duckdb")

    if not Path(db_path).exists():
        click.echo("thread.duckdb not found. Run 'thread refresh' first.", err=True)
        sys.exit(1)

    conn = duckdb.connect(db_path, read_only=True)
    try:
        rows = conn.execute(
            "SELECT m.session_id, m.started_at, m.ended_at, m.duration_secs, "
            "  m.bead_count, m.beads_closed, m.beads_open, "
            "  m.parallelism_ratio, m.avg_cycle_time_secs, "
            "  c.skip_claim_count, c.documented_closes, c.dep_violations "
            "FROM mart_session_summary m "
            "LEFT JOIN v_session_compliance c ON c.session_id = m.session_id "
            "ORDER BY m.started_at DESC "
            f"LIMIT {row_limit}"
        ).fetchall()

        if not rows:
            click.echo("No sessions found. Run 'thread refresh' first.")
            return

        if as_json:
            sessions_list = []
            for r in rows:
                s = {
                    "session_id": r[0],
                    "started_at": str(r[1]) if r[1] else None,
                    "ended_at": str(r[2]) if r[2] else None,
                    "duration_secs": int(r[3]) if r[3] else 0,
                    "bead_count": int(r[4]) if r[4] else 0,
                    "beads_closed": int(r[5]) if r[5] else 0,
                    "beads_open": int(r[6]) if r[6] else 0,
                    "parallelism_ratio": float(r[7]) if r[7] else None,
                    "avg_cycle_time_secs": int(r[8]) if r[8] else None,
                    "compliance": {
                        "skip_claim_count": int(r[9]) if r[9] else 0,
                        "documented_closes": int(r[10]) if r[10] else 0,
                        "dep_violations": int(r[11]) if r[11] else 0,
                    },
                }
                if detail:
                    beads = conn.execute(
                        "SELECT sb.issue_id, b.title "
                        "FROM bridge_session_bead sb "
                        "LEFT JOIN dim_bead b ON b.issue_id = sb.issue_id "
                        "WHERE sb.session_id = ?",
                        [r[0]],
                    ).fetchall()
                    s["beads"] = [{"id": b[0], "title": b[1]} for b in beads]
                sessions_list.append(s)
            click.echo(json.dumps(sessions_list, indent=2, default=str))
        else:
            click.echo(f"{'When':<18} {'Dur':>6}  {'Beads':>5}  {'Done':>5}  "
                       f"{'Cycle':>7}  {'Par':>5}  Session")
            click.echo("-" * 75)
            for r in rows:
                when = str(r[1])[:16] if r[1] else "—"
                dur = _fmt_duration(r[3])
                total = r[4] or 0
                closed = r[5] or 0
                cycle = _fmt_duration(r[8])
                par = f"{r[7]:.1f}x" if r[7] else "—"
                click.echo(f"{when:<18} {dur:>6}  {total:>5}  {closed:>5}  "
                           f"{cycle:>7}  {par:>5}  {r[0]}")

            if detail:
                click.echo("")
                for r in rows:
                    beads = conn.execute(
                        "SELECT sb.issue_id, b.title "
                        "FROM bridge_session_bead sb "
                        "LEFT JOIN dim_bead b ON b.issue_id = sb.issue_id "
                        "WHERE sb.session_id = ?",
                        [r[0]],
                    ).fetchall()
                    click.echo(f"  {r[0]}:")
                    for b in beads:
                        click.echo(f"    {b[0]}  {b[1] or '—'}")
    finally:
        conn.close()


@cli.command()
@click.option("--beads-dir", default=None,
              help="Path to .beads directory")
@click.option("--json", "as_json", is_flag=True,
              help="Full JSON dump of interaction data")
@click.option("--tools", is_flag=True,
              help="Show tool usage with success rates")
@click.option("--models", is_flag=True,
              help="Show model usage")
def interactions(beads_dir, as_json, tools, models):
    """Show audit trail summary from interactions.jsonl."""
    bd = find_beads_dir(beads_dir)
    db_path = str(bd / "thread.duckdb")

    if not Path(db_path).exists():
        click.echo("thread.duckdb not found. Run 'thread refresh' first.", err=True)
        sys.exit(1)

    conn = duckdb.connect(db_path, read_only=True)
    try:
        if as_json:
            data = compute_prime(db_path)
            click.echo(json.dumps(data.get("interactions", {}), indent=2, default=str))
            return

        if tools:
            rows = conn.execute(
                "SELECT tool_name, calls, successes, failures, success_rate "
                "FROM v_tool_usage ORDER BY calls DESC"
            ).fetchall()
            if not rows:
                click.echo("No tool call data available. "
                           "Run 'bd compact --audit' or enable agent hooks.")
                return
            _print_table(
                ["Tool", "Calls", "Successes", "Failures", "Rate %"],
                rows,
            )
            return

        if models:
            rows = conn.execute(
                "SELECT model, calls, avg_prompt_chars, avg_response_chars, errors "
                "FROM v_model_usage ORDER BY calls DESC"
            ).fetchall()
            if not rows:
                click.echo("No LLM call data available. "
                           "Run 'bd compact --audit' or enable agent hooks.")
                return
            _print_table(
                ["Model", "Calls", "Avg prompt", "Avg response", "Errors"],
                rows,
            )
            return

        # Default: summary by kind
        rows = conn.execute(
            "SELECT kind, count, beads_touched, actors FROM v_interaction_summary "
            "ORDER BY count DESC"
        ).fetchall()
        if not rows:
            click.echo("No interactions recorded. "
                       "Run 'thread refresh' to extract from interactions.jsonl.")
            return

        total = sum(r[1] for r in rows)
        click.echo(f"Audit trail: {total} interactions\n")
        _print_table(["Kind", "Count", "Beads", "Actors"], rows)
    finally:
        conn.close()


if __name__ == "__main__":
    cli()

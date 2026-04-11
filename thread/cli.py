"""Thread CLI entrypoint."""

import sys
from pathlib import Path

import click
import duckdb

from thread.extractor import refresh as _refresh
from thread.prime import compute_prime, format_human, format_json
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
def query(sql, beads_dir):
    """Run ad-hoc SQL against thread.duckdb."""
    bd = find_beads_dir(beads_dir)
    db_path = str(bd / "thread.duckdb")

    if not Path(db_path).exists():
        click.echo(
            "thread.duckdb not found. Run 'thread refresh' first.",
            err=True,
        )
        sys.exit(1)

    conn = duckdb.connect(db_path, read_only=True)
    try:
        result = conn.execute(sql)
        # Print column headers
        columns = [d[0] for d in result.description] if result.description else []
        rows = result.fetchall()

        if not rows:
            click.echo("(no results)")
            return

        # Simple column-width formatting
        widths = [len(c) for c in columns]
        for row in rows:
            for i, val in enumerate(row):
                widths[i] = max(widths[i], len(str(val)))

        header = "  ".join(c.ljust(widths[i]) for i, c in enumerate(columns))
        click.echo(header)
        click.echo("  ".join("-" * w for w in widths))
        for row in rows:
            click.echo("  ".join(str(v).ljust(widths[i]) for i, v in enumerate(row)))
    finally:
        conn.close()


if __name__ == "__main__":
    cli()

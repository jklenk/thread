# Thread

Thread is a read-only forensics and analytics layer for [Beads](https://github.com/steveyegge/beads). It reads your local Beads Dolt history and produces a DuckDB analytical layer, a CLI, and a self-contained HTML report — so you can see whether your agents are staying true to scope and what it costs when they don't.

## Quickstart

Install Thread as a user tool (uv / pipx / pip all work):

```bash
uv tool install git+https://github.com/jklenk/thread
# or: pipx install git+https://github.com/jklenk/thread
# or: pip install git+https://github.com/jklenk/thread
```

Then, from any Beads project directory:

```bash
# 1. Extract Beads Dolt history into the analytical database.
#    By default Thread looks in ./.beads; override with --beads-dir or BEADS_DIR.
thread refresh

# 2. Project health summary (plain English)
thread prime

# 3. Same summary as JSON for agents / scripts
thread prime --json

# 4. Self-contained HTML report
thread report --output thread-report.html
open thread-report.html

# 5. Ad-hoc SQL against the DuckDB database
thread query "SELECT COUNT(*) FROM dim_bead"
```

Thread is strictly read-only — it never writes to Beads or Dolt. All output lands in `.beads/thread.duckdb` plus whatever HTML/JSON you ask for.

**Contributor setup** (running from a checkout):

```bash
git clone https://github.com/jklenk/thread
cd thread
uv sync
uv run thread prime
```

## What Thread is

- A Python CLI + DuckDB file that sits in `.beads/` alongside the Beads Dolt database
- A forensics layer for your Beads history — fidelity score tells you if your agents stayed true to scope; rework cost tells you what it cost when they didn't
- Workflow-aware: adapts its output to epic-driven, flat singleton, or mixed workflows
- A community tool, not part of the Beads core project

## What Thread is not

- Not a UI or dashboard
- Not a server or daemon
- Not a planning tool
- Not a replacement for `bd query`

## What it measures

**Fidelity score** (0.0–1.0, higher is better) — how faithful closed work stayed to its original scope. Starts at 1.0 and gets penalized when work strays:

```
1.0 − min(1.0, reopens*0.4 + revisions_requested*0.4 + rejected*0.2)
```

A bead that closed cleanly on the first pass scores 1.0. One reopen drops it to 0.6.

**Rework cost** (unbounded, lower is better) — purely event-driven. Counts the fidelity failures and context-reset overhead of getting a bead done:

```
(reopens * 2.0)
+ (revisions_requested * 1.5)
+ (rejected * 1.0)
+ (compaction_level * 1.0)
+ (agent_actor_count * 0.5)
```

Wall-clock time is **not** part of rework cost — in agentic workflows a bead can sit `in_progress` for days between a quick claim and a batch close, so status duration is a terrible proxy for engagement.

**Elapsed vs estimate** — for epic/mixed workflows, the wall-clock span between first bead created and last bead closed within an epic, divided by the epic's estimated minutes. `1.0` = on time; `3.0` = took 3× longer than estimated. `NULL` when no estimate was given.

**Workflow type** — Thread detects whether your project is:
- `epic` — all beads belong to an epic
- `flat` — beads are tracked individually without epics
- `mixed` — some of both
- `empty` — no beads yet

`thread prime` adapts its signals and thresholds to the workflow it finds.

**Additional signals** — agent closure rate, dependency activity rate, orphan rate, and actor classification (Gas Town explicit attribution when available, otherwise behavioral inference).

## CLI commands

```bash
thread refresh          # extract from Dolt, rebuild thread.duckdb
thread prime            # human-readable project health summary
thread prime --json     # agent-consumable JSON
thread report           # generate thread-report.html
thread query "<sql>"    # ad-hoc query against thread.duckdb
```

All commands accept `--beads-dir <path>` or honor the `BEADS_DIR` environment variable. Default is `./.beads`.

## How it works

1. **`thread refresh`** reads the Beads Dolt database (see [Backend modes](#backend-modes) below), extracts the history into six DuckDB tables, and materializes five analytical views.
2. **`thread prime`** queries the views to produce a short project-health summary in plain outcome language — no technical terms, no data model jargon.
3. **`thread report`** renders a self-contained HTML file with Chart.js from CDN: headline stats, fidelity/rework trends, and either a top-epics table or a project summary depending on workflow type.
4. **`thread query`** is a direct DuckDB REPL replacement for ad-hoc investigation.

The analytical layer is built around three tenets:
- **LEFT JOIN everywhere** — orphan rows participate in scoring, never excluded
- **Plain outcome language** — all user-facing signal strings describe what happened, never how
- **The bead is the atomic unit** — agents are disposable pipeline; Thread observes behavior and surfaces patterns without inferring intent

## Backend modes

Thread supports both Beads Dolt backends and picks the right one automatically from the on-disk layout under `.beads/`:

- **Embedded** — `.beads/embeddeddolt/<db>/.dolt`. Thread spawns its own `dolt sql-server` on a free port, reads the history, and shuts the server down when it's finished. This was the original Beads default and requires nothing beyond the `dolt` binary on PATH.
- **Server** — `.beads/dolt/` managed by `bd dolt start`. Thread reads the connection info from `bd dolt show --json` and connects to the running server directly — it never spawns its own process. This is Beads' default in recent versions and the required path for team deployments where the Dolt server is shared (possibly remote).

For server mode, Thread delegates the config resolution cascade (env vars `BEADS_DOLT_*` → `.beads/metadata.json` → `.beads/config.yaml`) to bd itself, so anything you can configure through bd just works without additional Thread flags.

## Project structure

```
thread/
  dolt.py               # dolt backend detection + connection management
  extractor.py          # reads Dolt, populates thread.duckdb
  actor_classifier.py   # isolated 4-tier classification cascade
  schema.sql            # 6 tables + 5 views
  prime.py              # thread prime — workflow-aware health summary
  report.py             # thread report — workflow-aware HTML
  cli.py                # click entrypoint
docs/
  DESIGN_v0.7.md        # full design rationale (includes v0.8 changelog)
  agent-vs-human-identification.md
tests/                  # 90 tests (77 unit + 13 integration)
```

## Requirements

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) for dependency management
- `dolt` binary on PATH (needed by `thread refresh`, not by unit tests)

## Testing

```bash
uv run pytest                        # all 90 tests
uv run pytest -m "not integration"   # 77 unit tests (no dolt binary needed)
uv run pytest -m integration         # 13 integration tests (require dolt)
```

## Design doc

See `docs/DESIGN_v0.7.md` for the full design rationale, including the v0.8 changelog that documents revisions discovered during live validation.

<img width="1186" height="1022" alt="image" src="https://github.com/user-attachments/assets/dce36884-c292-4e36-a9f9-197b0aa849c8" />
<img width="1184" height="877" alt="image" src="https://github.com/user-attachments/assets/239b637b-269d-4289-bc3b-6cbd516b1983" />


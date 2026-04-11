# Agent Instructions

Tactical guide for AI agents working on Thread. For conceptual overview (what fidelity score means, what Thread is for), read `README.md` — do not duplicate that prose here.

**Canonical file:** this is it. `CLAUDE.md` points here. Keep all agent guidance in this file.

---

## Build & Test

```bash
uv sync                               # install dependencies
uv run pytest -m "not integration"    # 77 unit tests — no dolt binary needed
uv run pytest -m integration          # 13 integration tests — require dolt on PATH
uv run pytest                         # all 90 tests
```

Integration tests spin up a real `dolt sql-server` against a user-provided `sample-data/.beads/` directory at the repo root. `sample-data/` is gitignored — drop any real Beads project in there (or symlink one) to exercise the full pipeline. When `sample-data/.beads/` is absent the integration suite auto-skips. If tests fail with connection errors, check `dolt version` is on PATH.

Run the CLI during development:

```bash
uv run python -m thread.cli refresh
uv run python -m thread.cli prime
uv run python -m thread.cli prime --json
uv run python -m thread.cli report --output /tmp/thread-report.html
uv run python -m thread.cli query "SELECT COUNT(*) FROM dim_bead"
```

## Architecture at a glance

```
thread/
  dolt.py              # dolt sql-server lifecycle, connection context managers
  extractor.py         # reads Dolt → populates 6 DuckDB tables
  actor_classifier.py  # 4-tier cascade: hop_uri → role_type → session → heuristic
  schema.sql           # 6 tables + 5 views, all LEFT JOINs, COALESCE on nullables
  prime.py             # workflow-aware project health summary (human + JSON)
  report.py            # workflow-aware self-contained HTML report
  cli.py               # click entrypoint
```

**Pipeline:** `refresh` starts dolt sql-server → extracts into `thread.duckdb` → `prime` / `report` / `query` read from that file. Read-only against Beads. Output lands in `.beads/thread.duckdb`.

**Tables:** `dim_bead`, `dim_hierarchy`, `dim_actor`, `fact_bead_lifecycle`, `fact_bead_events`, `fact_dep_activity`.

**Views:** `v_bead_scores`, `v_bead_dep_activity`, `v_weekly_trends`, `mart_epic_summary`, `mart_project_summary`.

## Critical invariants

These are load-bearing. Break one and the numbers lie.

- **`closer_actor_key` must never be `'root'`.** Dolt embedded mode commits every change as user `root`. Closer attribution must come from the events table, not Dolt commit author. Integration test `test_closer_is_not_dolt_root` enforces this.
- **LEFT JOIN everywhere.** Orphan rows (hierarchy entries with no matching `dim_bead`, lifecycle rows without actors, etc.) must participate in scoring. Never `INNER JOIN` in `schema.sql`.
- **`COALESCE` on every nullable before arithmetic.** Effort, fidelity, and penalty formulas all coalesce to 0 before multiplying. Missing the coalesce silently drops rows from aggregates.
- **Views must work on empty tables.** `test_views_work_on_empty_tables` runs every view against a fresh empty schema and asserts it returns 0 rows (not an error).
- **Rework cost is event-driven only.** No wall-clock time, no `active_time_secs`. In agentic workflows a bead can sit `in_progress` for days between a fast claim and a batch close — status duration is not engagement time. If you find yourself reaching for a duration column in `v_bead_scores`, stop.
- **Plain outcome language in all user-facing strings.** Never surface column names, formulas, or internal terms (`fidelity_score`, `compaction_level`, `v_bead_scores`) in `prime` output. `test_signals_are_plain_language` scans for these.
- **Workflow-awareness is mandatory in `prime` and `report`.** Detect `epic` / `flat` / `mixed` / `empty` via `mart_project_summary` and adapt headline metrics accordingly. Do not assume epics exist.

## Non-obvious gotchas

- **`quality_score` is ambiguous across joins.** `v_bead_scores` and `fact_bead_lifecycle` both carry it. When joining, always qualify as `s.quality_score` (or whichever alias). Same for `fidelity_score` and `effort_score`.
- **Hierarchy depth must be computed after all parent-child edges are applied**, not during the walk. Earlier versions raced: grandchildren processed before the parent's edge existed got `depth=1` instead of `2`. See `extract_dim_hierarchy` in `extractor.py` — the fix recomputes depths from the final `parents` map. `test_parent_child_depth_order_independent` is the regression.
- **Dotted bead IDs** (`beads-123.456`) exist but aren't fully parsed yet — tracked as a v1.1 bead. If you touch ID parsing, check for this.
- **`is_template` epics** must be filtered out of project-level metrics. `mart_project_summary` and most `mart_epic_summary` consumers apply `WHERE (is_template = false OR is_template IS NULL)`.
- **`DATE_DIFF` not `DATEDIFF`** in DuckDB. The underscore version is the correct spelling.

## Testing discipline

- **Schema tests assert exact table and view lists.** If you add a table or view, `test_all_tables_created` / `test_all_views_created` will fail — update them.
- **Integration tests assert headline shape against `sample-data`** (34 beads, clean agentic project, fidelity ≥ 0.9, agent_closure_rate == 1.0). If you change scoring formulas, expect these to move.
- **Write a regression test for every race/ordering bug you fix.** See `test_parent_child_depth_order_independent` and `test_effort_score_excludes_wall_clock` as templates.
- **Never mock the database.** Integration tests hit a real Dolt + DuckDB. Unit tests use a real in-memory DuckDB with `schema.sql` loaded via the `duckdb_conn` conftest fixture.

## Conventions

- **Signal language:** lead with outcome, never formula. "work is staying close to original scope" ✅ — "fidelity_score ≥ 0.9" ❌.
- **No emoji, no ceremony in output.** Plain text, direct.
- **One bead per logical unit of work.** File a bead before writing non-trivial code. Mark `in_progress` when you start, close when done.
- **Don't add features, refactors, or "improvements" beyond what was asked.** Thread v1 is deliberately small.

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:ca08a54f -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

## Session Completion

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd dolt push
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
<!-- END BEADS INTEGRATION -->

## Non-Interactive Shell Commands

**ALWAYS use non-interactive flags** with file operations to avoid hanging on confirmation prompts. `cp`, `mv`, `rm` may be aliased to `-i` on some systems.

```bash
cp -f source dest           # NOT: cp source dest
mv -f source dest           # NOT: mv source dest
rm -f file                  # NOT: rm file
rm -rf directory            # NOT: rm -r directory
```

Other commands that may prompt: `scp`/`ssh` (`-o BatchMode=yes`), `apt-get` (`-y`), `brew` (`HOMEBREW_NO_AUTO_UPDATE=1`).

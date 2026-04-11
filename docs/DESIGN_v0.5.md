# Thread Design Document v0.5

_April 2026_

---

## What is Thread

A standalone community project. Python CLI + DuckDB analytical layer over Beads' Dolt history.

**One line:** Thread is the forensics layer for your Beads history.

**Two lines:** Fidelity score tells you if your agents stayed true to scope. Effort score tells you what it cost when they didn't.

**The unique play:** Thread feeds historical pattern recognition back to agents as pre-flight calibration context via `thread prime` — something no existing Beads tool provides.

---

## What it is not

- Not a UI or dashboard
- Not a server or planning tool
- No continuous sync daemon
- No sessions concept — the bead is the atomic unit, agents are disposable pipeline

---

## Core design principles

- Lightweight over complete
- Pragmatic over Kimball-pure — denormalize where queries get painful, normalize where storage matters
- No redundant columns — derive through dimensional relationships
- No hardcoded weights in v1 — ship raw components plus one opinionated default view, weights configurable via `thread.yaml`
- Flat beats clever until real data validates otherwise
- Bead is always the entity. Never the agent.
- LEFT JOIN everywhere in views — orphan rows participate in scoring, never excluded
- Thread observes behavior and surfaces patterns. It never infers intent. Judgment belongs to the human or the Refinery.
- Data has no agenda. Thread is neutral and unopinionated.

---

## Signal language standard

All signal strings must be written in plain outcome language. Describe what happened and what it might mean — never expose internal implementation details, data model concepts, or system internals. If a signal requires technical knowledge to interpret, rewrite it.

Signal language is a v1 hypothesis — subject to revision based on real-world agent behavior testing.

---

## Architecture

```
Dolt (source of truth)
    ↓
native Beads tables + dolt_diff_ history
    ↓
Thread extractor (Python, runs locally)
    ↓
thread.duckdb (sits in .beads/)
    ↓
┌─────────────────┬──────────────────┬─────────────────┐
│ thread prime   │ thread report   │ BI tools /      │
│ (agents/humans) │ (HTML, humans)   │ Parquet export  │
└─────────────────┴──────────────────┴─────────────────┘
```

**Write path:** Dolt only, via `bd` CLI. Thread is strictly read-only.

**Refresh:** Full rebuild to ~10k beads, incremental beyond via Dolt `AS OF`.

**Integrations:** DuckDB speaks natively to Tableau, Metabase, Evidence, dbt, Parquet, Snowflake, BigQuery.

---

## Why not just query Dolt directly

- **Analytical workload.** Dolt is optimized for writes and point queries. DuckDB handles aggregations in milliseconds.
- **Historical query expertise.** Thread pre-computes `dolt_diff_` derivations so users never have to write that logic.
- **BI tool compatibility.** DuckDB bridges Beads history to existing analyst toolchains.

---

## CLI commands

```bash
thread refresh          # extract from Dolt, rebuild thread.duckdb
thread prime            # human-readable project health summary
thread prime --json     # agent-consumable JSON
thread report           # generate thread-report.html
thread query "<sql>"    # ad-hoc query against thread.duckdb
thread export           # export to Parquet
```

---

## Two headline metrics

### Fidelity Score

Did the bead execution stay faithful to original scope?

```
fidelity_score = 1.0 - MIN(1.0,
  (reopen_count               × 0.4)
  + (revision_requested_count × 0.4)
  + (rejected_count           × 0.2)
)
expressed as 0.0 → 1.0
```

Thresholds (v1 defaults, documented in README):
- 1.0 = perfectly faithful execution
- Below 0.7 = scoping problem
- Below 0.5 = something structurally wrong with how beads are being written

`quality_score` (native Beads/Gas Town Refinery signal) is surfaced alongside fidelity score
but never folded into the formula. They measure different things — fidelity is execution-time
behavior, quality is merge-time judgment.

### Effort Score

What did execution actually cost?

```
effort_score =
  (active_time_secs / 3600.0)      # base cost in hours
  + (reopen_count × 2.0)
  + (revision_requested_count × 1.5)
  + (rejected_count × 1.0)
  + (compaction_level × 0.5)
  + (agent_actor_count × 1.0)
```

All weights are v1 hypotheses. Configurable via `thread.yaml`. Easy to tune without forking.

---

## thread prime output schema

```json
{
  "project_fidelity_score": 0.74,
  "fidelity_signal": "agents are frequently not completing work as originally scoped",
  "avg_effort_score": 3.2,
  "effort_signal": "work is costing more than expected — context is being reset frequently",
  "avg_quality_score": 0.81,
  "quality_signal": "Refinery merge reviews are largely positive",
  "quality_score_note": null,
  "dep_activity_rate": 2.3,
  "dep_activity_signal": "dependency changes are frequent — review closed beads to assess if discovery was expected or scope was unclear",
  "orphan_bead_rate": 0.12,
  "orphan_signal": "12% of bead activity involves work that was cleaned up or temporary — your agents are moving fast but some context may be lost"
}
```

Every numeric score has a corresponding `_signal` field in plain outcome language.
`null` when insufficient data. `_note` fields explain missing data sources.

---

## thread report

Single self-contained HTML file. No server, no build step, works offline.

**Contents:**
- Three critical numbers: fidelity score, effort score, beads closed
- Two line charts: fidelity and effort over time (weekly)
- Top 10 epics table: title, fidelity, effort, penalty, had criteria, compactions

**Implementation:** Pure HTML + inline Chart.js from CDN. Python extractor writes data
as inline JSON blob. Chart.js reads on load.

---

## Forensics principle

Dep activity (formerly called churn) is not penalized. Thread cannot know if a dependency
change was good discovery or poor scoping. It surfaces the pattern. Judgment belongs to the human.

This applies broadly: Thread observes, never judges.

---

## Orphan handling

- LEFT JOIN everywhere — orphan rows participate in scoring
- Null dim attributes handled via COALESCE in display
- Orphan rate is a first-class `thread prime` signal
- `orphan_bead_count` surfaced in `mart_epic_summary`

Orphans are signal. A high orphan rate means aggressive compaction or wisp activity — that
is forensically meaningful and must not be silently excluded.

---

## Table definitions

### fact_bead_events ✅

Source: `schema.go#L155-L170`
https://github.com/gastownhall/beads/blob/67249651/internal/storage/dolt/schema.go#L155-L170

```sql
CREATE TABLE fact_bead_events (
  issue_id    VARCHAR,    -- events.issue_id
  event_type  VARCHAR,    -- events.event_type (IS the field indicator)
  actor       VARCHAR,    -- events.actor ($USER or agent name, free-form)
  old_value   VARCHAR,    -- events.old_value
  new_value   VARCHAR,    -- events.new_value
  created_at  TIMESTAMP   -- events.created_at
);
```

---

### fact_bead_lifecycle ✅

Source: `schema.go#L11-L95`, `types.go#L38-L44`
https://github.com/gastownhall/beads/blob/67249651/internal/storage/dolt/schema.go#L11-L95
https://github.com/steveyegge/beads/blob/67249651/internal/types/types.go#L38-L44

```sql
CREATE TABLE fact_bead_lifecycle (
  issue_id                  VARCHAR,
  created_at                TIMESTAMP,  -- issues.created_at
  first_claimed_at          TIMESTAMP,  -- DERIVED: first event new_value='in_progress'
  first_closed_at           TIMESTAMP,  -- DERIVED: first event new_value='closed'
  final_closed_at           TIMESTAMP,  -- issues.closed_at (native, enforced by invariant)
  closed_by_session         VARCHAR,    -- issues.closed_by_session (native)
                                        -- format unknown — validate against live data
  time_to_start_secs        BIGINT,     -- DERIVED: first_claimed_at - created_at
  active_time_secs          BIGINT,     -- DERIVED: sum of in_progress windows
  total_elapsed_secs        BIGINT,     -- DERIVED: final_closed_at - created_at
  reopen_count              INT,        -- DERIVED: count closed→open transitions
  agent_actor_count         INT,        -- DERIVED: distinct agents via Creator.Platform
                                        -- each = separate context window = real token spend
  compaction_level          INT,        -- issues.compaction_level (native, in-session cost)
  compacted_at              TIMESTAMP,  -- issues.compacted_at (native)
  validation_count          INT,        -- DERIVED from issues.validations
  revision_requested_count  INT,        -- DERIVED: outcome='revision_requested'
  rejected_count            INT,        -- DERIVED: outcome='rejected'
  quality_score             DOUBLE,     -- issues.quality_score (Refinery-set, nullable)
  crystallizes              BOOLEAN,    -- issues.crystallizes (true=compounding)
  has_derived_fields        BOOLEAN     -- flag: any DERIVED field used inference
);
```

---

### fact_dep_activity ✅

Source: `schema.go#L98-L115`, `types.go#L622-L635`
https://github.com/gastownhall/beads/blob/67249651/internal/storage/dolt/schema.go#L98-L115
https://github.com/steveyegge/beads/blob/67249651/internal/types/types.go#L622-L635

```sql
CREATE TABLE fact_dep_activity (
  issue_id          VARCHAR,    -- dependencies.issue_id
  depends_on_id     VARCHAR,    -- dependencies.depends_on_id
                                -- may be 'external:project:capability' — handle gracefully
  dep_type          VARCHAR,    -- dependencies.type
  dep_category      VARCHAR,    -- DERIVED: 'workflow'/'association'/'hop'/'reference'
                                -- only workflow types are activity signals
                                -- workflow = blocks/parent-child/waits-for/conditional-blocks
  dep_event         VARCHAR,    -- 'added' (native from created_at)
                                -- 'removed' (from dolt_diff_dependencies if available,
                                --            otherwise inferred from absence)
  created_at        TIMESTAMP,  -- dependencies.created_at (native — this is the timestamp)
  created_by        VARCHAR,    -- dependencies.created_by (native)
  after_first_claim BOOLEAN,    -- DERIVED: created_at > first_claimed_at
                                -- false = planning (not activity)
                                -- true = execution activity
  is_replan         BOOLEAN     -- DERIVED: remove+add same pair within 60 seconds
                                -- collapses to one logical event in reporting
);
```

Note: dep activity is a forensic signal only. It is never a scoring penalty.
Thread cannot know if a dependency change was good discovery or poor scoping.

---

### fact_conflicts ❌ REMOVED

Only available in multi-agent server mode with a Dolt remote. Cleared on resolution.
Not computable historically. Redundant with `bd vc conflicts`. Serves minority of users.
Do not implement.

---

### dim_bead ✅

Source: `schema.go#L11-L95`, `types.go#L15-L133`
https://github.com/gastownhall/beads/blob/67249651/internal/storage/dolt/schema.go#L11-L95
https://github.com/steveyegge/beads/blob/67249651/internal/types/types.go#L15-L133

```sql
CREATE TABLE dim_bead (
  issue_id                VARCHAR PRIMARY KEY,  -- issues.id
  title                   VARCHAR,              -- issues.title (NOT NULL, max 500)
  issue_type              VARCHAR,              -- issues.issue_type
                                                -- bug/feature/task/epic/chore/decision
  priority                INT,                  -- issues.priority (0-4, default 2)
  created_by              VARCHAR,              -- issues.created_by (may be agent)
  owner                   VARCHAR,              -- issues.owner (human CV attribution)
  assignee                VARCHAR,              -- issues.assignee (nullable)
  estimated_minutes       INT,                  -- issues.estimated_minutes (nullable)
                                                -- null = no upfront scope estimate given
  has_description         BOOLEAN,              -- DERIVED: description != ''
  has_acceptance_criteria BOOLEAN,              -- DERIVED: acceptance_criteria != ''
  has_design              BOOLEAN,              -- DERIVED: design != ''
  quality_score           DOUBLE,               -- issues.quality_score (Refinery, nullable)
  crystallizes            BOOLEAN,              -- issues.crystallizes
  source_system           VARCHAR,              -- issues.source_system (federation context)
  is_template             BOOLEAN               -- issues.is_template
                                                -- exclude templates from work scoring
);
```

---

### dim_hierarchy ✅

Source: `cmd/bd/dep.go#L29-L42`, dependencies table type='parent-child'
https://github.com/steveyegge/beads/blob/67249651/cmd/bd/dep.go#L29-L42

```sql
CREATE TABLE dim_hierarchy (
  issue_id   VARCHAR,    -- the bead
  parent_id  VARCHAR,    -- direct parent (null if root/epic)
  root_id    VARCHAR,    -- epic at top of tree (= issue_id if depth = 0)
  depth      INT,        -- 0=epic/root, 1=task, 2=subtask
  is_root    BOOLEAN,    -- DERIVED: depth = 0
  path       VARCHAR     -- DERIVED: 'bd-a3f8/bd-a3f8.1/bd-a3f8.1.2'
                         -- enables subtree queries without recursion:
                         -- WHERE path LIKE 'bd-a3f8%'
);
```

Notes:
- Hierarchy derived from ID string parsing (primary) + dep edges (validation fallback)
- `root_id = issue_id` when `depth = 0`
- Join to `dim_bead` on `root_id` to get epic title and scoping context
- Standalone beads: `parent_id = null`, `root_id = issue_id` — participate in scoring
- Beads enforces acyclic tree at write time — no cycle detection needed in extractor

---

### dim_actor ✅ HIGH VOLATILITY

Source: `types.go#L110-L115`, `types.go#L1107-L1132`
https://github.com/steveyegge/beads/blob/67249651/internal/types/types.go#L110-L115
https://github.com/steveyegge/beads/blob/67249651/internal/types/types.go#L1107-L1132

```sql
CREATE TABLE dim_actor (
  actor_key             VARCHAR PRIMARY KEY,  -- raw actor string OR hop:// URI
  actor_name            VARCHAR,              -- display name
  platform              VARCHAR,              -- EntityRef.Platform if HOP URI, null otherwise
                                              -- 'gastown' = agent, 'github' = human
  org                   VARCHAR,              -- EntityRef.Org if HOP URI, null otherwise
  role_type             VARCHAR,              -- issues.role_type if Gas Town agent
  rig                   VARCHAR,              -- issues.rig if agent bead, null otherwise
  actor_class           VARCHAR,              -- 'agent' / 'human' / 'unknown'
  classification_source VARCHAR               -- 'hop_uri' / 'role_type' / 'heuristic' / 'unknown'
                                              -- encodes confidence of classification
);
```

**IMPORTANT:** Classification logic must be isolated in `actor_classifier.py`.
Do not couple other extractor logic to its output.
This is the component most likely to change significantly after real-world testing.

Two tiers of actor data:
- Gas Town users: `hop://` URI → platform, org, id, role_type, rig all parseable
- Solo Beads users: `BEADS_ACTOR` string → free-form, best-effort heuristic only

`unknown` is a valid and honest actor class. Do not force a guess.

---

## View definitions

### v_bead_scores

Bead-level fidelity and effort scores.

```sql
CREATE VIEW v_bead_scores AS
SELECT
  f.issue_id,
  f.total_elapsed_secs,
  f.active_time_secs,
  f.reopen_count,
  f.agent_actor_count,
  f.compaction_level,
  f.revision_requested_count,
  f.rejected_count,
  f.quality_score,
  f.crystallizes,
  f.has_derived_fields,

  ROUND(f.active_time_secs / 3600.0, 2)              AS base_cost_hours,

  ROUND(
    1.0 - LEAST(1.0,
      (f.reopen_count * 0.4)
      + (f.revision_requested_count * 0.4)
      + (f.rejected_count * 0.2)
    ), 2
  )                                                    AS fidelity_score,

  ROUND(
    (f.active_time_secs / 3600.0)
    + (f.reopen_count * 2.0)
    + (f.revision_requested_count * 1.5)
    + (f.rejected_count * 1.0)
    + (f.compaction_level * 0.5)
    + (f.agent_actor_count * 1.0)
  , 2)                                                AS effort_score

FROM fact_bead_lifecycle f;
```

---

### v_bead_dep_activity

Dep activity rollup per bead. Workflow types only.

```sql
CREATE VIEW v_bead_dep_activity AS
SELECT
  issue_id,
  COUNT(*)                                            AS total_dep_events,
  SUM(after_first_claim::INT)                         AS post_claim_dep_events,
  SUM(is_replan::INT)                                 AS replan_events,
  COUNT(DISTINCT dep_type)                            AS dep_type_variety
FROM fact_dep_activity
WHERE dep_category = 'workflow'
GROUP BY issue_id;
```

---

### mart_epic_summary ✅

Primary analytical surface. Epic-level rollup of all signals.

```sql
CREATE VIEW mart_epic_summary AS
SELECT
  h.root_id                                           AS epic_id,
  epic.title                                          AS epic_title,
  epic.issue_type                                     AS epic_type,
  epic.priority                                       AS epic_priority,
  epic.estimated_minutes                              AS epic_estimated_minutes,
  epic.has_acceptance_criteria                        AS epic_had_criteria,
  epic.has_design                                     AS epic_had_design,
  epic.crystallizes                                   AS epic_crystallizes,

  COUNT(DISTINCT h.issue_id)                          AS bead_count,
  COUNT(DISTINCT CASE WHEN h.depth = 1
    THEN h.issue_id END)                              AS task_count,
  COUNT(DISTINCT CASE WHEN h.depth = 2
    THEN h.issue_id END)                              AS subtask_count,

  ROUND(AVG(s.fidelity_score), 2)                    AS avg_fidelity_score,
  MIN(s.fidelity_score)                               AS min_fidelity_score,
  SUM(f.reopen_count)                                 AS total_reopens,
  SUM(f.revision_requested_count)                     AS total_revisions_requested,
  SUM(f.rejected_count)                               AS total_rejections,

  ROUND(AVG(s.effort_score), 2)                      AS avg_effort_score,
  ROUND(SUM(s.base_cost_hours), 2)                   AS total_base_cost_hours,
  ROUND(SUM(s.effort_score) - SUM(s.base_cost_hours), 2) AS total_fidelity_penalty,
  SUM(f.compaction_level)                             AS total_compactions,
  SUM(f.agent_actor_count)                            AS total_agent_touches,

  COALESCE(SUM(da.post_claim_dep_events), 0)          AS post_claim_dep_events,
  COALESCE(SUM(da.replan_events), 0)                  AS replan_events,

  ROUND(AVG(f.quality_score), 2)                     AS avg_quality_score,

  SUM(f.has_derived_fields::INT)                      AS beads_with_inferred_data,
  COUNT(DISTINCT CASE WHEN epic.issue_id IS NULL
    THEN h.issue_id END)                              AS orphan_bead_count

FROM dim_hierarchy h
LEFT JOIN dim_bead epic    ON epic.issue_id = h.root_id
JOIN v_bead_scores s       ON s.issue_id = h.issue_id
JOIN fact_bead_lifecycle f ON f.issue_id = h.issue_id
LEFT JOIN v_bead_dep_activity da ON da.issue_id = h.issue_id
GROUP BY
  h.root_id,
  epic.title,
  epic.issue_type,
  epic.priority,
  epic.estimated_minutes,
  epic.has_acceptance_criteria,
  epic.has_design,
  epic.crystallizes,
  epic.issue_id;
```

---

### v_weekly_trends ✅

Time-windowed activity for trend analysis and before/after forensics.

```sql
CREATE VIEW v_weekly_trends AS
SELECT
  DATE_TRUNC('week', f.final_closed_at)   AS week,
  COUNT(*)                                 AS beads_closed,
  ROUND(AVG(s.fidelity_score), 2)         AS avg_fidelity,
  ROUND(AVG(s.effort_score), 2)           AS avg_effort,
  SUM(f.reopen_count)                     AS total_reopens,
  SUM(f.compaction_level)                 AS total_compactions
FROM fact_bead_lifecycle f
JOIN v_bead_scores s USING (issue_id)
WHERE f.final_closed_at IS NOT NULL
GROUP BY DATE_TRUNC('week', f.final_closed_at)
ORDER BY week;
```

Use case: slice by a known date (AGENTS.md change, template revision) to measure
spec revision impact on fidelity and effort trends.

---

## Headline query

```sql
SELECT
  epic_title,
  bead_count,
  avg_fidelity_score,
  avg_effort_score,
  total_fidelity_penalty,
  total_compactions,
  epic_had_criteria,
  avg_quality_score
FROM mart_epic_summary
WHERE epic_crystallizes = true
ORDER BY total_fidelity_penalty DESC
LIMIT 20;
```

---

## Consumer separation

| consumer | surface | format |
|---|---|---|
| agents | `thread prime --json` | machine-readable JSON |
| humans (quick glance) | `thread report` | self-contained HTML |
| humans (deep analysis) | `thread.duckdb` direct | SQL / BI tools |
| analysts / warehouses | `thread export` | Parquet |

---

## Open questions — validate against live Beads data before implementing

1. Does `dolt_diff_issues` work in embedded Dolt mode or server mode only?
2. What is the format of `closed_by_session`? UUID, name, or hash?
3. How populated is `Creator` EntityRef for non-Gas Town users?
4. Can dep removals be read from `dolt_diff_dependencies` or must we infer from absence?

Run these to answer them:
```bash
bd query "SELECT * FROM events LIMIT 5"
bd query "SELECT actor, event_type, old_value, new_value FROM events LIMIT 10"
bd query "SELECT closed_by_session FROM issues WHERE closed_at IS NOT NULL"
bd query "SELECT * FROM dolt_diff_issues LIMIT 5"
```

---

## Roadmap

### v1 — personal forensics
- `thread refresh`
- `thread prime` (solo, human + JSON)
- `thread report` (HTML)
- `thread query`
- All approved fact, dim, and view tables
- `mart_epic_summary` + `v_weekly_trends`
- Killer demo query

### Future — community forensics
- `thread prime --community`
- Contributor history transport mechanism
- Cross-fork aggregation
- Vibe maintainer triage use case
- Pending real-world usage signal and community input

---

## Repo

`jklenk/thread` — MIT License

---

_v0.5 — April 2026_

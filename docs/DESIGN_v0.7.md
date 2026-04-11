# Thread Design Document v0.7

_April 2026_

---

## Changelog — v0.8 (2026-04-10)

Revisions discovered during Phase 5 live validation. Code is authoritative;
this section documents the divergences from the v0.7 text below.

**1. effort_score is now purely event-driven.**
Wall-clock `active_time_secs` was removed from the formula. In agentic workflows
a bead can sit `in_progress` for days between a quick claim and a batch close —
wall time was measuring calendar gaps, not engagement. Running against the
Thread self-build surfaced 67-hour base costs on beads built in one session.

```python
effort_score = (
    (reopen_count * 2.0)
    + (revision_requested_count * 1.5)
    + (rejected_count * 1.0)
    + (compaction_level * 1.0)
    + (agent_actor_count * 0.5)
)
```

`active_time_secs`, `total_elapsed_secs`, and `time_to_start_secs` are retained
in `fact_bead_lifecycle` and surfaced via `base_cost_hours` as throughput signals.
They are not cost signals.

**2. `total_fidelity_penalty` renamed to `total_effort_penalty` and redefined.**
The old definition (`SUM(effort_score) - SUM(base_cost_hours)`) degenerated on
clean agentic projects to `bead_count × agent_actor_count × 0.5` — a bead-count
proxy with noise. The new definition counts fidelity failures only:

```sql
SUM(
  (f.reopen_count * 2.0)
  + (f.revision_requested_count * 1.5)
  + (f.rejected_count * 1.0)
) AS total_effort_penalty
```

Compaction and agent floor are cost signals, not fidelity failures.

**3. `mart_epic_summary` adds elapsed-time columns.**
For iterative workflows where beads are never reopened, wall-clock span between
first bead created and last bead closed within the epic is the real cost signal.
New columns: `epic_started_at`, `epic_completed_at`, `epic_elapsed_minutes`,
`elapsed_vs_estimate_ratio`. The ratio is NULL when no estimate was given.

**4. New view: `mart_project_summary`.**
Many users don't use epics. For them `mart_epic_summary` returns one row per bead
with `bead_count=1`. The project-level rollup works regardless, and distinguishes
`singleton_bead_count` (depth=0, no children) from `epic_count` (depth=0, has
children). Templates excluded via the standard
`is_template = false OR is_template IS NULL` filter.

**5. `thread prime` detects workflow type.**
Output now includes `workflow_type` ∈ {`epic`, `flat`, `mixed`, `empty`}, derived
from `epic_count` vs `singleton_bead_count`. Elapsed-vs-estimate metric is only
shown for epic/mixed workflows. New `singleton_signal` fires when singleton beads
exist. Headline metrics (fidelity, rework cost, agent closure) come from
`mart_project_summary` so they work for all workflow shapes.

**6. Headline query filter corrected.**
v0.7 specified `WHERE epic_crystallizes = true` which excluded normal project
work. Implementation uses `WHERE epic_is_template = false OR epic_is_template IS NULL`
and sorts by `total_effort_penalty DESC, avg_fidelity_score ASC`.

**7. Report is workflow-aware.**
`thread report` renders the top-epics table for epic/mixed workflows and a
project-summary table for flat workflows. Headline stats come from
`mart_project_summary` in both cases. Workflow type is shown as a tag in the
page header.

**8. `effort_signal` thresholds recalibrated.**
Because `effort_score` is now a pure penalty count (not wall-clock hours),
thresholds are: `<= 0.5` clean first pass, `< 2.0` normal discovery,
`< 5.0` noticeable rework, `>= 5.0` significant rework.

**Also fixed during Phase 5:**
- Hierarchy depth race in `extract_dim_hierarchy`: walked the parents dict during
  mutation, giving grandchildren the wrong depth. Fix: apply all parent-child
  edges before recomputing depths from the final map.
- `quality_score` ambiguous column error: both `v_bead_scores` and
  `fact_bead_lifecycle` carry `quality_score`. Prime and report queries now
  qualify as `s.quality_score`.

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
- Design for the majority user — solo Beads users are the primary path, Gas Town users get enrichment

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
but never folded into the formula. They measure different things.

### Effort Score

What did execution actually cost?

```
effort_score =
  (active_time_secs / 3600.0)      # base cost in hours
  + (reopen_count × 2.0)
  + (revision_requested_count × 1.5)
  + (rejected_count × 1.0)
  + (compaction_level × 0.5)
  + (agent_actor_count × 1.0)      # see actor classification note
```

All weights are v1 hypotheses. Configurable via `thread.yaml`.

**Note:** `agent_actor_count` is most reliable for Gas Town users. For solo users,
actor classification relies on behavioral heuristics — see `actor_classifier.py`.

---

## Validated open questions (from real sample data — April 2026)

All validated against a real 100% agentic Beads project in embedded mode.

### Q1: Does `dolt_diff_issues` work in embedded Dolt mode? ✅ YES

Confirmed working. Returns full `from_*` / `to_*` column pairs, `diff_type`
(added/modified), commit hashes and dates. 99 rows in sample (34 added + 65 modified).
Fully usable for lifecycle derivation. No changes needed.

### Q2: Format of `closed_by_session`? ✅ RESOLVED — FIELD REMOVED

Empty string across all 34 closed issues. Never written by current bd versions.
Removed from `fact_bead_lifecycle`. Do not implement.

### Q3: Creator EntityRef population for non-Gas Town users? ✅ RESOLVED

Not populated. Solo users show plain `$USER` strings throughout. All agent-specific
fields (`role_type`, `rig`, `agent_state`, `hop://` URIs) are empty.

**Critical finding from sample data analysis:**
`BEADS_ACTOR` defaults to `$USER`. Claude Code sub-agents write beads under the machine
owner's username. In the sample data, every bead was created by a human but closed by
an agent — but the database doesn't say that explicitly. It requires behavioral inference.

This is why `actor_classifier.py` uses a cascade of behavioral heuristics for solo users.
For the majority of Beads users, heuristic classification is the primary path — not a fallback.

### Q4: Dep removals from `dolt_diff_dependencies`? ✅ YES with fallback

`dolt_diff_dependencies` queryable with `diff_type` column. Sample shows all `'added'`
(no removals in this project). Schema supports removals — implement both `diff_type='removed'`
and inference-from-absence fallback.

---

## Actor identification — revised after real data analysis

### The fundamental challenge for solo users

In solo Beads usage, `BEADS_ACTOR` defaults to `$USER`. This means:
- Human creating a bead manually → writes `$USER`
- Claude Code creating a bead on the user's behalf → writes `$USER`
- A sub-agent closing a bead → writes `$USER`

All three are indistinguishable by actor string alone. Classification requires
behavioral inference from event patterns.

### The role distinction that matters

Creator and closer serve different analytical purposes:

- **Creator** = scoping accountability — who defined the work
- **Closer** = execution accountability — who did the work (or directed it to be done)

If an agent closed the bead, the agent executed it (or was told by another agent to
close it). That's the effort and fidelity signal. Both roles are tracked in
`fact_bead_lifecycle` as separate foreign keys to `dim_actor`.

### Actor classification cascade (isolated in `actor_classifier.py`)

```python
def classify_actor(actor_string, issue, events, dolt_diff):

    # Tier 1 — Gas Town explicit signals (high confidence)
    if issue.get('role_type'):
        return 'agent', 'role_type'
    if has_hop_uri(actor_string):
        return classify_by_platform(actor_string), 'hop_uri'
    if issue.get('closed_by_session'):
        return 'agent', 'session'
    if issue.get('agent_state'):
        return 'agent', 'agent_state'

    # Tier 2 — behavioral heuristics (solo users — PRIMARY PATH for majority)
    # strongest signals from real sample data analysis:
    if is_batch_close(events):       # 4+ closures within 12 seconds = agent loop
        return 'agent', 'heuristic'
    if is_velocity_burst(events):    # created→claimed→closed < 3 minutes
        return 'agent', 'heuristic'
    if has_compaction(issue):        # compaction_level > 0 = agent context pressure
        return 'agent', 'heuristic'
    if has_agent_close_reason(issue): # code-artifact specificity in close_reason
        return 'agent', 'heuristic'

    # Tier 3 — human inference (human-paced gaps, no agent signals)
    if has_human_paced_gaps(events): # gaps > 5 minutes between events
        return 'human', 'heuristic'

    # Tier 4 — genuinely unknown
    return 'unknown', 'unknown'
```

**Valid `actor_class` values:** `'agent'` / `'human'` / `'unknown'`

**Valid `classification_source` values:** `'hop_uri'` / `'role_type'` / `'session'` /
`'agent_state'` / `'heuristic'` / `'unknown'`

`classification_source` encodes confidence. Users can filter by source for
high-confidence analysis.

**This module is the highest-volatility component in Thread.** Isolate completely.
Do not couple other extractor logic to its output. Expected to change as:
- Gas Town adoption grows
- Beads adds native agent identity tracking
- More real-world usage patterns are observed

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
  "agent_closure_rate": 0.89,
  "agent_closure_signal": "89% of completed work was closed by an agent — your agentic workflow is well established",
  "dep_activity_rate": 2.3,
  "dep_activity_signal": "dependency changes are frequent — review closed beads to assess if discovery was expected or scope was unclear",
  "orphan_bead_rate": 0.12,
  "orphan_signal": "12% of bead activity involves work that was cleaned up or temporary — your agents are moving fast but some context may be lost",
  "actor_classification_note": null
}
```

`actor_classification_note` is set to a plain-language explanation when heuristic
classification is the primary path (solo mode). `null` when Gas Town provides
explicit attribution.

---

## thread report

Single self-contained HTML file. No server, no build step, works offline.

**Contents:**
- Three critical numbers: fidelity score, effort score, beads closed
- Two line charts: fidelity and effort over time (weekly)
- Top 10 epics table: title, fidelity, effort, penalty, had criteria, compactions

**Implementation:** Pure HTML + inline Chart.js from CDN. Python writes data as
inline JSON blob. Chart.js reads on load.

---

## Forensics principle

Dep activity is not penalized. Thread cannot know if a dependency change was good
discovery or poor scoping. It surfaces the pattern. Judgment belongs to the human.

Thread observes, never judges.

---

## Orphan handling

- LEFT JOIN everywhere — orphan rows participate in scoring
- Null dim attributes handled via COALESCE in display
- Orphan rate is a first-class `thread prime` signal
- `orphan_bead_count` surfaced in `mart_epic_summary`

Orphans are signal. Never exclude them.

---

## Table definitions

### fact_bead_events ✅

Source: `schema.go#L155-L170`
https://github.com/gastownhall/beads/blob/67249651/internal/storage/dolt/schema.go#L155-L170

```sql
CREATE TABLE fact_bead_events (
  issue_id    VARCHAR,    -- events.issue_id
  event_type  VARCHAR,    -- events.event_type (IS the field indicator)
  actor       VARCHAR,    -- events.actor ($USER or agent name)
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
  first_claimed_at          TIMESTAMP,  -- DERIVED: first dolt_diff_issues row where
                                        --   to_status = 'in_progress'
  first_closed_at           TIMESTAMP,  -- DERIVED: first dolt_diff_issues row where
                                        --   to_status = 'closed'
  final_closed_at           TIMESTAMP,  -- issues.closed_at
  time_to_start_secs        BIGINT,     -- DERIVED: first_claimed_at - created_at
  active_time_secs          BIGINT,     -- DERIVED: sum of in_progress windows
  total_elapsed_secs        BIGINT,     -- DERIVED: final_closed_at - created_at
  reopen_count              INT,        -- DERIVED: count closed→open in dolt_diff_issues
  agent_actor_count         INT,        -- DERIVED: distinct agents from fact_bead_events
                                        -- reliability depends on actor classification tier
  compaction_level          INT,        -- issues.compaction_level (native)
  compacted_at              TIMESTAMP,  -- issues.compacted_at (native)
  validation_count          INT,        -- DERIVED from issues.validations
  revision_requested_count  INT,        -- DERIVED: outcome='revision_requested'
  rejected_count            INT,        -- DERIVED: outcome='rejected'
  quality_score             DOUBLE,     -- issues.quality_score (nullable, Refinery)
  crystallizes              BOOLEAN,    -- issues.crystallizes
  has_derived_fields        BOOLEAN,    -- flag: any DERIVED field used inference

  -- Actor role tracking — creator vs closer serve different analytical purposes
  creator_actor_key         VARCHAR,    -- FK → dim_actor.actor_key
                                        -- from issues.created_by
                                        -- scoping accountability signal
  closer_actor_key          VARCHAR     -- FK → dim_actor.actor_key
                                        -- from dolt_diff_issues commit author
                                        --   at first to_status='closed' transition
                                        -- execution accountability signal
                                        -- if agent closed it, agent executed it

  -- REMOVED: closed_by_session — never populated by current bd versions
);
```

**Extraction source:** Use `dolt_diff_issues` (confirmed working in embedded mode)
for all DERIVED fields.

---

### fact_dep_activity ✅

Source: `schema.go#L98-L115`, `types.go#L622-L635`
https://github.com/gastownhall/beads/blob/67249651/internal/storage/dolt/schema.go#L98-L115
https://github.com/steveyegge/beads/blob/67249651/internal/types/types.go#L622-L635

```sql
CREATE TABLE fact_dep_activity (
  issue_id          VARCHAR,    -- dependencies.issue_id
  depends_on_id     VARCHAR,    -- dependencies.depends_on_id
                                -- may be 'external:project:capability'
  dep_type          VARCHAR,    -- dependencies.type
  dep_category      VARCHAR,    -- DERIVED: 'workflow'/'association'/'hop'/'reference'
  dep_event         VARCHAR,    -- 'added' or 'removed'
                                -- from dolt_diff_dependencies.diff_type
                                -- fallback: infer from absence
  created_at        TIMESTAMP,  -- dependencies.created_at (native)
  created_by        VARCHAR,    -- dependencies.created_by (native)
  after_first_claim BOOLEAN,    -- DERIVED: created_at > first_claimed_at
  is_replan         BOOLEAN     -- DERIVED: remove+add same pair within 60s
);
```

Dep activity is forensic signal only. Never a scoring penalty.

---

### fact_conflicts ❌ REMOVED

Do not implement.

---

### dim_bead ✅

Source: `schema.go#L11-L95`, `types.go#L15-L133`
https://github.com/gastownhall/beads/blob/67249651/internal/storage/dolt/schema.go#L11-L95
https://github.com/steveyegge/beads/blob/67249651/internal/types/types.go#L15-L133

```sql
CREATE TABLE dim_bead (
  issue_id                VARCHAR PRIMARY KEY,
  title                   VARCHAR,
  issue_type              VARCHAR,
  priority                INT,
  created_by              VARCHAR,    -- $USER in solo mode regardless of agent involvement
  owner                   VARCHAR,    -- email in solo mode
  assignee                VARCHAR,
  estimated_minutes       INT,        -- nullable — null = no upfront scope estimate
  has_description         BOOLEAN,    -- DERIVED: description != ''
  has_acceptance_criteria BOOLEAN,    -- DERIVED: acceptance_criteria != ''
  has_design              BOOLEAN,    -- DERIVED: design != ''
  quality_score           DOUBLE,     -- nullable, Refinery-set
  crystallizes            BOOLEAN,
  source_system           VARCHAR,
  is_template             BOOLEAN
);
```

---

### dim_hierarchy ✅

Source: `cmd/bd/dep.go#L29-L42`
https://github.com/steveyegge/beads/blob/67249651/cmd/bd/dep.go#L29-L42

```sql
CREATE TABLE dim_hierarchy (
  issue_id   VARCHAR,
  parent_id  VARCHAR,    -- null if root/epic
  root_id    VARCHAR,    -- = issue_id if depth = 0
  depth      INT,        -- 0=epic, 1=task, 2=subtask
  is_root    BOOLEAN,
  path       VARCHAR     -- 'bd-a3f8/bd-a3f8.1/bd-a3f8.1.2'
                         -- enables: WHERE path LIKE 'bd-a3f8%'
);
```

Hierarchy from ID string parsing (primary) + dep edges (validation fallback).
Beads enforces acyclic tree — no cycle detection needed.

---

### dim_actor ✅ HIGH VOLATILITY

Source: `types.go#L110-L115`, `types.go#L1107-L1132`
https://github.com/steveyegge/beads/blob/67249651/internal/types/types.go#L110-L115
https://github.com/steveyegge/beads/blob/67249651/internal/types/types.go#L1107-L1132

```sql
CREATE TABLE dim_actor (
  actor_key             VARCHAR PRIMARY KEY,  -- raw actor string OR hop:// URI
  actor_name            VARCHAR,
  platform              VARCHAR,              -- EntityRef.Platform if hop:// URI
  org                   VARCHAR,              -- EntityRef.Org if hop:// URI
  role_type             VARCHAR,              -- Gas Town only
  rig                   VARCHAR,              -- Gas Town only
  actor_class           VARCHAR,              -- 'agent' / 'human' / 'unknown'
  classification_source VARCHAR               -- 'hop_uri' / 'role_type' / 'session' /
                                              -- 'agent_state' / 'heuristic' / 'unknown'
);
```

One row per unique actor. The `fact_bead_lifecycle` table carries two FK references
(`creator_actor_key`, `closer_actor_key`) for role-based analysis.

Classification logic in `actor_classifier.py` — see cascade above.
Heuristic is the PRIMARY path for solo users, not a fallback.

---

## View definitions

### v_bead_scores

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
  f.creator_actor_key,
  f.closer_actor_key,

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

  -- agent closure rate — what % of beads were closed by an agent
  ROUND(
    COUNT(DISTINCT CASE WHEN ca.actor_class = 'agent'
      THEN h.issue_id END) * 1.0
    / NULLIF(COUNT(DISTINCT h.issue_id), 0)
  , 2)                                                AS agent_closure_rate,

  COALESCE(SUM(da.post_claim_dep_events), 0)          AS post_claim_dep_events,
  COALESCE(SUM(da.replan_events), 0)                  AS replan_events,

  ROUND(AVG(f.quality_score), 2)                     AS avg_quality_score,

  SUM(f.has_derived_fields::INT)                      AS beads_with_inferred_data,
  COUNT(DISTINCT CASE WHEN epic.issue_id IS NULL
    THEN h.issue_id END)                              AS orphan_bead_count

FROM dim_hierarchy h
LEFT JOIN dim_bead epic       ON epic.issue_id = h.root_id
JOIN v_bead_scores s          ON s.issue_id = h.issue_id
JOIN fact_bead_lifecycle f    ON f.issue_id = h.issue_id
LEFT JOIN dim_actor ca        ON ca.actor_key = f.closer_actor_key
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

```sql
CREATE VIEW v_weekly_trends AS
SELECT
  DATE_TRUNC('week', f.final_closed_at)   AS week,
  COUNT(*)                                 AS beads_closed,
  ROUND(AVG(s.fidelity_score), 2)         AS avg_fidelity,
  ROUND(AVG(s.effort_score), 2)           AS avg_effort,
  SUM(f.reopen_count)                     AS total_reopens,
  SUM(f.compaction_level)                 AS total_compactions,
  ROUND(
    COUNT(CASE WHEN ca.actor_class = 'agent' THEN 1 END) * 1.0
    / NULLIF(COUNT(*), 0)
  , 2)                                    AS agent_closure_rate
FROM fact_bead_lifecycle f
JOIN v_bead_scores s       USING (issue_id)
LEFT JOIN dim_actor ca     ON ca.actor_key = f.closer_actor_key
WHERE f.final_closed_at IS NOT NULL
GROUP BY DATE_TRUNC('week', f.final_closed_at)
ORDER BY week;
```

Use case: track agent closure rate over time — are you becoming more agentic?

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
  agent_closure_rate,
  epic_had_criteria,
  avg_quality_score
FROM mart_epic_summary
WHERE epic_crystallizes = true
ORDER BY total_fidelity_penalty DESC
LIMIT 20;
```

---

## Extraction order

1. `dim_bead` — from `issues` table
2. `dim_hierarchy` — from `dependencies` type='parent-child' + ID string parsing
3. `dim_actor` — from `events.actor` + Creator EntityRef (see `actor_classifier.py`)
4. `fact_bead_events` — from `events` table
5. `fact_bead_lifecycle` — DERIVED from `dolt_diff_issues` status transitions
   - includes `creator_actor_key` and `closer_actor_key` FK lookups to `dim_actor`
6. `fact_dep_activity` — from `dolt_diff_dependencies` + `dependencies.created_at`

Views after all tables:
1. `v_bead_scores`
2. `v_bead_dep_activity`
3. `mart_epic_summary`
4. `v_weekly_trends`

---

## Consumer separation

| consumer | surface | format |
|---|---|---|
| agents | `thread prime --json` | machine-readable JSON |
| humans (quick glance) | `thread report` | self-contained HTML |
| humans (deep analysis) | `thread.duckdb` direct | SQL / BI tools |
| analysts / warehouses | `thread export` | Parquet |

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
- Pending real-world usage signal

---

## Repo

`jklenk/thread` — MIT License

---

_v0.7 — April 2026_

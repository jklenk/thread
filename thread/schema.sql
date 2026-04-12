-- Thread v1 DuckDB schema
-- All table and view definitions for the analytical layer.
-- Matches DESIGN_v0.7.md. Execute in order: tables first, then views.

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_bead (
  issue_id                VARCHAR PRIMARY KEY,
  title                   VARCHAR,
  issue_type              VARCHAR,
  priority                INT,
  created_by              VARCHAR,
  owner                   VARCHAR,
  assignee                VARCHAR,
  estimated_minutes       INT,
  has_description         BOOLEAN,
  has_acceptance_criteria BOOLEAN,
  has_design              BOOLEAN,
  quality_score           DOUBLE,
  crystallizes            BOOLEAN,
  source_system           VARCHAR,
  is_template             BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_hierarchy (
  issue_id   VARCHAR,
  parent_id  VARCHAR,
  root_id    VARCHAR,
  depth      INT,
  is_root    BOOLEAN,
  path       VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_actor (
  actor_key             VARCHAR PRIMARY KEY,
  actor_name            VARCHAR,
  platform              VARCHAR,
  org                   VARCHAR,
  role_type             VARCHAR,
  rig                   VARCHAR,
  actor_class           VARCHAR,
  classification_source VARCHAR
);

-- ============================================================
-- FACT TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_bead_events (
  issue_id    VARCHAR,
  event_type  VARCHAR,
  actor       VARCHAR,
  old_value   VARCHAR,
  new_value   VARCHAR,
  created_at  TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_bead_lifecycle (
  issue_id                  VARCHAR,
  created_at                TIMESTAMP,
  first_claimed_at          TIMESTAMP,
  first_closed_at           TIMESTAMP,
  final_closed_at           TIMESTAMP,
  time_to_start_secs        BIGINT,
  active_time_secs          BIGINT,
  total_elapsed_secs        BIGINT,
  reopen_count              INT,
  agent_actor_count         INT,
  compaction_level          INT,
  compacted_at              TIMESTAMP,
  validation_count          INT,
  revision_requested_count  INT,
  rejected_count            INT,
  quality_score             DOUBLE,
  crystallizes              BOOLEAN,
  has_derived_fields        BOOLEAN,
  creator_actor_key         VARCHAR,
  closer_actor_key          VARCHAR
);

CREATE TABLE IF NOT EXISTS fact_dep_activity (
  issue_id          VARCHAR,
  depends_on_id     VARCHAR,
  dep_type          VARCHAR,
  dep_category      VARCHAR,
  dep_event         VARCHAR,
  created_at        TIMESTAMP,
  created_by        VARCHAR,
  after_first_claim BOOLEAN,
  is_replan         BOOLEAN
);

-- ============================================================
-- VIEWS
-- ============================================================

-- v_bead_scores — v2: replaces flatlined fidelity/effort formulas.
-- fidelity_score: compliance-based (1.0 if claimed before close, documented,
--   no dep violations; penalties for skipping workflow steps).
-- effort_score: relative cost multiple (active_time / project median).
-- base_cost_hours: retained as throughput signal.
CREATE OR REPLACE VIEW v_bead_scores AS
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

  ROUND(COALESCE(f.active_time_secs, 0) / 3600.0, 2)   AS base_cost_hours,

  -- fidelity_score v2: compliance-based, penalises workflow shortcuts
  ROUND(
    1.0
    - (CASE WHEN f.first_claimed_at IS NULL AND f.final_closed_at IS NOT NULL
            THEN 0.4 ELSE 0 END)             -- skip-claim penalty
    - (COALESCE(f.reopen_count, 0) * 0.3)    -- reopen penalty (still relevant)
    - (COALESCE(f.rejected_count, 0) * 0.2)  -- rejection penalty
  , 2)                                                    AS fidelity_score,

  -- effort_score v2: relative cost multiple (active_time / project p50)
  -- Falls back to 0.5 when median is unavailable (single-bead projects)
  ROUND(
    COALESCE(
      f.active_time_secs * 1.0
      / NULLIF((SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY active_time_secs)
                FROM fact_bead_lifecycle WHERE final_closed_at IS NOT NULL), 0),
      0.5
    )
  , 2)                                                    AS effort_score

FROM fact_bead_lifecycle f;


CREATE OR REPLACE VIEW v_bead_dep_activity AS
SELECT
  issue_id,
  COUNT(*)                                            AS total_dep_events,
  SUM(CAST(after_first_claim AS INT))                 AS post_claim_dep_events,
  SUM(CAST(is_replan AS INT))                         AS replan_events,
  COUNT(DISTINCT dep_type)                            AS dep_type_variety
FROM fact_dep_activity
WHERE dep_category = 'workflow'
GROUP BY issue_id;


CREATE OR REPLACE VIEW mart_epic_summary AS
SELECT
  h.root_id                                           AS epic_id,
  epic.title                                          AS epic_title,
  epic.issue_type                                     AS epic_type,
  epic.priority                                       AS epic_priority,
  epic.estimated_minutes                              AS epic_estimated_minutes,
  epic.has_acceptance_criteria                        AS epic_had_criteria,
  epic.has_design                                     AS epic_had_design,
  epic.crystallizes                                   AS epic_crystallizes,
  epic.is_template                                    AS epic_is_template,

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
  -- total_effort_penalty = fidelity failures only (reopens, revisions, rejections)
  -- Replaces the old total_fidelity_penalty which degenerated to a bead count proxy
  -- on clean agentic projects. See DESIGN_v0.8 §Change 2.
  ROUND(SUM(
    (COALESCE(f.reopen_count, 0) * 2.0)
    + (COALESCE(f.revision_requested_count, 0) * 1.5)
    + (COALESCE(f.rejected_count, 0) * 1.0)
  ), 2)                                               AS total_effort_penalty,
  SUM(f.compaction_level)                             AS total_compactions,
  SUM(f.agent_actor_count)                            AS total_agent_touches,

  -- Elapsed vs estimate — the real cost signal for iterative workflows
  -- where beads are never reopened. See DESIGN_v0.8 §Change 3.
  MIN(f.created_at)                                   AS epic_started_at,
  MAX(f.final_closed_at)                              AS epic_completed_at,
  ROUND(
    DATE_DIFF('second', MIN(f.created_at), MAX(f.final_closed_at))
    / 60.0
  , 0)                                                AS epic_elapsed_minutes,
  ROUND(
    (DATE_DIFF('second', MIN(f.created_at), MAX(f.final_closed_at)) / 60.0)
    / NULLIF(epic.estimated_minutes, 0)
  , 2)                                                AS elapsed_vs_estimate_ratio,

  ROUND(
    COUNT(DISTINCT CASE WHEN ca.actor_class = 'agent'
      THEN h.issue_id END) * 1.0
    / NULLIF(COUNT(DISTINCT h.issue_id), 0)
  , 2)                                                AS agent_closure_rate,

  COALESCE(SUM(da.post_claim_dep_events), 0)          AS post_claim_dep_events,
  COALESCE(SUM(da.replan_events), 0)                  AS replan_events,

  ROUND(AVG(f.quality_score), 2)                     AS avg_quality_score,

  SUM(CAST(f.has_derived_fields AS INT))              AS beads_with_inferred_data,
  COUNT(DISTINCT CASE WHEN epic.issue_id IS NULL
    THEN h.issue_id END)                              AS orphan_bead_count

FROM dim_hierarchy h
LEFT JOIN dim_bead epic       ON epic.issue_id = h.root_id
LEFT JOIN v_bead_scores s     ON s.issue_id = h.issue_id
LEFT JOIN fact_bead_lifecycle f ON f.issue_id = h.issue_id
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
  epic.is_template,
  epic.issue_id;


-- Project-level rollup that works regardless of whether epics exist.
-- Flat singleton workflows (no epics) need this because mart_epic_summary
-- returns one row per bead with bead_count=1. See DESIGN_v0.8 §Change 4.
CREATE OR REPLACE VIEW mart_project_summary AS
SELECT
  COUNT(DISTINCT b.issue_id)                          AS total_beads,

  -- singletons: depth=0 with no children
  COUNT(DISTINCT CASE
    WHEN h.depth = 0
    AND NOT EXISTS (
      SELECT 1 FROM dim_hierarchy c WHERE c.parent_id = h.issue_id
    )
    THEN b.issue_id END)                              AS singleton_bead_count,

  -- epics: depth=0 with at least one child
  COUNT(DISTINCT CASE
    WHEN h.depth = 0
    AND EXISTS (
      SELECT 1 FROM dim_hierarchy c WHERE c.parent_id = h.issue_id
    )
    THEN b.issue_id END)                              AS epic_count,

  ROUND(AVG(s.fidelity_score), 2)                    AS avg_fidelity_score,
  ROUND(AVG(s.effort_score), 2)                      AS avg_effort_score,
  SUM(f.reopen_count)                                 AS total_reopens,
  SUM(f.compaction_level)                             AS total_compactions,
  ROUND(
    COUNT(CASE WHEN ca.actor_class = 'agent' THEN 1 END) * 1.0
    / NULLIF(COUNT(*), 0)
  , 2)                                                AS agent_closure_rate,
  SUM(CAST(f.has_derived_fields AS INT))              AS beads_with_inferred_data
FROM dim_bead b
LEFT JOIN dim_hierarchy h       ON h.issue_id = b.issue_id
LEFT JOIN v_bead_scores s       ON s.issue_id = b.issue_id
LEFT JOIN fact_bead_lifecycle f ON f.issue_id = b.issue_id
LEFT JOIN dim_actor ca          ON ca.actor_key = f.closer_actor_key
WHERE b.is_template = false OR b.is_template IS NULL;


CREATE OR REPLACE VIEW v_weekly_trends AS
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
LEFT JOIN v_bead_scores s   ON s.issue_id = f.issue_id
LEFT JOIN dim_actor ca      ON ca.actor_key = f.closer_actor_key
WHERE f.final_closed_at IS NOT NULL
GROUP BY DATE_TRUNC('week', f.final_closed_at)
ORDER BY week;


-- ============================================================
-- THREAD v2 — SESSIONS, INTERACTIONS, COMPLIANCE, CORRELATIONS
-- ============================================================
-- Added by Thread v2 Phase 1 (strands-ef3). Tables and views
-- backing session detection, interactions audit trail, behavioral
-- compliance signals, queue wait, spec/priority/type correlations,
-- daily trends, session-level compliance rollup, and agent memories.

-- ============================================================
-- v2 DIMENSION / FACT TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_session (
  session_id    VARCHAR PRIMARY KEY,
  started_at    TIMESTAMP,
  ended_at      TIMESTAMP,
  duration_secs BIGINT,
  bead_count    INT,
  beads_closed  INT,
  beads_open    INT,
  epics_touched INT,
  actor_key     VARCHAR,
  issue_types   VARCHAR
);

CREATE TABLE IF NOT EXISTS bridge_session_bead (
  session_id VARCHAR,
  issue_id   VARCHAR
);

CREATE TABLE IF NOT EXISTS fact_interactions (
  interaction_id  VARCHAR PRIMARY KEY,
  kind            VARCHAR,
  created_at      TIMESTAMP,
  actor           VARCHAR,
  issue_id        VARCHAR,
  model           VARCHAR,
  prompt_length   INT,
  response_length INT,
  error           VARCHAR,
  tool_name       VARCHAR,
  exit_code       INT,
  parent_id       VARCHAR,
  label           VARCHAR,
  reason          VARCHAR,
  extra_json      VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_agent_memory (
  memory_key   VARCHAR PRIMARY KEY,
  memory_value VARCHAR,
  extracted_at TIMESTAMP
);

-- ============================================================
-- v2 VIEWS
-- ============================================================

-- Session summary with efficiency metrics.
-- LEFT JOIN bridge so sessions with no linked beads still appear.
CREATE OR REPLACE VIEW mart_session_summary AS
SELECT
  s.session_id,
  s.started_at,
  s.ended_at,
  s.duration_secs,
  s.bead_count,
  s.beads_closed,
  s.beads_open,
  s.epics_touched,
  s.issue_types,
  ROUND(
    SUM(COALESCE(f.active_time_secs, 0)) * 1.0
    / NULLIF(s.duration_secs, 0)
  , 2)                                                          AS parallelism_ratio,
  ROUND(AVG(f.total_elapsed_secs), 0)                           AS avg_cycle_time_secs,
  ROUND(
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.total_elapsed_secs)
  , 0)                                                          AS median_cycle_time_secs,
  ROUND(
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY f.total_elapsed_secs)
  , 0)                                                          AS p90_cycle_time_secs,
  COALESCE(SUM(da.post_claim_dep_events), 0)                    AS scope_changes
FROM dim_session s
LEFT JOIN bridge_session_bead sb   ON sb.session_id = s.session_id
LEFT JOIN fact_bead_lifecycle f    ON f.issue_id = sb.issue_id
LEFT JOIN v_bead_dep_activity da   ON da.issue_id = sb.issue_id
GROUP BY s.session_id, s.started_at, s.ended_at, s.duration_secs,
         s.bead_count, s.beads_closed, s.beads_open, s.epics_touched, s.issue_types;


-- Daily trends — replaces the weekly default for report/prime.
-- v_weekly_trends is kept alongside for anyone using it directly.
CREATE OR REPLACE VIEW v_daily_trends AS
SELECT
  DATE_TRUNC('day', f.final_closed_at)                          AS day,
  COUNT(*)                                                      AS beads_closed,
  ROUND(
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.total_elapsed_secs)
  , 0)                                                          AS median_cycle_time_secs,
  ROUND(
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY f.total_elapsed_secs)
  , 0)                                                          AS p90_cycle_time_secs,
  ROUND(
    COUNT(CASE WHEN ca.actor_class = 'agent' THEN 1 END) * 1.0
    / NULLIF(COUNT(*), 0)
  , 2)                                                          AS agent_closure_rate
FROM fact_bead_lifecycle f
LEFT JOIN dim_actor ca ON ca.actor_key = f.closer_actor_key
WHERE f.final_closed_at IS NOT NULL
GROUP BY DATE_TRUNC('day', f.final_closed_at)
ORDER BY day;


-- Interactions: breakdown by kind
CREATE OR REPLACE VIEW v_interaction_summary AS
SELECT
  kind,
  COUNT(*)                          AS count,
  COUNT(DISTINCT issue_id)          AS beads_touched,
  COUNT(DISTINCT actor)             AS actors,
  MIN(created_at)                   AS first_at,
  MAX(created_at)                   AS last_at
FROM fact_interactions
GROUP BY kind;


-- Interactions: LLM model usage (populated when llm_call records exist)
CREATE OR REPLACE VIEW v_model_usage AS
SELECT
  model,
  COUNT(*)                                                      AS calls,
  AVG(prompt_length)                                            AS avg_prompt_chars,
  AVG(response_length)                                          AS avg_response_chars,
  COUNT(CASE WHEN error IS NOT NULL THEN 1 END)                 AS errors,
  COUNT(DISTINCT issue_id)                                      AS beads_touched
FROM fact_interactions
WHERE kind = 'llm_call' AND model IS NOT NULL
GROUP BY model;


-- Interactions: tool usage (populated when tool_call records exist)
CREATE OR REPLACE VIEW v_tool_usage AS
SELECT
  tool_name,
  COUNT(*)                                                      AS calls,
  COUNT(CASE WHEN exit_code = 0 THEN 1 END)                     AS successes,
  COUNT(CASE WHEN exit_code != 0 OR error IS NOT NULL THEN 1 END) AS failures,
  ROUND(
    COUNT(CASE WHEN exit_code = 0 THEN 1 END) * 100.0
    / NULLIF(COUNT(*), 0)
  , 1)                                                          AS success_rate,
  COUNT(DISTINCT issue_id)                                      AS beads_touched
FROM fact_interactions
WHERE kind = 'tool_call' AND tool_name IS NOT NULL
GROUP BY tool_name
ORDER BY calls DESC;


-- Interactions: hourly heatmap (day-of-week x hour)
CREATE OR REPLACE VIEW v_interaction_hourly AS
SELECT
  EXTRACT(DOW FROM created_at)      AS day_of_week,
  EXTRACT(HOUR FROM created_at)     AS hour_of_day,
  COUNT(*)                          AS interactions,
  COUNT(DISTINCT issue_id)          AS beads_touched
FROM fact_interactions
GROUP BY EXTRACT(DOW FROM created_at), EXTRACT(HOUR FROM created_at);


-- Interactions: status transition breakdown (parses extra_json)
CREATE OR REPLACE VIEW v_status_transitions AS
SELECT
  json_extract_string(extra_json, '$.old_value') AS from_status,
  json_extract_string(extra_json, '$.new_value') AS to_status,
  COUNT(*)                                       AS count,
  COUNT(DISTINCT issue_id)                       AS beads
FROM fact_interactions
WHERE kind = 'field_change'
  AND json_extract_string(extra_json, '$.field') = 'status'
GROUP BY from_status, to_status
ORDER BY count DESC;


-- Interactions: non-trivial close reasons (effective commit messages for beads)
CREATE OR REPLACE VIEW v_close_reasons AS
SELECT
  i.issue_id,
  b.title                                        AS bead_title,
  json_extract_string(i.extra_json, '$.reason')  AS close_reason,
  i.created_at
FROM fact_interactions i
LEFT JOIN dim_bead b ON b.issue_id = i.issue_id
WHERE i.kind = 'field_change'
  AND json_extract_string(i.extra_json, '$.new_value') = 'closed'
  AND json_extract_string(i.extra_json, '$.reason') IS NOT NULL
  AND json_extract_string(i.extra_json, '$.reason') != 'Closed'
ORDER BY i.created_at;


-- Interactions: daily activity spans
CREATE OR REPLACE VIEW v_daily_activity AS
SELECT
  CAST(created_at AS DATE)                                       AS day,
  MIN(created_at)                                                AS first_activity,
  MAX(created_at)                                                AS last_activity,
  DATE_DIFF('second', MIN(created_at), MAX(created_at))          AS span_secs,
  COUNT(*)                                                       AS interactions,
  COUNT(DISTINCT issue_id)                                       AS beads_touched,
  COUNT(DISTINCT actor)                                          AS actors
FROM fact_interactions
GROUP BY CAST(created_at AS DATE)
ORDER BY day;


-- Interactions: inter-close gaps for cadence / burst analysis
CREATE OR REPLACE VIEW v_close_velocity AS
SELECT
  issue_id,
  created_at,
  LAG(created_at) OVER (ORDER BY created_at)                     AS prev_close_at,
  DATE_DIFF(
    'second',
    LAG(created_at) OVER (ORDER BY created_at),
    created_at
  )                                                              AS gap_secs
FROM fact_interactions
WHERE kind = 'field_change'
  AND json_extract_string(extra_json, '$.new_value') = 'closed'
ORDER BY created_at;


-- Compliance: dependency order violations (blocked bead closed before its blocker).
-- LEFT JOIN on f1 so empty-table runs return 0 rows cleanly.
CREATE OR REPLACE VIEW v_dep_order_violations AS
SELECT
  d.issue_id                 AS blocked_bead,
  b1.title                   AS blocked_title,
  d.depends_on_id            AS blocker_bead,
  b2.title                   AS blocker_title,
  f1.final_closed_at         AS blocked_closed_at,
  f2.final_closed_at         AS blocker_closed_at
FROM fact_dep_activity d
LEFT JOIN fact_bead_lifecycle f1 ON f1.issue_id = d.issue_id
LEFT JOIN fact_bead_lifecycle f2 ON f2.issue_id = d.depends_on_id
LEFT JOIN dim_bead b1            ON b1.issue_id = d.issue_id
LEFT JOIN dim_bead b2            ON b2.issue_id = d.depends_on_id
WHERE d.dep_type = 'blocks'
  AND d.dep_event = 'added'
  AND f1.final_closed_at IS NOT NULL
  AND (f2.final_closed_at IS NULL OR f2.final_closed_at > f1.final_closed_at);


-- Compliance: title vs close reason pairs for human alignment review
CREATE OR REPLACE VIEW v_title_reason_pairs AS
SELECT
  i.issue_id,
  b.title,
  json_extract_string(i.extra_json, '$.reason')  AS close_reason,
  i.created_at
FROM fact_interactions i
LEFT JOIN dim_bead b ON b.issue_id = i.issue_id
WHERE i.kind = 'field_change'
  AND json_extract_string(i.extra_json, '$.new_value') = 'closed'
  AND json_extract_string(i.extra_json, '$.reason') IS NOT NULL
  AND json_extract_string(i.extra_json, '$.reason') != 'Closed'
  AND b.title IS NOT NULL
ORDER BY i.created_at;


-- Queue wait: time-to-start for beads that were claimed
CREATE OR REPLACE VIEW v_queue_wait AS
SELECT
  f.issue_id,
  f.time_to_start_secs,
  b.issue_type,
  b.priority
FROM fact_bead_lifecycle f
LEFT JOIN dim_bead b ON b.issue_id = f.issue_id
WHERE f.first_claimed_at IS NOT NULL;


-- Correlation: does specification depth correlate with outcomes?
CREATE OR REPLACE VIEW v_spec_quality_correlation AS
SELECT
  CASE
    WHEN b.has_design              THEN 'design + description'
    WHEN b.has_acceptance_criteria THEN 'acceptance criteria'
    WHEN b.has_description         THEN 'description only'
    ELSE 'no spec'
  END                                                             AS spec_level,
  COUNT(*)                                                        AS bead_count,
  COUNT(CASE WHEN f.final_closed_at IS NOT NULL THEN 1 END)       AS completed,
  ROUND(
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.total_elapsed_secs)
  , 0)                                                            AS median_cycle_secs,
  ROUND(AVG(f.total_elapsed_secs), 0)                             AS avg_cycle_secs,
  ROUND(AVG(f.time_to_start_secs), 0)                             AS avg_wait_secs
FROM dim_bead b
LEFT JOIN fact_bead_lifecycle f ON f.issue_id = b.issue_id
GROUP BY spec_level
ORDER BY median_cycle_secs;


-- Correlation: does priority drive speed?
CREATE OR REPLACE VIEW v_priority_performance AS
SELECT
  b.priority,
  COUNT(*)                                                        AS bead_count,
  COUNT(CASE WHEN f.final_closed_at IS NOT NULL THEN 1 END)       AS completed,
  ROUND(
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.total_elapsed_secs)
  , 0)                                                            AS median_cycle_secs,
  ROUND(
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY f.total_elapsed_secs)
  , 0)                                                            AS p90_cycle_secs,
  ROUND(AVG(f.time_to_start_secs), 0)                             AS avg_wait_secs
FROM dim_bead b
LEFT JOIN fact_bead_lifecycle f ON f.issue_id = b.issue_id
GROUP BY b.priority
ORDER BY b.priority;


-- Correlation: cycle time by issue_type (tasks vs features vs bugs vs epics)
CREATE OR REPLACE VIEW v_type_performance AS
SELECT
  b.issue_type,
  COUNT(*)                                                        AS bead_count,
  COUNT(CASE WHEN f.final_closed_at IS NOT NULL THEN 1 END)       AS completed,
  ROUND(
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.total_elapsed_secs)
  , 0)                                                            AS median_cycle_secs,
  ROUND(
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY f.total_elapsed_secs)
  , 0)                                                            AS p90_cycle_secs,
  ROUND(AVG(da.post_claim_dep_events), 2)                         AS avg_scope_changes
FROM dim_bead b
LEFT JOIN fact_bead_lifecycle f  ON f.issue_id = b.issue_id
LEFT JOIN v_bead_dep_activity da ON da.issue_id = b.issue_id
GROUP BY b.issue_type
ORDER BY median_cycle_secs;


-- Compliance: late-add beads (created and claimed within 5s mid-session)
-- A bead is "late-add" if it was created + immediately claimed while work was
-- already underway in its session (at least one prior close already happened).
CREATE OR REPLACE VIEW v_late_add_beads AS
SELECT
  f.issue_id,
  b.title,
  f.created_at,
  f.first_claimed_at,
  f.time_to_start_secs,
  sb.session_id,
  -- How many beads were already closed in this session before this one was created?
  (SELECT COUNT(*)
   FROM bridge_session_bead sb2
   JOIN fact_bead_lifecycle f2 ON f2.issue_id = sb2.issue_id
   WHERE sb2.session_id = sb.session_id
     AND f2.final_closed_at IS NOT NULL
     AND f2.final_closed_at < f.created_at
  )                                                                   AS prior_closes_in_session
FROM fact_bead_lifecycle f
JOIN dim_bead b             ON b.issue_id = f.issue_id
LEFT JOIN bridge_session_bead sb ON sb.issue_id = f.issue_id
WHERE f.time_to_start_secs IS NOT NULL
  AND f.time_to_start_secs < 5;


-- Compliance: blockers added after work was already claimed
-- These are reactive dependencies that emerged mid-execution.
CREATE OR REPLACE VIEW v_late_add_blockers AS
SELECT
  da.issue_id          AS blocked_bead,
  b1.title             AS blocked_title,
  da.depends_on_id     AS blocker_bead,
  b2.title             AS blocker_title,
  da.created_at        AS added_at,
  f.first_claimed_at   AS blocked_claimed_at
FROM fact_dep_activity da
JOIN fact_bead_lifecycle f  ON f.issue_id = da.issue_id
LEFT JOIN dim_bead b1       ON b1.issue_id = da.issue_id
LEFT JOIN dim_bead b2       ON b2.issue_id = da.depends_on_id
WHERE da.dep_event = 'added'
  AND da.after_first_claim = true
  AND da.dep_type = 'blocks';


-- Compliance: per-session rollup (skip-claim, documentation, dep violations)
CREATE OR REPLACE VIEW v_session_compliance AS
SELECT
  sb.session_id,
  COUNT(DISTINCT sb.issue_id)                                     AS beads_in_session,
  COUNT(DISTINCT CASE
    WHEN f.first_claimed_at IS NULL AND f.final_closed_at IS NOT NULL
    THEN sb.issue_id
  END)                                                            AS skip_claim_count,
  COUNT(DISTINCT CASE
    WHEN cr.close_reason IS NOT NULL THEN sb.issue_id
  END)                                                            AS documented_closes,
  COUNT(DISTINCT dv.blocked_bead)                                 AS dep_violations
FROM bridge_session_bead sb
LEFT JOIN fact_bead_lifecycle f    ON f.issue_id = sb.issue_id
LEFT JOIN v_close_reasons cr       ON cr.issue_id = sb.issue_id
LEFT JOIN v_dep_order_violations dv ON dv.blocked_bead = sb.issue_id
GROUP BY sb.session_id;

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

  -- base_cost_hours retained as throughput signal (not in effort_score)
  ROUND(COALESCE(f.active_time_secs, 0) / 3600.0, 2)   AS base_cost_hours,

  ROUND(
    1.0 - LEAST(1.0,
      (COALESCE(f.reopen_count, 0) * 0.4)
      + (COALESCE(f.revision_requested_count, 0) * 0.4)
      + (COALESCE(f.rejected_count, 0) * 0.2)
    ), 2
  )                                                       AS fidelity_score,

  -- effort_score is purely event-driven. Wall clock is not a cost signal
  -- in agentic workflows (see DESIGN_v0.8 §Change 1).
  ROUND(
    (COALESCE(f.reopen_count, 0) * 2.0)
    + (COALESCE(f.revision_requested_count, 0) * 1.5)
    + (COALESCE(f.rejected_count, 0) * 1.0)
    + (COALESCE(f.compaction_level, 0) * 1.0)
    + (COALESCE(f.agent_actor_count, 0) * 0.5)
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

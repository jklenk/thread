# How to Identify Agent vs Human Activity in Beads

## The identification signals, ranked by reliability

### Tier 1 — Explicit, high-confidence (Gas Town / server mode only)

| Signal | Field | How it works |
|---|---|---|
| `role_type` | `issues.role_type` | Set when the issue *is* an agent bead. Values like `"agent"`, `"role"`, `"rig"` |
| `rig` | `issues.rig` | The rig (model config) assigned to an agent bead |
| `agent_state` | `issues.agent_state` | Lifecycle state of an agent bead (`"running"`, etc.) |
| `closed_by_session` | `issues.closed_by_session` | Claude Code session ID that closed the issue — direct proof of agent execution |
| `hop://` URI | `events.actor` | Structured URI with platform/org/id. `platform=gastown` = agent |

These fields are only populated in Gas Town / server-mode deployments. For solo CLI users, Tier 1 is empty.

### Tier 2 — Behavioral heuristics (works for solo users)

This is where the real design challenge lives. The `actor` field is always a plain string (from `BEADS_ACTOR` or `git config user.name`). But we can look at *patterns of behavior* rather than explicit labels:

| Signal | Source | Reasoning |
|---|---|---|
| **Timing between events** | `events.created_at`, `dolt_diff_issues` | Agent sessions create->claim->close in tight bursts (seconds apart). Humans have gaps. Many issues go open->in_progress->closed within 2-3 minutes in agent-driven workflows. |
| **Batch closes** | `interactions.jsonl` | Multiple issues closed within seconds of each other — this is an agent running `bd close` in a loop, not a human |
| **`compaction_level > 0`** | `issues.compaction_level` | Compaction is triggered by context window pressure — an agent-specific concern. A compacted bead was almost certainly in an agent session |
| **Description specificity** | `issues.description` | Agent-created beads tend to have extremely detailed, implementation-level descriptions. Humans write goals; agents write specs. This is a heuristic, not a rule |
| **`close_reason` content** | `issues.close_reason` | Agent closures reference specific code artifacts. Human closures tend to be terse ("Closed", "Done") |
| **`sender` field** | `issues.sender` | Set for inter-agent messages — if populated, it's agent-to-agent communication |
| **`mol_type` / `work_type`** | `issues.mol_type`, `issues.work_type` | Molecule workflows (swarm, patrol, work) are agent-orchestrated |

### Tier 3 — Inferred from context (lowest confidence)

| Signal | Source | Reasoning |
|---|---|---|
| **`BEADS_ACTOR` vs `git user.name`** | `events.actor`, `issues.created_by` | If an agent is configured with a distinct `BEADS_ACTOR` (e.g., `"claude-agent"`), it differs from the human's git username. But nothing *forces* this. |
| **Non-interactive detection** | Not in DB | Beads detects CI/non-TTY environments at runtime but doesn't persist this to the database |

## Recommendation for `dim_actor` / `actor_classifier.py`

The classification should be a **cascade**, not a binary:

```
1. role_type != ''           -> agent   (source: 'role_type')
2. hop:// URI parsed         -> agent/human by platform (source: 'hop_uri')
3. closed_by_session != ''   -> agent closed this bead (source: 'session')
4. agent_state != ''         -> agent bead (source: 'agent_state')
5. behavioral heuristics     -> probable agent/human (source: 'heuristic')
6. else                      -> 'unknown' (source: 'unknown')
```

For the behavioral heuristic layer (step 5), the strongest solo-user signal is **event velocity** — the time delta between `created->claimed->closed`. An agent session compresses this to seconds/minutes. A human session has natural gaps.

## The honest answer for solo CLI users

In a typical solo-user dataset, **beads are created by a human but executed by an agent.** The human wrote the issues via `bd create`, and an AI agent (Claude Code) did the implementation and called `bd close`. We can infer this from close_reason content (code-level specificity) and event timing (tight bursts). But the database doesn't *say* that explicitly — it's inference.

This is the core design tension: for solo Beads users, `actor_classifier.py` is doing forensic inference, not reading labels. The `classification_source = 'heuristic'` tier is not a fallback — for most real-world users, it's the primary path. The design doc's isolation of this component is well-justified.

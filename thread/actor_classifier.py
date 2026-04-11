"""Actor classifier — isolated, high-volatility component.

Classifies actors as 'agent', 'human', or 'unknown' using a tiered cascade.
This module is expected to change as Gas Town adoption grows and more real-world
usage patterns are observed. Do not couple other modules to its internals.

Only the dim_actor extractor should import from this module.
"""

import re
from datetime import timedelta

# ============================================================
# Confidence ranking for classification sources
# Lower number = higher confidence
# Used by dim_actor extractor to pick best classification per actor
# ============================================================

CONFIDENCE_RANK = {
    "hop_uri": 0,
    "role_type": 1,
    "session": 2,
    "agent_state": 3,
    "heuristic": 4,
    "unknown": 5,
}

# ============================================================
# Thresholds — calibrated from sample-data analysis (April 2026)
# One project, 34 beads — treat as v1 hypotheses, not ground truth
# ============================================================

# Batch close: N+ closures within this window across all events = agent loop
# Sample evidence: 4 closures in 12 seconds at 15:14:49-15:15:01
BATCH_CLOSE_WINDOW_SECS = 12
BATCH_CLOSE_MIN_COUNT = 4

# Velocity burst: created→closed under this threshold = agent speed
# Sample evidence: multiple beads at 88-162 seconds total lifecycle
VELOCITY_BURST_SECS = 180

# Compaction: level >= this indicates agent context pressure
# Sample: all level 0 (no compaction), but schema supports it
COMPACTION_THRESHOLD = 1

# Human pace: gaps > this between consecutive events suggest human
# Sample: all sub-7-min lifecycles, so this didn't fire — but real
# human-paced work has gaps of minutes to hours
HUMAN_PACE_GAP_SECS = 300

# Agent molecule workflow types — agent-orchestrated patterns
AGENT_MOL_TYPES = frozenset({"swarm", "patrol", "work"})

# Close reason patterns that indicate agent-level specificity
# Agents reference specific code artifacts; humans write "Done" or "Closed"
AGENT_CLOSE_REASON_PATTERNS = [
    re.compile(r"\d+\s+\w*\s*tests?\s+(?:all\s+)?pass", re.IGNORECASE),
    re.compile(r"^(?:Added|Replaced|Updated|Split|Routing|Core|Implemented|Wired)\s+\w+", re.IGNORECASE),
    re.compile(r"(?:class|function|method|module|helper)\s+\w+", re.IGNORECASE),
]


# ============================================================
# Classification cascade
# ============================================================

def classify_actor(actor_string: str, issue: dict, events: list,
                   all_events: list) -> tuple[str, str]:
    """Classify an actor's role on a specific issue.

    Args:
        actor_string: The raw actor string (e.g. "joshua.klenk" or "hop://...")
        issue: The issue dict from Dolt
        events: Events for THIS issue only
        all_events: ALL events across all issues (for batch close detection)

    Returns:
        (actor_class, classification_source) tuple.
        actor_class: 'agent' | 'human' | 'unknown'
        classification_source: 'hop_uri' | 'role_type' | 'agent_state' |
                               'heuristic' | 'unknown'
    """
    # ── Tier 1: Gas Town explicit signals (high confidence) ──

    if _safe_str(issue, "role_type"):
        return "agent", "role_type"

    if _has_hop_uri(actor_string):
        return _classify_by_platform(actor_string), "hop_uri"

    if _safe_str(issue, "agent_state"):
        return "agent", "agent_state"

    # ── Tier 2: Behavioral heuristics (primary path for solo users) ──

    if _safe_str(issue, "sender"):
        return "agent", "heuristic"

    if _is_agent_mol_type(issue):
        return "agent", "heuristic"

    if _is_batch_close(events, all_events):
        return "agent", "heuristic"

    if _is_velocity_burst(issue, events):
        return "agent", "heuristic"

    if _has_compaction(issue):
        return "agent", "heuristic"

    if _has_agent_close_reason(issue):
        return "agent", "heuristic"

    # ── Tier 3: Human inference ──

    if _has_human_paced_gaps(events):
        return "human", "heuristic"

    # ── Tier 4: Unknown ──
    return "unknown", "unknown"


# ============================================================
# Tier 1 helpers
# ============================================================

def _safe_str(row: dict, key: str) -> str:
    """Return non-empty string or empty string."""
    val = row.get(key, "")
    return val if val else ""


def _has_hop_uri(actor_string: str) -> bool:
    return actor_string.startswith("hop://")


def _classify_by_platform(actor_string: str) -> str:
    """Classify by hop:// URI platform segment."""
    # hop://platform/org/id
    parts = actor_string.replace("hop://", "").split("/")
    if parts and parts[0] == "gastown":
        return "agent"
    return "unknown"


# ============================================================
# Tier 2 helpers
# ============================================================

def _is_agent_mol_type(issue: dict) -> bool:
    mol_type = _safe_str(issue, "mol_type")
    return mol_type in AGENT_MOL_TYPES


def _is_batch_close(events: list, all_events: list) -> bool:
    """Detect batch close pattern: N+ closures within window across all events."""
    close_times = sorted(
        e["created_at"] for e in all_events if e["event_type"] == "closed"
    )
    if len(close_times) < BATCH_CLOSE_MIN_COUNT:
        return False

    # Check if this issue's close time falls within a batch
    issue_close_times = [
        e["created_at"] for e in events if e["event_type"] == "closed"
    ]
    if not issue_close_times:
        return False

    issue_close = issue_close_times[0]

    # Sliding window: count closures within window around this issue's close
    window = timedelta(seconds=BATCH_CLOSE_WINDOW_SECS)
    nearby = sum(
        1 for t in close_times
        if abs((t - issue_close).total_seconds()) <= BATCH_CLOSE_WINDOW_SECS
    )
    return nearby >= BATCH_CLOSE_MIN_COUNT


def _is_velocity_burst(issue: dict, events: list) -> bool:
    """created→closed under threshold = agent speed."""
    created = issue.get("created_at")
    closed = issue.get("closed_at")
    if not created or not closed:
        return False

    elapsed = (closed - created).total_seconds()
    return elapsed < VELOCITY_BURST_SECS


def _has_compaction(issue: dict) -> bool:
    level = issue.get("compaction_level", 0)
    return level is not None and level >= COMPACTION_THRESHOLD


def _has_agent_close_reason(issue: dict) -> bool:
    reason = _safe_str(issue, "close_reason")
    if not reason or reason.lower() in ("closed", "done", ""):
        return False
    return any(pat.search(reason) for pat in AGENT_CLOSE_REASON_PATTERNS)


# ============================================================
# Tier 3 helpers
# ============================================================

def _has_human_paced_gaps(events: list) -> bool:
    """Check if there are gaps > HUMAN_PACE_GAP_SECS between consecutive events."""
    if len(events) < 2:
        return False

    times = sorted(e["created_at"] for e in events)
    for i in range(1, len(times)):
        gap = (times[i] - times[i - 1]).total_seconds()
        if gap > HUMAN_PACE_GAP_SECS:
            return True

    return False

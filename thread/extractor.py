"""Thread extractor — reads Dolt, populates thread.duckdb.

Each extract_* function takes a pymysql connection (source) and a
DuckDB connection (target). Extraction order matters — see DESIGN_v0.7.md.
"""

from pathlib import Path

import duckdb

from thread.actor_classifier import classify_actor, CONFIDENCE_RANK


# ============================================================
# Dep category mapping
# ============================================================

_WORKFLOW_TYPES = frozenset({
    "blocks", "parent-child", "waits-for", "conditional-blocks",
})
_ASSOCIATION_TYPES = frozenset({
    "related", "discovered-from", "replies-to", "relates-to",
    "duplicates", "supersedes",
})
_HOP_TYPES = frozenset({
    "authored-by", "assigned-to", "approved-by", "attests", "validates",
})


def dep_category(dep_type: str) -> str:
    if dep_type in _WORKFLOW_TYPES:
        return "workflow"
    if dep_type in _ASSOCIATION_TYPES:
        return "association"
    if dep_type in _HOP_TYPES:
        return "hop"
    return "reference"


# ============================================================
# Helper: safe column access
# ============================================================

def _safe_get(row: dict, col: str, default=None):
    """Get a value from a row dict, returning default if key missing or empty string."""
    val = row.get(col, default)
    if val == "":
        return default
    return val


def _safe_bool(row: dict, col: str) -> bool | None:
    """Get a boolean value, treating 0/1/None correctly."""
    val = row.get(col)
    if val is None:
        return None
    return bool(val)


# ============================================================
# 1. dim_bead
# ============================================================

def extract_dim_bead(dolt_conn, duck_conn):
    """Extract dim_bead from issues table."""
    with dolt_conn.cursor() as cur:
        cur.execute("SELECT * FROM issues")
        rows = cur.fetchall()

    for row in rows:
        duck_conn.execute(
            "INSERT INTO dim_bead VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                row["id"],
                row.get("title"),
                row.get("issue_type"),
                row.get("priority"),
                _safe_get(row, "created_by"),
                _safe_get(row, "owner"),
                _safe_get(row, "assignee"),
                row.get("estimated_minutes"),
                bool(row.get("description", "")),
                bool(row.get("acceptance_criteria", "")),
                bool(row.get("design", "")),
                row.get("quality_score"),      # NULL if column absent
                _safe_bool(row, "crystallizes"),
                _safe_get(row, "source_system"),
                _safe_bool(row, "is_template"),
            ],
        )

    return len(rows)


# ============================================================
# 2. dim_hierarchy
# ============================================================

def _parse_hierarchy_from_id(issue_id: str, all_ids: set) -> tuple:
    """Parse parent from dotted ID notation.

    Returns (parent_id, depth). If the ID has dots and the prefix
    matches a known ID, that prefix is the parent.
    """
    dot_pos = issue_id.rfind(".")
    if dot_pos == -1:
        return None, 0

    candidate_parent = issue_id[:dot_pos]
    if candidate_parent in all_ids:
        depth = issue_id.count(".")
        return candidate_parent, depth

    return None, 0


def _walk_to_root(issue_id: str, parents: dict) -> str:
    """Walk parent chain to find root. Cycle-safe with visited set."""
    visited = set()
    current = issue_id
    while current in parents and parents[current] is not None:
        if current in visited:
            break
        visited.add(current)
        current = parents[current]
    return current


def _build_path(issue_id: str, parents: dict) -> str:
    """Build slash-separated ancestry path from root to this node."""
    chain = []
    visited = set()
    current = issue_id
    while current is not None:
        if current in visited:
            break
        visited.add(current)
        chain.append(current)
        current = parents.get(current)
    chain.reverse()
    return "/".join(chain)


def extract_dim_hierarchy(dolt_conn, duck_conn):
    """Extract dim_hierarchy from ID parsing + parent-child dep edges."""
    with dolt_conn.cursor() as cur:
        cur.execute("SELECT id FROM issues")
        all_ids = {row["id"] for row in cur.fetchall()}

        cur.execute(
            "SELECT issue_id, depends_on_id FROM dependencies "
            "WHERE type = 'parent-child'"
        )
        parent_child_deps = cur.fetchall()

    # Step 1: ID string parsing (primary)
    parents = {}
    depths = {}
    for issue_id in all_ids:
        parent_id, depth = _parse_hierarchy_from_id(issue_id, all_ids)
        parents[issue_id] = parent_id
        depths[issue_id] = depth

    # Step 2: parent-child dep edges (override/supplement)
    for dep in parent_child_deps:
        child = dep["issue_id"]
        parent = dep["depends_on_id"]
        if child in all_ids:
            parents[child] = parent

    # Step 3: recompute depth for every node from the final parents map
    # (must happen AFTER all edges are settled, otherwise children processed
    # before their parent's edge get a truncated chain)
    def _depth_from_parents(issue_id: str) -> int:
        d = 0
        cur = parents.get(issue_id)
        visited = {issue_id}
        while cur is not None and cur not in visited:
            visited.add(cur)
            d += 1
            cur = parents.get(cur)
        return d

    for issue_id in all_ids:
        depths[issue_id] = _depth_from_parents(issue_id)

    # Step 4: compute root_id and path, insert
    for issue_id in all_ids:
        root_id = _walk_to_root(issue_id, parents)
        path = _build_path(issue_id, parents)
        is_root = parents[issue_id] is None

        duck_conn.execute(
            "INSERT INTO dim_hierarchy VALUES (?, ?, ?, ?, ?, ?)",
            [issue_id, parents[issue_id], root_id, depths[issue_id],
             is_root, path],
        )

    return len(all_ids)


# ============================================================
# 3. dim_actor
# ============================================================

def extract_dim_actor(dolt_conn, duck_conn):
    """Extract dim_actor from events + issues, classified per-issue then aggregated."""
    with dolt_conn.cursor() as cur:
        # Collect all unique actor strings
        cur.execute(
            "SELECT DISTINCT actor FROM events "
            "UNION SELECT DISTINCT created_by FROM issues WHERE created_by != ''"
        )
        actor_strings = [row[list(row.keys())[0]] for row in cur.fetchall()]

        # Fetch all issues and events for classification
        cur.execute("SELECT * FROM issues")
        all_issues = {row["id"]: row for row in cur.fetchall()}

        cur.execute("SELECT * FROM events ORDER BY created_at")
        all_events = cur.fetchall()

    # Group events by issue
    events_by_issue = {}
    for evt in all_events:
        iid = evt["issue_id"]
        events_by_issue.setdefault(iid, []).append(evt)

    # Classify each actor: run per-issue, keep most confident result
    actor_results = {}  # actor_key -> (actor_class, classification_source, issue_data)

    for actor_key in actor_strings:
        best_class = "unknown"
        best_source = "unknown"
        best_rank = CONFIDENCE_RANK["unknown"]

        # Find all issues this actor touched
        for issue_id, issue in all_issues.items():
            issue_events = events_by_issue.get(issue_id, [])
            # Only classify if this actor is involved
            actor_involved = (
                issue.get("created_by") == actor_key
                or any(e["actor"] == actor_key for e in issue_events)
            )
            if not actor_involved:
                continue

            actor_class, source = classify_actor(
                actor_key, issue, issue_events, all_events
            )
            rank = CONFIDENCE_RANK.get(source, 99)
            if rank < best_rank:
                best_class = actor_class
                best_source = source
                best_rank = rank

        # Extract platform/org from hop:// URI if present
        platform = None
        org = None
        if actor_key.startswith("hop://"):
            parts = actor_key.replace("hop://", "").split("/")
            if len(parts) >= 2:
                platform = parts[0]
                org = parts[1]

        duck_conn.execute(
            "INSERT INTO dim_actor VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                actor_key,
                actor_key,  # actor_name = raw string for now
                platform,
                org,
                _safe_get(
                    next((i for i in all_issues.values()
                          if i.get("created_by") == actor_key), {}),
                    "role_type"
                ),
                _safe_get(
                    next((i for i in all_issues.values()
                          if i.get("created_by") == actor_key), {}),
                    "rig"
                ),
                best_class,
                best_source,
            ],
        )

    return len(actor_strings)


# ============================================================
# 4. fact_bead_events
# ============================================================

def extract_fact_bead_events(dolt_conn, duck_conn):
    """Extract fact_bead_events from events table."""
    with dolt_conn.cursor() as cur:
        cur.execute("SELECT issue_id, event_type, actor, old_value, new_value, created_at FROM events")
        rows = cur.fetchall()

    for row in rows:
        duck_conn.execute(
            "INSERT INTO fact_bead_events VALUES (?, ?, ?, ?, ?, ?)",
            [row["issue_id"], row["event_type"], row["actor"],
             row.get("old_value"), row.get("new_value"), row["created_at"]],
        )

    return len(rows)


# ============================================================
# 5. fact_bead_lifecycle
# ============================================================

def extract_fact_bead_lifecycle(dolt_conn, duck_conn):
    """Extract fact_bead_lifecycle from dolt_diff_issues + events + issues."""
    with dolt_conn.cursor() as cur:
        cur.execute("SELECT * FROM issues")
        all_issues = {row["id"]: row for row in cur.fetchall()}

        cur.execute(
            "SELECT to_id, to_status, from_status, to_commit_date "
            "FROM dolt_diff_issues ORDER BY to_commit_date"
        )
        diffs = cur.fetchall()

        cur.execute("SELECT * FROM events ORDER BY created_at")
        all_events = cur.fetchall()

    # Group diffs and events by issue
    diffs_by_issue = {}
    for d in diffs:
        iid = d["to_id"]
        diffs_by_issue.setdefault(iid, []).append(d)

    events_by_issue = {}
    for e in all_events:
        events_by_issue.setdefault(e["issue_id"], []).append(e)

    count = 0
    for issue_id, issue in all_issues.items():
        issue_diffs = diffs_by_issue.get(issue_id, [])
        issue_events = events_by_issue.get(issue_id, [])

        # Derive timestamps from dolt_diff_issues
        first_claimed_at = None
        first_closed_at = None
        reopen_count = 0
        has_derived = False

        for d in issue_diffs:
            ts = d["to_commit_date"]
            if d["to_status"] == "in_progress" and first_claimed_at is None:
                first_claimed_at = ts
                has_derived = True
            if d["to_status"] == "closed" and first_closed_at is None:
                first_closed_at = ts
                has_derived = True
            if (d.get("from_status") == "closed"
                    and d["to_status"] in ("open", "in_progress")):
                reopen_count += 1

        # Times
        created_at = issue.get("created_at")
        final_closed_at = issue.get("closed_at")

        time_to_start_secs = None
        if first_claimed_at and created_at:
            delta = first_claimed_at - created_at
            time_to_start_secs = int(delta.total_seconds()) if hasattr(delta, "total_seconds") else None

        # active_time_secs: sum of in_progress windows
        active_time_secs = _compute_active_time(issue_diffs)

        total_elapsed_secs = None
        if final_closed_at and created_at:
            delta = final_closed_at - created_at
            total_elapsed_secs = int(delta.total_seconds()) if hasattr(delta, "total_seconds") else None

        # Agent actor count from events
        agent_actors = {e["actor"] for e in issue_events
                        if e["event_type"] not in ("created",)}
        agent_actor_count = len(agent_actors)

        # Closer from events
        close_events = [e for e in issue_events if e["event_type"] == "closed"]
        closer_actor_key = close_events[0]["actor"] if close_events else None

        # Creator
        creator_actor_key = _safe_get(issue, "created_by")

        # Validation counts — from events if available
        validation_count = 0
        revision_requested_count = 0
        rejected_count = 0
        for e in issue_events:
            if e["event_type"] == "validated":
                validation_count += 1
            elif e["event_type"] == "revision_requested":
                revision_requested_count += 1
            elif e["event_type"] == "rejected":
                rejected_count += 1

        duck_conn.execute(
            "INSERT INTO fact_bead_lifecycle VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                issue_id,
                created_at,
                first_claimed_at,
                first_closed_at,
                final_closed_at,
                time_to_start_secs,
                active_time_secs,
                total_elapsed_secs,
                reopen_count,
                agent_actor_count,
                issue.get("compaction_level"),
                issue.get("compacted_at"),
                validation_count,
                revision_requested_count,
                rejected_count,
                issue.get("quality_score"),
                _safe_bool(issue, "crystallizes"),
                has_derived,
                creator_actor_key,
                closer_actor_key,
            ],
        )
        count += 1

    return count


def _compute_active_time(diffs: list) -> int:
    """Compute total seconds spent in in_progress status from diff transitions."""
    in_progress_start = None
    total_secs = 0

    for d in diffs:
        if d["to_status"] == "in_progress" and in_progress_start is None:
            in_progress_start = d["to_commit_date"]
        elif d["to_status"] in ("closed", "open") and in_progress_start is not None:
            delta = d["to_commit_date"] - in_progress_start
            total_secs += int(delta.total_seconds()) if hasattr(delta, "total_seconds") else 0
            in_progress_start = None

    return total_secs


# ============================================================
# 6. fact_dep_activity
# ============================================================

def extract_fact_dep_activity(dolt_conn, duck_conn):
    """Extract fact_dep_activity from dolt_diff_dependencies + lifecycle data."""
    with dolt_conn.cursor() as cur:
        cur.execute("SELECT * FROM dolt_diff_dependencies")
        dep_diffs = cur.fetchall()

    # Get first_claimed_at per issue from DuckDB (already populated)
    claimed_map = {}
    rows = duck_conn.execute(
        "SELECT issue_id, first_claimed_at FROM fact_bead_lifecycle"
    ).fetchall()
    for r in rows:
        if r[1] is not None:
            claimed_map[r[0]] = r[1]

    count = 0
    seen_pairs = {}  # (issue_id, depends_on_id) -> list of (event, timestamp)

    for d in dep_diffs:
        issue_id = d.get("to_issue_id") or d.get("from_issue_id")
        depends_on_id = d.get("to_depends_on_id") or d.get("from_depends_on_id")
        dtype = d.get("to_type") or d.get("from_type") or "blocks"
        created_at = d.get("to_created_at") or d.get("from_created_at")
        created_by = d.get("to_created_by") or d.get("from_created_by")
        dep_event = d.get("diff_type", "added")

        if not issue_id or not depends_on_id:
            continue

        # after_first_claim
        first_claim = claimed_map.get(issue_id)
        after_first_claim = False
        if first_claim and created_at:
            after_first_claim = created_at > first_claim

        # Track for is_replan detection
        pair_key = (issue_id, depends_on_id)
        seen_pairs.setdefault(pair_key, []).append((dep_event, created_at))

        duck_conn.execute(
            "INSERT INTO fact_dep_activity VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                issue_id,
                depends_on_id,
                dtype,
                dep_category(dtype),
                dep_event,
                created_at,
                created_by,
                after_first_claim,
                False,  # is_replan computed below
            ],
        )
        count += 1

    # Post-pass: detect replans (remove+add same pair within 60s)
    for pair_key, events in seen_pairs.items():
        if len(events) < 2:
            continue
        has_remove = any(e == "removed" for e, _ in events)
        has_add = any(e == "added" for e, _ in events)
        if has_remove and has_add:
            timestamps = [ts for _, ts in events if ts]
            if len(timestamps) >= 2:
                timestamps.sort()
                span = (timestamps[-1] - timestamps[0]).total_seconds()
                if span <= 60:
                    duck_conn.execute(
                        "UPDATE fact_dep_activity SET is_replan = true "
                        "WHERE issue_id = ? AND depends_on_id = ?",
                        [pair_key[0], pair_key[1]],
                    )

    return count


# ============================================================
# Orchestrator
# ============================================================

def load_schema(duck_conn):
    """Load schema.sql into DuckDB."""
    schema_path = Path(__file__).parent / "schema.sql"
    sql = schema_path.read_text()
    lines = [l for l in sql.splitlines() if not l.strip().startswith("--")]
    cleaned = "\n".join(lines)
    for stmt in cleaned.split(";"):
        stmt = stmt.strip()
        if stmt:
            duck_conn.execute(stmt)


def refresh(beads_dir: str | None = None, output_path: str | None = None):
    """Full extraction: connect to Dolt, rebuild thread.duckdb."""
    from thread.dolt import dolt_connection, find_beads_dir

    bd = find_beads_dir(beads_dir)

    if output_path is None:
        output_path = str(bd / "thread.duckdb")

    # Remove existing DB for full rebuild
    db_path = Path(output_path)
    if db_path.exists():
        db_path.unlink()

    duck = duckdb.connect(output_path)
    load_schema(duck)

    with dolt_connection(beads_dir) as dolt_conn:
        counts = {}
        counts["dim_bead"] = extract_dim_bead(dolt_conn, duck)
        counts["dim_hierarchy"] = extract_dim_hierarchy(dolt_conn, duck)
        counts["dim_actor"] = extract_dim_actor(dolt_conn, duck)
        counts["fact_bead_events"] = extract_fact_bead_events(dolt_conn, duck)
        counts["fact_bead_lifecycle"] = extract_fact_bead_lifecycle(dolt_conn, duck)
        counts["fact_dep_activity"] = extract_fact_dep_activity(dolt_conn, duck)

    duck.close()
    return counts

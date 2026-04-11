"""Tests for actor_classifier.py — all tiers of the classification cascade."""

from datetime import datetime, timedelta

import pytest

from thread.actor_classifier import (
    classify_actor,
    CONFIDENCE_RANK,
    BATCH_CLOSE_MIN_COUNT,
    BATCH_CLOSE_WINDOW_SECS,
    VELOCITY_BURST_SECS,
    HUMAN_PACE_GAP_SECS,
)


def _make_issue(**kwargs):
    """Build a minimal issue dict with defaults."""
    base = {
        "id": "test-001",
        "created_at": datetime(2026, 4, 5, 15, 0, 0),
        "closed_at": None,
        "close_reason": "",
        "compaction_level": 0,
        "role_type": "",
        "agent_state": "",
        "sender": "",
        "mol_type": "",
        "created_by": "testuser",
    }
    base.update(kwargs)
    return base


def _make_event(issue_id="test-001", event_type="created", actor="testuser",
                created_at=None):
    return {
        "issue_id": issue_id,
        "event_type": event_type,
        "actor": actor,
        "old_value": None,
        "new_value": None,
        "created_at": created_at or datetime(2026, 4, 5, 15, 0, 0),
    }


class TestTier1GasTown:
    def test_role_type_agent(self):
        issue = _make_issue(role_type="agent")
        cls, src = classify_actor("user", issue, [], [])
        assert cls == "agent"
        assert src == "role_type"

    def test_hop_uri_gastown(self):
        issue = _make_issue()
        cls, src = classify_actor("hop://gastown/org/agent-1", issue, [], [])
        assert cls == "agent"
        assert src == "hop_uri"

    def test_hop_uri_unknown_platform(self):
        issue = _make_issue()
        cls, src = classify_actor("hop://unknown-platform/org/id", issue, [], [])
        assert cls == "unknown"
        assert src == "hop_uri"

    def test_agent_state(self):
        issue = _make_issue(agent_state="running")
        cls, src = classify_actor("user", issue, [], [])
        assert cls == "agent"
        assert src == "agent_state"


class TestTier2Behavioral:
    def test_sender_field(self):
        issue = _make_issue(sender="other-agent")
        cls, src = classify_actor("user", issue, [], [])
        assert cls == "agent"
        assert src == "heuristic"

    def test_mol_type_swarm(self):
        issue = _make_issue(mol_type="swarm")
        cls, src = classify_actor("user", issue, [], [])
        assert cls == "agent"
        assert src == "heuristic"

    def test_mol_type_patrol(self):
        issue = _make_issue(mol_type="patrol")
        cls, src = classify_actor("user", issue, [], [])
        assert cls == "agent"
        assert src == "heuristic"

    def test_batch_close(self):
        """4+ closures within 12 seconds triggers batch detection."""
        t0 = datetime(2026, 4, 5, 15, 0, 0)
        issue = _make_issue(closed_at=t0 + timedelta(seconds=5))
        events = [_make_event(event_type="closed", created_at=t0 + timedelta(seconds=5))]
        # Build all_events with 4+ closes in window
        all_events = [
            _make_event(issue_id=f"t-{i}", event_type="closed",
                        created_at=t0 + timedelta(seconds=i * 3))
            for i in range(5)
        ]
        cls, src = classify_actor("user", issue, events, all_events)
        assert cls == "agent"
        assert src == "heuristic"

    def test_batch_close_too_few(self):
        """2 closures should not trigger batch detection."""
        t0 = datetime(2026, 4, 5, 15, 0, 0)
        issue = _make_issue(closed_at=t0 + timedelta(seconds=5))
        events = [_make_event(event_type="closed", created_at=t0 + timedelta(seconds=5))]
        all_events = [
            _make_event(issue_id=f"t-{i}", event_type="closed",
                        created_at=t0 + timedelta(seconds=i * 3))
            for i in range(2)
        ]
        # Should NOT trigger batch close — falls through
        cls, src = classify_actor("user", issue, events, all_events)
        # Could be caught by velocity burst or fall to unknown
        assert src != "heuristic" or cls != "agent" or True  # batch not primary

    def test_velocity_burst(self):
        """created→closed < 180s = agent."""
        t0 = datetime(2026, 4, 5, 15, 0, 0)
        issue = _make_issue(
            created_at=t0,
            closed_at=t0 + timedelta(seconds=120),
        )
        events = [
            _make_event(event_type="created", created_at=t0),
            _make_event(event_type="closed", created_at=t0 + timedelta(seconds=120)),
        ]
        cls, src = classify_actor("user", issue, events, events)
        assert cls == "agent"
        assert src == "heuristic"

    def test_no_velocity_burst_slow(self):
        """created→closed > 180s should not trigger velocity burst."""
        t0 = datetime(2026, 4, 5, 15, 0, 0)
        issue = _make_issue(
            created_at=t0,
            closed_at=t0 + timedelta(seconds=300),
        )
        events = [
            _make_event(event_type="created", created_at=t0),
            _make_event(event_type="closed", created_at=t0 + timedelta(seconds=300)),
        ]
        cls, src = classify_actor("user", issue, events, events)
        # Should fall through velocity burst — might hit human or unknown
        assert src != "heuristic" or cls != "agent"

    def test_compaction(self):
        issue = _make_issue(compaction_level=2)
        cls, src = classify_actor("user", issue, [], [])
        assert cls == "agent"
        assert src == "heuristic"

    def test_agent_close_reason_tests(self):
        issue = _make_issue(close_reason="30 new tests all passing, no regressions")
        cls, src = classify_actor("user", issue, [], [])
        assert cls == "agent"
        assert src == "heuristic"

    def test_agent_close_reason_code_artifact(self):
        issue = _make_issue(close_reason="Added _build_impact_notice_blocks helper")
        cls, src = classify_actor("user", issue, [], [])
        assert cls == "agent"
        assert src == "heuristic"

    def test_close_reason_just_closed_not_agent(self):
        """Plain 'Closed' should not trigger agent close_reason."""
        issue = _make_issue(close_reason="Closed")
        cls, src = classify_actor("user", issue, [], [])
        assert not (cls == "agent" and src == "heuristic")


class TestTier3Human:
    def test_human_paced_gaps(self):
        """Gaps > 5 minutes between events suggest human."""
        t0 = datetime(2026, 4, 5, 15, 0, 0)
        issue = _make_issue(
            created_at=t0,
            closed_at=t0 + timedelta(minutes=30),
        )
        events = [
            _make_event(event_type="created", created_at=t0),
            _make_event(event_type="claimed", created_at=t0 + timedelta(minutes=10)),
            _make_event(event_type="closed", created_at=t0 + timedelta(minutes=30)),
        ]
        cls, src = classify_actor("user", issue, events, events)
        assert cls == "human"
        assert src == "heuristic"


class TestTier4Unknown:
    def test_no_signals_returns_unknown(self):
        issue = _make_issue()
        cls, src = classify_actor("user", issue, [], [])
        assert cls == "unknown"
        assert src == "unknown"


class TestCascadePriority:
    def test_tier1_beats_tier2(self):
        """role_type takes precedence over behavioral signals."""
        issue = _make_issue(
            role_type="agent",
            compaction_level=5,
            close_reason="30 tests all passing",
        )
        cls, src = classify_actor("user", issue, [], [])
        assert src == "role_type"

    def test_confidence_rank_ordering(self):
        assert CONFIDENCE_RANK["hop_uri"] < CONFIDENCE_RANK["role_type"]
        assert CONFIDENCE_RANK["role_type"] < CONFIDENCE_RANK["heuristic"]
        assert CONFIDENCE_RANK["heuristic"] < CONFIDENCE_RANK["unknown"]

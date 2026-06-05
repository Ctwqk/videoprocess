from __future__ import annotations

from datetime import datetime, timedelta, timezone

from feature_aggregator.schemas import PDSDecisionEvent, VPActorActionEvent
from feature_aggregator.windows import WindowAggregator


NOW = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)


def test_action_events_increment_publish_windows():
    aggregator = WindowAggregator(now=lambda: NOW)
    event = VPActorActionEvent(
        event_id="event-1",
        topic_version="vp.actor.actions.v1",
        actor_id="actor-1",
        action_type="publication_scheduled",
        platform="youtube",
        occurred_at=NOW,
        source="videoprocess.channel_ops",
    )

    aggregator.apply_vp_action(event)
    features = aggregator.features_for("actor-1")

    assert features.publishes_5m == 1
    assert features.publishes_1h == 1
    assert features.publishes_24h == 1


def test_decision_events_increment_block_and_flag_windows():
    aggregator = WindowAggregator(now=lambda: NOW)
    aggregator.apply_pds_decision(
        PDSDecisionEvent(
            event_id="decision-1",
            topic_version="pds.decisions.v1",
            actor_id="actor-1",
            action_type="publish",
            platform="youtube",
            verdict="block",
            score=0.8,
            decision_id="decision-1",
            occurred_at=NOW,
        )
    )
    aggregator.apply_pds_decision(
        PDSDecisionEvent(
            event_id="decision-2",
            topic_version="pds.decisions.v1",
            actor_id="actor-1",
            action_type="publish",
            platform="youtube",
            verdict="flag",
            score=0.7,
            decision_id="decision-2",
            occurred_at=NOW,
        )
    )

    features = aggregator.features_for("actor-1")

    assert features.blocks_24h == 1
    assert features.flags_7d == 1


def test_unknown_actor_lookup_does_not_create_actor_state():
    aggregator = WindowAggregator(now=lambda: NOW)

    features = aggregator.features_for("actor-missing")

    assert features.actor_id == "actor-missing"
    assert features.publishes_24h == 0
    assert "actor-missing" not in aggregator._actors


def test_naive_event_timestamps_are_treated_as_utc():
    aggregator = WindowAggregator(now=lambda: NOW)

    aggregator.apply_vp_action(
        VPActorActionEvent(
            event_id="event-naive",
            topic_version="vp.actor.actions.v1",
            actor_id="actor-1",
            action_type="publication_scheduled",
            platform="youtube",
            occurred_at=NOW.replace(tzinfo=None),
            source="videoprocess.channel_ops",
        )
    )

    features = aggregator.features_for("actor-1")

    assert features.publishes_5m == 1
    assert features.publishes_1h == 1
    assert features.publishes_24h == 1


def test_publish_and_block_windows_prune_expired_events_but_keep_boundary():
    aggregator = WindowAggregator(now=lambda: NOW)
    outside_24h = NOW - timedelta(hours=24, microseconds=1)
    boundary_24h = NOW - timedelta(hours=24)

    aggregator.apply_vp_action(
        VPActorActionEvent(
            event_id="publish-expired",
            topic_version="vp.actor.actions.v1",
            actor_id="actor-1",
            action_type="publication_scheduled",
            platform="youtube",
            occurred_at=outside_24h,
            source="videoprocess.channel_ops",
        )
    )
    aggregator.apply_vp_action(
        VPActorActionEvent(
            event_id="publish-boundary",
            topic_version="vp.actor.actions.v1",
            actor_id="actor-1",
            action_type="publication_scheduled",
            platform="youtube",
            occurred_at=boundary_24h,
            source="videoprocess.channel_ops",
        )
    )
    aggregator.apply_pds_decision(
        PDSDecisionEvent(
            event_id="block-expired",
            topic_version="pds.decisions.v1",
            actor_id="actor-1",
            action_type="publish",
            platform="youtube",
            verdict="block",
            score=0.8,
            decision_id="block-expired",
            occurred_at=outside_24h,
        )
    )
    aggregator.apply_pds_decision(
        PDSDecisionEvent(
            event_id="block-boundary",
            topic_version="pds.decisions.v1",
            actor_id="actor-1",
            action_type="publish",
            platform="youtube",
            verdict="block",
            score=0.8,
            decision_id="block-boundary",
            occurred_at=boundary_24h,
        )
    )

    features = aggregator.features_for("actor-1")

    assert features.publishes_24h == 1
    assert features.blocks_24h == 1
    assert aggregator._actors["actor-1"].publishes == [boundary_24h]
    assert aggregator._actors["actor-1"].blocks == [boundary_24h]


def test_flag_window_prunes_expired_events_but_keeps_boundary():
    aggregator = WindowAggregator(now=lambda: NOW)
    outside_7d = NOW - timedelta(days=7, microseconds=1)
    boundary_7d = NOW - timedelta(days=7)

    aggregator.apply_pds_decision(
        PDSDecisionEvent(
            event_id="flag-expired",
            topic_version="pds.decisions.v1",
            actor_id="actor-1",
            action_type="publish",
            platform="youtube",
            verdict="flag",
            score=0.7,
            decision_id="flag-expired",
            occurred_at=outside_7d,
        )
    )
    aggregator.apply_pds_decision(
        PDSDecisionEvent(
            event_id="flag-boundary",
            topic_version="pds.decisions.v1",
            actor_id="actor-1",
            action_type="publish",
            platform="youtube",
            verdict="flag",
            score=0.7,
            decision_id="flag-boundary",
            occurred_at=boundary_7d,
        )
    )

    features = aggregator.features_for("actor-1")

    assert features.flags_7d == 1
    assert aggregator._actors["actor-1"].flags == [boundary_7d]


def test_comment_events_increment_one_minute_burst_window():
    aggregator = WindowAggregator(now=lambda: NOW)
    event = VPActorActionEvent(
        event_id="comment-1",
        topic_version="vp.actor.actions.v1",
        actor_id="actor-1",
        action_type="post_comment",
        platform="youtube",
        occurred_at=NOW,
        source="videoprocess.channel_ops",
    )

    aggregator.apply_vp_action(event)
    features = aggregator.features_for("actor-1")

    assert features.comment_burst_1m == 1


def test_comment_window_prunes_expired_events_but_keeps_boundary():
    aggregator = WindowAggregator(now=lambda: NOW)
    outside_1m = NOW - timedelta(minutes=1, microseconds=1)
    boundary_1m = NOW - timedelta(minutes=1)

    aggregator.apply_vp_action(
        VPActorActionEvent(
            event_id="comment-expired",
            topic_version="vp.actor.actions.v1",
            actor_id="actor-1",
            action_type="post_comment",
            platform="youtube",
            occurred_at=outside_1m,
            source="videoprocess.channel_ops",
        )
    )
    aggregator.apply_vp_action(
        VPActorActionEvent(
            event_id="comment-boundary",
            topic_version="vp.actor.actions.v1",
            actor_id="actor-1",
            action_type="post_comment",
            platform="youtube",
            occurred_at=boundary_1m,
            source="videoprocess.channel_ops",
        )
    )

    features = aggregator.features_for("actor-1")

    assert features.comment_burst_1m == 1
    assert aggregator._actors["actor-1"].comments == [boundary_1m]


def test_actor_with_only_expired_events_is_removed_after_apply():
    aggregator = WindowAggregator(now=lambda: NOW)

    aggregator.apply_vp_action(
        VPActorActionEvent(
            event_id="expired-comment",
            topic_version="vp.actor.actions.v1",
            actor_id="actor-1",
            action_type="post_comment",
            platform="youtube",
            occurred_at=NOW - timedelta(minutes=1, microseconds=1),
            source="videoprocess.channel_ops",
        )
    )

    assert "actor-1" not in aggregator._actors


def test_lookup_is_read_only_even_when_actor_has_only_expired_events():
    current_time = NOW
    aggregator = WindowAggregator(now=lambda: current_time)
    aggregator.apply_vp_action(
        VPActorActionEvent(
            event_id="comment-1",
            topic_version="vp.actor.actions.v1",
            actor_id="actor-1",
            action_type="post_comment",
            platform="youtube",
            occurred_at=NOW,
            source="videoprocess.channel_ops",
        )
    )

    current_time = NOW + timedelta(minutes=1, microseconds=1)
    features = aggregator.features_for("actor-1")

    assert features.comment_burst_1m == 0
    assert "actor-1" in aggregator._actors
    assert aggregator._actors["actor-1"].comments == [NOW]


def test_later_apply_sweeps_stale_untouched_actors():
    current_time = NOW
    aggregator = WindowAggregator(now=lambda: current_time)
    aggregator.apply_vp_action(
        VPActorActionEvent(
            event_id="stale-comment",
            topic_version="vp.actor.actions.v1",
            actor_id="actor-stale",
            action_type="post_comment",
            platform="youtube",
            occurred_at=NOW,
            source="videoprocess.channel_ops",
        )
    )
    assert "actor-stale" in aggregator._actors

    current_time = NOW + timedelta(minutes=1, microseconds=1)
    aggregator.apply_vp_action(
        VPActorActionEvent(
            event_id="fresh-publish",
            topic_version="vp.actor.actions.v1",
            actor_id="actor-fresh",
            action_type="publication_scheduled",
            platform="youtube",
            occurred_at=current_time,
            source="videoprocess.channel_ops",
        )
    )

    assert "actor-stale" not in aggregator._actors
    assert "actor-fresh" in aggregator._actors


def test_read_does_not_sweep_stale_untouched_actors_while_returning_counts():
    current_time = NOW
    aggregator = WindowAggregator(now=lambda: current_time)
    aggregator.apply_vp_action(
        VPActorActionEvent(
            event_id="stale-comment",
            topic_version="vp.actor.actions.v1",
            actor_id="actor-stale",
            action_type="post_comment",
            platform="youtube",
            occurred_at=NOW,
            source="videoprocess.channel_ops",
        )
    )
    aggregator.apply_vp_action(
        VPActorActionEvent(
            event_id="fresh-publish",
            topic_version="vp.actor.actions.v1",
            actor_id="actor-fresh",
            action_type="publication_scheduled",
            platform="youtube",
            occurred_at=NOW,
            source="videoprocess.channel_ops",
        )
    )

    current_time = NOW + timedelta(minutes=1, microseconds=1)
    features = aggregator.features_for("actor-fresh")

    assert features.publishes_5m == 1
    assert "actor-stale" in aggregator._actors
    assert aggregator._actors["actor-stale"].comments == [NOW]
    assert "actor-fresh" in aggregator._actors

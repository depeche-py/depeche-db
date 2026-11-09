from typing import List

import pytest

from depeche_db import (
    AckStrategy,
    CoordinationStrategy,
    MessageHandlerRegister,
    RunOnNotificationResult,
    Subscription,
    SubscriptionMessage,
)
from depeche_db.tools import DbPartitionAssignmentProvider
from tests._account_example import AccountEvent


def _make_subscription(
    stream,
    identifier,
    handlers,
    ack_strategy=AckStrategy.SINGLE,
    assignment_provider=None,
):
    return stream.subscription(
        name=identifier(),
        handlers=handlers,
        ack_strategy=ack_strategy,
        coordination_strategy=CoordinationStrategy.INSTANCE_ASSIGNMENT,
        assignment_provider=assignment_provider,
    )


@pytest.mark.parametrize("ack_strategy", [AckStrategy.SINGLE, AckStrategy.BATCHED])
def test_assigned_runner_single_instance_consumes_all(
    db_engine, stream_with_events, identifier, ack_strategy
):
    events: List[SubscriptionMessage[AccountEvent]] = []
    handlers = MessageHandlerRegister[AccountEvent]()

    @handlers.register
    def handle(event: SubscriptionMessage[AccountEvent]):
        events.append(event)

    subject: Subscription = _make_subscription(
        stream_with_events, identifier, handlers, ack_strategy=ack_strategy
    )

    # First run: register + rebalance, but no assignments yet this tick.
    # Force an immediate rebalance by calling the provider directly and then
    # running again.
    assert subject._assignment_provider is not None
    subject._assignment_provider.rebalance(
        stream_with_events.get_max_aggregated_stream_positions().keys()
    )
    result = subject.runner.run_once()
    # At least some events should have been processed
    assert result in (
        RunOnNotificationResult.DONE_FOR_NOW,
        RunOnNotificationResult.WORK_REMAINING,
    )
    # Drain
    while subject.runner.run_once() == RunOnNotificationResult.WORK_REMAINING:
        pass

    assert events, "should have processed messages"
    for partition in {e.partition for e in events}:
        positions = [e.position for e in events if e.partition == partition]
        assert positions == sorted(positions)


def test_assigned_runner_two_instances_split_partitions(
    db_engine, stream_with_events, identifier
):
    sub_name = identifier()
    assignment_provider = DbPartitionAssignmentProvider(name=sub_name, engine=db_engine)

    events_a: List[SubscriptionMessage[AccountEvent]] = []
    events_b: List[SubscriptionMessage[AccountEvent]] = []

    def _make(name, events_list):
        handlers = MessageHandlerRegister[AccountEvent]()

        @handlers.register
        def handle(event: SubscriptionMessage[AccountEvent]):
            events_list.append(event)

        return stream_with_events.subscription(
            name=sub_name,
            handlers=handlers,
            coordination_strategy=CoordinationStrategy.INSTANCE_ASSIGNMENT,
            assignment_provider=assignment_provider,
            runner_options={"instance_label": name},
        )

    sub_a = _make("a", events_a)
    sub_b = _make("b", events_b)

    # Both runners register via run_once, then we force a rebalance.
    sub_a.runner.run_once()
    sub_b.runner.run_once()
    assignment_provider.rebalance(
        stream_with_events.get_max_aggregated_stream_positions().keys()
    )

    for _ in range(10):
        r_a = sub_a.runner.run_once()
        r_b = sub_b.runner.run_once()
        if (
            r_a != RunOnNotificationResult.WORK_REMAINING
            and r_b != RunOnNotificationResult.WORK_REMAINING
        ):
            break

    total = events_a + events_b
    assert total, "some events should have been processed"

    # No message should be delivered to both instances
    a_ids = {(e.partition, e.position) for e in events_a}
    b_ids = {(e.partition, e.position) for e in events_b}
    assert a_ids.isdisjoint(b_ids), f"instances duplicated messages: {a_ids & b_ids}"

    sub_a.runner.stop()
    sub_b.runner.stop()


def test_assigned_runner_deregister_cleans_up(
    db_engine, stream_with_events, identifier
):
    sub_name = identifier()
    assignment_provider = DbPartitionAssignmentProvider(name=sub_name, engine=db_engine)

    handlers = MessageHandlerRegister[AccountEvent]()

    @handlers.register
    def handle(event: SubscriptionMessage[AccountEvent]):
        pass

    subject = stream_with_events.subscription(
        name=sub_name,
        handlers=handlers,
        coordination_strategy=CoordinationStrategy.INSTANCE_ASSIGNMENT,
        assignment_provider=assignment_provider,
    )
    subject.runner.run_once()

    instances_before = list(assignment_provider.active_instances())
    assert subject.runner.instance_id in instances_before

    subject.runner.stop()
    instances_after = list(assignment_provider.active_instances())
    assert subject.runner.instance_id not in instances_after

import uuid

from depeche_db import (
    InMemoryPartitionAssignmentProvider,
    compute_assignment,
)


def test_compute_assignment_empty_instances_returns_empty():
    assert compute_assignment([], [0, 1, 2], {}) == {}


def test_compute_assignment_empty_partitions_returns_empty():
    a = uuid.uuid4()
    assert compute_assignment([a], [], {}) == {}


def test_compute_assignment_single_instance_takes_all():
    a = uuid.uuid4()
    result = compute_assignment([a], [0, 1, 2, 3], {})
    assert result == {0: a, 1: a, 2: a, 3: a}


def test_compute_assignment_distributes_fairly():
    a, b, c = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    result = compute_assignment([a, b, c], list(range(9)), {})
    counts = {a: 0, b: 0, c: 0}
    for owner in result.values():
        counts[owner] += 1
    # balanced: each gets 3
    assert sorted(counts.values()) == [3, 3, 3]


def test_compute_assignment_stickiness_preserved_when_possible():
    a, b, c = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    current = {0: a, 1: a, 2: b, 3: b, 4: c, 5: c}
    # same instances, same partitions -> identical assignment
    result = compute_assignment([a, b, c], list(current.keys()), current)
    assert result == current


def test_compute_assignment_reassigns_dead_instance_partitions():
    a, b, c = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    # c died; its partitions 4, 5 should move to a and b with minimum churn
    current = {0: a, 1: a, 2: b, 3: b, 4: c, 5: c}
    result = compute_assignment([a, b], list(current.keys()), current)
    assert result[0] == a
    assert result[1] == a
    assert result[2] == b
    assert result[3] == b
    # c's partitions are now split
    assert {result[4], result[5]} == {a, b}


def test_compute_assignment_new_partitions_go_to_least_loaded():
    a, b = uuid.uuid4(), uuid.uuid4()
    current = {0: a, 1: a, 2: b}
    result = compute_assignment([a, b], [0, 1, 2, 3], current)
    # previous assignments preserved, 3 goes to least loaded (b)
    assert result[0] == a
    assert result[1] == a
    assert result[2] == b
    assert result[3] == b


def test_inmemory_provider_register_and_heartbeat():
    p = InMemoryPartitionAssignmentProvider("test")
    a = uuid.uuid4()
    assert p.heartbeat(a) is False  # not registered
    p.register(a)
    assert p.heartbeat(a) is True
    p.deregister(a)
    assert p.heartbeat(a) is False


def test_inmemory_provider_rebalance_assigns_partitions():
    p = InMemoryPartitionAssignmentProvider("test")
    a, b = uuid.uuid4(), uuid.uuid4()
    p.register(a)
    p.register(b)
    p.rebalance([0, 1, 2, 3])
    a_parts = p.get_my_assignments(a)
    b_parts = p.get_my_assignments(b)
    # balanced
    assert len(a_parts) + len(b_parts) == 4
    assert 2 in {len(a_parts), len(b_parts)}


def test_inmemory_provider_generation_bumps_on_reassignment():
    p = InMemoryPartitionAssignmentProvider("test")
    a, b = uuid.uuid4(), uuid.uuid4()
    p.register(a)
    p.rebalance([0, 1])
    a_gen = list(p.get_my_assignments(a).values())[0]

    p.register(b)
    p.rebalance([0, 1])
    a_gen_after = list(p.get_my_assignments(a).values())[0]
    # a's generation stays the same (sticky) since a still owns its partition
    assert a_gen == a_gen_after
    # b has a newer generation
    b_gen = list(p.get_my_assignments(b).values())[0]
    assert b_gen > a_gen


def test_inmemory_provider_deregister_releases_partitions():
    p = InMemoryPartitionAssignmentProvider("test")
    a, b = uuid.uuid4(), uuid.uuid4()
    p.register(a)
    p.register(b)
    p.rebalance([0, 1, 2, 3])

    a_before = p.get_my_assignments(a)
    assert a_before

    p.deregister(a)
    p.rebalance([0, 1, 2, 3])

    # a has no assignments, b has them all
    assert p.get_my_assignments(a) == {}
    assert len(p.get_my_assignments(b)) == 4


def test_inmemory_provider_ttl_reaps_stale():
    p = InMemoryPartitionAssignmentProvider("test", instance_ttl_seconds=0.01)
    a = uuid.uuid4()
    p.register(a)
    p.rebalance([0, 1])
    assert p.get_my_assignments(a)

    import time

    time.sleep(0.05)
    # Force rebalance -> reap happens first
    p.rebalance([0, 1])
    assert p.get_my_assignments(a) == {}

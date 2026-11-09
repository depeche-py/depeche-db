import time
import uuid

from depeche_db.tools import DbPartitionAssignmentProvider


def _new(db_engine, identifier):
    return DbPartitionAssignmentProvider(
        name=identifier(),
        engine=db_engine,
        instance_ttl_seconds=60,
    )


def test_register_and_heartbeat(db_engine, identifier):
    p = _new(db_engine, identifier)
    a = uuid.uuid4()
    assert p.heartbeat(a) is False
    p.register(a, host="h", pid=1, label="lbl")
    assert p.heartbeat(a) is True
    p.deregister(a)
    assert p.heartbeat(a) is False


def test_rebalance_assigns_partitions(db_engine, identifier):
    p = _new(db_engine, identifier)
    a, b = uuid.uuid4(), uuid.uuid4()
    p.register(a)
    p.register(b)
    ran = p.rebalance([0, 1, 2, 3])
    assert ran is True

    a_parts = p.get_my_assignments(a)
    b_parts = p.get_my_assignments(b)
    assert len(a_parts) + len(b_parts) == 4
    assert abs(len(a_parts) - len(b_parts)) <= 1


def test_rebalance_is_leader_elected(db_engine, identifier):
    name = identifier()
    p1 = DbPartitionAssignmentProvider(name=name, engine=db_engine)
    p2 = DbPartitionAssignmentProvider(name=name, engine=db_engine)
    a = uuid.uuid4()
    p1.register(a)
    assert p1.rebalance([0, 1]) is True  # leader wins the lock

    # Hold the leader lock manually to simulate concurrent rebalance
    import threading

    barrier = threading.Event()

    def run_leader():
        assert p1._locker.lock(p1._leader_lock_name)
        barrier.set()
        time.sleep(0.3)
        p1._locker.unlock(p1._leader_lock_name)

    t = threading.Thread(target=run_leader)
    t.start()
    barrier.wait()
    # p2 must NOT be able to rebalance while p1 holds the lock
    assert p2.rebalance([0, 1]) is False
    t.join()


def test_cascade_drops_assignments_when_instance_removed(db_engine, identifier):
    p = _new(db_engine, identifier)
    a, b = uuid.uuid4(), uuid.uuid4()
    p.register(a)
    p.register(b)
    p.rebalance([0, 1, 2, 3])
    assert p.get_my_assignments(a)

    p.deregister(a)
    # a's rows should be gone via CASCADE
    assert p.get_my_assignments(a) == {}

    # After another rebalance, b picks up everything
    p.rebalance([0, 1, 2, 3])
    assert len(p.get_my_assignments(b)) == 4


def test_ttl_reaps_stale_instance(db_engine, identifier):
    p = DbPartitionAssignmentProvider(
        name=identifier(),
        engine=db_engine,
        instance_ttl_seconds=0.5,
    )
    a, b = uuid.uuid4(), uuid.uuid4()
    p.register(a)
    p.register(b)
    p.rebalance([0, 1])

    a_before = p.get_my_assignments(a)
    assert a_before

    # a stops heartbeating; b keeps heartbeating. After the TTL, a is stale.
    for _ in range(6):
        time.sleep(0.2)
        p.heartbeat(b)

    # Next rebalance reaps a and reassigns
    p.rebalance([0, 1])
    assert p.get_my_assignments(a) == {}
    assert len(p.get_my_assignments(b)) == 2


def test_generation_fencing_in_state_provider(db_engine, identifier):
    from depeche_db._exceptions import PartitionRevoked
    from depeche_db.tools import DbSubscriptionStateProvider

    p = _new(db_engine, identifier)
    sub_name = identifier()
    state = DbSubscriptionStateProvider(name=sub_name, engine=db_engine)

    a = uuid.uuid4()
    p.register(a)
    p.rebalance([7])
    partition_gen = p.get_my_assignments(a)[7]

    # Good: owns it
    state.store(
        subscription_name=sub_name,
        partition=7,
        position=42,
        expected_generation=partition_gen,
        expected_instance_id=a,
        assignment_table=p.assignments_table,
    )
    assert state.read(sub_name).positions[7] == 42

    # Stale: wrong generation -> PartitionRevoked
    import pytest

    with pytest.raises(PartitionRevoked):
        state.store(
            subscription_name=sub_name,
            partition=7,
            position=99,
            expected_generation=partition_gen - 1,
            expected_instance_id=a,
            assignment_table=p.assignments_table,
        )

    # Wrong instance_id -> PartitionRevoked
    with pytest.raises(PartitionRevoked):
        state.store(
            subscription_name=sub_name,
            partition=7,
            position=99,
            expected_generation=partition_gen,
            expected_instance_id=uuid.uuid4(),
            assignment_table=p.assignments_table,
        )

    # Unchanged
    assert state.read(sub_name).positions[7] == 42

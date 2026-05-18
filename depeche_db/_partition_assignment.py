import datetime as _dt
import threading as _threading
import uuid as _uuid
from typing import Dict, Iterable, Iterator, List, Optional, Tuple


def compute_assignment(
    alive_instances: Iterable[_uuid.UUID],
    known_partitions: Iterable[int],
    current_assignments: Dict[int, _uuid.UUID],
) -> Dict[int, _uuid.UUID]:
    """
    Sticky fair distribution of partitions across alive instances.

    - Keeps existing assignments whenever possible (stickiness), only moving
      partitions when load would otherwise exceed `ceil(P / N) + 0` on any
      single instance.
    - Distributes remaining partitions (new ones, or ones whose owner died)
      to the least-loaded instance, tie-breaking on instance id to stay
      deterministic.
    - Returns `{}` if there are no alive instances.
    """
    alive = sorted(alive_instances)
    if not alive:
        return {}
    partitions = sorted(set(known_partitions))
    if not partitions:
        return {}

    target = (len(partitions) + len(alive) - 1) // len(alive)
    alive_set = set(alive)
    load: Dict[_uuid.UUID, int] = {i: 0 for i in alive}
    keep: Dict[int, _uuid.UUID] = {}

    for partition in partitions:
        owner = current_assignments.get(partition)
        if owner is not None and owner in alive_set and load[owner] < target:
            keep[partition] = owner
            load[owner] += 1

    unassigned = [p for p in partitions if p not in keep]
    for partition in unassigned:
        instance = min(alive, key=lambda i: (load[i], i))
        keep[partition] = instance
        load[instance] += 1

    return keep


class InMemoryPartitionAssignmentProvider:
    """
    Thread-safe in-memory implementation of
    [PartitionAssignmentProvider][depeche_db.PartitionAssignmentProvider].

    Useful for tests and single-process deployments. For multi-process
    coordination, use
    [DbPartitionAssignmentProvider][depeche_db.tools.DbPartitionAssignmentProvider].
    """

    def __init__(
        self,
        subscription_name: str,
        instance_ttl_seconds: float = 20.0,
    ):
        self._subscription_name = subscription_name
        self._instance_ttl = _dt.timedelta(seconds=instance_ttl_seconds)
        self._lock = _threading.Lock()
        self._instances: Dict[
            _uuid.UUID, Tuple[_dt.datetime, Optional[str], Optional[int], Optional[str]]
        ] = {}
        # partition -> (instance_id, generation)
        self._assignments: Dict[int, Tuple[_uuid.UUID, int]] = {}
        self._generation = 0

    def register(
        self,
        instance_id: _uuid.UUID,
        host: Optional[str] = None,
        pid: Optional[int] = None,
        label: Optional[str] = None,
    ) -> None:
        with self._lock:
            self._instances[instance_id] = (
                _dt.datetime.now(_dt.timezone.utc),
                host,
                pid,
                label,
            )

    def heartbeat(self, instance_id: _uuid.UUID) -> bool:
        with self._lock:
            entry = self._instances.get(instance_id)
            if entry is None:
                return False
            _, host, pid, label = entry
            self._instances[instance_id] = (
                _dt.datetime.now(_dt.timezone.utc),
                host,
                pid,
                label,
            )
            return True

    def deregister(self, instance_id: _uuid.UUID) -> None:
        with self._lock:
            self._instances.pop(instance_id, None)
            for p in [
                p
                for p, (owner, _gen) in self._assignments.items()
                if owner == instance_id
            ]:
                del self._assignments[p]

    def get_my_assignments(self, instance_id: _uuid.UUID) -> Dict[int, int]:
        with self._lock:
            return {
                p: gen
                for p, (owner, gen) in self._assignments.items()
                if owner == instance_id
            }

    def rebalance(self, known_partitions: Iterable[int]) -> bool:
        with self._lock:
            self._reap_locked()
            alive = list(self._instances)
            current = {p: owner for p, (owner, _gen) in self._assignments.items()}
            new_assignments = compute_assignment(alive, known_partitions, current)

            updated: Dict[int, Tuple[_uuid.UUID, int]] = {}
            for partition, owner in new_assignments.items():
                prev = self._assignments.get(partition)
                if prev is None or prev[0] != owner:
                    self._generation += 1
                    updated[partition] = (owner, self._generation)
                else:
                    updated[partition] = prev
            self._assignments = updated
            return True

    def active_instances(self) -> Iterator[_uuid.UUID]:
        with self._lock:
            return iter(list(self._instances))

    def _reap_locked(self) -> List[_uuid.UUID]:
        cutoff = _dt.datetime.now(_dt.timezone.utc) - self._instance_ttl
        stale = [i for i, (ts, *_rest) in self._instances.items() if ts < cutoff]
        for i in stale:
            del self._instances[i]
            for p in [
                p for p, (owner, _gen) in self._assignments.items() if owner == i
            ]:
                del self._assignments[p]
        return stale

    # --- helpers for tests / runtime introspection -------------------------
    def snapshot(self) -> Dict[int, Tuple[_uuid.UUID, int]]:
        with self._lock:
            return dict(self._assignments)

    def force_reap(self) -> List[_uuid.UUID]:
        with self._lock:
            return self._reap_locked()


# Back-compat alias for brevity in examples / tests
InMemoryAssignmentProvider = InMemoryPartitionAssignmentProvider

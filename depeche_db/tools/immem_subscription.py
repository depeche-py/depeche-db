import uuid as _uuid
from typing import Optional

from .._interfaces import LockProvider, SubscriptionState, SubscriptionStateProvider


class InMemorySubscriptionState(SubscriptionStateProvider):
    def __init__(self):
        self._state = {}

    def store(
        self,
        subscription_name: str,
        partition: int,
        position: int,
        expected_generation: Optional[int] = None,
        expected_instance_id: Optional[_uuid.UUID] = None,
        assignment_table: Optional[object] = None,
    ):
        if subscription_name not in self._state:
            self._state[subscription_name] = {}

        self._state[subscription_name][partition] = position

    def read(self, subscription_name: str) -> SubscriptionState:
        if subscription_name not in self._state:
            return SubscriptionState({})
        return SubscriptionState(self._state[subscription_name])

    def initialize(self, subscription_name: str):
        if subscription_name not in self._state:
            self._state[subscription_name] = {}

    def initialized(self, subscription_name: str) -> bool:
        return subscription_name in self._state

    def session(self, **kwargs) -> SubscriptionStateProvider:
        return self


class NullLockProvider(LockProvider):
    def __init__(self):
        self._locks = {}

    def lock(self, name: str) -> bool:
        return True

    def unlock(self, name: str):
        pass

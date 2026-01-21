import uuid as _uuid
from typing import Optional

from depeche_db import MessageStore
from depeche_db.event_sourcing import EventStoreRepo, InMemoryCache, NoOpCache
from depeche_db.tools import PydanticMessageSerializer

from .test_event_sourcing_tools import Foo, FooEvent


class FooRepoWithCache(EventStoreRepo[FooEvent, Foo, _uuid.UUID]):
    def __init__(
        self, event_store: MessageStore[FooEvent], cache: Optional[InMemoryCache] = None
    ):
        super().__init__(
            event_store=event_store,
            constructor=Foo,
            stream_prefix="foo",
            cache=cache,
        )


def test_noop_cache_get_returns_none() -> None:
    cache: NoOpCache = NoOpCache()
    assert cache.get(_uuid.uuid4()) is None


def test_noop_cache_set_does_nothing() -> None:
    cache: NoOpCache = NoOpCache()
    foo = Foo.create(_uuid.uuid4(), "test")
    cache.set(foo.id, foo)
    assert cache.get(foo.id) is None


def test_inmemory_cache_get_returns_none_for_missing() -> None:
    cache: InMemoryCache = InMemoryCache()
    assert cache.get(_uuid.uuid4()) is None


def test_inmemory_cache_set_and_get() -> None:
    cache: InMemoryCache = InMemoryCache(cache_every_x_version=1)
    foo = Foo.create(_uuid.uuid4(), "test")
    cache.set(foo.id, foo)

    retrieved = cache.get(foo.id)
    assert retrieved is not None
    assert retrieved.id == foo.id
    assert retrieved.name == foo.name
    assert retrieved.version == foo.version


def test_inmemory_cache_returns_copy() -> None:
    cache: InMemoryCache = InMemoryCache(cache_every_x_version=1)
    foo = Foo.create(_uuid.uuid4(), "test")
    cache.set(foo.id, foo)

    retrieved = cache.get(foo.id)
    assert retrieved is not foo  # Should be a different object (deserialized)


def test_inmemory_cache_every_x_versions() -> None:
    cache: InMemoryCache = InMemoryCache(cache_every_x_version=5)
    foo = Foo.create(_uuid.uuid4(), "test")
    assert foo.version == 1

    # First set should always work
    cache.set(foo.id, foo)
    assert cache.get(foo.id) is not None
    assert cache.get(foo.id).version == 1

    # Update to version 3 - should not update cache (diff < 5)
    foo.rename("v2")
    foo.rename("v3")
    assert foo.version == 3
    cache.set(foo.id, foo)
    assert cache.get(foo.id).version == 1  # Still version 1

    # Update to version 6 - should update cache (diff >= 5)
    foo.rename("v4")
    foo.rename("v5")
    foo.rename("v6")
    assert foo.version == 6
    cache.set(foo.id, foo)
    assert cache.get(foo.id).version == 6  # Now version 6


def test_repo_without_cache(identifier, db_engine) -> None:
    store = MessageStore[FooEvent](
        name=identifier(),
        engine=db_engine,
        serializer=PydanticMessageSerializer(FooEvent),
    )
    repo = FooRepoWithCache(store)

    foo = Foo.create(_uuid.uuid4(), "test")
    repo.add(foo)

    retrieved = repo.get(foo.id)
    assert retrieved == foo


def test_repo_with_cache(identifier, db_engine) -> None:
    store = MessageStore[FooEvent](
        name=identifier(),
        engine=db_engine,
        serializer=PydanticMessageSerializer(FooEvent),
    )
    cache: InMemoryCache = InMemoryCache(cache_every_x_version=1)
    repo = FooRepoWithCache(store, cache=cache)

    foo = Foo.create(_uuid.uuid4(), "test")
    repo.add(foo)

    # First get - cache miss, should populate cache
    retrieved = repo.get(foo.id)
    assert retrieved == foo

    # Second get - should hit cache
    retrieved2 = repo.get(foo.id)
    assert retrieved2 == foo


def test_cache_is_updated_with_new_events(identifier, db_engine) -> None:
    store = MessageStore[FooEvent](
        name=identifier(),
        engine=db_engine,
        serializer=PydanticMessageSerializer(FooEvent),
    )
    cache: InMemoryCache = InMemoryCache(cache_every_x_version=1)
    repo = FooRepoWithCache(store, cache=cache)

    foo = Foo.create(_uuid.uuid4(), "test")
    repo.add(foo)

    # Get to populate cache
    retrieved = repo.get(foo.id)
    assert retrieved.version == 1

    # Add more events directly to store
    foo.rename("updated")
    repo.save(foo, expected_version=1)

    # Get should return updated version (cache + new events)
    retrieved2 = repo.get(foo.id)
    assert retrieved2.version == 2
    assert retrieved2.name == "updated"


def test_cache_hit_replays_only_new_events(identifier, db_engine) -> None:
    store = MessageStore[FooEvent](
        name=identifier(),
        engine=db_engine,
        serializer=PydanticMessageSerializer(FooEvent),
    )
    cache: InMemoryCache = InMemoryCache(cache_every_x_version=1)
    repo = FooRepoWithCache(store, cache=cache)

    # Create foo with multiple events
    foo = Foo.create(_uuid.uuid4(), "v1")
    foo.rename("v2")
    foo.rename("v3")
    repo.add(foo)
    assert foo.version == 3

    # Populate cache
    retrieved = repo.get(foo.id)
    assert retrieved.version == 3

    # Add more events
    foo.rename("v4")
    foo.rename("v5")
    repo.save(foo, expected_version=3)

    # Get should work correctly
    retrieved2 = repo.get(foo.id)
    assert retrieved2.version == 5
    assert retrieved2.name == "v5"

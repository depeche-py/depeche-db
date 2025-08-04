# 0.12.3

* Add internal storage of maximum position per partition on aggregated stream.
  This should fix a lot of performance issues.

# 0.12.2

* Fix silent failure in PgNotificationListener

# 0.12.1

* Add experimental `ThreadedExecutor`.

# 0.12.0

* Simplify partition selection in subscriptions
    * We do NOT guarantee anymore that the partition with the oldest message is read next.
    * This improves performance a lot.
* **BREAKING CHANGE**: Simplify interface of `AggregatedStream.get_partition_statistics`
    * Removed arguments `position_limits` and `ignore_partitions`.

# 0.11.0

* Improved performance of aggregated stream projection.
* Fixed bug in return of AggregatedStream.projector.run which would always re-run it in the executor right away.
* **BREAKING CHANGE**: Extended the RunOnNotification protocol
    * Added `interested_in_notification(dict) -> bool`
    * Added `take_notification_hint(dict) -> bool`
* **BREAKING CHANGE**: Changed DB schema of aggregated streams
    * Added columns; you will have to do a downtime deployment for these changes
    * Use the following command to generate a migration script:
      `python -m depeche_db generate-migration-script <PREV-VERSION> 0.11 --message-store=<NAME> --aggregated-stream=<STREAM-NAME> --aggregated-stream=<ANOTHER...>`

# 0.10.3

* Use one insert operation per batch in aggregated stream update instead of one per message

# 0.10.2

* Make exception on Message ID mismatch in `MessageStore.synchronize` more specific.

# 0.10.1

* Fix performance of aggregated stream projection

# 0.10.0

* Add batched acknowledgement strategy
* Performance improvements
    * Use less connections on load in subscription
    * Removed overly defensive check in acknowledgement operation
    * Pre-calculate hot SqlA queries
    * Improved Performance in pydantic (de)serialization

# 0.9.1

* Fix passing of batch size in stream/subscription factory

# 0.9.0

* Add a stateful reader to aggregated streams
* Make RunOnNotification implementations fair by exiting after time budget is used.

# 0.8.3

* Logging fix in LogAndIgnore error handler

# 0.8.2 (never released)

* Compatibility with psycopg 2 and 3

# 0.8.1

* Remove direct dependency on psycopg2 -> allow using psycopg2-binary as well

# 0.8.0

* Allow `ack` in client transaction: This allows for `exactly once` delivery semantics
* **BREAKING CHANGE**: Change DB object names
    * AggregatedStream: `{name}_projected_stream` -> `depeche_stream_{name}`
        * see `AggregatedStream.[get_migration_ddl|migrate_db_objects]`
    * MessageStore (ie. Storage): `{name}_messages` -> `depeche_msgs_{name}`
        * see `Storage.[get_migration_ddl|migrate_db_objects]`
    * DbSuscriptionStateProvider: `{name}_subscription_state` -> `depeche_subscriptions_{name}`
        * see `DbSuscriptionStateProvider.[get_migration_ddl|migrate_db_objects]`

# 0.7.1

* Add `global_position_to_positions` method on aggregated stream

# 0.7.0

* Add `start_point` to subscriptions
* Add three implementations for the subscription start point
    * Default: Beginning of aggregated stream
    * Next message
    * Point in time

# 0.6.2

* Add `register_manual` method to `MessageHandlerRegister`

# 0.6.1

* Add small event sourcing lib

# 0.6.0

* Nicer interface to create `AggregatedStream` & `Subscription`
* Move runner back onto `Subscription`
* Improved exceptions

# 0.5.0

* Fix excessive CPU usage caused by `threading.Event.wait`
* Add API docs
* Split `SubscriptionHandler` into 3 classes and invert dependency to `Subscription`
    * `MessageHandlerRegister` allows registering and retrieving handlers using type hints
    * `SubscriptionMessageHandler` uses register to message handling (including call middleware & error handling)
    * `SubscriptionRunner` allows contionously running the message handler on a `Subscription`
* Add class-based `MessageHandler` which wraps a `MessageHandlerRegister` and
  implements the same interface


# 0.4.5

* Expose `RunOnNotification` and `CallMiddleware`

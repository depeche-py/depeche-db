# 0.8.2

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

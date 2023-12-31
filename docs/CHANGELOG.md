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

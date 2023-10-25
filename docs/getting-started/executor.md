# Executor

Executing the regular jobs of

* updating aggregated streams
* running handlers on subscriptions

by hand is cumbersome. `StreamProjector` and `SubscriptionHandler` both
implement the `RunOnNotification` interface which allows the executor to
determine when to run them based on notifications sent by the message store and
the aggregated stream.

Given our objects from the previous chapters, we can use the executor like this:

```python
from depeche_db import Executor

executor = Executor(db_dsn=DB_DSN)
executor.register(aggregated_stream.projector)
executor.register(subscription.runner)

# this will run until stopped via SIGINT etc
executor.run()
```

You can run multiple instances of the same executor on any number of machines,
as long as they can talk to the same PostgreSQL database.

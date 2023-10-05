# Executor

Executing the regular jobs of

* updating aggregated streams
* running handlers on subscriptions

by hand is cumbersome.

Here is how we would do this:


```python
from depeche_db import Executor

executor = Executor(db_dsn=DB_DSN)
executor.register(stream_projector)
executor.register(my_handler)

# this will run until stopped via SIGINT etc
executor.run()
```

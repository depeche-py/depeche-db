# Performance considerations


## Bad Performance when using connection pooling & psycopg3

Depeche DB issues the same queries over and over again. This will cause
`psycopg3` (in default configuration) to [create prepared
statements](https://www.psycopg.org/psycopg3/docs/advanced/prepare.html#). The
query plan for these prepared statements is cached on a connection level. If
this cached plan becomes non-optimal because the input variables change, it
will *NOT* notice. Using connection pooling adds to the problem, because
connections live pretty long in a default pool and the caches are not reset
when the connections are returned to the pool.

There are three ways of dealing with this problem at the moment:

1. Disable the prepared statements altogether: `engine = create_engine(...,
   connect_args={"prepare_threshold": None})`.
2. Set up a [custom reset-on-return
   scheme](https://docs.sqlalchemy.org/en/20/core/pooling.html#custom-reset-on-return-schemes)
   that runs `DEALLOCATE ALL` to get rid of prepared statements (and their
   cached query plans) when connections are returned to the pool.
3. You can also set a low (some seconds) value for `pool_recycle` so that
   connections do not get very old in the pool and cannot accumulate that much
   of a cache of (bad) query plans.

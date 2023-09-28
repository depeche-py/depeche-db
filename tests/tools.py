import uuid as _uuid

import sqlalchemy as _sa


def pg_check_if_db_exists(uri: str) -> bool:
    with pg_get_connection(uri, use_postgres_db=True) as conn:
        db_name = _get_db_name(uri)
        return (  # type: ignore
            conn.execute(
                _sa.text("select count(*) from pg_database where datname = :db_name"),
                {"db_name": db_name},
            ).scalar()
            > 0
        )


def pg_create_db(uri: str):
    db_name = _get_db_name(uri)
    print(f"Creating database {db_name}")
    with pg_get_connection(uri, use_postgres_db=True) as conn:
        conn.execute(_sa.text("ROLLBACK"))
        # transaction = conn.begin()
        conn.execute(_sa.text(f"CREATE DATABASE {db_name}"))
        # transaction.commit()
        print(f"Created database {db_name}")


def pg_drop_db(uri: str):
    db_name = _get_db_name(uri)
    print(f"Dropping database {db_name}")
    with pg_get_connection(uri, use_postgres_db=True) as conn:
        conn.execute(_sa.text("ROLLBACK"))
        # transaction = conn.begin()
        conn.execute(_sa.text(f"DROP DATABASE {db_name}"))
        # transaction.commit()
        print(f"Dropped database {db_name}")


def pg_get_connection(uri: str, use_postgres_db: bool = False) -> _sa.engine.Connection:
    from urllib.parse import urlparse, urlunparse

    if use_postgres_db:
        parts = list(urlparse(uri))
        parts[2] = "/postgres"
        uri = urlunparse(parts)

    return _sa.create_engine(uri).connect()


def _get_db_name(uri: str) -> str:
    from urllib.parse import urlparse

    return urlparse(uri).path[1:]


def identifier() -> str:
    return f"id-{_uuid.uuid4()}".replace("-", "_")

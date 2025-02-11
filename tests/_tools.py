import uuid as _uuid
from urllib import parse as _urlparse


def pg_check_if_db_exists(uri: str) -> bool:
    conn = pg_get_connection(uri, use_postgres_db=True)
    db_name = _get_db_name(uri)
    count: int = conn.execute(
        "select count(*) from pg_database where datname = %(db_name)s",
        {"db_name": db_name},
    ).fetchone()[0]
    return count > 0


def pg_create_db(uri: str):
    db_name = _get_db_name(uri)
    print(f"Creating database {db_name}")
    with pg_get_connection(uri, use_postgres_db=True) as conn:
        conn.execute(f"CREATE DATABASE {db_name}")
        print(f"Created database {db_name}")


def pg_drop_db(uri: str):
    db_name = _get_db_name(uri)
    print(f"Dropping database {db_name}")
    with pg_get_connection(uri, use_postgres_db=True) as conn:
        conn.execute(f"DROP DATABASE {db_name}")
        print(f"Dropped database {db_name}")


def pg_get_connection(uri: str, use_postgres_db: bool = False):
    from depeche_db._compat import psycopg

    parts = list(_urlparse.urlparse(uri))
    if parts[0] == "postgresql+psycopg" or parts[0] == "postgresql+psycopg2":
        parts[0] = "postgresql"
    if use_postgres_db:
        parts[2] = "/postgres"
    uri = _urlparse.urlunparse(parts)

    # autocommit=True is necessary for creating/dropping databases.
    return psycopg.connect(uri, autocommit=True)


def _get_db_name(uri: str) -> str:
    from urllib.parse import urlparse

    return urlparse(uri).path[1:]


def identifier() -> str:
    return f"id-{_uuid.uuid4()}".replace("-", "_")

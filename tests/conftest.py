import os

import psycopg
import pytest

DSN = os.getenv('PG_DSN', 'postgresql://json:json@localhost:5432/jsonstorage')


pytest_plugins = ('tests.fixtures.db',)


@pytest.fixture(autouse=True)
def cleanup_db_after_test():
    # код до yield выполняется перед тестом
    yield
    # код после yield выполняется после теста

    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute('drop table if exists json_buffer cascade;')
        cur.execute('drop table if exists json_chunks cascade;')

        cur.execute(
            """
            select tablename
            from pg_tables
            where schemaname = 'public'
              and tablename like 'ns\_%\_metadata' escape '\\';
            """
        )
        tables = [row[0] for row in cur.fetchall()]

        for table in tables:
            cur.execute(f'drop table if exists "{table}" cascade;')

        conn.commit()

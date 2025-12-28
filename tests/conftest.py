import pytest_asyncio
from elasticsearch import AsyncElasticsearch

from json_storage.settings import settings
import psycopg
import pytest

DSN = settings.postgres.dsn

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


@pytest.fixture
def index_for_test():
    return 'test-index'


@pytest.fixture
def elastic_dsn():
    return settings.elastic_search.dsn


@pytest_asyncio.fixture
async def es_client(elastic_dsn):
    client = AsyncElasticsearch(elastic_dsn)
    yield client
    await client.close()


@pytest_asyncio.fixture
async def clean_index(elasticsearch_repo, index_for_test):
    await elasticsearch_repo.delete_index(index_for_test)
    await elasticsearch_repo.create_index(index_for_test)
    yield
    await elasticsearch_repo.delete_index(index_for_test)

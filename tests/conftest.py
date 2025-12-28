import pytest_asyncio
from elasticsearch import AsyncElasticsearch

from json_storage.services import MultiRepositoryService
from json_storage.settings import settings
import psycopg
import pytest

DSN = settings.postgres.dsn

pytest_plugins = ('tests.fixtures.db', 'tests.fixtures.taskiq')


@pytest.fixture(autouse=True)
def cleanup_postgres_after_test():
    # код до yield выполняется перед тестом
    yield
    # код после yield выполняется после теста

    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute('truncate table json_chunks cascade;')

        cur.execute(
            """
            select tablename
            from pg_tables
            where schemaname = 'public'
              and tablename like 'ns\\_%\\_metadata' escape '\\';
            """
        )
        tables = [row[0] for row in cur.fetchall()]

        for table in tables:
            cur.execute(f'drop table if exists "{table}" cascade;')

        conn.commit()

    MultiRepositoryService.NAMESPACES.clear()
    MultiRepositoryService.SEARCH_SCHEMAS.clear()


@pytest_asyncio.fixture(autouse=True)
async def cleanup_elasticsearch_after_test(es_client):
    """
    Фикстура для очистки тестовых индексов Elasticsearch после каждого теста.
    Удаляет все индексы, начинающиеся с 'test-'.
    """
    # Код до yield выполняется перед тестом
    yield
    # Код после yield выполняется после теста

    try:
        # Получаем список всех индексов
        indices = await es_client.indices.get(index='*')

        # Удаляем каждый тестовый индекс
        for index_name in indices.keys():
            await es_client.indices.delete(index=index_name, ignore_unavailable=True)

    except Exception as e:
        # Если нет индексов с префиксом test-*, просто игнорируем
        if 'index_not_found_exception' not in str(e):
            print(f'Warning: Failed to cleanup Elasticsearch indices: {e}')


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
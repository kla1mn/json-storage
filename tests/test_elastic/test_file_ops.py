import pytest
import pytest_asyncio

from json_storage.settings import settings
from elasticsearch import AsyncElasticsearch


DSN = settings.elastic_search.dsn
TEST_INDEX = 'test-index-repo'


@pytest_asyncio.fixture(scope='session')
async def es_client():
    client = AsyncElasticsearch(DSN)
    yield client
    await client.close()


@pytest_asyncio.fixture
async def clean_index(elasticsearch_repo):
    await elasticsearch_repo.delete_index(TEST_INDEX)
    await elasticsearch_repo.create_index(TEST_INDEX)
    yield
    await elasticsearch_repo.delete_index(TEST_INDEX)


@pytest.mark.asyncio
async def test_create_index(elasticsearch_repo, es_client):
    await elasticsearch_repo.delete_index(TEST_INDEX)
    await elasticsearch_repo.create_index(TEST_INDEX)

    exists = await es_client.indices.exists(index=TEST_INDEX)
    assert exists.body is True


@pytest.mark.asyncio
async def test_add_and_get_document(elasticsearch_repo, clean_index):
    ok = await elasticsearch_repo.insert_document(TEST_INDEX, '1', {'a': 1})
    assert ok is True

    doc = await elasticsearch_repo.get_document(TEST_INDEX, '1')
    assert doc == {'a': 1}


@pytest.mark.asyncio
async def test_delete_document(elasticsearch_repo, clean_index):
    await elasticsearch_repo.insert_document(TEST_INDEX, '1', {'a': 1})

    ok = await elasticsearch_repo.delete_document(TEST_INDEX, '1')
    assert ok is True

    doc = await elasticsearch_repo.get_document(TEST_INDEX, '1')
    assert doc is None

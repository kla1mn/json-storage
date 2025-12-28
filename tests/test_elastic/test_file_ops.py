import pytest


@pytest.mark.asyncio
async def test_create_index(elasticsearch_repo, es_client, index_for_test):
    await elasticsearch_repo.delete_index(index_for_test)
    await elasticsearch_repo.create_index(index_for_test)

    exists = await es_client.indices.exists(index=index_for_test)
    assert exists.body is True


@pytest.mark.asyncio
async def test_add_and_get_document(elasticsearch_repo, clean_index, index_for_test):
    ok = await elasticsearch_repo.insert_document(index_for_test, '1', {'a': 1})
    assert ok is True

    doc = await elasticsearch_repo.get_document(index_for_test, '1')
    assert doc == {'a': 1}


@pytest.mark.asyncio
async def test_delete_document(elasticsearch_repo, clean_index, index_for_test):
    await elasticsearch_repo.insert_document(index_for_test, '1', {'a': 1})

    ok = await elasticsearch_repo.delete_document(index_for_test, '1')
    assert ok is True

    doc = await elasticsearch_repo.get_document(index_for_test, '1')
    assert doc is None

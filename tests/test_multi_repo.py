import uuid

import pytest

from json_storage.services import MultiRepositoryService


@pytest.fixture
def namespace():
    return 'namespace_test'


@pytest.mark.asyncio
async def test_set_many_search_schema_and_search(
    multi_repository_service: MultiRepositoryService,
    elasticsearch_repo,
    namespace,
):
    search_schema = {
        'status': '$.status',
        'userId': '$.user.id',
    }
    document = {'status': 'active', 'user.id': 'user_12345', 'a': 'мяу', 'b': 'мур'}
    doc_id = f'{uuid.uuid4()}'
    await multi_repository_service.set_search_schema(namespace, search_schema)
    await elasticsearch_repo.insert_document(namespace, doc_id, document)
    search_schema['a'] = '$.a'
    await multi_repository_service.set_search_schema(namespace, search_schema)
    search_schema['b'] = '$.b'
    await multi_repository_service.set_search_schema(namespace, search_schema)
    docs = await multi_repository_service.search_objects(namespace, '$.b == "мур"')
    assert docs == [document]

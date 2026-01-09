import pytest

from json_storage.services import MultiRepositoryService


@pytest.fixture
def namespace():
    return 'namespace_test'


async def _body_bytes(raw: bytes):
    yield raw


@pytest.mark.asyncio
async def test_set_many_search_schema_and_search(
    multi_repository_service: MultiRepositoryService,
    postgres_repo,
    elasticsearch_repo,
    namespace,
):
    search_schema = {
        'status': '$.status',
        'userId': '$.user.id',
    }
    document_dict = {
        'status': 'active',
        'user.id': 'user_12345',
        'a': 'mau',
        'b': 'mur',
    }
    document = b'{"status": "active", "user.id": "user_12345", "a": "mau", "b": "mur"}'
    await multi_repository_service.create_object_stream(namespace, _body_bytes(document), document_name='doc_name')
    search_schema['a'] = '$.a'
    await multi_repository_service.set_search_schema(namespace, search_schema)
    search_schema['b'] = '$.b'
    await multi_repository_service.set_search_schema(namespace, search_schema)
    docs = await multi_repository_service.search_objects(namespace, '$.b == "mur"', 100, 0)
    assert len(docs) == 1
    doc = docs[0]
    assert (doc_id := doc.get('id'))
    assert document_dict == await elasticsearch_repo.get_document(namespace, doc_id)

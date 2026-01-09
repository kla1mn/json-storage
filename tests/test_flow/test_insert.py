import pytest


async def _body_bytes(raw: bytes):
    yield raw


@pytest.mark.asyncio
async def test_insert_document_in_reindex_namespace(
    multi_repository_service, elasticsearch_repo, es_client, redis_repo
):
    # Вставляем документ
    namespace_for_test = 'test-namespace'
    document = b'{"a": "52", "name": "Hi", "mau": "mau"}'
    doc_id = str(
        await multi_repository_service.create_object_stream(
            namespace_for_test, _body_bytes(document), document_name='doc'
        )
    )
    assert await elasticsearch_repo.get_document(namespace_for_test, doc_id)

    # Настраиваем поисковую схему
    search_schema = {
        'a': '$.a',
        'name': '$.name',
    }
    await multi_repository_service.set_search_schema(namespace_for_test, search_schema)
    created_index = await redis_repo.get_from_dict(
        elasticsearch_repo.MAPPING_NAMESPACE_INDEX_DICT_NAME, namespace_for_test
    )
    assert created_index

    # Проверяем, что поисковая схема действительно установилась
    mapping = await es_client.indices.get_mapping(index=created_index)
    physical_index_name = list(mapping.keys())[0]
    props = mapping[physical_index_name]['mappings']['properties']
    assert 'a' in props
    assert props['a']['type'] == 'keyword'
    assert 'name' in props
    assert props['name']['type'] == 'keyword'

    # Ищем наш документ
    body_for_search = {'query': {'term': {'a': '52'}}}
    docs = await elasticsearch_repo.search_in_index(namespace_for_test, body_for_search)
    assert len(docs) == 1
    assert docs[0] == {'a': '52', 'name': 'Hi', 'mau': 'mau'}

    # Эмулируем ситуацию, что идёт переиндексация
    new_index = 'reindex'
    mappings = {
        'mappings': {
            'dynamic': False,
            'properties': {
                'a': {'type': 'keyword'},
                'name': {'type': 'keyword'},
                'mau': {'type': 'keyword'},
            },
        },
    }
    async with elasticsearch_repo.get_client() as client:
        await client.indices.create(index=new_index, body=mappings)
    await redis_repo.add_to_dict(elasticsearch_repo.REINDEX_NAMESPACES_DICT_NAME, namespace_for_test, new_index)

    # Вставляем новый документ
    new_document = b'{"a": "42", "name": "Hi", "mau": "miu"}'
    await multi_repository_service.create_object_stream(
        namespace_for_test, _body_bytes(new_document), document_name='new_doc'
    )
    # Ищем вставленные документы в два индекса
    body_for_search = {'query': {'term': {'a': '42'}}}
    async with elasticsearch_repo.get_client() as client:
        resp = await client.search(index=created_index, body=body_for_search)
        first_doc = [hit['_source'] for hit in resp.body['hits']['hits']]
    async with elasticsearch_repo.get_client() as client:
        resp = await client.search(index=new_index, body=body_for_search)
        second_doc = [hit['_source'] for hit in resp.body['hits']['hits']]
    assert first_doc == second_doc

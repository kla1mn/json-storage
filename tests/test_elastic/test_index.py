import uuid
import pytest


@pytest.fixture
def mappings_for_test():
    return {
        'mappings': {
            'dynamic': False,
            'properties': {
                'a': {'type': 'integer'},
                'name': {'type': 'keyword'},
            },
        }
    }


@pytest.mark.asyncio
async def test_create_or_update_index_with_mappings(
    elasticsearch_repo, es_client, index_for_test, mappings_for_test
):
    await elasticsearch_repo.create_or_update_index(
        index_for_test,
        mappings=mappings_for_test,
    )

    mapping = await es_client.indices.get_mapping(index=index_for_test)
    props = mapping[index_for_test]['mappings']['properties']

    assert 'a' in props
    assert props['a']['type'] == 'integer'

    assert 'name' in props
    assert props['name']['type'] == 'keyword'


@pytest.mark.asyncio
async def test_set_document_in_elasticsearch(
    elasticsearch_repo, es_client, index_for_test, mappings_for_test
):
    await elasticsearch_repo.create_or_update_index(
        index_for_test,
        mappings=mappings_for_test,
    )
    doc_id = f'{uuid.uuid4()}'
    document = {'a': 52, 'name': 'Привет Эластик!'}
    await elasticsearch_repo.insert_document(index_for_test, doc_id, document)
    fetched_document = await elasticsearch_repo.get_document(index_for_test, doc_id)
    assert document == fetched_document


@pytest.mark.asyncio
async def test_search_document_in_elasticsearch(
    elasticsearch_repo, es_client, index_for_test, mappings_for_test
):
    await elasticsearch_repo.create_or_update_index(
        index_for_test,
        mappings=mappings_for_test,
    )
    doc_id = f'{uuid.uuid4()}'
    document = {'a': 52, 'name': 'Привет Эластик!', 'mau': 'МЯЯЯЯУУУУ'}
    await elasticsearch_repo.insert_document(index_for_test, doc_id, document)
    body_for_search = {'query': {'term': {'a': 52}}}
    docs = await elasticsearch_repo.search_in_index(index_for_test, body_for_search)
    assert docs == [document]


@pytest.mark.asyncio
async def test_not_existent_document_in_elasticsearch(
    elasticsearch_repo, es_client, index_for_test, mappings_for_test
):
    await elasticsearch_repo.create_or_update_index(
        index_for_test,
        mappings=mappings_for_test,
    )
    body_for_search = {'query': {'term': {'a': 52}}}
    docs = await elasticsearch_repo.search_in_index(index_for_test, body_for_search)
    assert docs == []


@pytest.mark.asyncio
async def test_search_in_not_index_field_in_elasticsearch(
    elasticsearch_repo, es_client, index_for_test, mappings_for_test
):
    await elasticsearch_repo.create_or_update_index(
        index_for_test,
        mappings=mappings_for_test,
    )
    doc_id = f'{uuid.uuid4()}'
    document = {'a': 52, 'name': 'Привет Эластик!', 'mau': 'МЯЯЯЯУУУУ'}
    await elasticsearch_repo.insert_document(index_for_test, doc_id, document)
    body_for_search = {'query': {'term': {'mau': 'МЯЯЯЯУУУУ'}}}
    docs = await elasticsearch_repo.search_in_index(index_for_test, body_for_search)
    assert docs == []


@pytest.mark.asyncio
async def test_list_document_in_elasticsearch(
    elasticsearch_repo, es_client, index_for_test
):
    mappings_for_test = {
        'mappings': {
            'dynamic': False,
            'properties': {
                'a': {'type': 'integer'},
                'name': {'type': 'keyword'},
            },
        },
    }
    await elasticsearch_repo.create_or_update_index(
        index_for_test,
        mappings=mappings_for_test,
    )
    documents = [
        {'a': [52, 100], 'name': 'Привет Эластик!', 'mau': 'МЯЯЯЯУУУУ'},
        {'a': [1, 100], 'name': 'Привет Эластик!', 'mau': 'МЯЯЯЯУУУУ'},
        {'a': [2], 'name': '<UNK> <UNK>!', 'mau': '<UNK>'},
        {'a': [10000], 'name': 'Привет Эластик!', 'mau': 'МЯЯЯЯУУУУ'},
        {'a': [], 'name': 'Привет Эластик!', 'mau': 'МЯЯЯЯУУУУ'},
    ]
    for doc in documents:
        doc_id = f'{uuid.uuid4()}'
        await elasticsearch_repo.insert_document(index_for_test, doc_id, doc)
    body_for_search = {'query': {'terms': {'a': [52, 1, 10000, 102]}}}
    docs = await elasticsearch_repo.search_in_index(index_for_test, body_for_search)
    assert len(docs) == 3
    expected_result = [
        {'a': [52, 100], 'name': 'Привет Эластик!', 'mau': 'МЯЯЯЯУУУУ'},
        {'a': [1, 100], 'name': 'Привет Эластик!', 'mau': 'МЯЯЯЯУУУУ'},
        {'a': [10000], 'name': 'Привет Эластик!', 'mau': 'МЯЯЯЯУУУУ'},
    ]
    assert sorted(docs, key=lambda x: x['a'][0]) == sorted(
        expected_result, key=lambda x: x['a'][0]
    )

@pytest.mark.asyncio
async def test_reindex(elasticsearch_repo, es_client, index_for_test):
    first_mappings = {
        'mappings': {
            'dynamic': False,
            'properties': {
                'a': {'type': 'integer'},
                'name': {'type': 'keyword'},
            },
        },
    }
    first_create = await elasticsearch_repo.create_or_update_index(
        index_for_test,
        mappings=first_mappings,
    )
    assert first_create is None

    doc_id = f'{uuid.uuid4()}'
    document = {'a': 52, 'name': 'Привет Эластик!', 'mau': 44}
    await elasticsearch_repo.insert_document(index_for_test, doc_id, document)
    body_for_search_first = {'query': {'term': {'a': 52}}}
    docs = await elasticsearch_repo.search_in_index(index_for_test, body_for_search_first)
    assert docs == [document]

    second_mappings = {
        'mappings': {
            'dynamic': False,
            'properties': {
                'a': {'type': 'integer'},
                'name': {'type': 'keyword'},
                'mau': {'type': 'integer'},
            },
        },
    }
    second_create = await elasticsearch_repo.create_or_update_index(
        index_for_test,
        second_mappings
    )
    assert second_create and isinstance(second_create, str)
    body_for_search_second = {'query': {'term': {'mau': 44}}}
    docs = await elasticsearch_repo.search_in_index(index_for_test, body_for_search_second)
    assert docs == [document]

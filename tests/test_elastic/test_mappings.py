import pytest

from json_storage.settings import settings

DSN = settings.elastic_search.dsn
TEST_INDEX = 'test-index-mappings'


TEST_MAPPINGS = {
    'dynamic': False,
    'properties': {
        'a': {'type': 'integer'},
        'name': {'type': 'keyword'},
    },
}


@pytest.mark.asyncio
async def test_create_index_with_mappings(elasticsearch_repo, es_client):
    await elasticsearch_repo.delete_index(TEST_INDEX)

    await elasticsearch_repo.create_index(
        TEST_INDEX,
        mappings=TEST_MAPPINGS,
    )

    mapping = await es_client.indices.get_mapping(index=TEST_INDEX)
    props = mapping[TEST_INDEX]['mappings']['properties']

    assert 'a' in props
    assert props['a']['type'] == 'integer'

    assert 'name' in props
    assert props['name']['type'] == 'keyword'

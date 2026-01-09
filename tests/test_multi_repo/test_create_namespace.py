import pytest

from json_storage.errors import NotExistentReindexTaskError
from json_storage.schemas import ProgressBarSchema
from json_storage.schemas.progress_bar import ProgressStatusEnum


async def _body_bytes(raw: bytes):
    yield raw


@pytest.mark.asyncio
async def test_create_namespace(multi_repository_service):
    namespaces = ['namespace1', 'namespace2', 'namespace3']
    for i, namespace in enumerate(namespaces):
        all_namespaces = await multi_repository_service.create_namespace(namespace)
        assert len(all_namespaces) == i + 1
        assert sorted(namespaces[: i + 1]) == sorted(all_namespaces)


@pytest.mark.asyncio
async def test_progress_bar(multi_repository_service, elasticsearch_repo):
    namespace_for_test = 'test-namespace'
    document = b'{"a": "52", "name": "Hi", "mau": "mau"}'
    await multi_repository_service.create_object_stream(namespace_for_test, _body_bytes(document), document_name='doc')
    first_search_schema = {
        'a': '$.a',
        'name': '$.name',
    }
    await multi_repository_service.set_search_schema(namespace_for_test, first_search_schema)
    second_search_schema = {
        'a': '$.a',
    }
    await multi_repository_service.set_search_schema(namespace_for_test, second_search_schema)
    progress_bar = await multi_repository_service.get_progress_bar(namespace_for_test)
    assert ProgressBarSchema(status=ProgressStatusEnum.SUCCESS, percent=100) == progress_bar


@pytest.mark.asyncio
async def test_failed_progress_bar(multi_repository_service, elasticsearch_repo):
    namespace_for_test = 'test-namespace'
    document = b'{"a": "52", "name": "Hi", "mau": "mau"}'
    await multi_repository_service.create_object_stream(namespace_for_test, _body_bytes(document), document_name='doc')
    with pytest.raises(NotExistentReindexTaskError):
        await multi_repository_service.get_progress_bar(namespace_for_test)

import pytest


@pytest.mark.asyncio
async def test_create_namespace(multi_repository_service):
    namespaces = ['namespace1', 'namespace2', 'namespace3']
    for i, namespace in enumerate(namespaces):
        all_namespaces = await multi_repository_service.create_namespace(namespace)
        assert len(all_namespaces) == i + 1
        assert sorted(namespaces[: i + 1]) == sorted(all_namespaces)

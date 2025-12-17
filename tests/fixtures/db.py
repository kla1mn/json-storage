import pytest_asyncio
from typing import AsyncIterator

from dishka import AsyncContainer
from json_storage.container import get_container
from json_storage.repositories import PostgresDBRepository, ElasticSearchDBRepository
from json_storage.services import MultiRepositoryService
from json_storage.depends import provider


@pytest_asyncio.fixture
async def container() -> AsyncIterator[AsyncContainer]:
    async with get_container([provider]) as container:
        yield container


@pytest_asyncio.fixture
async def postgres_repo(container: AsyncContainer) -> PostgresDBRepository:
    return await container.get(PostgresDBRepository)


@pytest_asyncio.fixture
async def elasticsearch_repo(container: AsyncContainer) -> ElasticSearchDBRepository:
    return await container.get(ElasticSearchDBRepository)


@pytest_asyncio.fixture
async def multi_repository_service(container: AsyncContainer) -> MultiRepositoryService:
    return await container.get(MultiRepositoryService)

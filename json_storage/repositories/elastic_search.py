import asyncio
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Optional, ClassVar, AsyncGenerator

from json_storage.errors import ReindexNamespaceYetError

from elasticsearch import AsyncElasticsearch, NotFoundError

from json_storage.repositories.redis import RedisDBRepository
from json_storage.schemas.progress_bar import ProgressStatusEnum

JSONType = dict[str, Any]
MappingsType = dict[str, Any]


@dataclass
class ElasticSearchDBRepository:
    redis_repository: RedisDBRepository
    dsn: str
    _client: AsyncElasticsearch | None = field(init=False, default=None)
    MAPPING_NAMESPACE_INDEX_DICT_NAME: ClassVar[str] = 'elastic_namespaces'
    REINDEX_NAMESPACES_DICT_NAME: ClassVar[str] = 'reindex_namespaces'
    PROGRESS_BAR_DICT_NAME: ClassVar[str] = 'progress_bar'

    @asynccontextmanager
    async def get_client(self) -> AsyncGenerator[AsyncElasticsearch, None]:
        if self._client is None:
            self._client = AsyncElasticsearch(self.dsn)
        yield self._client

    async def _get_real_index(self, namespace: str) -> str:
        real_index = await self.redis_repository.get_from_dict(self.MAPPING_NAMESPACE_INDEX_DICT_NAME, namespace)
        if not real_index:
            raise RuntimeError(f'Не существует namespace: {namespace}')
        return real_index

    async def reindex_namespace(self, index: str, real_namespace: str, mappings: MappingsType) -> None:
        async with self.get_client() as client:
            new_index = f'{real_namespace}_{uuid.uuid4()}'
            try:
                await client.indices.create(index=new_index, body=mappings)
                await self.redis_repository.add_to_dict(self.REINDEX_NAMESPACES_DICT_NAME, real_namespace, new_index)
                reindex_body: dict[str, Any] = {
                    'source': {'index': index},
                    'dest': {'index': new_index},
                    'conflicts': 'proceed',
                }
                resp = await client.reindex(body=reindex_body, wait_for_completion=False, refresh=True)
                task_id = resp.get('task')
                await asyncio.gather(
                    self.redis_repository.add_to_dict(self.PROGRESS_BAR_DICT_NAME, real_namespace, task_id),
                    self.redis_repository.add_to_dict(
                        self.MAPPING_NAMESPACE_INDEX_DICT_NAME, real_namespace, new_index
                    ),
                )
                while True:
                    task = await client.tasks.get(task_id=task_id)
                    if task.get('completed'):
                        break
                    await asyncio.sleep(0.2)
                await client.indices.delete(index=index, ignore_unavailable=True)
            except Exception as exc:
                await client.indices.delete(index=new_index, ignore_unavailable=True)
                raise exc
            finally:
                await self.redis_repository.remove_from_dict(self.REINDEX_NAMESPACES_DICT_NAME, real_namespace)
                await client.close()

    async def create_or_update_index(
        self,
        namespace: str,
        mappings: MappingsType | None = None,
    ) -> None:
        if not (
            real_index := await self.redis_repository.get_from_dict(self.MAPPING_NAMESPACE_INDEX_DICT_NAME, namespace)
        ):
            await self.redis_repository.add_to_dict(self.MAPPING_NAMESPACE_INDEX_DICT_NAME, namespace, namespace)
            real_index = namespace
        if mappings is None:
            mappings = {'mappings': {'dynamic': True, 'properties': {}}}

        async with self.get_client() as client:
            try:
                _exists = await client.indices.exists(index=real_index)
                exists = bool(_exists.body) if hasattr(_exists, 'body') else bool(_exists)
            except Exception:
                exists = False

            if not exists:
                await client.indices.create(index=real_index, body=mappings)
                return
            if await self.redis_repository.get_from_dict(self.REINDEX_NAMESPACES_DICT_NAME, namespace):
                raise ReindexNamespaceYetError

            from json_storage.tasks import reindex_namespace

            await asyncio.gather(
                self.redis_repository.add_to_dict(self.REINDEX_NAMESPACES_DICT_NAME, namespace, ''),
                self.redis_repository.add_to_dict(self.PROGRESS_BAR_DICT_NAME, namespace, ProgressStatusEnum.INIT),
                reindex_namespace.kiq(index=real_index, real_namespace=namespace, mappings=mappings),
            )
            return

    async def insert_document(
        self,
        namespace: str,
        doc_id: str,
        document: JSONType,
        refresh: str | None = 'wait_for',
    ) -> bool:
        real_index = await self._get_real_index(namespace)
        if await self.redis_repository.get_from_dict(self.REINDEX_NAMESPACES_DICT_NAME, namespace):
            from json_storage.tasks import insert_in_reindex_namespace

            await insert_in_reindex_namespace.kiq(namespace, doc_id, document)

        async with self.get_client() as client:
            resp = await client.index(
                index=real_index,
                id=doc_id,
                document=document,
                refresh=refresh,
            )
            return resp.get('result') in ('created', 'updated')

    async def insert_in_index(
        self,
        index: str,
        doc_id: str,
        document: JSONType,
        refresh: str | None = 'wait_for',
    ) -> bool:
        async with self.get_client() as client:
            resp = await client.index(
                index=index,
                id=doc_id,
                document=document,
                refresh=refresh,
            )
            return resp.get('result') in ('created', 'updated')

    async def get_document(
        self,
        namespace: str,
        doc_id: str,
    ) -> Optional[JSONType]:
        real_index = await self._get_real_index(namespace)
        async with self.get_client() as client:
            try:
                resp = await client.get(index=real_index, id=doc_id)
            except NotFoundError:
                return None
            return resp.get('_source')

    async def delete_document(
        self,
        namespace: str,
        doc_id: str,
        refresh: str | None = 'wait_for',
    ) -> bool:
        real_index = await self._get_real_index(namespace)
        async with self.get_client() as client:
            try:
                resp = await client.delete(
                    index=real_index,
                    id=doc_id,
                    refresh=refresh,
                )
            except NotFoundError:
                return False
            return resp.get('result') == 'deleted'

    async def search_in_index(self, namespace: str, body: dict, size: int = 10, from_: int = 0) -> list[Any]:
        real_index = await self._get_real_index(namespace)
        async with self.get_client() as client:
            resp = await client.search(index=real_index, body=body, size=size, from_=from_)
            return [hit['_source'] for hit in resp.body['hits']['hits']]

    async def search_ids_in_index(self, namespace: str, body: dict, size: int = 10, from_: int = 0) -> list[str]:
        real_index = await self._get_real_index(namespace)
        async with self.get_client() as client:
            resp = await client.search(index=real_index, body=body, size=size, from_=from_)
            return [hit['_id'] for hit in resp.body['hits']['hits']]

    async def get_progress_bar_by_task_id(
        self,
        task_id: str,
    ) -> int:
        async with self.get_client() as client:
            task = await client.tasks.get(task_id=task_id)
            if task.get('completed'):
                return 100
            status = task.get('status')
            total = status.get('total')
            if not total:
                return 0
            return (status.get('created', 0) + status.get('updated', 0)) // total

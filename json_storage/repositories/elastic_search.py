from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Optional, ClassVar, AsyncGenerator

from json_storage.errors import ReindexNamespaceYetError
from json_storage.settings import settings
from elasticsearch import AsyncElasticsearch, NotFoundError


JSONType = dict[str, Any]
MappingsType = dict[str, Any]


@dataclass
class ElasticSearchDBRepository:
    NAMESPACES: ClassVar[dict[str, str]] = {}
    REINDEX_NAMESPACES: ClassVar[dict[str, str | None]] = {}
    url: str
    _client: AsyncElasticsearch | None = field(init=False, default=None)

    @asynccontextmanager
    async def get_client(self) -> AsyncGenerator[AsyncElasticsearch, None]:
        try:
            if self._client is None:
                self._client = AsyncElasticsearch(self.url)
            yield self._client
        finally:
            await self._client.close()
            self._client = None

    @classmethod
    def _get_real_index(cls, namespace: str) -> str:
        # В docker app и worker не делят память, поэтому не падаем,
        # а считаем, что физический индекс = namespace.
        return cls.NAMESPACES.setdefault(namespace, namespace)

    @classmethod
    async def reindex_namespace(
        cls, index: str, real_namespace: str, mappings: MappingsType
    ) -> None:
        client = AsyncElasticsearch(settings.elastic_search.dsn)
        new_index = f'{real_namespace}_{uuid.uuid4()}'
        try:
            await client.indices.create(index=new_index, body=mappings)
            cls.REINDEX_NAMESPACES[real_namespace] = new_index
            reindex_body: dict[str, Any] = {
                'source': {'index': index},
                'dest': {'index': new_index},
                'conflicts': 'proceed',
            }
            await client.reindex(
                body=reindex_body, wait_for_completion=True, refresh=True
            )
            cls.NAMESPACES[real_namespace] = new_index
        except Exception as exc:
            await client.indices.delete(index=new_index, ignore_unavailable=True)
            raise exc
        finally:
            cls.REINDEX_NAMESPACES.pop(real_namespace)
            await client.close()

    async def create_or_update_index(
        self,
        namespace: str,
        mappings: MappingsType | None = None,
    ) -> None:
        if not (real_index := self.NAMESPACES.get(namespace)):
            ElasticSearchDBRepository.NAMESPACES[namespace] = namespace
            real_index = namespace
        if mappings is None:
            mappings = {'mappings': {'dynamic': True, 'properties': {}}}

        async with self.get_client() as client:
            try:
                _exists = await client.indices.exists(index=real_index)
                exists = (
                    bool(_exists.body) if hasattr(_exists, 'body') else bool(_exists)
                )
            except Exception:
                exists = False

            if not exists:
                await client.indices.create(index=real_index, body=mappings)
                return

            # Вместо фонового reindex — обновляем mapping на месте
            await client.indices.put_mapping(
                index=real_index,
                body=mappings.get('mappings', mappings),
            )
            return

    async def insert_document(
        self,
        namespace: str,
        doc_id: str,
        document: JSONType,
        refresh: str | None = 'wait_for',
    ) -> bool:
        real_index = self._get_real_index(namespace)
        if namespace in self.REINDEX_NAMESPACES:
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
        real_index = self._get_real_index(namespace)
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
        real_index = self._get_real_index(namespace)
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

    async def search_in_index(
        self, namespace: str, body: dict, size: int = 10, from_: int = 0
    ) -> list[Any]:
        real_index = self._get_real_index(namespace)
        async with self.get_client() as client:
            resp = await client.search(
                index=real_index, body=body, size=size, from_=from_
            )
            return [hit['_source'] for hit in resp.body['hits']['hits']]

    async def search_ids_in_index(
            self, namespace: str, body: dict, size: int = 10, from_: int = 0
    ) -> list[str]:
        real_index = self._get_real_index(namespace)
        async with self.get_client() as client:
            resp = await client.search(index=real_index, body=body, size=size, from_=from_)
            return [hit['_id'] for hit in resp.body['hits']['hits']]

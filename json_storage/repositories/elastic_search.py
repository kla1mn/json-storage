from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Optional, ClassVar
from json_storage.settings import settings
from elasticsearch import AsyncElasticsearch, NotFoundError


JSONType = dict[str, Any]
MappingsType = dict[str, Any]


@dataclass
class ElasticSearchDBRepository:
    NAMESPACES: ClassVar[dict[str, str]] = {}
    REINDEX_NAMESPACE: ClassVar[set[str]] = set()
    url: str
    _client: AsyncElasticsearch | None = field(init=False, default=None)

    def _get_client(self) -> AsyncElasticsearch:
        if self._client is None:
            self._client = AsyncElasticsearch(self.url)
        return self._client

    @classmethod
    def _get_real_index(cls, namespace: str) -> str:
        real_index = cls.NAMESPACES.get(namespace)
        if not real_index:
            raise RuntimeError(f'Не существует namespace: {namespace}')
        return real_index

    @classmethod
    async def reindex_namespace(
        cls, index: str, real_namespace: str, mappings: MappingsType
    ) -> None:
        client = AsyncElasticsearch(settings.elastic_search.dsn)
        new_index = f'{real_namespace}_{uuid.uuid4()}'
        try:
            await client.indices.create(index=new_index, body=mappings)
            reindex_body: dict[str, Any] = {
                'source': {'index': index},
                'dest': {'index': new_index},
                'conflicts': 'proceed',
            }

            resp = await client.reindex(
                body=reindex_body, wait_for_completion=True, refresh=True
            )
            if isinstance(resp, dict):
                failures = resp.get('failures') or resp.get('failures', [])
                if failures:
                    try:
                        await client.indices.delete(
                            index=new_index, ignore_unavailable=True
                        )
                    except Exception:
                        pass
                    raise RuntimeError(f'Reindex finished with failures: {failures}')
            cls.NAMESPACES[real_namespace] = new_index
        except Exception as exc:
            await client.indices.delete(index=new_index, ignore_unavailable=True)
            raise exc
        finally:
            cls.REINDEX_NAMESPACE.remove(real_namespace)

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

        client = self._get_client()

        try:
            _exists = await client.indices.exists(index=real_index)
            exists = bool(_exists.body) if hasattr(_exists, 'body') else bool(_exists)
        except Exception:
            exists = False

        if not exists:
            await client.indices.create(index=real_index, body=mappings)
        if namespace in self.REINDEX_NAMESPACE:
            raise RuntimeError('Уже производится переиндексация')

        from json_storage.tasks import reindex_namespace
        self.REINDEX_NAMESPACE.add(namespace)
        await reindex_namespace.kiq(index=real_index, real_namespace=namespace, mappings=mappings)

    async def insert_document(
        self,
        namespace: str,
        doc_id: str,
        document: JSONType,
        refresh: str | None = 'wait_for',
    ) -> bool:
        real_index = self._get_real_index(namespace)
        client = self._get_client()
        resp = await client.index(
            index=real_index,
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
        client = self._get_client()
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
        client = self._get_client()
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
        client = self._get_client()
        resp = await client.search(index=real_index, body=body, size=size, from_=from_)
        return [hit['_source'] for hit in resp.body['hits']['hits']]

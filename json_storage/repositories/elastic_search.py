from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from elasticsearch import AsyncElasticsearch, NotFoundError


JSONType = dict[str, Any]
MappingsType = dict[str, Any]


@dataclass
class ElasticSearchDBRepository:
    url: str = 'http://localhost:9200'
    _client: AsyncElasticsearch | None = field(init=False, default=None)

    async def _get_client(self) -> AsyncElasticsearch:
        if self._client is None:
            self._client = AsyncElasticsearch(self.url)
        return self._client

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None

    async def create_index(
        self,
        index: str,
        mappings: MappingsType | None = None,
    ) -> None:
        client = await self._get_client()

        exists = await client.indices.exists(index=index)
        if exists.body is True:
            return

        body: dict[str, Any] = {}
        if mappings is not None:
            body['mappings'] = mappings

        await client.indices.create(
            index=index,
            **body,
        )

    async def delete_index(self, index: str) -> None:
        client = await self._get_client()
        try:
            await client.indices.delete(index=index)
        except NotFoundError:
            pass

    async def insert_document(
        self,
        index: str,
        doc_id: str,
        document: JSONType,
        refresh: str | None = 'wait_for',
    ) -> bool:
        client = await self._get_client()
        resp = await client.index(
            index=index,
            id=doc_id,
            document=document,
            refresh=refresh,
        )
        return resp.get('result') in ('created', 'updated')

    async def get_document(
        self,
        index: str,
        doc_id: str,
    ) -> Optional[JSONType]:
        client = await self._get_client()
        try:
            resp = await client.get(index=index, id=doc_id)
        except NotFoundError:
            return None
        return resp.get('_source')

    async def delete_document(
        self,
        index: str,
        doc_id: str,
        refresh: str | None = 'wait_for',
    ) -> bool:
        client = await self._get_client()
        try:
            resp = await client.delete(
                index=index,
                id=doc_id,
                refresh=refresh,
            )
        except NotFoundError:
            return False
        return resp.get('result') == 'deleted'

    async def search_in_index(
        self, index: str, body: dict, size: int = 10, from_: int = 0
    ) -> JSONType:
        client = await self._get_client()
        resp = await client.search(index=index, body=body, size=size, from_=from_)
        return resp.body

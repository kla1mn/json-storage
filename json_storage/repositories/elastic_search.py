from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

from elasticsearch import AsyncElasticsearch, NotFoundError


JSONType = dict[str, Any]
MappingsType = dict[str, Any]


@dataclass
class ElasticSearchDBRepository:
    url: str
    _client: AsyncElasticsearch | None = field(init=False, default=None)

    async def _get_client(self) -> AsyncElasticsearch:
        if self._client is None:
            self._client = AsyncElasticsearch(self.url)
        return self._client

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None

    async def create_or_update_index(
        self,
        index: str,
        mappings: MappingsType,
        *,
        wait_for_completion: bool = True,
        reindex_conflicts: str = 'proceed',
    ) -> str | None:
        """
        Попытка:
          1) если индекс не существует — создаём с маппингом и возвращаем None
          3) создаём новый индекс (без копирования settings),
             реиндексим документы и атомарно переключаем алиасы / удаляем старый индекс.
             Возвращаем имя нового индекса.
        """
        client = await self._get_client()

        async def _exists(idx: str) -> bool:
            try:
                res = await client.indices.exists(index=idx)
            except Exception:
                return False
            if hasattr(res, 'body'):
                return bool(res.body)
            return bool(res)

        # 1. Создаем индекс, если его нет
        exists = await _exists(index)
        if not exists:
            await client.indices.create(index=index, body=mappings)
            return None

        # 2. Создаем новый индекс для переиндексации
        new_index = f'{index}_reindexed_{uuid.uuid4().hex[:8]}'
        try:
            await client.indices.create(index=new_index, body=mappings)
        except Exception:
            raise
        reindex_body: dict[str, Any] = {
            'source': {'index': index},
            'dest': {'index': new_index},
            'conflicts': reindex_conflicts,
        }
        # 3. Реиндексируем
        try:
            await client.reindex(
                body=reindex_body, wait_for_completion=wait_for_completion, refresh=True
            )
        except Exception:
            try:
                await client.indices.delete(index=new_index)
            except Exception:
                pass
            raise

        # 4. Удаляем старый индекс
        try:
            await client.indices.delete(index=index)
        except NotFoundError:
            pass
        except Exception:
            pass

        # 5. Создаём алиас со старым именем → на новый индекс
        try:
            await client.indices.put_alias(index=new_index, name=index)
        except Exception:
            try:
                await client.indices.delete(index=new_index)
            except Exception:
                pass
            raise

        return new_index

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
    ) -> list[Any]:
        client = await self._get_client()
        resp = await client.search(index=index, body=body, size=size, from_=from_)
        return [hit['_source'] for hit in resp.body['hits']['hits']]

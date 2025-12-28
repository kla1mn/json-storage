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
        mappings: MappingsType | None = None,
        *,
        wait_for_completion: bool = True,
        reindex_conflicts: str = 'proceed',
    ) -> str:
        """
        Попытка:
          1) если индекс/алиас не существует — создаём индекс с уникальным именем и алиас на него
          2) создаём новый индекс, реиндексим документы и атомарно переключаем алиас.
             Возвращаем имя нового индекса.
        """
        if mappings is None:
            mappings = {
                "mappings": {
                    "dynamic": True,
                    "properties": {}
                }
            }
        client = await self._get_client()

        async def _exists(idx: str) -> bool:
            try:
                res = await client.indices.exists(index=idx)
            except Exception:
                return False
            if hasattr(res, 'body'):
                return bool(res.body)
            return bool(res)

        # Проверяем, существует ли индекс или алиас с таким именем
        exists = await _exists(index)

        if not exists:
            # 1. Создаем индекс с уникальным именем и алиас на него
            physical_index = f'{index}_{uuid.uuid4().hex[:8]}'
            await client.indices.create(index=physical_index, body=mappings)
            await client.indices.put_alias(index=physical_index, name=index)
            return physical_index

        # 2. Определяем реальные индексы за алиасом
        try:
            alias_info = await client.indices.get_alias(name=index)
            # Если index - это алиас, получаем список реальных индексов
            old_indices = (
                list(alias_info.body.keys())
                if hasattr(alias_info, 'body')
                else list(alias_info.keys())
            )
        except NotFoundError:
            # Это реальный индекс без алиаса (старая схема)
            # Нужно мигрировать: создать новый индекс и алиас
            old_indices = [index]
        except Exception:
            old_indices = [index]

        # 3. Создаем новый физический индекс
        new_index = f'{index}_{uuid.uuid4().hex[:8]}'
        try:
            await client.indices.create(index=new_index, body=mappings)
        except Exception:
            raise

        reindex_body: dict[str, Any] = {
            'source': {'index': index},
            'dest': {'index': new_index},
            'conflicts': reindex_conflicts,
        }

        # 4. Реиндексируем
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

        # 5. Атомарное переключение алиаса
        actions = []

        # Удаляем алиас со всех старых индексов (если он есть)
        for old_idx in old_indices:
            try:
                # Проверяем, есть ли алиас на этом индексе
                await client.indices.get_alias(index=old_idx, name=index)
                actions.append({'remove': {'index': old_idx, 'alias': index}})
            except NotFoundError:
                # Это случай, когда index - это реальный индекс, не алиас
                # Просто удалим его после создания алиаса
                pass
            except Exception:
                pass

        # Добавляем алиас на новый индекс
        actions.append({'add': {'index': new_index, 'alias': index}})

        try:
            await client.indices.update_aliases(body={'actions': actions})
        except Exception:
            try:
                await client.indices.delete(index=new_index)
            except Exception:
                pass
            raise

        # 6. Удаляем старые индексы
        for old_idx in old_indices:
            try:
                await client.indices.delete(index=old_idx)
            except NotFoundError:
                pass
            except Exception:
                pass

        return new_index

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

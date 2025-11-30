from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from elasticsearch import AsyncElasticsearch
from elasticsearch import NotFoundError


JSONType = dict[str, Any]


@dataclass
class ElasticSearchDBRepository:
    url: str = "http://localhost:9200"

    _client: AsyncElasticsearch | None = field(init=False, default=None)

    async def _get_client(self) -> AsyncElasticsearch:
        if self._client is None:
            self._client = AsyncElasticsearch(self.url)
        return self._client

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None

    async def index_document(
        self,
        index: str,
        doc_id: str,
        document: JSONType,
        refresh: str | None = "wait_for",
    ) -> bool:
        client = await self._get_client()
        resp = await client.index(
            index=index,
            id=doc_id,
            document=document,
            refresh=refresh,
        )

        body = getattr(resp, "body", resp)
        result = body.get("result")
        return result in ("created", "updated")

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

        body = getattr(resp, "body", resp)
        return body.get("_source")

    async def delete_document(
        self,
        index: str,
        doc_id: str,
        refresh: str | None = "wait_for",
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

        body = getattr(resp, "body", resp)
        return body.get("result") == "deleted"

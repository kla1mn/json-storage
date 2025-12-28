from __future__ import annotations

import json
from typing import Any

from json_storage.cmd.taskiq_broker import taskiq_broker
from json_storage.repositories import ElasticSearchDBRepository, PostgresDBRepository
from json_storage.settings import settings


async def _index_document_to_elastic_impl(namespace: str, object_id: str) -> None:
    postgres = PostgresDBRepository(dsn=settings.postgres.dsn)
    elastic = ElasticSearchDBRepository(url=settings.elastic_search.dsn)

    try:
        meta = await postgres.get_document_meta(namespace, object_id)
        if meta is None:
            return

        index_name = namespace
        await elastic.create_or_update_index(index=index_name)

        buf = bytearray()
        async for chunk in postgres.iter_chunks_by_id(object_id):
            buf.extend(chunk)

        payload: Any = json.loads(buf)
        if not isinstance(payload, dict):
            raise TypeError('Only JSON objects (dict) are supported for indexing')

        ok = await elastic.insert_document(
            index=index_name, doc_id=object_id, document=payload
        )
        if ok:
            await postgres.delete_chunks_by_id(object_id)
    finally:
        await postgres.aclose()
        await elastic.aclose()


@taskiq_broker.task(retry_on_error=True, max_retries=10)
async def index_document_to_elastic(namespace: str, object_id: str) -> None:
    await _index_document_to_elastic_impl(namespace=namespace, object_id=object_id)

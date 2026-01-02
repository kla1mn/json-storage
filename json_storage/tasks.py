from __future__ import annotations
from dishka import FromDishka
from dishka.integrations.taskiq import inject
import json
from typing import Any
from json_storage.cmd.taskiq_broker import taskiq_broker
from json_storage.repositories import ElasticSearchDBRepository, PostgresDBRepository

MappingsType = dict[str, Any]


@taskiq_broker.task(retry_on_error=True, max_retries=10)
@inject
async def index_document_to_elastic(
    namespace: str,
    object_id: str,
    postgres: FromDishka[PostgresDBRepository],
    elastic: FromDishka[ElasticSearchDBRepository],
) -> None:
    meta = await postgres.get_document_meta(namespace, object_id)
    if meta is None:
        return

    index_name = namespace
    await elastic.create_or_update_index(namespace=index_name)

    buf = bytearray()
    async for chunk in postgres.iter_chunks_by_id(object_id):
        buf.extend(chunk)

    payload: Any = json.loads(buf)
    if not isinstance(payload, dict):
        raise TypeError('Only JSON objects (dict) are supported for indexing')

    ok = await elastic.insert_document(
        namespace=index_name, doc_id=object_id, document=payload
    )
    if ok:
        await postgres.delete_chunks_by_id(object_id)


@taskiq_broker.task()
@inject
async def reindex_namespace(index: str, real_namespace: str, mappings: MappingsType, elastic: FromDishka[ElasticSearchDBRepository],) -> None:
    await elastic.reindex_namespace(index, real_namespace, mappings)

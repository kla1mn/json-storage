from __future__ import annotations
from dishka import FromDishka
from dishka.integrations.taskiq import inject
import json
from typing import Any
from json_storage.cmd.taskiq_broker import taskiq_broker
from json_storage.repositories import (
    ElasticSearchDBRepository,
    PostgresDBRepository,
    RedisDBRepository,
)
import asyncio

MappingsType = dict[str, Any]
JSONType = dict[str, Any]


@taskiq_broker.task(retry_on_error=True, max_retries=10)
@inject
async def index_document_to_elastic(
    namespace: str,
    object_id: str,
    postgres: FromDishka[PostgresDBRepository],
    elastic_repo: FromDishka[ElasticSearchDBRepository],
) -> None:
    meta = await postgres.get_document_meta(namespace, object_id)
    if meta is None:
        return
    index_name = namespace

    buf = bytearray()
    async for chunk in postgres.iter_chunks_by_id(object_id):
        buf.extend(chunk)

    payload: Any = json.loads(buf)
    if not isinstance(payload, dict):
        raise TypeError('Only JSON objects (dict) are supported for indexing')

    ok = await elastic_repo.insert_document(namespace=index_name, doc_id=object_id, document=payload)
    if ok:
        await postgres.delete_chunks_by_id(object_id)


@taskiq_broker.task()
@inject
async def reindex_namespace(
    index: str,
    real_namespace: str,
    mappings: MappingsType,
    elastic_repo: FromDishka[ElasticSearchDBRepository],
) -> None:
    await elastic_repo.reindex_namespace(index, real_namespace, mappings)


@taskiq_broker.task(retry_on_error=True, max_retries=10)
@inject
async def insert_in_reindex_namespace(
    real_namespace: str,
    doc_id: str,
    document: JSONType,
    elastic_repo: FromDishka[ElasticSearchDBRepository],
    redis_repo: FromDishka[RedisDBRepository],
) -> None:
    index = await redis_repo.get_from_dict(elastic_repo.REINDEX_NAMESPACES_DICT_NAME, real_namespace)
    if not index:
        await asyncio.sleep(3)
        raise RuntimeError('Ждем, когда инициализируется индекс.')
    await elastic_repo.insert_in_index(index, doc_id, document)


@taskiq_broker.task(retry_on_error=True, max_retries=10)
@inject
async def delete_document_from_elastic(
    namespace: str,
    object_id: str,
    elastic_repo: FromDishka[ElasticSearchDBRepository],
) -> None:
    await elastic_repo.delete_document(namespace, object_id, refresh=None)

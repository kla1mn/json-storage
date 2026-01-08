import asyncio
import json
from typing import Any, ClassVar, TypeVar
from uuid import UUID
from collections.abc import AsyncIterator
import uuid
from dataclasses import dataclass

from fastapi import HTTPException
from .dsl_translator import DSLTranslator
from json_storage.repositories import (
    PostgresDBRepository,
    ElasticSearchDBRepository,
    RedisDBRepository,
)
from json_storage.schemas import DocumentListSchema, DocumentSchema

JSONType = TypeVar('JSONType', bound=dict[str, Any])


@dataclass
class MultiRepositoryService:
    postgres_repository: PostgresDBRepository
    elastic_repository: ElasticSearchDBRepository
    redis_repository: RedisDBRepository
    is_init_chunks_table: ClassVar[bool] = False
    ALL_NAMESPACES_SET_NAME: ClassVar[str] = 'all_namespaces'
    SEARCH_SCHEMAS_DICT_NAME: ClassVar[str] = 'search_schemas'

    async def get_object_meta(self, namespace: str, object_id: UUID) -> DocumentSchema:
        meta = await self.postgres_repository.get_document_meta(
            namespace, str(object_id)
        )
        if meta is None:
            raise HTTPException(status_code=404)
        return meta

    async def get_object_body(self, namespace: str, object_id: UUID) -> dict[str, Any]:
        await self.get_object_meta(namespace, object_id)

        doc = await self.elastic_repository.get_document(
            namespace=namespace,
            doc_id=str(object_id),
        )
        if doc is None:
            raise HTTPException(status_code=202, detail='Документ ещё индексируется')

        return doc

    async def create_object_stream(
        self,
        namespace: str,
        body: AsyncIterator[bytes],
        *,
        document_name: str,
    ) -> UUID:
        if not await self.redis_repository.check_in_set(
            self.ALL_NAMESPACES_SET_NAME, namespace
        ):
            await self.create_namespace(namespace)

        doc = await self.postgres_repository.create_document_stream(
            namespace=namespace,
            document_name=document_name,
            body=body,
        )

        from json_storage.tasks import index_document_to_elastic

        await index_document_to_elastic.kiq(namespace=namespace, object_id=doc.id)

        return uuid.UUID(doc.id)

    async def delete_object_by_id(self, namespace: str, object_id: UUID) -> None:
        await asyncio.gather(
            self.postgres_repository.delete_object_by_id(namespace, str(object_id)),
            self.elastic_repository.delete_document(namespace, str(object_id)),
        )

    async def set_search_schema(
        self,
        namespace: str,
        search_schema: dict[str, Any],
    ) -> None:
        set_schema = await self.redis_repository.get_from_dict(
            self.SEARCH_SCHEMAS_DICT_NAME, namespace
        )
        if set_schema and json.loads(set_schema) == search_schema:
            return
        search_schema_json_str = json.dumps(search_schema)
        await self.redis_repository.add_to_dict(
            self.SEARCH_SCHEMAS_DICT_NAME, namespace, search_schema_json_str
        )
        mapping = DSLTranslator.schema_to_es_mapping(search_schema)
        await self.elastic_repository.create_or_update_index(
            namespace=namespace,
            mappings=mapping,
        )

    async def search_objects(
        self, namespace: str, filters: str
    ) -> list[dict[str, Any]]:
        schema = await self.redis_repository.get_from_dict(
            self.SEARCH_SCHEMAS_DICT_NAME, namespace
        )
        if not schema:
            raise HTTPException(400, 'Поисковая схема не установлена')
        query = DSLTranslator.build_query_from_expression(filters)

        ids = await self.elastic_repository.search_ids_in_index(
            namespace=namespace,
            body=query,
            size=50,
            from_=0,
        )
        metas = await asyncio.gather(
            *[
                self.postgres_repository.get_document_meta(namespace, doc_id)
                for doc_id in ids
            ]
        )

        return [m for m in metas if m is not None]

    async def read_namespace(self, namespace: str) -> DocumentListSchema:
        if not await self.redis_repository.check_in_set(
            self.ALL_NAMESPACES_SET_NAME, namespace
        ):
            return DocumentListSchema()
        return await self.postgres_repository.list_documents_meta(namespace)

    async def read_limit_namespace(
        self,
        namespace: str,
        limit: int,
        cursor: str,
    ) -> DocumentListSchema:
        if not await self.redis_repository.check_in_set(
            self.ALL_NAMESPACES_SET_NAME, namespace
        ):
            return DocumentListSchema()
        return await self.postgres_repository.list_documents_meta(
            namespace, limit=limit, cursor=cursor
        )

    async def get_namespace(self) -> list[str]:
        return sorted(
            await self.redis_repository.get_all_from_set(self.ALL_NAMESPACES_SET_NAME)
        )

    async def create_namespace(self, namespace: str) -> list[str]:
        await asyncio.gather(
            self.postgres_repository.create_meta_table_by_namespace(namespace),
            self.elastic_repository.create_or_update_index(namespace),
            self.redis_repository.add_to_set(self.ALL_NAMESPACES_SET_NAME, namespace),
        )
        if not MultiRepositoryService.is_init_chunks_table:
            await self.postgres_repository.create_chunks_table()
            MultiRepositoryService.is_init_chunks_table = True
        return sorted(
            await self.redis_repository.get_all_from_set(self.ALL_NAMESPACES_SET_NAME)
        )

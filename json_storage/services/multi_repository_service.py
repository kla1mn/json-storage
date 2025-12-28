from typing import Any, ClassVar, TypeVar
from uuid import UUID
from collections.abc import AsyncIterator
import uuid
from dataclasses import dataclass

from fastapi import HTTPException
from .dsl_translator import DSLTranslator
from .jsonpath_parser import JSONPathParser
from json_storage.repositories import PostgresDBRepository, ElasticSearchDBRepository
from json_storage.schemas import DocumentListSchema, DocumentSchema

JSONType = TypeVar('JSONType', bound=dict[str, Any])


@dataclass
class MultiRepositoryService:
    NAMESPACES: ClassVar[set[str]] = set()
    SEARCH_SCHEMAS: ClassVar[dict[str, dict[str, Any]]] = {}
    postgres_repository: PostgresDBRepository
    elastic_repository: ElasticSearchDBRepository

    async def get_object_meta(self, namespace: str, object_id: UUID) -> DocumentSchema:
        meta = await self.postgres_repository.get_document_meta(
            namespace, str(object_id)
        )
        if meta is None:
            raise HTTPException(status_code=404)
        return meta

    async def get_object_body(self, namespace: str, object_id: UUID) -> JSONType | None:
        return await self.elastic_repository.get_document(namespace, str(object_id))

    async def create_object_stream(
        self,
        namespace: str,
        body: AsyncIterator[bytes],
        *,
        document_name: str,
    ) -> UUID:
        if namespace not in self.NAMESPACES:
            self.NAMESPACES.add(namespace)
            await self.postgres_repository.create_chunks_table()
            await self.postgres_repository.create_meta_table_by_namespace(namespace)

        doc = await self.postgres_repository.create_document_stream(
            namespace=namespace,
            document_name=document_name,
            body=body,
        )
        return uuid.UUID(doc.id)

    async def delete_object_by_id(self, namespace: str, object_id: UUID) -> None:
        await self.postgres_repository.delete_object_by_id(namespace, str(object_id))

    async def set_search_schema(
        self,
        namespace: str,
        search_schema: dict[str, Any],
    ) -> None:
        self.SEARCH_SCHEMAS[namespace] = search_schema
        mapping = DSLTranslator.schema_to_es_mapping(search_schema)
        await self.elastic_repository.create_or_update_index(
            index=namespace,
            mappings=mapping,
        )

    async def search_objects(
        self,
        namespace: str,
        filters: dict[str, Any],
    ) -> list[dict[str, Any]]:
        schema = self.SEARCH_SCHEMAS.get(namespace)
        if not schema:
            raise HTTPException(400, 'Search schema not set')

        nested_groups: dict[str, list[dict]] = {}
        must: list[dict] = []

        for logical_name, value in filters.items():
            if logical_name not in schema:
                raise HTTPException(400, f'Unknown field: {logical_name}')

            jsonpath = schema[logical_name]
            segments = JSONPathParser.parse_json_path(jsonpath)
            es_path = DSLTranslator.to_es_path(segments)

            term = {'term': {es_path.field: value}}

            if es_path.is_nested:
                nested_groups.setdefault(es_path.nested_path, []).append(term)
            else:
                must.append(term)

        for path, terms in nested_groups.items():
            must.append({'nested': {'path': path, 'query': {'bool': {'must': terms}}}})

        query = (
            {'query': {'bool': {'must': must}}}
            if must
            else {'query': {'match_all': {}}}
        )

        resp = await self.elastic_repository.search_in_index(
            index=namespace,
            body=query,
        )

        return resp

    async def read_namespace(self, namespace: str) -> DocumentListSchema:
        if namespace not in self.NAMESPACES:
            return DocumentListSchema()
        return await self.postgres_repository.list_documents_meta(namespace)

    async def read_limit_namespace(
        self,
        namespace: str,
        limit: int,
        cursor: str,
    ) -> DocumentListSchema:
        if namespace not in self.NAMESPACES:
            return DocumentListSchema()
        return await self.postgres_repository.list_documents_meta(
            namespace, limit=limit, cursor=cursor
        )

    async def get_namespace(self) -> list[str]:
        return sorted(list(self.NAMESPACES))

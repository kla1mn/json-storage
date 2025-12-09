from typing import Any, ClassVar, TypeVar
from uuid import UUID
from dataclasses import dataclass

from json_storage.repositories import PostgresDBRepository, ElasticSearchDBRepository
from json_storage.schemas import DocumentListSchema

JSONType = TypeVar('JSONType', bound=dict[str, Any])


@dataclass
class MultiRepositoryService:
    NAMESPACES: ClassVar[set[str]] = set()
    SEARCH_SCHEMAS: ClassVar[dict[str, dict[str, Any]]] = {}
    postgres_repository: PostgresDBRepository
    elastic_repository: ElasticSearchDBRepository

    async def get_object_by_id(self, namespace: str, object_id: UUID) -> JSONType: ...

    async def create_object(
        self, namespace: str, data: JSONType
    ) -> UUID: ...  # TODO: taskiq

    async def delete_object_by_id(
        self, namespace: str, object_id: UUID
    ) -> None: ...  # TODO: taskiq

    async def set_search_schema(
        self,
        namespace: str,
        search_schema: JSONType,
    ) -> None: ...

    async def search_objects(self, namespace: str) -> list[JSONType]: ...

    async def read_namespace(self, namespace: str) -> DocumentListSchema:
        if namespace not in self.NAMESPACES:
            return DocumentListSchema()
        return await self.postgres_repository.list_documents_meta(namespace)

    async def read_limit_namespace(
        self,
        namespace: str,
        limit: int,
        cursor: str,
    ) -> DocumentListSchema: ...

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

import json
import uuid
import uuid_extensions
from psycopg import sql

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from json_storage.schemas import DocumentSchema, DocumentListSchema

# await cur.execute(
#     """
#     create table if not exists json_documents_meta
#     (
#         id
#         uuid
#         primary
#         key,
#         namespace
#         text
#         not
#         null,
#         status
#         text
#         not
#         null,
#         metadata
#         jsonb,
#         created_at
#         timestamptz
#         not
#         null
#         default
#         now
#     (
#     ),
#         updated_at timestamptz not null default now
#     (
#     )
#         );
#     """
# )

@dataclass
class PostgresDBRepository:
    dsn: str

    _pool: AsyncConnectionPool | None = field(init=False, default=None)

    async def _get_pool(self) -> AsyncConnectionPool:
        if self._pool is None:
            self._pool = AsyncConnectionPool(conninfo=self.dsn)

        return self._pool

    async def create_data_table(self) -> None:
        pool = await self._get_pool()
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    create table if not exists json_documents_data (
                        id uuid primary key references json_documents_meta(id) on delete cascade,
                        content bytea not null
                    );
                    """
                )

            await conn.commit()

    async def create_meta_table_by_namespace(self, namespace: str) -> None:
        # TODO: хочеца проверку на содержимое неймспейса по регулярочке, а пока "слушаю и верю каждому твоему слову"
        pool = await self._get_pool()
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    sql.SQL(
                    """
                    create table if not exists {} (
                        id uuid primary key,
                        namespace text not null,
                        status text not null,
                        metadata jsonb,
                        created_at timestamptz not null default now(),
                        updated_at timestamptz not null default now()
                    );
                    """
                ).format(sql.Identifier(namespace)))

            await conn.commit()

    async def drop_meta_table_by_namespace(self, namespace: str) -> None:
        # TODO: тоже бы проверочку названия, хотя if exists скипнет,
        #  но в целом чтоб не делать лишний запрос можно и проверить на этом этапе
        pool = await self._get_pool()

        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    sql.SQL("drop table if exists {}")
                    .format(sql.Identifier(namespace))
                )
            await conn.commit()


    async def create_document(
        self,
        namespace: str,
        payload: dict[str, Any],
        status: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> str:
        pool = await self._get_pool()
        doc_id = uuid_extensions.uuid7()

        raw_bytes = json.dumps(payload).encode("utf-8")

        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    insert into json_documents_meta
                        (id, namespace, status, metadata)
                    values
                        (%s, %s, %s, %s)
                    """,
                    (doc_id, namespace, status, json.dumps(metadata)),
                )
                await cur.execute(
                    """
                    insert into json_documents_data
                        (id, content)
                    values
                        (%s, %s)
                    """,
                    (doc_id, raw_bytes),
                )
            await conn.commit()

        return str(doc_id)

    async def get_document(
        self,
        namespace: str,
        doc_id: str,
    ) -> Optional[dict[str, Any]]:
        pool = await self._get_pool()
        uid = uuid.UUID(doc_id)

        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    select m.id,
                           m.namespace,
                           m.status,
                           m.metadata,
                           m.created_at,
                           m.updated_at,
                           d.content
                    from json_documents_meta m
                    join json_documents_data d on d.id = m.id
                    where m.id = %s and m.namespace = %s
                    """,
                    (uid, namespace),
                )
                row = await cur.fetchone()

        if row is None:
            return None

        payload = json.loads(bytes(row["content"]).decode("utf-8"))
        return payload

    async def delete_document(self, namespace: str, doc_id: str) -> bool:
        pool = await self._get_pool()
        uid = uuid.UUID(doc_id)

        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    delete from json_documents_meta
                    where id = %s and namespace = %s
                    """,
                    (uid, namespace),
                )
                deleted = cur.rowcount
            await conn.commit()

        return deleted > 0

    async def update_document(
        self,
        namespace: str,
        doc_id: str,
        payload: dict[str, Any],
        status: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> bool:
        pool = await self._get_pool()
        uid = uuid.UUID(doc_id)

        ...

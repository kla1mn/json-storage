from __future__ import annotations

import hashlib
from collections.abc import AsyncGenerator, AsyncIterator
from dataclasses import dataclass, field
from typing import Any, Optional

import json
import uuid
import uuid_extensions
from psycopg import sql

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from json_storage.schemas import DocumentSchema, DocumentListSchema


@dataclass
class PostgresDBRepository:
    # TODO: хочу кастомный контекстный менеджер вместо вложенных with connection, with pool и тд
    dsn: str

    _pool: AsyncConnectionPool | None = field(init=False, default=None)

    async def _get_pool(self) -> AsyncConnectionPool:
        if self._pool is None:
            self._pool = AsyncConnectionPool(conninfo=self.dsn)

        return self._pool

    # TODO: без этой штуки не умирает коннекшен в тестах, скорее всего и в реальной работе тоже умирать не будет
    async def aclose(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def create_buffer_table(self) -> None:
        pool = await self._get_pool()
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    create table if not exists json_buffer (
                        id uuid primary key,
                        content bytea not null
                    );
                    """
                )
            await conn.commit()

    async def create_chunks_table(self) -> None:
        pool = await self._get_pool()
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    create table if not exists json_chunks (
                        id uuid not null,
                        part integer not null,
                        data bytea not null,
                        primary key (id, part)
                    );
                    """
                )
            await conn.commit()

    async def iter_chunks_by_id(self, doc_id: str) -> AsyncGenerator[bytes, None]:
        pool = await self._get_pool()
        uid = uuid.UUID(doc_id)

        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    select data
                    from json_chunks
                    where id = %s
                    order by part
                    """,
                    (uid,),
                )
                async for row in cur:
                    (data,) = row
                    yield bytes(data)

    async def create_document_stream(
        self,
        namespace: str,
        document_name: str,
        body: AsyncIterator[bytes],
        *,
        max_batch_bytes: int = 1024 * 1024,
    ) -> DocumentSchema:
        pool = await self._get_pool()
        table = namespace + '_metadata'

        doc_id = uuid_extensions.uuid7()
        hasher = hashlib.sha256()
        total = 0
        part = 0
        batch: list[tuple[uuid.UUID, int, bytes]] = []
        batch_bytes = 0

        async with pool.connection() as conn:
            try:
                async with conn.cursor() as cur:
                    async for chunk in body:
                        if not chunk:
                            continue
                        b = bytes(chunk)
                        total += len(b)
                        hasher.update(b)
                        batch.append((doc_id, part, b))
                        batch_bytes += len(b)
                        part += 1

                        if batch_bytes >= max_batch_bytes:
                            await cur.executemany(
                                """
                                insert into json_chunks (id, part, data)
                                values (%s, %s, %s)
                                """,
                                batch,
                            )
                            batch.clear()
                            batch_bytes = 0

                    if batch:
                        await cur.executemany(
                            """
                            insert into json_chunks (id, part, data)
                            values (%s, %s, %s)
                            """,
                            batch,
                        )

                    content_hash = hasher.hexdigest()

                    await cur.execute(
                        sql.SQL(
                            """
                            insert into {} (id, document_name, content_length, content_hash)
                            values (%s, %s, %s, %s)
                            returning created_at, updated_at
                            """
                        ).format(sql.Identifier(table)),
                        (doc_id, document_name, total, content_hash),
                    )
                    created_at, updated_at = await cur.fetchone()

                await conn.commit()
            except Exception:
                await conn.rollback()
                raise

        return DocumentSchema(
            id=str(doc_id),
            document_name=document_name,
            created_at=created_at,
            updated_at=updated_at,
            content_length=total,
            content_hash=content_hash,
        )

    async def get_data_by_id(self, doc_id: str) -> Optional[bytes]:
        pool = await self._get_pool()
        uid = uuid.UUID(doc_id)

        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    select content
                    from json_buffer
                    where id = %s
                    """,
                    (uid,),
                )
                row = await cur.fetchone()

        if row is None:
            return None

        (content,) = row
        return content

    async def delete_data_by_id(self, doc_id: str) -> bool:
        pool = await self._get_pool()
        uid = uuid.UUID(doc_id)

        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    delete
                    from json_buffer
                    where id = %s
                    """,
                    (uid,),
                )
                deleted_rows = cur.rowcount
            await conn.commit()

        return deleted_rows > 0

    async def create_meta_table_by_namespace(self, namespace: str) -> None:
        # TODO: хочеца проверку на содержимое неймспейса по регулярочке, а пока "слушаю и верю каждому твоему слову"
        table = namespace + '_metadata'
        pool = await self._get_pool()
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    sql.SQL(
                        """
                    create table if not exists {} (
                        id uuid primary key,
                        document_name text not null,
                        content_length integer not null,
                        content_hash text not null,
                        created_at timestamptz not null default now(),
                        updated_at timestamptz not null default now()
                    );
                    """
                    ).format(sql.Identifier(table))
                )

            await conn.commit()

    async def drop_meta_table_by_namespace(self, namespace: str) -> None:
        # TODO: тоже бы проверочку названия, хотя if exists скипнет,
        #  но в целом чтоб не делать лишний запрос можно и проверить на этом этапе
        pool = await self._get_pool()
        table = namespace + '_metadata'
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    sql.SQL('drop table if exists {}').format(sql.Identifier(table))
                )
            await conn.commit()

    async def create_document(
        self,
        namespace: str,
        document_name: str,
        payload: dict[str, Any],
    ) -> DocumentSchema:
        pool = await self._get_pool()
        table = namespace + '_metadata'

        doc_id = uuid_extensions.uuid7()
        raw_bytes = json.dumps(payload, separators=(',', ':')).encode('utf-8')
        content_length = len(raw_bytes)
        content_hash = hashlib.sha256(raw_bytes).hexdigest()

        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    sql.SQL(
                        """
                        insert into {} (id, document_name, content_length, content_hash)
                        values (%s, %s, %s, %s)
                        returning created_at, updated_at
                        """
                    ).format(sql.Identifier(table)),
                    (doc_id, document_name, content_length, content_hash),
                )
                created_at, updated_at = await cur.fetchone()

                await cur.execute(
                    """
                    insert into json_buffer (id, content)
                    values (%s, %s)
                    """,
                    (doc_id, raw_bytes),
                )

            await conn.commit()

        return DocumentSchema(
            id=str(doc_id),
            document_name=document_name,
            created_at=created_at,
            updated_at=updated_at,
            content_length=content_length,
            content_hash=content_hash,
        )

    async def get_document_meta(
        self,
        namespace: str,
        doc_id: str,
    ) -> Optional[DocumentSchema]:
        pool = await self._get_pool()
        table = namespace + '_metadata'
        uid = uuid.UUID(doc_id)

        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    sql.SQL(
                        """
                        select id,
                               document_name,
                               content_length,
                               content_hash,
                               created_at,
                               updated_at
                        from {}
                        where id = %s
                        """
                    ).format(sql.Identifier(table)),
                    (uid,),
                )
                row = await cur.fetchone()

        if row is None:
            return None

        return DocumentSchema(
            id=str(row['id']),
            document_name=row['document_name'],
            created_at=row['created_at'],
            updated_at=row['updated_at'],
            content_length=row['content_length'],
            content_hash=row['content_hash'],
        )

    async def delete_document_meta(
        self,
        namespace: str,
        doc_id: str,
    ) -> bool:
        pool = await self._get_pool()
        table = namespace + '_metadata'
        uid = uuid.UUID(doc_id)

        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    sql.SQL(
                        """
                        delete from {}
                        where id = %s
                        """
                    ).format(sql.Identifier(table)),
                    (uid,),
                )
                deleted = cur.rowcount
            await conn.commit()

        return deleted > 0

    async def list_documents_meta(
        self,
        namespace: str,
        limit: int | None = None,
        cursor: str | None = None,
        offset: int | None = None,
    ) -> DocumentListSchema:
        pool = await self._get_pool()
        table = namespace + '_metadata'

        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                query = [
                    sql.SQL(
                        """
                        select id,
                            document_name,
                            content_length,
                            content_hash,
                            created_at,
                            updated_at
                        from {}
                        """
                    ).format(sql.Identifier(table)),
                ]
                params = []
                if cursor is not None:
                    query.append(sql.SQL('where id <= %s'))
                    params.append(cursor)
                query.append(sql.SQL('order by created_at desc'))
                if limit is not None:
                    query.append(sql.SQL('limit %s'))
                    params.append(limit)
                if offset is not None:
                    query.append(sql.SQL('offset %s'))
                    params.append(offset)
                await cur.execute(
                    sql.SQL(' ').join(query),
                    params,
                )
                rows = await cur.fetchall()

                await cur.execute(
                    sql.SQL('select count(*) as cnt from {}').format(
                        sql.Identifier(table)
                    )
                )
                total_row = await cur.fetchone()

        items = [
            DocumentSchema(
                id=str(r['id']),
                document_name=r['document_name'],
                created_at=r['created_at'],
                updated_at=r['updated_at'],
                content_length=r['content_length'],
                content_hash=r['content_hash'],
            )
            for r in rows
            if r is not None
        ]

        return DocumentListSchema(items=items, count=total_row['cnt'])

    async def delete_object_by_id(self, namespace: str, doc_id: str) -> bool:
        pool = await self._get_pool()
        table = namespace + '_metadata'
        uid = uuid.UUID(doc_id)

        async with pool.connection() as conn:
            try:
                async with conn.cursor() as cur:
                    await cur.execute(
                        sql.SQL(
                            """
                            delete
                            from {}
                            where id = %s
                            """
                        ).format(sql.Identifier(table)),
                        (uid,),
                    )
                    meta_deleted = cur.rowcount

                    await cur.execute(
                        """
                        delete
                        from json_chunks
                        where id = %s
                        """,
                        (uid,),
                    )
                    chunks_deleted = cur.rowcount

                await conn.commit()
            except Exception:
                await conn.rollback()
                raise

        return (meta_deleted > 0) and (chunks_deleted > 0)

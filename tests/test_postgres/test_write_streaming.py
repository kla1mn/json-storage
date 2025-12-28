import hashlib

import psycopg
import pytest
import uuid_extensions as uuid_ext

from json_storage.repositories.postgres import PostgresDBRepository
from json_storage.settings import settings

DSN = settings.postgres.dsn


async def chunker(data: bytes, chunk_size: int):
    for i in range(0, len(data), chunk_size):
        yield data[i : i + chunk_size]


@pytest.mark.asyncio
async def test_create_chunks_table():
    repo = PostgresDBRepository(dsn=DSN)
    await repo.create_chunks_table()

    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public';
            """
        )
        tables = {row[0] for row in cur.fetchall()}
        assert 'json_chunks' in tables

        cur.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'json_chunks'
            ORDER BY ordinal_position;
            """
        )
        columns = {
            row[0]: {
                'data_type': row[1],
                'is_nullable': row[2],
            }
            for row in cur.fetchall()
        }

    await repo.aclose()

    assert columns['id']['data_type'] == 'uuid'
    assert columns['part']['data_type'] in {'integer', 'int4'}
    assert columns['data']['data_type'] == 'bytea'


@pytest.mark.asyncio
async def test_create_document_stream_writes_chunks_and_meta_and_roundtrips():
    repo = PostgresDBRepository(dsn=DSN)

    await repo.create_chunks_table()

    namespace = f'ns_{uuid_ext.uuid7().hex[:8]}'
    table = f'{namespace}_metadata'
    await repo.create_meta_table_by_namespace(namespace)

    raw = b'{"k":"' + b'x' * (2 * 1024 * 1024) + b'"}'
    expected_hash = hashlib.sha256(raw).hexdigest()

    doc = await repo.create_document_stream(
        namespace=namespace,
        document_name='пупупипи',
        body=chunker(raw, chunk_size=64 * 1024),
        max_batch_bytes=256 * 1024,
    )

    assert doc.document_name == 'пупупипи'
    assert doc.content_length == len(raw)
    assert doc.content_hash == expected_hash

    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            select id, document_name, content_length, content_hash
            from {table}
            where id = %s
            """,
            (doc.id,),
        )
        row = cur.fetchone()
        assert row is not None
        assert str(row[0]) == doc.id
        assert row[1] == 'пупупипи'
        assert row[2] == len(raw)
        assert row[3] == expected_hash

        cur.execute(
            """
            select count(*)
            from json_chunks
            where id = %s
            """,
            (doc.id,),
        )
        (chunk_cnt,) = cur.fetchone()
        assert chunk_cnt > 0

        cur.execute(
            """
            select min(part), max(part)
            from json_chunks
            where id = %s
            """,
            (doc.id,),
        )
        mn, mx = cur.fetchone()
        assert mn == 0
        assert mx == chunk_cnt - 1

    reassembled = b''.join([chunk async for chunk in repo.iter_chunks_by_id(doc.id)])
    assert reassembled == raw

    await repo.aclose()

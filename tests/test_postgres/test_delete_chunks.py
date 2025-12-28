import pytest
import psycopg
import uuid_extensions as uuid_ext

from json_storage.repositories.postgres import PostgresDBRepository
from json_storage.settings import settings

DSN = settings.postgres.dsn


async def chunker(data: bytes, chunk_size: int):
    for i in range(0, len(data), chunk_size):
        yield data[i : i + chunk_size]


@pytest.mark.asyncio
async def test_delete_document_deletes_meta_and_chunks():
    repo = PostgresDBRepository(dsn=DSN)

    await repo.create_chunks_table()

    namespace = f'ns_{uuid_ext.uuid7().hex[:8]}'
    table = f'{namespace}_metadata'
    await repo.create_meta_table_by_namespace(namespace)

    raw = b'{"k":"' + b'x' * (512 * 1024) + b'"}'
    doc = await repo.create_document_stream(
        namespace=namespace,
        document_name='to-delete',
        body=chunker(raw, 64 * 1024),
        max_batch_bytes=128 * 1024,
    )

    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(f'select count(*) from {table} where id = %s', (doc.id,))
        (meta_cnt,) = cur.fetchone()
        assert meta_cnt == 1

        cur.execute('select count(*) from json_chunks where id = %s', (doc.id,))
        (chunks_cnt,) = cur.fetchone()
        assert chunks_cnt > 0

    deleted = await repo.delete_object_by_id(namespace, doc.id)
    assert deleted is True

    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(f'select count(*) from {table} where id = %s', (doc.id,))
        (meta_cnt2,) = cur.fetchone()
        assert meta_cnt2 == 0

        cur.execute('select count(*) from json_chunks where id = %s', (doc.id,))
        (chunks_cnt2,) = cur.fetchone()
        assert chunks_cnt2 == 0

    await repo.aclose()

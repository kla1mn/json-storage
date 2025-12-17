import hashlib
import pytest
import uuid_extensions

from json_storage.repositories.postgres import PostgresDBRepository
from json_storage.settings import settings

DSN = settings.postgres.dsn


async def chunker(data: bytes, chunk_size: int):
    for i in range(0, len(data), chunk_size):
        yield data[i : i + chunk_size]


@pytest.mark.asyncio
async def test_iter_chunks_by_id_stream_hash_and_len():
    repo = PostgresDBRepository(dsn=DSN)
    await repo.create_chunks_table()

    namespace = f'ns_{uuid_extensions.uuid7().hex[:8]}'
    await repo.create_meta_table_by_namespace(namespace)

    raw = (b'{"k":"' + b"x" * (2 * 1024 * 1024) + b'"}')
    expected_hash = hashlib.sha256(raw).hexdigest()
    expected_len = len(raw)

    doc = await repo.create_document_stream(
        namespace=namespace,
        document_name='пипипупу',
        body=chunker(raw, chunk_size=64 * 1024),
        max_batch_bytes=256 * 1024,
    )

    hasher = hashlib.sha256()
    total = 0

    async for chunk in repo.iter_chunks_by_id(doc.id):
        total += len(chunk)
        hasher.update(chunk)

    assert total == expected_len
    assert hasher.hexdigest() == expected_hash

    await repo.aclose()

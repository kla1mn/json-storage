import json
import uuid

import psycopg
import pytest
import uuid_extensions as uuid_ext

from json_storage.settings import settings


async def _body_bytes(raw: bytes):
    yield raw


@pytest.mark.asyncio
async def test_taskiq_indexes_document_to_es(
    multi_repository_service,
    elasticsearch_repo,
    taskiq_inmemory_broker,
):
    namespace = f'ns_{uuid_ext.uuid7().hex[:8]}'
    raw = b'{"k":"v"}'

    obj_id = await multi_repository_service.create_object_stream(
        namespace=namespace,
        body=_body_bytes(raw),
        document_name='doc',
    )

    before = await elasticsearch_repo.get_document(index=namespace, doc_id=str(obj_id))
    assert before is None

    await taskiq_inmemory_broker.wait_all()

    got = await elasticsearch_repo.get_document(index=namespace, doc_id=str(obj_id))
    assert got == json.loads(raw)


@pytest.mark.asyncio
async def test_taskiq_deletes_chunks_after_success(
    multi_repository_service,
    taskiq_inmemory_broker,
):
    namespace = f'ns_{uuid_ext.uuid7().hex[:8]}'
    raw = b'{"k":"v"}'

    obj_id = await multi_repository_service.create_object_stream(
        namespace=namespace,
        body=_body_bytes(raw),
        document_name='doc',
    )

    with psycopg.connect(settings.postgres.dsn) as conn, conn.cursor() as cur:
        cur.execute('select count(*) from json_chunks where id = %s', (str(obj_id),))
        (cnt_before,) = cur.fetchone()
        assert cnt_before > 0

    await taskiq_inmemory_broker.wait_all()

    with psycopg.connect(settings.postgres.dsn) as conn, conn.cursor() as cur:
        cur.execute('select count(*) from json_chunks where id = %s', (str(obj_id),))
        (cnt_after,) = cur.fetchone()
        assert cnt_after == 0


@pytest.mark.asyncio
async def test_get_object_body_reads_from_elastic(
    multi_repository_service,
    taskiq_inmemory_broker,
):
    namespace = f'ns_{uuid_ext.uuid7().hex[:8]}'
    raw = b'{"k":"v"}'

    obj_id = await multi_repository_service.create_object_stream(
        namespace=namespace,
        body=_body_bytes(raw),
        document_name='doc',
    )

    await taskiq_inmemory_broker.wait_all()

    body = await multi_repository_service.get_object_body(
        namespace, uuid.UUID(str(obj_id))
    )
    assert body == json.loads(raw)

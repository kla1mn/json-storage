import os
import uuid_extensions as uuid_ext
import json

import psycopg
import pytest

from json_storage.repositories.postgres import PostgresDBRepository
from json_storage.schemas import DocumentSchema, DocumentListSchema


DSN = os.getenv("PG_DSN", "postgresql://json:json@localhost:5432/jsonstorage")


@pytest.mark.asyncio
async def test_create_buffer_table():
    repo = PostgresDBRepository(dsn=DSN)
    await repo.create_buffer_table()

    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public';
            """
        )
        tables = {row[0] for row in cur.fetchall()}
        assert "json_buffer" in tables

        cur.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'json_buffer'
            ORDER BY ordinal_position;
            """
        )
        columns = {
            row[0]: {
                "data_type": row[1],
                "is_nullable": row[2],
            }
            for row in cur.fetchall()
        }

    await repo.aclose()

    assert "id" in columns
    assert columns["id"]["data_type"] == "uuid"
    assert columns["id"]["is_nullable"] == "NO"

    assert "content" in columns
    assert columns["content"]["data_type"] == "bytea"
    assert columns["content"]["is_nullable"] == "NO"


@pytest.mark.asyncio
async def test_buffer_insert_and_get_and_delete_roundtrip():
    repo = PostgresDBRepository(dsn=DSN)
    await repo.create_buffer_table()

    doc_id = str(uuid_ext.uuid7())
    payload = {"a": 1, "b": "test"}
    raw = json.dumps(payload).encode("utf-8")

    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into json_buffer (id, content)
            values (%s, %s)
            """,
            (doc_id, raw),
        )
        conn.commit()

    # get_data_by_id
    got = await repo.get_data_by_id(doc_id)
    assert got is not None
    assert got == raw
    assert json.loads(got.decode("utf-8")) == payload

    # delete_data_by_id
    deleted = await repo.delete_data_by_id(doc_id)
    assert deleted is True

    # после удаления – ничего нет
    got_after = await repo.get_data_by_id(doc_id)
    assert got_after is None

    await repo.aclose()


@pytest.mark.asyncio
async def test_create_and_drop_meta_table_by_namespace():
    repo = PostgresDBRepository(dsn=DSN)
    namespace = f"ns_{uuid_ext.uuid7().hex[:8]}"
    table = f"{namespace}_metadata"

    # create
    await repo.create_meta_table_by_namespace(namespace)

    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public';
            """
        )
        tables = {row[0] for row in cur.fetchall()}
        assert table in tables

    # drop
    await repo.drop_meta_table_by_namespace(namespace)

    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public';
            """
        )
        tables = {row[0] for row in cur.fetchall()}
        assert table not in tables

    await repo.aclose()


@pytest.mark.asyncio
async def test_create_document_writes_meta_and_buffer():
    repo = PostgresDBRepository(dsn=DSN)
    await repo.create_buffer_table()

    namespace = f"ns_{uuid_ext.uuid7().hex[:8]}"
    table = f"{namespace}_metadata"
    await repo.create_meta_table_by_namespace(namespace)

    payload = {"foo": "bar", "n": 42}
    document_name = "doc-1"

    doc = await repo.create_document(
        namespace=namespace,
        document_name=document_name,
        payload=payload,
    )

    assert isinstance(doc, DocumentSchema)
    assert doc.document_name == document_name
    assert doc.content_length == len(
        json.dumps(payload, separators=(",", ":")).encode("utf-8")
    )
    assert isinstance(doc.content_hash, str)
    assert doc.id

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
        assert row[1] == document_name

        cur.execute(
            """
            select content
            from json_buffer
            where id = %s
            """,
            (doc.id,),
        )
        buf_row = cur.fetchone()
        assert buf_row is not None
        raw = buf_row[0]
        assert json.loads(bytes(raw).decode("utf-8")) == payload

    await repo.aclose()


@pytest.mark.asyncio
async def test_get_document_meta_returns_correct_schema():
    repo = PostgresDBRepository(dsn=DSN)
    await repo.create_buffer_table()

    namespace = f"ns_{uuid_ext.uuid7().hex[:8]}"
    await repo.create_meta_table_by_namespace(namespace)

    payload = {"x": 123}
    doc = await repo.create_document(namespace, "name-1", payload)

    fetched = await repo.get_document_meta(namespace, doc.id)
    assert isinstance(fetched, DocumentSchema)
    assert fetched.id == doc.id
    assert fetched.document_name == "name-1"
    assert fetched.content_length == doc.content_length
    assert fetched.content_hash == doc.content_hash
    assert fetched.created_at is not None
    assert fetched.updated_at is not None

    random_id = str(uuid_ext.uuid7())
    nothing = await repo.get_document_meta(namespace, random_id)
    assert nothing is None

    await repo.aclose()


@pytest.mark.asyncio
async def test_delete_document_meta():
    repo = PostgresDBRepository(dsn=DSN)
    await repo.create_buffer_table()

    namespace = f"ns_{uuid_ext.uuid7().hex[:8]}"
    table = f"{namespace}_metadata"
    await repo.create_meta_table_by_namespace(namespace)

    payload = {"x": "y"}
    doc = await repo.create_document(namespace, "to-delete", payload)

    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(
            f"select count(*) from {table} where id = %s",
            (doc.id,),
        )
        (cnt_before,) = cur.fetchone()
        assert cnt_before == 1

    deleted = await repo.delete_document_meta(namespace, doc.id)
    assert deleted is True

    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(
            f"select count(*) from {table} where id = %s",
            (doc.id,),
        )
        (cnt_after,) = cur.fetchone()
        assert cnt_after == 0

    deleted_again = await repo.delete_document_meta(namespace, doc.id)
    assert deleted_again is False

    await repo.aclose()


@pytest.mark.asyncio
async def test_list_documents_meta_pagination_and_count():
    repo = PostgresDBRepository(dsn=DSN)
    await repo.create_buffer_table()

    namespace = f"ns_{uuid_ext.uuid7().hex[:8]}"
    await repo.create_meta_table_by_namespace(namespace)

    payload = {"k": "v"}
    docs = []
    for i in range(5):
        d = await repo.create_document(
            namespace,
            f"name-{i}",
            {**payload, "idx": i},
        )
        docs.append(d)

    lst = await repo.list_documents_meta(namespace, limit=3, offset=0)
    assert isinstance(lst, DocumentListSchema)
    assert lst.count == 5
    assert len(lst.items) == 3

    names = [it.document_name for it in lst.items]
    assert names == ["name-4", "name-3", "name-2"]

    lst2 = await repo.list_documents_meta(namespace, limit=3, offset=2)
    assert lst2.count == 5
    assert len(lst2.items) == 3

    await repo.aclose()

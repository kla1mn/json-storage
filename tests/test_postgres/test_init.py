import os
import pytest
import psycopg

from json_storage.repositories.postgres import PostgresDBRepository


@pytest.mark.asyncio
async def test_tables_created_with_compose():
    dsn = "postgresql://json:json@localhost:5432/jsonstorage"

    repo = PostgresDBRepository(dsn=dsn)
    await repo.init_tables()

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("""SELECT table_name FROM information_schema.tables;""")
        tables = {row[0] for row in cur.fetchall()}

    assert "json_documents_meta" in tables
    assert "json_documents_data" in tables

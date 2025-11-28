import os
import pytest
import psycopg

from json_storage.repositories.postgres import PostgresDBRepository


@pytest.mark.asyncio
async def test_data_table_created_with_compose():
    dsn = "postgresql://json:json@localhost:5432/jsonstorage"

    repo = PostgresDBRepository(dsn=dsn)
    await repo.create_data_table()

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public';
            """
        )
        tables = {row[0] for row in cur.fetchall()}
        assert "json_documents_data" in tables

        cur.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'json_documents_data'
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

from elasticsearch import AsyncElasticsearch
from prometheus_client import Gauge
from time import time
from json_storage.settings import settings
from psycopg_pool import AsyncConnectionPool

# тута не все метрики заведены в графики, надо прикинуть, что мы точно хотим мониторить
postgres_up = Gauge('json_storage_postgres_metrics_up', 'Postgres metrics availability')
postgres_db_size = Gauge('json_storage_postgres_database_size_bytes', 'Postgres database size in bytes')
postgres_connections = Gauge('json_storage_postgres_connections', 'Postgres active connections')
postgres_table_size = Gauge('json_storage_postgres_table_size_bytes', 'Size of the tables and indexes in bytes',
                            ['table', 'kind'])

elastic_up = Gauge('json_storage_elastic_metrics_up', 'Elastic metrics availability')
elastic_cluster_health = Gauge('json_storage_elastic_cluster_health',
                               'Elastic cluster health status (0=red, 1=yellow, 2=green)')
elastic_index_store_size = Gauge('json_storage_elastic_index_store_size_bytes', 'Elastic index store size in bytes',
                                 ['index'])
elastic_index_docs = Gauge('json_storage_elastic_index_docs', 'Elastic index document count', ['index'])

indexing_pending = Gauge('json_storage_indexing_pending_total', 'Total number of documents pending for indexing')
indexing_oldest_age = Gauge('json_storage_indexing_oldest_age_seconds',
                            'Age in seconds of the oldest document in the indexing queue')
indexing_avg_age = Gauge('json_storage_indexing_avg_age_seconds', 'Average age of documents in the indexing queue')




async def collect_postgres_metrics():
    pool = AsyncConnectionPool(conninfo=settings.postgres.dsn)

    async with pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT pg_database_size(current_database())")
            db_size = await cursor.fetchone()
            postgres_db_size.set(db_size[0])

            await cursor.execute("SELECT count(*) FROM pg_stat_activity WHERE state = 'active'")
            active_connections = await cursor.fetchone()
            postgres_connections.set(active_connections[0])

            await cursor.execute("""
                                 SELECT relname, pg_total_relation_size(relid)
                                 FROM pg_catalog.pg_statio_user_tables
                                 """)
            async for table, size in cursor:
                postgres_table_size.labels(table=table, kind='total').set(size)

    postgres_up.set(1)


async def collect_elastic_metrics():
    es = AsyncElasticsearch([settings.elastic_search.dsn])

    health = await es.cluster.health()
    cluster_health = health['status']
    elastic_cluster_health.set({'green': 2, 'yellow': 1, 'red': 0}.get(cluster_health, 0))

    indices = await es.indices.stats()
    for index, stats in indices['indices'].items():
        elastic_index_store_size.labels(index=index).set(stats['total']['store']['size_in_bytes'])
        elastic_index_docs.labels(index=index).set(stats['total']['docs']['count'])

    await es.close()
    elastic_up.set(1)


async def collect_indexing_lag():
    pool = AsyncConnectionPool(conninfo=settings.postgres.dsn)

    async with pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT created_at FROM json_buffer ORDER BY created_at LIMIT 1")
            oldest = await cursor.fetchone()
            if oldest:
                oldest_age = (time() - oldest[0].timestamp())
                indexing_oldest_age.set(oldest_age)

            await cursor.execute("SELECT count(*) FROM json_buffer WHERE indexed = FALSE")
            pending = await cursor.fetchone()[0]
            indexing_pending.set(pending)

            await cursor.execute("""
                SELECT AVG(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - created_at)))
                FROM json_buffer
                WHERE indexed = FALSE
            """)
            avg_age = await cursor.fetchone()[0]
            indexing_avg_age.set(avg_age)

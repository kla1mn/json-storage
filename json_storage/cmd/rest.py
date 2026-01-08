import asyncio
import os

from json_storage.bootstrap import create_fastapi_app
from json_storage.cmd.taskiq_broker import taskiq_broker
from json_storage.storage_metrics import (
    collect_postgres_metrics,
    collect_elastic_metrics,
)

SLEEP_INTERVAL = int(os.getenv('METRICS__COLLECT_INTERVAL_SECONDS', 30))

app = create_fastapi_app()


async def start_metric_collection():
    while True:
        await collect_postgres_metrics()
        await collect_elastic_metrics()
        # await collect_indexing_lag()
        await asyncio.sleep(SLEEP_INTERVAL)


@app.on_event('startup')
async def _startup() -> None:
    await taskiq_broker.startup()
    asyncio.create_task(start_metric_collection())


@app.on_event('shutdown')
async def _shutdown() -> None:
    await taskiq_broker.shutdown()

from aio_pika import ExchangeType

from .depends import provider
from taskiq_aio_pika import AioPikaBroker
from .router import router
from fastapi import FastAPI
from dishka.integrations.fastapi import setup_dishka as fastapi_setup_dishka
from dishka.integrations.fastapi import FastapiProvider
from dishka.integrations.taskiq import setup_dishka as taskiq_setup_dishka
from dishka.integrations.taskiq import TaskiqProvider
from .container import ContainerManager


def create_fastapi_app() -> FastAPI:
    app = FastAPI(title='json-storage', docs_url='/docs', openapi_url='/docs.json')
    app.include_router(router)
    application_providers = [FastapiProvider(), provider]
    container = ContainerManager.create(application_providers)
    fastapi_setup_dishka(container, app)
    return app


def create_taskiq_broker() -> AioPikaBroker:
    broker = AioPikaBroker(
        queue_name='taskiq',
        exchange='taskiq',
        exchange_type=ExchangeType.DIRECT,
        dead_letter_queue_name='taskiq_dlx',
        declare_exchange=True,
        declare_queues=True,
        routing_key='taskiq',
    )
    application_providers = [TaskiqProvider(), provider]
    container = ContainerManager.create(application_providers)
    taskiq_setup_dishka(container, broker)
    return broker

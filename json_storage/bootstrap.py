from aio_pika import ExchangeType
from json_storage.settings import settings, EnvironmentEnum
from .const import NS_LATENCY_BUCKETS
from .depends import provider
from taskiq_aio_pika import AioPikaBroker
from taskiq import InMemoryBroker
from fastapi.middleware.cors import CORSMiddleware
from .router import router
from fastapi import FastAPI
from dishka.integrations.fastapi import setup_dishka as fastapi_setup_dishka
from dishka.integrations.fastapi import FastapiProvider
from dishka.integrations.taskiq import setup_dishka as taskiq_setup_dishka
from dishka.integrations.taskiq import TaskiqProvider
from .container import ContainerManager
from prometheus_fastapi_instrumentator import Instrumentator, metrics


def configure_metrics(app: FastAPI) -> None:
    instrumentator = Instrumentator(
        excluded_handlers=['/metrics'],
    )

    instrumentator.add(metrics.default())
    instrumentator.add(
        metrics.latency(
            buckets=NS_LATENCY_BUCKETS,
            should_include_handler=True,
            should_include_method=True,
            metric_namespace='ns',
            metric_subsystem='json_storage',
        )
    )

    instrumentator.instrument(app).expose(
        app,
        endpoint='/metrics',
        include_in_schema=False,
    )


def create_fastapi_app() -> FastAPI:
    app = FastAPI(title='json-storage', docs_url='/docs', openapi_url='/docs.json')

    app.include_router(router)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=['*'],
        allow_credentials=True,
        allow_methods=['*'],
        allow_headers=['*'],
    )

    configure_metrics(app)

    application_providers = [FastapiProvider(), provider]
    container = ContainerManager.create(application_providers)
    fastapi_setup_dishka(container, app)
    return app


def create_taskiq_broker() -> AioPikaBroker:
    if settings.environment == EnvironmentEnum.TEST:
        broker = InMemoryBroker(await_inplace=True)
    else:
        broker = AioPikaBroker(
            url=settings.rabbit_mq.dsn,
            queue_name='taskiq',
            exchange='taskiq',
            exchange_type=ExchangeType.DIRECT,
            dead_letter_queue_name='taskiq_dlx',
            declare_exchange=True,
            declare_exchange_kwargs={"durable": True},
            declare_queues=True,
            declare_queues_kwargs={
                "durable": True,
                "arguments": {
                    "x-queue-type": "quorum",
                },
            },
            routing_key='taskiq',
        )
    application_providers = [TaskiqProvider(), provider]
    container = ContainerManager.create(application_providers)
    taskiq_setup_dishka(container, broker)
    return broker

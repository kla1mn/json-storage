from dishka import make_async_container
from .depends import provider

from .router import router
from fastapi import FastAPI
from dishka.integrations.fastapi import setup_dishka as fastapi_setup_dishka
from dishka.integrations.fastapi import FastapiProvider


def create_app() -> FastAPI:
    app = FastAPI(title="json-storage", docs_url="/docs", openapi_url="/docs.json")
    app.include_router(router)
    application_providers = [FastapiProvider(), provider]
    container = make_async_container(*application_providers)
    fastapi_setup_dishka(container, app)
    return app

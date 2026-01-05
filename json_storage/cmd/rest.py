from json_storage.bootstrap import create_fastapi_app
from json_storage.cmd.taskiq_broker import taskiq_broker

app = create_fastapi_app()


@app.on_event('startup')
async def _startup() -> None:
    await taskiq_broker.startup()


@app.on_event('shutdown')
async def _shutdown() -> None:
    await taskiq_broker.shutdown()

import importlib

import pytest_asyncio
from taskiq import InMemoryBroker


@pytest_asyncio.fixture
async def taskiq_inmemory_broker(monkeypatch):

    broker = InMemoryBroker(await_inplace=False)

    import json_storage.cmd.taskiq_broker as broker_mod
    monkeypatch.setattr(broker_mod, "taskiq_broker", broker, raising=True)

    import json_storage.tasks as tasks_mod
    importlib.reload(tasks_mod)

    await broker.startup()
    try:
        yield broker
    finally:
        try:
            await broker.wait_all()
        except Exception:
            pass
        await broker.shutdown()

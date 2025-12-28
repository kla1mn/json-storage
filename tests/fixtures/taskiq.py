import importlib

import pytest_asyncio
from taskiq import InMemoryBroker


@pytest_asyncio.fixture
async def taskiq_inmemory_broker(monkeypatch):
    broker = InMemoryBroker(await_inplace=False)

    import json_storage.cmd.taskiq_broker as broker_mod

    monkeypatch.setattr(broker_mod, 'taskiq_broker', broker, raising=True)

    import json_storage.tasks as tasks_mod

    importlib.reload(tasks_mod)

    await broker.startup()
    try:
        yield broker
    finally:
        await broker.shutdown()


@pytest_asyncio.fixture
async def captured_taskiq_tasks(taskiq_inmemory_broker, monkeypatch):
    import json_storage.tasks as tasks_mod

    sent = []
    orig_kiq = tasks_mod.index_document_to_elastic.kiq

    async def kiq_wrapper(*args, **kwargs):
        task = await orig_kiq(*args, **kwargs)
        sent.append(task)
        return task

    monkeypatch.setattr(
        tasks_mod.index_document_to_elastic,
        'kiq',
        kiq_wrapper,
        raising=True,
    )
    return sent

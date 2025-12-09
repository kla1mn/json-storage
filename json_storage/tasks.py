from json_storage.cmd.taskiq_broker import taskiq_broker


@taskiq_broker.task()
async def delete_record(): ...

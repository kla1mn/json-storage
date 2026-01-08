import pytest


@pytest.mark.asyncio
async def test_add_to_dict(redis_repo):
    dict_name = 'dict_name'
    key = 'key'
    value = 'value'
    await redis_repo.add_to_dict(dict_name, key, value)

    assert value == await redis_repo.get_from_dict(dict_name, key)


@pytest.mark.asyncio
async def test_add_to_set(redis_repo):
    set_name = 'set_name'
    value = 'value'
    await redis_repo.add_to_set(set_name, value)

    assert await redis_repo.check_in_set(set_name, value)
    assert await redis_repo.get_all_from_set(set_name) == [value]


@pytest.mark.asyncio
async def test_remove_from_dict(redis_repo):
    dict_name = 'dict_name'
    key = 'key'
    value = 'value'
    await redis_repo.add_to_dict(dict_name, key, value)
    assert value == await redis_repo.get_from_dict(dict_name, key)

    await redis_repo.remove_from_dict(dict_name, key)
    assert not await redis_repo.get_from_dict(dict_name, key)


@pytest.mark.asyncio
async def test_remove_from_set(redis_repo):
    set_name = 'set_name'
    value = 'value'
    await redis_repo.add_to_set(set_name, value)
    assert await redis_repo.check_in_set(set_name, value)
    assert await redis_repo.get_all_from_set(set_name) == [value]

    await redis_repo.remove_from_set(set_name, value)
    assert not await redis_repo.check_in_set(set_name, value)
    assert not await redis_repo.get_all_from_set(set_name)
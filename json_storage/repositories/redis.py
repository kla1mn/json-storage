from dataclasses import dataclass
from redis.asyncio import Redis, from_url


@dataclass
class RedisDBRepository:
    dsn: str
    _client: Redis | None = None

    async def get_client(self) -> Redis:
        if self._client is None:
            self._client = from_url(
                self.dsn,
                decode_responses=True,
                socket_keepalive=True,
                socket_timeout=5.0,
                socket_connect_timeout=3.0,
                retry_on_timeout=True,
                health_check_interval=30,
                max_connections=100,
            )
        return self._client

    async def add_to_dict(self, dict_name: str, key: str, value: str) -> bool:
        client = await self.get_client()
        return bool(await client.hset(dict_name, key, value))

    async def get_from_dict(self, dict_name: str, key: str) -> str:
        client = await self.get_client()
        return await client.hget(dict_name, key)

    async def remove_from_dict(self, dict_name: str, key: str) -> bool:
        client = await self.get_client()
        result = await client.hdel(dict_name, key)
        return bool(result)

    async def add_to_set(self, set_name: str, value: str) -> bool:
        client = await self.get_client()
        return bool(await client.sadd(set_name, value))

    async def get_all_from_set(self, set_name: str) -> list[str]:
        client = await self.get_client()
        members = await client.smembers(set_name)
        return list(members)

    async def check_in_set(self, set_name: str, value: str) -> bool:
        client = await self.get_client()
        return bool(await client.sismember(set_name, value))

    async def remove_from_set(self, set_name: str, value: str) -> int:
        client = await self.get_client()
        return await client.srem(set_name, value)

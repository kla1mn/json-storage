from dishka import Provider, Scope, provide
from .repositories import (
    PostgresDBRepository,
    ElasticSearchDBRepository,
    RedisDBRepository,
)
from .services import MultiRepositoryService
from .settings import settings


class DataBaseProvider(Provider):
    scope = Scope.REQUEST

    @provide(scope=Scope.REQUEST)
    @staticmethod
    def get_postgres_db() -> PostgresDBRepository:
        return PostgresDBRepository(dsn=settings.postgres.dsn)

    @provide(scope=Scope.REQUEST)
    @staticmethod
    def get_elasticsearch_db() -> ElasticSearchDBRepository:
        return ElasticSearchDBRepository(
            dsn=settings.elastic_search.dsn,
            redis_repository=RedisDBRepository(dsn=settings.redis.dsn),
        )

    @provide(scope=Scope.REQUEST)
    @staticmethod
    def get_redis_db() -> RedisDBRepository:
        return RedisDBRepository(dsn=settings.redis.dsn)


provider = DataBaseProvider()

# Services
provider.provide(MultiRepositoryService)

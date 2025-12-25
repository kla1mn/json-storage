from dishka import Provider, Scope, provide
from .repositories import PostgresDBRepository, ElasticSearchDBRepository
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
        return ElasticSearchDBRepository(url=settings.elastic_search.dsn)


provider = DataBaseProvider()

# Services
provider.provide(MultiRepositoryService)

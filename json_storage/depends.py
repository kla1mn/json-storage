from dishka import Provider, Scope
from .repositories import PostgresDBRepository, ElasticSearchDBRepository
from .services import MultiRepositoryService

provider = Provider(scope=Scope.REQUEST)


# Repositories

provider.provide(PostgresDBRepository)
provider.provide(ElasticSearchDBRepository)
# Services

provider.provide(MultiRepositoryService)
# UseCases

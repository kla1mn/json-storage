from typing import Any
from dishka import FromDishka
from dishka.integrations.fastapi import DishkaRoute
from starlette.responses import JSONResponse, Response
from fastapi import APIRouter, Body, Query
from uuid import UUID

from .schemas import DocumentListSchema
from .services import MultiRepositoryService

router = APIRouter(prefix='/ns', route_class=DishkaRoute)


@router.get('/{namespace}/objects/{object_id}', response_model=dict[str, Any])
async def get_object_by_id(
    namespace: str,
    object_id: UUID,
    multi_repo: FromDishka[MultiRepositoryService],
) -> JSONResponse:
    return JSONResponse(content=await multi_repo.get_object_by_id(namespace, object_id))


@router.post('/{namespace}/objects', response_model=UUID)
async def create_object(
    namespace: str,
    multi_repo: FromDishka[MultiRepositoryService],
    data: dict[str, Any] = Body(..., description='JSON объект для создания'),
) -> JSONResponse:
    return JSONResponse(content=await multi_repo.create_object(namespace, data))


@router.delete('/{namespace}/objects/{object_id}', response_model=None)
async def delete_object_by_id(
    namespace: str,
    object_id: UUID,
    multi_repo: FromDishka[MultiRepositoryService],
) -> Response:
    await multi_repo.delete_object_by_id(namespace, object_id)
    return Response(status_code=204)


@router.put('/{namespace}/search-schema', response_model=None)
async def set_search_schema(
    namespace: str,
    multi_repo: FromDishka[MultiRepositoryService],
    search_schema: dict[str, Any] = Body(..., description='Схема поиска'),
) -> Response:
    await multi_repo.set_search_schema(namespace, search_schema)
    return Response(status_code=204)


@router.post('/{namespace}/search', response_model=list[dict[str, Any]])
async def search_objects(
    namespace: str,
    multi_repo: FromDishka[MultiRepositoryService],
) -> JSONResponse:
    return JSONResponse(content=await multi_repo.search_objects(namespace))


@router.get('/{namespace}', response_model=DocumentListSchema)
async def read_namespace(
    namespace: str,
    multi_repo: FromDishka[MultiRepositoryService],
) -> JSONResponse:
    return JSONResponse(content=await multi_repo.read_namespace(namespace))


@router.get('/{namespace}/objects', response_model=DocumentListSchema)
async def list_objects(
    namespace: str,
    multi_repo: FromDishka[MultiRepositoryService],
    limit: int = Query(
        50,
        ge=1,
        le=100,
        description='Максимальное число объектов в ответе (по умолчанию 50, макс 100)',
    ),
    cursor: str | None = Query(
        None,
        description='Открывок/токен курсора для получения следующей страницы (opaque)',
    ),
) -> JSONResponse:
    content = await multi_repo.read_limit_namespace(namespace, limit, cursor)
    return JSONResponse(content=content)

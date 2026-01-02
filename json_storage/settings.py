from pydantic_settings import BaseSettings, SettingsConfigDict
from enum import StrEnum, auto


class DsnSettingsSchema(BaseSettings):
    dsn: str


class EnvironmentEnum(StrEnum):
    TEST = auto()
    LOCAL = auto()


class SettingsSchema(BaseSettings):
    elastic_search: DsnSettingsSchema
    postgres: DsnSettingsSchema
    rabbit_mq: DsnSettingsSchema
    environment: str = EnvironmentEnum.LOCAL
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        env_nested_delimiter='__',
        case_sensitive=False,
        extra='ignore',
    )


settings = SettingsSchema()  # type: ignore

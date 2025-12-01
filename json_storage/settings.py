from pydantic_settings import BaseSettings, SettingsConfigDict


class DsnSettingsSchema(BaseSettings):
    dsn: str


class SettingsSchema(BaseSettings):
    elastic_search: DsnSettingsSchema
    postgres: DsnSettingsSchema
    rabbit_mq: DsnSettingsSchema
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        env_nested_delimiter='__',
        case_sensitive=False,
        extra='ignore',
    )


settings = SettingsSchema()  # type: ignore

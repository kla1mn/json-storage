from datetime import datetime

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


class DocumentSchema(BaseModel):
    model_config = ConfigDict(
        serialize_by_alias=True, populate_by_name=True, alias_generator=to_camel
    )

    id: str
    document_name: str
    created_at: datetime
    updated_at: datetime
    content_length: int
    content_hash: str

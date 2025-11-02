from .document import DocumentSchema
from pydantic import BaseModel, Field


class DocumentListSchema(BaseModel):
    items: list[DocumentSchema] = Field(default_factory=list)
    count: int = 0

from pydantic import BaseModel
from enum import StrEnum


class ProgressStatusEnum(StrEnum):
    INIT = 'init'
    PROGRESS = 'progress'
    SUCCESS = 'success'
    FAILED = 'failed'


class ProgressBarSchema(BaseModel):
    status: ProgressStatusEnum = ProgressStatusEnum.INIT
    percent: int = 0

from enum import Enum
from pydantic import BaseModel


class LogLevelController(Enum):
    info = "info"
    debug = "debug"


class LogConfig(BaseModel):
    log_name: str
    file_name: str
    log_level: LogLevelController

from enum import Enum, auto
from pathlib import Path
from pydantic import BaseModel

class WatcherChangeType(Enum):
    CREATE = auto()  # File created or first seen
    WRITE = auto()   # File modified
    DELETE = auto()  # File deleted (optional, for future robustness)
    IDLE = auto()    # File has not been modified for stream_timeout_seconds

class StreamEvent(BaseModel):
    change_type: WatcherChangeType
    file_path: Path
    # stream_id: str | None = None # stream_id will be generated by the processor 
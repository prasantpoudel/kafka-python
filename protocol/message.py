import time
from dataclasses import dataclass, field


@dataclass
class Message:
    value: bytes
    key: bytes | None = None
    offset: int = -1
    timestamp: float = field(default_factory=time.time)

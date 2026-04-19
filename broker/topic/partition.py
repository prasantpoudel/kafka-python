from pathlib import Path

from broker.storage.log import PartitionLog
from protocol.message import Message


class Partition:
    def __init__(self, partition_id: int, data_dir: Path):
        self.partition_id = partition_id
        # PartitionLog owns all disk I/O — Partition just delegates to it.
        self._log = PartitionLog(data_dir)

    def append(self, message: Message) -> int:
        return self._log.append(message)

    def read(self, offset: int, limit: int = 10) -> list[Message]:
        return self._log.read(offset, limit)

    @property
    def size(self) -> int:
        return self._log.next_offset

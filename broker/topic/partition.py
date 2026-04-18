from protocol.message import Message


class Partition:
    def __init__(self, partition_id: int):
        self.partition_id = partition_id
        self._log: list[Message] = []
        self._next_offset: int = 0  # write pointer

    def append(self, message: Message) -> int:
        message.offset = self._next_offset
        self._log.append(message)
        self._next_offset += 1
        return message.offset

    def read(self, offset: int, limit: int = 10) -> list[Message]:
        if offset < 0 or offset >= self._next_offset:
            return []
        return self._log[offset : offset + limit]

    @property
    def size(self) -> int:
        return len(self._log)

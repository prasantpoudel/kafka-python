import struct
from collections.abc import Generator
from pathlib import Path

from protocol.message import Message

# Record layout (big-endian):
#   offset (int64, 8 bytes) — logical message offset assigned by the partition
#   timestamp (float64, 8 bytes) — unix timestamp from the Message
#   key_len (int32, 4 bytes) — byte length of the key; 0 means key is None
#   value_len (int32, 4 bytes) — byte length of the value
#   key bytes (key_len bytes)
#   value bytes (value_len bytes)
_HEADER = struct.Struct(">qd ii")
_HEADER_SIZE = _HEADER.size  # 24 bytes

""""
TODO: Phase 5
Improvement:
Next level

1.Recovery logic
 - truncate partial records on startup
2.Batch reads
 - improve throughput

"""


class Segment:
    def __init__(self, path: Path, base_offset: int):
        # base_offset is the logical offset of the first message in this segment.
        # It is used by PartitionLog to decide which segment owns a given offset.
        self.path = path
        self.base_offset = base_offset
        # stat before open so recovery gets the correct pre-existing size.
        self._size: int = path.stat().st_size if path.exists() else 0
        # Open in append-binary mode — every write goes to the end of the file.
        self._file = open(path, "ab")

    def append(self, message: Message) -> int:
        pos = self._size
        key_bytes = message.key if message.key is not None else b""
        value_bytes = message.value
        header = _HEADER.pack(
            message.offset,  # int64 (b bytes)
            message.timestamp,  # float64 (8 bytes)
            len(key_bytes),  # int32 (4 bytes)
            len(value_bytes),  # int32 (4 bytes)
        )
        # [HEADER: 24 bytes][KEY][VALUE]
        self._file.write(header)
        self._file.write(key_bytes)
        self._file.write(value_bytes)
        self._size += _HEADER_SIZE + len(key_bytes) + len(value_bytes)
        # TODO: Batch wise flush and with fsync
        self._file.flush()
        return pos

    def iter(self, from_byte: int = 0) -> Generator[Message, None, None]:
        # Don't use self._file it is append only
        with open(self.path, "rb") as f:
            f.seek(from_byte)

            while True:
                header_bytes = f.read(_HEADER_SIZE)
                if len(header_bytes) < _HEADER_SIZE:
                    break  # EOF

                offset, timestamp, key_len, value_len = _HEADER.unpack(header_bytes)

                # Read key
                if key_len > 0:
                    key = f.read(key_len)
                    if len(key) < key_len:
                        break  # corrupted/incomplete
                else:
                    key = None

                # Read value
                value = f.read(value_len)
                if len(value) < value_len:
                    break  # corrupted/incomplete

                yield Message(value=value, key=key, offset=offset, timestamp=timestamp)

    @property
    def size(self) -> int:
        return self._size

    def close(self) -> None:
        self._file.close()

import struct
from pathlib import Path

# Each index entry is 16 bytes:
#   logical_offset (int64, 8 bytes) — the message offset stored at this position
#   byte_pos (uint64, 8 bytes) — byte position of that message inside its segment file
_ENTRY = struct.Struct(">qQ")
_ENTRY_SIZE = _ENTRY.size  # 16 bytes

# Write one index entry every INTERVAL messages.
# A smaller value means faster lookups but a larger index file.
_DEFAULT_INTERVAL = 128


class OffsetIndex:
    def __init__(self, path: Path, interval: int = _DEFAULT_INTERVAL):
        self.path = path
        self.interval = interval
        # In-memory copy of all entries — list of (logical_offset, byte_pos) tuples.
        # Kept sorted ascending by offset (entries are always appended in order).
        self._entries: list[tuple[int, int]] = []
        # If an index file already exists (broker restart), load it into memory.
        if path.exists() and path.stat().st_size > 0:
            self._load()

    def _load(self) -> None:
        # Read the raw bytes of the index file.
        # For every 16-byte chunk, unpack an (offset, byte_pos) pair and
        # append it to self._entries.
        with open(self.path, "rb") as f:
            while True:
                chunk = f.read(_ENTRY_SIZE)
                if len(chunk) < _ENTRY_SIZE:
                    break

                offset, byte_pos = _ENTRY.unpack(chunk)
                self._entries.append((offset, byte_pos))

    def maybe_append(self, offset: int, byte_pos: int, message_count: int) -> None:
        # Called after every append in PartitionLog.
        # Only write an entry when message_count is a multiple of self.interval
        # so the index stays sparse (e.g. one entry per 128 messages).

        if message_count % self.interval == 0:
            self._append(offset, byte_pos)

    def _append(self, offset: int, byte_pos: int) -> None:
        # Pack one entry and append it to the index file (open in "ab" mode).
        # Also add the tuple to self._entries so lookups work without re-reading disk.
        with open(self.path, "ab") as f:
            f.write(_ENTRY.pack(offset, byte_pos))

        self._entries.append((offset, byte_pos))

    def lookup(self, target_offset: int) -> int:
        if not self._entries:
            return 0

        left, right = 0, len(self._entries) - 1
        result = 0

        while left <= right:
            mid = (left + right) // 2
            offset, byte_pos = self._entries[mid]

            if offset == target_offset:
                return byte_pos
            elif offset < target_offset:
                result = byte_pos  # best candidate so far
                left = mid + 1
            else:
                right = mid - 1
        return result

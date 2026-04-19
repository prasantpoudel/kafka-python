from pathlib import Path

from broker.storage.index import OffsetIndex
from broker.storage.segment import Segment
from protocol.message import Message

# Default max size per segment — roll to a new file after this many bytes.
# 1 MB is small enough to test rolling easily during development.
_DEFAULT_MAX_SEGMENT_BYTES = 1 * 1024 * 1024


class PartitionLog:
    def __init__(self, data_dir: Path, max_segment_bytes: int = _DEFAULT_MAX_SEGMENT_BYTES):
        self._dir = data_dir
        self._dir.mkdir(parents=True, exist_ok=True)
        self._max_bytes = max_segment_bytes
        # Each element is a (Segment, OffsetIndex) pair.
        # The last element is always the active (writable) segment.
        self._segments: list[tuple[Segment, OffsetIndex]] = []
        # Next logical offset to assign — 0 on a brand-new log, recovered from disk otherwise.
        self._next_offset: int = 0
        # Total messages written across all segments — used to decide when to write index entries.
        self._message_count: int = 0
        self._load_or_init()

    def _load_or_init(self) -> None:
        files = sorted(self._dir.glob("*.log"))
        if not files:
            self._new_segment(0)
            return

        for file in files:
            base_offset = int(file.stem)
            seg = Segment(file, base_offset)

            index_path = file.with_suffix(".index")
            idx = OffsetIndex(index_path)

            self._segments.append((seg, idx))
        self._recover()

    def _new_segment(self, base_offset: int) -> None:
        # Create a new Segment and a matching OffsetIndex for it.
        # Name both files using the 20-digit zero-padded base_offset.
        # Append the (Segment, OffsetIndex) pair to self._segments.

        name = f"{base_offset:020d}"
        log_path = self._dir / f"{name}.log"
        index_path = self._dir / f"{name}.index"

        seg = Segment(log_path, base_offset)
        idx = OffsetIndex(index_path)
        self._segments.append((seg, idx))

    def _recover(self) -> None:
        # Called on startup when existing segment files are found.
        # Iterate through all messages in the last segment to find the highest offset.
        # Set self._next_offset = highest_offset + 1.
        # This ensures we don't overwrite messages that survived a restart.

        if not self._segments:
            self._next_offset = 0
            return

        # Count across all segments so _message_count stays consistent after restart.
        # Only the last segment needs to be scanned for highest_offset — earlier segments
        # are full and their last message offset equals (next_segment.base_offset - 1).
        total_count = 0
        for i, (seg, _) in enumerate(self._segments[:-1]):
            # Closed segments: message count = next segment's base_offset - this base_offset
            next_base = self._segments[i + 1][0].base_offset
            total_count += next_base - seg.base_offset

        last_seg, _ = self._segments[-1]
        highest_offset = -1
        last_seg_count = 0

        for msg in last_seg.iter():
            highest_offset = msg.offset
            last_seg_count += 1

        self._next_offset = highest_offset + 1
        self._message_count = total_count + last_seg_count

    def append(self, message: Message) -> int:
        seg, idx = self._segments[-1]

        # 1. assign offset
        message.offset = self._next_offset

        # 2. write to segment
        byte_pos = seg.append(message)

        # 3. update index
        idx.maybe_append(message.offset, byte_pos, self._message_count)

        # 4. increment counters
        self._next_offset += 1
        self._message_count += 1

        # 5. roll if needed
        if seg.size >= self._max_bytes:
            self._new_segment(self._next_offset)

        return message.offset

    def read(self, offset: int, limit: int = 10) -> list[Message]:
        if offset < 0 or offset >= self._next_offset:
            return []

        seg_idx = self._find_segment_index(offset)
        result = []

        for i in range(seg_idx, len(self._segments)):
            seg, idx = self._segments[i]

            if i == seg_idx:
                byte_pos = idx.lookup(offset)
            else:
                byte_pos = 0

            for msg in seg.iter(from_byte=byte_pos):
                if msg.offset < offset:
                    continue

                result.append(msg)
                if len(result) >= limit:
                    return result
        return result

    def _find_segment_index(self, offset: int) -> int:
        # Binary search self._segments for the last segment whose base_offset <= offset.
        # Return the list index (not the offset) of that segment.
        # This tells the caller which segment to start reading from.
        left, right = 0, len(self._segments) - 1
        result = 0

        while left <= right:
            mid = (left + right) // 2
            seg, _ = self._segments[mid]

            if seg.base_offset <= offset:
                result = mid
                left = mid + 1
            else:
                right = mid - 1

        return result

    @property
    def next_offset(self) -> int:
        return self._next_offset

    def close(self) -> None:
        for seg, _ in self._segments:
            seg.close()

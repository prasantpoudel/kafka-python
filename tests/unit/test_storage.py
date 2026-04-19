import pytest

from broker.storage.index import OffsetIndex
from broker.storage.log import PartitionLog
from broker.storage.segment import Segment
from protocol.message import Message

# ---------------------------------------------------------------------------
# Segment
# ---------------------------------------------------------------------------


@pytest.fixture
def segment(tmp_path):
    return Segment(path=tmp_path / "00000.log", base_offset=0)


def test_segment_append_returns_byte_position(segment):
    pos = segment.append(Message(value=b"hello", offset=0))
    assert pos == 0


def test_segment_append_increments_size(segment):
    size_before = segment.size
    segment.append(Message(value=b"hello", offset=0))
    assert segment.size > size_before


def test_segment_iter_reads_back_message(segment):
    msg = Message(value=b"hello", offset=0)
    segment.append(msg)
    messages = list(segment.iter())
    assert len(messages) == 1
    assert messages[0].value == b"hello"
    assert messages[0].offset == 0


def test_segment_iter_reads_multiple_messages(segment):
    for i in range(5):
        segment.append(Message(value=f"msg-{i}".encode(), offset=i))
    messages = list(segment.iter())
    assert len(messages) == 5
    assert [m.value for m in messages] == [f"msg-{i}".encode() for i in range(5)]


def test_segment_iter_from_byte_skips_earlier_records(segment):
    segment.append(Message(value=b"first", offset=0))
    pos = segment.append(Message(value=b"second", offset=1))
    messages = list(segment.iter(from_byte=pos))
    assert len(messages) == 1
    assert messages[0].value == b"second"


def test_segment_preserves_key_none(segment):
    segment.append(Message(value=b"no-key", key=None, offset=0))
    messages = list(segment.iter())
    assert messages[0].key is None


def test_segment_preserves_key_bytes(segment):
    segment.append(Message(value=b"with-key", key=b"mykey", offset=0))
    messages = list(segment.iter())
    assert messages[0].key == b"mykey"


# ---------------------------------------------------------------------------
# OffsetIndex
# ---------------------------------------------------------------------------


@pytest.fixture
def index(tmp_path):
    return OffsetIndex(path=tmp_path / "00000.index", interval=4)


def test_index_lookup_empty_returns_zero(index):
    assert index.lookup(99) == 0


def test_index_lookup_returns_correct_byte_pos(index):
    index._append(0, 0)
    index._append(4, 100)
    index._append(8, 200)
    assert index.lookup(4) == 100
    assert index.lookup(8) == 200


def test_index_lookup_returns_floor_entry(index):
    index._append(0, 0)
    index._append(8, 200)
    # offset 5 is not indexed — should return byte_pos of offset 0
    assert index.lookup(5) == 0


def test_index_maybe_append_writes_on_interval(index):
    # interval=4, so entries should be written at message_count 0, 4, 8 ...
    index.maybe_append(offset=0, byte_pos=0, message_count=0)
    index.maybe_append(offset=1, byte_pos=50, message_count=1)
    index.maybe_append(offset=4, byte_pos=200, message_count=4)
    assert len(index._entries) == 2  # only message_count 0 and 4


def test_index_survives_reload(tmp_path):
    path = tmp_path / "00000.index"
    idx = OffsetIndex(path=path, interval=1)
    idx._append(0, 0)
    idx._append(1, 100)

    reloaded = OffsetIndex(path=path, interval=1)
    assert reloaded._entries == [(0, 0), (1, 100)]


# ---------------------------------------------------------------------------
# PartitionLog
# ---------------------------------------------------------------------------


@pytest.fixture
def log(tmp_path):
    return PartitionLog(data_dir=tmp_path)


def test_log_append_assigns_offset(log):
    msg = Message(value=b"hello")
    offset = log.append(msg)
    assert offset == 0


def test_log_append_increments_offset(log):
    log.append(Message(value=b"first"))
    offset = log.append(Message(value=b"second"))
    assert offset == 1


def test_log_read_returns_message(log):
    log.append(Message(value=b"hello"))
    messages = log.read(offset=0)
    assert messages[0].value == b"hello"


def test_log_read_invalid_offset_returns_empty(log):
    log.append(Message(value=b"hello"))
    assert log.read(offset=99) == []
    assert log.read(offset=-1) == []


def test_log_read_with_limit(log):
    for i in range(10):
        log.append(Message(value=f"msg-{i}".encode()))
    messages = log.read(offset=0, limit=3)
    assert len(messages) == 3
    assert messages[0].value == b"msg-0"


def test_log_rolls_segment_when_full(tmp_path):
    # tiny max so rolling happens after a few messages
    log = PartitionLog(data_dir=tmp_path, max_segment_bytes=100)
    for i in range(20):
        log.append(Message(value=b"x" * 10, offset=i))
    assert len(log._segments) > 1


def test_log_read_across_segments(tmp_path):
    log = PartitionLog(data_dir=tmp_path, max_segment_bytes=100)
    for i in range(20):
        log.append(Message(value=f"msg-{i}".encode()))
    # read spanning multiple segments
    messages = log.read(offset=0, limit=20)
    assert len(messages) == 20
    assert messages[0].value == b"msg-0"
    assert messages[19].value == b"msg-19"


def test_log_survives_restart(tmp_path):
    log = PartitionLog(data_dir=tmp_path)
    for i in range(5):
        log.append(Message(value=f"msg-{i}".encode()))
    log.close()

    # reopen from same directory — simulates broker restart
    recovered = PartitionLog(data_dir=tmp_path)
    assert recovered.next_offset == 5
    messages = recovered.read(offset=0, limit=5)
    assert len(messages) == 5
    assert messages[4].value == b"msg-4"

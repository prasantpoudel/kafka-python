import pytest

from broker.topic.partition import Partition
from protocol.message import Message


@pytest.fixture
def partition():
    return Partition(partition_id=0)


def test_append_assigns_offset(partition):
    msg = Message(value=b"hello")
    offset = partition.append(msg)
    assert offset == 0


def test_append_increments_offset_sequentially(partition):
    msg1 = Message(value=b"hello")
    offset = partition.append(msg1)
    assert offset == 0

    msg2 = Message(value=b"hi")
    offset = partition.append(msg2)
    assert offset == 1


def test_read_returns_correct_messages(partition):
    msg = Message(value=b"hello")
    offset = partition.append(msg)
    poll_message = partition.read(offset)
    assert msg == poll_message[0]


def test_read_with_limit(partition):
    for i in range(5):
        partition.append(Message(value=f"msg-{i}".encode()))
    messages = partition.read(offset=0, limit=2)
    assert len(messages) == 2
    assert messages[0].value == b"msg-0"
    assert messages[1].value == b"msg-1"


def test_read_from_invalid_offset_returns_empty(partition):
    partition.append(Message(value=b"hello"))
    assert partition.read(offset=99) == []


def test_read_negative_offset_returns_empty(partition):
    assert partition.read(offset=-1) == []


def test_size_reflects_appended_messages(partition):
    assert partition.size == 0
    for i in range(3):
        partition.append(Message(value=f"msg-{i}".encode()))
    assert partition.size == 3

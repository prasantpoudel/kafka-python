import pytest

from broker.topic.manager import TopicManager
from client.consumer import Consumer
from client.producer import Producer


@pytest.fixture
def setup():
    tm = TopicManager()
    tm.create_topic("orders", num_partitions=3)
    producer = Producer(tm)
    consumer = Consumer(tm, group_id="group-1")
    return tm, producer, consumer


def test_producer_send_returns_offset(setup):
    tm, producer, consumer = setup
    offset, partition_id = producer.send("orders", b"hello")
    assert offset == 0


def test_producer_auto_creates_topic(setup):
    tm, producer, consumer = setup
    offset, _ = producer.send("delivery", b"test_delivery")
    assert offset == 0
    assert "delivery" in tm.topics


def test_consumer_poll_reads_messages(setup):
    tm, producer, consumer = setup
    producer.send("orders", b"order done")
    assert consumer.poll("orders", 0)[0].value == b"order done"


def test_consumer_poll_advances_offset(setup):
    tm, producer, consumer = setup
    for _ in range(3):
        producer.send("orders", b"msg", key=b"user-1")
    # find which partition user-1 maps to
    partition_id = tm.get_partition("orders", key=b"user-1").partition_id
    consumer.poll("orders", partition_id, limit=2)
    assert consumer._offsets[("orders", partition_id)] == 2


def test_consumer_poll_returns_empty_when_no_new_messages(setup):
    tm, producer, consumer = setup
    producer.send("orders", b"msg", key=b"user-1")
    partition_id = tm.get_partition("orders", key=b"user-1").partition_id
    consumer.poll("orders", partition_id)
    # second poll — no new messages written, should return empty
    assert consumer.poll("orders", partition_id) == []


def test_consumer_resumes_from_last_position(setup):
    tm, producer, consumer = setup
    for i in range(4):
        producer.send("orders", f"msg-{i}".encode(), key=b"user-1")
    partition_id = tm.get_partition("orders", key=b"user-1").partition_id
    # first poll reads 2
    consumer.poll("orders", partition_id, limit=2)
    # second poll should continue from offset 2, not restart from 0
    messages = consumer.poll("orders", partition_id, limit=2)
    assert messages[0].value == b"msg-2"
    assert messages[1].value == b"msg-3"


def test_two_consumers_have_independent_offsets(setup):
    tm, producer, _ = setup
    consumer_a = Consumer(tm, group_id="group-a")
    consumer_b = Consumer(tm, group_id="group-b")
    for i in range(3):
        producer.send("orders", f"msg-{i}".encode(), key=b"user-1")
    partition_id = tm.get_partition("orders", key=b"user-1").partition_id
    # consumer_a reads all 3
    consumer_a.poll("orders", partition_id, limit=3)
    # consumer_b hasn't read anything yet — should still get all 3
    messages = consumer_b.poll("orders", partition_id, limit=3)
    assert len(messages) == 3
    assert messages[0].value == b"msg-0"

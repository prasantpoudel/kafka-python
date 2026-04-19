import pytest
from fastapi.testclient import TestClient

from broker.server import app
from client.http_consumer import HttpConsumer
from client.http_producer import HttpProducer


@pytest.fixture
def broker():
    with TestClient(app) as client:
        yield client


@pytest.fixture
def http_producer(broker):
    return HttpProducer(broker_url="http://testserver", transport=broker._transport)


@pytest.fixture
def http_consumer(broker):
    return HttpConsumer(broker_url="http://testserver", transport=broker._transport)


def test_http_producer_send_returns_offset(http_producer, broker):
    broker.post("/topics", json={"name": "orders", "num_partitions": 3})
    offset, partition_id = http_producer.send("orders", b"hello")
    assert offset == 0
    assert 0 <= partition_id < 3


def test_http_consumer_poll_returns_messages(http_producer, http_consumer, broker):
    broker.post("/topics", json={"name": "orders", "num_partitions": 3})
    _, partition_id = http_producer.send("orders", b"hello", key=b"user-1")
    messages = http_consumer.poll("orders", partition_id)
    assert messages[0].value == b"hello"


def test_http_consumer_commit(http_producer, http_consumer, broker):
    broker.post("/topics", json={"name": "orders", "num_partitions": 3})
    _, partition_id = http_producer.send("orders", b"hello", key=b"user-1")
    http_consumer.poll("orders", partition_id)
    http_consumer.commit("orders", partition_id)  # should not raise

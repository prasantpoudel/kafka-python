import pytest

from broker.metadata.store import MetadataStore


@pytest.fixture
def store():
    return MetadataStore()


@pytest.fixture
def store_with_topic(store):
    store.add_topic("orders", num_partitions=3, replicas=[1, 2, 3])
    return store


def test_add_topic_stores_partition_count(store):
    store.add_topic("orders", num_partitions=3, replicas=[1, 2, 3])
    assert store.get_topics()["orders"] == 3


def test_add_topic_duplicate_is_ignored(store):
    store.add_topic("orders", num_partitions=3, replicas=[1, 2, 3])
    store.add_topic("orders", num_partitions=5, replicas=[1, 2])
    assert store.get_topics()["orders"] == 3


def test_add_topic_first_replica_is_leader(store):
    store.add_topic("orders", num_partitions=2, replicas=[2, 3, 1])
    assert store.get_leader("orders", 0) == 2
    assert store.get_leader("orders", 1) == 2


def test_add_topic_all_replicas_start_in_isr(store):
    store.add_topic("orders", num_partitions=1, replicas=[1, 2, 3])
    assert store.get_isr("orders", 0) == [1, 2, 3]


def test_add_topic_partitions_have_independent_isr(store):
    # Mutating one partition's ISR must not affect another
    store.add_topic("orders", num_partitions=2, replicas=[1, 2, 3])
    store.update_isr("orders", 0, [1])
    assert store.get_isr("orders", 1) == [1, 2, 3]


def test_get_leader(store_with_topic):
    assert store_with_topic.get_leader("orders", 0) == 1


def test_set_leader(store_with_topic):
    store_with_topic.set_leader("orders", 0, broker_id=3)
    assert store_with_topic.get_leader("orders", 0) == 3


def test_set_leader_does_not_affect_other_partitions(store_with_topic):
    store_with_topic.set_leader("orders", 0, broker_id=3)
    assert store_with_topic.get_leader("orders", 1) == 1


def test_get_isr(store_with_topic):
    assert store_with_topic.get_isr("orders", 0) == [1, 2, 3]


def test_update_isr(store_with_topic):
    store_with_topic.update_isr("orders", 0, [1, 2])
    assert store_with_topic.get_isr("orders", 0) == [1, 2]


def test_update_isr_does_not_affect_other_partitions(store_with_topic):
    store_with_topic.update_isr("orders", 0, [1])
    assert store_with_topic.get_isr("orders", 1) == [1, 2, 3]


def test_get_topics_returns_all(store):
    store.add_topic("orders", num_partitions=3, replicas=[1, 2, 3])
    store.add_topic("payments", num_partitions=2, replicas=[1, 2])
    assert store.get_topics() == {"orders": 3, "payments": 2}

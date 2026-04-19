import pytest

from broker.topic.manager import TopicManager


@pytest.fixture
def topic_manager(tmp_path):
    return TopicManager(data_dir=tmp_path)


def test_create_topic(topic_manager):
    topic_manager.create_topic(name="kafka")
    assert "kafka" in topic_manager.topics


def test_create_duplicate_topic_is_ignored(topic_manager):
    for _ in range(2):
        topic_manager.create_topic(name="topic")
    assert len(topic_manager.topics) == 1


def test_get_partitions_returns_correct_count(topic_manager):
    topic_manager.create_topic(name="test", num_partitions=2)
    assert len(topic_manager.topic_partitions["test"]) == 2


def test_get_partitions_unknown_topic_returns_empty(topic_manager):
    assert topic_manager.get_partitions("unknow") == []


def test_get_partition_round_robin_cycles(topic_manager):
    topic_manager.create_topic(name="roundrobin", num_partitions=3)
    ids = [topic_manager.get_partition("roundrobin").partition_id for _ in range(4)]
    assert ids == [0, 1, 2, 0]


def test_get_partition_same_key_always_same_partition(topic_manager):
    topic_manager.create_topic(name="orders", num_partitions=3)
    partitions = [
        topic_manager.get_partition("orders", key=b"user-1").partition_id for _ in range(3)
    ]
    assert len(set(partitions)) == 1


def test_get_partition_different_keys_can_map_differently(topic_manager):
    topic_manager.create_topic(name="orders", num_partitions=3)
    p1 = topic_manager.get_partition("orders", key=b"user-1").partition_id
    p2 = topic_manager.get_partition("orders", key=b"user-2").partition_id
    assert p1 != p2


def test_delete_topic_removes_it(topic_manager):
    topic_manager.create_topic(name="temp")
    topic_manager.delete_topic("temp")
    assert "temp" not in topic_manager.topics
    assert "temp" not in topic_manager.topic_partitions
    assert "temp" not in topic_manager.topics_counter


def test_list_topics(topic_manager):
    topic_manager.create_topic(name="topic-a")
    topic_manager.create_topic(name="topic-b")
    assert {"topic-a", "topic-b"} == topic_manager.list_topics()

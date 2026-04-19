from unittest.mock import MagicMock, patch

import pytest

from broker.metadata.store import MetadataStore
from broker.replication.follower import ReplicationFollower
from broker.replication.leader import ReplicationLeader
from broker.topic.partition import Partition
from protocol.message import Message

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def store():
    s = MetadataStore()
    s.add_topic("orders", num_partitions=1, replicas=[1, 2, 3])
    return s


@pytest.fixture
def leader_partition(tmp_path):
    return Partition(partition_id=0, data_dir=tmp_path / "leader")


@pytest.fixture
def follower_partition(tmp_path):
    return Partition(partition_id=0, data_dir=tmp_path / "follower")


@pytest.fixture
def leader(leader_partition, store):
    return ReplicationLeader(
        topic="orders",
        partition_id=0,
        broker_id=1,
        partition=leader_partition,
        metadata_store=store,
    )


@pytest.fixture
def follower(follower_partition):
    return ReplicationFollower(
        topic="orders",
        partition_id=0,
        broker_id=2,
        partition=follower_partition,
        leader_url="http://broker-1:9092",
    )


# ---------------------------------------------------------------------------
# ReplicationFollower
# ---------------------------------------------------------------------------


def test_follower_apply_writes_to_partition(follower):
    msg = Message(value=b"hello", offset=0)
    follower.apply(msg)
    assert follower.partition.size == 1


def test_follower_fetch_offset_starts_at_zero(follower):
    assert follower.fetch_offset() == 0


def test_follower_fetch_offset_advances_after_apply(follower):
    follower.apply(Message(value=b"a", offset=0))
    follower.apply(Message(value=b"b", offset=1))
    assert follower.fetch_offset() == 2


# ---------------------------------------------------------------------------
# ReplicationLeader
# ---------------------------------------------------------------------------


def test_leader_write_appends_to_partition(leader):
    with patch("broker.replication.leader.httpx.post"):
        offset = leader.write(Message(value=b"hello"))
    assert offset == 0
    assert leader.partition.size == 1


def test_leader_write_returns_correct_offset(leader):
    with patch("broker.replication.leader.httpx.post"):
        assert leader.write(Message(value=b"first")) == 0
        assert leader.write(Message(value=b"second")) == 1


def test_leader_add_follower_registers_with_zero_offset(leader):
    leader.add_follower(broker_id=2, url="http://broker-2:9092")
    assert leader._fetch_offsets[2] == 0


def test_leader_replicate_posts_to_follower_url(leader):
    leader.add_follower(broker_id=2, url="http://broker-2:9092")
    with patch("broker.replication.leader.httpx.post") as mock_post:
        mock_post.return_value = MagicMock(raise_for_status=lambda: None)
        leader.write(Message(value=b"hello"))
        called_url = mock_post.call_args[0][0]
        assert called_url == "http://broker-2:9092/internal/replicate/orders/0"


def test_leader_replicate_updates_fetch_offset_on_success(leader):
    leader.add_follower(broker_id=2, url="http://broker-2:9092")
    with patch("broker.replication.leader.httpx.post") as mock_post:
        mock_post.return_value = MagicMock(raise_for_status=lambda: None)
        leader.write(Message(value=b"hello"))
        assert leader._fetch_offsets[2] == 1


def test_leader_replicate_skips_fetch_offset_update_on_failure(leader):
    leader.add_follower(broker_id=2, url="http://broker-2:9092")
    with patch("broker.replication.leader.httpx.post", side_effect=Exception("down")):
        leader.write(Message(value=b"hello"))
        assert leader._fetch_offsets[2] == 0


def test_leader_update_isr_includes_caught_up_followers(leader, store):
    leader.add_follower(broker_id=2, url="http://broker-2:9092")
    leader.add_follower(broker_id=3, url="http://broker-3:9092")
    with patch("broker.replication.leader.httpx.post") as mock_post:
        mock_post.return_value = MagicMock(raise_for_status=lambda: None)
        leader.write(Message(value=b"hello"))
    leader.update_isr()
    assert set(store.get_isr("orders", 0)) == {1, 2, 3}


def test_leader_update_isr_excludes_lagging_followers(leader, store):
    leader.add_follower(broker_id=2, url="http://broker-2:9092")
    # broker-2 never receives replication — fetch_offset stays 0
    with patch("broker.replication.leader.httpx.post", side_effect=Exception("down")):
        leader.write(Message(value=b"hello"))
    leader.update_isr()
    assert store.get_isr("orders", 0) == [1]

import pytest

from broker.consumer_group.coordinator import GroupCoordinator
from broker.consumer_group.rebalancer import Rebalancer

# ---------------------------------------------------------------------------
# Rebalancer
# ---------------------------------------------------------------------------


def test_rebalance_empty_consumers():
    assert Rebalancer().rebalance([], num_partitions=3) == {}


def test_rebalance_single_consumer_gets_all_partitions():
    result = Rebalancer().rebalance(["c0"], num_partitions=3)
    assert result == {"c0": [0, 1, 2]}


def test_rebalance_even_distribution():
    result = Rebalancer().rebalance(["c0", "c1", "c2"], num_partitions=3)
    assert result == {"c0": [0], "c1": [1], "c2": [2]}


def test_rebalance_uneven_distribution():
    result = Rebalancer().rebalance(["c0", "c1", "c2"], num_partitions=5)
    assert result == {"c0": [0, 3], "c1": [1, 4], "c2": [2]}


def test_rebalance_more_consumers_than_partitions():
    result = Rebalancer().rebalance(["c0", "c1", "c2"], num_partitions=2)
    assert result == {"c0": [0], "c1": [1], "c2": []}


def test_rebalance_covers_all_partitions():
    result = Rebalancer().rebalance(["c0", "c1"], num_partitions=6)
    all_partitions = sorted(p for parts in result.values() for p in parts)
    assert all_partitions == list(range(6))


# ---------------------------------------------------------------------------
# GroupCoordinator
# ---------------------------------------------------------------------------


@pytest.fixture
def coordinator():
    return GroupCoordinator()


def test_join_returns_assigned_partitions(coordinator):
    partitions = coordinator.join("g1", "c0", "orders", num_partitions=3)
    assert partitions == [0, 1, 2]


def test_join_second_consumer_triggers_rebalance(coordinator):
    coordinator.join("g1", "c0", "orders", num_partitions=4)
    coordinator.join("g1", "c1", "orders", num_partitions=4)
    assert coordinator.get_assignment("g1", "c0") == [0, 2]
    assert coordinator.get_assignment("g1", "c1") == [1, 3]


def test_join_same_consumer_twice_is_idempotent(coordinator):
    coordinator.join("g1", "c0", "orders", num_partitions=3)
    coordinator.join("g1", "c0", "orders", num_partitions=3)
    assert coordinator._member["g1"].count("c0") == 1


def test_join_different_groups_are_independent(coordinator):
    coordinator.join("g1", "c0", "orders", num_partitions=3)
    coordinator.join("g2", "c0", "orders", num_partitions=3)
    assert coordinator.get_assignment("g1", "c0") == [0, 1, 2]
    assert coordinator.get_assignment("g2", "c0") == [0, 1, 2]


def test_leave_triggers_rebalance_for_remaining(coordinator):
    coordinator.join("g1", "c0", "orders", num_partitions=4)
    coordinator.join("g1", "c1", "orders", num_partitions=4)
    coordinator.leave("g1", "c1", num_partitions=4)
    assert coordinator.get_assignment("g1", "c0") == [0, 1, 2, 3]


def test_leave_removes_consumer_assignment(coordinator):
    coordinator.join("g1", "c0", "orders", num_partitions=3)
    coordinator.join("g1", "c1", "orders", num_partitions=3)
    coordinator.leave("g1", "c1", num_partitions=3)
    assert coordinator.get_assignment("g1", "c1") == []


def test_leave_last_consumer_leaves_group_empty(coordinator):
    coordinator.join("g1", "c0", "orders", num_partitions=3)
    coordinator.leave("g1", "c0", num_partitions=3)
    assert coordinator._member["g1"] == []


def test_commit_and_get_offset(coordinator):
    coordinator.commit("g1", "orders", partition_id=0, offset=42)
    assert coordinator.get_offset("g1", "orders", partition_id=0) == 42


def test_get_offset_returns_zero_if_never_committed(coordinator):
    assert coordinator.get_offset("g1", "orders", partition_id=0) == 0


def test_commit_updates_existing_offset(coordinator):
    coordinator.commit("g1", "orders", 0, 10)
    coordinator.commit("g1", "orders", 0, 25)
    assert coordinator.get_offset("g1", "orders", 0) == 25


def test_get_assignment_unknown_consumer_returns_empty(coordinator):
    assert coordinator.get_assignment("g1", "unknown") == []

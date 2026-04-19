from broker.consumer_group.rebalancer import Rebalancer


class GroupCoordinator:
    def __init__(self):
        # group_id → list of consumer_ids currently in the group
        self._member: dict[str, list[str]] = {}
        # (group_id, consumer_id) → list of assigned partition_ids
        self._assignment: dict[tuple[str, str], list[int]] = {}
        # (group_id, topic, partition_id) → last committed offset
        self._offsets: dict[tuple[str, str, int], int] = {}
        self._rebalancer = Rebalancer()

    def join(self, group_id: str, consumer_id: str, topic: str, num_partitions: int) -> list[int]:
        if group_id not in self._member:
            self._member[group_id] = []

        if consumer_id not in self._member[group_id]:
            self._member[group_id].append(consumer_id)
            self._rebalance(group_id, num_partitions)

        return self._assignment[(group_id, consumer_id)]

    def leave(self, group_id: str, consumer_id: str, num_partitions: int) -> None:
        if consumer_id in self._member[group_id]:
            self._member[group_id].remove(consumer_id)
            del self._assignment[(group_id, consumer_id)]

            if self._member.get(group_id):
                self._rebalance(group_id, num_partitions)
        return

    def commit(self, group_id: str, topic: str, partition_id: int, offset: int) -> None:
        self._offsets[(group_id, topic, partition_id)] = offset
        return

    def get_offset(self, group_id: str, topic: str, partition_id: int) -> int:
        return self._offsets.get((group_id, topic, partition_id), 0)

    def get_assignment(self, group_id: str, consumer_id: str) -> list[int]:
        return self._assignment.get((group_id, consumer_id), [])

    def _rebalance(self, group_id: str, num_partitions: int) -> None:
        new_assignment = self._rebalancer.rebalance(self._member[group_id], num_partitions)
        for consumer_id, partition_ids in new_assignment.items():
            self._assignment[(group_id, consumer_id)] = partition_ids

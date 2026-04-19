from dataclasses import dataclass


@dataclass
class PartitionMetadata:
    leader: int  # broker_id of the current leader
    isr: list[int]  # in-sync replica broker_ids e.g. [1, 2, 3]
    replicas: list[int]  # all replica broker_ids (never changes after creation)


class MetadataStore:
    def __init__(self):
        # topic → num_partitions
        self._topics: dict[str, int] = {}
        # (topic, partition_id) → PartitionMetadata
        self._partitions: dict[tuple[str, int], PartitionMetadata] = {}

    def add_topic(self, name: str, num_partitions: int, replicas: list[int]) -> None:
        if name in self._topics:
            return
        self._topics[name] = num_partitions
        for partition_id in range(num_partitions):
            self._partitions[(name, partition_id)] = PartitionMetadata(
                leader=replicas[0],
                isr=list(replicas),
                replicas=list(replicas),
            )

    def get_leader(self, topic: str, partition_id: int) -> int:
        return self._partitions[(topic, partition_id)].leader

    def set_leader(self, topic: str, partition_id: int, broker_id: int) -> None:
        self._partitions[(topic, partition_id)].leader = broker_id

    def get_isr(self, topic: str, partition_id: int) -> list[int]:
        return self._partitions[(topic, partition_id)].isr

    def update_isr(self, topic: str, partition_id: int, broker_ids: list[int]) -> None:
        self._partitions[(topic, partition_id)].isr = broker_ids

    def get_topics(self) -> dict[str, int]:
        return self._topics

import hashlib
from pathlib import Path

from broker.topic.partition import Partition


class TopicManager:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.topics: set[str] = set()
        self.topic_partitions = {}
        self.topics_counter = {}

    def create_topic(self, name: str, num_partitions: int = 3) -> None:
        if name in self.topics:
            return
        self.topics.add(name)
        self.topic_partitions[name] = [
            Partition(partition_id=i, data_dir=self.data_dir / name / str(i))
            for i in range(num_partitions)
        ]
        self.topics_counter[name] = 0

    def get_partitions(self, topic: str) -> list[Partition]:
        if topic not in self.topics:
            return []
        return self.topic_partitions[topic]

    def get_partition(self, topic: str, key: bytes | None = None) -> Partition:
        # No key → round-robin across partitions.
        # A per-topic counter cycles 0 → 1 → 2 → 0 ...
        # This spreads load evenly when producers don't care about ordering.
        if not key:
            old_counter = self.topics_counter[topic]
            self.topics_counter[topic] = (old_counter + 1) % len(self.topic_partitions[topic])
            return self.topic_partitions[topic][old_counter]

        # Key provided → deterministic hash-based partitioning.
        # We use hashlib.md5 instead of Python's built-in hash() because:
        #   - hash() is randomised per process start (Python's hash-randomisation
        #     security feature), so the same key maps to a different partition on
        #     every restart — breaking ordering guarantees.
        #   - md5(key) always produces the same digest everywhere, forever.
        # md5().hexdigest() returns a hex string e.g. 'a3f1c2b8...'.
        # int(..., 16) converts that hex string to a plain integer.
        # % num_partitions maps it to a valid partition index.
        # Real Kafka uses murmur2 for the same reason — fast and deterministic,
        # not cryptographically secure.
        partition_index = int(hashlib.md5(key).hexdigest(), 16) % len(self.topic_partitions[topic])
        return self.topic_partitions[topic][partition_index]

    def list_topics(self) -> set[str]:
        return self.topics

    def delete_topic(self, name: str) -> None:
        if name in self.topics:
            self.topics.remove(name)
            del self.topic_partitions[name]
            del self.topics_counter[name]
        return

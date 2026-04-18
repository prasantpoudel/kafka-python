from broker.topic.manager import TopicManager
from protocol.message import Message


class Consumer:
    def __init__(self, topic_manager: TopicManager, group_id: str):
        self.topic_manager = topic_manager
        self.group_id = group_id
        # tracks read position per (topic, partition_id) for this consumer.
        # keyed by (topic, partition_id) because each partition has its own
        # independent offset — offset=3 on partition 0 is different from
        # offset=3 on partition 1.
        self._offsets: dict[tuple[str, int], int] = {}

    def poll(self, topic: str, partition_id: int, limit: int = 10) -> list[Message]:
        partitions = self.topic_manager.get_partitions(topic)
        partition = partitions[partition_id]

        # start from 0 if this consumer has never read this partition before
        current_offset = self._offsets.get((topic, partition_id), 0)
        messages = partition.read(offset=current_offset, limit=limit)

        # advance the internal offset by however many messages were returned.
        # we don't advance by `limit` because the partition may have fewer
        # messages than requested — len(messages) is always accurate.
        self._offsets[(topic, partition_id)] = current_offset + len(messages)

        return messages

    def commit(self, topic: str, partition_id: int) -> None:
        # In Phase 1 (in-memory), offsets are already tracked in _offsets via poll.
        # commit() is a no-op here but is intentionally kept as a separate method
        # because in Phase 3 it will persist the offset to disk so the consumer
        # can resume after a restart without re-reading from the beginning.
        pass

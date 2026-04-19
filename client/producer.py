from broker.topic.manager import TopicManager
from protocol.message import Message


class Producer:
    def __init__(self, topic_manager: TopicManager):
        self.topic_manager = topic_manager

    def send(self, topic: str, value: bytes, key: bytes | None = None) -> tuple[int, int]:
        # Auto-create topic if it doesn't exist.
        # Real Kafka requires topics to be created explicitly by default.
        # We skip that strictness for now since we have no config layer yet.
        if topic not in self.topic_manager.topics:
            self.topic_manager.create_topic(topic)

        partition = self.topic_manager.get_partition(topic, key)
        message = Message(value=value, key=key)
        offset = partition.append(message)
        # return both offset and partition_id so callers don't need to
        # call get_partition() again — a second call would move the
        # round-robin counter and return the wrong partition.
        return offset, partition.partition_id

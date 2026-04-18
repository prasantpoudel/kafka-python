from broker.topic.manager import TopicManager
from protocol.message import Message


class Consumer:
    def __init__(self, topic_manager: TopicManager, group_id: int): ...

    def poll(self, topic: str, partition_id: int, limit: int = 10) -> list[Message]: ...

    def commit(self, topic: str, partition_id: int) -> None: ...

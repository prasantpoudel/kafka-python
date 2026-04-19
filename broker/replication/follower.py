from broker.topic.partition import Partition
from protocol.message import Message


class ReplicationFollower:
    def __init__(
        self, topic: str, partition_id: int, broker_id: int, partition: Partition, leader_url: str
    ):
        self.topic = topic
        self.partition_id = partition_id
        self.broker_id = broker_id
        self.partition = partition
        self.leader_url = leader_url

    def apply(self, message: Message) -> None:
        # Called when the leader POSTs a replicated message to /internal/replicate/...
        # Just append it directly to the local partition — no offset assignment needed
        # because the leader already set message.offset before sending.
        self.partition.append(message)

    def fetch_offset(self) -> int:
        # The next offset this follower needs from the leader.
        # partition.size equals the count of messages written, which is also
        # the next expected offset (offsets start at 0).
        # The leader compares this against its own partition.size to decide ISR membership.
        return self.partition.size

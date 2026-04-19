import httpx

from broker.metadata.store import MetadataStore
from broker.topic.partition import Partition
from protocol.message import Message


class ReplicationLeader:
    def __init__(
        self,
        topic: str,
        partition_id: int,
        broker_id: int,
        partition: Partition,
        metadata_store: MetadataStore,
    ):
        self.topic = topic
        self.partition_id = partition_id
        self.broker_id = broker_id
        self.partition = partition
        self.metadata_store = metadata_store
        # _followers: dict[int, str] — broker_id → follower URL
        self._followers: dict[int, str] = {}

        # _fetch_offsets: dict[int, int] — broker_id → last fetched offset (for ISR tracking)
        self._fetch_offsets: dict[int, int] = {}

    def add_follower(self, broker_id: int, url: str) -> None:
        self._followers[broker_id] = url
        self._fetch_offsets[broker_id] = 0

    def write(self, message: Message) -> int:
        offset = self.partition.append(message=message)
        self._replicate(message)
        return offset

    def _replicate(self, message: Message) -> None:
        payload = {
            "offset": message.offset,
            "value": message.value.decode("utf-8"),
            "key": message.key.decode("utf-8") if message.key else None,
            "timestamp": message.timestamp,
        }
        for broker_id, url in self._followers.items():
            try:
                response = httpx.post(
                    f"{url}/internal/replicate/{self.topic}/{self.partition_id}", json=payload
                )
                response.raise_for_status()
                self._fetch_offsets[broker_id] = message.offset + 1
            except Exception:
                continue

    def update_isr(self) -> None:
        in_sync = [
            broker_id
            for broker_id, fetch_offset in self._fetch_offsets.items()
            if fetch_offset >= self.partition.size
        ]
        self.metadata_store.update_isr(self.topic, self.partition_id, [self.broker_id] + in_sync)

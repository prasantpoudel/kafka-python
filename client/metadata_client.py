import httpx


class MetadataClient:
    def __init__(self, broker_url: str):
        self.broker_url = broker_url
        self._cache: dict[str, int] = {}

    def get_partition_count(self, topic: str) -> int:
        if topic in self._cache:
            return self._cache[topic]
        return self.refresh(topic)

    def refresh(self, topic: str) -> int:
        # force fetch from broker even if cached.
        # called on first access and when metadata becomes stale
        # (e.g. leader change in Phase 4 — partition count may have changed).
        response = httpx.get(f"{self.broker_url}/topics/{topic}")
        response.raise_for_status()
        self._cache[topic] = response.json()["num_partitions"]
        return self._cache[topic]

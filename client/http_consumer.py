import httpx

from protocol.message import Message


class HttpConsumer:
    def __init__(self, broker_url: str, transport=None):
        self.client = httpx.Client(base_url=broker_url, transport=transport)

    def poll(self, topic: str, partition_id: int, limit: int = 10) -> list[Message]:
        response = self.client.get(f"/consume/{topic}/{partition_id}", params={"limit": limit})
        response.raise_for_status()
        res = response.json()
        return [
            Message(
                value=m["value"].encode("utf-8"),
                key=m["key"].encode("utf-8") if m["key"] else None,
                offset=m["offset"],
            )
            for m in res
        ]

    def commit(self, topic: str, partition_id: int) -> None:
        response = self.client.post(f"/commit/{topic}/{partition_id}")
        response.raise_for_status()

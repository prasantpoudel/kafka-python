import httpx


class HttpProducer:
    def __init__(self, broker_url: str, transport=None):
        self.client = httpx.Client(base_url=broker_url, transport=transport)

    def send(self, topic: str, value: bytes, key: bytes | None = None) -> tuple[int, int]:
        payload = {
            "value": value.decode("utf-8"),
            "key": key.decode("utf-8") if key else None,
        }
        response = self.client.post(f"/produce/{topic}", json=payload)
        response.raise_for_status()
        res = response.json()
        return res["offset"], res["partition_id"]

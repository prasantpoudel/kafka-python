import httpx


class MetadataPropagator:
    def __init__(self, broker_id: int):
        # This broker's own ID — skip self when propagating to peers.
        self.broker_id = broker_id

    def propagate(self, peers: list[dict], snapshot: dict) -> None:
        for peer in peers:
            if peer["id"] == self.broker_id:
                continue

            url = f"http://{peer['host']}:{peer['port']}/internal/metadata"
            try:
                httpx.post(url, json=snapshot, timeout=5.0)
            except httpx.RequestError:
                # Peer is temporarily down — skip and continue to others.
                pass

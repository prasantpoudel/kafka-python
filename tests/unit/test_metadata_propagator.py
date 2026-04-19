from unittest.mock import patch

import pytest

from broker.metadata.propagator import MetadataPropagator

PEERS = [
    {"id": 1, "host": "broker-1", "port": 9092},
    {"id": 2, "host": "broker-2", "port": 9092},
    {"id": 3, "host": "broker-3", "port": 9092},
]

SNAPSHOT = {"topics": {"orders": 3}, "partitions": {}}


@pytest.fixture
def propagator():
    return MetadataPropagator(broker_id=1)


def test_propagate_skips_self(propagator):
    with patch("broker.metadata.propagator.httpx.post") as mock_post:
        propagator.propagate(PEERS, SNAPSHOT)
        called_urls = [c.args[0] for c in mock_post.call_args_list]
        assert "http://broker-1:9092/internal/metadata" not in called_urls


def test_propagate_calls_all_peers(propagator):
    with patch("broker.metadata.propagator.httpx.post") as mock_post:
        propagator.propagate(PEERS, SNAPSHOT)
        called_urls = [c.args[0] for c in mock_post.call_args_list]
        assert "http://broker-2:9092/internal/metadata" in called_urls
        assert "http://broker-3:9092/internal/metadata" in called_urls


def test_propagate_sends_snapshot_as_json(propagator):
    with patch("broker.metadata.propagator.httpx.post") as mock_post:
        propagator.propagate(PEERS, SNAPSHOT)
        for c in mock_post.call_args_list:
            assert c.kwargs["json"] == SNAPSHOT


def test_propagate_continues_after_peer_failure(propagator):
    import httpx

    with patch("broker.metadata.propagator.httpx.post") as mock_post:
        # broker-2 is down, broker-3 should still receive the snapshot
        mock_post.side_effect = [httpx.RequestError("connection refused"), None]
        propagator.propagate(PEERS, SNAPSHOT)
        assert mock_post.call_count == 2


def test_propagate_empty_peers_does_nothing(propagator):
    with patch("broker.metadata.propagator.httpx.post") as mock_post:
        propagator.propagate([], SNAPSHOT)
        mock_post.assert_not_called()


def test_propagate_only_self_in_peers_does_nothing(propagator):
    with patch("broker.metadata.propagator.httpx.post") as mock_post:
        propagator.propagate([{"id": 1, "host": "broker-1", "port": 9092}], SNAPSHOT)
        mock_post.assert_not_called()

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies (uses uv)
uv sync

# Run a single broker locally (for development)
uv run python -m broker.server

# Run the full 3-broker cluster via Docker
docker compose -f docker/docker-compose.yml up --build

# Run all tests
uv run pytest

# Run a single test file
uv run pytest tests/unit/test_partition.py

# Run only integration tests
uv run pytest tests/integration/

# Control log verbosity
LOG_LEVEL=DEBUG uv run python -m broker.server
```

## Architecture

This is a Kafka-like messaging broker implemented in Python, designed to run as a Docker service. There is no external coordination service (no ZooKeeper) — metadata is managed internally by the brokers.

### Layer breakdown

```
client/          Producer and Consumer client library. Clients fetch partition/leader
                 metadata from any broker and cache it locally.

protocol/        Wire protocol — message framing, serialization, and codec.
                 All broker-to-broker and client-to-broker communication goes through here.

broker/
  server.py      Entry point. Starts the TCP listener and wires up all broker subsystems.
  controller.py  One broker in the cluster is elected controller. Responsible for
                 leader election, partition assignment, and broker membership.

  metadata/      Internal metadata layer (replaces ZooKeeper). Stores topic configs,
                 partition-to-broker mapping, leader info, and ISR lists.
                 store.py     — in-memory + persisted metadata state
                 propagator.py — pushes metadata changes from controller to peer brokers

  topic/         Topic and partition lifecycle management.
  storage/       Append-only disk log per partition.
                 log.py     — the partition log (write path + read by offset)
                 segment.py — rolling segment files within a partition log
                 index.py   — sparse offset index per segment for fast seeks

  replication/   Leader/follower replication.
                 leader.py   — accepts writes, tracks ISR, replicates to followers
                 follower.py — fetches from leader, applies to local log

  consumer_group/ Consumer group coordination.
                 coordinator.py — tracks group membership and committed offsets
                 rebalancer.py  — assigns partitions to consumers on join/leave

config/          broker.yaml and client.yaml — all tunable settings (ports, retention,
                 replication factor, producer acks, etc.)

docker/          Dockerfile and docker-compose.yml for running a 3-broker cluster.
                 Each broker gets BROKER_ID, BROKER_HOST, BROKER_PORT via env vars.
```

### Key design decisions

- **No external metadata service** — the controller broker manages all metadata internally and propagates changes to peers over the same broker network.
- **Append-only log** — each partition is stored as a series of rolling segment files on disk; reads are offset-based against an index.
- **Config via YAML + env vars** — `config/broker.yaml` holds defaults; Docker env vars (`BROKER_ID`, `BROKER_HOST`, `BROKER_PORT`) override per-instance values so the same image runs as any broker in the cluster.

## graphify

This project has a graphify knowledge graph at graphify-out/.

Rules:
- Before answering architecture or codebase questions, read graphify-out/GRAPH_REPORT.md for god nodes and community structure
- If graphify-out/wiki/index.md exists, navigate it instead of reading raw files
- After modifying code files in this session, run `graphify update .` to keep the graph current (AST-only, no API cost)

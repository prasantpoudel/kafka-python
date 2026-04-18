# Kafka-Python

A Python implementation of a Kafka-like distributed messaging service, built from scratch as a learning project to understand Kafka's internal architecture and low-level design (LLD).

---

## Features

### Core Messaging
- [ ] **Producer API** — publish messages to a named topic
- [ ] **Consumer API** — subscribe to a topic and read messages
- [ ] **Pub/Sub and Queue modes** — consumers in different groups each get all messages (pub/sub); consumers in the same group share messages (queue/competing consumers)

### Topics & Partitions
- [ ] **Topic management** — create, list, and delete topics
- [ ] **Partitioning** — each topic is split into N partitions for parallelism
- [ ] **Key-based partitioning** — messages with a key go to `hash(key) % num_partitions`; messages without a key are distributed via round-robin

### Offset & Delivery
- [ ] **Offset tracking** — each consumer tracks its read position (offset) per partition
- [ ] **Resume on failure** — consumers can restart and continue from the last committed offset
- [ ] **Offset commit strategies** — auto-commit on interval, or manual commit after processing
- [ ] **Delivery semantics** — at-least-once delivery (default); foundation for exactly-once

### Consumer Groups
- [ ] **Consumer group membership** — multiple consumers form a group identified by a group ID
- [ ] **Partition assignment** — partitions are divided among consumers in a group (each partition owned by one consumer)
- [ ] **Rebalancing** — reassign partitions when a consumer joins or leaves the group

### Broker & Storage
- [ ] **Message log** — append-only log file per partition (segments on disk)
- [ ] **Log retention** — delete or compact old messages based on time (e.g., 7 days) or disk size (e.g., 1 GB)
- [ ] **Log segmentation** — partition log is split into rolling segment files for efficient retention and indexing
- [ ] **Message index** — sparse offset index per segment for fast seeks

### Reliability
- [ ] **Replication** — each partition has a leader and N-1 follower replicas across brokers
- [ ] **Leader election** — if the leader goes down, a follower is promoted
- [ ] **In-sync replicas (ISR)** — producer can require acknowledgement from all ISR before a write is confirmed

### Producer Controls
- [ ] **Acknowledgement modes** — `acks=0` (fire and forget), `acks=1` (leader only), `acks=all` (full ISR)
- [ ] **Batching & compression** — buffer messages into batches; compress with gzip/snappy before sending
- [ ] **Idempotent producer** — deduplicate retried messages using a sequence number

### Broker Coordination
- [ ] **Multi-broker cluster** — multiple broker nodes, each owning a subset of partition leaders
- [ ] **Built-in metadata layer** — metadata (topic config, partition-to-broker mapping, leader info) is managed internally by the brokers themselves — no external service like ZooKeeper required
- [ ] **Controller broker** — one broker is elected as the cluster controller; it manages leader election, partition assignment, and broker membership using the internal metadata layer
- [ ] **Metadata propagation** — when metadata changes (new topic, leader change), the controller pushes updates to all other brokers so every node has a consistent view
- [ ] **Client metadata cache** — producers and consumers fetch and cache partition/leader info directly from any broker; they refresh automatically on leader change

---

## Architecture Overview

```
Producer ──► any Broker (metadata cache) ──► Leader Broker for Partition ──► Disk Log
                                                         │
                                                         ├──► Follower Replica 1
                                                         └──► Follower Replica 2

                    ┌─────────────────────────────────┐
                    │         Broker Cluster           │
                    │                                  │
                    │  Broker 1 (Controller)           │
                    │    └─ Internal Metadata Layer    │  ◄── no ZooKeeper, no external service
                    │         (topics, leaders, ISR)   │
                    │  Broker 2  ◄── metadata sync     │
                    │  Broker 3  ◄── metadata sync     │
                    └─────────────────────────────────┘

Consumer Group A (each consumer owns some partitions of a topic)
Consumer Group B (independent — gets its own copy of all messages)
```

---

## Learning Goals

- Understand how Kafka achieves high throughput via append-only logs and sequential I/O
- Understand partition-level parallelism and consumer group rebalancing
- Understand the trade-offs between delivery guarantees (at-most-once, at-least-once, exactly-once)
- Understand leader/follower replication and how ISR works

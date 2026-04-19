# Architecture

Flow diagrams for each component, showing responsibilities and interactions.

---

## Producer

**Job:** Take a message from the application and put it in the right partition.

```
app code                 Producer                  TopicManager          Partition
   │                        │                           │                    │
   │  send(topic, value)    │                           │                    │
   │ ─────────────────────► │                           │                    │
   │                        │  get_partition(topic,key) │                    │
   │                        │ ─────────────────────────►│                    │
   │                        │      Partition            │                    │
   │                        │ ◄─────────────────────────│                    │
   │                        │                           │  append(message)   │
   │                        │ ──────────────────────────────────────────────►│
   │                        │                           │     offset         │
   │                        │ ◄──────────────────────────────────────────────│
   │       offset           │                           │                    │
   │ ◄──────────────────────│                           │                    │
```

**Responsibilities:**
- Create a `Message` object from raw `value` and `key`
- Ask `TopicManager` for the right partition (key-based or round-robin)
- Append the message to that partition
- Return the offset back to the caller

**Not responsible for:**
- Choosing which partition — that is `TopicManager`
- Storing anything — that is `Partition`
- Encoding/decoding — caller passes `bytes`

---

## Consumer

**Job:** Read messages from a partition and track where it left off.

```
app code                Consumer              TopicManager           Partition
   │                       │                      │                      │
   │  poll(topic,          │                      │                      │
   │    partition_id)      │                      │                      │
   │ ─────────────────────►│                      │                      │
   │                       │  get_partitions(topic)                      │
   │                       │ ─────────────────────►                      │
   │                       │      [Partition]      │                      │
   │                       │ ◄─────────────────────                      │
   │                       │                       │  read(offset, limit) │
   │                       │ ────────────────────────────────────────────►│
   │                       │                       │    [Message, ...]    │
   │                       │ ◄────────────────────────────────────────────│
   │   [Message, ...]      │                      │                      │
   │ ◄─────────────────────│                      │                      │
   │                       │                      │                      │
   │  commit(topic,        │                      │                      │
   │    partition_id)      │                      │                      │
   │ ─────────────────────►│                      │                      │
   │                       │  save current offset │                      │
   │                       │  to _offsets         │                      │
```

**What the consumer owns internally:**
```python
# tracks read position per topic+partition
_offsets = {
    ("orders", 0): 5,   # read up to offset 5 on partition 0
    ("orders", 1): 12,  # read up to offset 12 on partition 1
}
```

**Responsibilities:**
- Track read position (offset) per topic+partition
- On `poll` — read from current offset, advance internal offset by messages received
- On `commit` — checkpoint the current offset so restarts resume from here

**Not responsible for:**
- Storing messages — that is `Partition`
- Partition assignment across a group — that is `ConsumerGroupCoordinator` (Phase 4)

**Why commit is separate from poll:**
The app might poll 10 messages, process 6, then crash. If commit happened
automatically on poll, those 4 unprocessed messages would be lost. With manual
commit, the app only commits after successful processing — guaranteeing
at-least-once delivery.

---

## Phase 2 — Network Layer

**Job:** Expose broker operations over HTTP so producers and consumers run in separate processes.

```
Phase 1 (in-process)              Phase 2 (over network)

  Producer                           Producer
     │                                  │
     │ direct call                      │ HTTP POST /produce
     ▼                                  ▼
  TopicManager               broker/server.py (FastAPI)
     │                                  │
     ▼                                  │ direct call (same as Phase 1)
  Partition                       TopicManager
                                        │
  Consumer                             ▼
     │                              Partition
     │ direct call
     ▼                           Consumer (HTTP GET /consume)
  TopicManager
```

**Endpoints:**
```
POST   /topics                        → create topic
GET    /topics                        → list topics
DELETE /topics/{name}                 → delete topic
POST   /produce/{topic}               → send message, returns offset
GET    /consume/{topic}/{partition_id} → poll messages, returns list
POST   /commit/{topic}/{partition_id}  → commit offset for a consumer group
```

**Why FastAPI over raw TCP:**
Real Kafka uses a custom binary TCP protocol for performance. HTTP is used here
because it is debuggable with curl, has auto-generated docs at /docs, and makes
it easy to see exact request/response shapes. Raw TCP comes in a later phase.

---

## MetadataClient

**Job:** Fetch topic/partition info from the broker and cache it locally so producers
and consumers don't make an HTTP call on every message.

```
HTTP Producer                MetadataClient              Broker HTTP API
     │                            │                            │
     │  get_partition_count(topic) │                            │
     │ ──────────────────────────►│                            │
     │                            │  GET /topics/{topic}       │
     │                            │ ──────────────────────────►│
     │                            │  {"num_partitions": 3}     │
     │                            │ ◄──────────────────────────│
     │                            │  cache locally             │
     │        3                   │                            │
     │ ◄──────────────────────────│                            │
     │                            │                            │
     │  get_partition_count(topic) │                            │
     │ ──────────────────────────►│                            │
     │                            │  return from cache         │
     │        3                   │  (no HTTP call made)       │
     │ ◄──────────────────────────│                            │
```

**Responsibilities:**
- Know the broker URL
- Fetch `GET /topics/{topic}` on first access, cache the result
- Expose `refresh(topic)` to force re-fetch when metadata becomes stale (e.g. leader change in Phase 4)

**Cache structure:**
```python
_cache = {
    "orders": 3,    # topic → num_partitions
    "payments": 5,
}
```

**Not responsible for:**
- Routing messages — that is `Producer`
- Leader tracking per partition — added in Phase 4

---

## Phase 3 — Persistence (Disk Log)

**Job:** Replace the in-memory message list with an append-only disk log so the broker survives restarts.

```
Partition                PartitionLog             Segment / OffsetIndex
   │                          │                          │
   │  append(message)         │                          │
   │ ────────────────────────►│                          │
   │                          │  assign offset           │
   │                          │  seg.append(message)     │
   │                          │ ────────────────────────►│
   │                          │  byte_pos                │
   │                          │ ◄────────────────────────│
   │                          │  idx.maybe_append(...)   │
   │                          │ ────────────────────────►│
   │         offset           │                          │
   │ ◄────────────────────────│                          │
   │                          │                          │
   │  read(offset, limit)     │                          │
   │ ────────────────────────►│                          │
   │                          │  _find_segment_index()   │
   │                          │  idx.lookup(offset)      │
   │                          │ ────────────────────────►│
   │                          │  byte_pos                │
   │                          │ ◄────────────────────────│
   │                          │  seg.iter(from_byte=pos) │
   │                          │ ────────────────────────►│
   │                          │  [Message, ...]          │
   │                          │ ◄────────────────────────│
   │   [Message, ...]         │                          │
   │ ◄────────────────────────│                          │
```

**Storage layout on disk:**
```
/tmp/kafka-logs/
  orders/
    0/                          ← partition 0
      00000000000000000000.log   ← segment file (binary records)
      00000000000000000000.index ← sparse offset index
      00000000000000001024.log   ← rolled segment (base offset = 1024)
      00000000000000001024.index
    1/                          ← partition 1
      ...
```

**Binary record format inside each `.log` file:**
```
[offset: 8B][timestamp: 8B][key_len: 4B][value_len: 4B][key bytes][value bytes]
```

**Responsibilities:**
- `Segment` — append binary records to a single file, iterate records from any byte position
- `OffsetIndex` — write one sparse entry every N messages; binary search to find the nearest byte position for a given offset
- `PartitionLog` — own multiple segments, assign offsets, roll segments when size limit is hit, recover `next_offset` on startup by scanning the last segment

**Why a sparse index instead of one entry per message:**
A full index (one entry per message) would be as large as the log itself. A sparse index trades a small sequential scan (at most N records) for a much smaller index file — the same tradeoff real Kafka makes.

**Why segment rolling:**
A single file that grows forever is slow to search and impossible to delete partially. Rolling into fixed-size segments means old data can be deleted by simply removing old segment files without touching newer ones (retention policy — Phase 5).

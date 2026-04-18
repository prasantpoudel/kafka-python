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

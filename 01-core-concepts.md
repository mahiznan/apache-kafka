# Stage 1: Core Concepts & Why Kafka Exists

---

## The Problem: Why Do We Need Kafka?

Imagine you have an **e-commerce system** with these services:

```
Order Service → Payment Service → Inventory Service → Notification Service → Analytics Service
```

### Without Kafka (Direct / Synchronous Calls)

```
┌──────────┐  HTTP ┌──────────┐  HTTP ┌───────────┐  HTTP ┌──────────────┐
│  Order   │──────→│ Payment  │──────→│ Inventory │──────→│ Notification │
│ Service  │       │ Service  │       │  Service  │       │   Service    │
└──────────┘       └──────────┘       └───────────┘       └──────────────┘
                                                                  │
                                                           HTTP   │
                                                                  ▼
                                                          ┌───────────┐
                                                          │ Analytics │
                                                          │  Service  │
                                                          └───────────┘
```

**Problems with this approach:**

| Problem | What Happens |
|---------|-------------|
| **Tight coupling** | Order Service must know about every downstream service. Add a new service? Change Order Service. |
| **Cascading failures** | Payment Service is down? The entire order flow fails — even though Inventory and Notification are fine. |
| **No buffering** | If Analytics is slow, it slows down the entire chain. Every service runs at the speed of the slowest one. |
| **Lost messages** | If Notification Service crashes mid-request, that message is gone forever. |
| **No replay** | Something went wrong in Analytics last Tuesday? You can't re-process those events — they're gone. |

---

### With Kafka (Event-Driven / Asynchronous)

```
                              ┌──────────────┐
                         ┌───→│ Payment Svc  │  (Consumer Group: payments)
                         │    └──────────────┘
                         │
┌──────────┐  write ┌────┴───────────────┐   ┌───────────┐
│  Order   │───────→│       KAFKA        │──→│ Inventory │  (Consumer Group: inventory)
│ Service  │        │  Topic: orders.*   │   └───────────┘
└──────────┘        └────┬───────────────┘
                         │    ┌──────────────┐
                         ├───→│ Notification │  (Consumer Group: notif)
                         │    └──────────────┘
                         │    ┌───────────┐
                         └───→│ Analytics │  (Consumer Group: analytics)
                              └───────────┘
```

**What changed:**

| Benefit | How |
|---------|-----|
| **Decoupled** | Order Service writes to Kafka and is done. It doesn't know or care who reads it. |
| **Fault tolerant** | Notification is down? Messages wait in Kafka. When it comes back, it picks up where it left off. |
| **Buffering** | Analytics is slow? It reads at its own pace. Others are unaffected. |
| **No data loss** | Messages are persisted to disk and replicated across brokers. |
| **Replayable** | Need to reprocess last week's orders? Reset the offset and read again. |

---

## What Is Apache Kafka?

> **Kafka is a distributed, log-based event streaming platform.**

- **Distributed** — runs as a cluster of multiple servers (brokers) for fault tolerance and scale
- **Log-based** — stores messages in an append-only, ordered, immutable log (not a queue that deletes on read)
- **Event streaming** — designed for continuous flow of events, not just point-to-point messaging

### Kafka vs Traditional Message Queues

| Feature | Traditional Queue (RabbitMQ/ActiveMQ) | Kafka |
|---------|---------------------------------------|-------|
| Message retention | Deleted after consumer acknowledges | Retained for a configurable period (days/weeks/forever) |
| Replay | Not possible — message is gone | Possible — reset offset and re-read |
| Consumer model | Push-based (broker pushes to consumer) | Pull-based (consumer polls from broker) |
| Ordering | Per-queue ordering | Per-partition ordering (more scalable) |
| Throughput | Thousands/sec | Millions/sec |
| Storage | In-memory primarily | Disk-based (sequential I/O — surprisingly fast) |
| Multiple consumers | Need to duplicate queues or use pub/sub exchanges | Multiple consumer groups read independently from same topic |

**Key insight:** A traditional queue is like a **pipe** — message goes in one end, comes out the other, and is gone. Kafka is like a **newspaper** — it's published once, multiple people can read it, and back issues are available.

---

## Key Terminology

### Cluster, Broker, Topic, Partition

```
┌─────────────────────────── CLUSTER ───────────────────────────┐
│                                                               │
│  ┌─  Broker 0 ──┐   ┌─ Broker 1  ──┐   ┌─ Broker 2  ──┐       │
│  │              │   │              │   │              │       │
│  │  Topic: orders                                     │       │
│  │  ┌─────────────────────────────────────────────┐   │       │
│  │  │ Partition 0  │ Partition 1  │ Partition 2   │   │       │
│  │  │ (Broker 0)   │ (Broker 1)   │ (Broker 2)    │   │       │
│  │  └─────────────────────────────────────────────┘   │       │
│  │              │   │              │   │              │       │
│  └──────────────┘   └──────────────┘   └──────────────┘       │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

### 1. Broker
A single Kafka server. A cluster has multiple brokers (typically 3+). Each broker:
- Stores a subset of partitions
- Handles read/write requests
- Replicates data to other brokers

### 2. Topic
A **named category** of messages. Like a database table, but append-only.
- Example: `order.created`, `payment.completed`, `user.signup`
- You write to a topic. You read from a topic.

### 3. Partition
A topic is split into **partitions** — this is how Kafka scales.

```
Topic: orders (3 partitions)

Partition 0:  [msg0] [msg3] [msg6] [msg9]  ...
Partition 1:  [msg1] [msg4] [msg7] [msg10] ...
Partition 2:  [msg2] [msg5] [msg8] [msg11] ...
```

- Each partition is an **ordered, immutable sequence** of messages
- Messages within a partition have a guaranteed order
- Messages across partitions have **no ordering guarantee**
- More partitions = more parallelism

### 4. Offset
A **sequential ID** for each message within a partition. Think of it as a line number.

```
Partition 0:
Offset:  0     1     2     3     4     5
       [msg] [msg] [msg] [msg] [msg] [msg]
                          ▲
                    Consumer is here
                    (committed offset = 3)
```

- Each consumer tracks its position via offsets
- "Replay" = move the offset backward
- "Skip ahead" = move the offset forward

### 5. Producer
An application that **writes** messages to a topic.

```
Producer → decides which partition → writes to leader broker
```

- If message has a **key**: `hash(key) % num_partitions` → always goes to same partition
- If message has **no key**: round-robin across partitions

### 6. Consumer
An application that **reads** messages from a topic.

- Consumers **pull** messages (not pushed by broker)
- Each consumer tracks its offset per partition
- Can read from the beginning, latest, or a specific offset

### 7. Consumer Group
A **set of consumers** that cooperatively read from a topic.

```
Topic: orders (3 partitions)

Consumer Group: "payment-service"
┌────────────┐  reads  Partition 0
│ Consumer A │────────→ [msg0] [msg3] [msg6]
└────────────┘
┌────────────┐  reads  Partition 1
│ Consumer B │────────→ [msg1] [msg4] [msg7]
└────────────┘
┌────────────┐  reads  Partition 2
│ Consumer C │────────→ [msg2] [msg5] [msg8]
└────────────┘
```

**Rule:** Each partition is read by **exactly one** consumer in a group. This guarantees no duplicate processing within a group.

### 8. Segment
Partitions are stored as **segment files** on disk:
- Active segment receives new writes
- Older segments are immutable and eligible for deletion/compaction

---

## ZooKeeper vs KRaft

### ZooKeeper (Legacy — pre Kafka 3.3)
- External service that managed broker metadata, leader election, topic configs
- Required running a separate ZooKeeper cluster (3-5 nodes)
- Added operational complexity

### KRaft (Current — Kafka 3.3+)
- **K**afka **R**aft — built-in consensus protocol
- No external dependency
- Metadata managed by a subset of brokers (controllers)
- Faster startup, simpler operations

```
Legacy:                           Current:
┌────────┐   ┌───────────┐      ┌────────────────┐
│ Broker │──→│ ZooKeeper │      │ Broker (KRaft) │
│        │   │ Cluster   │      │ Self-managed   │
└────────┘   └───────────┘      └────────────────┘
```

**We use KRaft mode** — it's the modern standard.

---

## How a Message Flows End-to-End

```
1. Producer                    2. Broker Cluster              3. Consumer
┌──────────┐                  ┌──────────────────┐           ┌──────────┐
│          │  send("orders",  │  Topic: orders   │  poll()   │          │
│ Order    │  key="user-42",  │  ┌────────────┐  │ ────────→ │ Payment  │
│ Service  │  value={...})    │  │ Partition 1│  │           │ Service  │
│          │ ───────────────→ │  │ offset=47  │  │           │          │
└──────────┘                  │  │ [new msg]  │  │           └──────────┘
                              │  └────────────┘  │
                              │  Replicated to   │
                              │  Broker 1 & 2    │
                              └──────────────────┘

Step by step:
1. Producer serializes the message (key + value)
2. Producer determines partition: hash("user-42") % 3 = 1
3. Message sent to Partition 1's leader broker
4. Leader writes to local log, replicates to followers
5. Leader acknowledges the write (based on acks setting)
6. Consumer polls Partition 1, receives the message at offset 47
7. Consumer processes the message and commits offset 47
```

---

## Quick Recap

| Concept | One-Liner |
|---------|-----------|
| **Kafka** | Distributed, log-based event streaming platform |
| **Broker** | A single server in the Kafka cluster |
| **Topic** | Named category of messages (like a DB table) |
| **Partition** | Ordered sub-stream of a topic (unit of parallelism) |
| **Offset** | Sequential position of a message in a partition |
| **Producer** | Writes messages to topics |
| **Consumer** | Reads messages from topics |
| **Consumer Group** | Set of consumers that share the work of reading a topic |
| **KRaft** | Built-in consensus replacing ZooKeeper |

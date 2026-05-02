# Apache Kafka — 2-Hour Learning Plan

> **Audience:** Developer new to message queues, familiar with Java
> **Goal:** Understand Kafka end-to-end, set up a cluster in Docker, write producers/consumers, and learn industry best practices
> **Approach:** Each stage has theory + hands-on exercises

---

## Stage 1: Core Concepts & Why Kafka Exists (15 min)

| Topic | Details |
|-------|---------|
| The problem | Tight coupling in synchronous service-to-service calls |
| What is a message queue | Decoupling producers from consumers via an intermediary |
| What is Kafka | A distributed, log-based event streaming platform |
| Key terminology | Broker, Topic, Partition, Offset, Producer, Consumer, Consumer Group, Segment |
| Log-based vs traditional queues | Kafka retains messages after consumption — replayable, durable, ordered |
| Architecture overview | Cluster → Brokers → Topics → Partitions → Segments |
| ZooKeeper vs KRaft | Legacy coordination (ZooKeeper) vs new built-in consensus (KRaft) |

**Outcome:** Understand why Kafka exists and how it differs from traditional message queues.

---

## Stage 2: Apache Kafka vs Confluent Kafka (10 min)

| Topic | Details |
|-------|---------|
| What is Apache Kafka | Open-source core — broker, producer, consumer, Kafka Streams, Kafka Connect |
| What is Confluent Platform | Commercial distribution built on top of Apache Kafka |
| Confluent additions | Schema Registry, ksqlDB, Control Center (UI), Tiered Storage, Cluster Linking |
| Confluent Cloud | Fully managed Kafka-as-a-service |
| When to use which | Open-source for learning/small workloads; Confluent for enterprise features, support, managed ops |
| Licensing | Apache Kafka = Apache 2.0; Confluent extras = Confluent Community / Commercial license |
| Docker images | `apache/kafka` (open-source) vs `confluentinc/cp-kafka` (Confluent) |

**Outcome:** Know the differences and make informed choices for your projects.

---

## Stage 3: Docker Setup & First Touch (15 min)

| Topic | Details |
|-------|---------|
| Docker Compose setup | Spin up a 3-broker Kafka cluster in KRaft mode (no ZooKeeper) |
| Confluent Docker setup | Side-by-side: same cluster using Confluent images + Schema Registry |
| CLI tools | `kafka-topics.sh`, `kafka-console-producer.sh`, `kafka-console-consumer.sh` |
| Create a topic | `--partitions 3 --replication-factor 3` |
| Produce & consume via CLI | End-to-end message flow from terminal |
| Inspect cluster | `kafka-metadata.sh`, `kafka-broker-api-versions.sh` |

**Hands-on deliverable:** A running 3-broker cluster with a test topic, messages produced and consumed.

---

## Stage 4: Java Producer & Consumer (20 min)

| Topic | Details |
|-------|---------|
| Maven/Gradle dependencies | `kafka-clients` library setup |
| `KafkaProducer` | Send messages with keys, callbacks, error handling |
| Serialization | `StringSerializer`, `JsonSerializer`, custom serializers |
| `KafkaConsumer` | Poll loop, `subscribe()`, deserialization |
| Key-based partitioning | Messages with same key always go to the same partition |
| Round-robin partitioning | Messages without keys are distributed across partitions |
| Graceful shutdown | `producer.close()`, `consumer.wakeup()` patterns |

**Hands-on deliverable:** A Java producer sending keyed JSON messages, a consumer reading and printing them.

---

## Stage 5: Consumer Groups & Multi-Consumer Patterns (15 min)

| Topic | Details |
|-------|---------|
| What is a Consumer Group | A set of consumers that cooperatively read from a topic |
| Partition assignment | Each partition is assigned to exactly one consumer in a group |
| Scaling consumers | Add consumers to a group → partitions rebalance automatically |
| Max parallelism | Number of consumers in a group ≤ number of partitions |
| Multiple consumer groups | Each group gets its own independent copy of all messages |
| Use case: same topic, different services | Service A (group-A) and Service B (group-B) both read the same topic independently |
| Rebalancing strategies | Range, RoundRobin, Sticky, CooperativeSticky |
| `group.instance.id` | Static membership to avoid unnecessary rebalances |

**Hands-on deliverable:** Run 2 consumers in the same group (observe partition split), then a 3rd consumer in a different group (observe full replay).

---

## Stage 6: Filtering Messages for Multiple Consumers (15 min)

> **Problem:** A topic has messages meant for different consumers. How does each consumer get only what it needs?

| Pattern | How It Works | When to Use |
|---------|-------------|-------------|
| **Separate topics** | Route messages to `orders.created`, `orders.shipped` etc. at producer side | Cleanest approach — use when event types are known upfront |
| **Header-based filtering** | Producer sets headers (`event-type: SHIPPED`); consumer reads headers and skips irrelevant messages | When topics can't be split; moderate message volume |
| **Key-based filtering** | Use message key for routing; consumer checks key prefix | When natural key already encodes the event type |
| **Client-side filtering** | Consumer reads all messages, applies `if/switch` logic, discards unwanted | Simplest but wasteful at high volume |
| **Kafka Streams filter** | `stream.filter(predicate)` writes matching records to a new topic | When you need a derived/filtered topic for downstream consumers |
| **ksqlDB (Confluent)** | `CREATE STREAM filtered AS SELECT * FROM source WHERE type = 'X'` | SQL-based filtering with Confluent Platform |

**Best practice:** Prefer **separate topics** or **header-based filtering**. Avoid reading everything and discarding — it wastes network and CPU at scale.

**Hands-on deliverable:** Produce messages with headers; write a consumer that filters by header value.

---

## Stage 7: Authentication — Kerberos / Keytabs & SASL (15 min)

| Topic | Details |
|-------|---------|
| Why authentication matters | Prevent unauthorized producers/consumers in shared or production clusters |
| Kafka auth mechanisms | SASL/PLAIN, SASL/SCRAM, SASL/GSSAPI (Kerberos), SASL/OAUTHBEARER, SSL/mTLS |
| What is Kerberos | Ticket-based authentication protocol; widely used in enterprise (Active Directory, Hadoop) |
| What is a keytab | A file containing Kerberos principal + encrypted key — allows passwordless auth |
| How Kafka uses keytabs | Broker and client each have a keytab; JAAS config points to it |
| JAAS configuration | `KafkaClient` and `KafkaServer` login contexts |
| Producer/consumer config | `security.protocol=SASL_SSL`, `sasl.mechanism=GSSAPI`, `sasl.kerberos.service.name=kafka` |
| ACLs | `kafka-acls.sh` — grant `READ`, `WRITE`, `DESCRIBE` per principal per topic |
| SASL/SCRAM (simpler alternative) | Username/password stored in ZooKeeper/KRaft — good for Docker/dev environments |
| Confluent RBAC | Role-Based Access Control — Confluent's enterprise layer over ACLs |

**Hands-on deliverable:** Configure SASL/SCRAM auth on Docker cluster; produce/consume with authenticated clients.

---

## Stage 8: Partitions, Offsets & Reliability (15 min)

| Topic | Details |
|-------|---------|
| Offsets deep dive | Committed vs current position, `auto.offset.reset` (`earliest` / `latest`) |
| Manual vs auto commit | `enable.auto.commit=false` + `commitSync()` / `commitAsync()` |
| Replayability | Reset consumer group offsets to re-process messages |
| Replication | `replication-factor`, ISR (In-Sync Replicas), leader election |
| Producer `acks` | `acks=0` (fire & forget), `acks=1` (leader only), `acks=all` (all ISR) |
| Idempotent producer | `enable.idempotence=true` — exactly-once at producer level |
| `min.insync.replicas` | Combined with `acks=all` to prevent data loss |
| Transactions | `initTransactions()`, `beginTransaction()`, `commitTransaction()` for exactly-once across topics |

**Hands-on deliverable:** Demonstrate offset reset, observe message replay. Test `acks=all` with `min.insync.replicas=2`.

---

## Stage 9: Industry Best Practices (10 min)

### Topic Design
| Practice | Recommendation |
|----------|---------------|
| Naming convention | `<domain>.<entity>.<event>` — e.g., `order.payment.completed` |
| Partition count | Start with `max(throughput_MB / partition_throughput, consumer_count)` — typically 6-12 for most workloads |
| Retention | Time-based (`retention.ms`) or size-based (`retention.bytes`); 7 days default is a good start |
| Compaction | Use `cleanup.policy=compact` for latest-state-per-key topics (e.g., user profiles) |

### Producer Best Practices
| Practice | Recommendation |
|----------|---------------|
| Always use `acks=all` in production | Prevents data loss |
| Enable idempotence | `enable.idempotence=true` (default in Kafka 3.x+) |
| Use compression | `compression.type=lz4` or `zstd` — saves network and disk |
| Set `delivery.timeout.ms` | Upper bound on total retry time |
| Use callbacks | Handle send failures asynchronously |

### Consumer Best Practices
| Practice | Recommendation |
|----------|---------------|
| Manual offset commit | Commit after processing, not before |
| Idempotent processing | Design consumers to handle duplicate messages safely |
| Use Dead Letter Queue (DLQ) | Route poison messages to a DLQ topic instead of crashing |
| Monitor consumer lag | Alert when lag grows — means consumers can't keep up |
| Graceful shutdown | `consumer.wakeup()` + shutdown hook |

### Operational Best Practices
| Practice | Recommendation |
|----------|---------------|
| Schema Registry | Enforce schemas (Avro/Protobuf) — prevents breaking changes |
| Monitoring | Track: consumer lag, under-replicated partitions, request latency, disk usage |
| Security | SASL + SSL in production; ACLs on all topics; no anonymous access |
| Cluster sizing | Minimum 3 brokers; replication factor = 3; `min.insync.replicas = 2` |
| Topic governance | Centralized topic creation; no auto-creation in production (`auto.create.topics.enable=false`) |

---

## Stage 10: What's Next (5 min)

| Topic | Details |
|-------|---------|
| Kafka Streams | Lightweight stream processing library built into Kafka |
| Kafka Connect | Pre-built connectors for databases, S3, Elasticsearch, etc. |
| ksqlDB (Confluent) | SQL interface for stream processing |
| Event sourcing & CQRS | Architectural patterns powered by Kafka |
| Kafka in Kubernetes | Strimzi operator, Confluent for Kubernetes |
| Resources | [Apache Kafka Docs](https://kafka.apache.org/documentation/), [Confluent Developer](https://developer.confluent.io/), *Designing Data-Intensive Applications* (Kleppmann) |

---

## Summary Timeline

| Time | Stage | Focus |
|------|-------|-------|
| 0:00 – 0:15 | Stage 1 | Core concepts |
| 0:15 – 0:25 | Stage 2 | Apache vs Confluent Kafka |
| 0:25 – 0:40 | Stage 3 | Docker setup & CLI |
| 0:40 – 1:00 | Stage 4 | Java producer & consumer |
| 1:00 – 1:15 | Stage 5 | Consumer groups |
| 1:15 – 1:30 | Stage 6 | Filtering messages |
| 1:30 – 1:45 | Stage 7 | Authentication & keytabs |
| 1:45 – 2:00 | Stage 8 | Offsets & reliability |
| — | Stage 9 | Best practices (reference) |
| — | Stage 10 | What's next (reference) |

> **Note:** Stages 9 and 10 are reference material to revisit as needed. Each stage will have its own dedicated document created during hands-on sessions.

# Glossary

Key terms used throughout Surgewave documentation.

## Core Concepts

### Broker
The Surgewave server process that stores messages, handles client connections, and participates in cluster coordination. A single broker can handle thousands of topics and millions of messages per second.

### Topic
A named feed of messages. Topics are the primary way to organize messages in Surgewave. Each topic can have multiple partitions for parallel processing.

### Partition
A subset of a topic's messages. Each partition is an ordered, immutable sequence of messages. Partitions enable parallel processing and horizontal scaling.

### Message / Record
A single unit of data in Surgewave. Contains:
- **Key** (optional) - Used for partitioning and compaction
- **Value** - The message payload
- **Headers** (optional) - Metadata key-value pairs
- **Timestamp** - When the message was produced
- **Offset** - Position in the partition

### Offset
A unique identifier for a message within a partition. Offsets are sequential integers starting from 0. Consumers track their position using offsets.

### Producer
A client that publishes messages to topics. Producers are responsible for serialization and partition selection.

### Consumer
A client that reads messages from topics. Consumers track their position (offset) and can participate in consumer groups.

### Consumer Group
A set of consumers that cooperatively consume from topics. Each partition is assigned to exactly one consumer in the group, enabling parallel processing while ensuring each message is processed once.

## Cluster Concepts

### Cluster
Multiple Surgewave brokers working together. Clusters provide:
- **Replication** - Data redundancy across nodes
- **Failover** - Automatic recovery from node failures
- **Scalability** - Distributed load across nodes

### KRaft
Kafka Raft - The consensus protocol used for cluster coordination. KRaft eliminates the need for ZooKeeper by embedding the metadata store within the brokers.

### Controller
The broker elected to manage cluster metadata:
- Topic/partition creation
- Replica assignment
- Leader election

### Leader
The broker responsible for a partition's reads and writes. Each partition has exactly one leader at a time.

### Follower
A broker that replicates a partition from its leader. Followers are ready to become leader if the current leader fails.

### ISR (In-Sync Replicas)
The set of replicas (leader + followers) that are fully caught up with the leader. Messages are considered committed only when written to all ISR members.

### Replication Factor
The number of copies of each partition across the cluster. A replication factor of 3 means data is stored on 3 different brokers.

## Consumer Group Concepts

### Rebalancing
The process of redistributing partition assignments among consumers when:
- A consumer joins or leaves the group
- Topics/partitions are added
- A consumer fails health checks

### Heartbeat
Periodic signal sent by consumers to the broker to indicate they're alive. If no heartbeat is received within `SessionTimeoutMs`, the consumer is considered dead.

### Session Timeout
Maximum time between heartbeats before a consumer is considered failed and a rebalance is triggered.

### Auto-Commit
Automatic periodic offset commits. When enabled, the consumer automatically commits offsets at `AutoCommitIntervalMs` intervals.

### Manual Commit
Explicit offset commits by the application. Provides more control over when messages are considered processed.

## Storage Concepts

### Log
The append-only storage structure for partition data. Messages are written sequentially and never modified.

### Segment
A chunk of the partition log. Segments are rotated based on size or time, enabling efficient retention management.

### Retention
How long messages are kept before deletion:
- **Time-based** - Delete after X hours/days
- **Size-based** - Delete when partition exceeds X bytes

### Compaction
Retention policy that keeps only the latest message for each key. Useful for maintaining current state (e.g., user profiles, configuration).

### High Watermark
The offset of the last message replicated to all ISR members. Consumers only read up to the high watermark.

## Protocol Concepts

### Kafka Protocol
The wire protocol for Kafka client compatibility. Enables existing Kafka clients to work with Surgewave unchanged.

### Native Protocol
Surgewave's optimized binary protocol. Provides lower latency (lower latency than Kafka wire) for .NET clients.

### gRPC
Google's RPC protocol. Provides cross-platform streaming support with Protocol Buffers serialization.

### Shared Memory
IPC transport for same-machine communication. Provides sub-microsecond latency for co-located services.

## Serialization

### Serializer
Converts objects to bytes for transmission. Built-in serializers: String, JSON, Int32, Int64, ByteArray.

### Deserializer
Converts bytes back to objects. Must match the serializer used by the producer.

### Serde
Combined serializer/deserializer. Used in Kafka Streams for state stores.

### Schema Registry
Service that stores and validates message schemas. Supports Avro, JSON Schema, Protobuf, and FlatBuffers.

## Delivery Semantics

### At-Most-Once
Messages may be lost but never duplicated. Achieved by committing offsets before processing.

### At-Least-Once
Messages are never lost but may be duplicated. Achieved by committing offsets after processing.

### Exactly-Once
Messages are processed exactly once. Achieved using transactions or idempotent producers with transactional consumers.

### Idempotent Producer
Producer that ensures each message is written exactly once, even with retries. Prevents duplicates from network issues.

## Transactions

### Transaction
A group of messages that are either all committed or all aborted. Enables atomic writes across multiple topics/partitions.

### Transactional ID
Unique identifier for a transactional producer. Enables transaction recovery across producer restarts.

### Transaction Coordinator
Broker responsible for managing transactions for a transactional producer.

### Isolation Level
Controls which messages a consumer sees:
- **ReadUncommitted** - See all messages (including uncommitted)
- **ReadCommitted** - Only see committed messages

## Performance Terms

### Throughput
Messages or bytes processed per second. Measured separately for producers and consumers.

### Latency
Time from message production to consumption. Key percentiles: P50 (median), P99 (99th percentile).

### Batching
Grouping multiple messages into a single request. Improves throughput at the cost of latency.

### Linger Time
How long the producer waits for batch to fill before sending. Higher values improve batching but increase latency.

### Compression
Reducing message size before transmission. Options: None, Gzip, Snappy, LZ4, Zstd.

## See Also

- [Architecture](setup/architecture.md) - System design overview
- [Configuration](setup/configuration.md) - All configuration options
- [Troubleshooting](operations/troubleshooting.md) - Common issues and solutions

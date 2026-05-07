# Tutorial 07: Clustering

Set up a multi-broker Surgewave cluster with replication and automatic failover.

## Prerequisites

- .NET 10 SDK installed
- Surgewave broker binary or Docker available
- Familiarity with topics and partitions from earlier tutorials

## What You Will Build

A 3-node Surgewave cluster that:
1. Replicates data across brokers for durability
2. Automatically elects new leaders on failure
3. Continues operating when one broker goes down

## Step 1: Configure Three Brokers

Each broker needs a unique `BrokerId` and must know about all cluster members.

### Broker 1

Create `broker1/appsettings.json`:

```json
{
  "Surgewave": {
    "BrokerId": 1,
    "Host": "localhost",
    "Port": 9092,
    "GrpcPort": 9093,
    "DataDirectory": "./data/broker1",
    "ClusterNodes": "localhost:9092,localhost:9094,localhost:9096",
    "ClusterId": "surgewave-tutorial-cluster",
    "UseRaftConsensus": true,
    "ReplicationPort": 10092,
    "DefaultReplicationFactor": 3,
    "MinInSyncReplicas": 2
  }
}
```

### Broker 2

Create `broker2/appsettings.json`:

```json
{
  "Surgewave": {
    "BrokerId": 2,
    "Host": "localhost",
    "Port": 9094,
    "GrpcPort": 9095,
    "DataDirectory": "./data/broker2",
    "ClusterNodes": "localhost:9092,localhost:9094,localhost:9096",
    "ClusterId": "surgewave-tutorial-cluster",
    "UseRaftConsensus": true,
    "ReplicationPort": 10094,
    "DefaultReplicationFactor": 3,
    "MinInSyncReplicas": 2
  }
}
```

### Broker 3

Create `broker3/appsettings.json`:

```json
{
  "Surgewave": {
    "BrokerId": 3,
    "Host": "localhost",
    "Port": 9096,
    "GrpcPort": 9097,
    "DataDirectory": "./data/broker3",
    "ClusterNodes": "localhost:9092,localhost:9094,localhost:9096",
    "ClusterId": "surgewave-tutorial-cluster",
    "UseRaftConsensus": true,
    "ReplicationPort": 10096,
    "DefaultReplicationFactor": 3,
    "MinInSyncReplicas": 2
  }
}
```

## Step 2: Start the Cluster

Open three terminals and start each broker:

**Terminal 1:**

```bash
dotnet run --project src/Kuestenlogik.Surgewave.Broker -- --config broker1/appsettings.json
```

**Terminal 2:**

```bash
dotnet run --project src/Kuestenlogik.Surgewave.Broker -- --config broker2/appsettings.json
```

**Terminal 3:**

```bash
dotnet run --project src/Kuestenlogik.Surgewave.Broker -- --config broker3/appsettings.json
```

### Using Docker Compose

Alternatively, create a `docker-compose.yml`:

```yaml
services:
  broker1:
    image: kuestenlogik/surgewave
    ports:
      - "9092:9092"
      - "9093:9093"
    environment:
      Surgewave__BrokerId: 1
      Surgewave__Port: 9092
      Surgewave__ClusterNodes: broker1:9092,broker2:9092,broker3:9092
      Surgewave__UseRaftConsensus: true
      Surgewave__DefaultReplicationFactor: 3
      Surgewave__MinInSyncReplicas: 2

  broker2:
    image: kuestenlogik/surgewave
    ports:
      - "9094:9092"
      - "9095:9093"
    environment:
      Surgewave__BrokerId: 2
      Surgewave__Port: 9092
      Surgewave__ClusterNodes: broker1:9092,broker2:9092,broker3:9092
      Surgewave__UseRaftConsensus: true

  broker3:
    image: kuestenlogik/surgewave
    ports:
      - "9096:9092"
      - "9097:9093"
    environment:
      Surgewave__BrokerId: 3
      Surgewave__Port: 9092
      Surgewave__ClusterNodes: broker1:9092,broker2:9092,broker3:9092
      Surgewave__UseRaftConsensus: true
```

```bash
docker compose up -d
```

## Step 3: Verify the Cluster

```bash
surgewave cluster status --bootstrap-server localhost:9092
```

Expected output:

```
Cluster: surgewave-tutorial-cluster
Controller: Broker 1
Brokers: 3
  Broker 1: localhost:9092 (controller)
  Broker 2: localhost:9094
  Broker 3: localhost:9096
```

## Step 4: Create a Replicated Topic

```bash
surgewave topics create events \
    --partitions 6 \
    --replication-factor 3 \
    --bootstrap-server localhost:9092
```

Verify the topic:

```bash
surgewave topics describe events --bootstrap-server localhost:9092
```

Expected output:

```
Topic: events
  Partition 0: Leader=1, Replicas=[1,2,3], ISR=[1,2,3]
  Partition 1: Leader=2, Replicas=[2,3,1], ISR=[2,3,1]
  Partition 2: Leader=3, Replicas=[3,1,2], ISR=[3,1,2]
  Partition 3: Leader=1, Replicas=[1,3,2], ISR=[1,3,2]
  Partition 4: Leader=2, Replicas=[2,1,3], ISR=[2,1,3]
  Partition 5: Leader=3, Replicas=[3,2,1], ISR=[3,2,1]
```

Partitions are spread evenly, each with 3 replicas.

## Step 5: Produce and Consume

### Producer

Connect to any broker in the cluster:

```csharp
await using var producer = new SurgewaveProducer<string, string>(options =>
{
    options.BootstrapServers = "localhost:9092,localhost:9094,localhost:9096";
    options.Acks = Acks.All; // Wait for all ISR replicas
});

for (var i = 0; i < 100; i++)
{
    await producer.ProduceAsync("events", $"key-{i}", $"Event #{i}");
}

await producer.FlushAsync();
Console.WriteLine("Produced 100 events across the cluster.");
```

### Consumer

```csharp
await using var consumer = new SurgewaveConsumer<string, string>(options =>
{
    options.BootstrapServers = "localhost:9092,localhost:9094,localhost:9096";
    options.GroupId = "cluster-consumer";
    options.AutoOffsetReset = AutoOffsetReset.Earliest;
});

consumer.Subscribe("events");

while (!ct.IsCancellationRequested)
{
    var result = await consumer.ConsumeAsync(ct);
    if (result != null)
        Console.WriteLine($"[Broker {result.Partition}] {result.Value}");
}
```

## Step 6: Test Failover

### Simulate a Broker Failure

1. Stop Broker 3 (Ctrl+C in its terminal or `docker compose stop broker3`).
2. Check the cluster status:

```bash
surgewave cluster status --bootstrap-server localhost:9092
```

Expected output:

```
Cluster: surgewave-tutorial-cluster
Controller: Broker 1
Brokers: 2 (1 offline)
  Broker 1: localhost:9092 (controller)
  Broker 2: localhost:9094
  Broker 3: OFFLINE
```

3. Verify topic health:

```bash
surgewave topics describe events --bootstrap-server localhost:9092
```

Partitions that had Broker 3 as leader automatically elected a new leader. The ISR shrinks to exclude the offline broker.

4. Produce and consume normally -- the cluster continues operating:

```bash
surgewave produce events --value "Message during outage" --bootstrap-server localhost:9092
surgewave consume events --offset latest --max-messages 1 --bootstrap-server localhost:9092
```

### Recover the Broker

Restart Broker 3. It will:
1. Rejoin the cluster
2. Catch up on missed data from leaders
3. Re-enter the ISR once fully caught up

```bash
surgewave cluster nodes --bootstrap-server localhost:9092
```

All three brokers should be back online with full ISR membership.

## Key Settings Reference

| Setting | Default | Description |
|---------|---------|-------------|
| `ClusterNodes` | "" | All broker endpoints, comma-separated |
| `ClusterId` | "surgewave-cluster" | Unique cluster identifier |
| `UseRaftConsensus` | false | Enable KRaft consensus protocol |
| `ReplicationPort` | 10092 | Port for inter-broker replication traffic |
| `DefaultReplicationFactor` | 1 | Default replication for new topics |
| `MinInSyncReplicas` | 1 | Minimum ISR count required for writes |
| `ReplicaLagTimeMaxMs` | 10000 | Max lag before removing from ISR |
| `ReplicaLagMaxMessages` | 4000 | Max message lag before removing from ISR |

## Production Recommendations

| Cluster Size | Tolerates | Recommended `MinInSyncReplicas` |
|:---:|:---:|:---:|
| 3 brokers | 1 failure | 2 |
| 5 brokers | 2 failures | 3 |
| 7 brokers | 3 failures | 4 |

Always use `Acks.All` on producers and `MinInSyncReplicas >= 2` for data-critical topics.

## Next Steps

- [Tutorial 08: AI Agents](08-ai-agents.md) -- build AI-powered pipelines
- [Replication Reference](../clustering/replication.md) -- deep dive on replication mechanics
- [KRaft Consensus](../clustering/raft.md) -- how leader election works
- [Failover](../clustering/failover.md) -- automatic failure recovery

# Failover

Automatic failure detection and recovery.

## Leader Election

When partition leader fails:

1. **Detection** - Controller detects broker failure
2. **Selection** - Choose new leader from ISR
3. **Notification** - Update metadata, notify clients
4. **Recovery** - Clients connect to new leader

```
Before: Topic-0 Leader = Broker1 (ISR: 1,2,3)
        ↓ Broker1 fails
After:  Topic-0 Leader = Broker2 (ISR: 2,3)
```

## Detection

### Heartbeat Monitoring

```json
{
  "Surgewave": {
    "HeartbeatIntervalMs": 3000,
    "HeartbeatTimeoutMs": 10000,
    "MaxHeartbeatFailures": 3
  }
}
```

Broker marked failed after:
- `HeartbeatTimeoutMs` without heartbeat, OR
- `MaxHeartbeatFailures` consecutive failures

### Session Timeout

Consumer groups use session timeout:

```csharp
var consumer = new SurgewaveConsumer<string, string>(options =>
{
    options.SessionTimeoutMs = 10000;
    options.HeartbeatIntervalMs = 3000;
});
```

## Leader Election Types

### Preferred Leader

Elect the preferred (first) replica:

```bash
surgewave partitions elect-leader --topic my-topic
surgewave partitions elect-leader --all
```

### When the last in-sync replica fails

A partition whose leader fails is led by another in-sync replica. When the failed broker was the only in-sync
replica, no other replica is known to hold everything the partition acknowledged, and the controller elects none:
the partition has no leader, the failed broker stays in its ISR, and clients are answered `LEADER_NOT_AVAILABLE`
until that broker is back. It is then elected again, with nothing lost. This is what Kafka does.

### Unclean Leader

An unclean election lets a live replica that is not in sync lead such a partition. What the in-sync replica had
acknowledged and this replica had not fetched is lost, and the new leader becomes the whole ISR. It happens only
where it is asked for:

- for a topic, with the topic config `unclean.leader.election.enable=true` (default `false`): the controller then
  elects a live replica by itself when the last in-sync replica fails;
- for single partitions, with an `ElectLeaders` request of the unclean election type:

```bash
kafka-leader-election.sh --bootstrap-server localhost:9092 \
  --election-type unclean --topic my-topic --partition 0
```

Only use when availability > consistency.

## Graceful Shutdown

Controller coordinates shutdown:

1. Broker sends ControlledShutdown request
2. Controller moves leaders away
3. Broker leaves cluster cleanly

```bash
# Broker initiates graceful shutdown
SIGTERM to broker process
```

Configuration:

```json
{
  "Surgewave": {
    "ShutdownTimeoutSeconds": 30
  }
}
```

## Client Handling

### Producer Failover

```csharp
var producer = new SurgewaveProducer<string, string>(options =>
{
    options.Retries = 3;
    options.RetryBackoffMs = 100;
});

try
{
    await producer.ProduceAsync("topic", "key", "value");
}
catch (NotLeaderException)
{
    // Retry will redirect to new leader
}
```

### Consumer Failover

Consumer group automatically rebalances:

```
Before: Consumer1 → Partitions 0,1
        Consumer2 → Partitions 2,3
        ↓ Consumer1 fails
After:  Consumer2 → Partitions 0,1,2,3
```

## Controller Failover

When controller fails:

1. KRaft triggers new election
2. New controller elected by majority
3. Metadata recovered from log
4. Brokers register with new controller

```
Controller (Broker1) fails
    ↓
Election: Broker2 wins
    ↓
Broker2 becomes controller
```

## Recovery Scenarios

### Single Broker Failure

| Condition | Action |
|-----------|--------|
| ISR > min.insync | Leader elected from ISR |
| ISR = min.insync | Writes blocked if leader fails |
| ISR < min.insync | Writes already blocked |

### Minority Partition

Network partition isolating minority:

```
[Broker1] | [Broker2, Broker3]
   ↓           ↓
 Isolated    Majority continues
 (read-only)
```

### Full Cluster Restart

1. Start brokers in any order
2. Wait for quorum (majority)
3. Controller elected
4. Metadata recovered
5. Leaders assigned

## Monitoring

| Metric | Description |
|--------|-------------|
| `surgewave_offline_partitions` | Partitions without leader |
| `surgewave_under_replicated_partitions` | Under-replicated count |
| `surgewave_active_controller` | Controller broker ID |
| `surgewave_leader_elections_total` | Total leader elections |

## CLI Commands

```bash
# Check cluster health
surgewave cluster status

# Check for offline partitions
surgewave topics describe my-topic

# Force leader election
surgewave partitions elect-leader --topic my-topic --partition 0
```

## Best Practices

1. **3+ brokers** - Tolerate single failure
2. **min.insync.replicas = 2** - Require majority for writes
3. **Monitor offline partitions** - Alert immediately
4. **Test failover** - Regular chaos testing
5. **Graceful shutdown** - Use SIGTERM, not SIGKILL

## Next Steps

- [Security](../security/index.md) - Authentication and authorization
- [Monitoring](../monitoring/index.md) - Observability

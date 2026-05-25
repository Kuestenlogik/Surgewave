# Troubleshooting Guide

Common issues and their solutions when running Surgewave.

## Connection Issues

### Cannot Connect to Broker

**Symptoms:**
- `BrokerConnectionException: Failed to connect to broker`
- Connection timeout errors
- `SocketException: Connection refused`

**Solutions:**

1. **Verify broker is running:**
   ```bash
   surgewave health
   # or check process
   ps aux | grep surgewave-broker
   ```

2. **Check port availability:**
   ```bash
   netstat -an | grep 9092
   ```

3. **Verify firewall rules:**
   ```bash
   # Linux
   sudo ufw status
   # Windows
   netsh advfirewall firewall show rule name=all | findstr 9092
   ```

4. **Check bootstrap servers configuration:**
   ```csharp
   // Ensure correct format
   options.BootstrapServers = "localhost:9092";  // Not "http://localhost:9092"
   ```

### Connection Drops / Disconnects

**Symptoms:**
- `Disconnected` event fires unexpectedly
- Intermittent `IOException` errors

**Solutions:**

1. **Enable auto-reconnect (default):**
   ```csharp
   options.EnableAutoReconnect = true;
   options.MaxReconnectAttempts = 10;
   options.ReconnectBackoffMs = 100;
   ```

2. **Check network stability:**
   ```bash
   ping localhost
   surgewave health --verbose
   ```

3. **Increase timeouts:**
   ```csharp
   options.RequestTimeoutMs = 60000;
   options.SessionTimeoutMs = 30000;
   ```

## Producer Issues

### Messages Not Being Delivered

**Symptoms:**
- `ProduceAsync` hangs or times out
- No messages appearing in topic

**Solutions:**

1. **Check topic exists:**
   ```bash
   surgewave topics list
   surgewave topics create my-topic --partitions 3
   ```

2. **Verify serialization:**
   ```csharp
   // Ensure serializer matches data type
   options.ValueSerializer = Serializers.Json<Order>();
   ```

3. **Flush pending messages:**
   ```csharp
   await producer.FlushAsync();
   ```

### MessageTooLarge Error

**Symptoms:**
- `SurgewaveException` with `MessageTooLarge` error code

**Solutions:**

1. **Check message size against broker limit:**
   ```bash
   surgewave config get message.max.bytes
   ```

2. **Enable compression:**
   ```csharp
   // Use compression for large messages
   .UsePreset(SendPreset.HighCompression)
   ```

3. **Split large messages:**
   ```csharp
   // Chunk large payloads
   var chunks = SplitIntoChunks(largePayload, maxSize: 900_000);
   ```

## Consumer Issues

### Consumer Not Receiving Messages

**Symptoms:**
- `ConsumeAsync` returns null repeatedly
- Consumer appears stuck

**Solutions:**

1. **Verify subscription:**
   ```csharp
   // Must subscribe before consuming
   consumer.Subscribe("my-topic");
   // or
   await consumer.SubscribeAsync(cancellationToken, "my-topic");
   ```

2. **Check offset reset policy:**
   ```csharp
   // Start from beginning if no committed offset
   options.AutoOffsetReset = AutoOffsetReset.Earliest;
   ```

3. **Check for paused partitions:**
   ```csharp
   var paused = consumer.PausedPartitions;
   if (paused.Count > 0)
       consumer.Resume(paused.ToArray());
   ```

4. **Verify messages exist:**
   ```bash
   surgewave consume my-topic --from-beginning --max-messages 1
   ```

### Consumer Lag Growing

**Symptoms:**
- `GetLagAsync` returns increasing values
- Processing falling behind

**Solutions:**

1. **Check consumer throughput:**
   ```csharp
   var lag = await consumer.GetAllLagAsync();
   foreach (var (tp, lagValue) in lag)
       Console.WriteLine($"{tp}: {lagValue} behind");
   ```

2. **Increase parallelism:**
   ```csharp
   // Process messages concurrently
   var tasks = messages.Select(ProcessAsync);
   await Task.WhenAll(tasks);
   ```

3. **Batch commits:**
   ```csharp
   // Don't commit every message
   if (processedCount % 100 == 0)
       await consumer.CommitAsync();
   ```

4. **Scale consumers:**
   - Add more consumers to the group (up to partition count)

### Rebalancing Too Frequently

**Symptoms:**
- Frequent `PartitionsRevoked` / `PartitionsAssigned` events
- Processing interruptions

**Solutions:**

1. **Increase session timeout:**
   ```csharp
   options.SessionTimeoutMs = 45000;  // Default 30000
   options.HeartbeatIntervalMs = 10000;  // Default 3000
   ```

2. **Reduce processing time per message:**
   ```csharp
   options.MaxPollIntervalMs = 600000;  // 10 minutes
   ```

3. **Handle rebalance gracefully:**
   ```csharp
   consumer.PartitionsRevoked += async (s, e) =>
   {
       await consumer.CommitAsync();  // Commit before losing partitions
   };
   ```

## Consumer Group Issues

### UnknownMemberId Error

**Symptoms:**
- `ProtocolException` with `UnknownMemberId`
- Consumer kicked from group

**Solutions:**

1. **Rejoin the group:**
   ```csharp
   await consumer.SubscribeAsync(cancellationToken, topics);
   ```

2. **Ensure timely polling:**
   ```csharp
   // Don't let MaxPollIntervalMs expire
   while (!ct.IsCancellationRequested)
   {
       var result = await consumer.ConsumeAsync(ct);
       // Process quickly or in background
   }
   ```

### GroupCoordinatorNotAvailable

**Symptoms:**
- Cannot join consumer group
- Group operations fail

**Solutions:**

1. **Check broker health:**
   ```bash
   surgewave health
   surgewave cluster status
   ```

2. **Retry with backoff:**
   ```csharp
   // Built-in auto-reconnect handles this
   options.EnableAutoReconnect = true;
   ```

## Storage Issues

### Disk Space Running Low

**Symptoms:**
- Write failures
- Broker performance degradation

**Solutions:**

1. **Check disk usage:**
   ```bash
   surgewave storage status
   df -h /var/surgewave/data
   ```

2. **Reduce retention:**
   ```bash
   surgewave topics alter my-topic --retention-ms 86400000  # 1 day
   ```

3. **Enable log compaction:**
   ```bash
   surgewave topics alter my-topic --cleanup-policy compact
   ```

4. **Use tiered storage:**
   ```json
   {
     "Surgewave": {
       "Storage": {
         "Mode": "Tiered",
         "TieredStorage": {
           "HotRetentionMs": 86400000,
           "RemoteStorage": "s3://my-bucket/surgewave"
         }
       }
     }
   }
   ```

### Slow Read/Write Performance

**Symptoms:**
- High latency on produce/consume
- Broker CPU/IO saturation

**Solutions:**

1. **Check storage mode:**
   ```bash
   surgewave config get storage.mode
   ```

2. **Use faster storage backend:**
   ```json
   {
     "Surgewave": {
       "Storage": {
         "Mode": "ZeroCopyWal"  // Faster than FileSystem
       }
     }
   }
   ```

3. **Tune OS settings:**
   ```bash
   # Increase file descriptors
   ulimit -n 65535

   # Tune vm settings
   sysctl -w vm.swappiness=1
   ```

## Cluster Issues

### Leader Election Failing

**Symptoms:**
- Partitions without leaders
- `NotLeaderForPartition` errors

**Solutions:**

1. **Check cluster health:**
   ```bash
   surgewave cluster status
   surgewave cluster nodes
   ```

2. **Check quorum:**
   - Ensure majority of nodes are healthy
   - For 3-node cluster, at least 2 must be up

3. **Force leader election:**
   ```bash
   surgewave cluster elect-leaders --all
   ```

### Replication Falling Behind

**Symptoms:**
- ISR (In-Sync Replicas) shrinking
- Follower lag increasing

**Solutions:**

1. **Check replica status:**
   ```bash
   surgewave topics describe my-topic --show-replicas
   ```

2. **Increase replication bandwidth:**
   ```json
   {
     "Surgewave": {
       "Clustering": {
         "ReplicationFetchMaxBytes": 10485760
       }
     }
   }
   ```

## Diagnostic Commands

```bash
# Overall health
surgewave health --verbose

# Topic details
surgewave topics describe my-topic

# Consumer group status
surgewave groups describe my-group

# Broker metrics
surgewave metrics --category broker

# Recent errors
surgewave logs --level error --since 1h

# Cluster status
surgewave cluster status
```

## Getting Help

If issues persist:

1. **Collect diagnostics:**
   ```bash
   surgewave diagnose --output diagnostics.zip
   ```

2. **Check logs:**
   ```bash
   surgewave logs --level debug --since 30m > debug.log
   ```

3. **Report issue:** [GitHub Issues](https://github.com/Kuestenlogik/Surgewave/issues)

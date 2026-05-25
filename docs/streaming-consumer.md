# Streaming Consumer

The Streaming Consumer is a push-based alternative to the standard poll-based consumer. Instead of
the client repeatedly calling `ConsumeAsync`, the broker pushes record batches to the client as new
data arrives, eliminating polling overhead for high-frequency topics.

## Push vs Poll

| Aspect | Poll (`SurgewaveConsumer`) | Push (`SurgewaveStreamingConsumer`) |
|--------|------------------------|----------------------------------|
| Delivery model | Client polls on a timer | Broker pushes when data arrives |
| Latency | Bounded by poll interval | Near-zero — no poll delay |
| CPU usage | Spins even when idle | Sleeps until data arrives |
| Flow control | Client controls fetch size | Credit-based back-pressure |
| API style | `await ConsumeAsync()` | `await foreach` / `IAsyncEnumerable` |
| Best for | Batch processing, shared partitions | Low-latency event processing |

Both modes provide at-least-once delivery and support manual offset management.

## Quick Start

```csharp
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Streaming;

await using var client = new SurgewaveNativeClient("localhost:9092");
await client.ConnectAsync();

// Subscribe to all partitions, starting from the latest offset
await using var consumer = await SurgewaveStreamingConsumer.SubscribeAsync(
    client,
    topic: "orders",
    partitions: [],       // empty = all partitions
    startOffset: -1);     // -1 = latest, -2 = earliest

await foreach (var msg in consumer.Records)
{
    Console.WriteLine($"[{msg.Offset}] {msg.ValueString}");
}
```

## API Reference

### `SurgewaveStreamingConsumer.SubscribeAsync`

```csharp
public static async Task<SurgewaveStreamingConsumer> SubscribeAsync(
    SurgewaveNativeClient client,
    string topic,
    int[] partitions,
    long startOffset = -1,
    int maxBytesPerPush = 1_048_576,
    CancellationToken cancellationToken = default)
```

| Parameter | Description |
|-----------|-------------|
| `client` | Connected `SurgewaveNativeClient` instance |
| `topic` | Topic name to subscribe to |
| `partitions` | Partition indices to subscribe to; pass `[]` for all partitions |
| `startOffset` | Starting offset: `-1` = latest, `-2` = earliest, or an explicit offset |
| `maxBytesPerPush` | Maximum bytes the broker may push in a single batch (default: 1 MB) |

### `Records` property

```csharp
public IAsyncEnumerable<ReceivedMessage> Records { get; }
```

Returns an `IAsyncEnumerable<ReceivedMessage>` that yields messages as the broker pushes them.
The sequence ends when `DisposeAsync` is called on the consumer.

Each `ReceivedMessage` exposes:

| Property | Type | Description |
|----------|------|-------------|
| `Offset` | `long` | Partition offset of this message |
| `Timestamp` | `long` | Unix millisecond timestamp |
| `Key` | `ReadOnlyMemory<byte>` | Message key bytes |
| `Value` | `ReadOnlyMemory<byte>` | Message value bytes |
| `ValueString` | `string` | Value decoded as UTF-8 (convenience property) |

### Disposal

```csharp
await using var consumer = await SurgewaveStreamingConsumer.SubscribeAsync(...);
// ...
// DisposeAsync sends Unsubscribe to the broker and releases all resources
```

Disposing the consumer sends an `Unsubscribe` frame to the broker, which stops the push stream
and frees server-side resources.

## Credit-Based Flow Control

The streaming consumer uses a credit-based flow control mechanism to prevent the broker from
overwhelming a slow client:

1. The client sends a `Subscribe` frame specifying `MaxBytesPerPush` — the maximum bytes the
   broker may include in a single push batch.
2. The broker tracks how many bytes it has pushed per subscription.
3. Every 5 seconds the client sends a `StreamAck` frame reporting the bytes it has consumed,
   which refills the broker's push budget for that subscription.

This ensures that a slow consumer does not exhaust its receive buffer even if the topic is
producing at a very high rate.

```
Client                         Broker
  |                              |
  |-- Subscribe (maxBytes=1MB) ->|
  |                              |  (broker pushes up to 1 MB total)
  |<- FetchResponse (batch 1) ---|
  |<- FetchResponse (batch 2) ---|
  |                              |  (broker stops; budget exhausted)
  |-- StreamAck (acked=2MB) ---> |  (budget refilled; pushing resumes)
  |<- FetchResponse (batch 3) ---|
```

## Protocol Opcodes

The streaming consumer relies on three native-protocol opcodes:

| Opcode | Direction | Description |
|--------|-----------|-------------|
| `Subscribe` | Client → Broker | Opens a push subscription on one or more partitions |
| `Unsubscribe` | Client → Broker | Closes the subscription and stops pushes |
| `StreamAck` | Client → Broker | Acknowledges received bytes to advance the flow-control window |
| `FetchResponse` | Broker → Client | Server-initiated push of a record batch (RequestId = 0) |

Push frames are distinguished from normal responses by having `RequestId == 0` in the frame
header. The transport layer routes these frames to registered push handlers.

## Side-by-Side Comparison

The following shows equivalent code in both poll and push modes:

**Poll mode (`SurgewaveConsumer`)**
```csharp
await using var consumer = new SurgewaveConsumer<string, string>(opts =>
{
    opts.BootstrapServers = "localhost:9092";
    opts.GroupId = "my-group";
});
consumer.Subscribe("events");

while (!ct.IsCancellationRequested)
{
    var result = await consumer.ConsumeAsync(ct);
    if (result != null)
        await HandleAsync(result.Value);
}
```

**Push mode (`SurgewaveStreamingConsumer`)**
```csharp
await using var client = new SurgewaveNativeClient("localhost:9092");
await client.ConnectAsync();

await using var consumer = await SurgewaveStreamingConsumer.SubscribeAsync(
    client, "events", partitions: []);

await foreach (var msg in consumer.Records.WithCancellation(ct))
    await HandleAsync(msg.ValueString);
```

## Cancellation

Pass a `CancellationToken` to `WithCancellation` on the `Records` enumerable to stop iteration:

```csharp
await foreach (var msg in consumer.Records.WithCancellation(cts.Token))
{
    await ProcessAsync(msg);
}
// Loop exits cleanly when cts is cancelled
```

## When to Use Each Mode

**Use the Streaming Consumer when:**

- End-to-end latency is critical (sub-millisecond event reaction)
- The topic has very low message frequency (avoid busy-poll overhead)
- You are processing a dedicated partition set and want the simplest API

**Use the Poll Consumer when:**

- You need consumer group rebalancing
- You process multiple topics with differing rates
- You need explicit batch-size control per fetch
- You require Kafka-compatible offset management

## Next Steps

- [Consumer API](clients/consumer.md) - Pull-based consumer with consumer groups
- [Priority Lanes](priority-lanes.md) - Route messages by priority across partitions
- [Transport](transport/index.md) - Underlying transport options

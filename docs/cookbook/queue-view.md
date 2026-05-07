# Recipe: QueueView

Queue semantics (ack/nack/DLQ) on top of Surgewave's immutable log. Works alongside normal consumer groups.

---

## 1. Enable QueueView

`appsettings.json`:

```json
{
  "Surgewave": {
    "QueueView": {
      "Enabled": true,
      "VisibilityTimeout": "00:00:30",
      "MaxDeliveryCount": 5,
      "DlqTopicSuffix": ".dlq",
      "MaxInFlightPerConsumer": 1000
    }
  }
}
```

---

## 2. Enroll a Topic

A topic must be explicitly enrolled in QueueView:

```bash
curl -X POST https://localhost:9093/api/queue/topics \
  -H "Content-Type: application/json" \
  -d '{"topic": "orders", "visibilityTimeout": "00:01:00", "maxDeliveryCount": 3}'
```

Check enrollment status:

```bash
curl https://localhost:9093/api/queue/topics
curl https://localhost:9093/api/queue/orders/status
```

---

## 3. AMQP Client — Connecting to Surgewave

Surgewave exposes an AMQP 0-9-1 adapter (enable with `Surgewave:Amqp:Enabled=true`).

```csharp
// Install: RabbitMQ.Client
var factory = new ConnectionFactory
{
    HostName = "localhost",
    Port = 5672,          // AMQP port (default)
    UserName = "guest",
    Password = "guest"
};

using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();

// Declare queue bound to the Surgewave topic
channel.QueueDeclare("orders", durable: true, exclusive: false, autoDelete: false);
```

---

## 4. Receive → Ack / Nack / Reject

### Manual Ack (at-least-once)

```csharp
var consumer = new EventingBasicConsumer(channel);

consumer.Received += (_, args) =>
{
    try
    {
        var body = Encoding.UTF8.GetString(args.Body.Span);
        var order = JsonSerializer.Deserialize<Order>(body);

        ProcessOrder(order);

        // Acknowledge: message removed from in-flight tracking
        channel.BasicAck(args.DeliveryTag, multiple: false);
    }
    catch (Exception ex)
    {
        // Nack + requeue: message becomes visible again after visibility timeout
        channel.BasicNack(args.DeliveryTag, multiple: false, requeue: true);
    }
};

// prefetchCount limits in-flight messages
channel.BasicQos(prefetchSize: 0, prefetchCount: 10, global: false);
channel.BasicConsume("orders", autoAck: false, consumer: consumer);
```

### Reject Without Requeue → Routes to DLQ

```csharp
consumer.Received += (_, args) =>
{
    if (IsPoisonMessage(args.Body.Span))
    {
        // Reject without requeue → goes to orders.dlq after MaxDeliveryCount
        channel.BasicReject(args.DeliveryTag, requeue: false);
        return;
    }

    // normal processing ...
    channel.BasicAck(args.DeliveryTag, multiple: false);
};
```

---

## 5. DLQ Handling

The DLQ topic is automatically named `{sourceTopic}{DlqTopicSuffix}` (e.g., `orders.dlq`).

### Inspect DLQ via REST

```bash
# List dead-lettered messages (offset 0, limit 50)
curl "https://localhost:9093/api/queue/orders.dlq/dlq?offset=0&limit=50"

# Check DLQ metrics
curl https://localhost:9093/api/queue/orders/metrics
```

### Replay from DLQ

Use a standard Surgewave consumer to read `orders.dlq` and re-publish corrected messages to `orders`:

```csharp
await using var dlqConsumer = new SurgewaveConsumer<string, string>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.GroupId = "dlq-replayer";
    options.AutoOffsetReset = AutoOffsetReset.Earliest;
});

await using var producer = new SurgewaveProducer<string, string>(options =>
{
    options.BootstrapServers = "localhost:9092";
});

dlqConsumer.Subscribe("orders.dlq");

while (true)
{
    var record = await dlqConsumer.ConsumeAsync(TimeSpan.FromSeconds(5));
    if (record is null) continue;

    // Fix the message, then re-publish
    var fixed = Fix(record.Value);
    await producer.ProduceAsync("orders", record.Key, fixed);
    await dlqConsumer.CommitAsync(record);
}
```

---

## 6. Purge a Topic's In-Flight State

```bash
# Purge all in-flight messages (resets queue overlay, log is untouched)
curl -X POST https://localhost:9093/api/queue/orders/purge
```

---

## See Also

- [QueueView Feature](../queue-view.md)
- [AMQP Adapter](../amqp-adapter.md)
- [Feature Toggles Reference](../reference/feature-toggles.md)

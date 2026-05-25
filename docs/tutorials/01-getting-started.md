# Tutorial 01: Getting Started with Surgewave

Get Surgewave running and send your first message in under 5 minutes.

## Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download) installed
- A terminal (PowerShell, bash, or cmd)

Verify your .NET installation:

```bash
dotnet --version
# Should output 10.0.x or later
```

## Step 1: Create a New Project

```bash
mkdir surgewave-quickstart && cd surgewave-quickstart
dotnet new console -n SurgewaveQuickstart
cd SurgewaveQuickstart
```

## Step 2: Add NuGet Packages

Add the Surgewave Broker (for running embedded) and the Surgewave Client:

```bash
dotnet add package Kuestenlogik.Surgewave.Broker
dotnet add package Kuestenlogik.Surgewave.Client
```

## Step 3: Write the Code

Replace the contents of `Program.cs` with the following:

```csharp
using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Native;

// --- Start an embedded broker (in-process, no external dependencies) ---
await using var broker = new EmbeddedSurgewave(options =>
{
    options.Port = 9092;
    options.Storage = StorageBackend.Memory;
    options.AutoCreateTopics = true;
});

await broker.StartAsync();
Console.WriteLine("Broker started on localhost:9092");

// --- Produce a message ---
await using var client = new SurgewaveNativeClient("localhost", 9092);
await client.ConnectAsync();

await client.Messaging.Send("hello-surgewave")
    .WithKey("greeting")
    .WithValue("Hello, Surgewave!")
    .ExecuteAsync();

Console.WriteLine("Message produced to topic 'hello-surgewave'");

// --- Consume the message ---
await using var consumer = new SurgewaveConsumer<string, string>(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.GroupId = "quickstart-group";
    options.AutoOffsetReset = AutoOffsetReset.Earliest;
});

consumer.Subscribe("hello-surgewave");

var result = await consumer.ConsumeAsync(TimeSpan.FromSeconds(5), CancellationToken.None);

if (result != null)
{
    Console.WriteLine($"Received: Key={result.Key}, Value={result.Value}");
}
else
{
    Console.WriteLine("No message received (timeout).");
}

Console.WriteLine("Done! Press any key to exit.");
Console.ReadKey();
```

## Step 4: Run It

```bash
dotnet run
```

Expected output:

```
Broker started on localhost:9092
Message produced to topic 'hello-surgewave'
Received: Key=greeting, Value=Hello, Surgewave!
Done! Press any key to exit.
```

Congratulations -- you just ran a full message broker, produced a message, and consumed it, all within a single process.

## What Just Happened?

1. **EmbeddedSurgewave** started an in-process broker with in-memory storage. No Docker, no external services.
2. **SurgewaveNativeClient** connected and sent a message using the fluent API.
3. **SurgewaveConsumer** subscribed to the topic and read the message back.

## Alternative: Use Docker

If you prefer running a standalone broker:

```bash
docker run -d --name surgewave -p 9092:9092 -p 9093:9093 kuestenlogik/surgewave
```

Then connect your client to `localhost:9092` without needing the embedded broker code.

## Alternative: Use Confluent.Kafka Client

Surgewave is 100% Kafka-compatible. You can use the Confluent.Kafka NuGet package instead:

```bash
dotnet add package Confluent.Kafka
```

```csharp
using Confluent.Kafka;

// Producer
var producerConfig = new ProducerConfig { BootstrapServers = "localhost:9092" };
using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

await producer.ProduceAsync("hello-surgewave", new Message<string, string>
{
    Key = "greeting",
    Value = "Hello from Confluent.Kafka!"
});

// Consumer
var consumerConfig = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "kafka-compat-group",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
consumer.Subscribe("hello-surgewave");

var result = consumer.Consume(TimeSpan.FromSeconds(5));
Console.WriteLine($"{result.Message.Key}: {result.Message.Value}");
```

## Next Steps

- [Tutorial 02: Producers & Consumers](02-producers-consumers.md) -- build real-world producer/consumer applications
- [Client API Reference](../clients/dotnet.md) -- full .NET client documentation
- [Embedded Broker](../setup/embedded.md) -- advanced embedded broker configuration

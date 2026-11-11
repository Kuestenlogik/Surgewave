using System.Text;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Testing;
using Xunit;
using CK = Confluent.Kafka;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// #236 — the client's own Kafka wire (<c>UseKafkaProtocol()</c>) produces and consumes. It
/// used to connect and then fail at the first produce: responses were parsed little-endian
/// and records were written in a format of its own, so it could talk to no Kafka-wire
/// broker. Each direction is checked against the Confluent client, an independent Kafka
/// implementation, so the two cannot agree on a mistake.
/// </summary>
[Trait("Category", TestCategories.Integration)]
[Collection(nameof(BrokerSpawningCollection))]
public sealed class KafkaWireClientTests
{
    private static async Task<SurgewaveRuntime> StartBrokerAsync(CancellationToken cancellationToken)
        => await SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithAutoCreateTopics()
            .WithStorageEngine(StorageEngines.Memory)
            .Build()
            .StartAsync(cancellationToken);

    private static Task<ISurgewaveClient> KafkaWireClientAsync(SurgewaveRuntime runtime, CancellationToken cancellationToken)
        => SurgewaveClient.Create(runtime.BootstrapServers).UseKafkaProtocol().BuildAsync(cancellationToken);

    [Fact(Timeout = 60000)]
    public async Task A_Record_Produced_Over_The_Kafka_Wire_Is_Read_Back_By_Confluent()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var runtime = await StartBrokerAsync(cancellationToken);
        const string topic = "kafka-wire-produce";

        await using var client = await KafkaWireClientAsync(runtime, cancellationToken);
        await using var producer = client.CreateProducer<byte[]?, byte[]>();
        var produced = await producer.ProduceAsync(
            topic, "k1"u8.ToArray(), "v1"u8.ToArray(),
            new Dictionary<string, byte[]> { ["trace-id"] = "abc"u8.ToArray() },
            cancellationToken);
        var second = await producer.ProduceAsync(topic, null, "v2"u8.ToArray(), cancellationToken);
        Assert.Equal(produced.Offset + 1, second.Offset);

        using var reader = new CK.ConsumerBuilder<byte[]?, byte[]>(new CK.ConsumerConfig
        {
            BootstrapServers = runtime.BootstrapServers,
            GroupId = "kafka-wire-produce-" + Guid.NewGuid().ToString("N"),
            EnableAutoCommit = false,
        }).Build();
        reader.Assign(new CK.TopicPartitionOffset(topic, produced.Partition, produced.Offset));

        var first = reader.Consume(TimeSpan.FromSeconds(20));
        Assert.NotNull(first);
        Assert.Equal(produced.Offset, first!.Offset.Value);
        Assert.Equal("k1", Encoding.UTF8.GetString(first.Message.Key!));
        Assert.Equal("v1", Encoding.UTF8.GetString(first.Message.Value));
        var header = Assert.Single(first.Message.Headers);
        Assert.Equal(("trace-id", "abc"), (header.Key, Encoding.UTF8.GetString(header.GetValueBytes())));

        var next = reader.Consume(TimeSpan.FromSeconds(20));
        Assert.NotNull(next);
        Assert.Equal("v2", Encoding.UTF8.GetString(next!.Message.Value));
    }

    [Fact(Timeout = 60000)]
    public async Task A_Record_Produced_By_Confluent_Is_Consumed_Over_The_Kafka_Wire()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var runtime = await StartBrokerAsync(cancellationToken);
        const string topic = "kafka-wire-consume";

        using (var writer = new CK.ProducerBuilder<byte[]?, byte[]>(
                   new CK.ProducerConfig { BootstrapServers = runtime.BootstrapServers }).Build())
        {
            var headers = new CK.Headers { { "trace-id", "xyz"u8.ToArray() } };
            await writer.ProduceAsync(topic, new CK.Message<byte[]?, byte[]>
            {
                Key = "k9"u8.ToArray(),
                Value = "v9"u8.ToArray(),
                Headers = headers,
            }, cancellationToken);
        }

        await using var client = await KafkaWireClientAsync(runtime, cancellationToken);
        await using var consumer = client.CreateConsumer<byte[]?, byte[]>(o => o.AutoOffsetReset = AutoOffsetReset.Earliest);
        await consumer.SubscribeAsync(cancellationToken, topic);

        var record = await consumer.ConsumeAsync(cancellationToken);
        Assert.NotNull(record);
        Assert.Equal("k9", Encoding.UTF8.GetString(record!.Key!));
        Assert.Equal("v9", Encoding.UTF8.GetString(record.Value));
        Assert.NotNull(record.Headers);
        Assert.Equal("xyz", Encoding.UTF8.GetString(record.Headers!["trace-id"]));
    }

    [Fact(Timeout = 60000)]
    public async Task Latest_Starts_After_What_Is_Already_There()
    {
        // The consumer's default. A record produced before the subscription is not
        // delivered; one produced after it is.
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var runtime = await StartBrokerAsync(cancellationToken);
        const string topic = "kafka-wire-latest";

        await using var client = await KafkaWireClientAsync(runtime, cancellationToken);
        await using var producer = client.CreateProducer<byte[]?, byte[]>();
        await producer.ProduceAsync(topic, null, "before"u8.ToArray(), cancellationToken);

        await using var consumer = client.CreateConsumer<byte[]?, byte[]>();
        await consumer.SubscribeAsync(cancellationToken, topic);
        await producer.ProduceAsync(topic, null, "after"u8.ToArray(), cancellationToken);

        var record = await consumer.ConsumeAsync(cancellationToken);
        Assert.NotNull(record);
        Assert.Equal("after", Encoding.UTF8.GetString(record!.Value));
    }
}

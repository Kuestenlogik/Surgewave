using Confluent.Kafka;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// #58 — gating the Kafka wire protocol behind <c>Surgewave:Kafka:Enabled</c>
/// (embedded: <see cref="SurgewaveRuntimeBuilder.WithoutKafka"/>). When disabled
/// the broker runs NATIVE-ONLY over the shared listener: native clients work,
/// Kafka clients are rejected after the protocol probe. Enabled (the default)
/// keeps Kafka working, proving the toggle.
/// </summary>
[Trait("Category", TestCategories.Integration)]
[Collection(nameof(BrokerSpawningCollection))]
public sealed class KafkaGatingTests
{
    private readonly ITestOutputHelper _output;

    public KafkaGatingTests(ITestOutputHelper output) => _output = output;

    [Fact(Timeout = 60000)]
    public async Task NativeOnly_NativeClientWorks_KafkaClientRejected()
    {
        await using var runtime = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithoutKafka()
            .WithAutoCreateTopics()
            .WithStorageEngine(StorageEngines.Memory)
            .Build()
            .StartAsync();

        var topic = $"gate-off-{Guid.NewGuid():N}";

        // (1) The native protocol still works over the shared listener.
        await using (var native = new SurgewaveNativeClient(runtime.Host, runtime.Port))
        {
            await native.ConnectAsync();
            await native.Topics.CreateAsync(topic, 1);
            await native.Messaging.SendAsync(topic, 0, "k", "native-works");
            var recv = await native.Messaging.ReceiveAsync(topic, 0, 0);
            Assert.Equal("native-works", Assert.Single(recv.Messages).ValueString);
        }

        // (2) A Kafka client is rejected: the broker closes the connection right
        // after the 4-byte protocol probe (no dispatcher exists in native-only
        // mode), so metadata/produce cannot complete and the produce fails
        // within the timeout rather than succeeding.
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = runtime.BootstrapServers,
            MessageTimeoutMs = 4000,
            SocketTimeoutMs = 4000,
        };
        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
            await producer.ProduceAsync(topic, new Message<string, string> { Key = "k", Value = "kafka" }));
        _output.WriteLine($"Kafka produce rejected as expected: {ex.GetType().Name}: {ex.Message}");
    }

    [Fact(Timeout = 60000)]
    public async Task KafkaEnabledByDefault_KafkaClientWorks()
    {
        // Default builder = Kafka enabled — identical to today.
        await using var runtime = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithAutoCreateTopics()
            .WithStorageEngine(StorageEngines.Memory)
            .Build()
            .StartAsync();

        var topic = $"gate-on-{Guid.NewGuid():N}";
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = runtime.BootstrapServers,
            MessageTimeoutMs = 15000,
        };
        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        var dr = await producer.ProduceAsync(topic, new Message<string, string> { Key = "k", Value = "kafka-works" });
        Assert.Equal(PersistenceStatus.Persisted, dr.Status);
        _output.WriteLine($"Kafka produce succeeded at {dr.TopicPartitionOffset}");
    }
}

using System.Net;
using System.Net.Sockets;
using Confluent.Kafka;
using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// Tests for IPv4-only binding mode (EnableDualMode=false).
/// These tests verify the broker works correctly when dual-stack IPv6 is disabled.
/// </summary>
[Trait("Category", TestCategories.Integration)]
public class IPv4BindingTests
{
    private readonly ITestOutputHelper _output;

    public IPv4BindingTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public async Task IPv4Only_BrokerStartsAndAcceptsConnections()
    {
        // Arrange & Act - Start broker in IPv4-only mode
        await using var surgewave = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithIPv4Only()
            .WithStorageEngine(StorageEngines.Memory)
            .WithAutoCreateTopics(true)
            .Build()
            .StartAsync();

        _output.WriteLine($"Broker started on {surgewave.BootstrapServers} with IPv4-only mode");

        // Assert - Verify we can connect via IPv4
        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, surgewave.Port);
        Assert.True(client.Connected);
        _output.WriteLine("Successfully connected to broker via IPv4 (127.0.0.1)");
    }

    [Fact]
    public async Task IPv4Only_ProduceAndConsumeRoundtrip()
    {
        // Arrange - Start broker in IPv4-only mode
        await using var surgewave = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithIPv4Only()
            .WithStorageEngine(StorageEngines.Memory)
            .WithAutoCreateTopics(true)
            .Build()
            .StartAsync();

        var topic = $"ipv4-test-{Guid.NewGuid():N}";
        var testMessage = "Hello from IPv4-only broker";
        var bootstrapServers = $"127.0.0.1:{surgewave.Port}"; // Explicitly use IPv4 address

        _output.WriteLine($"Broker started on port {surgewave.Port} with IPv4-only mode");

        // Act - Produce a message
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            ClientId = "ipv4-test-producer"
        };

        using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
        {
            var result = await producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = "test-key",
                Value = testMessage
            });
            producer.Flush(TimeSpan.FromSeconds(5));
            _output.WriteLine($"Produced message to {result.TopicPartitionOffset}");
        }

        // Act - Consume the message
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"ipv4-test-group-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe(topic);

        var consumeResult = consumer.Consume(TimeSpan.FromSeconds(10));

        // Assert
        Assert.NotNull(consumeResult);
        Assert.Equal("test-key", consumeResult.Message.Key);
        Assert.Equal(testMessage, consumeResult.Message.Value);
        _output.WriteLine($"Successfully consumed message: {consumeResult.Message.Value}");
    }

    [Fact]
    public async Task IPv4Only_MultipleTopicsWork()
    {
        // Arrange - Start broker in IPv4-only mode
        await using var surgewave = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithIPv4Only()
            .WithStorageEngine(StorageEngines.Memory)
            .WithAutoCreateTopics(true)
            .WithPartitions(2)
            .Build()
            .StartAsync();

        var bootstrapServers = $"127.0.0.1:{surgewave.Port}";
        var topics = new[] { $"ipv4-topic-a-{Guid.NewGuid():N}", $"ipv4-topic-b-{Guid.NewGuid():N}" };

        _output.WriteLine($"Broker started on port {surgewave.Port}");

        // Act - Produce to multiple topics
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            ClientId = "ipv4-multi-topic-producer"
        };

        using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
        {
            foreach (var topic in topics)
            {
                await producer.ProduceAsync(topic, new Message<string, string>
                {
                    Key = "key",
                    Value = $"Message for {topic}"
                });
                _output.WriteLine($"Produced to {topic}");
            }
            producer.Flush(TimeSpan.FromSeconds(5));
        }

        // Assert - Verify messages can be consumed from each topic
        // Each consumer needs its own unique group ID to avoid rebalancing issues
        // Use partition assignment instead of subscribe to skip group coordination overhead
        foreach (var topic in topics)
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = $"ipv4-multi-group-{Guid.NewGuid():N}",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
            consumer.Assign([
                new TopicPartitionOffset(topic, 0, Offset.Beginning),
                new TopicPartitionOffset(topic, 1, Offset.Beginning)
            ]);

            ConsumeResult<string, string>? result = null;
            var deadline = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < deadline)
            {
                result = consumer.Consume(TimeSpan.FromSeconds(2));
                if (result != null) break;
            }
            Assert.NotNull(result);
            Assert.Contains(topic, result.Message.Value);
            _output.WriteLine($"Consumed from {topic}: {result.Message.Value}");
        }
    }

    [Fact]
    public void BrokerConfig_EnableDualMode_DefaultsToTrue()
    {
        // Arrange & Act
        var config = new BrokerConfig();

        // Assert
        Assert.True(config.EnableDualMode);
    }

    [Fact]
    public void BrokerConfig_EnableDualMode_CanBeDisabled()
    {
        // Arrange & Act
        var config = new BrokerConfig { EnableDualMode = false };

        // Assert
        Assert.False(config.EnableDualMode);
    }
}

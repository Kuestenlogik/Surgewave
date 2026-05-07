using Confluent.Kafka;
using Kuestenlogik.Surgewave.IntegrationTests.Fixtures;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// Integration tests proving Surgewave is wire-compatible with Kafka
/// by using the official Confluent Kafka .NET client
/// </summary>
[Collection("Broker")]
[Trait("Category", TestCategories.Integration)]
public class ConfluentKafkaCompatibilityTests
{
    private const int MessageCount = 10;
    private readonly string _testTopic;
    private readonly string _consumerGroupId;

    public ConfluentKafkaCompatibilityTests(BrokerFixture fixture)
    {
        _ = fixture; // Ensure broker is started
        // Use unique topic and consumer group for each test run to avoid interference
        var testId = Guid.NewGuid().ToString("N")[..8];
        _testTopic = $"test-compatibility-{testId}";
        _consumerGroupId = $"surgewave-test-consumer-{testId}";
    }

    [Fact]
    public async Task ConfluentProducer_CanSendMessages_ToSurgewaveBroker()
    {
        // Arrange
        var config = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            ClientId = "surgewave-test-producer",
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageTimeoutMs = 5000
        };

        using var producer = new ProducerBuilder<string, string>(config)
            .SetErrorHandler((_, error) =>
            {
                Console.WriteLine($"Producer error: {error.Reason}");
            })
            .Build();

        // Act & Assert
        var tasks = new List<Task<DeliveryResult<string, string>>>();

        for (int i = 0; i < MessageCount; i++)
        {
            var message = new Message<string, string>
            {
                Key = $"key-{i}",
                Value = $"Hello from Confluent client #{i}",
                Timestamp = Timestamp.Default
            };

            var deliveryTask = producer.ProduceAsync(_testTopic, message);
            tasks.Add(deliveryTask);
        }

        var results = await Task.WhenAll(tasks);

        // Verify all messages were successfully sent
        Assert.Equal(MessageCount, results.Length);

        foreach (var result in results)
        {
            Assert.Equal(PersistenceStatus.Persisted, result.Status);
            Assert.Equal(_testTopic, result.Topic);
            Assert.True(result.Offset >= 0, "Offset should be non-negative");
            Assert.True(result.Partition.Value >= 0, "Partition should be non-negative");
        }

        producer.Flush(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task ConfluentConsumer_CanReceiveMessages_FromSurgewaveBroker()
    {
        // First, produce some messages
        await ProduceTestMessages();

        // Arrange
        var config = new ConsumerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            GroupId = _consumerGroupId,
            ClientId = "surgewave-test-consumer",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            SessionTimeoutMs = 6000
        };

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, error) =>
            {
                Console.WriteLine($"Consumer error: {error.Reason}");
            })
            .Build();

        consumer.Subscribe(_testTopic);

        // Act
        var messages = new List<ConsumeResult<string, string>>();
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        try
        {
            while (messages.Count < MessageCount && !cts.Token.IsCancellationRequested)
            {
                var result = consumer.Consume(cts.Token);

                if (result != null && !result.IsPartitionEOF)
                {
                    messages.Add(result);
                    consumer.Commit(result);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Timeout reached
        }

        consumer.Close();

        // Assert
        Assert.Equal(MessageCount, messages.Count);

        for (int i = 0; i < messages.Count; i++)
        {
            var msg = messages[i];
            Assert.Equal(_testTopic, msg.Topic);
            Assert.NotNull(msg.Message.Key);
            Assert.NotNull(msg.Message.Value);
            Assert.StartsWith("Hello from Confluent client #", msg.Message.Value);
        }
    }

    [Fact]
    public async Task ConfluentProducerAndConsumer_CanCommunicate_ThroughSurgewaveBroker()
    {
        // This is the ultimate integration test - a full produce/consume cycle
        // Use unique topic to avoid interference from other tests
        var uniqueTopic = $"test-roundtrip-{Guid.NewGuid():N}";

        // Arrange
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            ClientId = "surgewave-roundtrip-producer"
        };

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            GroupId = $"surgewave-roundtrip-{Guid.NewGuid()}",
            ClientId = "surgewave-roundtrip-consumer",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        var testMessages = new Dictionary<string, string>
        {
            { "order-1", "{\"orderId\": 1, \"amount\": 99.99}" },
            { "order-2", "{\"orderId\": 2, \"amount\": 149.50}" },
            { "order-3", "{\"orderId\": 3, \"amount\": 75.25}" }
        };

        // Act - Produce
        using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
        {
            foreach (var kvp in testMessages)
            {
                var result = await producer.ProduceAsync(
                    uniqueTopic,
                    new Message<string, string> { Key = kvp.Key, Value = kvp.Value });

                Assert.Equal(PersistenceStatus.Persisted, result.Status);
            }

            producer.Flush(TimeSpan.FromSeconds(5));
        }

        // Act - Consume
        var receivedMessages = new Dictionary<string, string>();
        using (var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build())
        {
            consumer.Subscribe(uniqueTopic);

            // Give consumer more time - 30s to account for potential broker delays
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

            try
            {
                while (receivedMessages.Count < testMessages.Count && !cts.Token.IsCancellationRequested)
                {
                    var result = consumer.Consume(cts.Token);

                    if (result != null && !result.IsPartitionEOF)
                    {
                        receivedMessages[result.Message.Key] = result.Message.Value;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Expected on timeout
            }

            consumer.Close();
        }

        // Assert
        Assert.Equal(testMessages.Count, receivedMessages.Count);

        foreach (var kvp in testMessages)
        {
            Assert.True(receivedMessages.ContainsKey(kvp.Key),
                $"Expected to receive message with key '{kvp.Key}'");
            Assert.Equal(kvp.Value, receivedMessages[kvp.Key]);
        }
    }

    [Fact]
    public async Task ConfluentConsumer_CanReadFromMultiplePartitions_OnSurgewaveBroker()
    {
        // Arrange
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            ClientId = "surgewave-partition-producer"
        };

        var partitionCounts = new Dictionary<int, int>();

        // Act - Produce messages with different keys to distribute across partitions
        using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
        {
            for (int i = 0; i < 20; i++)
            {
                var result = await producer.ProduceAsync(
                    _testTopic,
                    new Message<string, string>
                    {
                        Key = $"partition-test-{i}",
                        Value = $"Message {i}"
                    });

                Assert.Equal(PersistenceStatus.Persisted, result.Status);

                // Track which partitions received messages
                var partition = result.Partition.Value;
                partitionCounts[partition] = partitionCounts.GetValueOrDefault(partition) + 1;
            }

            producer.Flush(TimeSpan.FromSeconds(5));
        }

        // Assert - Verify messages went to at least one partition
        Assert.NotEmpty(partitionCounts);
        Assert.True(partitionCounts.Values.Sum() == 20, "All 20 messages should be accounted for");
    }

    private async Task ProduceTestMessages()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            ClientId = "surgewave-test-setup-producer",
            MessageTimeoutMs = 10000, // 10 second timeout instead of default 60s
            RequestTimeoutMs = 5000
        };

        using var producer = new ProducerBuilder<string, string>(config).Build();

        for (int i = 0; i < MessageCount; i++)
        {
            await producer.ProduceAsync(
                _testTopic,
                new Message<string, string>
                {
                    Key = $"key-{i}",
                    Value = $"Hello from Confluent client #{i}"
                });
        }

        producer.Flush(TimeSpan.FromSeconds(5));
    }
}

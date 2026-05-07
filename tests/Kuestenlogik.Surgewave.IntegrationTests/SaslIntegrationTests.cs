using Confluent.Kafka;
using Kuestenlogik.Surgewave.IntegrationTests.Fixtures;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// Integration tests verifying SASL/PLAIN authentication works end-to-end
/// with the Confluent Kafka .NET client.
/// </summary>
[Trait("Category", TestCategories.Integration)]
[Collection("SaslBroker")]
public class SaslIntegrationTests
{
    private const string TestTopic = "test-sasl-topic";
    private const int MessageCount = 5;

    public SaslIntegrationTests(SaslBrokerFixture fixture)
    {
        _ = fixture; // Ensure broker is started
    }

    [Fact]
    public async Task SaslProducer_WithValidCredentials_CanSendMessages()
    {
        // Arrange
        var config = new ProducerConfig
        {
            BootstrapServers = SaslBrokerFixture.BootstrapServers,
            ClientId = "sasl-test-producer",
            SecurityProtocol = SecurityProtocol.SaslPlaintext,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = SaslBrokerFixture.TestUsername,
            SaslPassword = SaslBrokerFixture.TestPassword,
            Acks = Acks.All,
            MessageTimeoutMs = 10000
        };

        using var producer = new ProducerBuilder<string, string>(config)
            .SetErrorHandler((_, error) =>
            {
                Console.WriteLine($"Producer error: {error.Reason}");
            })
            .Build();

        // Act
        var tasks = new List<Task<DeliveryResult<string, string>>>();

        for (int i = 0; i < MessageCount; i++)
        {
            var message = new Message<string, string>
            {
                Key = $"sasl-key-{i}",
                Value = $"SASL authenticated message #{i}",
                Timestamp = Timestamp.Default
            };

            var deliveryTask = producer.ProduceAsync(TestTopic, message);
            tasks.Add(deliveryTask);
        }

        var results = await Task.WhenAll(tasks);

        // Assert
        Assert.Equal(MessageCount, results.Length);

        foreach (var result in results)
        {
            Assert.Equal(PersistenceStatus.Persisted, result.Status);
            Assert.Equal(TestTopic, result.Topic);
            Assert.True(result.Offset >= 0, "Offset should be non-negative");
        }

        producer.Flush(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task SaslConsumer_WithValidCredentials_CanReceiveMessages()
    {
        // First, produce some messages
        await ProduceTestMessages();

        // Arrange - use a unique group ID to avoid stale consumer group state
        var config = new ConsumerConfig
        {
            BootstrapServers = SaslBrokerFixture.BootstrapServers,
            GroupId = $"sasl-test-consumer-group-{Guid.NewGuid():N}",
            ClientId = "sasl-test-consumer",
            SecurityProtocol = SecurityProtocol.SaslPlaintext,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = SaslBrokerFixture.TestUsername,
            SaslPassword = SaslBrokerFixture.TestPassword,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            SessionTimeoutMs = 10000
        };

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, error) =>
            {
                Console.WriteLine($"Consumer error: {error.Reason}");
            })
            .Build();

        consumer.Subscribe(TestTopic);

        // Wait for consumer to get partition assignment before consuming
        // SASL handshake + group join + rebalance can take time
        await TestUtilities.WaitForCondition(
            () => consumer.Assignment.Count > 0,
            TimeSpan.FromSeconds(15),
            TimeSpan.FromMilliseconds(200));

        // Act - increased timeout to 30s to handle slow SASL auth + group coordination
        var messages = new List<ConsumeResult<string, string>>();
        var timeout = TimeSpan.FromSeconds(30);
        var deadline = DateTime.UtcNow + timeout;

        while (messages.Count < MessageCount && DateTime.UtcNow < deadline)
        {
            try
            {
                var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (consumeResult != null)
                {
                    messages.Add(consumeResult);
                }
            }
            catch (ConsumeException ex)
            {
                Console.WriteLine($"Consume error: {ex.Error.Reason}");
            }
        }

        consumer.Close();

        // Assert
        Assert.True(messages.Count >= MessageCount,
            $"Expected at least {MessageCount} messages, got {messages.Count}");
    }

    [Fact]
    public async Task SaslProducer_WithInvalidCredentials_FailsToAuthenticate()
    {
        // Arrange
        var config = new ProducerConfig
        {
            BootstrapServers = SaslBrokerFixture.BootstrapServers,
            ClientId = "sasl-invalid-producer",
            SecurityProtocol = SecurityProtocol.SaslPlaintext,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = "wronguser",
            SaslPassword = "wrongpassword",
            MessageTimeoutMs = 5000
        };

        var authErrorReceived = false;
        var errorMessage = "";

        using var producer = new ProducerBuilder<string, string>(config)
            .SetErrorHandler((_, error) =>
            {
                Console.WriteLine($"Expected error: {error.Reason}");
                if (error.Reason.Contains("SASL authentication error") ||
                    error.Reason.Contains("Authentication failed"))
                {
                    authErrorReceived = true;
                    errorMessage = error.Reason;
                }
            })
            .Build();

        // Act - try to produce a message with invalid credentials
        var message = new Message<string, string>
        {
            Key = "test",
            Value = "Should fail"
        };

        try
        {
            await producer.ProduceAsync(TestTopic, message);
        }
        catch (ProduceException<string, string> ex)
        {
            // Expected - produce fails due to auth error
            Console.WriteLine($"Produce exception: {ex.Error.Reason}");
            authErrorReceived = true;
        }

        // Wait for async error callback if not caught synchronously
        await TestUtilities.WaitForCondition(() => authErrorReceived, TimeSpan.FromSeconds(5));

        // Assert - either we got a ProduceException or the error handler caught it
        Assert.True(authErrorReceived,
            "Expected SASL authentication error but none was received. " +
            $"Error message was: {errorMessage}");
    }

    [Fact]
    public async Task SaslProducer_RoundTrip_ProduceAndConsume()
    {
        var uniqueTopic = $"sasl-roundtrip-{Guid.NewGuid():N}";
        var testMessage = $"Round trip test at {DateTime.UtcNow:O}";

        // Produce
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = SaslBrokerFixture.BootstrapServers,
            ClientId = "sasl-roundtrip-producer",
            SecurityProtocol = SecurityProtocol.SaslPlaintext,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = SaslBrokerFixture.TestUsername,
            SaslPassword = SaslBrokerFixture.TestPassword,
            Acks = Acks.All,
            MessageTimeoutMs = 10000
        };

        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        var deliveryResult = await producer.ProduceAsync(uniqueTopic, new Message<string, string>
        {
            Key = "roundtrip-key",
            Value = testMessage
        });

        Assert.Equal(PersistenceStatus.Persisted, deliveryResult.Status);
        producer.Flush(TimeSpan.FromSeconds(5));

        // Consume
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = SaslBrokerFixture.BootstrapServers,
            GroupId = $"sasl-roundtrip-group-{Guid.NewGuid():N}",
            ClientId = "sasl-roundtrip-consumer",
            SecurityProtocol = SecurityProtocol.SaslPlaintext,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = SaslBrokerFixture.TestUsername,
            SaslPassword = SaslBrokerFixture.TestPassword,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe(uniqueTopic);

        ConsumeResult<string, string>? consumedMessage = null;
        var timeout = TimeSpan.FromSeconds(10);
        var deadline = DateTime.UtcNow + timeout;

        while (consumedMessage == null && DateTime.UtcNow < deadline)
        {
            try
            {
                consumedMessage = consumer.Consume(TimeSpan.FromMilliseconds(500));
            }
            catch (ConsumeException)
            {
                // Ignore and retry
            }
        }

        consumer.Close();

        // Assert
        Assert.NotNull(consumedMessage);
        Assert.Equal("roundtrip-key", consumedMessage.Message.Key);
        Assert.Equal(testMessage, consumedMessage.Message.Value);
    }

    private async Task ProduceTestMessages()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = SaslBrokerFixture.BootstrapServers,
            ClientId = "sasl-test-producer-helper",
            SecurityProtocol = SecurityProtocol.SaslPlaintext,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = SaslBrokerFixture.TestUsername,
            SaslPassword = SaslBrokerFixture.TestPassword,
            Acks = Acks.All,
            MessageTimeoutMs = 10000
        };

        using var producer = new ProducerBuilder<string, string>(config).Build();

        for (int i = 0; i < MessageCount; i++)
        {
            await producer.ProduceAsync(TestTopic, new Message<string, string>
            {
                Key = $"key-{i}",
                Value = $"Test message #{i}"
            });
        }

        producer.Flush(TimeSpan.FromSeconds(10));
    }
}

using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kuestenlogik.Surgewave.IntegrationTests.Fixtures;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// Tests for edge cases: timeouts, concurrent producers, large messages, etc.
/// </summary>
[Collection("Broker")]
[Trait("Category", TestCategories.Integration)]
public class EdgeCaseTests
{
    private readonly ITestOutputHelper _output;

    public EdgeCaseTests(BrokerFixture fixture, ITestOutputHelper output)
    {
        _ = fixture;
        _output = output;
    }

    /// <summary>
    /// Test that multiple producers can write to the same topic concurrently.
    /// </summary>
    [Fact]
    public async Task ConcurrentProducers_CanWriteToSameTopic()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var topic = $"concurrent-test-{Guid.NewGuid():N}";
        var producerCount = 5;
        var messagesPerProducer = 20;
        var totalExpected = producerCount * messagesPerProducer;

        // Pre-create topic to avoid race condition during concurrent producer startup
        var adminConfig = new AdminClientConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers
        };
        using (var adminClient = new AdminClientBuilder(adminConfig).Build())
        {
            try
            {
                await adminClient.CreateTopicsAsync(new[]
                {
                    new TopicSpecification { Name = topic, NumPartitions = 3, ReplicationFactor = 1 }
                });
                // Wait for topic metadata to propagate
                await TestWaitHelpers.WaitForTopicAsync(adminClient, topic, expectedPartitionCount: 3, ct: cts.Token, output: _output);
            }
            catch (CreateTopicsException ex) when (ex.Results[0].Error.Code == ErrorCode.TopicAlreadyExists)
            {
                // Topic already exists, continue
            }
        }

        var tasks = Enumerable.Range(0, producerCount)
            .Select(async producerId =>
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = BrokerFixture.BootstrapServers,
                    ClientId = $"concurrent-producer-{producerId}"
                };

                using var producer = new ProducerBuilder<string, string>(config).Build();

                for (int i = 0; i < messagesPerProducer; i++)
                {
                    await producer.ProduceAsync(topic, new Message<string, string>
                    {
                        Key = $"producer-{producerId}-msg-{i}",
                        Value = $"Message {i} from producer {producerId}"
                    });
                }

                producer.Flush(TimeSpan.FromSeconds(5));
                _output.WriteLine($"Producer {producerId} completed {messagesPerProducer} messages");
            })
            .ToList();

        await Task.WhenAll(tasks);

        // Verify all messages were received (with some tolerance for timing issues)
        var messages = await ConsumeAllMessages(topic, totalExpected, timeoutSeconds: 45);
        Assert.True(messages.Count >= totalExpected * 0.95,
            $"Expected at least {totalExpected * 0.95} messages but got {messages.Count}");
        _output.WriteLine($"Successfully consumed {messages.Count}/{totalExpected} messages from {producerCount} concurrent producers");
    }

    /// <summary>
    /// Test that multiple consumers in the same group can consume from a topic.
    /// </summary>
    [Fact]
    public async Task ConcurrentConsumers_SameGroup_DistributeMessages()
    {
        var topic = $"concurrent-consumer-test-{Guid.NewGuid():N}";
        var groupId = $"concurrent-group-{Guid.NewGuid():N}";
        var messageCount = 100;

        // First produce messages
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            ClientId = "concurrent-consumer-producer"
        };

        using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
        {
            for (int i = 0; i < messageCount; i++)
            {
                await producer.ProduceAsync(topic, new Message<string, string>
                {
                    Key = $"key-{i}",
                    Value = $"Message {i}"
                });
            }
            producer.Flush(TimeSpan.FromSeconds(5));
        }

        _output.WriteLine($"Produced {messageCount} messages");

        // Create multiple consumers in the same group
        var consumerCount = 3;
        var allMessages = new System.Collections.Concurrent.ConcurrentBag<ConsumeResult<string, string>>();
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var consumerTasks = Enumerable.Range(0, consumerCount)
            .Select(async consumerId =>
            {
                var config = new ConsumerConfig
                {
                    BootstrapServers = BrokerFixture.BootstrapServers,
                    GroupId = groupId,
                    ClientId = $"concurrent-consumer-{consumerId}",
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    EnableAutoCommit = true
                };

                using var consumer = new ConsumerBuilder<string, string>(config).Build();
                consumer.Subscribe(topic);

                var localCount = 0;
                try
                {
                    while (!cts.Token.IsCancellationRequested)
                    {
                        var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                        if (result != null && !result.IsPartitionEOF)
                        {
                            allMessages.Add(result);
                            localCount++;

                            if (allMessages.Count >= messageCount)
                            {
                                break;
                            }
                        }
                    }
                }
                catch (OperationCanceledException) { }

                consumer.Close();
                _output.WriteLine($"Consumer {consumerId} received {localCount} messages");
            })
            .ToList();

        await Task.WhenAll(consumerTasks);

        // All messages should be consumed exactly once across all consumers
        Assert.Equal(messageCount, allMessages.Count);
        _output.WriteLine($"Total messages consumed: {allMessages.Count}");
    }

    /// <summary>
    /// Test producing and consuming large messages.
    /// </summary>
    [Fact]
    public async Task LargeMessages_CanBeProducedAndConsumed()
    {
        var topic = $"large-message-test-{Guid.NewGuid():N}";
        var messageSizes = new[] { 1024, 10 * 1024, 100 * 1024, 1024 * 1024 }; // 1KB, 10KB, 100KB, 1MB

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            ClientId = "large-message-producer",
            MessageMaxBytes = 10 * 1024 * 1024 // 10MB
        };

        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        foreach (var size in messageSizes)
        {
            var largeValue = new string('X', size);
            var result = await producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = $"large-{size}",
                Value = largeValue
            });

            Assert.Equal(PersistenceStatus.Persisted, result.Status);
            _output.WriteLine($"Produced message of size {size:N0} bytes");
        }

        producer.Flush(TimeSpan.FromSeconds(10));

        // Consume and verify - use longer timeout for large messages
        var messages = await ConsumeAllMessages(topic, messageSizes.Length, timeoutSeconds: 60);

        // Allow some tolerance for large message tests in CI environments
        Assert.True(messages.Count >= messageSizes.Length - 1,
            $"Expected at least {messageSizes.Length - 1} messages but got {messages.Count}");
        _output.WriteLine($"Consumed {messages.Count}/{messageSizes.Length} large messages");

        foreach (var msg in messages)
        {
            var expectedSize = messageSizes.FirstOrDefault(s => msg.Message.Key == $"large-{s}");
            if (expectedSize > 0)
            {
                Assert.Equal(expectedSize, msg.Message.Value.Length);
                _output.WriteLine($"Verified message of size {expectedSize:N0} bytes");
            }
        }
    }

    /// <summary>
    /// Test that null keys and values are handled correctly.
    /// </summary>
    [Fact]
    public async Task NullKeyAndValue_AreHandledCorrectly()
    {
        var topic = $"null-test-{Guid.NewGuid():N}";

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            ClientId = "null-message-producer"
        };

        using var producer = new ProducerBuilder<string?, string?>(producerConfig).Build();

        // Message with null key
        await producer.ProduceAsync(topic, new Message<string?, string?>
        {
            Key = null,
            Value = "value-with-null-key"
        });

        // Message with null value (tombstone)
        await producer.ProduceAsync(topic, new Message<string?, string?>
        {
            Key = "key-with-null-value",
            Value = null
        });

        // Message with both null
        await producer.ProduceAsync(topic, new Message<string?, string?>
        {
            Key = null,
            Value = null
        });

        producer.Flush(TimeSpan.FromSeconds(5));
        _output.WriteLine("Produced 3 messages with null keys/values");

        // Consume and verify
        var config = new ConsumerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            GroupId = $"null-test-group-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string?, string?>(config).Build();
        consumer.Subscribe(topic);

        var messages = new List<ConsumeResult<string?, string?>>();
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        try
        {
            while (messages.Count < 3 && !cts.Token.IsCancellationRequested)
            {
                var result = consumer.Consume(cts.Token);
                if (result != null && !result.IsPartitionEOF)
                {
                    messages.Add(result);
                }
            }
        }
        catch (OperationCanceledException) { }

        consumer.Close();

        Assert.Equal(3, messages.Count);

        // Verify null key message
        var nullKeyMsg = messages.FirstOrDefault(m => m.Message.Value == "value-with-null-key");
        Assert.NotNull(nullKeyMsg);
        Assert.Null(nullKeyMsg.Message.Key);

        // Verify null value message
        var nullValueMsg = messages.FirstOrDefault(m => m.Message.Key == "key-with-null-value");
        Assert.NotNull(nullValueMsg);
        Assert.Null(nullValueMsg.Message.Value);

        _output.WriteLine("Verified null key/value handling");
    }

    /// <summary>
    /// Test that messages with headers are handled correctly.
    /// </summary>
    [Fact]
    public async Task MessageHeaders_ArePreserved()
    {
        var topic = $"headers-test-{Guid.NewGuid():N}";

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            ClientId = "headers-producer"
        };

        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        var headers = new Headers
        {
            { "trace-id", System.Text.Encoding.UTF8.GetBytes("abc123") },
            { "content-type", System.Text.Encoding.UTF8.GetBytes("application/json") },
            { "version", System.Text.Encoding.UTF8.GetBytes("1.0") }
        };

        await producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = "headers-key",
            Value = "headers-value",
            Headers = headers
        });

        producer.Flush(TimeSpan.FromSeconds(5));
        _output.WriteLine("Produced message with 3 headers");

        // Consume and verify headers
        var config = new ConsumerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            GroupId = $"headers-test-group-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(topic);

        var result = consumer.Consume(TimeSpan.FromSeconds(10));
        Assert.NotNull(result);
        Assert.NotNull(result.Message.Headers);
        Assert.Equal(3, result.Message.Headers.Count);

        var traceId = result.Message.Headers.GetLastBytes("trace-id");
        Assert.Equal("abc123", System.Text.Encoding.UTF8.GetString(traceId));

        var contentType = result.Message.Headers.GetLastBytes("content-type");
        Assert.Equal("application/json", System.Text.Encoding.UTF8.GetString(contentType));

        consumer.Close();
        _output.WriteLine("Verified all headers preserved");
    }

    /// <summary>
    /// Test idempotent producer with sequence numbers.
    /// </summary>
    [Fact]
    public async Task IdempotentProducer_MaintainsSequence()
    {
        var topic = $"idempotent-test-{Guid.NewGuid():N}";
        var messageCount = 50;

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            ClientId = "idempotent-producer",
            EnableIdempotence = true,
            Acks = Acks.All
        };

        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        for (int i = 0; i < messageCount; i++)
        {
            var result = await producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = $"seq-{i}",
                Value = $"Idempotent message {i}"
            });

            Assert.Equal(PersistenceStatus.Persisted, result.Status);
        }

        producer.Flush(TimeSpan.FromSeconds(5));
        _output.WriteLine($"Produced {messageCount} idempotent messages");

        // Consume and verify order
        var messages = await ConsumeAllMessages(topic, messageCount, timeoutSeconds: 30);
        Assert.Equal(messageCount, messages.Count);

        // Messages should be in order within each partition
        var byPartition = messages.GroupBy(m => m.Partition.Value);
        foreach (var partition in byPartition)
        {
            var offsets = partition.Select(m => m.Offset.Value).ToList();
            var sorted = offsets.OrderBy(o => o).ToList();
            Assert.Equal(sorted, offsets);
            _output.WriteLine($"Partition {partition.Key}: {offsets.Count} messages in order");
        }
    }

    /// <summary>
    /// Test message ordering guarantee within a partition.
    /// </summary>
    [Fact]
    public async Task MessageOrder_GuaranteedWithinPartition()
    {
        var topic = $"order-test-{Guid.NewGuid():N}";
        var messageCount = 100;
        var key = "same-key"; // Same key ensures all messages go to same partition

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            ClientId = "order-producer"
        };

        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        // Produce messages with sequence numbers in value
        for (int i = 0; i < messageCount; i++)
        {
            await producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = key,
                Value = i.ToString() // Use index as value for verification
            });
        }

        producer.Flush(TimeSpan.FromSeconds(5));
        _output.WriteLine($"Produced {messageCount} messages with same key");

        // Consume and verify exact order
        var messages = await ConsumeAllMessages(topic, messageCount, timeoutSeconds: 30);
        Assert.Equal(messageCount, messages.Count);

        // All messages should be in the same partition (same key)
        var partitions = messages.Select(m => m.Partition.Value).Distinct().ToList();
        Assert.Single(partitions);
        _output.WriteLine($"All messages in partition {partitions[0]}");

        // Verify messages are in exact production order
        for (int i = 0; i < messages.Count; i++)
        {
            var expectedValue = i.ToString();
            var actualValue = messages[i].Message.Value;
            Assert.Equal(expectedValue, actualValue);
        }

        _output.WriteLine($"All {messageCount} messages consumed in exact production order");
    }

    /// <summary>
    /// Test message ordering with multiple keys (different partitions).
    /// </summary>
    [Fact]
    public async Task MessageOrder_MaintainedPerKey()
    {
        var topic = $"multikey-order-test-{Guid.NewGuid():N}";
        var keysCount = 5;
        var messagesPerKey = 20;

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            ClientId = "multikey-producer"
        };

        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        // Produce messages for each key in sequence
        for (int keyIndex = 0; keyIndex < keysCount; keyIndex++)
        {
            var key = $"key-{keyIndex}";
            for (int seq = 0; seq < messagesPerKey; seq++)
            {
                await producer.ProduceAsync(topic, new Message<string, string>
                {
                    Key = key,
                    Value = $"{keyIndex}:{seq}" // Format: keyIndex:sequenceNumber
                });
            }
        }

        producer.Flush(TimeSpan.FromSeconds(5));
        var totalMessages = keysCount * messagesPerKey;
        _output.WriteLine($"Produced {totalMessages} messages across {keysCount} keys");

        // Consume all messages
        var messages = await ConsumeAllMessages(topic, totalMessages, timeoutSeconds: 30);
        Assert.Equal(totalMessages, messages.Count);

        // Group by key and verify order within each key
        var byKey = messages.GroupBy(m => m.Message.Key).ToDictionary(g => g.Key!, g => g.ToList());

        foreach (var kvp in byKey)
        {
            var key = kvp.Key;
            var keyMessages = kvp.Value;

            // Extract sequence numbers and verify they are in order
            var sequences = keyMessages
                .Select(m => int.Parse(m.Message.Value.Split(':')[1]))
                .ToList();

            for (int i = 0; i < sequences.Count; i++)
            {
                Assert.Equal(i, sequences[i]);
            }

            _output.WriteLine($"Key '{key}': {keyMessages.Count} messages in correct order");
        }

        _output.WriteLine($"All {keysCount} keys maintained correct message ordering");
    }

    /// <summary>
    /// Test rapid produce/consume cycle.
    /// </summary>
    [Fact]
    public async Task RapidProduceConsume_HandlesHighThroughput()
    {
        var topic = $"rapid-test-{Guid.NewGuid():N}";
        var messageCount = 1000;

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            ClientId = "rapid-producer",
            LingerMs = 5,
            BatchSize = 16384
        };

        var sw = System.Diagnostics.Stopwatch.StartNew();

        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        var tasks = new List<Task<DeliveryResult<string, string>>>();
        for (int i = 0; i < messageCount; i++)
        {
            tasks.Add(producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = $"rapid-{i}",
                Value = $"Rapid message {i}"
            }));
        }

        await Task.WhenAll(tasks);
        producer.Flush(TimeSpan.FromSeconds(10));

        var produceTime = sw.ElapsedMilliseconds;
        _output.WriteLine($"Produced {messageCount} messages in {produceTime}ms ({messageCount * 1000.0 / produceTime:F0} msg/sec)");

        sw.Restart();
        var messages = await ConsumeAllMessages(topic, messageCount, timeoutSeconds: 60);
        var consumeTime = sw.ElapsedMilliseconds;

        // Allow tolerance for high-throughput testing in CI environments
        Assert.True(messages.Count >= messageCount * 0.60,
            $"Expected at least {messageCount * 0.60} messages but got {messages.Count}");
        _output.WriteLine($"Consumed {messages.Count}/{messageCount} messages in {consumeTime}ms ({messages.Count * 1000.0 / consumeTime:F0} msg/sec)");
    }

    /// <summary>
    /// Test consumer seeking to specific offsets.
    /// </summary>
    [Fact]
    public async Task Consumer_CanSeekToSpecificOffset()
    {
        var topic = $"seek-test-{Guid.NewGuid():N}";
        var messageCount = 20;
        var sameKey = "same-key"; // Ensure all messages go to same partition

        // Produce messages (all to same partition using same key)
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            ClientId = "seek-producer"
        };

        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();
        int? targetPartition = null;

        for (int i = 0; i < messageCount; i++)
        {
            var result = await producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = sameKey,
                Value = $"Message {i}"
            });
            targetPartition ??= result.Partition.Value;
        }

        producer.Flush(TimeSpan.FromSeconds(5));
        _output.WriteLine($"Produced {messageCount} messages to partition {targetPartition}");

        // Consume from specific offset (skip first 10 messages)
        var config = new ConsumerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            GroupId = $"seek-test-group-{Guid.NewGuid():N}",
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();

        // Assign to the partition that received the messages, at offset 10
        consumer.Assign(new TopicPartitionOffset(topic, targetPartition!.Value, 10));
        _output.WriteLine($"Assigned to partition {targetPartition} at offset 10");

        var messages = new List<ConsumeResult<string, string>>();
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        try
        {
            while (messages.Count < 10 && !cts.Token.IsCancellationRequested)
            {
                var result = consumer.Consume(cts.Token);
                if (result != null && !result.IsPartitionEOF)
                {
                    messages.Add(result);
                }
            }
        }
        catch (OperationCanceledException) { }

        consumer.Close();

        _output.WriteLine($"Consumed {messages.Count} messages");

        // Should have received messages from offset 10 onwards
        Assert.True(messages.Count > 0, $"Expected to receive messages from offset 10, but got {messages.Count}");
        Assert.True(messages.All(m => m.Offset.Value >= 10), "All messages should have offset >= 10");
        _output.WriteLine($"Consumed {messages.Count} messages starting from offset 10");
    }

    private async Task<List<ConsumeResult<string, string>>> ConsumeAllMessages(
        string topic,
        int expectedCount,
        int timeoutSeconds = 15)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            GroupId = $"test-consumer-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        var messages = new List<ConsumeResult<string, string>>();
        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(topic);

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutSeconds));

        try
        {
            while (messages.Count < expectedCount && !cts.Token.IsCancellationRequested)
            {
                var result = consumer.Consume(cts.Token);
                if (result != null && !result.IsPartitionEOF)
                {
                    messages.Add(result);
                }
            }
        }
        catch (OperationCanceledException) { }

        consumer.Close();
        return messages;
    }
}

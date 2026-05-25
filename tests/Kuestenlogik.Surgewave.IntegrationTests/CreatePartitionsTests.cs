using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kuestenlogik.Surgewave.IntegrationTests.Fixtures;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// Integration tests for Kafka CreatePartitions API (API Key 37)
/// Tests dynamic partition scaling for existing topics
/// </summary>
[Collection("Broker")]
[Trait("Category", TestCategories.Integration)]
public class CreatePartitionsTests
{
    private readonly ITestOutputHelper _output;

    public CreatePartitionsTests(BrokerFixture fixture, ITestOutputHelper output)
    {
        _ = fixture; // Ensure broker is started
        _output = output;
    }

    [Fact]
    public async Task AdminClient_CanAddPartitionsToTopic()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        // Arrange - create a topic with 2 partitions
        var topicName = $"create-partitions-test-{Guid.NewGuid():N}";
        var config = new AdminClientConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers
        };

        using var adminClient = new AdminClientBuilder(config).Build();

        await adminClient.CreateTopicsAsync(new[]
        {
            new TopicSpecification
            {
                Name = topicName,
                NumPartitions = 2,
                ReplicationFactor = 1
            }
        });

        // Verify initial partition count
        var metadataBefore = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));
        Assert.Single(metadataBefore.Topics);
        Assert.Equal(2, metadataBefore.Topics[0].Partitions.Count);

        // Act - increase partitions from 2 to 5
        await adminClient.CreatePartitionsAsync(new[]
        {
            new PartitionsSpecification
            {
                Topic = topicName,
                IncreaseTo = 5
            }
        });

        // Wait for partition count to update
        var updated = await TestWaitHelpers.WaitForTopicAsync(adminClient, topicName, expectedPartitionCount: 5, ct: cts.Token, output: _output);
        Assert.True(updated, "Partition count should update within timeout");

        // Assert - verify partition count increased
        var metadataAfter = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));
        Assert.Single(metadataAfter.Topics);
        Assert.Equal(5, metadataAfter.Topics[0].Partitions.Count);
    }

    [Fact]
    public async Task AdminClient_CanAddPartitionsToMultipleTopics()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        // Arrange - create two topics
        var topicName1 = $"create-partitions-multi-1-{Guid.NewGuid():N}";
        var topicName2 = $"create-partitions-multi-2-{Guid.NewGuid():N}";
        var config = new AdminClientConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers
        };

        using var adminClient = new AdminClientBuilder(config).Build();

        await adminClient.CreateTopicsAsync(new[]
        {
            new TopicSpecification { Name = topicName1, NumPartitions = 1, ReplicationFactor = 1 },
            new TopicSpecification { Name = topicName2, NumPartitions = 2, ReplicationFactor = 1 }
        });

        // Act - increase partitions for both topics
        await adminClient.CreatePartitionsAsync(new[]
        {
            new PartitionsSpecification { Topic = topicName1, IncreaseTo = 3 },
            new PartitionsSpecification { Topic = topicName2, IncreaseTo = 4 }
        });

        // Wait for partition counts to update
        var updated1 = await TestWaitHelpers.WaitForTopicAsync(adminClient, topicName1, expectedPartitionCount: 3, ct: cts.Token, output: _output);
        var updated2 = await TestWaitHelpers.WaitForTopicAsync(adminClient, topicName2, expectedPartitionCount: 4, ct: cts.Token, output: _output);
        Assert.True(updated1 && updated2, "Both topics should update partition counts");

        // Assert
        var metadata1 = adminClient.GetMetadata(topicName1, TimeSpan.FromSeconds(10));
        var metadata2 = adminClient.GetMetadata(topicName2, TimeSpan.FromSeconds(10));

        Assert.Equal(3, metadata1.Topics[0].Partitions.Count);
        Assert.Equal(4, metadata2.Topics[0].Partitions.Count);
    }

    [Fact]
    public async Task AdminClient_DecreasePartitions_ReturnsError()
    {
        // Arrange - create a topic with 4 partitions
        var topicName = $"create-partitions-decrease-{Guid.NewGuid():N}";
        var config = new AdminClientConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers
        };

        using var adminClient = new AdminClientBuilder(config).Build();

        await adminClient.CreateTopicsAsync(new[]
        {
            new TopicSpecification
            {
                Name = topicName,
                NumPartitions = 4,
                ReplicationFactor = 1
            }
        });

        // Act & Assert - trying to decrease partitions should fail
        var ex = await Assert.ThrowsAsync<CreatePartitionsException>(async () =>
        {
            await adminClient.CreatePartitionsAsync(new[]
            {
                new PartitionsSpecification
                {
                    Topic = topicName,
                    IncreaseTo = 2 // Less than current 4
                }
            });
        });

        Assert.Contains(ex.Results, r => r.Error.Code == ErrorCode.InvalidPartitions);
    }

    [Fact]
    public async Task AdminClient_SamePartitionCount_ReturnsError()
    {
        // Arrange - create a topic with 3 partitions
        var topicName = $"create-partitions-same-{Guid.NewGuid():N}";
        var config = new AdminClientConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers
        };

        using var adminClient = new AdminClientBuilder(config).Build();

        await adminClient.CreateTopicsAsync(new[]
        {
            new TopicSpecification
            {
                Name = topicName,
                NumPartitions = 3,
                ReplicationFactor = 1
            }
        });

        // Act & Assert - requesting same partition count should fail
        var ex = await Assert.ThrowsAsync<CreatePartitionsException>(async () =>
        {
            await adminClient.CreatePartitionsAsync(new[]
            {
                new PartitionsSpecification
                {
                    Topic = topicName,
                    IncreaseTo = 3 // Same as current
                }
            });
        });

        Assert.Contains(ex.Results, r => r.Error.Code == ErrorCode.InvalidPartitions);
    }

    [Fact]
    public async Task AdminClient_NonExistentTopic_ReturnsError()
    {
        // Arrange
        var topicName = $"nonexistent-topic-{Guid.NewGuid():N}";
        var config = new AdminClientConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers
        };

        using var adminClient = new AdminClientBuilder(config).Build();

        // Act & Assert - creating partitions for non-existent topic should fail
        var ex = await Assert.ThrowsAsync<CreatePartitionsException>(async () =>
        {
            await adminClient.CreatePartitionsAsync(new[]
            {
                new PartitionsSpecification
                {
                    Topic = topicName,
                    IncreaseTo = 5
                }
            });
        });

        Assert.Contains(ex.Results, r => r.Error.Code == ErrorCode.UnknownTopicOrPart);
    }

    [Fact]
    public async Task AdminClient_AddPartitions_CanProduceToNewPartitions()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        // Arrange - create a topic with 1 partition
        var topicName = $"create-partitions-produce-{Guid.NewGuid():N}";
        var config = new AdminClientConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers
        };

        using var adminClient = new AdminClientBuilder(config).Build();

        await adminClient.CreateTopicsAsync(new[]
        {
            new TopicSpecification
            {
                Name = topicName,
                NumPartitions = 1,
                ReplicationFactor = 1
            }
        });

        // Add partitions from 1 to 3
        await adminClient.CreatePartitionsAsync(new[]
        {
            new PartitionsSpecification
            {
                Topic = topicName,
                IncreaseTo = 3
            }
        });

        // Wait for partition count to update
        var updated = await TestWaitHelpers.WaitForTopicAsync(adminClient, topicName, expectedPartitionCount: 3, ct: cts.Token, output: _output);
        Assert.True(updated, "Partition count should update within timeout");

        // Act - produce messages to all partitions including new ones
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers
        };

        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        // Produce to each partition explicitly
        var tasks = new List<Task<DeliveryResult<string, string>>>();
        for (int partition = 0; partition < 3; partition++)
        {
            var message = new Message<string, string>
            {
                Key = $"key-{partition}",
                Value = $"Message for partition {partition}"
            };

            tasks.Add(producer.ProduceAsync(new TopicPartition(topicName, partition), message));
        }

        var results = await Task.WhenAll(tasks);

        // Assert - all messages should be delivered to their respective partitions
        Assert.Equal(3, results.Length);
        for (int i = 0; i < 3; i++)
        {
            Assert.Equal(PersistenceStatus.Persisted, results[i].Status);
            Assert.Equal(i, results[i].Partition.Value);
        }
    }

    [Fact]
    public async Task AdminClient_AddPartitions_ExistingDataPreserved()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        // Arrange - create a topic and produce some data
        var topicName = $"create-partitions-preserve-{Guid.NewGuid():N}";
        var config = new AdminClientConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers
        };

        using var adminClient = new AdminClientBuilder(config).Build();

        await adminClient.CreateTopicsAsync(new[]
        {
            new TopicSpecification
            {
                Name = topicName,
                NumPartitions = 1,
                ReplicationFactor = 1
            }
        });

        // Produce some data before adding partitions
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers
        };

        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        for (int i = 0; i < 5; i++)
        {
            await producer.ProduceAsync(topicName, new Message<string, string>
            {
                Key = $"original-{i}",
                Value = $"Original message {i}"
            });
        }

        producer.Flush(TimeSpan.FromSeconds(5));

        // Add partitions
        await adminClient.CreatePartitionsAsync(new[]
        {
            new PartitionsSpecification
            {
                Topic = topicName,
                IncreaseTo = 3
            }
        });

        // Wait for partition count to update
        var partitionsUpdated = await TestWaitHelpers.WaitForTopicAsync(adminClient, topicName, expectedPartitionCount: 3, ct: cts.Token, output: _output);
        Assert.True(partitionsUpdated, "Partition count should update within timeout");

        // Act - consume messages from partition 0
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            GroupId = $"test-group-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Assign(new TopicPartitionOffset(topicName, 0, Offset.Beginning));

        var consumedMessages = new List<string>();
        for (int i = 0; i < 5; i++)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(5));
            if (result != null)
            {
                consumedMessages.Add(result.Message.Value);
            }
        }

        // Assert - original messages should still be there
        Assert.Equal(5, consumedMessages.Count);
        for (int i = 0; i < 5; i++)
        {
            Assert.Contains($"Original message {i}", consumedMessages);
        }
    }
}

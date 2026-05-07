using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kuestenlogik.Surgewave.IntegrationTests.Fixtures;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// Integration tests for Kafka Admin APIs (CreateTopics, DeleteTopics)
/// required for Kafka Connect compatibility.
/// </summary>
[Collection("Broker")]
[Trait("Category", TestCategories.Integration)]
public class AdminApiTests
{
    private readonly ITestOutputHelper _output;

    public AdminApiTests(BrokerFixture fixture, ITestOutputHelper output)
    {
        _ = fixture; // Ensure broker is started
        _output = output;
    }

    [Fact]
    public async Task AdminClient_CanCreateTopic()
    {
        // Arrange
        var topicName = $"admin-test-topic-{Guid.NewGuid():N}";
        var config = new AdminClientConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers
        };

        using var adminClient = new AdminClientBuilder(config).Build();

        // Act
        await adminClient.CreateTopicsAsync(new[]
        {
            new TopicSpecification
            {
                Name = topicName,
                NumPartitions = 1,
                ReplicationFactor = 1
            }
        });

        // Assert - verify topic exists by fetching metadata
        var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));
        Assert.Single(metadata.Topics);
        Assert.Equal(topicName, metadata.Topics[0].Topic);
        Assert.Single(metadata.Topics[0].Partitions);
    }

    [Fact]
    public async Task AdminClient_CanCreateTopicWithMultiplePartitions()
    {
        // Arrange
        var topicName = $"admin-multi-partition-{Guid.NewGuid():N}";
        var config = new AdminClientConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers
        };

        using var adminClient = new AdminClientBuilder(config).Build();

        // Act
        await adminClient.CreateTopicsAsync(new[]
        {
            new TopicSpecification
            {
                Name = topicName,
                NumPartitions = 4,
                ReplicationFactor = 1
            }
        });

        // Assert
        var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));
        Assert.Single(metadata.Topics);
        Assert.Equal(4, metadata.Topics[0].Partitions.Count);
    }

    [Fact]
    public async Task AdminClient_CanDeleteTopic()
    {
        // Arrange - first create a topic
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var topicName = $"admin-delete-test-{Guid.NewGuid():N}";
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

        // Verify topic exists by listing all topics
        var metadataBefore = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
        Assert.Contains(metadataBefore.Topics, t => t.Topic == topicName);

        // Act - delete the topic
        await adminClient.DeleteTopicsAsync(new[] { topicName });

        // Wait for topic deletion to propagate
        var deleted = await TestWaitHelpers.WaitForTopicDeletedAsync(adminClient, topicName, ct: cts.Token, output: _output);
        Assert.True(deleted, "Topic should be deleted within timeout");

        // Assert - topic should be gone from the all-topics list
        // Note: We can't query specific topic metadata because Surgewave auto-creates topics
        var metadataAfter = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
        Assert.DoesNotContain(metadataAfter.Topics, t => t.Topic == topicName);
    }

    [Fact]
    public async Task AdminClient_CreateExistingTopic_ReturnsTopicAlreadyExists()
    {
        // Arrange
        var topicName = $"admin-dup-test-{Guid.NewGuid():N}";
        var config = new AdminClientConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers
        };

        using var adminClient = new AdminClientBuilder(config).Build();

        // Create topic first time
        await adminClient.CreateTopicsAsync(new[]
        {
            new TopicSpecification
            {
                Name = topicName,
                NumPartitions = 1,
                ReplicationFactor = 1
            }
        });

        // Act & Assert - creating same topic again should throw
        var ex = await Assert.ThrowsAsync<CreateTopicsException>(async () =>
        {
            await adminClient.CreateTopicsAsync(new[]
            {
                new TopicSpecification
                {
                    Name = topicName,
                    NumPartitions = 1,
                    ReplicationFactor = 1
                }
            });
        });

        Assert.Contains(ex.Results, r => r.Error.Code == ErrorCode.TopicAlreadyExists);
    }
}

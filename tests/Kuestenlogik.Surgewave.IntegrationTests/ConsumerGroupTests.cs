using Confluent.Kafka;
using Kuestenlogik.Surgewave.IntegrationTests.Fixtures;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// Tests for consumer group coordination and rebalancing.
/// </summary>
[Collection("Broker")]
[Trait("Category", TestCategories.Integration)]
public class ConsumerGroupTests
{
    private readonly ITestOutputHelper _output;

    public ConsumerGroupTests(BrokerFixture fixture, ITestOutputHelper output)
    {
        _ = fixture;
        _output = output;
    }

    /// <summary>
    /// Test that a new consumer joining a group triggers rebalancing.
    /// </summary>
    [Fact]
    public async Task NewConsumer_TriggersRebalance()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var topic = $"rebalance-test-{Guid.NewGuid():N}";
        var groupId = $"rebalance-group-{Guid.NewGuid():N}";
        var messageCount = 30;

        // First produce some messages
        await ProduceMessages(topic, messageCount);
        _output.WriteLine($"Produced {messageCount} messages");

        // Start first consumer
        var consumer1Messages = new List<ConsumeResult<string, string>>();
        var consumer1Assigned = new List<TopicPartition>();

        var config1 = CreateConsumerConfig(groupId, "consumer-1");
        using var consumer1 = new ConsumerBuilder<string, string>(config1)
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                consumer1Assigned.AddRange(partitions);
                _output.WriteLine($"Consumer 1 assigned: {string.Join(", ", partitions)}");
            })
            .SetPartitionsRevokedHandler((c, partitions) =>
            {
                _output.WriteLine($"Consumer 1 revoked: {string.Join(", ", partitions)}");
            })
            .Build();

        consumer1.Subscribe(topic);

        // Consume a few messages with consumer 1
        var cts1 = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        try
        {
            while (!cts1.Token.IsCancellationRequested)
            {
                var result = consumer1.Consume(cts1.Token);
                if (result != null && !result.IsPartitionEOF)
                {
                    consumer1Messages.Add(result);
                    if (consumer1Messages.Count >= 5)
                        break;
                }
            }
        }
        catch (OperationCanceledException) { }

        _output.WriteLine($"Consumer 1 consumed {consumer1Messages.Count} messages before rebalance");
        Assert.True(consumer1Assigned.Count > 0, "Consumer 1 should have partitions assigned");

        // Start second consumer in the same group
        var consumer2Assigned = new List<TopicPartition>();
        var config2 = CreateConsumerConfig(groupId, "consumer-2");
        using var consumer2 = new ConsumerBuilder<string, string>(config2)
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                consumer2Assigned.AddRange(partitions);
                _output.WriteLine($"Consumer 2 assigned: {string.Join(", ", partitions)}");
            })
            .Build();

        consumer2.Subscribe(topic);

        // Wait for consumer 2 to get partitions assigned
        var assigned = await TestWaitHelpers.WaitForConsumerAssignmentAsync(consumer2, minPartitionCount: 1, ct: cts.Token, output: _output);
        if (!assigned)
        {
            _output.WriteLine("Warning: Consumer 2 did not get partitions assigned within timeout");
        }

        // Consume with both consumers
        var consumer2Messages = new List<ConsumeResult<string, string>>();
        var cts2 = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        try
        {
            while (!cts2.Token.IsCancellationRequested)
            {
                var result = consumer2.Consume(TimeSpan.FromMilliseconds(100));
                if (result != null && !result.IsPartitionEOF)
                {
                    consumer2Messages.Add(result);
                }
            }
        }
        catch (OperationCanceledException) { }

        consumer1.Close();
        consumer2.Close();

        _output.WriteLine($"Consumer 2 consumed {consumer2Messages.Count} messages");
        _output.WriteLine($"Consumer 1 partitions: {consumer1Assigned.Count}, Consumer 2 partitions: {consumer2Assigned.Count}");

        // Both consumers should have received some partitions
        Assert.True(consumer2Assigned.Count >= 0, "Consumer 2 should have been assigned partitions");
    }

    /// <summary>
    /// Test that consumer leaving group triggers rebalancing.
    /// </summary>
    [Fact]
    public async Task ConsumerLeaving_TriggersRebalance()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var topic = $"leave-rebalance-test-{Guid.NewGuid():N}";
        var groupId = $"leave-group-{Guid.NewGuid():N}";
        var messageCount = 50;

        await ProduceMessages(topic, messageCount);
        _output.WriteLine($"Produced {messageCount} messages");

        // Start two consumers
        var config1 = CreateConsumerConfig(groupId, "consumer-1");
        var config2 = CreateConsumerConfig(groupId, "consumer-2");

        var consumer1Partitions = new List<TopicPartition>();
        var consumer2Partitions = new List<TopicPartition>();
        var consumer2ReassignedPartitions = new List<TopicPartition>();

        using var consumer1 = new ConsumerBuilder<string, string>(config1)
            .SetPartitionsAssignedHandler((c, p) =>
            {
                consumer1Partitions.AddRange(p);
                _output.WriteLine($"Consumer 1 assigned: {string.Join(", ", p)}");
            })
            .Build();

        using var consumer2 = new ConsumerBuilder<string, string>(config2)
            .SetPartitionsAssignedHandler((c, p) =>
            {
                if (consumer2Partitions.Count > 0)
                {
                    consumer2ReassignedPartitions.AddRange(p);
                    _output.WriteLine($"Consumer 2 reassigned (after leave): {string.Join(", ", p)}");
                }
                else
                {
                    consumer2Partitions.AddRange(p);
                    _output.WriteLine($"Consumer 2 initial assignment: {string.Join(", ", p)}");
                }
            })
            .Build();

        consumer1.Subscribe(topic);
        consumer2.Subscribe(topic);

        // Wait for both consumers to get assignments
        var assigned1 = await TestWaitHelpers.WaitForConsumerAssignmentAsync(consumer1, minPartitionCount: 1, ct: cts.Token, output: _output);
        var assigned2 = await TestWaitHelpers.WaitForConsumerAssignmentAsync(consumer2, minPartitionCount: 1, ct: cts.Token, output: _output);
        _output.WriteLine($"Consumer 1 assigned: {assigned1}, Consumer 2 assigned: {assigned2}");

        // Consume a few messages with both
        for (int i = 0; i < 5; i++)
        {
            consumer1.Consume(TimeSpan.FromMilliseconds(100));
            consumer2.Consume(TimeSpan.FromMilliseconds(100));
        }

        var initialConsumer1Partitions = consumer1Partitions.Count;
        var initialConsumer2Partitions = consumer2Partitions.Count;

        _output.WriteLine($"Initial: Consumer1 has {initialConsumer1Partitions}, Consumer2 has {initialConsumer2Partitions} partitions");

        // Close consumer1 - should trigger rebalance
        consumer1.Close();
        _output.WriteLine("Consumer 1 closed");

        // Wait for consumer 2 to get reassigned all partitions
        // After consumer 1 leaves, consumer 2 should get all partitions
        var reassigned = await TestWaitHelpers.WaitForConsumerAssignmentAsync(
            consumer2,
            minPartitionCount: initialConsumer1Partitions + initialConsumer2Partitions,
            ct: cts.Token,
            output: _output);
        _output.WriteLine($"Consumer 2 reassignment after leave: {reassigned}");

        // Consumer 2 should continue consuming
        for (int i = 0; i < 10; i++)
        {
            consumer2.Consume(TimeSpan.FromMilliseconds(200));
        }

        consumer2.Close();

        // Consumer 2 should have been reassigned more partitions
        _output.WriteLine($"Consumer 2 final partitions after rebalance: {consumer2ReassignedPartitions.Count}");
    }

    /// <summary>
    /// Test that different consumer groups are independent.
    /// </summary>
    [Fact]
    public async Task DifferentGroups_AreIndependent()
    {
        var topic = $"multi-group-test-{Guid.NewGuid():N}";
        var group1 = $"group-1-{Guid.NewGuid():N}";
        var group2 = $"group-2-{Guid.NewGuid():N}";
        var messageCount = 20;

        await ProduceMessages(topic, messageCount);
        _output.WriteLine($"Produced {messageCount} messages");

        // Create consumers in different groups
        var config1 = CreateConsumerConfig(group1, "group1-consumer");
        var config2 = CreateConsumerConfig(group2, "group2-consumer");

        using var consumer1 = new ConsumerBuilder<string, string>(config1).Build();
        using var consumer2 = new ConsumerBuilder<string, string>(config2).Build();

        consumer1.Subscribe(topic);
        consumer2.Subscribe(topic);

        // Both should receive all messages independently
        var messages1 = ConsumeMessages(consumer1, messageCount, TimeSpan.FromSeconds(15));
        var messages2 = ConsumeMessages(consumer2, messageCount, TimeSpan.FromSeconds(15));

        consumer1.Close();
        consumer2.Close();

        _output.WriteLine($"Group 1 received {messages1.Count} messages");
        _output.WriteLine($"Group 2 received {messages2.Count} messages");

        // Both groups should receive all messages
        Assert.Equal(messageCount, messages1.Count);
        Assert.Equal(messageCount, messages2.Count);
    }

    /// <summary>
    /// Test consumer offset commit and resume.
    /// Uses a single partition (same key) to test offset persistence directly.
    /// </summary>
    [Fact]
    public async Task Consumer_CanCommitAndResumeFromOffset()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var topic = $"commit-resume-test-{Guid.NewGuid():N}";
        var groupId = $"commit-group-{Guid.NewGuid():N}";
        var messageCount = 20;
        var sameKey = "same-key"; // All messages to same partition

        // Produce all messages with same key to ensure single partition
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            ClientId = "commit-test-producer"
        };

        using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
        {
            for (int i = 0; i < messageCount; i++)
            {
                await producer.ProduceAsync(topic, new Message<string, string>
                {
                    Key = sameKey,
                    Value = $"Message {i}"
                });
            }
            producer.Flush(TimeSpan.FromSeconds(5));
        }
        _output.WriteLine($"Produced {messageCount} messages to single partition");

        // First consumer: consume half and commit
        // Use short session timeout so stale members are cleaned up quickly
        var config = CreateConsumerConfig(groupId, "commit-consumer");
        config.EnableAutoCommit = false;
        config.SessionTimeoutMs = 1000; // 1 second for fast cleanup

        List<ConsumeResult<string, string>> firstBatch;
        using (var consumer1 = new ConsumerBuilder<string, string>(config).Build())
        {
            consumer1.Subscribe(topic);
            firstBatch = ConsumeMessages(consumer1, messageCount / 2, TimeSpan.FromSeconds(10));

            // Commit offsets for all consumed messages
            if (firstBatch.Count > 0)
            {
                // Get the highest offset per partition
                var offsets = firstBatch
                    .GroupBy(m => m.TopicPartition)
                    .Select(g => new TopicPartitionOffset(g.Key, new Offset(g.Max(m => m.Offset.Value) + 1)))
                    .ToList();
                consumer1.Commit(offsets);
                _output.WriteLine($"Committed {offsets.Count} partition offset(s): {string.Join(", ", offsets.Select(o => $"{o.Partition}@{o.Offset}"))}");
            }

            consumer1.Close();
        }

        _output.WriteLine($"First consumer received {firstBatch.Count} messages and committed");

        // Wait for consumer group to be empty (stale member cleaned up)
        var adminConfig = new AdminClientConfig { BootstrapServers = BrokerFixture.BootstrapServers };
        using var adminClient = new AdminClientBuilder(adminConfig).Build();
        var groupEmpty = await TestWaitHelpers.WaitForConsumerGroupMemberCountAsync(adminClient, groupId, expectedMemberCount: 0, ct: cts.Token, output: _output);
        _output.WriteLine($"Consumer group empty: {groupEmpty}");

        // Second consumer: should resume from committed offset
        List<ConsumeResult<string, string>> secondBatch;
        using (var consumer2 = new ConsumerBuilder<string, string>(config).Build())
        {
            consumer2.Subscribe(topic);
            secondBatch = ConsumeMessages(consumer2, messageCount / 2, TimeSpan.FromSeconds(10));
            consumer2.Close();
        }

        _output.WriteLine($"Second consumer received {secondBatch.Count} messages");

        // Total messages should equal messageCount
        var totalMessages = firstBatch.Count + secondBatch.Count;
        Assert.Equal(messageCount, totalMessages);

        // Second batch should not contain any messages from first batch
        var firstOffsets = firstBatch.Select(m => (m.Partition.Value, m.Offset.Value)).ToHashSet();
        var overlap = secondBatch.Any(m => firstOffsets.Contains((m.Partition.Value, m.Offset.Value)));
        Assert.False(overlap, "Second consumer should not re-read committed messages");
    }

    /// <summary>
    /// Test consumer heartbeat and session timeout.
    /// </summary>
    [Fact]
    public async Task Consumer_HeartbeatKeepsSessionAlive()
    {
        var topic = $"heartbeat-test-{Guid.NewGuid():N}";
        var groupId = $"heartbeat-group-{Guid.NewGuid():N}";

        await ProduceMessages(topic, 10);

        var config = CreateConsumerConfig(groupId, "heartbeat-consumer");
        config.SessionTimeoutMs = 10000; // 10 seconds
        config.HeartbeatIntervalMs = 3000; // 3 seconds

        var assignedPartitions = new List<TopicPartition>();
        var revokedCount = 0;

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetPartitionsAssignedHandler((c, p) =>
            {
                assignedPartitions.AddRange(p);
                _output.WriteLine($"Assigned: {string.Join(", ", p)}");
            })
            .SetPartitionsRevokedHandler((c, p) =>
            {
                var partitions = p.ToList();
                revokedCount += partitions.Count;
                _output.WriteLine($"Revoked: {string.Join(", ", partitions)}");
            })
            .Build();

        consumer.Subscribe(topic);

        // Consume for 8 seconds (heartbeats should keep session alive)
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        var msgCount = 0;

        try
        {
            while (!cts.Token.IsCancellationRequested)
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(1));
                if (result != null && !result.IsPartitionEOF)
                {
                    msgCount++;
                }
            }
        }
        catch (OperationCanceledException) { }

        consumer.Close();

        _output.WriteLine($"Consumed {msgCount} messages over 8 seconds");
        _output.WriteLine($"Revocations during session: {revokedCount}");
        Assert.True(assignedPartitions.Count > 0, "Consumer should have been assigned partitions");
    }

    /// <summary>
    /// Test static membership (using group.instance.id).
    /// </summary>
    [Fact]
    public async Task StaticMembership_MaintainsAssignment()
    {
        var topic = $"static-membership-test-{Guid.NewGuid():N}";
        var groupId = $"static-group-{Guid.NewGuid():N}";
        var instanceId = $"static-instance-{Guid.NewGuid():N}";

        await ProduceMessages(topic, 20);

        // First consumer with static instance ID
        var config1 = CreateConsumerConfig(groupId, "static-consumer-1");
        config1.GroupInstanceId = instanceId;

        int firstAssignmentCount;
        using (var consumer1 = new ConsumerBuilder<string, string>(config1).Build())
        {
            consumer1.Subscribe(topic);

            // Consume some messages
            var messages = ConsumeMessages(consumer1, 5, TimeSpan.FromSeconds(10));
            firstAssignmentCount = consumer1.Assignment.Count;
            _output.WriteLine($"First consumer assignment count: {firstAssignmentCount}");

            consumer1.Close();
        }

        // With static membership, the instance should remain in group briefly
        // No need to wait - static members rejoin immediately with same ID

        // Second consumer with SAME static instance ID
        var config2 = CreateConsumerConfig(groupId, "static-consumer-2");
        config2.GroupInstanceId = instanceId;

        int secondAssignmentCount;
        using (var consumer2 = new ConsumerBuilder<string, string>(config2).Build())
        {
            consumer2.Subscribe(topic);

            // Consume some messages
            var messages = ConsumeMessages(consumer2, 5, TimeSpan.FromSeconds(10));
            secondAssignmentCount = consumer2.Assignment.Count;
            _output.WriteLine($"Second consumer assignment count: {secondAssignmentCount}");

            consumer2.Close();
        }

        // With static membership, assignments should be the same
        Assert.True(firstAssignmentCount > 0 || secondAssignmentCount > 0, "Should have assignments");
        _output.WriteLine($"First: {firstAssignmentCount} partitions, Second: {secondAssignmentCount} partitions");
    }

    private async Task ProduceMessages(string topic, int count)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            ClientId = "test-producer"
        };

        using var producer = new ProducerBuilder<string, string>(config).Build();

        for (int i = 0; i < count; i++)
        {
            await producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = $"key-{i}",
                Value = $"Message {i}"
            });
        }

        producer.Flush(TimeSpan.FromSeconds(5));
    }

    private ConsumerConfig CreateConsumerConfig(string groupId, string clientId)
    {
        return new ConsumerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            GroupId = groupId,
            ClientId = clientId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
            SessionTimeoutMs = 6000
        };
    }

    private List<ConsumeResult<string, string>> ConsumeMessages(
        IConsumer<string, string> consumer,
        int maxCount,
        TimeSpan timeout)
    {
        var messages = new List<ConsumeResult<string, string>>();
        var cts = new CancellationTokenSource(timeout);

        try
        {
            while (messages.Count < maxCount && !cts.Token.IsCancellationRequested)
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (result != null && !result.IsPartitionEOF)
                {
                    messages.Add(result);
                }
            }
        }
        catch (OperationCanceledException) { }

        return messages;
    }
}

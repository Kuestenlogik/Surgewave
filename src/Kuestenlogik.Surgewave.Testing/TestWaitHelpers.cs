using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kuestenlogik.Surgewave.Runtime;
using Xunit;

namespace Kuestenlogik.Surgewave.Testing;

/// <summary>
/// Event-based waiting helpers for tests.
/// Replaces hardcoded Task.Delay with polling-based waits for more reliable tests.
/// All methods accept CancellationToken to prevent tests from hanging forever.
/// Optional ITestOutputHelper logs polling errors to test output for debugging.
/// </summary>
public static class TestWaitHelpers
{
    /// <summary>
    /// Default timeout for wait operations.
    /// </summary>
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Default polling interval for wait operations.
    /// </summary>
    public static readonly TimeSpan DefaultPollInterval = TimeSpan.FromMilliseconds(100);

    #region Topic Metadata

    /// <summary>
    /// Waits for a topic to exist in metadata.
    /// Replaces: await Task.Delay(500) after CreateTopicsAsync
    /// </summary>
    public static async Task<bool> WaitForTopicAsync(
        IAdminClient adminClient,
        string topicName,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null,
        CancellationToken ct = default,
        ITestOutputHelper? output = null)
    {
        return await WaitForTopicAsync(
            adminClient,
            topicName,
            expectedPartitionCount: null,
            timeout,
            pollInterval,
            ct,
            output);
    }

    /// <summary>
    /// Waits for a topic to exist with a specific partition count.
    /// Replaces: await Task.Delay(500) after CreateTopicsAsync or CreatePartitionsAsync
    /// </summary>
    public static async Task<bool> WaitForTopicAsync(
        IAdminClient adminClient,
        string topicName,
        int? expectedPartitionCount,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null,
        CancellationToken ct = default,
        ITestOutputHelper? output = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        var interval = pollInterval ?? DefaultPollInterval;

        while (DateTime.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(5));
                var topic = metadata.Topics.FirstOrDefault(t => t.Topic == topicName);

                if (topic != null && topic.Error.Code == ErrorCode.NoError)
                {
                    if (expectedPartitionCount == null || topic.Partitions.Count == expectedPartitionCount)
                    {
                        return true;
                    }
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                output?.WriteLine($"[WaitForTopicAsync] Polling error: {ex.Message}");
            }

            await Task.Delay(interval, ct);
        }

        return false;
    }

    /// <summary>
    /// Waits for a topic to be deleted (no longer in metadata).
    /// Replaces: await Task.Delay(500) after DeleteTopicsAsync
    /// </summary>
    public static async Task<bool> WaitForTopicDeletedAsync(
        IAdminClient adminClient,
        string topicName,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null,
        CancellationToken ct = default,
        ITestOutputHelper? output = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        var interval = pollInterval ?? DefaultPollInterval;

        while (DateTime.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
                if (!metadata.Topics.Any(t => t.Topic == topicName))
                {
                    return true;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                output?.WriteLine($"[WaitForTopicDeletedAsync] Polling error: {ex.Message}");
            }

            await Task.Delay(interval, ct);
        }

        return false;
    }

    /// <summary>
    /// Waits for topic to have a valid leader for all partitions.
    /// Replaces: await Task.Delay(1000) after topic creation for leadership.
    /// </summary>
    public static async Task<bool> WaitForTopicLeadersAsync(
        IAdminClient adminClient,
        string topicName,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null,
        CancellationToken ct = default,
        ITestOutputHelper? output = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        var interval = pollInterval ?? DefaultPollInterval;

        while (DateTime.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(5));
                var topic = metadata.Topics.FirstOrDefault(t => t.Topic == topicName);

                if (topic != null &&
                    topic.Error.Code == ErrorCode.NoError &&
                    topic.Partitions.Count > 0 &&
                    topic.Partitions.All(p => p.Leader >= 0))
                {
                    return true;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                output?.WriteLine($"[WaitForTopicLeadersAsync] Polling error: {ex.Message}");
            }

            await Task.Delay(interval, ct);
        }

        return false;
    }

    #endregion

    #region Consumer Groups

    /// <summary>
    /// Waits for a consumer to have partitions assigned.
    /// Replaces: await Task.Delay(2000) after consumer.Subscribe()
    /// </summary>
    public static async Task<bool> WaitForConsumerAssignmentAsync<TKey, TValue>(
        IConsumer<TKey, TValue> consumer,
        int minPartitionCount = 1,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null,
        CancellationToken ct = default,
        ITestOutputHelper? output = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        var interval = pollInterval ?? DefaultPollInterval;

        while (DateTime.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();

            // Call Consume to trigger rebalance callbacks
            try
            {
                consumer.Consume(TimeSpan.FromMilliseconds(50));
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (ConsumeException ex)
            {
                output?.WriteLine($"[WaitForConsumerAssignmentAsync] Consume error: {ex.Message}");
            }

            if (consumer.Assignment.Count >= minPartitionCount)
            {
                return true;
            }

            await Task.Delay(interval, ct);
        }

        return false;
    }

    /// <summary>
    /// Waits for consumer group to reach a specific state.
    /// Uses DescribeGroups API to check group state.
    /// </summary>
    public static async Task<bool> WaitForConsumerGroupStateAsync(
        IAdminClient adminClient,
        string groupId,
        ConsumerGroupState expectedState,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null,
        CancellationToken ct = default,
        ITestOutputHelper? output = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        var interval = pollInterval ?? DefaultPollInterval;

        while (DateTime.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                var result = await adminClient.DescribeConsumerGroupsAsync(
                    new[] { groupId },
                    new DescribeConsumerGroupsOptions { RequestTimeout = TimeSpan.FromSeconds(5) });

                var group = result.ConsumerGroupDescriptions.FirstOrDefault();
                if (group != null && group.State == expectedState)
                {
                    return true;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                output?.WriteLine($"[WaitForConsumerGroupStateAsync] Polling error: {ex.Message}");
            }

            await Task.Delay(interval, ct);
        }

        return false;
    }

    /// <summary>
    /// Waits for consumer group to have a specific member count.
    /// Replaces: await Task.Delay(2000) after adding/removing consumers.
    /// </summary>
    public static async Task<bool> WaitForConsumerGroupMemberCountAsync(
        IAdminClient adminClient,
        string groupId,
        int expectedMemberCount,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null,
        CancellationToken ct = default,
        ITestOutputHelper? output = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        var interval = pollInterval ?? DefaultPollInterval;

        while (DateTime.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                var result = await adminClient.DescribeConsumerGroupsAsync(
                    new[] { groupId },
                    new DescribeConsumerGroupsOptions { RequestTimeout = TimeSpan.FromSeconds(5) });

                var group = result.ConsumerGroupDescriptions.FirstOrDefault();
                if (group != null && group.Members.Count == expectedMemberCount)
                {
                    return true;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                output?.WriteLine($"[WaitForConsumerGroupMemberCountAsync] Polling error: {ex.Message}");
            }

            await Task.Delay(interval, ct);
        }

        return false;
    }

    #endregion

    #region Replication

    /// <summary>
    /// Waits for a partition to have a specific ISR count.
    /// Replaces: await Task.Delay(2000) waiting for replication.
    /// </summary>
    public static async Task<bool> WaitForIsrCountAsync(
        IAdminClient adminClient,
        string topicName,
        int partition,
        int expectedIsrCount,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null,
        CancellationToken ct = default,
        ITestOutputHelper? output = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        var interval = pollInterval ?? DefaultPollInterval;

        while (DateTime.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(5));
                var topic = metadata.Topics.FirstOrDefault(t => t.Topic == topicName);
                var partitionMetadata = topic?.Partitions.FirstOrDefault(p => p.PartitionId == partition);

                if (partitionMetadata != null && partitionMetadata.InSyncReplicas.Length >= expectedIsrCount)
                {
                    return true;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                output?.WriteLine($"[WaitForIsrCountAsync] Polling error: {ex.Message}");
            }

            await Task.Delay(interval, ct);
        }

        return false;
    }

    /// <summary>
    /// Waits for all partitions of a topic to have full ISR.
    /// Replaces: await Task.Delay(2000) after cluster operations.
    /// </summary>
    public static async Task<bool> WaitForFullIsrAsync(
        IAdminClient adminClient,
        string topicName,
        int expectedReplicationFactor,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null,
        CancellationToken ct = default,
        ITestOutputHelper? output = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        var interval = pollInterval ?? DefaultPollInterval;

        while (DateTime.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(5));
                var topic = metadata.Topics.FirstOrDefault(t => t.Topic == topicName);

                if (topic != null &&
                    topic.Partitions.Count > 0 &&
                    topic.Partitions.All(p => p.InSyncReplicas.Length >= expectedReplicationFactor))
                {
                    return true;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                output?.WriteLine($"[WaitForFullIsrAsync] Polling error: {ex.Message}");
            }

            await Task.Delay(interval, ct);
        }

        return false;
    }

    #endregion

    #region Cluster

    /// <summary>
    /// Waits for a controller to be elected in the cluster.
    /// Replaces: await Task.Delay(1000) waiting for controller election.
    /// </summary>
    public static async Task<bool> WaitForControllerElectionAsync(
        IEnumerable<SurgewaveRuntime> brokers,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null,
        CancellationToken ct = default,
        ITestOutputHelper? output = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        var interval = pollInterval ?? DefaultPollInterval;
        var brokerList = brokers.ToList();

        while (DateTime.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                if (brokerList.Any(b => b.IsController))
                {
                    return true;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                output?.WriteLine($"[WaitForControllerElectionAsync] Polling error: {ex.Message}");
            }

            await Task.Delay(interval, ct);
        }

        return false;
    }

    /// <summary>
    /// Waits for cluster to stabilize with all brokers registered.
    /// Replaces: await Task.Delay(2000) after cluster initialization.
    /// </summary>
    public static async Task<bool> WaitForClusterStabilizationAsync(
        IEnumerable<SurgewaveRuntime> brokers,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null,
        CancellationToken ct = default,
        ITestOutputHelper? output = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        var interval = pollInterval ?? DefaultPollInterval;
        var brokerList = brokers.ToList();

        while (DateTime.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                // Check all brokers are running and have cluster state
                var allReady = brokerList.All(b =>
                    b.IsClusterEnabled &&
                    b.ClusterState != null);

                // Check exactly one controller
                var controllerCount = brokerList.Count(b => b.IsController);

                if (allReady && controllerCount == 1)
                {
                    return true;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                output?.WriteLine($"[WaitForClusterStabilizationAsync] Polling error: {ex.Message}");
            }

            await Task.Delay(interval, ct);
        }

        return false;
    }

    /// <summary>
    /// Waits for metadata to show expected broker count.
    /// Replaces: await Task.Delay(1000) waiting for broker discovery.
    /// </summary>
    public static async Task<bool> WaitForBrokerCountAsync(
        IAdminClient adminClient,
        int expectedBrokerCount,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null,
        CancellationToken ct = default,
        ITestOutputHelper? output = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        var interval = pollInterval ?? DefaultPollInterval;

        while (DateTime.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
                if (metadata.Brokers.Count >= expectedBrokerCount)
                {
                    return true;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                output?.WriteLine($"[WaitForBrokerCountAsync] Polling error: {ex.Message}");
            }

            await Task.Delay(interval, ct);
        }

        return false;
    }

    /// <summary>
    /// Waits for a broker to become unavailable (e.g., after shutdown).
    /// Replaces: await Task.Delay(12000) waiting for heartbeat timeout.
    /// </summary>
    public static async Task<bool> WaitForBrokerUnavailableAsync(
        IAdminClient adminClient,
        int brokerId,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null,
        CancellationToken ct = default,
        ITestOutputHelper? output = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        var interval = pollInterval ?? DefaultPollInterval;

        while (DateTime.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
                if (!metadata.Brokers.Any(b => b.BrokerId == brokerId))
                {
                    return true;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                output?.WriteLine($"[WaitForBrokerUnavailableAsync] Polling error: {ex.Message}");
            }

            await Task.Delay(interval, ct);
        }

        return false;
    }

    #endregion

    #region Messages

    /// <summary>
    /// Waits for messages to be consumable from a topic.
    /// Replaces: await Task.Delay(500) after producing messages.
    /// </summary>
    public static async Task<bool> WaitForMessagesAsync<TKey, TValue>(
        IConsumer<TKey, TValue> consumer,
        int expectedMessageCount,
        TimeSpan? timeout = null,
        CancellationToken ct = default,
        ITestOutputHelper? output = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        var consumedCount = 0;

        while (DateTime.UtcNow < deadline && consumedCount < expectedMessageCount)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(100));
                if (result != null && !result.IsPartitionEOF)
                {
                    consumedCount++;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (ConsumeException ex)
            {
                output?.WriteLine($"[WaitForMessagesAsync] Consume error: {ex.Message}");
            }
        }

        return consumedCount >= expectedMessageCount;
    }

    /// <summary>
    /// Waits for topic to have messages at a specific offset.
    /// Replaces: await Task.Delay(500) after producing.
    /// </summary>
    public static async Task<bool> WaitForOffsetAsync(
        IAdminClient adminClient,
        string bootstrapServers,
        string topicName,
        int partition,
        long expectedOffset,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null,
        CancellationToken ct = default,
        ITestOutputHelper? output = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        var interval = pollInterval ?? DefaultPollInterval;

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"offset-check-{Guid.NewGuid():N}",
            EnableAutoCommit = false
        };

        while (DateTime.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                using var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build();
                var watermarks = consumer.QueryWatermarkOffsets(
                    new TopicPartition(topicName, partition),
                    TimeSpan.FromSeconds(5));

                if (watermarks.High.Value >= expectedOffset)
                {
                    return true;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                output?.WriteLine($"[WaitForOffsetAsync] Polling error: {ex.Message}");
            }

            await Task.Delay(interval, ct);
        }

        return false;
    }

    #endregion

    #region Generic Helpers

    /// <summary>
    /// Generic polling wait with configurable predicate.
    /// </summary>
    public static async Task<T?> WaitForAsync<T>(
        Func<Task<T?>> getter,
        Func<T?, bool> predicate,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null,
        CancellationToken ct = default,
        ITestOutputHelper? output = null)
        where T : class
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        var interval = pollInterval ?? DefaultPollInterval;

        while (DateTime.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                var result = await getter();
                if (predicate(result))
                {
                    return result;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                output?.WriteLine($"[WaitForAsync] Polling error: {ex.Message}");
            }

            await Task.Delay(interval, ct);
        }

        return default;
    }

    /// <summary>
    /// Generic polling wait with a simple boolean condition.
    /// </summary>
    public static async Task<bool> WaitForConditionAsync(
        Func<Task<bool>> condition,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null,
        CancellationToken ct = default,
        ITestOutputHelper? output = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        var interval = pollInterval ?? DefaultPollInterval;

        while (DateTime.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                if (await condition())
                {
                    return true;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                output?.WriteLine($"[WaitForConditionAsync] Polling error: {ex.Message}");
            }

            await Task.Delay(interval, ct);
        }

        return false;
    }

    /// <summary>
    /// Generic polling wait with a synchronous boolean condition.
    /// </summary>
    public static async Task<bool> WaitForConditionAsync(
        Func<bool> condition,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null,
        CancellationToken ct = default,
        ITestOutputHelper? output = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        var interval = pollInterval ?? DefaultPollInterval;

        while (DateTime.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                if (condition())
                {
                    return true;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                output?.WriteLine($"[WaitForConditionAsync] Polling error: {ex.Message}");
            }

            await Task.Delay(interval, ct);
        }

        return false;
    }

    #endregion
}

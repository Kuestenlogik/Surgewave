using Confluent.Kafka;

namespace Kuestenlogik.Surgewave.Testing;

/// <summary>
/// Shared test utilities for creating common test configurations and data.
/// </summary>
public static class TestUtilities
{
    /// <summary>
    /// Creates a consumer configuration for testing.
    /// </summary>
    public static ConsumerConfig CreateConsumerConfig(
        string bootstrapServers,
        string groupId,
        string? clientId = null,
        AutoOffsetReset autoOffsetReset = AutoOffsetReset.Earliest)
    {
        return new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            ClientId = clientId ?? $"test-consumer-{Guid.NewGuid():N}",
            AutoOffsetReset = autoOffsetReset,
            EnableAutoCommit = false,
            SessionTimeoutMs = 6000,
            HeartbeatIntervalMs = 2000
        };
    }

    /// <summary>
    /// Creates a producer configuration for testing.
    /// </summary>
    public static ProducerConfig CreateProducerConfig(string bootstrapServers, string? clientId = null)
    {
        return new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            ClientId = clientId ?? $"test-producer-{Guid.NewGuid():N}",
            Acks = Acks.All
        };
    }

    /// <summary>
    /// Generates a unique topic name for testing.
    /// </summary>
    public static string GenerateTopicName(string prefix = "test")
    {
        return $"{prefix}-{Guid.NewGuid():N}";
    }

    /// <summary>
    /// Generates a unique group ID for testing.
    /// </summary>
    public static string GenerateGroupId(string prefix = "test-group")
    {
        return $"{prefix}-{Guid.NewGuid():N}";
    }

    /// <summary>
    /// Waits for a condition to become true with timeout.
    /// </summary>
    public static async Task<bool> WaitForCondition(
        Func<bool> condition,
        TimeSpan timeout,
        TimeSpan? pollInterval = null)
    {
        var interval = pollInterval ?? TimeSpan.FromMilliseconds(100);
        var deadline = DateTime.UtcNow + timeout;

        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return true;

            await Task.Delay(interval);
        }

        return false;
    }

    /// <summary>
    /// Waits for an async condition to become true with timeout.
    /// </summary>
    public static async Task<bool> WaitForConditionAsync(
        Func<Task<bool>> condition,
        TimeSpan timeout,
        TimeSpan? pollInterval = null)
    {
        var interval = pollInterval ?? TimeSpan.FromMilliseconds(100);
        var deadline = DateTime.UtcNow + timeout;

        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
                return true;

            await Task.Delay(interval);
        }

        return false;
    }
}

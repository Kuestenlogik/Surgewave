using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Kuestenlogik.Surgewave.Client.Validation;

/// <summary>
/// Provides validation guards for common parameter validation scenarios.
/// </summary>
public static class Guard
{
    /// <summary>
    /// Throws if the value is null or empty/whitespace.
    /// </summary>
    public static void NotNullOrEmpty(
        [NotNull] string? value,
        [CallerArgumentExpression(nameof(value))] string? paramName = null)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new InvalidConfigurationException(paramName ?? "value");
        }
    }

    /// <summary>
    /// Validates a topic name according to Kafka naming rules.
    /// </summary>
    public static void ValidTopicName(
        [NotNull] string? topic,
        [CallerArgumentExpression(nameof(topic))] string? paramName = null)
    {
        if (topic == null)
        {
            throw new InvalidConfigurationException(paramName ?? "topic");
        }

        var result = TopicNameValidator.Validate(topic);
        if (!result.IsValid)
        {
            throw new InvalidConfigurationException(
                paramName ?? "topic",
                topic,
                result.ErrorMessage);
        }
    }

    /// <summary>
    /// Validates a partition number.
    /// </summary>
    public static void ValidPartition(
        int partition,
        int? maxPartitions = null,
        [CallerArgumentExpression(nameof(partition))] string? paramName = null)
    {
        if (partition < 0)
        {
            throw new InvalidConfigurationException(
                paramName ?? "partition",
                partition,
                "partition must be >= 0");
        }

        if (maxPartitions.HasValue && partition >= maxPartitions.Value)
        {
            throw new InvalidConfigurationException(
                paramName ?? "partition",
                partition,
                $"partition must be < {maxPartitions.Value}");
        }
    }

    /// <summary>
    /// Validates a timeout value.
    /// </summary>
    public static void ValidTimeout(
        TimeSpan timeout,
        TimeSpan? maxTimeout = null,
        [CallerArgumentExpression(nameof(timeout))] string? paramName = null)
    {
        if (timeout <= TimeSpan.Zero)
        {
            throw new InvalidConfigurationException(
                paramName ?? "timeout",
                timeout,
                "timeout must be greater than zero");
        }

        var max = maxTimeout ?? TimeSpan.FromMinutes(10);
        if (timeout > max)
        {
            throw new InvalidConfigurationException(
                paramName ?? "timeout",
                timeout,
                $"timeout must be <= {max.TotalSeconds} seconds");
        }
    }

    /// <summary>
    /// Validates a timeout value in milliseconds.
    /// </summary>
    public static void ValidTimeoutMs(
        int timeoutMs,
        int? maxTimeoutMs = null,
        [CallerArgumentExpression(nameof(timeoutMs))] string? paramName = null)
    {
        if (timeoutMs <= 0)
        {
            throw new InvalidConfigurationException(
                paramName ?? "timeout",
                timeoutMs,
                "timeout must be greater than zero");
        }

        var max = maxTimeoutMs ?? 600000; // 10 minutes
        if (timeoutMs > max)
        {
            throw new InvalidConfigurationException(
                paramName ?? "timeout",
                timeoutMs,
                $"timeout must be <= {max}ms");
        }
    }

    /// <summary>
    /// Validates bootstrap servers format.
    /// </summary>
    public static void ValidBootstrapServers(
        [NotNull] string? servers,
        [CallerArgumentExpression(nameof(servers))] string? paramName = null)
    {
        if (string.IsNullOrWhiteSpace(servers))
        {
            throw new InvalidConfigurationException(paramName ?? "BootstrapServers");
        }

        // Validate each server in the list
        var serverList = servers.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (serverList.Length == 0)
        {
            throw new InvalidConfigurationException(
                paramName ?? "BootstrapServers",
                servers,
                "at least one server must be specified");
        }

        foreach (var server in serverList)
        {
            var result = BootstrapServerValidator.Validate(server);
            if (!result.IsValid)
            {
                throw new InvalidConfigurationException(
                    paramName ?? "BootstrapServers",
                    server,
                    result.ErrorMessage);
            }
        }
    }

    /// <summary>
    /// Validates that a value is within a specified range.
    /// </summary>
    public static void InRange<T>(
        T value,
        T min,
        T max,
        [CallerArgumentExpression(nameof(value))] string? paramName = null)
        where T : IComparable<T>
    {
        if (value.CompareTo(min) < 0 || value.CompareTo(max) > 0)
        {
            throw new InvalidConfigurationException(
                paramName ?? "value",
                value,
                $"must be between {min} and {max}");
        }
    }

    /// <summary>
    /// Validates that a value is greater than a minimum.
    /// </summary>
    public static void GreaterThan<T>(
        T value,
        T min,
        [CallerArgumentExpression(nameof(value))] string? paramName = null)
        where T : IComparable<T>
    {
        if (value.CompareTo(min) <= 0)
        {
            throw new InvalidConfigurationException(
                paramName ?? "value",
                value,
                $"must be greater than {min}");
        }
    }

    /// <summary>
    /// Validates that a value is greater than or equal to a minimum.
    /// </summary>
    public static void GreaterThanOrEqual<T>(
        T value,
        T min,
        [CallerArgumentExpression(nameof(value))] string? paramName = null)
        where T : IComparable<T>
    {
        if (value.CompareTo(min) < 0)
        {
            throw new InvalidConfigurationException(
                paramName ?? "value",
                value,
                $"must be >= {min}");
        }
    }

    /// <summary>
    /// Validates a client ID.
    /// </summary>
    public static void ValidClientId(
        string? clientId,
        [CallerArgumentExpression(nameof(clientId))] string? paramName = null)
    {
        if (clientId == null) return; // ClientId is optional

        if (clientId.Length > 255)
        {
            throw new InvalidConfigurationException(
                paramName ?? "ClientId",
                clientId,
                "must be <= 255 characters");
        }
    }

    /// <summary>
    /// Validates a group ID.
    /// </summary>
    public static void ValidGroupId(
        [NotNull] string? groupId,
        [CallerArgumentExpression(nameof(groupId))] string? paramName = null)
    {
        if (string.IsNullOrWhiteSpace(groupId))
        {
            throw new InvalidConfigurationException(paramName ?? "GroupId");
        }

        if (groupId.Length > 255)
        {
            throw new InvalidConfigurationException(
                paramName ?? "GroupId",
                groupId,
                "must be <= 255 characters");
        }
    }
}

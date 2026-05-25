using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;

namespace Kuestenlogik.Surgewave.Client.Extensions;

/// <summary>
/// Extension methods for IConsumer to access Surgewave-specific features.
/// </summary>
public static class SurgewaveConsumerExtensions
{
    /// <summary>
    /// Check if the consumer is using Surgewave Native protocol.
    /// </summary>
    public static bool IsSurgewaveNative<TKey, TValue>(this IConsumer<TKey, TValue> consumer)
        => consumer.Protocol == ProtocolType.SurgewaveNative;

    /// <summary>
    /// Check if the consumer is using Kafka protocol.
    /// </summary>
    public static bool IsKafka<TKey, TValue>(this IConsumer<TKey, TValue> consumer)
        => consumer.Protocol == ProtocolType.Kafka;

    /// <summary>
    /// Cast to SurgewaveConsumer if using Surgewave Native protocol.
    /// Returns null if using Kafka protocol.
    /// </summary>
    public static SurgewaveConsumer<TKey, TValue>? AsSurgewaveConsumer<TKey, TValue>(
        this IConsumer<TKey, TValue> consumer)
        => consumer as SurgewaveConsumer<TKey, TValue>;

    /// <summary>
    /// Register a handler for a specific derived type (Surgewave-only feature).
    /// Use with polymorphic deserializers to dispatch messages based on their runtime type.
    /// </summary>
    /// <exception cref="NotSupportedException">Thrown when using Kafka protocol.</exception>
    public static IConsumer<TKey, TValue> OnMessage<TKey, TValue, TDerived>(
        this IConsumer<TKey, TValue> consumer,
        Func<ConsumeResult<TKey, TDerived>, CancellationToken, Task> handler)
        where TDerived : TValue
    {
        if (consumer is SurgewaveConsumer<TKey, TValue> surgewaveConsumer)
        {
            surgewaveConsumer.OnMessage(handler);
            return consumer;
        }

        throw new NotSupportedException(
            "Handler-based message dispatch is only supported with Surgewave Native protocol. " +
            "Use manual pattern matching in consume loop for Kafka protocol.");
    }

    /// <summary>
    /// Pause consumption from the specified partitions (Surgewave-only feature).
    /// </summary>
    /// <exception cref="NotSupportedException">Thrown when using Kafka protocol.</exception>
    public static void Pause<TKey, TValue>(
        this IConsumer<TKey, TValue> consumer,
        params (string topic, int partition)[] partitions)
    {
        if (consumer is SurgewaveConsumer<TKey, TValue> surgewaveConsumer)
        {
            surgewaveConsumer.Pause(partitions);
            return;
        }

        throw new NotSupportedException(
            "Pause/Resume is only supported with Surgewave Native protocol.");
    }

    /// <summary>
    /// Resume consumption from the specified partitions (Surgewave-only feature).
    /// </summary>
    /// <exception cref="NotSupportedException">Thrown when using Kafka protocol.</exception>
    public static void Resume<TKey, TValue>(
        this IConsumer<TKey, TValue> consumer,
        params (string topic, int partition)[] partitions)
    {
        if (consumer is SurgewaveConsumer<TKey, TValue> surgewaveConsumer)
        {
            surgewaveConsumer.Resume(partitions);
            return;
        }

        throw new NotSupportedException(
            "Pause/Resume is only supported with Surgewave Native protocol.");
    }

    /// <summary>
    /// Get consumer lag for a specific partition (Surgewave-only feature).
    /// </summary>
    /// <exception cref="NotSupportedException">Thrown when using Kafka protocol.</exception>
    public static async Task<long> GetLagAsync<TKey, TValue>(
        this IConsumer<TKey, TValue> consumer,
        string topic,
        int partition,
        CancellationToken cancellationToken = default)
    {
        if (consumer is SurgewaveConsumer<TKey, TValue> surgewaveConsumer)
        {
            return await surgewaveConsumer.GetLagAsync(topic, partition, cancellationToken);
        }

        throw new NotSupportedException(
            "Lag tracking is only supported with Surgewave Native protocol.");
    }

    /// <summary>
    /// Get lag for all assigned partitions (Surgewave-only feature).
    /// </summary>
    /// <exception cref="NotSupportedException">Thrown when using Kafka protocol.</exception>
    public static async Task<Dictionary<(string topic, int partition), long>> GetAllLagAsync<TKey, TValue>(
        this IConsumer<TKey, TValue> consumer,
        CancellationToken cancellationToken = default)
    {
        if (consumer is SurgewaveConsumer<TKey, TValue> surgewaveConsumer)
        {
            return await surgewaveConsumer.GetAllLagAsync(cancellationToken);
        }

        throw new NotSupportedException(
            "Lag tracking is only supported with Surgewave Native protocol.");
    }

    /// <summary>
    /// Start a consume loop with handler dispatch (Surgewave-only feature).
    /// </summary>
    /// <exception cref="NotSupportedException">Thrown when using Kafka protocol.</exception>
    public static async Task ConsumeLoopAsync<TKey, TValue>(
        this IConsumer<TKey, TValue> consumer,
        CancellationToken cancellationToken = default)
    {
        if (consumer is SurgewaveConsumer<TKey, TValue> surgewaveConsumer)
        {
            await surgewaveConsumer.ConsumeLoopAsync(cancellationToken);
            return;
        }

        throw new NotSupportedException(
            "Handler-based consume loop is only supported with Surgewave Native protocol. " +
            "Use a manual consume loop with ConsumeAsync() for Kafka protocol.");
    }
}

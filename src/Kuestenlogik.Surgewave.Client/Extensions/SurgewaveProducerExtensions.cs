using Kuestenlogik.Surgewave.Client.Abstractions;

namespace Kuestenlogik.Surgewave.Client.Extensions;

/// <summary>
/// Extension methods for IProducer to access Surgewave-specific features.
/// </summary>
public static class SurgewaveProducerExtensions
{
    /// <summary>
    /// Check if the producer is using Surgewave Native protocol.
    /// </summary>
    public static bool IsSurgewaveNative<TKey, TValue>(this IProducer<TKey, TValue> producer)
        => producer.Protocol == ProtocolType.SurgewaveNative;

    /// <summary>
    /// Check if the producer is using Kafka protocol.
    /// </summary>
    public static bool IsKafka<TKey, TValue>(this IProducer<TKey, TValue> producer)
        => producer.Protocol == ProtocolType.Kafka;

    /// <summary>
    /// Cast to SurgewaveProducer if using Surgewave Native protocol.
    /// Returns null if using Kafka protocol.
    /// </summary>
    public static SurgewaveProducer<TKey, TValue>? AsSurgewaveProducer<TKey, TValue>(
        this IProducer<TKey, TValue> producer)
        => producer as SurgewaveProducer<TKey, TValue>;
}

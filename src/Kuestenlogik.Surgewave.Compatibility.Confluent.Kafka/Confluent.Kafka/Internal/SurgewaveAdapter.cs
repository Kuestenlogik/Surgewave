using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;

namespace Confluent.Kafka.Internal;

/// <summary>
/// Adapter utilities for converting between Confluent.Kafka and Surgewave.Client types.
/// </summary>
internal static class SurgewaveAdapter
{
    /// <summary>
    /// Convert Surgewave ProduceResult to Confluent DeliveryResult.
    /// </summary>
    public static DeliveryResult<TKey, TValue> ToDeliveryResult<TKey, TValue>(
        ProduceResult surgewaveResult,
        Message<TKey, TValue> message) => new()
        {
            Topic = surgewaveResult.Topic,
            Partition = new Partition(surgewaveResult.Partition),
            Offset = new Offset(surgewaveResult.Offset),
            Timestamp = new Timestamp(surgewaveResult.Timestamp),
            Message = message,
            Status = PersistenceStatus.Persisted
        };

    /// <summary>
    /// Convert Surgewave ConsumeResult to Confluent ConsumeResult.
    /// </summary>
    public static ConsumeResult<TKey, TValue> ToConsumeResult<TKey, TValue>(
        Kuestenlogik.Surgewave.Client.Consumer.ConsumeResult<byte[], byte[]> surgewaveResult,
        TKey? key,
        TValue? value) => new()
        {
            Topic = surgewaveResult.Topic,
            Partition = new Partition(surgewaveResult.Partition),
            Offset = new Offset(surgewaveResult.Offset),
            Timestamp = new Timestamp(surgewaveResult.Timestamp),
            Message = new Message<TKey, TValue>
            {
                Key = key,
                Value = value,
                Headers = surgewaveResult.Headers is not null
                    ? Headers.FromDictionary(surgewaveResult.Headers)
                    : null
            },
            IsPartitionEOF = false
        };

    /// <summary>
    /// Convert Confluent TopicPartitionOffset to Surgewave TopicPartitionOffset.
    /// </summary>
    public static TopicPartitionOffset ToSurgewaveTopicPartitionOffset(
        Confluent.Kafka.TopicPartitionOffset tpo) =>
        new(tpo.Topic, tpo.Partition.Value, tpo.Offset.Value);

    /// <summary>
    /// Convert Surgewave TopicPartitionOffset to Confluent TopicPartitionOffset.
    /// </summary>
    public static Confluent.Kafka.TopicPartitionOffset ToConfluentTopicPartitionOffset(
        TopicPartitionOffset surgewaveTpo) => new(
            surgewaveTpo.Topic,
            new Partition(surgewaveTpo.Partition),
            new Offset(surgewaveTpo.Offset));

    /// <summary>
    /// Determine protocol type from config.
    /// </summary>
    public static ProtocolType GetProtocolType(string? surgewaveProtocol) =>
        surgewaveProtocol?.ToLowerInvariant() switch
        {
            "surgewave" => ProtocolType.SurgewaveNative,
            "kafka" => ProtocolType.Kafka,
            "auto" => ProtocolType.Auto,
            _ => ProtocolType.Auto
        };
}

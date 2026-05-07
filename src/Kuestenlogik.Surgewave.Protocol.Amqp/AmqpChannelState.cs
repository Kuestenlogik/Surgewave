namespace Kuestenlogik.Surgewave.Protocol.Amqp;

/// <summary>
/// Tracks per-channel AMQP state: declared exchanges, queues, bindings, and active consumers.
/// </summary>
internal sealed class AmqpChannelState
{
    /// <summary>Channel number (1-based).</summary>
    public ushort ChannelNumber { get; }

    /// <summary>Exchanges declared on this channel: name → type.</summary>
    public Dictionary<string, AmqpExchangeType> Exchanges { get; } = new(StringComparer.Ordinal);

    /// <summary>Queues declared on this channel: queue name → Surgewave topic.</summary>
    public Dictionary<string, string> Queues { get; } = new(StringComparer.Ordinal);

    /// <summary>Queue bindings: queue name → (exchange name, routing key).</summary>
    public Dictionary<string, (string Exchange, string RoutingKey)> Bindings { get; } = new(StringComparer.Ordinal);

    /// <summary>Active consumers: consumer tag → queue name.</summary>
    public Dictionary<string, string> Consumers { get; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Maps AMQP delivery tags to Surgewave offsets for offset-commit on Ack (fallback path, no QueueView).
    /// Key: deliveryTag, Value: (topic, partition, offset).
    /// </summary>
    public Dictionary<ulong, (string Topic, int Partition, long Offset)> DeliveryTagToOffset { get; } = [];

    /// <summary>Highest committed offset per topic-partition (fallback path, no QueueView).</summary>
    public Dictionary<(string Topic, int Partition), long> CommittedOffsets { get; } = [];

    /// <summary>
    /// Maps AMQP delivery tags to QueueView message IDs (used when QueueView is active).
    /// Key: deliveryTag, Value: QueueView message ID string.
    /// </summary>
    public Dictionary<ulong, string> DeliveryTagToMessageId { get; } = [];

    /// <summary>Max tracked delivery tags before pruning oldest entries.</summary>
    private const int MaxTrackedDeliveryTags = 100_000;

    public AmqpChannelState(ushort channelNumber)
    {
        ChannelNumber = channelNumber;
    }

    /// <summary>
    /// Prunes oldest delivery tag entries if dictionaries exceed the limit.
    /// Called periodically during consume to prevent unbounded memory growth.
    /// </summary>
    public void PruneIfNeeded()
    {
        if (DeliveryTagToOffset.Count > MaxTrackedDeliveryTags)
        {
            var oldest = DeliveryTagToOffset.Keys.Order().Take(DeliveryTagToOffset.Count / 2).ToList();
            foreach (var key in oldest)
                DeliveryTagToOffset.Remove(key);
        }

        if (DeliveryTagToMessageId.Count > MaxTrackedDeliveryTags)
        {
            var oldest = DeliveryTagToMessageId.Keys.Order().Take(DeliveryTagToMessageId.Count / 2).ToList();
            foreach (var key in oldest)
                DeliveryTagToMessageId.Remove(key);
        }
    }
}

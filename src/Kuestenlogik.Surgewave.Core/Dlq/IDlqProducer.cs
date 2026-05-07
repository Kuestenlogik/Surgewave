namespace Kuestenlogik.Surgewave.Core.Dlq;

/// <summary>
/// Abstraction for producing messages to DLQ topics.
/// Allows DLQ routing to work with different producer implementations.
/// </summary>
public interface IDlqProducer
{
    /// <summary>
    /// Produce a message to a DLQ topic.
    /// </summary>
    /// <param name="topic">The DLQ topic name.</param>
    /// <param name="key">Optional message key.</param>
    /// <param name="value">The message value (serialized DLQ record).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task ProduceAsync(string topic, byte[]? key, byte[] value, CancellationToken cancellationToken = default);

    /// <summary>
    /// Ensure the DLQ topic exists, creating it if necessary.
    /// </summary>
    /// <param name="topic">The DLQ topic name.</param>
    /// <param name="partitionCount">Number of partitions to create.</param>
    /// <param name="config">Optional topic configuration.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task EnsureTopicExistsAsync(
        string topic,
        int partitionCount,
        Dictionary<string, string>? config = null,
        CancellationToken cancellationToken = default);
}
